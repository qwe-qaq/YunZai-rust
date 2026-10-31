//! 一条「协议端 ↔ Yunzai」会话。
//!
//! 结构上是一个双向管道：
//!
//! ```text
//!   upstream_ws  --read-->  [event / action 响应]  -->  queue  -->  downstream_ws
//!   upstream_ws  <--write-- [action]                <--  queue  <-- downstream_ws --read--
//! ```
//!
//! 关键点：**一个上游连接固定对应一条下游连接**。Yunzai 是 WebSocket 服务端，
//! 每条连接在它内部就是一个 `Bot[self_id]`，所以网关按账号各建一条连接，
//! 就能完整保留 Yunzai 的多账号语义（Bot[data.self_id]、Bot.uin、pickFriend 跨账号路由），
//! 而不需要在 action 里猜测 self_id。

use crate::status::Stats;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// 单任务使用的令牌桶。rate <= 0 表示不限速。
struct RateLimiter {
    rate: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    fn new(rate: f64) -> Self {
        Self {
            rate,
            tokens: rate.max(1.0),
            last: Instant::now(),
        }
    }

    async fn acquire(&mut self) {
        if self.rate <= 0.0 {
            return;
        }
        loop {
            let now = Instant::now();
            let elapsed = now.duration_since(self.last).as_secs_f64();
            self.last = now;
            self.tokens = (self.tokens + elapsed * self.rate).min(self.rate);
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return;
            }
            let wait = (1.0 - self.tokens) / self.rate;
            tokio::time::sleep(Duration::from_secs_f64(wait.min(1.0))).await;
        }
    }
}

pub struct SessionParams {
    pub name: String,
    pub self_id: i64,
    pub queue: usize,
    pub action_rate: f64,
    pub event_rate: f64,
    pub keepalive: Duration,
    pub stats: Arc<Stats>,
}

/// 运行会话，直到任一端断开。
///
/// 泛型化底层传输，使「网关主动连出去」的 `MaybeTlsStream` 与
/// 「反向 WS 接进来」的 `TcpStream` 能共用同一套会话逻辑。
pub async fn run<U, D>(up: WebSocketStream<U>, down: WebSocketStream<D>, p: SessionParams)
where
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    D: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut up_write, mut up_read) = up.split();
    let (mut down_write, mut down_read) = down.split();

    let cap = p.queue;
    let (to_down_tx, mut to_down_rx) = mpsc::channel::<Message>(cap);
    let (to_up_tx, mut to_up_rx) = mpsc::channel::<Message>(cap);
    // 收尾时用它俩把 Close 帧塞进写队列，让对端看到优雅关闭而不是 TCP 被掐断
    let close_down = to_down_tx.clone();
    let close_up = to_up_tx.clone();

    let mut set = JoinSet::new();

    // 上游 -> 下游：事件与 action 响应都走这条路
    {
        let stats = p.stats.clone();
        let name = p.name.clone();
        let mut limiter = RateLimiter::new(p.event_rate);
        set.spawn(async move {
            loop {
                match up_read.next().await {
                    Some(Ok(Message::Text(t))) => {
                        let bytes = t.len() as u64;
                        stats.bytes_up.fetch_add(bytes, Ordering::Relaxed);
                        let is_ev = serde_json::from_str::<serde_json::Value>(&t)
                            .map(|v| v.get("post_type").is_some())
                            .unwrap_or(false);
                        if is_ev {
                            stats.events.fetch_add(1, Ordering::Relaxed);
                            limiter.acquire().await;
                        } else {
                            stats.responses.fetch_add(1, Ordering::Relaxed);
                        }
                        if to_down_tx.send(Message::Text(t)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        tracing::trace!(upstream = %name, len = payload.len(), "收到上游 Ping");
                    }
                    Some(Ok(Message::Close(frame))) => {
                        tracing::info!(upstream = %name, ?frame, "上游发来 Close 帧");
                        break;
                    }
                    None => {
                        tracing::info!(upstream = %name, "上游流结束");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::warn!(upstream = %name, error = %e, "上游读取出错");
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
            "up_read"
        });
    }

    // 下游写：把队列里的事件发给 Yunzai
    set.spawn(async move {
        while let Some(msg) = to_down_rx.recv().await {
            if down_write.send(msg).await.is_err() {
                break;
            }
        }
        let _ = down_write.close().await;
        "down_write"
    });

    // 下游 -> 上游：action 透传
    let ka_tx = to_up_tx.clone();
    {
        let stats = p.stats.clone();
        let name = p.name.clone();
        let mut limiter = RateLimiter::new(p.action_rate);
        set.spawn(async move {
            loop {
                match down_read.next().await {
                    Some(Ok(Message::Text(t))) => {
                        stats
                            .bytes_down
                            .fetch_add(t.len() as u64, Ordering::Relaxed);
                        stats.actions.fetch_add(1, Ordering::Relaxed);
                        limiter.acquire().await;
                        if to_up_tx.send(Message::Text(t)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        tracing::info!(upstream = %name, ?frame, "Yunzai 发来 Close 帧");
                        break;
                    }
                    None => {
                        tracing::info!(upstream = %name, "Yunzai 流结束");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::warn!(upstream = %name, error = %e, "Yunzai 读取出错");
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
            "down_read"
        });
    }

    // 上游写：把 Yunzai 下发的 action 发给协议端
    set.spawn(async move {
        while let Some(msg) = to_up_rx.recv().await {
            if up_write.send(msg).await.is_err() {
                break;
            }
        }
        let _ = up_write.close().await;
        "up_write"
    });

    // 心跳：部分协议端在空闲时会掐连接，主动 Ping 保活
    if !p.keepalive.is_zero() {
        let mut ticker = tokio::time::interval(p.keepalive);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // 第一个 tick 立刻返回，跳过
        set.spawn(async move {
            loop {
                ticker.tick().await;
                if ka_tx.send(Message::Ping(Vec::new())).await.is_err() {
                    break;
                }
            }
            "keepalive"
        });
    }

    // 任一管道结束，整条会话结束（例如协议端掉线 -> Yunzai 侧连接一并断开，
    // 让 Yunzai 正确进入离线状态，而不是挂着一条死连接）
    if let Some(joined) = set.join_next().await {
        match joined {
            Ok(which) => tracing::warn!(upstream = %p.name, task = which, "会话中的任务先退出"),
            Err(e) => tracing::error!(upstream = %p.name, error = %e, "会话任务异常退出"),
        }
    }

    // 双向都发一帧 Close，再留一点时间把缓冲刷出去。
    // 直接 abort 会让对端只看到 TCP 断开（协议端可能记为异常掉线）。
    let _ = close_down.send(Message::Close(None)).await;
    let _ = close_up.send(Message::Close(None)).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    set.shutdown().await;
    tracing::info!(upstream = %p.name, self_id = p.self_id, "会话结束");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limiter_allows_unlimited() {
        let mut l = RateLimiter::new(0.0);
        let t = Instant::now();
        for _ in 0..1000 {
            l.acquire().await;
        }
        assert!(t.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn limiter_throttles() {
        let mut l = RateLimiter::new(100.0); // 100/s
        let t = Instant::now();
        for _ in 0..20 {
            l.acquire().await;
        }
        // 初始桶容量为 100，20 次不应有等待
        assert!(t.elapsed() < Duration::from_millis(50));
    }
}
