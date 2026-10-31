//! 上游（协议端）连接的生命周期管理：建连、预热、会话、退避重连。
//!
//! 支持两种接入方式：
//! - `forward`：网关主动连协议端的正向 WS（NapCat / Lagrange 最常用）
//! - `reverse`：协议端主动连网关（反向 WS），网关按首帧里的 self_id 配对

use crate::config::{Config, DownstreamCfg, LimitsCfg, Mode, UpstreamCfg};
use crate::session::{self, SessionParams};
use crate::status::Global;
use crate::ws;
use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::{
    handshake::server::{ErrorResponse, Request, Response},
    http::StatusCode,
    Message,
};

/// 指数退避 + 抖动，避免多账号在同一时刻一起重连。
struct Backoff {
    max: u64,
    current: u64,
    seed: u64,
}

impl Backoff {
    fn new(min: u64, max: u64, seed: u64) -> Self {
        Self {
            max,
            current: min,
            seed: seed | 1,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let base = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        // 线性同余抖动，范围 ±25%
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let span = (base / 4).max(1);
        let jitter = self.seed % span;
        Duration::from_millis(base + jitter)
    }
}

pub async fn supervise(
    cfg: UpstreamCfg,
    down: DownstreamCfg,
    limits: LimitsCfg,
    global: Arc<Global>,
) {
    let mut backoff = Backoff::new(
        cfg.reconnect_min_ms,
        cfg.reconnect_max_ms,
        cfg.self_id as u64,
    );
    let mut attempt: u64 = 0;

    loop {
        global.mark_connecting(&cfg.name);
        if attempt > 0 {
            let delay = backoff.next_delay();
            tracing::info!(
                upstream = %cfg.name,
                attempt,
                delay_ms = delay.as_millis() as u64,
                "等待重连"
            );
            tokio::time::sleep(delay).await;
        }

        let result = match cfg.mode {
            Mode::Forward => run_forward(&cfg, &down, &limits, &global).await,
            Mode::Reverse => run_reverse(&cfg, &down, &limits, &global).await,
        };

        match result {
            Ok(()) => tracing::info!(upstream = %cfg.name, "会话正常结束"),
            Err(e) => {
                attempt += 1;
                global.mark_error(&cfg.name, &e.to_string());
                tracing::warn!(upstream = %cfg.name, attempt, error = %e, "上游会话失败");
            }
        }
        global.mark_disconnected(&cfg.name);
    }
}

async fn run_forward(
    cfg: &UpstreamCfg,
    down_cfg: &DownstreamCfg,
    limits: &LimitsCfg,
    global: &Arc<Global>,
) -> Result<()> {
    // 顺序很重要：先确认 Yunzai 可用，再去连协议端。
    // 否则 Yunzai 尚未就绪时会白连一次协议端，随即断开重连——
    // 对协议端来说就是一次无意义的抖动（还可能触发风控）。
    let mut down = ws::connect(
        &down_cfg.url,
        &down_cfg.token,
        Duration::from_millis(down_cfg.connect_timeout_ms),
    )
    .await?;

    let mut up = ws::connect(
        &cfg.url,
        &cfg.token,
        Duration::from_millis(down_cfg.connect_timeout_ms),
    )
    .await?;

    // 预热：等协议端的首帧（通常是 lifecycle/connect）。
    // 拿到就原样转发，拿不到就合成一条，保证 Yunzai 一定会初始化 Bot。
    let primed = tokio::time::timeout(Duration::from_millis(limits.prime_timeout_ms), up.next())
        .await
        .ok()
        .flatten()
        .and_then(|r| r.ok())
        .and_then(|m| match m {
            Message::Text(t) => Some(t.to_string()),
            _ => None,
        });

    let is_lifecycle = primed
        .as_deref()
        .and_then(|t| serde_json::from_str::<Value>(t).ok())
        .map(|v| {
            v.get("post_type").and_then(|x| x.as_str()) == Some("meta_event")
                && v.get("meta_event_type").and_then(|x| x.as_str()) == Some("lifecycle")
        })
        .unwrap_or(false);

    if !is_lifecycle {
        let synthetic = synthetic_lifecycle(cfg.self_id);
        down.send(Message::Text(synthetic)).await?;
        tracing::info!(upstream = %cfg.name, "未收到协议端 lifecycle，已合成一条");
    }
    if let Some(first) = primed {
        down.send(Message::Text(first)).await?;
    }

    global.mark_connected(&cfg.name);
    tracing::info!(
        upstream = %cfg.name,
        self_id = cfg.self_id,
        "已连通协议端与 Yunzai"
    );

    session::run(up, down, params(cfg, limits, global)).await;
    Ok(())
}

async fn run_reverse(
    cfg: &UpstreamCfg,
    down_cfg: &DownstreamCfg,
    limits: &LimitsCfg,
    global: &Arc<Global>,
) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    global.registry.register(cfg.self_id, tx).await;

    let received = tokio::time::timeout(Duration::from_secs(600), rx.recv()).await;
    global.registry.unregister(cfg.self_id).await;

    let (up, first) = match received {
        Ok(Some(slot)) => slot,
        Ok(None) => bail!("反向接入通道被关闭"),
        Err(_) => bail!("600 秒内没有协议端接入"),
    };

    let mut down = ws::connect(
        &down_cfg.url,
        &down_cfg.token,
        Duration::from_millis(down_cfg.connect_timeout_ms),
    )
    .await?;

    let is_lifecycle = first
        .as_deref()
        .and_then(|t| serde_json::from_str::<Value>(t).ok())
        .map(|v| {
            v.get("post_type").and_then(|x| x.as_str()) == Some("meta_event")
                && v.get("meta_event_type").and_then(|x| x.as_str()) == Some("lifecycle")
        })
        .unwrap_or(false);

    if !is_lifecycle {
        down.send(Message::Text(synthetic_lifecycle(cfg.self_id)))
            .await?;
    }
    if let Some(first) = first {
        down.send(Message::Text(first)).await?;
    }

    global.mark_connected(&cfg.name);
    tracing::info!(upstream = %cfg.name, self_id = cfg.self_id, "反向接入已配对");

    session::run(up, down, params(cfg, limits, global)).await;
    Ok(())
}

fn params(cfg: &UpstreamCfg, limits: &LimitsCfg, global: &Arc<Global>) -> SessionParams {
    SessionParams {
        name: cfg.name.clone(),
        self_id: cfg.self_id,
        queue: limits.queue,
        action_rate: limits.action_rate_per_sec,
        event_rate: limits.event_rate_per_sec,
        keepalive: Duration::from_secs(limits.keepalive_secs),
        stats: global.stats.clone(),
    }
}

fn synthetic_lifecycle(self_id: i64) -> String {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    json!({
        "time": time,
        "self_id": self_id,
        "post_type": "meta_event",
        "meta_event_type": "lifecycle",
        "sub_type": "connect"
    })
    .to_string()
}

/// 反向 WS 接入服务：接受协议端连接，按首帧的 self_id 交给等待中的上游任务。
pub async fn serve_reverse(listen: &str, cfg: Arc<Config>, global: Arc<Global>) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| anyhow!("反向 WS 监听 {listen} 失败: {e}"))?;
    tracing::info!(%listen, "反向 WS 接入已启动");

    loop {
        let (stream, peer) = listener.accept().await?;
        let cfg = cfg.clone();
        let global = global.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_reverse_conn(stream, cfg, global).await {
                tracing::warn!(%peer, error = %e, "反向连接未完成配对");
            }
        });
    }
}

async fn handle_reverse_conn(
    stream: TcpStream,
    cfg: Arc<Config>,
    global: Arc<Global>,
) -> Result<()> {
    let token = cfg.server.reverse_token.clone();
    // 握手回调的返回类型由 tungstenite 的 accept_hdr_async 签名决定，
    // ErrorResponse（http::Response<Option<String>>）超过 clippy 的 128 字节阈值，
    // 这里无法改成 Box，只能定点豁免。
    #[allow(clippy::result_large_err)]
    let ws = tokio_tungstenite::accept_hdr_async(
        stream,
        move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
            if token.is_empty() {
                return Ok(resp);
            }
            let header_ok = req
                .headers()
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .map(|v| v == token)
                .unwrap_or(false);
            let query_ok = req
                .uri()
                .query()
                .map(|q| q.split('&').any(|kv| kv == format!("access_token={token}")))
                .unwrap_or(false);
            if header_ok || query_ok {
                Ok(resp)
            } else {
                let mut err = ErrorResponse::new(Some("unauthorized".into()));
                *err.status_mut() = StatusCode::UNAUTHORIZED;
                Err(err)
            }
        },
    )
    .await
    .map_err(|e| anyhow!("WS 握手失败: {e}"))?;

    let mut ws = ws;
    let first = tokio::time::timeout(Duration::from_secs(15), ws.next())
        .await
        .map_err(|_| anyhow!("等待首帧超时"))?
        .ok_or_else(|| anyhow!("连接在首帧前关闭"))?
        .map_err(|e| anyhow!("读取首帧失败: {e}"))?;

    let first_text = match first {
        Message::Text(t) => t.to_string(),
        other => bail!("首帧类型不支持: {other:?}"),
    };

    let parsed: Value =
        serde_json::from_str(&first_text).map_err(|e| anyhow!("首帧不是合法 JSON: {e}"))?;
    let self_id = ws::self_id_of(&parsed).ok_or_else(|| anyhow!("首帧缺少 self_id"))?;

    if !cfg.active_upstreams().any(|u| u.self_id == self_id) {
        bail!("self_id {self_id} 未在配置的 upstream 中声明");
    }

    if global
        .registry
        .deliver(self_id, (ws, Some(first_text)))
        .await
    {
        tracing::info!(self_id, "反向连接已交付给上游任务");
        Ok(())
    } else {
        bail!("self_id {self_id} 当前没有等待中的上游任务");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let mut b = Backoff::new(500, 30_000, 12345);
        let d1 = b.next_delay().as_millis();
        let d2 = b.next_delay().as_millis();
        let d3 = b.next_delay().as_millis();
        assert!((500..700).contains(&d1));
        assert!(d2 > d1);
        assert!(d3 > d2);
        for _ in 0..20 {
            let _ = b.next_delay();
        }
        let d = b.next_delay().as_millis();
        assert!(d <= 30_000 + 7_500);
    }

    #[test]
    fn lifecycle_shape() {
        let v: Value = serde_json::from_str(&synthetic_lifecycle(42)).unwrap();
        assert_eq!(v["post_type"], "meta_event");
        assert_eq!(v["meta_event_type"], "lifecycle");
        assert_eq!(v["self_id"], 42);
    }
}
