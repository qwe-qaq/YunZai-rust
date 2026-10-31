//! 极简 CDP（Chrome DevTools Protocol）客户端。
//!
//! 只实现渲染需要的部分：请求/响应按 `id` 配对，事件用广播分发给所有订阅者。
//! 用 `flatten: true` 的 session 模式，一个 WebSocket 上可以并发跑多个 page。

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message;

pub struct Cdp {
    tx: mpsc::UnboundedSender<Message>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    pub events: broadcast::Sender<Value>,
    next_id: AtomicU64,
}

impl Cdp {
    pub async fn connect(url: &str) -> Result<Arc<Self>> {
        let (ws, _) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::connect_async(url),
        )
        .await
        .context("连接 CDP 超时")?
        .context("连接 CDP 失败")?;

        let (mut write, mut read) = ws.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if write.send(msg).await.is_err() {
                    break;
                }
            }
        });

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (events, _) = broadcast::channel(2048);

        let p = pending.clone();
        let e = events.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = read.next().await {
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Close(_) => break,
                    _ => continue,
                };
                let v: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Some(id) = v.get("id").and_then(|x| x.as_u64()) {
                    if let Some(tx) = p.lock().await.remove(&id) {
                        let _ = tx.send(v);
                    }
                } else if v.get("method").is_some() {
                    // 没有订阅者时 send 会失败，忽略即可
                    let _ = e.send(v);
                }
            }
            // 连接断开：唤醒所有等待者，避免请求一直挂到超时
            let mut guard = p.lock().await;
            for (_, tx) in guard.drain() {
                let _ = tx.send(json!({"error": {"message": "CDP 连接已断开"}}));
            }
        });

        Ok(Arc::new(Self {
            tx,
            pending,
            events,
            next_id: AtomicU64::new(1),
        }))
    }

    pub async fn call(&self, method: &str, params: Value, session: Option<&str>) -> Result<Value> {
        self.call_timeout(method, params, session, Duration::from_secs(30))
            .await
    }

    pub async fn call_timeout(
        &self,
        method: &str,
        params: Value,
        session: Option<&str>,
        timeout: Duration,
    ) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut req = json!({"id": id, "method": method, "params": params});
        if let Some(s) = session {
            req["sessionId"] = json!(s);
        }

        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        if self.tx.send(Message::Text(req.to_string().into())).is_err() {
            self.pending.lock().await.remove(&id);
            bail!("CDP 通道已关闭");
        }

        let resp = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(v)) => v,
            Ok(Err(_)) => bail!("CDP 响应通道取消"),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                bail!("CDP 调用超时: {method}")
            }
        };

        if let Some(err) = resp.get("error") {
            let msg = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("未知 CDP 错误");
            return Err(anyhow!("CDP {method} 失败: {msg}"));
        }

        Ok(resp.get("result").cloned().unwrap_or(Value::Null))
    }
}
