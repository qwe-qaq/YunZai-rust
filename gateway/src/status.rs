//! 运行状态与指标。
//!
//! 这里刻意用同步 RwLock：临界区只有「换几个字段」，没有 await，
//! 比异步锁更轻，也不会在状态查询和转发路径之间制造阻塞。

use crate::config::Config;
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

#[derive(Default)]
pub struct Stats {
    pub events: AtomicU64,
    pub actions: AtomicU64,
    pub responses: AtomicU64,
    pub bytes_up: AtomicU64,
    pub bytes_down: AtomicU64,
    pub errors: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            events: self.events.load(Ordering::Relaxed),
            actions: self.actions.load(Ordering::Relaxed),
            responses: self.responses.load(Ordering::Relaxed),
            bytes_up: self.bytes_up.load(Ordering::Relaxed),
            bytes_down: self.bytes_down.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
pub struct StatsSnapshot {
    pub events: u64,
    pub actions: u64,
    pub responses: u64,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpstreamState {
    pub name: String,
    pub self_id: i64,
    pub mode: String,
    /// connecting | connected | disconnected | error
    pub phase: String,
    pub reconnects: u64,
    pub last_error: Option<String>,
    pub updated_at: u64,
}

/// 反向 WS 交出来的连接：底层直接是 TcpStream（没有 TLS 包装）。
pub type ReverseSlot = (
    tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    Option<String>,
);

/// 反向 WS 的「待配对」队列：协议端连进来时，把连接交给对应 self_id 的等待者。
pub struct ReverseRegistry {
    waiters: tokio::sync::Mutex<BTreeMap<i64, mpsc::Sender<ReverseSlot>>>,
}

impl ReverseRegistry {
    pub fn new() -> Self {
        Self {
            waiters: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn register(&self, self_id: i64, tx: mpsc::Sender<ReverseSlot>) {
        self.waiters.lock().await.insert(self_id, tx);
    }

    pub async fn unregister(&self, self_id: i64) {
        self.waiters.lock().await.remove(&self_id);
    }

    pub async fn deliver(&self, self_id: i64, slot: ReverseSlot) -> bool {
        let guard = self.waiters.lock().await;
        match guard.get(&self_id) {
            Some(tx) => tx.send(slot).await.is_ok(),
            None => false,
        }
    }
}

pub struct Global {
    pub started: Instant,
    pub stats: Arc<Stats>,
    pub registry: Arc<ReverseRegistry>,
    upstreams: RwLock<BTreeMap<String, UpstreamState>>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl Global {
    pub fn new(cfg: &Config) -> Self {
        let mut map = BTreeMap::new();
        for u in cfg.active_upstreams() {
            map.insert(
                u.name.clone(),
                UpstreamState {
                    name: u.name.clone(),
                    self_id: u.self_id,
                    mode: u.mode.as_str().to_string(),
                    phase: "connecting".into(),
                    reconnects: 0,
                    last_error: None,
                    updated_at: now_secs(),
                },
            );
        }
        Self {
            started: Instant::now(),
            stats: Arc::new(Stats::default()),
            registry: Arc::new(ReverseRegistry::new()),
            upstreams: RwLock::new(map),
        }
    }

    fn update<F: FnOnce(&mut UpstreamState)>(&self, name: &str, f: F) {
        if let Ok(mut guard) = self.upstreams.write() {
            if let Some(st) = guard.get_mut(name) {
                f(st);
                st.updated_at = now_secs();
            }
        }
    }

    pub fn mark_connecting(&self, name: &str) {
        self.update(name, |s| s.phase = "connecting".into());
    }

    pub fn mark_connected(&self, name: &str) {
        self.update(name, |s| {
            s.phase = "connected".into();
            s.last_error = None;
        });
    }

    pub fn mark_disconnected(&self, name: &str) {
        self.update(name, |s| {
            s.phase = "disconnected".into();
            s.reconnects += 1;
        });
    }

    pub fn mark_error(&self, name: &str, err: &str) {
        self.stats.errors.fetch_add(1, Ordering::Relaxed);
        self.update(name, |s| {
            s.phase = "error".into();
            s.last_error = Some(err.chars().take(300).collect());
        });
    }

    pub fn snapshot(&self) -> (Vec<UpstreamState>, StatsSnapshot, u64) {
        let list = self
            .upstreams
            .read()
            .map(|g| g.values().cloned().collect())
            .unwrap_or_default();
        (
            list,
            self.stats.snapshot(),
            self.started.elapsed().as_secs(),
        )
    }
}

async fn healthz() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct StatusBody {
    uptime_secs: u64,
    upstreams: Vec<UpstreamState>,
    stats: StatsSnapshot,
}

async fn status(State(g): State<Arc<Global>>) -> Json<StatusBody> {
    let (upstreams, stats, uptime_secs) = g.snapshot();
    Json(StatusBody {
        uptime_secs,
        upstreams,
        stats,
    })
}

async fn metrics(State(g): State<Arc<Global>>) -> impl IntoResponse {
    let (upstreams, s, uptime) = g.snapshot();
    let mut out = String::with_capacity(1024);
    out.push_str("# HELP yunzai_gateway_uptime_seconds 运行时长\n");
    out.push_str("# TYPE yunzai_gateway_uptime_seconds gauge\n");
    out.push_str(&format!("yunzai_gateway_uptime_seconds {uptime}\n"));

    for (name, value) in [
        ("events", s.events),
        ("actions", s.actions),
        ("responses", s.responses),
        ("bytes_up", s.bytes_up),
        ("bytes_down", s.bytes_down),
        ("errors", s.errors),
    ] {
        out.push_str(&format!(
            "# TYPE yunzai_gateway_{name}_total counter\nyunzai_gateway_{name}_total {value}\n"
        ));
    }

    out.push_str("# TYPE yunzai_gateway_upstream_connected gauge\n");
    out.push_str("# TYPE yunzai_gateway_upstream_reconnects counter\n");
    for u in upstreams {
        let v = if u.phase == "connected" { 1 } else { 0 };
        out.push_str(&format!(
            "yunzai_gateway_upstream_connected{{name=\"{}\",self_id=\"{}\"}} {v}\n",
            u.name, u.self_id
        ));
        out.push_str(&format!(
            "yunzai_gateway_upstream_reconnects{{name=\"{}\"}} {}\n",
            u.name, u.reconnects
        ));
    }
    ([("content-type", "text/plain; version=0.0.4")], out)
}

pub async fn serve(listen: &str, global: Arc<Global>) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .with_state(global);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| anyhow::anyhow!("状态服务监听 {listen} 失败: {e}"))?;
    tracing::info!(%listen, "状态服务已启动 (/healthz /status /metrics)");
    axum::serve(listener, app).await?;
    Ok(())
}
