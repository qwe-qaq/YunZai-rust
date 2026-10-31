//! yz-gateway —— Yunzai 的 OneBot v11 接入网关。
//!
//! 位置：夹在「真实协议端」和「Yunzai」中间，对协议端是客户端/服务端，
//! 对 Yunzai 则伪装成协议端（Yunzai 只认 `/OneBotv11` 这条 WS 路径）。
//!
//! 它不解析业务语义、不碰插件，只做四件事：连接管理、账号配对、双向透传、限流观测。
//! 因此 Yunzai 侧不需要改任何插件，只需把协议端地址指向网关。

mod config;
mod session;
mod status;
mod upstream;
mod ws;

use anyhow::Result;
use std::sync::Arc;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/gateway.toml".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cfg = Arc::new(config::Config::load(&cfg_path)?);
    tracing::info!(
        file = %cfg_path,
        upstreams = cfg.active_upstreams().count(),
        "配置已加载"
    );

    let global = Arc::new(status::Global::new(&cfg));

    // 状态 / 指标服务
    {
        let global = global.clone();
        let listen = cfg.server.status_listen.clone();
        tokio::spawn(async move {
            if let Err(e) = status::serve(&listen, global).await {
                tracing::error!(error = %e, "状态服务退出");
            }
        });
    }

    // 反向 WS 接入（仅在存在 reverse 模式上游时启动）
    if cfg
        .active_upstreams()
        .any(|u| u.mode == config::Mode::Reverse)
    {
        let global = global.clone();
        let listen = cfg.server.reverse_listen.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = upstream::serve_reverse(&listen, cfg2, global).await {
                tracing::error!(error = %e, "反向 WS 服务退出");
            }
        });
    }

    let mut tasks = JoinSet::new();
    for up in cfg.active_upstreams().cloned() {
        let global = global.clone();
        let down = cfg.downstream.clone();
        let limits = cfg.limits.clone();
        tasks.spawn(async move {
            upstream::supervise(up, down, limits, global).await;
        });
    }

    tokio::select! {
        r = tokio::signal::ctrl_c() => {
            if let Err(e) = r {
                tracing::warn!(error = %e, "监听退出信号失败");
            }
            tracing::info!("收到退出信号，正在关闭");
        }
        Some(joined) = tasks.join_next() => {
            match joined {
                Ok(()) => tracing::warn!("上游任务意外退出"),
                Err(e) => tracing::error!(error = %e, "上游任务 panic"),
            }
        }
    }

    tasks.shutdown().await;
    Ok(())
}
