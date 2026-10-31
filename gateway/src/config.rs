//! 配置加载与校验。
//!
//! 设计原则：网关只做「透传 + 管控」，因此配置里不出现任何业务字段，
//! 只描述「从哪连、连到哪、限多少」。

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerCfg,
    pub downstream: DownstreamCfg,
    #[serde(default)]
    pub limits: LimitsCfg,
    #[serde(default)]
    pub upstream: Vec<UpstreamCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerCfg {
    /// 状态/指标 HTTP 服务
    #[serde(default = "default_status_listen")]
    pub status_listen: String,
    /// 协议端「反向 WS」接入地址（mode = "reverse" 时使用）
    #[serde(default = "default_reverse_listen")]
    pub reverse_listen: String,
    /// 反向 WS 的鉴权令牌，空表示不校验（仅建议在回环地址上这样做）
    #[serde(default)]
    pub reverse_token: String,
}

impl Default for ServerCfg {
    fn default() -> Self {
        Self {
            status_listen: default_status_listen(),
            reverse_listen: default_reverse_listen(),
            reverse_token: String::new(),
        }
    }
}

fn default_status_listen() -> String {
    "127.0.0.1:6701".into()
}
fn default_reverse_listen() -> String {
    "0.0.0.0:6700".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownstreamCfg {
    /// Yunzai 的 OneBotv11 接入地址，例如 ws://127.0.0.1:2536/OneBotv11
    pub url: String,
    /// 与 Yunzai config/config/server.yaml 中的 auth 保持一致，形如 "Bearer xxx"
    #[serde(default)]
    pub token: String,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamCfg {
    /// 仅用于日志与指标
    pub name: String,
    #[serde(default)]
    pub mode: Mode,
    /// 机器人 QQ 号，必填：网关靠它把「协议端连接」与「Yunzai 连接」一一配对
    pub self_id: i64,
    /// mode = forward 时必填，协议端的正向 WS 地址
    #[serde(default)]
    pub url: String,
    /// mode = reverse 时可选，用于日志
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub token: String,
    #[serde(default = "default_reconnect_min")]
    pub reconnect_min_ms: u64,
    #[serde(default = "default_reconnect_max")]
    pub reconnect_max_ms: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// 网关作为客户端连接协议端（NapCat/Lagrange 的「正向 WS」）
    #[default]
    Forward,
    /// 网关作为服务端接受协议端连接（「反向 WS」）
    Reverse,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Forward => "forward",
            Mode::Reverse => "reverse",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsCfg {
    /// 单向转发队列长度；队列满时反压上游，而不是丢消息
    #[serde(default = "default_queue")]
    pub queue: usize,
    /// 下游（Yunzai）每秒最多下发多少条 action，0 表示不限
    #[serde(default = "default_action_rate")]
    pub action_rate_per_sec: f64,
    /// 上游每秒最多推给 Yunzai 多少条事件，0 表示不限
    #[serde(default = "default_event_rate")]
    pub event_rate_per_sec: f64,
    /// 心跳间隔（秒），0 表示关闭
    #[serde(default = "default_keepalive")]
    pub keepalive_secs: u64,
    /// 建连后等待协议端首帧的时长（毫秒），超时则合成 lifecycle
    #[serde(default = "default_prime_timeout")]
    pub prime_timeout_ms: u64,
}

impl Default for LimitsCfg {
    fn default() -> Self {
        Self {
            queue: default_queue(),
            action_rate_per_sec: default_action_rate(),
            event_rate_per_sec: default_event_rate(),
            keepalive_secs: default_keepalive(),
            prime_timeout_ms: default_prime_timeout(),
        }
    }
}

fn default_queue() -> usize {
    2048
}
fn default_action_rate() -> f64 {
    30.0
}
fn default_event_rate() -> f64 {
    200.0
}
fn default_keepalive() -> u64 {
    25
}
fn default_prime_timeout() -> u64 {
    2000
}
fn default_connect_timeout() -> u64 {
    8000
}
fn default_reconnect_min() -> u64 {
    500
}
fn default_reconnect_max() -> u64 {
    30_000
}
fn default_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("读取配置文件失败: {path}"))?;
        let cfg: Config = toml::from_str(&raw).context("解析 TOML 配置失败")?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        let active: Vec<&UpstreamCfg> = self.upstream.iter().filter(|u| u.enabled).collect();
        if active.is_empty() {
            bail!("至少需要一个 enabled 的 [[upstream]]");
        }

        let mut seen = std::collections::HashSet::new();
        for u in &active {
            if !seen.insert(u.self_id) {
                bail!("self_id 重复: {}（一个账号只能对应一个上游）", u.self_id);
            }
            if u.self_id <= 0 {
                bail!("upstream「{}」的 self_id 非法: {}", u.name, u.self_id);
            }
            match u.mode {
                Mode::Forward => {
                    if u.url.is_empty() {
                        bail!("upstream「{}」为 forward 模式，必须提供 url", u.name);
                    }
                    if !u.url.starts_with("ws://") && !u.url.starts_with("wss://") {
                        bail!("upstream「{}」的 url 必须是 ws:// 或 wss://", u.name);
                    }
                }
                Mode::Reverse => {
                    if u.path.is_empty() {
                        tracing::warn!(upstream = %u.name, "reverse 模式未配置 path，仅用于日志");
                    }
                }
            }
            if u.reconnect_max_ms < u.reconnect_min_ms {
                bail!(
                    "upstream「{}」的 reconnect_max_ms 小于 reconnect_min_ms",
                    u.name
                );
            }
        }

        if !self.downstream.url.starts_with("ws://") && !self.downstream.url.starts_with("wss://") {
            bail!("downstream.url 必须是 ws:// 或 wss://");
        }
        if self.limits.queue < 16 {
            bail!("limits.queue 至少为 16");
        }
        Ok(())
    }

    pub fn active_upstreams(&self) -> impl Iterator<Item = &UpstreamCfg> {
        self.upstream.iter().filter(|u| u.enabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Config> {
        let cfg: Config = toml::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn minimal_config_ok() {
        let cfg = parse(
            r#"
[downstream]
url = "ws://127.0.0.1:2536/OneBotv11"

[[upstream]]
name = "napcat"
self_id = 10001
url = "ws://127.0.0.1:3001"
"#,
        )
        .unwrap();
        assert_eq!(cfg.upstream.len(), 1);
        assert_eq!(cfg.upstream[0].mode, Mode::Forward);
        assert_eq!(cfg.limits.queue, 2048);
    }

    #[test]
    fn duplicate_self_id_rejected() {
        let err = parse(
            r#"
[downstream]
url = "ws://127.0.0.1:2536/OneBotv11"

[[upstream]]
name = "a"
self_id = 1
url = "ws://127.0.0.1:3001"

[[upstream]]
name = "b"
self_id = 1
url = "ws://127.0.0.1:3002"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("self_id 重复"));
    }
}
