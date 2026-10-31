//! WebSocket 连接的薄封装：统一处理 URL 解析、鉴权头与超时。

use anyhow::{Context, Result};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue},
    MaybeTlsStream, WebSocketStream,
};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(url: &str, token: &str, timeout: Duration) -> Result<WsStream> {
    let mut req = url
        .into_client_request()
        .with_context(|| format!("非法 WebSocket 地址: {url}"))?;

    if !token.is_empty() {
        req.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(token).context("access token 含非法字符")?,
        );
    }

    let (ws, resp) = tokio::time::timeout(timeout, connect_async(req))
        .await
        .with_context(|| format!("连接超时: {url}"))??;

    tracing::debug!(%url, status = ?resp.status(), "WebSocket 已连接");
    Ok(ws)
}

/// 判断一条下游消息是不是 OneBot 事件（而不是 action 的响应）。
#[allow(dead_code)]
pub fn is_event(v: &serde_json::Value) -> bool {
    v.get("post_type").is_some()
}

/// 提取事件里的 self_id，兼容字符串与数字两种写法。
pub fn self_id_of(v: &serde_json::Value) -> Option<i64> {
    match v.get("self_id") {
        Some(serde_json::Value::Number(n)) => n.as_i64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classify_message() {
        assert!(is_event(&json!({"post_type": "message"})));
        assert!(!is_event(&json!({"echo": "1", "retcode": 0})));
    }

    #[test]
    fn parse_self_id_both_types() {
        assert_eq!(self_id_of(&json!({"self_id": 123})), Some(123));
        assert_eq!(self_id_of(&json!({"self_id": "123"})), Some(123));
        assert_eq!(self_id_of(&json!({})), None);
    }
}
