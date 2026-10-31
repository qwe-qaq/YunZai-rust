//! yz-render —— 把「HTML 文件」渲染成图片的 HTTP 服务。
//!
//! Yunzai 的插件仍然用 art-template 生成 HTML（插件无感知），
//! 只是把最后「Puppeteer 截图」这一步换成 POST /render。
//!
//! ```text
//!   Node: Renderer.dealTpl() -> temp/html/x/1.html
//!         POST /render {"url":"file:///.../1.html","width":800,"scale":2}
//!   Rust: 常驻 Chrome + CDP 并发截图 -> image/png
//! ```

mod browser;
mod cdp;

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use browser::{Browser, ChromeCfg, Job};
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
struct FileCfg {
    #[serde(default)]
    server: ServerCfg,
    chrome: ChromeSection,
}

#[derive(Debug, Deserialize)]
struct ServerCfg {
    #[serde(default = "default_listen")]
    listen: String,
    #[serde(default = "default_conc")]
    concurrency: usize,
}

impl Default for ServerCfg {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            concurrency: default_conc(),
        }
    }
}

fn default_listen() -> String {
    "127.0.0.1:6702".into()
}
fn default_conc() -> usize {
    6
}

#[derive(Debug, Deserialize)]
struct ChromeSection {
    #[serde(default = "default_bin")]
    binary: String,
    #[serde(default = "default_profile")]
    user_data_dir: String,
    #[serde(default)]
    extra_args: Vec<String>,
}

fn default_bin() -> String {
    "chromium".into()
}
fn default_profile() -> String {
    "/tmp/yz-render-profile".into()
}

struct App {
    browser: Arc<Browser>,
    started: Instant,
}

#[derive(Debug, Deserialize)]
struct RenderReq {
    /// file:// 或 http(s):// 地址，与 url 二选一
    url: Option<String>,
    /// 直接给 HTML 内容
    html: Option<String>,
    #[serde(default)]
    width: Option<u32>,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    scale: Option<f64>,
    #[serde(default)]
    selector: Option<String>,
    #[serde(default)]
    img_type: Option<String>,
    #[serde(default)]
    quality: Option<u8>,
    #[serde(default)]
    full_page: Option<bool>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

fn mime_of(img_type: &str) -> &'static str {
    match img_type {
        "jpeg" | "jpg" => "image/jpeg",
        "webp" => "image/webp",
        _ => "image/png",
    }
}

async fn render(State(app): State<Arc<App>>, Json(req): Json<RenderReq>) -> Response {
    let img_type = match req.img_type.as_deref() {
        Some("jpeg") | Some("jpg") => "jpeg",
        Some("webp") => "webp",
        _ => "png",
    };

    let (url, temp) = match (&req.url, &req.html) {
        (Some(u), _) if !u.is_empty() => (u.clone(), None),
        (_, Some(h)) => match Browser::materialize_html(h) {
            Ok((u, p)) => (u, Some(p)),
            Err(e) => return fail(StatusCode::BAD_REQUEST, format!("写入临时 HTML 失败: {e}")),
        },
        _ => return fail(StatusCode::BAD_REQUEST, "必须提供 url 或 html".into()),
    };

    let job = Job {
        url,
        width: req.width.unwrap_or(800).clamp(16, 10000),
        height: req.height.unwrap_or(600).clamp(16, 32000),
        scale: req.scale.unwrap_or(1.0).clamp(0.1, 4.0),
        selector: req.selector.filter(|s| !s.is_empty()),
        img_type: img_type.to_string(),
        quality: req.quality.unwrap_or(90),
        full_page: req.full_page.unwrap_or(true),
        timeout: Duration::from_millis(req.timeout_ms.unwrap_or(20000).clamp(1000, 120000)),
    };

    let result = app.browser.render(job).await;

    if let Some(p) = temp {
        let _ = std::fs::remove_file(p);
    }

    match result {
        Ok(bytes) => (
            [
                (header::CONTENT_TYPE, mime_of(img_type)),
                (header::CACHE_CONTROL, "no-store"),
            ],
            bytes,
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "渲染失败");
            fail(StatusCode::BAD_GATEWAY, format!("渲染失败: {e}"))
        }
    }
}

fn fail(code: StatusCode, msg: String) -> Response {
    (code, Json(serde_json::json!({"ok": false, "error": msg}))).into_response()
}

async fn healthz(State(app): State<Arc<App>>) -> Response {
    let alive = app.browser.alive().await;
    let body = Json(serde_json::json!({
        "ok": alive,
        "chrome": if alive { "up" } else { "down" },
        "uptime_secs": app.started.elapsed().as_secs(),
        "renders": app.browser.renders.load(Ordering::Relaxed),
    }));
    if alive {
        body.into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, body).into_response()
    }
}

async fn metrics(State(app): State<Arc<App>>) -> Response {
    let renders = app.browser.renders.load(Ordering::Relaxed);
    let alive = if app.browser.alive().await { 1 } else { 0 };
    let body = format!(
        "# TYPE yz_render_renders_total counter\nyz_render_renders_total {renders}\n\
         # TYPE yz_render_chrome_up gauge\nyz_render_chrome_up {alive}\n\
         # TYPE yz_render_uptime_seconds gauge\nyz_render_uptime_seconds {}\n",
        app.started.elapsed().as_secs()
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/render.toml".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let raw = std::fs::read_to_string(&path).with_context(|| format!("读取配置失败: {path}"))?;
    let cfg: FileCfg = toml::from_str(&raw).context("解析 TOML 失败")?;

    let chrome = ChromeCfg {
        binary: cfg.chrome.binary.clone(),
        user_data_dir: cfg.chrome.user_data_dir.clone(),
        extra_args: cfg.chrome.extra_args.clone(),
    };

    let browser = Arc::new(
        Browser::launch(&chrome, cfg.server.concurrency)
            .await
            .context("启动 Chrome 失败")?,
    );
    tracing::info!(
        binary = %chrome.binary,
        profile = %browser.profile_dir().display(),
        concurrency = cfg.server.concurrency,
        "渲染服务已就绪"
    );

    let app = Arc::new(App {
        browser,
        started: Instant::now(),
    });

    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/render", post(render))
        .with_state(app);

    let listener = tokio::net::TcpListener::bind(&cfg.server.listen)
        .await
        .with_context(|| format!("监听 {} 失败", cfg.server.listen))?;
    tracing::info!(listen = %cfg.server.listen, "HTTP 服务已启动");

    axum::serve(listener, router).await?;
    Ok(())
}
