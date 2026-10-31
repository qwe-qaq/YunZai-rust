//! Chrome 生命周期与渲染流程。
//!
//! 一个常驻 Chrome + 每个请求一个 page（target），通过 CDP 并发控制。
//! 相比「每次截图新起浏览器」，省掉了 ~300ms 的冷启动和一份进程内存。

use crate::cdp::Cdp;
use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use tokio::sync::Semaphore;

static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct ChromeCfg {
    pub binary: String,
    pub user_data_dir: String,
    pub extra_args: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub url: String,
    pub width: u32,
    pub height: u32,
    pub scale: f64,
    pub selector: Option<String>,
    pub img_type: String,
    pub quality: u8,
    pub full_page: bool,
    pub timeout: Duration,
}

pub struct Browser {
    cdp: Arc<Cdp>,
    child: tokio::sync::Mutex<Child>,
    dir: PathBuf,
    sem: Arc<Semaphore>,
    pub renders: AtomicU64,
}

impl Browser {
    pub async fn launch(cfg: &ChromeCfg, concurrency: usize) -> Result<Self> {
        let dir = PathBuf::from(&cfg.user_data_dir);
        // 清掉旧的 profile：否则残留的 DevToolsActivePort 会指向一个死端口
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).context("创建 Chrome profile 目录失败")?;

        let mut cmd = Command::new(&cfg.binary);
        cmd.arg("--headless=new")
            .arg("--remote-debugging-port=0")
            .arg(format!("--user-data-dir={}", dir.display()))
            .arg("--no-sandbox")
            .arg("--disable-dev-shm-usage")
            .arg("--disable-gpu")
            .arg("--hide-scrollbars")
            .arg("--mute-audio")
            .arg("--no-first-run")
            .arg("--no-default-browser-check")
            .arg("--disable-extensions")
            .arg("--disable-background-timer-throttling")
            .arg("--disable-renderer-backgrounding")
            .arg("--font-render-hinting=none")
            .arg("about:blank")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);

        for a in &cfg.extra_args {
            cmd.arg(a);
        }

        let child = cmd.spawn().with_context(|| {
            format!(
                "启动 Chrome 失败: {}（可用 chrome_path 指定路径）",
                cfg.binary
            )
        })?;

        let port_file = dir.join("DevToolsActivePort");
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if Instant::now() > deadline {
                bail!("等待 Chrome DevTools 端口超时");
            }
            if let Ok(content) = std::fs::read_to_string(&port_file) {
                let mut lines = content.lines();
                let port = lines.next().and_then(|l| l.trim().parse::<u16>().ok());
                let path = lines.next().map(|l| l.trim().to_string());
                if let (Some(port), Some(path)) = (port, path) {
                    if !path.is_empty() {
                        let url = format!("ws://127.0.0.1:{port}{path}");
                        match Cdp::connect(&url).await {
                            Ok(cdp) => {
                                tracing::info!(%url, "Chrome 已就绪");
                                return Ok(Self {
                                    cdp,
                                    child: tokio::sync::Mutex::new(child),
                                    dir,
                                    sem: Arc::new(Semaphore::new(concurrency.max(1))),
                                    renders: AtomicU64::new(0),
                                });
                            }
                            Err(e) => tracing::warn!(error = %e, "CDP 连接失败，继续等待"),
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
    }

    pub async fn alive(&self) -> bool {
        matches!(self.child.lock().await.try_wait(), Ok(None))
    }

    pub async fn render(&self, job: Job) -> Result<Vec<u8>> {
        let _permit = self
            .sem
            .acquire()
            .await
            .map_err(|_| anyhow!("渲染并发信号量已关闭"))?;

        let target = self
            .cdp
            .call("Target.createTarget", json!({"url": "about:blank"}), None)
            .await?;
        let target_id = target
            .get("targetId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("createTarget 未返回 targetId"))?
            .to_string();

        let result = self.render_on(&target_id, &job).await;
        let _ = self
            .cdp
            .call("Target.closeTarget", json!({"targetId": target_id}), None)
            .await;

        if result.is_ok() {
            self.renders.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn render_on(&self, target_id: &str, job: &Job) -> Result<Vec<u8>> {
        let attached = self
            .cdp
            .call(
                "Target.attachToTarget",
                json!({"targetId": target_id, "flatten": true}),
                None,
            )
            .await?;
        let sid = attached
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("attachToTarget 未返回 sessionId"))?
            .to_string();

        // 先订阅再导航，避免页面加载太快错过 loadEventFired
        let mut events = self.cdp.events.subscribe();

        self.cdp.call("Page.enable", json!({}), Some(&sid)).await?;
        self.cdp
            .call(
                "Emulation.setDeviceMetricsOverride",
                json!({
                    "width": job.width,
                    "height": job.height,
                    "deviceScaleFactor": job.scale,
                    "mobile": false
                }),
                Some(&sid),
            )
            .await?;

        let nav = self
            .cdp
            .call_timeout(
                "Page.navigate",
                json!({"url": job.url}),
                Some(&sid),
                job.timeout,
            )
            .await?;
        if let Some(err) = nav.get("errorText").and_then(|v| v.as_str()) {
            bail!("页面加载失败: {err}");
        }

        self.wait_event(&mut events, "Page.loadEventFired", &sid, job.timeout)
            .await?;

        if let Some(sel) = &job.selector {
            self.wait_selector(&sid, sel, job.timeout).await;
        }

        // 整页截图：先把视口拉到内容高度，再截整屏
        let mut shot_height = job.height;
        if job.full_page {
            if let Some(h) = self.page_height(&sid).await {
                if h > job.height {
                    shot_height = h.min(32000);
                    let _ = self
                        .cdp
                        .call(
                            "Emulation.setDeviceMetricsOverride",
                            json!({
                                "width": job.width,
                                "height": shot_height,
                                "deviceScaleFactor": job.scale,
                                "mobile": false
                            }),
                            Some(&sid),
                        )
                        .await;
                    tokio::time::sleep(Duration::from_millis(60)).await;
                }
            }
        }

        let mut params = json!({
            "format": job.img_type,
            "captureBeyondViewport": true,
            "fromSurface": true,
        });
        if job.img_type == "jpeg" || job.img_type == "webp" {
            params["quality"] = json!(job.quality.clamp(1, 100));
        }

        let shot = self
            .cdp
            .call_timeout("Page.captureScreenshot", params, Some(&sid), job.timeout)
            .await?;
        let b64 = shot
            .get("data")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("截图未返回数据"))?;
        let bytes = STANDARD.decode(b64).context("截图 base64 解码失败")?;

        tracing::debug!(
            width = job.width,
            height = shot_height,
            scale = job.scale,
            size = bytes.len(),
            "渲染完成"
        );
        Ok(bytes)
    }

    async fn wait_event(
        &self,
        events: &mut tokio::sync::broadcast::Receiver<Value>,
        method: &str,
        sid: &str,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                bail!("等待事件超时: {method}");
            }
            match tokio::time::timeout(left, events.recv()).await {
                Ok(Ok(v)) => {
                    let same_session = v.get("sessionId").and_then(|x| x.as_str()) == Some(sid);
                    if same_session && v.get("method").and_then(|x| x.as_str()) == Some(method) {
                        return Ok(());
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(_)) => bail!("事件通道已关闭"),
                Err(_) => bail!("等待事件超时: {method}"),
            }
        }
    }

    async fn wait_selector(&self, sid: &str, selector: &str, timeout: Duration) {
        let expr = format!(
            "!!document.querySelector({})",
            serde_json::to_string(selector).unwrap_or_else(|_| "\"body\"".into())
        );
        let deadline = Instant::now() + timeout;
        loop {
            if Instant::now() > deadline {
                tracing::warn!(selector, "等待元素超时，按当前状态截图");
                return;
            }
            let r = self
                .cdp
                .call_timeout(
                    "Runtime.evaluate",
                    json!({"expression": expr, "returnByValue": true}),
                    Some(sid),
                    Duration::from_secs(5),
                )
                .await;
            if let Ok(v) = r {
                if v.get("result")
                    .and_then(|x| x.get("value"))
                    .and_then(|x| x.as_bool())
                    == Some(true)
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn page_height(&self, sid: &str) -> Option<u32> {
        let r = self
            .cdp
            .call(
                "Runtime.evaluate",
                json!({
                    "expression": "Math.max(document.body ? document.body.scrollHeight : 0, document.documentElement.scrollHeight)",
                    "returnByValue": true
                }),
                Some(sid),
            )
            .await
            .ok()?;
        let h = r.get("result")?.get("value")?.as_f64()?;
        Some(h.ceil() as u32)
    }

    /// 请求体里直接给 HTML 时，落一个临时文件再导航——这样相对资源仍能加载。
    pub fn materialize_html(html: &str) -> Result<(String, PathBuf)> {
        let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("yz-render-{}-{}.html", std::process::id(), seq));
        std::fs::write(&path, html).context("写入临时 HTML 失败")?;
        let url = format!("file://{}", path.display());
        Ok((url, path))
    }

    pub fn profile_dir(&self) -> &PathBuf {
        &self.dir
    }
}
