[![CI](https://github.com/qwe-qaq/YunZai-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/qwe-qaq/YunZai-rust/actions/workflows/ci.yml)
[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)

# Yunzai × Rust：接入网关 + 渲染服务

这是把 TRSS-Yunzai「按路线 A 重写」的落地实现：**只替换基础设施层，插件层保持原样**。

```
        ┌──────────────┐        ws         ┌─────────────────┐        ws         ┌──────────────┐
        │ 协议端        │ ───────────────>  │  yz-gateway      │ ───────────────>  │  Yunzai       │
        │ NapCat /      │                   │  · 连接管理      │                   │  · 插件（不动）│
        │ Lagrange      │ <───────────────  │  · 多账号配对    │ <───────────────  │  · 事件分发    │
        └──────────────┘                   │  · 双向透传/限流 │                   └──────┬───────┘
                                            │  · 指标          │                          │ HTTP
                                            └─────────────────┘                   ┌──────▼───────┐
                                                                                   │  yz-render    │
                                                                                   │ 常驻 Chrome   │
                                                                                   │ CDP 并发截图   │
                                                                                   └──────────────┘
```

**Yunzai 侧零改动**：网关对 Yunzai 伪装成协议端，连它的 `/OneBotv11`。插件、适配器、配置都不用动。
唯一可选改动是渲染后端（新增 `renderers/yz-render/` + `renderer.yaml` 改一行），用来把截图换成 Rust。

---

## 一、为什么这样切

前一轮评估的结论是：核心 1.2 万行 JS 重写不难，难的是 20 万行起步的插件生态。所以这里不碰插件层，只接管三件「与业务无关、但决定稳定性」的事：

| 组件 | 接管什么 | 收益 |
|---|---|---|
| `yz-gateway` | 协议端连接、心跳保活、断线重连、多账号路由、限流、可观测性 | 协议端抖动不再影响 Yunzai；一个账号掉线不牵连其他账号；网关重启不丢插件状态 |
| `yz-render` | HTML → 图片（Chrome 复用 + CDP 并发） | 单张 148ms（新起 Chrome 要 368ms）；内存只有一份 Chrome |

**没有做的事**（以及为什么）：没有实现 JS 运行时、没有重写 art-template、没有改任何插件 API。这些是生态的地基，动了就等于换一个项目。

---

## 二、快速开始

### 2.1 编译

```bash
cargo build --release
# 产物：target/release/yz-gateway  target/release/yz-render
```

要求：Rust **stable**（CI 与本地均在 1.93.0 验证过；更早版本未测，不写死 MSRV）；`yz-render` 需要本机有 Chrome/Chromium（`--headless=new` 要求 Chrome ≥ 112）。

### 2.2 启动顺序

```bash
# 1) 协议端（NapCat / Lagrange）开正向 WS，例如 ws://127.0.0.1:3001
# 2) Yunzai 正常启动，监听 2536
node app.js

# 3) 渲染服务（可选，但推荐）
./target/release/yz-render config/render.toml

# 4) 网关
./target/release/yz-gateway config/gateway.toml
```

嫌路径长就先 `cp target/release/yz-* .`，之后直接 `./yz-gateway config/gateway.toml`。
或者用 `./deploy/start.sh`（自动找 `target/release`，两个进程一起起，Ctrl-C 一起退）。

启动后：

```bash
curl http://127.0.0.1:6701/status    # 连接状态
curl http://127.0.0.1:6701/metrics   # Prometheus
curl http://127.0.0.1:6702/healthz   # 渲染服务健康
```

`/status` 示例：

```json
{
  "uptime_secs": 42,
  "upstreams": [
    {"name": "napcat-main", "self_id": 10001, "mode": "forward",
     "phase": "connected", "reconnects": 0, "last_error": null}
  ],
  "stats": {"events": 128, "actions": 41, "responses": 41, "bytes_up": 91234, "bytes_down": 5342, "errors": 0}
}
```

### 2.3 接入 Yunzai（零改动）

不需要改 Yunzai 的任何文件。原来协议端直连 `ws://yunzai:2536/OneBotv11`，现在改成：

```
协议端  ->  yz-gateway  ->  ws://yunzai:2536/OneBotv11
```

如果 Yunzai 配了 `server.auth`（例如 `Authorization: Bearer xxx`），把同一个值填到网关的 `downstream.token`。

### 2.4 接入渲染服务（可选）

```bash
cp -r node-patch/renderers/yz-render  <yunzai>/renderers/
# 编辑 <yunzai>/config/config/renderer.yaml
name: yz-render
```

插件完全无感：它们仍然调用 `e.runtime.render(...)` / `puppeteer.screenshot(...)`，
只是「HTML → 图片」这一步从本机 Puppeteer 变成了 `POST http://127.0.0.1:6702/render`。

---

## 三、配置

### 3.1 `config/gateway.toml`

```toml
[server]
status_listen = "127.0.0.1:6701"
reverse_listen = "0.0.0.0:6700"   # mode = "reverse" 时协议端连这里
reverse_token = ""

[downstream]
url = "ws://127.0.0.1:2536/OneBotv11"
token = ""                        # 与 Yunzai server.auth 一致，如 "Bearer xxx"

[limits]
queue = 2048                      # 单向队列；满了反压上游，不丢消息
action_rate_per_sec = 30.0        # Yunzai -> 协议端
event_rate_per_sec = 200.0        # 协议端 -> Yunzai
keepalive_secs = 25
prime_timeout_ms = 2000           # 等协议端首帧的时间，超时则合成 lifecycle

[[upstream]]
name = "napcat-main"
mode = "forward"                  # forward = 网关连协议端；reverse = 协议端连网关
self_id = 10001
url = "ws://127.0.0.1:3001"
```

**一个 `[[upstream]]` = 一个 QQ 号 = 一条到 Yunzai 的下游连接。**
这是保留 Yunzai 多账号语义的关键：Yunzai 是 WS 服务端，每条连接在它内部就是一个 `Bot[self_id]`，所以网关按账号分别建连，`Bot.uin`、`pickFriend` 跨账号路由都照常工作。

反向接入（NapCat 的「反向 WS」）：

```toml
[[upstream]]
name = "lagrange"
mode = "reverse"
self_id = 10002
```

协议端连 `ws://gateway:6700`，网关读首帧里的 `self_id` 自动配对。

### 3.2 `config/render.toml`

```toml
[server]
listen = "127.0.0.1:6702"
concurrency = 6        # 同时渲染的 page 数，每 page 约 30–80MB

[chrome]
binary = "chromium"    # 也可以是 google-chrome / 绝对路径
user_data_dir = "/tmp/yz-render-profile"
extra_args = []        # 例如 ["--proxy-server=http://127.0.0.1:7890"]
```

### 3.3 渲染 HTTP 接口

```http
POST /render
{"url":"file:///path/to/page.html","width":800,"height":600,"scale":2,
 "selector":"#container","img_type":"jpeg","quality":90,"full_page":true,"timeout_ms":20000}
```

也支持直接传 `{"html":"<html>...</html>"}`（落临时文件后导航，相对资源仍可加载）。
返回 `image/png|jpeg|webp` 二进制。

---

## 四、验证

三个脚本，都不需要真实的 QQ 号。

```bash
# 1) 单账号：lifecycle、事件、action 往返、掉线重连、指标
python3 scripts/e2e.py ./target/release/yz-gateway

# 2) 多账号：两条下游连接、事件不串号、单账号掉线隔离
python3 scripts/e2e_multi.py ./target/release/yz-gateway

# 3) 真实 Yunzai：零改动上线 + 插件真实执行
python3 scripts/e2e_real_yunzai.py ./target/release/yz-gateway /path/to/yunzai

# 4) 完整链路：协议端 -> 网关 -> Yunzai 插件 -> Rust 渲染 -> 图片回复
python3 scripts/e2e_render_chain.py ./target/release/yz-gateway ./target/release/yz-render /path/to/yunzai
```

本机实测结果：

| 测试 | 结果 |
|---|---|
| `e2e.py`（10 项） | 连续 10 轮 **100/100 通过** |
| `e2e_multi.py`（6 项） | **6/6 通过** |
| `e2e_real_yunzai.py`（4 项） | **4/4 通过**，注入 `#状态` 后 Yunzai 插件真实回复 |
| `e2e_render_chain.py`（4 项） | **4/4 通过**，出图 784×330 / 106ms |

渲染性能（800×600 @2x，JPEG q85，容器 32 核）：

| 方式 | 单张耗时 | 吞吐 |
|---|---|---|
| `yz-render`（Chrome 复用） | 平均 148ms，P95 151ms | 11.2 张/秒（并发 16） |
| 每次新起 `chromium --headless --screenshot` | 368ms | — |

---

### CI 覆盖范围

GitHub Actions（`.github/workflows/ci.yml`）分三个 job，每次 push / PR 自动跑：

| job | 内容 | 说明 |
|---|---|---|
| 格式与静态检查 | `cargo fmt --check`、`cargo clippy -D warnings` | 零告警门禁 |
| 单元测试 | `cargo test --workspace --all-targets` | 8 个用例 |
| 端到端与渲染 | release 构建 → 网关单/多账号 e2e → 装 Chrome → 渲染冒烟 | 23 项断言，产物上传 artifact |

本地复现整条流水线：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo build --release --workspace
python3 scripts/e2e.py target/release/yz-gateway        # 10 项
python3 scripts/e2e_multi.py target/release/yz-gateway  # 6 项
python3 scripts/smoke_render.py target/release/yz-render chromium  # 7 项
```

CI 不覆盖的两项（依赖外部环境，需手工跑）：`e2e_real_yunzai.py`（真实 Yunzai + Node + Redis）、
`e2e_render_chain.py`（真实 Yunzai 出图全链路）。

## 五、运维

### systemd

```ini
# /etc/systemd/system/yz-gateway.service
[Unit]
Description=Yunzai OneBot Gateway
After=network-online.target

[Service]
WorkingDirectory=/opt/yunzai-rust
ExecStart=/opt/yunzai-rust/yz-gateway /opt/yunzai-rust/config/gateway.toml
Restart=always
RestartSec=2
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

部署时先把产物和配置放到 `WorkingDirectory`：

```bash
sudo mkdir -p /opt/yunzai-rust/config
sudo cp target/release/yz-gateway target/release/yz-render /opt/yunzai-rust/
sudo cp config/*.toml /opt/yunzai-rust/config/
sudo cp deploy/yz-gateway.service deploy/yz-render.service /etc/systemd/system/
sudo systemctl enable --now yz-gateway yz-render
```

`yz-render` 同理，建议 `Restart=always`：Chrome 崩溃时由 systemd 拉起（网关会自动重连）。

### 观测

| 指标 | 含义 | 告警建议 |
|---|---|---|
| `yunzai_gateway_upstream_connected` | 账号是否在线 | 持续 0 超过 1 分钟 |
| `yunzai_gateway_upstream_reconnects` | 重连次数 | 5 分钟内增长 > 5 |
| `yunzai_gateway_errors_total` | 协议/网络错误 | 增长率突增 |
| `yz_render_chrome_up` | Chrome 存活 | 为 0 |
| `yz_render_renders_total` | 渲染张数 | 用于容量规划 |

### 日志

`RUST_LOG=info` 只输出连接、重连、错误；`RUST_LOG=debug` 会打印每帧的收发（排查协议字段问题时用）。

---

## 六、边界与已知限制

1. **不解决「插件运行时」**。Rust 依旧无法加载 JS 插件；这是路线 A 的自觉取舍，插件仍由 Node 执行。
2. **`reverse` 模式按首帧 `self_id` 配对**。若协议端首帧不是标准 lifecycle/事件（不含 `self_id`），需要在 URL 上带 `?self_id=`，或改用 `forward`。
3. **图片字节仍在 Node 侧中转**。渲染结果是 Buffer 回到 Node 再发出去；大图（>2MB）会多一次内存拷贝。若要彻底避免，可让渲染服务直接把图上传到图床/对象存储，再把 URL 交给 Yunzai（需要改 `segment.image` 的入参）。
4. **art-template 未替换**。模板仍在 Node 渲染，所以 `art-template` 的语法、`_res_path` 注入等行为完全保留——这也是插件零改动的前提。
5. **单进程单 Chrome**。`concurrency` 是唯一的并发旋钮；要横向扩展就多开几个 `yz-render`，Node 侧按权重分流（当前实现只配一个 endpoint）。
6. **许可证**：本工程为独立实现的网关/渲染服务，未复制 Yunzai 源码；Yunzai 本体为 GPLv3，若分发衍生整体请遵循其许可。

---

## 七、目录结构

```
yunzai-rust/
├── Cargo.toml                 workspace
├── config/
│   ├── gateway.toml           单账号
│   ├── gateway.multi.toml     多账号示例
│   └── render.toml
├── gateway/src/
│   ├── main.rs                启动、信号、任务编排
│   ├── config.rs              配置解析与校验（含单测）
│   ├── ws.rs                  连接封装、鉴权头、self_id 解析
│   ├── session.rs             双向管道 + 令牌桶 + 心跳（含单测）
│   ├── upstream.rs            forward/reverse 接入、退避重连、lifecycle 补发
│   └── status.rs              /status /metrics、反向接入配对表
├── render/src/
│   ├── main.rs                HTTP 服务、参数校验
│   ├── cdp.rs                 CDP 客户端（id 配对 + 事件广播）
│   └── browser.rs             Chrome 生命周期、page 生命周期、截图
├── node-patch/
│   ├── renderers/yz-render/   Yunzai 渲染后端（新增，不修改原文件）
│   └── examples/yz-test-render/  验证链路用的示例插件（非生产）
├── .github/workflows/ci.yml   CI 流水线
├── deploy/                    systemd 单元 + 本地启动脚本
└── scripts/                   e2e 测试
```

单测：

```bash
cargo test
# 8 passed（退避、令牌桶、配置校验、self_id 解析、lifecycle 形状）
```

代码量：Rust 约 2000 行（含测试），Node 侧新增约 120 行（渲染后端）。
release 产物：`yz-gateway` 2.4 MB、`yz-render` 2.3 MB（strip + LTO），网关空载 RSS 约 4.5 MB。

## 许可证

本项目是 [TRSS-Yunzai](https://github.com/TimeRainStarSky/Yunzai) 的衍生作品（复刻其协议层语义、并提供其渲染器后端），
按上游要求以 **GPLv3** 发布，见 [LICENSE](LICENSE)。

`node-patch/renderers/yz-render` 直接 import Yunzai 的 `Renderer` 基类，属同一衍生范围。
若你只使用 `yz-gateway` 的协议转发能力而不引入 node-patch，仍建议保留同一许可证，避免争议。

