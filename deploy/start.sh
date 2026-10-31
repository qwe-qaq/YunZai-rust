#!/usr/bin/env bash
# 本地一键起（前台调试用）。生产建议用 systemd 单元（见 deploy/*.service）。
set -euo pipefail
cd "$(dirname "$0")/.."

# 默认用 cargo 的 release 产物；也可 BIN=/opt/yunzai-rust ./deploy/start.sh
BIN="${BIN:-./target/release}"
for exe in yz-gateway yz-render; do
  if [ ! -x "$BIN/$exe" ]; then
    echo "找不到 $BIN/$exe —— 先跑：cargo build --release（或用 BIN=... 指定目录）" >&2
    exit 1
  fi
done

mkdir -p logs
"$BIN/yz-render" config/render.toml > logs/render.log 2>&1 &
RENDER_PID=$!
"$BIN/yz-gateway" config/gateway.toml > logs/gateway.log 2>&1 &
GW_PID=$!

cleanup() {
  # 先撤销陷阱，避免 kill 触发的信号再次进入 cleanup
  trap - EXIT INT TERM HUP
  kill "$RENDER_PID" "$GW_PID" 2>/dev/null || true
  # 给 3 秒优雅退出，超时强杀（否则日志里会留下一堆孤儿进程）
  for _ in $(seq 1 15); do
    if ! kill -0 "$RENDER_PID" 2>/dev/null && ! kill -0 "$GW_PID" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  kill -9 "$RENDER_PID" "$GW_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM HUP

echo "gateway pid=$GW_PID  render pid=$RENDER_PID"
echo "状态: http://127.0.0.1:6701/status   渲染: http://127.0.0.1:6702/healthz"
echo "日志: logs/gateway.log  logs/render.log（Ctrl-C 一起退出）"
wait
