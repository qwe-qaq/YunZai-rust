#!/usr/bin/env bash
# 本地一键起（前台调试用）。生产建议用 systemd 单元。
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p logs
./yz-render config/render.toml > logs/render.log 2>&1 &
RENDER_PID=$!
./yz-gateway config/gateway.toml > logs/gateway.log 2>&1 &
GW_PID=$!

trap 'kill $RENDER_PID $GW_PID 2>/dev/null || true' EXIT INT TERM
echo "gateway pid=$GW_PID  render pid=$RENDER_PID"
echo "状态: http://127.0.0.1:6701/status"
wait
