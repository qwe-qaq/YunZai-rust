#!/usr/bin/env python3
"""
完整链路验证：协议端 -> Rust 网关 -> Yunzai 插件 -> Rust 渲染服务 -> 图片回复

需要先装好测试插件与渲染后端（见 README「5 分钟接入」）：
  renderers/yz-render/         node-patch/renderers/yz-render
  plugins/yz-test-render/      测试用插件（真实业务插件无需改动）
  config/config/renderer.yaml  name: yz-render
"""
import asyncio
import base64
import json
import os
import signal
import subprocess
import sys
import time

GATEWAY = sys.argv[1] if len(sys.argv) > 1 else "./target/release/yz-gateway"
RENDER = sys.argv[2] if len(sys.argv) > 2 else "./target/release/yz-render"
YUNZAI_DIR = sys.argv[3] if len(sys.argv) > 3 else "/root/.codebuddy/artifact/yunzai"
LOG = "/tmp/render_e2e_yunzai.log"
SELF_ID = 10001

state = {"connected": 0, "replies": [], "images": []}
results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name} {detail}")


async def napcat_handler(ws):
    state["connected"] += 1
    state["ws"] = ws
    await ws.send(json.dumps({
        "post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "connect",
        "self_id": SELF_ID, "time": int(time.time()),
    }))
    try:
        async for raw in ws:
            msg = json.loads(raw)
            action, params, echo = msg.get("action"), msg.get("params", {}), msg.get("echo")
            data = {}
            if action == "get_login_info":
                data = {"user_id": SELF_ID, "nickname": "联调机器人"}
            elif action == "get_friend_list":
                data = []
            elif action == "get_group_list":
                data = [{"group_id": 456789, "group_name": "测试群", "member_count": 3}]
            elif action == "get_group_member_list":
                data = [{"user_id": SELF_ID, "nickname": "联调机器人", "card": "", "role": "owner"}]
            elif action in ("send_group_msg", "send_msg", "send_private_msg"):
                data = {"message_id": 90001}
                segs = params.get("message") or []
                if isinstance(segs, str):
                    segs = [{"type": "text", "data": {"text": segs}}]
                state["replies"].append(segs)
                imgs = [s for s in segs if isinstance(s, dict) and s.get("type") == "image"]
                if imgs:
                    state["images"].append(imgs[0])
                    d = imgs[0].get("data", {})
                    size = len(d.get("file") or "")
                    print(f"[napcat] <= 收到图片回复，字段={list(d)} 长度={size}")
                    f = d.get("file", "")
                    if f.startswith("base64://"):
                        raw_bytes = base64.b64decode(f[9:])
                        state["image_bytes"] = raw_bytes
                        print(f"[napcat] 图片解码 {len(raw_bytes)} 字节")
                    elif f.startswith("file://") or os.path.exists(f):
                        p = f[7:] if f.startswith("file://") else f
                        if os.path.exists(p):
                            state["image_bytes"] = open(p, "rb").read()
                            print(f"[napcat] 图片读取 {len(state['image_bytes'])} 字节")
                else:
                    print(f"[napcat] <= 文本回复: {json.dumps(segs, ensure_ascii=False)[:120]}")
            await ws.send(json.dumps({"status": "ok", "retcode": 0, "data": data, "echo": echo}))
    except Exception as e:
        print(f"[napcat] 连接结束: {type(e).__name__}")


def tail(n=25):
    try:
        with open(LOG, encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-n:])
    except FileNotFoundError:
        return ""


async def wait_for(pred, timeout=60.0, interval=0.2):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        await asyncio.sleep(interval)
    return False


async def main():
    import websockets

    redis = subprocess.Popen(["redis-server", "--port", "6379", "--save", "", "--appendonly", "no"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    render = subprocess.Popen([RENDER, "config/render.toml"], stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, start_new_session=True)
    server = await websockets.serve(napcat_handler, "127.0.0.1", 3001)
    gw = subprocess.Popen([GATEWAY, "config/gateway.toml"], stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, start_new_session=True)
    open(LOG, "w").close()
    logf = open(LOG, "w")
    node = subprocess.Popen(["node", "app.js"], cwd=YUNZAI_DIR, stdout=logf,
                            stderr=subprocess.STDOUT, start_new_session=True)
    try:
        ok = await wait_for(lambda: state["connected"] > 0, timeout=60)
        check("Yunzai 上线（经网关）", ok, "")
        await wait_for(lambda: "加载渲染后端 yz-render" in tail(400), timeout=30)
        check("Yunzai 加载 yz-render 后端", "加载渲染后端 yz-render" in tail(400), "")

        await wait_for(lambda: "加载插件" in tail(300), timeout=40)
        await asyncio.sleep(1.0)

        print("[test] 注入 #出图")
        await state["ws"].send(json.dumps({
            "post_type": "message", "message_type": "group", "sub_type": "normal",
            "self_id": SELF_ID, "user_id": 123456, "group_id": 456789, "message_id": 70003,
            "raw_message": "#出图",
            "message": [{"type": "text", "data": {"text": "#出图"}}],
            "sender": {"user_id": 123456, "nickname": "好友A", "card": ""},
            "time": int(time.time()),
        }))
        ok = await wait_for(lambda: len(state["images"]) > 0, timeout=40)
        check("插件调用 Renderer.screenshot -> Rust 渲染 -> 图片回复", ok,
              f"images={len(state['images'])}")
        data = state.get("image_bytes", b"")
        check("图片有内容", len(data) > 5000, f"{len(data)} bytes")
        if data:
            open("/tmp/yztest/yunzai_render.jpg", "wb").write(data)
    finally:
        for proc in (node, gw, render, redis):
            if proc.poll() is None:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    proc.kill()
        logf.close()
        server.close()

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== {passed}/{len(results)} 通过 =====")
    print("\n--- Yunzai 日志 ---")
    print("\n".join(tail(20).splitlines()[-20:]))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
