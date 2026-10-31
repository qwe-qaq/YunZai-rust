#!/usr/bin/env python3
"""
真实 Yunzai 联调：验证「零改动接入」。

拓扑：
    mock napcat (3001)  <--ws-->  yz-gateway  <--ws-->  真实 TRSS-Yunzai (2536)

Yunzai 侧不做任何代码改动，只把协议端地址指向网关。
判定标准：
  1. Yunzai 日志出现 bot 上线 / 完成 connect
  2. Yunzai 主动下发 get_login_info 等 action，能被网关转给协议端并拿到响应
  3. 通过协议端注入 #状态，Yunzai 真的执行插件并回消息

用法：python3 scripts/e2e_real_yunzai.py <gateway-bin> <yunzai-dir>
"""
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request

GATEWAY = sys.argv[1] if len(sys.argv) > 1 else "./target/release/yz-gateway"
YUNZAI_DIR = sys.argv[2] if len(sys.argv) > 2 else "/root/.codebuddy/artifact/yunzai"
YUNZAI_LOG = "/tmp/real_yunzai.log"
NAPCAT_PORT = 3001
SELF_ID = 10001

state = {"connected": 0, "actions": [], "replies": []}
results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name} {detail}")


async def napcat_handler(ws):
    state["connected"] += 1
    state["ws"] = ws
    print("[napcat] Yunzai(经网关) 已连接")
    await ws.send(json.dumps({
        "post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "connect",
        "self_id": SELF_ID, "time": int(time.time()),
    }))
    try:
        async for raw in ws:
            msg = json.loads(raw)
            action, params, echo = msg.get("action"), msg.get("params", {}), msg.get("echo")
            state["actions"].append(action)
            data = {}
            if action == "get_login_info":
                data = {"user_id": SELF_ID, "nickname": "联调机器人"}
            elif action == "get_friend_list":
                data = [{"user_id": 123456, "nickname": "好友A", "remark": ""}]
            elif action == "get_group_list":
                data = [{"group_id": 456789, "group_name": "测试群", "member_count": 3}]
            elif action == "get_group_member_list":
                data = [{"user_id": SELF_ID, "nickname": "联调机器人", "card": "", "role": "owner"},
                        {"user_id": 123456, "nickname": "好友A", "card": "", "role": "member"}]
            elif action == "get_group_member_info":
                data = {"user_id": params.get("user_id", 123456), "nickname": "好友A", "card": "", "role": "member"}
            elif action in ("send_group_msg", "send_msg"):
                data = {"message_id": 90001 + len(state["replies"])}
                state["replies"].append(params.get("message") or params.get("message_id"))
                print(f"[napcat] <= 回复: {json.dumps(params.get('message'), ensure_ascii=False)[:160]}")
            elif action == "send_private_msg":
                data = {"message_id": 90002}
                state["replies"].append(params.get("message"))
                print(f"[napcat] <= 私聊回复: {json.dumps(params.get('message'), ensure_ascii=False)[:160]}")
            await ws.send(json.dumps({"status": "ok", "retcode": 0, "data": data, "echo": echo}))
    except Exception as e:
        print(f"[napcat] 连接结束: {type(e).__name__}")


def tail_log(n=40):
    try:
        with open(YUNZAI_LOG, encoding="utf-8", errors="replace") as f:
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

    # redis（Yunzai 强依赖）
    redis = subprocess.Popen(["redis-server", "--port", "6379", "--save", "", "--appendonly", "no"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await asyncio.sleep(0.5)

    server = await websockets.serve(napcat_handler, "127.0.0.1", NAPCAT_PORT)
    print(f"[test] mock 协议端就绪: {NAPCAT_PORT}")

    gw = subprocess.Popen([GATEWAY, "config/gateway.toml"], stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, start_new_session=True)
    open(YUNZAI_LOG, "w").close()
    logf = open(YUNZAI_LOG, "w")
    node = subprocess.Popen(["node", "app.js"], cwd=YUNZAI_DIR, stdout=logf,
                            stderr=subprocess.STDOUT, start_new_session=True)

    try:
        up = await wait_for(lambda: state["connected"] > 0, timeout=60)
        check("Yunzai 主动连上网关（无需改代码）", up,
              f"网关连接数={state['connected']}")

        ok = await wait_for(lambda: "get_login_info" in state["actions"], timeout=30)
        check("Yunzai 下发 get_login_info 并拿到响应", ok, f"actions={state['actions'][:6]}")

        ok = await wait_for(lambda: "get_group_list" in state["actions"], timeout=30)
        check("Yunzai 拉取群列表（Bot 初始化完成）", ok, "")

        # 等插件加载完成（日志出现「加载插件」）
        await wait_for(lambda: "加载插件" in tail_log(300), timeout=40)
        await asyncio.sleep(1.0)

        print("[test] 通过协议端注入一条 #状态")
        await state["ws"].send(json.dumps({
            "post_type": "message", "message_type": "group", "sub_type": "normal",
            "self_id": SELF_ID, "user_id": 123456, "group_id": 456789, "message_id": 70002,
            "raw_message": "#状态",
            "message": [{"type": "text", "data": {"text": "#状态"}}],
            "sender": {"user_id": 123456, "nickname": "好友A", "card": ""},
            "time": int(time.time()),
        }))
        ok = await wait_for(lambda: len(state["replies"]) > 0, timeout=25)
        text = json.dumps(state["replies"], ensure_ascii=False)
        check("插件真实执行并回消息（#状态）", ok, text[:220])

    finally:
        # 直接 SIGKILL，避免 node 优雅退出时等待 redis / 定时器导致脚本挂住
        for proc in (node, gw, redis):
            if proc.poll() is None:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    proc.kill()
        logf.close()
        server.close()

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== {passed}/{len(results)} 通过 =====")
    print("\n--- Yunzai 日志（尾 25 行）---")
    print("\n".join(tail_log(25).splitlines()[-25:]))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
