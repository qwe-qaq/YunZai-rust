#!/usr/bin/env python3
"""
端到端联调脚本（不需要真的 Yunzai / NapCat）。

模拟两端：
  - mock napcat：OneBot v11 正向 WS 服务端（3001），发事件、应答 action
  - mock yunzai：OneBot v11 适配器行为（2536 /OneBotv11），收事件、发 action

验证：
  1. lifecycle 透传，Yunzai 能「上线」
  2. 事件 上游 -> 下游
  3. action 下游 -> 上游 -> 响应回传（echo 匹配）
  4. /status /metrics 可观测
  5. 上游掉线后，下游连接被主动断开（避免 Yunzai 挂死连接），并自动重连
"""
import asyncio
import json
import subprocess
import sys
import time
import urllib.request

NAPCAT_PORT = 3001
YUNZAI_PORT = 2536
STATUS = "http://127.0.0.1:6701"
SELF_ID = 10001
GATEWAY = sys.argv[1] if len(sys.argv) > 1 else "./target/release/yz-gateway"

state = {
    "napcat_connects": 0,
    "napcat_actions": [],
    "yunzai_events": [],
    "yunzai_responses": {},
    "yunzai_connects": 0,
    "yunzai_disconnects": 0,
}
napcat_ws = None
results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name} {detail}")


async def napcat_handler(ws):
    global napcat_ws
    napcat_ws = ws
    state["napcat_connects"] += 1
    print(f"[napcat] 网关已连接 (#{state['napcat_connects']})")
    now = int(time.time())
    await ws.send(json.dumps({
        "post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "connect",
        "self_id": SELF_ID, "time": now,
    }))
    await ws.send(json.dumps({
        "post_type": "meta_event", "meta_event_type": "heartbeat", "self_id": SELF_ID,
        "time": now, "status": {"online": True, "good": True}, "interval": 30000,
    }))
    try:
        async for raw in ws:
            msg = json.loads(raw)
            action, params, echo = msg.get("action"), msg.get("params", {}), msg.get("echo")
            state["napcat_actions"].append((action, params))
            print(f"[napcat] <- {action} {params}")
            data = {}
            if action == "get_login_info":
                data = {"user_id": SELF_ID, "nickname": "测试机器人"}
            elif action == "get_friend_list":
                data = [{"user_id": 123456, "nickname": "好友A", "remark": ""}]
            elif action == "get_group_list":
                data = [{"group_id": 456789, "group_name": "测试群", "member_count": 3}]
            elif action == "get_group_member_list":
                data = [{"user_id": SELF_ID, "nickname": "测试机器人", "card": "", "role": "owner"}]
            elif action == "send_group_msg":
                data = {"message_id": 90001}
            elif action == "send_private_msg":
                data = {"message_id": 90002}
            await ws.send(json.dumps({"status": "ok", "retcode": 0, "data": data, "echo": echo}))
    except Exception as e:
        print(f"[napcat] 连接中断: {type(e).__name__}")


async def yunzai_handler(ws):
    state["yunzai_connects"] += 1
    print(f"[yunzai] 网关已接入 ({ws.request.path})")
    try:
        async for raw in ws:
            msg = json.loads(raw)
            if "post_type" in msg:
                state["yunzai_events"].append(msg)
                tag = msg["post_type"]
                if msg.get("meta_event_type"):
                    tag += "/" + msg["meta_event_type"]
                if msg.get("message_type"):
                    tag += "/" + msg["message_type"]
                print(f"[yunzai] <- 事件 {tag} raw={msg.get('raw_message')!r}")
                if msg.get("post_type") == "meta_event" and msg.get("meta_event_type") == "lifecycle":
                    await ws.send(json.dumps({"action": "get_login_info", "params": {}, "echo": "e-login"}))
                    await ws.send(json.dumps({"action": "get_group_list", "params": {}, "echo": "e-groups"}))
                elif msg.get("post_type") == "message":
                    await ws.send(json.dumps({
                        "action": "send_group_msg",
                        "params": {"group_id": msg.get("group_id"), "message": "收到:" + (msg.get("raw_message") or "")},
                        "echo": "e-send",
                    }))
            else:
                state["yunzai_responses"][msg.get("echo")] = msg
                print(f"[yunzai] <- 响应 {msg.get('echo')} retcode={msg.get('retcode')}")
    except Exception as e:
        print(f"[yunzai] 连接中断: {type(e).__name__}")
    finally:
        # 正常关闭或异常断开，都代表网关主动断开了这条下游连接
        state["yunzai_disconnects"] += 1


def http_json(path):
    with urllib.request.urlopen(STATUS + path, timeout=3) as r:
        return r.read().decode()


async def wait_for(pred, timeout=5.0, interval=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        await asyncio.sleep(interval)
    return False


async def main():
    import websockets

    napcat_server = await websockets.serve(napcat_handler, "127.0.0.1", NAPCAT_PORT)
    yunzai_server = await websockets.serve(yunzai_handler, "127.0.0.1", YUNZAI_PORT)
    print(f"[test] mock 就绪: napcat:{NAPCAT_PORT}  yunzai:{YUNZAI_PORT}/OneBotv11")

    proc = subprocess.Popen(
        [GATEWAY, "config/gateway.toml"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=True,
    )
    try:
        ok = await wait_for(lambda: len(state["yunzai_events"]) > 0, timeout=15)
        check("lifecycle 透传（Yunzai 上线）", ok,
              f"events={[e.get('meta_event_type') for e in state['yunzai_events']]}")

        ok = await wait_for(lambda: "e-login" in state["yunzai_responses"], timeout=5)
        resp = state["yunzai_responses"].get("e-login", {})
        check("action 往返 get_login_info", ok and resp.get("data", {}).get("nickname") == "测试机器人",
              f"{resp.get('data')}")

        ok = await wait_for(lambda: "e-groups" in state["yunzai_responses"], timeout=5)
        check("action 往返 get_group_list", ok, "")

        await napcat_ws.send(json.dumps({
            "post_type": "message", "message_type": "group", "sub_type": "normal",
            "self_id": SELF_ID, "user_id": 123456, "group_id": 456789,
            "message_id": 70001, "raw_message": "#帮助",
            "message": [{"type": "text", "data": {"text": "#帮助"}}],
            "sender": {"user_id": 123456, "nickname": "好友A"}, "time": int(time.time()),
        }))
        ok = await wait_for(lambda: any(e.get("raw_message") == "#帮助" for e in state["yunzai_events"]))
        check("事件 上游 -> 下游", ok, "")

        ok = await wait_for(lambda: any(a == "send_group_msg" for a, _ in state["napcat_actions"]))
        check("action 下游 -> 上游 send_group_msg", ok, "")

        ok = await wait_for(lambda: "e-send" in state["yunzai_responses"])
        sent = state["yunzai_responses"].get("e-send", {})
        check("action 响应回传（echo 匹配）", ok and sent.get("data", {}).get("message_id") == 90001,
              f"{sent.get('data')}")

        st = json.loads(http_json("/status"))
        connected = st["upstreams"][0]["phase"] == "connected"
        check("/status 上报连接状态", connected and st["stats"]["events"] > 0,
              f"phase={st['upstreams'][0]['phase']} events={st['stats']['events']}")
        m = http_json("/metrics")
        check("/metrics 输出 Prometheus 文本", "yunzai_gateway_events_total" in m, "")

        before = state["napcat_connects"]
        disconnects_before = state["yunzai_disconnects"]
        await napcat_ws.close()
        ok = await wait_for(lambda: state["yunzai_disconnects"] > disconnects_before, timeout=5)
        check("上游掉线 -> 断开下游（避免僵尸连接）", ok,
              f"disconnects={state['yunzai_disconnects']}")
        ok = await wait_for(lambda: state["napcat_connects"] > before, timeout=10)
        check("自动重连协议端", ok, f"connects={state['napcat_connects']}")
    finally:
        proc.terminate()
        try:
            out, _ = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            out = ""
        print("\n--- 网关日志（尾 20 行）---")
        print("\n".join((out or "").splitlines()[-20:]))
        napcat_server.close()
        yunzai_server.close()

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== {passed}/{len(results)} 通过 =====")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
