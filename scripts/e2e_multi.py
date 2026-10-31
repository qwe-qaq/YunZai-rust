#!/usr/bin/env python3
"""
多账号路由验证。

两个协议端（3001 = 10001，3002 = 10002）共用一个 Yunzai。
检查：
  1. 网关为每个账号建一条独立的下游连接
  2. 事件只投递给对应账号的连接（不串号）
  3. action 只回到发起它的协议端
  4. 一个账号掉线不影响另一个
"""
import asyncio
import json
import subprocess
import sys
import time
import urllib.request

PORTS = {10001: 3001, 10002: 3002}
YUNZAI_PORT = 2536
STATUS = "http://127.0.0.1:6701"
GATEWAY = sys.argv[1] if len(sys.argv) > 1 else "./target/release/yz-gateway"
CONFIG = "config/gateway.multi.toml"

state = {
    "up_conns": {},          # self_id -> ws
    "up_connected": 0,
    "down": {},              # conn_seq -> {"ws": ws, "self_id": None, "events": []}
    "down_seq": 0,
    "actions": {},           # self_id -> [action names]
}
results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name} {detail}")


def make_napcat(self_id):
    async def handler(ws):
        state["up_conns"][self_id] = ws
        state["up_connected"] += 1
        state["actions"].setdefault(self_id, [])
        print(f"[napcat:{self_id}] 网关已连接")
        await ws.send(json.dumps({
            "post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "connect",
            "self_id": self_id, "time": int(time.time()),
        }))
        try:
            async for raw in ws:
                msg = json.loads(raw)
                action = msg.get("action")
                state["actions"][self_id].append(action)
                print(f"[napcat:{self_id}] <- {action}")
                data = {"message_id": self_id * 10}
                if action == "get_login_info":
                    data = {"user_id": self_id, "nickname": f"机器人{self_id}"}
                elif action in ("get_group_list", "get_friend_list"):
                    data = []
                await ws.send(json.dumps({"status": "ok", "retcode": 0, "data": data, "echo": msg.get("echo")}))
        except Exception:
            pass
    return handler


async def yunzai_handler(ws):
    state["down_seq"] += 1
    seq = state["down_seq"]
    rec = {"ws": ws, "self_id": None, "events": []}
    state["down"][seq] = rec
    print(f"[yunzai] 下游连接 #{seq}")
    try:
        async for raw in ws:
            msg = json.loads(raw)
            if "post_type" in msg:
                sid = msg.get("self_id")
                rec["self_id"] = sid
                rec["events"].append(msg)
                print(f"[yunzai:#{seq}] 事件 self_id={sid} raw={msg.get('raw_message')!r}")
                if msg.get("post_type") == "meta_event" and msg.get("meta_event_type") == "lifecycle":
                    await ws.send(json.dumps({"action": "get_login_info", "params": {}, "echo": f"login-{sid}"}))
                elif msg.get("post_type") == "message":
                    await ws.send(json.dumps({
                        "action": "send_group_msg",
                        "params": {"group_id": msg.get("group_id"), "message": f"echo:{msg.get('raw_message')}"},
                        "echo": f"send-{sid}",
                    }))
            else:
                pass
    except Exception:
        pass
    finally:
        rec["closed"] = True
        print(f"[yunzai:#{seq}] 连接关闭")


async def wait_for(pred, timeout=8.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        await asyncio.sleep(0.05)
    return False


def http_json(path):
    with urllib.request.urlopen(STATUS + path, timeout=3) as r:
        return r.read().decode()


async def main():
    import websockets

    servers = [await websockets.serve(make_napcat(sid), "127.0.0.1", port) for sid, port in PORTS.items()]
    yunzai_server = await websockets.serve(yunzai_handler, "127.0.0.1", YUNZAI_PORT)
    print("[test] mock 就绪: 10001->3001, 10002->3002, yunzai->2536")

    proc = subprocess.Popen([GATEWAY, CONFIG], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, start_new_session=True)
    try:
        ok = await wait_for(lambda: len(state["down"]) >= 2 and
                            all(r["self_id"] for r in state["down"].values()), timeout=15)
        check("两个账号各建一条下游连接", ok,
              f"conns={[(k, v['self_id']) for k, v in state['down'].items()]}")

        # 账号 A 收消息
        await state["up_conns"][10001].send(json.dumps({
            "post_type": "message", "message_type": "group", "sub_type": "normal",
            "self_id": 10001, "user_id": 1, "group_id": 111, "message_id": 1,
            "raw_message": "A的消息", "message": [], "sender": {"user_id": 1, "nickname": "u"},
            "time": int(time.time()),
        }))
        ok = await wait_for(lambda: any(e.get("raw_message") == "A的消息"
                                        for r in state["down"].values() for e in r["events"]))
        holder = [k for k, r in state["down"].items() if any(e.get("raw_message") == "A的消息" for e in r["events"])]
        others = [k for k, r in state["down"].items() if any(e.get("raw_message") == "A的消息" for e in r["events"])
                  and r["self_id"] != 10001]
        check("事件不串号（只投递给 10001 的连接）", ok and not others, f"holder={holder}")

        ok = await wait_for(lambda: "send_group_msg" in state["actions"].get(10001, []))
        check("action 回到正确的协议端（10001）", ok and "send_group_msg" not in state["actions"].get(10002, []),
              f"A={state['actions'].get(10001)} B={state['actions'].get(10002)}")

        # 账号 B 收消息
        await state["up_conns"][10002].send(json.dumps({
            "post_type": "message", "message_type": "group", "sub_type": "normal",
            "self_id": 10002, "user_id": 2, "group_id": 222, "message_id": 2,
            "raw_message": "B的消息", "message": [], "sender": {"user_id": 2, "nickname": "v"},
            "time": int(time.time()),
        }))
        ok = await wait_for(lambda: any(e.get("raw_message") == "B的消息"
                                        for r in state["down"].values() if r["self_id"] == 10002 for e in r["events"]))
        check("第二个账号独立收发", ok, "")

        st = json.loads(http_json("/status"))
        check("/status 列出两个上游", len(st["upstreams"]) == 2 and
              all(u["phase"] == "connected" for u in st["upstreams"]),
              str([(u["name"], u["self_id"], u["phase"]) for u in st["upstreams"]]))

        # 掉线隔离：用掉线前的连接快照做断言（网关会立刻重连并新建下游连接）
        a_conn = [k for k, r in state["down"].items() if r["self_id"] == 10001]
        b_conn = [k for k, r in state["down"].items() if r["self_id"] == 10002]
        await state["up_conns"][10001].close()
        ok = await wait_for(lambda: all(state["down"][k].get("closed") for k in a_conn))
        alive_b = all(not state["down"][k].get("closed") for k in b_conn)
        check("单账号掉线不影响另一个", ok and alive_b,
              f"A_closed={[state['down'][k].get('closed') for k in a_conn]} B_alive={alive_b}")
    finally:
        proc.terminate()
        try:
            out, _ = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            out = ""
        print("\n--- 网关日志 ---")
        print("\n".join((out or "").splitlines()[-12:]))
        for s in servers:
            s.close()
        yunzai_server.close()

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n===== {passed}/{len(results)} 通过 =====")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
