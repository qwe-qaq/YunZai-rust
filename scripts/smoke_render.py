#!/usr/bin/env python3
"""yz-render 冒烟测试（CI 用，不需要 Yunzai）。

流程：
  1. 生成一个中文 HTML
  2. 用临时 toml 起 yz-render（Chrome 路径可覆盖）
  3. /healthz 就绪后 POST /render
  4. 校验返回的是 PNG/JPEG 且尺寸合理
  5. 连续渲染 3 张，确认 Chrome 复用没泄漏

用法：
  python3 scripts/smoke_render.py ./yz-render [chrome 路径]
退出码非 0 表示失败。
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

RENDER = sys.argv[1] if len(sys.argv) > 1 else "./target/release/yz-render"
CHROME = sys.argv[2] if len(sys.argv) > 2 else ""
PORT = 6799
BASE = f"http://127.0.0.1:{PORT}"

HTML = """<!doctype html><html><head><meta charset="utf-8"><style>
body{margin:0;font-family:sans-serif;background:#0f172a}
#container{width:600px;padding:24px;color:#e2e8f0}
h1{font-size:28px;margin:0 0 8px;color:#7dd3fc}
</style></head><body><div id="container">
<h1>渲染冒烟测试</h1><div>中文 / Latin / 数字 1234567890</div>
<div style="margin-top:12px;font-size:40px;font-weight:700;color:#ffd479">48,127</div>
</div></body></html>"""

results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name} {detail}")


def free_port_ok():
    import socket

    s = socket.socket()
    # 加 SO_REUSEADDR：否则上一轮残留的 TIME_WAIT 会让 bind 失败，
    # 误报成「端口被占用」。
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", PORT))
        return True
    except OSError:
        return False
    finally:
        s.close()


def wait_http(path, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(BASE + path, timeout=2) as r:
                return r.status == 200
        except Exception:
            time.sleep(0.2)
    return False


def render(payload, timeout=30):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE + "/render", data=body, headers={"content-type": "application/json"}
    )
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
    return data, (time.perf_counter() - t) * 1000


def main():
    if not os.path.isfile(RENDER):
        print(f"找不到渲染服务二进制: {RENDER}")
        return 1
    if not free_port_ok():
        print(f"端口 {PORT} 已被占用")
        return 1

    work = "/tmp/yz-smoke"
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)
    html_path = os.path.join(work, "page.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(HTML)

    profile = os.path.join(work, "profile")
    toml = os.path.join(work, "render.toml")
    binary = CHROME or "chromium"
    with open(toml, "w", encoding="utf-8") as f:
        f.write(
            f"""[server]
listen = "127.0.0.1:{PORT}"
concurrency = 3

[chrome]
binary = "{binary}"
user_data_dir = "{profile}"
extra_args = []
"""
        )

    proc = subprocess.Popen(
        [RENDER, toml],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        ok = wait_http("/healthz", timeout=25)
        check("/healthz 就绪", ok)
        if not ok:
            return 1

        def png_size(blob):
            if blob[:4] != b"\x89PNG" or len(blob) < 24:
                return None
            return (
                int.from_bytes(blob[16:20], "big"),
                int.from_bytes(blob[20:24], "big"),
            )

        base = {
            "url": "file://" + html_path,
            "width": 600,
            "height": 300,
            "img_type": "png",
        }
        d1, ms1 = render({**base, "scale": 1})
        d2, ms2 = render({**base, "scale": 2})
        check("返回 PNG（魔数 89 50 4E 47）", d2[:4] == b"\x89PNG", f"{len(d2)}B")
        s1, s2 = png_size(d1), png_size(d2)
        if s1 and s2:
            # 截图宽度取文档实际宽度（含 padding），不是 viewport，
            # 所以用相对比值断言：scale=2 的宽高应正好是 scale=1 的两倍。
            ok = abs(s2[0] - s1[0] * 2) <= 2 and abs(s2[1] - s1[1] * 2) <= 2
            check("scale 生效（2x 尺寸为 1x 的两倍）", ok, f"{s1[0]}x{s1[1]} -> {s2[0]}x{s2[1]}")
            check("宽高为正且非 1x1 占位图", s1[0] > 100 and s1[1] > 50, f"{s1[0]}x{s1[1]}")
        else:
            check("scale 生效（2x 尺寸为 1x 的两倍）", False, "非 PNG")
            check("宽高为正且非 1x1 占位图", False, "非 PNG")
        check("图像体积合理（>10KB，说明渲染出内容而非空白）", len(d2) > 10_000, f"{len(d2)}B")
        print(f"[info] 首张渲染 {ms1:.0f}ms / 2x {ms2:.0f}ms")

        # 连渲染 3 张，验证 page 复用（第二张起应明显快于首张的冷启动）
        times = []
        for i in range(3):
            d, ms = render(
                {
                    "url": "file://" + html_path,
                    "width": 400,
                    "height": 200,
                    "scale": 1,
                    "img_type": "jpeg",
                    "quality": 80,
                }
            )
            times.append(ms)
            if d[:2] != b"\xff\xd8":
                check(f"第 {i + 2} 张返回 JPEG", False, f"{d[:8]!r}")
                break
        else:
            check("连续 3 张复用 Chrome 成功", True, f"耗时 {[round(t) for t in times]}ms")
            check("复用后单张 < 1s", max(times) < 1000, f"max={max(times):.0f}ms")
    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            proc.kill()
        try:
            out, _ = proc.communicate(timeout=5)
        except Exception:
            out = ""
        if out:
            print("--- yz-render 日志（尾 15 行）---")
            print("\n".join(out.splitlines()[-15:]))

    passed = sum(1 for _, ok in results if ok)
    print(f"\n===== {passed}/{len(results)} 通过 =====")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
