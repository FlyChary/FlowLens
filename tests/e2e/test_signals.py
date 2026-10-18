"""退出信号测试（B1b）：docker stop 发 SIGTERM，服务要能几秒内干净退出（退出码 0）。

背景：`__main__.py` 之前只 `except KeyboardInterrupt`（SIGINT）并 `asyncio.Event().wait()`
永远等下去；容器里 PID 1 收到的 SIGTERM 被忽略，`docker stop` 只能等满宽限期再 SIGKILL。
现在 `__main__.py` 用 `loop.add_signal_handler` 为 SIGTERM/SIGINT 注册处理器，收到即置位
停止事件、照常 `runner.cleanup()` 后退出。本测试用真实子进程（`python -m flowlens`）验证：
  - 空闲时发 SIGTERM，5 秒内以退出码 0 结束；
  - 有一个 SSE 订阅连接未断开时，发 SIGTERM 同样 5 秒内退出（cleanup 会结束所有流）。

`add_signal_handler` 只在支持信号的事件循环上可用（POSIX），Windows 走 Ctrl+C 路径，
故本测试仅 Linux 运行，其它平台跳过。
"""

import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

# tests/e2e/ → tests/ → 项目根
ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(sys.platform != "linux",
                                reason="信号处理（add_signal_handler）仅 POSIX 支持，非 Linux 跳过")


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_healthz(port: int, timeout: float = 20.0) -> bool:
    """轮询 /healthz 直到 200 或超时。"""
    url = f"http://127.0.0.1:{port}/healthz"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def _start_service(port: int) -> subprocess.Popen:
    env = dict(os.environ, PYTHONHASHSEED="0")
    return subprocess.Popen(
        [sys.executable, "-m", "flowlens", "--port", str(port)],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )


def test_sigterm_clean_exit():
    """空闲服务收到 SIGTERM：5 秒内退出码 0，日志记录收到的信号。"""
    port = _free_port()
    proc = _start_service(port)
    try:
        assert _wait_healthz(port), "服务未在超时内就绪"
        t0 = time.time()
        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=5)
        elapsed = time.time() - t0
        out = proc.stdout.read().decode(errors="replace")
        assert rc == 0, f"退出码 {rc}（应为 0）\n日志:\n{out}"
        assert elapsed < 5, f"退出耗时 {elapsed:.2f}s（应 < 5s）"
        # 日志记录了收到的信号（退出原因）
        assert "SIGTERM" in out, f"日志未记录收到的信号:\n{out}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_sigterm_clean_exit_with_sse_subscription():
    """有一个 SSE 订阅连接未断开时，SIGTERM 仍 5 秒内退出（cleanup 结束所有流）。"""
    port = _free_port()
    proc = _start_service(port)
    sse = None
    try:
        assert _wait_healthz(port), "服务未在超时内就绪"
        # 建立 SSE 订阅：原始 socket 发 GET 请求头并保持打开（不读响应，连接常驻）
        sse = socket.create_connection(("127.0.0.1", port), timeout=5)
        sse.sendall(
            b"GET /v1/topk/stream?window=60s&k=10&by=url&interval=1s HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Accept: text/event-stream\r\n\r\n"
        )
        # 给服务一点时间把订阅登记进 StreamManager
        time.sleep(0.5)
        # 确认订阅确实建立（读到 200 响应头）
        first = sse.recv(1024)
        assert first.startswith(b"HTTP/1.1 200"), f"SSE 订阅未建立: {first[:64]!r}"

        t0 = time.time()
        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=5)
        elapsed = time.time() - t0
        out = proc.stdout.read().decode(errors="replace")
        assert rc == 0, f"退出码 {rc}（应为 0）\n日志:\n{out}"
        assert elapsed < 5, f"退出耗时 {elapsed:.2f}s（应 < 5s）"
    finally:
        if sse is not None:
            try:
                sse.close()
            except Exception:
                pass
        if proc.poll() is None:
            proc.kill()
            proc.wait()
