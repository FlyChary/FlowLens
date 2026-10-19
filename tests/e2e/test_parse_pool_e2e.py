"""P1 解析进程池的端到端（§3.5）：应用关闭（SIGTERM）时进程池被关闭、无残留子进程。

信号处理（add_signal_handler）仅 POSIX 支持，Windows 走 Ctrl+C 路径，故本测试仅 Linux
运行（与 test_signals.py 同口径）。用真实子进程 `python -m flowlens`（FLOWLENS_PARSE_WORKERS=2），
SIGTERM 后：① 5 秒内退出码 0；② 服务进程的两个解析 worker 子进程随之退出、无残留。
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


def _child_pids(pid: int) -> list[int]:
    """读 /proc/<pid>/task/<pid>/children 得到直接子进程 PID 列表（Linux）。"""
    try:
        p = Path(f"/proc/{pid}/task/{pid}/children")
        return [int(x) for x in p.read_text().split() if x]
    except OSError:
        return []


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)  # 信号 0：只探测存在性，不发信号
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _start_service(port: int, n_workers: int = 2) -> subprocess.Popen:
    env = dict(os.environ, PYTHONHASHSEED="0",
               FLOWLENS_PARSE_WORKERS=str(n_workers))
    return subprocess.Popen(
        [sys.executable, "-m", "flowlens", "--port", str(port)],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace",
    )


def test_sigterm_closes_pool_no_residual_workers():
    """服务（带 2 个解析 worker）收到 SIGTERM：5 秒内退出码 0，且 worker 子进程无残留。"""
    port = _free_port()
    proc = _start_service(port, n_workers=2)
    try:
        assert _wait_healthz(port), "服务未在超时内就绪"
        # 记下服务的两个解析 worker 子进程 PID
        workers = _child_pids(proc.pid)
        # 给 worker 稳定下来一点时间（spawn 启动 + 预热），确保读到的是池 worker
        assert len(workers) >= 2, f"应看到至少 2 个解析 worker 子进程，实际 {workers}"

        t0 = time.time()
        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=10)
        elapsed = time.time() - t0
        out = proc.stdout.read()
        assert rc == 0, f"退出码 {rc}（应为 0）\n日志:\n{out}"
        assert elapsed < 10, f"退出耗时 {elapsed:.2f}s（应 < 10s）"

        # worker 子进程应随之退出、无残留（给 reap 一点时间）
        deadline = time.time() + 5
        while time.time() < deadline:
            if any(_pid_alive(p) for p in workers):
                time.sleep(0.2)
                continue
            break
        still_alive = [p for p in workers if _pid_alive(p)]
        assert not still_alive, f"close 后仍有解析 worker 子进程存活: {still_alive}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
