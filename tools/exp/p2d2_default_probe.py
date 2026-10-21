"""P2d-2 第七节补充测量：在 20 万/秒写入下，**默认窗口**（60s、不带 at）的
topk 与 stats 由前端本地回答，直接测它们的 p50/p95/p99（指令目标 p99 < 50 ms）。

S2 的 query_loop 里 "stats" 用的是随机 10~60s 窗口（绝大多数走转发/回放，
不是"默认 stats"），所以这里单独把默认窗口的 topk 与 stats 各以 ~40/s 打
40 秒。写入由 loadgen 子进程（20 万/s 快速模式）提供；本脚本用服务 /healthz 的
events_ingested 增量**确认写入确实到 ~20 万/s 之后**才开始采样（快速模式每
worker 预生成模板要 ~12 s，之前没有流量），避免"空转服务"的假低值。

用法（WSL，服务容器已在跑、监听 --port）：
    python tools/exp/p2d2_default_probe.py --port 12345 --duration 40
"""
import argparse
import asyncio
import json
import statistics as st
import subprocess
import sys
import time
import urllib.request

import aiohttp

TOOLS = None


def ingested(port) -> int:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz",
                                timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))["events_ingested"]


def pct(v, p):
    if not v:
        return None
    v = sorted(v)
    k = (len(v) - 1) * p / 100.0
    f, c = int(k), min(int(k) + 1, len(v) - 1)
    return v[f] + (v[c] - v[f]) * (k - f)


async def fire(ses, url, out, dur, rate):
    deadline = time.monotonic() + dur
    next_t = time.monotonic()
    while time.monotonic() < deadline:
        t0 = time.perf_counter()
        try:
            async with ses.get(url) as r:
                await r.read()
        except Exception:
            pass
        out.append((time.perf_counter() - t0) * 1000.0)
        next_t = max(next_t, time.monotonic())
        await asyncio.sleep(max(0.0, 1.0 / rate - (time.monotonic() - next_t)))


def start_loadgen(url, rate, duration, workers, batch) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, TOOLS + "/loadgen.py", "--url", url, "--workers",
         str(workers), "--conc", "8", "--rate", str(rate), "--batch",
         str(batch), "--duration", str(duration), "--mode", "长尾",
         "--seed", "200", "--fast", "--fast-templates", "32"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


async def wait_write_rate(port, target, timeout=60.0):
    """等到 /healthz 的 events_ingested 增量达到 target（每秒），返回实测速率。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        e1 = ingested(port)
        await asyncio.sleep(2.0)
        e2 = ingested(port)
        rate = (e2 - e1) // 2
        if rate >= target:
            return rate
    return rate


async def main(port, duration):
    base = f"http://127.0.0.1:{port}"
    lg = start_loadgen(base, 200000, duration + 30, 8, 5000)
    try:
        rate = await wait_write_rate(port, 150000, timeout=90.0)
        print(f"写入速率（healthz 增量/秒）= {rate}（目标 ≥150000）", flush=True)
        if rate < 150000:
            print("ABORT: 写入未达 ~20 万/s，本测量不作数", flush=True)
            return
        async with aiohttp.ClientSession(
                trust_env=False, timeout=aiohttp.ClientTimeout(total=10)) as ses:
            await ses.get(f"{base}/v1/topk?by=url&k=10&window=60s")   # 预热
            topk, stats = [], []
            await asyncio.gather(
                fire(ses, f"{base}/v1/topk?by=url&k=10&window=60s",
                     topk, duration, 40),
                fire(ses, f"{base}/v1/stats?window=60s", stats, duration, 40),
            )
        for name, v in (("topk_def", topk), ("stats_def", stats)):
            print(f"{name:10s} n={len(v):4d} p50={st.median(v):6.1f} "
                  f"p95={pct(v, 95):6.1f} p99={pct(v, 99):6.1f} "
                  f"max={max(v):6.1f}", flush=True)
    finally:
        lg.terminate()
        try:
            lg.wait(timeout=10)
        except subprocess.TimeoutExpired:
            lg.kill()


if __name__ == "__main__":
    import os
    TOOLS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=12345)
    ap.add_argument("--duration", type=float, default=40)
    a = ap.parse_args()
    asyncio.run(main(a.port, a.duration))
