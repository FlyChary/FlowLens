"""M1 第二.1：评测配置（256 MB、自动单进程、**不加 --cpuset-cpus**）下，
不同写入速率（0/3/5/8 万每秒）下三类查询的 p50/p95/p99 + 最高降级级别 + 工作集峰值。

与 P2d-3 二.1 的 p2d3_query_probe.py 差异（按 M1 指令）：
- 容器**不加 --cpuset-cpus**（与评测方最可能的方式一致），压测进程用 taskset 固定核；
- 不设置 FLOWLENS_FRONTENDS（自动判断），每档从 docker logs 确认前端数 = 0；
- 只测三类：topk_default（60s 不带 at）、stats_default（60s 不带 at）、
  topk_replay（随机 at + 随机 10~60s 窗口）；
- 0 速率档不起 loadgen（只查询）；
- 窗内逐秒采 /healthz 记工作集峰值（memory_bytes）与各级别的出现情况。

结果 JSON 每档一行（增量写盘），供汇总成"行=写入速率、列=三类查询 p99"表。

用法（WSL，服务镜像已构建）：
    python tools/exp/m1_q21.py --image flowlens:merged \
        --rates 0,30000,50000,80000 --duration 120 --out results/m1/q21.json
"""
import argparse
import asyncio
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TOOLS = os.path.join(ROOT, "tools")
sys.path.insert(0, TOOLS)

import experiments as ex   # noqa: E402  复用 read_front_count / expand_cpus

import aiohttp  # noqa: E402

LOAD_CP = "8-15"
LOAD_WORKERS = 8
BATCH = 5000
Q_RATE = 20.0   # 每类查询 20 q/s（与 S2 的 rate_per_type 同量级）


def stop_loadgen(lg: subprocess.Popen) -> None:
    """停 loadgen：对整个进程组发 SIGTERM，宽限 5 s 后 SIGKILL 兜底
    （worker 是 spawn 的 daemon 子进程，只杀主进程会留孤儿——P2d-3 已踩过）。"""
    try:
        os.killpg(os.getpgid(lg.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        lg.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(os.getpgid(lg.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        lg.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def pct(v, p):
    if not v:
        return None
    v = sorted(v)
    k = (len(v) - 1) * p / 100.0
    f, c = int(k), min(int(k) + 1, len(v) - 1)
    return v[f] + (v[c] - v[f]) * (k - f)


def _r2(x):
    return None if x is None else round(x, 2)


def _q(v):
    return {"n": len(v), "p50": _r2(pct(v, 50)), "p95": _r2(pct(v, 95)),
            "p99": _r2(pct(v, 99)), "max": _r2(max(v)) if v else None}


def healthz(port) -> dict:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz",
                                    timeout=5) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return {}


async def docker_logs(container) -> str:
    proc = await asyncio.create_subprocess_exec("docker", "logs", container,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    return out.decode("utf-8", "replace")


async def docker_run(container, port, image, envs, mem="256m") -> None:
    """与评测方一致：--memory/--memory-swap，**不加 --cpuset-cpus**、不设
    FLOWLENS_FRONTENDS（自动判断）。"""
    cmd = ["docker", "run", "-d", "--name", container,
           f"--memory={mem}", f"--memory-swap={mem}",
           *envs, "-p", f"{port}:8080", image]
    proc = await asyncio.create_subprocess_exec(*cmd,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    await proc.communicate()


def free_port() -> int:
    s = socket.socket(); s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]; s.close(); return p


async def wait_ready(port, timeout=60.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if healthz(port).get("ok"):
            return True
        await asyncio.sleep(0.5)
    return False


async def wait_write_rate(port, target, timeout=180.0) -> int:
    """等到 /healthz events_ingested 增量达到目标 90%（快速模式预生成模板要 ~12 s）。"""
    rate = 0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        e1 = int(healthz(port).get("events_ingested", 0))
        await asyncio.sleep(2.0)
        e2 = int(healthz(port).get("events_ingested", 0))
        rate = (e2 - e1) // 2
        if rate >= int(target * 0.90):
            return rate
    return rate


async def fire(ses, url_fn, out, dur, rate):
    """按 rate 次/秒连续发 url_fn(rng) 生成的 GET，记录每次延迟 ms。"""
    deadline = time.monotonic() + dur
    while time.monotonic() < deadline:
        url = url_fn(random)
        t0 = time.perf_counter()
        try:
            async with ses.get(url) as r:
                await r.read()
        except Exception:
            pass
        out.append((time.perf_counter() - t0) * 1000.0)
        await asyncio.sleep(max(0.0, 1.0 / rate))


async def mem_monitor(port, dur) -> dict:
    """逐秒采 /healthz：工作集峰值（memory_bytes）、原始读数峰值（memory_current_bytes）、
    出现的降级级别集合。"""
    peak_ws, peak_cur, levels = 0, 0, set()
    deadline = time.monotonic() + dur
    while time.monotonic() < deadline:
        h = healthz(port)
        peak_ws = max(peak_ws, int(h.get("memory_bytes") or 0))
        peak_cur = max(peak_cur, int(h.get("memory_current_bytes") or 0))
        d = h.get("degrade") or {}
        if d:
            levels.add(int(d.get("level") or 0))
        await asyncio.sleep(1.0)
    return {"workset_peak_mib": round(peak_ws / 2**20, 1),
            "memcur_peak_mib": round(peak_cur / 2**20, 1),
            "degrade_levels_seen": sorted(levels)}


async def run_rate(port, name, rate, duration, seed) -> dict:
    base = f"http://127.0.0.1:{port}"
    lg = None
    if rate > 0:
        lg_args = [sys.executable, os.path.join(TOOLS, "loadgen.py"),
                   "--url", base, "--workers", str(LOAD_WORKERS), "--conc", "8",
                   "--rate", str(rate), "--batch", str(BATCH),
                   "--duration", str(int(duration) + 180),
                   "--mode", "长尾", "--seed", str(seed),
                   "--fast", "--fast-templates", "32"]
        if shutil.which("taskset"):
            lg_args = ["taskset", "-c", LOAD_CP] + lg_args
        lg = subprocess.Popen(lg_args, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, start_new_session=True)
        wr = await wait_write_rate(port, rate)
        if wr < int(rate * 0.80):
            print(f"  [warn] 写入 {wr}/s 偏低（目标 {rate}），本档 p99 仅供参考", flush=True)
    else:
        wr = 0
        print(f"  [info] 0 速率档：不起 loadgen，只查询", flush=True)

    t0 = time.monotonic()
    topk_def, stats_def, topk_rep = [], [], []
    async with aiohttp.ClientSession(trust_env=False,
                                     timeout=aiohttp.ClientTimeout(total=10)) as ses:
        await ses.get(f"{base}/v1/topk?by=url&k=10&window=60s")  # 预热
        mem, _, _, _ = await asyncio.gather(
            mem_monitor(port, duration),
            fire(ses, lambda r: (f"{base}/v1/topk?by={'url' if r.random() < 0.5 else 'user'}"
                                 f"&k=10&window=60s"), topk_def, duration, Q_RATE),
            fire(ses, lambda r: f"{base}/v1/stats?window=60s", stats_def, duration, Q_RATE),
            fire(ses, lambda r: (f"{base}/v1/topk?by={'url' if r.random() < 0.5 else 'user'}"
                                 f"&k=10&window={r.randint(10, 60)}s&at="
                                 f"{int(time.time() * 1000) - r.randint(10, 300) * 1000}"),
                 topk_rep, duration, Q_RATE),
        )
    wall = time.monotonic() - t0
    if lg is not None:
        stop_loadgen(lg)

    return {
        "rate": rate, "write_rate_meas": wr, "window_s": round(wall, 1),
        "workset_peak_mib": mem["workset_peak_mib"],
        "memcur_peak_mib": mem["memcur_peak_mib"],
        "degrade_levels_seen": mem["degrade_levels_seen"],
        "queries": {
            "topk_default": _q(topk_def), "stats_default": _q(stats_def),
            "topk_replay": _q(topk_rep),
        },
    }


def save_partial(out_path: str, obj: dict) -> None:
    """每档完成后立即写盘（长时间实验防中断丢数据；临时文件再改名防半截 JSON）。"""
    tmp = out_path + ".partial"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, out_path)


async def main_async(args) -> dict:
    rates = [int(x) for x in args.rates.split(",") if x]
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    envs = ["-e", "PYTHONHASHSEED=0"]
    rows = []
    for i, rate in enumerate(rates):
        port = free_port()
        name = f"m1-q21-{rate}-{int(time.time() * 1000)}"
        print(f"### M1 二.1 写入 {rate}/s（新容器 {name}，256m、无 cpuset）###", flush=True)
        await docker_run(name, port, args.image, envs, mem=args.mem)
        try:
            if not await wait_ready(port):
                raise RuntimeError("服务未就绪：\n" + (await docker_logs(name))[-1500:])
            await asyncio.sleep(2.0)
            log = await docker_logs(name)
            n, reason = ex.parse_front_count(log)
            row = await run_rate(port, name, rate, args.duration, seed=300 + i)
            row["front_count"] = n
            row["front_reason"] = reason
            rows.append(row)
            save_partial(args.out, {"label": args.label, "image": args.image,
                                    "mem": args.mem, "cpuset": "none",
                                    "duration": args.duration, "rows": rows})
            q = row["queries"]
            print(f"  前端数 {n}（{reason}）  写入实测 {row['write_rate_meas']}/s "
                  f"ws_peak={row['workset_peak_mib']}MiB levels={row['degrade_levels_seen']}",
                  flush=True)
            for k in ("topk_default", "stats_default", "topk_replay"):
                d = q[k]
                print(f"    {k:14s} n={d['n']:3d} p50={d['p50']} p95={d['p95']} "
                      f"p99={d['p99']} max={d['max']}", flush=True)
        finally:
            subprocess.run(["docker", "rm", "-f", name],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return {"label": args.label, "image": args.image, "mem": args.mem,
            "cpuset": "none", "duration": args.duration, "rows": rows}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", required=True)
    ap.add_argument("--rates", default="0,30000,50000,80000")
    ap.add_argument("--duration", type=float, default=120)
    ap.add_argument("--mem", default="256m")
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="m1-q21")
    a = ap.parse_args()
    out = await main_async(a)
    save_partial(a.out, out)
    print(f"\n[done] -> {a.out}", flush=True)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    asyncio.run(main())
