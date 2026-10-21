"""P2d-3 第二.1：不同写入速率下，四类查询的 p50/p95/p99 + SSE + 引擎/前端 CPU%。

写入速率 3/10/14/20 万每秒、每档 120 s、每档一个**新容器**（512m、3 前端、两额度
16 MB、快速压测长尾）。每档在写入速率稳定（/healthz events_ingested 增量达到目标）
后开 120 s 测量窗，窗内并发打四类查询 + 20 个 SSE 订阅：

- topk_default   默认窗口（60s、不带 at）topk url/user（前端本地答）
- stats_default  默认窗口（60s、不带 at）stats（前端本地答；P2d-2 第七节专测口径）
- topk_replay    回放 topk（随机 at + 随机 10~60s 窗口，url/user，转发引擎）
- stats_replay   非默认窗口 stats（随机 10~60s 窗口，转发引擎）

同时从宿主 /proc 读引擎（容器主进程）与各前端（其直接子进程、排除 resource_tracker
与健康检查，s1_measure 口径）的 CPU 时间，算窗口内引擎 CPU% / 前端合计 CPU% /
引擎每条 μs。结果写 JSON（每档一行），供汇总成"行=写入速率、列=四类查询 p99"表。

用法（WSL，服务镜像已构建）：
    python tools/exp/p2d3_query_probe.py --image flowlens:p2d3 \
        --rates 30000,100000,140000,200000 --duration 120 --out results/p2d3/q21.json
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

TOOLS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../flowlens-c/tools
ROOT = os.path.dirname(TOOLS)                                        # .../flowlens-c
sys.path.insert(0, TOOLS)

import s1_measure as s1m   # noqa: E402  复用 find_parse_pids/cpu_times
import experiments as ex   # noqa: E402  复用 parse_front_count

import aiohttp  # noqa: E402

SVC_CP = "0,2,4,6"
LOAD_CP = "8-15"
LOAD_WORKERS = 8
BATCH = 5000
# 每类查询的目标速率（次/秒）。四类合计 ~80 q/s，与 S2 的 rate_per_type=20 同量级。
Q_RATE = 20.0


def stop_loadgen(lg: subprocess.Popen) -> None:
    """停 loadgen：对整个**进程组**发 SIGTERM（主进程 + 全部 worker），宽限 5 s
    后 SIGKILL 兜底。

    必须按进程组杀、不能 `lg.terminate()`（只杀主进程）：loadgen 的 worker 是
    spawn 的 daemon 子进程，主进程被 SIGTERM 后 worker 被 reparent 到 init 继续
    活着，且它们往读端已死的 multiprocessing 队列 put() 永不返回——永不自然
    退出，成为占内存的孤儿进程（P2d-3 二.1 首跑 4 档共漏 22 个、~10 GB RSS）。
    """
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


async def docker_run(container, port, image, envs, mem="512m") -> None:
    cmd = ["docker", "run", "-d", "--name", container,
           f"--memory={mem}", f"--memory-swap={mem}", f"--cpuset-cpus={SVC_CP}",
           *envs, "-p", f"{port}:8080", image]
    proc = await asyncio.create_subprocess_exec(*cmd,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    await proc.communicate()


def free_port() -> int:
    s = socket.socket(); s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]; s.close(); return p


def container_main_pid(name) -> int:
    return int(subprocess.check_output(
        ["docker", "inspect", "-f", "{{.State.Pid}}", name], text=True).strip())


async def wait_ready(port, timeout=40.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if healthz(port).get("ok"):
            return True
        await asyncio.sleep(0.5)
    return False


async def wait_write_rate(port, target, timeout=120.0) -> int:
    """等到 /healthz events_ingested 增量达到目标 90%（每秒）。快速模式每 worker
    预生成模板要 ~12 s，之前没流量，故必须确认写入到位再开测量窗。"""
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
    next_t = time.monotonic()
    while time.monotonic() < deadline:
        url = url_fn(random)
        t0 = time.perf_counter()
        try:
            async with ses.get(url) as r:
                await r.read()
        except Exception:
            pass
        out.append((time.perf_counter() - t0) * 1000.0)
        next_t = max(next_t, time.monotonic())
        await asyncio.sleep(max(0.0, 1.0 / rate - (time.monotonic() - next_t)))


async def sse_loop(base, dur) -> dict:
    """20 个 SSE 订阅（4 组 × 5，与 S2 同），统计帧数与连接错误。"""
    frames, errors = 0, 0
    deadline = time.monotonic() + dur

    async def one(sub_id):
        nonlocal frames, errors
        group = sub_id % 4
        interval_ms = 1000 + group * 1000
        url = (f"{base}/v1/topk/stream?window=60s&k=10&by=url&interval={interval_ms}ms")
        async with aiohttp.ClientSession(trust_env=False) as ses:
            while time.monotonic() < deadline:
                try:
                    async with ses.get(url, headers={"Accept": "text/event-stream"},
                                       timeout=aiohttp.ClientTimeout(total=None, sock_read=20)) as r:
                        async for raw in r.content:
                            if b"event: topk" in raw:
                                frames += 1
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    errors += 1
                    await asyncio.sleep(1.0)

    tasks = [asyncio.create_task(one(i)) for i in range(20)]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True),
                               timeout=dur + 15)
    except asyncio.TimeoutError:
        pass
    for t in tasks:
        if not t.done():
            t.cancel()
    return {"frames": frames, "errors": errors}


def _proc_times(main_pid):
    """返回 ({pid: CPU秒}, front_pids)。s1_measure 口径：主进程 + 直接子进程（前端）。"""
    fronts = s1m.find_parse_pids(main_pid)
    return s1m.cpu_times([main_pid] + fronts), fronts


async def run_rate(port, name, rate, duration, seed) -> dict:
    """单档：起 loadgen 写 rate/s，确认写入到位后开 duration 秒测量窗并发四类查询。"""
    base = f"http://127.0.0.1:{port}"
    main_pid = container_main_pid(name)
    # 起 loadgen（快速模式，长尾），taskset 固定到压测核（与 s1_measure 同口径）
    lg_args = [sys.executable, os.path.join(TOOLS, "loadgen.py"), "--url", base,
               "--workers", str(LOAD_WORKERS), "--conc", "8", "--rate", str(rate),
               # duration + 180：给"快速模式预生成模板（~12 s）+ wait_write_rate
               # 确认写入到位（每 2 s 轮询一次）"留足余量，确保测量窗全程有流量
               "--batch", str(BATCH), "--duration", str(int(duration) + 180),
               "--mode", "长尾", "--seed", str(seed), "--fast", "--fast-templates", "32"]
    if shutil.which("taskset"):
        lg_args = ["taskset", "-c", LOAD_CP] + lg_args
    # start_new_session：loadgen 自成进程组，停时 killpg 能连 worker 一起带走
    lg = subprocess.Popen(lg_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                          start_new_session=True)
    wr = await wait_write_rate(port, rate)
    if wr < int(rate * 0.80):
        print(f"  [warn] 写入 {wr}/s 偏低（目标 {rate}），本档 p99 仅供参考", flush=True)
    # 窗前：CPU 基线 + healthz
    H0 = healthz(port)
    times0, fronts0 = _proc_times(main_pid)
    t0 = time.monotonic()

    topk_def, stats_def, topk_rep, stats_rep = [], [], [], []
    async with aiohttp.ClientSession(trust_env=False,
                                     timeout=aiohttp.ClientTimeout(total=10)) as ses:
        await ses.get(f"{base}/v1/topk?by=url&k=10&window=60s")  # 预热
        # fire 把延迟 append 进各自的 out 列表、返回 None；sse_loop 返回 (frames, errors)。
        # 故 gather 只取 sse_loop 的结果，四个列表由 fire 原地填充。
        sse, _, _, _, _ = await asyncio.gather(
            sse_loop(base, duration),
            fire(ses, lambda r: (f"{base}/v1/topk?by={'url' if r.random() < 0.5 else 'user'}"
                                 f"&k=10&window=60s"), topk_def, duration, Q_RATE),
            fire(ses, lambda r: f"{base}/v1/stats?window=60s", stats_def, duration, Q_RATE),
            fire(ses, lambda r: (f"{base}/v1/topk?by={'url' if r.random() < 0.5 else 'user'}"
                                 f"&k=10&window={r.randint(10, 60)}s&at="
                                 f"{int(time.time() * 1000) - r.randint(10, 300) * 1000}"),
                 topk_rep, duration, Q_RATE),
            fire(ses, lambda r: f"{base}/v1/stats?window={r.randint(10, 60)}s",
                 stats_rep, duration, Q_RATE),
        )
    t1 = time.monotonic()
    H1 = healthz(port)
    times1, fronts1 = _proc_times(main_pid)
    stop_loadgen(lg)

    wall = t1 - t0
    acc = int(H1.get("events_ingested", 0)) - int(H0.get("events_ingested", 0))
    main_sec = times1.get(main_pid, 0) - times0.get(main_pid, 0)
    front_sec = sum(times1.get(p, 0) - times0.get(p, 0)
                    for p in fronts1 if p in times0 and p in times1)
    max_level = max(int(H1.get("degrade", {}).get("level") or 0),
                    int(H0.get("degrade", {}).get("level") or 0))
    return {
        "rate": rate, "write_rate_meas": wr, "window_s": round(wall, 1),
        "accepted": acc,
        "accept_rate": round(acc / (int(rate) * wall), 4) if wall else None,
        "max_degrade_level": max_level,
        "sse": sse,
        "engine_cpu_pct": round(main_sec / wall * 100.0, 1) if wall > 0 else None,
        "front_cpu_pct": round(front_sec / wall * 100.0, 1) if wall > 0 else None,
        "engine_us_ev": round(main_sec / acc * 1e6, 2) if acc > 0 else None,
        "front_procs": len(fronts1),
        "queries": {
            "topk_default": _q(topk_def), "stats_default": _q(stats_def),
            "topk_replay": _q(topk_rep), "stats_replay": _q(stats_rep),
        },
    }


async def main_async(args) -> dict:
    rates = [int(x) for x in args.rates.split(",") if x]
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    # fronts=auto：不注入 FLOWLENS_FRONTENDS，让服务自动判断（P2d-3 八.5 的
    # 256 MB 单进程行用）；此时只记录前端数，不断言期望值。
    auto_fronts = (args.fronts == "auto")
    envs = ["-e", "PYTHONHASHSEED=0"]
    if not auto_fronts:
        envs += ["-e", f"FLOWLENS_FRONTENDS={args.fronts}"]
    if args.max_pending_bytes:
        envs += ["-e", f"FLOWLENS_MAX_PENDING_BYTES={args.max_pending_bytes}"]
    if args.raw_budget_bytes:
        envs += ["-e", f"FLOWLENS_RAW_BUDGET_BYTES={args.raw_budget_bytes}"]
    if args.chunk_events:
        envs += ["-e", f"FLOWLENS_POOLED_CHUNK_EVENTS={args.chunk_events}"]
    rows = []
    for i, rate in enumerate(rates):
        port = free_port()
        name = f"p2d3-q21-{rate}-{int(time.time() * 1000)}"
        print(f"### 二.1 写入 {rate}/s（新容器 {name}）###", flush=True)
        await docker_run(name, port, args.image, envs, mem=args.mem)
        try:
            if not await wait_ready(port):
                raise RuntimeError("服务未就绪：\n" + (await docker_logs(name))[-1500:])
            await asyncio.sleep(2.0)
            # 核对前端数（P2d-3 第零节第 2 条）
            log = await docker_logs(name)
            n, reason = ex.parse_front_count(log)
            if n is None:
                last = log.strip().splitlines()[-1] if log.strip() else ""
                raise RuntimeError(f"读不到前端数（末行：{last!r}）")
            if not auto_fronts and n != args.fronts:
                raise RuntimeError(f"前端数不符：期望 {args.fronts}，实得 {n}（{reason}）")
            print(f"  前端数 {n}（{reason}）", flush=True)
            row = await run_rate(port, name, rate, args.duration, seed=200 + i)
            row["front_count"] = n
            row["front_reason"] = reason
            rows.append(row)
            save_partial(args.out, {"label": args.label, "image": args.image,
                                    "fronts": args.fronts,
                                    "chunk_events": args.chunk_events or 10000,
                                    "duration": args.duration, "rows": rows})
            q = row["queries"]
            print(f"  写入实测 {row['write_rate_meas']}/s  accept={row['accept_rate']} "
                  f"eng={row['engine_cpu_pct']}% front={row['front_cpu_pct']}% "
                  f"us/ev={row['engine_us_ev']} lvl={row['max_degrade_level']} "
                  f"sse={row['sse']}", flush=True)
            for k in ("topk_default", "stats_default", "topk_replay", "stats_replay"):
                d = q[k]
                print(f"    {k:14s} n={d['n']:3d} p50={d['p50']} p95={d['p95']} "
                      f"p99={d['p99']} max={d['max']}", flush=True)
        finally:
            subprocess.run(["docker", "rm", "-f", name],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return {"label": args.label, "image": args.image, "fronts": args.fronts,
            "chunk_events": args.chunk_events or 10000,
            "duration": args.duration, "rows": rows}


def save_partial(out_path: str, obj: dict) -> None:
    """每档完成后立即写盘（长时间实验防中断丢数据；写临时文件再改名，防半截 JSON）。"""
    tmp = out_path + ".partial"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, out_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", required=True)
    ap.add_argument("--rates", default="30000,100000,140000,200000")
    ap.add_argument("--duration", type=float, default=120)
    ap.add_argument("--fronts", default="3",
                    help="前端数（整数）或 auto（不注入 FLOWLENS_FRONTENDS，服务自动判断）")
    ap.add_argument("--mem", default="512m", help="容器内存上限（--memory / --memory-swap）")
    ap.add_argument("--max-pending-bytes", type=int, default=16777216)
    ap.add_argument("--raw-budget-bytes", type=int, default=16777216)
    ap.add_argument("--chunk-events", type=int, default=0,
                    help="FLOWLENS_POOLED_CHUNK_EVENTS（二.2 块大小对比；0=默认 10000）")
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="p2d3-q21")
    a = ap.parse_args()
    if a.fronts != "auto":
        a.fronts = int(a.fronts)
    out = asyncio.run(main_async(a))
    save_partial(a.out, out)
    print(f"\n[done] -> {a.out}", flush=True)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    main()
