"""P2d-3 第二.3：20 万/秒写入时，引擎事件循环延迟的 p50/p99/max 与最长阻塞来源。

起一个 512m / 3 前端 / 两额度 16 MB 的容器，额外设 FLOWLENS_DEBUG=1（只启用
loop_probe 与 /debug 路由，不注入 FLOWLENS_TEST_MEM）；20 万/秒写入 duration 秒，
然后从 docker logs 解析：

- 探针每分钟一行"事件循环延迟 本分钟 p50=… p99=… max=…（n=…）；本分钟最长 5 次: …"
- Ticker 每拍一行"tick 分解 定稿(advance)=… ms 快照=… ms @…"

汇总成 JSON：每分钟 p50/p99/max、全生命周期 top 延迟（来自每分钟的"最长 5 次"）、
定稿与快照耗时的 p50/p99/max（判断最长阻塞来自写入块、定稿还是快照）。

用法（WSL，服务镜像已构建）：
    python tools/exp/p2d3_loop_probe.py --image flowlens:p2d3 --duration 240 \
        --out results/p2d3/q23.json
"""
import argparse
import asyncio
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request

TOOLS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(TOOLS)
sys.path.insert(0, TOOLS)

import s1_measure as s1m   # noqa: E402  复用 find_parse_pids/cpu_times
import experiments as ex   # noqa: E402  复用 parse_front_count

SVC_CP = "0,2,4,6"
LOAD_CP = "8-15"
LOAD_WORKERS = 8
BATCH = 5000
RATE = 200000

LOOP_RE = re.compile(
    r"事件循环延迟 本分钟 p50=([\d.]+) p99=([\d.]+) max=([\d.]+) μs（n=(\d+)）；"
    r"本分钟最长 5 次: (.*)$")
TICK_RE = re.compile(r"tick 分解 定稿\(advance\)=(\d+\.\d+) ms 快照=(\d+\.\d+) ms @(\d+)")
CHUNK_RE = re.compile(r"写入块 (\d+) 条 ingest_parsed=(\d+\.\d+) ms @(\d+)")


def stop_loadgen(lg: subprocess.Popen) -> None:
    """停 loadgen：对整个进程组发 SIGTERM（主进程 + 全部 worker），宽限后 SIGKILL
    兜底。只 `lg.terminate()` 会留下孤儿 worker（往读端已死的多进程队列写、永不
    退出）——见 p2d3_query_probe 的同名函数注释。"""
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


async def docker_run(container, port, image, envs) -> None:
    cmd = ["docker", "run", "-d", "--name", container,
           "--memory=512m", "--memory-swap=512m", f"--cpuset-cpus={SVC_CP}",
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


def parse_loop_log(log_text: str) -> dict:
    """从 docker logs 解析探针每分钟报告、tick 分解行（带 tick 开始时刻）与写入块行。"""
    minutes, fin_ms, snap_ms = [], [], []
    ticks = []      # (start_ms, fin_ms, snap_ms)
    chunks = []     # (start_ms, ms)
    for line in log_text.splitlines():
        m = LOOP_RE.search(line)
        if m:
            # "最长 5 次"里每项形如 "1234μs@1792550000000"
            tops = [(float(a), int(t))
                    for a, t in re.findall(r"([\d.]+)μs@(\d+)", m.group(5))]
            minutes.append({"p50_us": float(m.group(1)), "p99_us": float(m.group(2)),
                            "max_us": float(m.group(3)), "n": int(m.group(4)),
                            "top5": tops})
            continue
        m = TICK_RE.search(line)
        if m:
            fin, snap, ts = float(m.group(1)), float(m.group(2)), int(m.group(3))
            fin_ms.append(fin); snap_ms.append(snap)
            ticks.append((ts, fin, snap))
            continue
        m = CHUNK_RE.search(line)
        if m:
            chunks.append((int(m.group(3)), float(m.group(2))))
    # 全程 p50 ≈ 各分钟 p50 的 n 加权均值；p99/max 取各分钟最大值（上界，够用）
    if minutes:
        tot_n = sum(m["n"] for m in minutes)
        p50_all = sum(m["p50_us"] * m["n"] for m in minutes) / tot_n if tot_n else None
    else:
        p50_all = None
    top_all = sorted((t for m in minutes for t in m["top5"]), key=lambda x: -x[0])[:5]
    return {
        "per_minute": minutes,
        "loop_p50_us": _r2(p50_all),
        "loop_p99_us": _r2(max(m["p99_us"] for m in minutes)) if minutes else None,
        "loop_max_us": _r2(max(m["max_us"] for m in minutes)) if minutes else None,
        "loop_top5": top_all,
        "tick_n": len(fin_ms),
        "finalize_ms": {"p50": _r2(pct(fin_ms, 50)), "p99": _r2(pct(fin_ms, 99)),
                        "max": _r2(max(fin_ms)) if fin_ms else None},
        "snapshot_ms": {"p50": _r2(pct(snap_ms, 50)), "p99": _r2(pct(snap_ms, 99)),
                        "max": _r2(max(snap_ms)) if snap_ms else None},
        "chunk_n": len(chunks),
        "chunk_ms": {"p50": _r2(pct([c[1] for c in chunks], 50)),
                     "p99": _r2(pct([c[1] for c in chunks], 99)),
                     "max": _r2(max((c[1] for c in chunks), default=None)),
                     "n": len(chunks)},
        "attribution": attribute_top(top_all, ticks, chunks),
    }


def attribute_top(top5, ticks, chunks) -> list[dict]:
    """把最长的几次事件循环延迟归因到"写入块 / 定稿(advance) / 快照 / 其他"。

    一次延迟 t 落在某 tick 的 [start, start + 定稿 + 快照] 窗内 → 归因该 tick 的
    定稿或快照（取耗时大者）；落在某写入块 [start, start + 块耗时] 窗内 → 写入块；
    都不落 → 其他（查询/SSE/调度等）。延迟时刻 ≈ 回调实际执行时刻，其阻塞发生在
    略早的时段，窗各放宽 50 ms 容忍边界。
    """
    out = []
    for us, ts in top5:
        reason = "其他"
        for s, fin, snap in ticks:  # 单位都是 ms
            if s - 50 <= ts <= s + (fin + snap) + 50:
                reason = f"tick（定稿 {fin:.1f}ms / 快照 {snap:.1f}ms）"
                break
        if reason == "其他":
            for s, ms in chunks:  # s 是块**结束**时刻（日志在 ingest_parsed 后打）、ms 是耗时
                if s - ms - 50 <= ts <= s + 50:
                    reason = f"写入块（{ms:.1f}ms）"
                    break
        out.append({"us": us, "ts": ts, "reason": reason})
    return out


async def main_async(args) -> dict:
    envs = ["-e", "PYTHONHASHSEED=0", "-e", f"FLOWLENS_FRONTENDS={args.fronts}",
            "-e", "FLOWLENS_DEBUG=1",
            "-e", f"FLOWLENS_MAX_PENDING_BYTES={args.max_pending_bytes}",
            "-e", f"FLOWLENS_RAW_BUDGET_BYTES={args.raw_budget_bytes}"]
    port = free_port()
    name = f"p2d3-q23-{int(time.time() * 1000)}"
    print(f"### 二.3 事件循环探针（容器 {name}）###", flush=True)
    await docker_run(name, port, args.image, envs)
    try:
        if not await wait_ready(port):
            raise RuntimeError("服务未就绪：\n" + (await docker_logs(name))[-1500:])
        await asyncio.sleep(2.0)
        log = await docker_logs(name)
        n, reason = ex.parse_front_count(log)
        if n is None:
            last = log.strip().splitlines()[-1] if log.strip() else ""
            raise RuntimeError(f"读不到前端数（末行：{last!r}）")
        if args.fronts is not None and n != args.fronts:
            raise RuntimeError(f"前端数不符：期望 {args.fronts}，实得 {n}（{reason}）")
        print(f"  前端数 {n}（{reason}）；FLOWLENS_DEBUG=1（loop_probe 启用）", flush=True)
        main_pid = container_main_pid(name)
        # 起 loadgen（快速模式，长尾，20 万/秒）
        lg_args = [sys.executable, os.path.join(TOOLS, "loadgen.py"),
                   "--url", f"http://127.0.0.1:{port}",
                   "--workers", str(LOAD_WORKERS), "--conc", "8", "--rate", str(RATE),
                   "--batch", str(BATCH),
                   "--duration", str(int(args.duration) + 60),
                   "--mode", "长尾", "--seed", "300", "--fast", "--fast-templates", "32"]
        if shutil.which("taskset"):
            lg_args = ["taskset", "-c", LOAD_CP] + lg_args
        lg = subprocess.Popen(lg_args, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            wr = await wait_write_rate(port, RATE)
            print(f"  写入实测 {wr}/s，采集 {args.duration} s …", flush=True)
            # 窗前 CPU 基线（窗口 = 满速写入期）
            fronts0 = s1m.find_parse_pids(main_pid)
            times0 = s1m.cpu_times([main_pid] + fronts0)
            H0 = healthz(port)
            t0 = time.monotonic()
            await asyncio.sleep(args.duration)
            t1 = time.monotonic()
            H1 = healthz(port)
            fronts1 = s1m.find_parse_pids(main_pid)
            times1 = s1m.cpu_times([main_pid] + fronts1)
        finally:
            stop_loadgen(lg)
        wall = t1 - t0
        acc = int(H1.get("events_ingested", 0)) - int(H0.get("events_ingested", 0))
        main_sec = times1.get(main_pid, 0) - times0.get(main_pid, 0)
        front_sec = sum(times1.get(p, 0) - times0.get(p, 0)
                        for p in fronts1 if p in times0 and p in times1)
        out = {
            "label": args.label, "image": args.image, "front_count": n,
            "front_reason": reason, "write_rate_meas": wr,
            "duration_s": round(wall, 1),
            "accepted": acc,
            "engine_cpu_pct": round(main_sec / wall * 100.0, 1) if wall > 0 else None,
            "front_cpu_pct": round(front_sec / wall * 100.0) if wall > 0 else None,
            "engine_us_ev": round(main_sec / acc * 1e6, 2) if acc > 0 else None,
        }
        out.update(parse_loop_log(await docker_logs(name)))
        return out
    finally:
        subprocess.run(["docker", "rm", "-f", name],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", required=True)
    ap.add_argument("--duration", type=float, default=240)
    ap.add_argument("--fronts", type=int, default=3)
    ap.add_argument("--max-pending-bytes", type=int, default=16777216)
    ap.add_argument("--raw-budget-bytes", type=int, default=16777216)
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="p2d3-q23")
    a = ap.parse_args()
    out = asyncio.run(main_async(a))
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n[done] -> {a.out}", flush=True)
    # 汇总行（命令只打印汇总）
    lm = out.get("per_minute", [])
    if lm:
        print(f"  事件循环延迟 全程 p99={out['loop_p99_us']}μs max={out['loop_max_us']}μs "
              f"（{len(lm)} 个分钟报告）", flush=True)
        print(f"  tick 分解 定稿 p99={out['finalize_ms']['p99']}ms "
              f"快照 p99={out['snapshot_ms']['p99']}ms", flush=True)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    main()
