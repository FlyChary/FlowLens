"""P1b S1 爬坡测量（严格串行版）：单容器，每档档前/档后各同步读一次 /healthz 与宿主
/proc CPU 时间，差值即该档。运行在 WSL 宿主机（读 CPU 不进容器）。

每档（严格串行）：
  档前  H0=healthz；C0=主进程+各解析进程 CPU 时间；t0=monotonic
  loadgen --rate R --duration 20，阻塞等退出，读最后汇总行
  sleep 1（让最后几个请求写完）
  档后  H1=healthz；C1=CPU 时间；t1=monotonic
  接受条数      = H1.events_ingested − H0.events_ingested
  在途/原始拒绝 = H1 与 H0 的 ingest_rejected、raw_rejected 差
  主进程 CPU 秒 = C1.main − C0.main；解析 CPU 秒 = 各解析进程差值之和
  主进程每条 μs = 主进程 CPU 秒 ÷ 接受条数 × 1e6   ← 主要指标（分子分母同窗口，不受窗口含
                 爬坡/收尾影响）
  主进程 CPU%   = 主进程 CPU 秒 ÷ (t1−t0) × 100（含爬坡/收尾一两秒，略偏低）
  交叉核对：接受条数 与 loadgen 汇总 accepted 应在 2% 内，超出则标出
"""
import argparse
import asyncio
import json
import os
import socket
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
CLK = os.sysconf("SC_CLK_TCK")  # Linux 通常 100Hz；/proc stat 的 utime/stime 单位


def _free_port() -> int:
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def _run(cmd: list[str]) -> str:
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    out, _ = await proc.communicate()
    return out.decode("utf-8", errors="replace")


# ---- 宿主 /proc（不进容器读 CPU；修正 #1） ----

def _stat(pid: int) -> tuple[float, int] | None:
    """读 /proc/<pid>/stat，返回 (utime+stime 秒, ppid)；读不到返回 None。
    按最后一个 ')' 之后计数：utime=第 12 个(0 基 11)、stime=第 13 个、ppid=第 2 个。"""
    try:
        t = open(f"/proc/{pid}/stat", encoding="utf-8").read()
    except OSError:
        return None
    r = t.rfind(")")
    if r < 0:
        return None
    f = t[r + 1:].split()
    if len(f) < 13:
        return None
    try:
        return ((int(f[11]) + int(f[12])) / CLK, int(f[1]))
    except (ValueError, IndexError):
        return None


def _cmdline(pid: int) -> str:
    try:
        return open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode("utf-8", "replace")
    except OSError:
        return ""


def find_parse_pids(main_pid: int) -> list[int]:
    """主进程的直接子进程里，排除 resource_tracker（修正 #2）与容器健康检查，即解析 worker。"""
    out = []
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        st = _stat(int(d))
        if not st or st[1] != main_pid:
            continue
        cl = _cmdline(int(d))
        if "resource_tracker" in cl:
            continue
        if "healthz" in cl or "urllib.request" in cl:  # 容器内健康检查（非解析进程）
            continue
        out.append(int(d))
    return out


def cpu_times(pids: list[int]) -> dict[int, float]:
    """{pid: CPU 秒}（utime+stime）；读不到的跳过。"""
    out = {}
    for p in pids:
        st = _stat(p)
        if st:
            out[p] = st[0]
    return out


async def container_main_pid(container: str) -> int | None:
    raw = await _run(["docker", "inspect", "-f", "{{.State.Pid}}", container])
    return int(raw.strip() or 0) or None


async def healthz(url: str) -> dict:
    import aiohttp
    try:
        async with aiohttp.ClientSession(trust_env=False,
                                         timeout=aiohttp.ClientTimeout(total=5)) as s:
            async with s.get(url.rstrip("/") + "/healthz") as r:
                if r.status == 200:
                    return await r.json()
    except Exception:
        pass
    return {}


async def wait_ready(url: str, timeout: float = 40.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (await healthz(url)).get("ok"):
            return True
        await asyncio.sleep(0.5)
    return False


async def run_loadgen(url: str, rate: int, dur: float, batch: int,
                      workers: int, load_cp: str, seed: int) -> dict:
    """跑一次 loadgen（taskset 固定到 load 核），阻塞等退出，返回最后汇总行 dict。"""
    args = [sys.executable, str(TOOLS / "loadgen.py"), "--url", url,
            "--workers", str(workers), "--conc", "8", "--rate", str(rate),
            "--batch", str(batch), "--duration", str(dur), "--mode", "长尾", "--seed", str(seed)]
    if os.path.exists("/usr/bin/taskset"):
        args = ["taskset", "-c", load_cp] + args
    env = dict(os.environ, NO_PROXY="localhost,127.0.0.1", PYTHONHASHSEED="0")
    proc = await asyncio.create_subprocess_exec(*args, env=env, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.DEVNULL)
    assert proc.stdout is not None
    summary = {}
    async for raw in proc.stdout:
        line = raw.decode("utf-8", "replace").strip()
        if line:
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            if o.get("type") == "summary":
                summary = o
    await proc.wait()
    return summary


async def mem_sample(url: str, stop: asyncio.Event) -> dict:
    """每秒读一次 healthz，返回本档窗口内的：工作集峰值（memory_bytes）、原始读数峰值
    （memory_current_bytes）、达到的最大降级级别（degrade.level）。只用于记录，不参与其他计算。"""
    ws = cur = 0
    max_level = 0
    while not stop.is_set():
        d = await healthz(url)
        for k in ("memory_bytes", "memory_current_bytes"):
            v = d.get(k)
            if isinstance(v, (int, float)):
                if k == "memory_bytes":
                    ws = max(ws, v)
                else:
                    cur = max(cur, v)
        lvl = (d.get("degrade") or {}).get("level")
        if isinstance(lvl, int):
            max_level = max(max_level, lvl)
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
    return {"ws_peak": ws, "memcur_peak": cur, "max_level": max_level}


async def measure_level(url, container, main_pid, rate, dur, batch, workers, load_cp, seed):
    """严格串行的一档。返回本档指标 dict。"""
    H0 = await healthz(url)
    parse0 = find_parse_pids(main_pid)
    C0 = cpu_times([main_pid] + parse0)
    stop = asyncio.Event()
    mt = asyncio.create_task(mem_sample(url, stop))
    t0 = time.monotonic()
    summary = await run_loadgen(url, rate, dur, batch, workers, load_cp, seed)
    await asyncio.sleep(1.0)                       # 让最后几个请求写完
    H1 = await healthz(url)
    stop.set(); mem = await mt
    parse1 = find_parse_pids(main_pid)
    C1 = cpu_times([main_pid] + parse1)
    t1 = time.monotonic()

    acc = int(H1.get("events_ingested", 0)) - int(H0.get("events_ingested", 0))
    infl = int(H1.get("ingest_rejected", 0)) - int(H0.get("ingest_rejected", 0))
    rawr = int(H1.get("raw_rejected", 0)) - int(H0.get("raw_rejected", 0))
    main_sec = C1.get(main_pid, 0) - C0.get(main_pid, 0)
    parse_sec = sum(C1.get(p, 0) - C0.get(p, 0) for p in parse1 if p in C0 and p in C1)
    wall = t1 - t0
    acc_lg = int(summary.get("accepted", 0))
    sent_lg = int(summary.get("sent", 0) or 0)
    cross = abs(acc - acc_lg) / acc * 100 if acc else None
    return {
        "rate": rate, "sent_target": int(rate * dur), "sent_lg": sent_lg,
        "sent_rate": round(sent_lg / (rate * dur), 4) if rate else None,
        "acc_h": acc, "acc_lg": acc_lg,
        "errors_lg": int(summary.get("errors", 0) or 0),
        "cross_pct": round(cross, 1) if cross is not None else None,
        "cross_ok": cross is not None and cross <= 2.0,
        "accept_rate": round(acc / sent_lg, 4) if sent_lg else None,
        "inflight_rej": infl, "raw_rej": rawr,
        "max_degrade_level": mem["max_level"],          # 本档窗口内达到的最大降级级别
        "main_cpu_pct": round(main_sec / wall * 100, 1) if wall > 0 else None,
        "main_us_ev": round(main_sec / acc * 1e6, 2) if acc > 0 else None,
        "parse_cpu_pct": round(parse_sec / wall * 100, 1) if wall > 0 else None,
        "loadgen_cpu_pct": summary.get("client_cpu_pct"),  # 压测进程合计（worker 之和 ÷ 时长）
        "parse_procs": len(parse1),
        "ws_peak_mb": round(mem["ws_peak"] / 1048576, 1),
        "memcur_peak_mb": round(mem["memcur_peak"] / 1048576, 1),
        "wall_s": round(wall, 1),
    }


async def main_async(args) -> dict:
    port = args.port or _free_port()
    name = f"flowlens-p1b-{int(time.time() * 1000)}"
    url = f"http://127.0.0.1:{port}"
    if args.pool == "auto":
        # 不注入 FLOWLENS_PARSE_WORKERS，让服务按可用核数自动决定（P1c 二.5 单核对照：
        # 1 核时应自动退回单进程路径，解析进程数为 0）
        expect = None
        env = ["-e", "PYTHONHASHSEED=0"]
    else:
        expect = args.parse_workers if args.pool == "on" else 0
        env = ["-e", "PYTHONHASHSEED=0",
               "-e", f"FLOWLENS_PARSE_WORKERS={args.parse_workers if args.pool == 'on' else 0}"]
    if args.max_pending_bytes:
        env += ["-e", f"FLOWLENS_MAX_PENDING_BYTES={args.max_pending_bytes}"]
    if args.raw_budget_bytes:
        env += ["-e", f"FLOWLENS_RAW_BUDGET_BYTES={args.raw_budget_bytes}"]
    await _run(["docker", "run", "--rm", "-d", "--name", name, f"--memory={args.mem}",
                f"--memory-swap={args.mem}", f"--cpuset-cpus={args.svc_cp}", *env,
                "-p", f"{port}:8080", args.image])
    try:
        if not await wait_ready(url):
            raise RuntimeError("服务未就绪：\n" + (await _run(["docker", "logs", name]))[-2000:])
        await asyncio.sleep(2.0)  # 等池预热稳定
        main_pid = await container_main_pid(name)
        parse = find_parse_pids(main_pid)
        if expect is not None and len(parse) != expect:
            raise RuntimeError(f"解析进程数不符：期望 {expect}，实得 {len(parse)}，pids={parse}")
        print(f"[setup] 解析进程数={len(parse)}（期望 {expect if expect is not None else '自动'}）",
              flush=True)
        rows = []
        for i, rate in enumerate(args.rates):
            print(f"[level {i + 1}/{len(args.rates)}] rate={rate} ...", flush=True)
            row = await measure_level(url, name, main_pid, rate, args.level_sec, args.batch,
                                      args.load_workers, args.load_cp, 100 + i)
            rows.append(row)
            print(f"  acc_h={row['acc_h']} sent={row['sent_lg']}/{row['sent_target']} "
                  f"acc={row['accept_rate']} infl={row['inflight_rej']} raw={row['raw_rej']} "
                  f"maxlvl={row['max_degrade_level']} main_cpu={row['main_cpu_pct']}% "
                  f"us/ev={row['main_us_ev']} parse_cpu={row['parse_cpu_pct']}% "
                  f"load_cpu={row['loadgen_cpu_pct']}% ws={row['ws_peak_mb']}MiB "
                  f"memcur={row['memcur_peak_mb']}MiB cross={row['cross_pct']}% ok={row['cross_ok']}",
                  flush=True)
        result = {"label": args.label, "image": args.image, "pool": args.pool, "expect_parse": expect,
                  "mem": args.mem,
                  "max_pending_bytes": args.max_pending_bytes, "raw_budget_bytes": args.raw_budget_bytes,
                  "svc_cp": args.svc_cp, "load_cp": args.load_cp, "load_workers": args.load_workers,
                  "level_sec": args.level_sec, "batch": args.batch, "port": port, "rows": rows}
        p = Path(args.out); p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n[done] -> {args.out}", flush=True)
        return result
    finally:
        await _run(["docker", "rm", "-f", name])


def main() -> None:
    ap = argparse.ArgumentParser(description="P1b S1 爬坡测量（严格串行版）")
    ap.add_argument("--image", required=True)
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--svc-cp", default="0,2,4,6")
    ap.add_argument("--mem", default="256m", help="容器内存上限（评测=256m；本地分离实验可放宽）")
    ap.add_argument("--load-cp", default="8-15")
    ap.add_argument("--load-workers", type=int, default=8)
    ap.add_argument("--rates", default="40000,60000,80000")
    ap.add_argument("--level-sec", type=int, default=20)
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--pool", choices=["on", "off", "auto"], default="off")
    ap.add_argument("--parse-workers", type=int, default=3)
    ap.add_argument("--max-pending-bytes", type=int, default=0, help="在途额度（0=默认 4MB）")
    ap.add_argument("--raw-budget-bytes", type=int, default=0, help="原始请求体额度（0=默认 8MB）")
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="")
    args = ap.parse_args()
    args.rates = [int(x) for x in args.rates.split(",") if x]
    args.label = args.label or f"pool_{args.pool}_pend{args.max_pending_bytes}_raw{args.raw_budget_bytes}"
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
