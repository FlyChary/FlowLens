"""实验运行脚本（T10b 第二节）：端到端场景 S1/S2/S3/S5。

每个场景启动一个全新的服务（docker 容器或本机进程），跑压测与查询，逐秒采样
/healthz 与服务 CPU，结束后把结果写进 results/<日期时间>/<场景>/。服务固定在一个
CPU 核上（单线程服务一个核足够），压测进程固定在其他核上，互不争抢。

场景：
- S1 吞吐爬坡：发送速率阶梯上升（默认 5 万~30 万、每级 20 秒、每批 5000），不压缩
  与 gzip 各跑一次；判"最大可持续吞吐"（接受率≥99%、无 503、写入 p99<500ms 的最高级）。
- S2 混合负载：写入保持 S1 最大可持续的 70%，同时跑查询（默认窗口 topk url/user、
  随机 at+window 回放 topk、stats）与 4 组共 20 个 SSE 订阅，持续 120 秒。
- S3 精度：指定分布（默认长尾）3 万/秒 5 分钟并存档；运行期每秒记录默认窗口
  topk+stats 的服务响应；结束后（数据仍在保留期）抽 200 个已定稿区间与精确解比较。
- S4 攻击流量（C3）：全随机分布（每个 URL 与用户都不重复）3 万/秒 10 分钟 + 停发
  观察 60 秒。记录工作集/RssAnon 随时间、降级级别、接受率、trim 回收量与"每分钟
  一行"汇总表——对抗性流量下内存有界的直接证据。
- S5 过载与恢复：以 S1 最大可持续的 1.5 倍发送 60 秒，停发后再观察 60 秒，记录降级
  级别随时间变化与恢复时间。

用法：
  python tools/experiments.py --target docker --scenarios S1 S2 S3 S5
  python tools/experiments.py --target local  --scenarios S1 --port 9000

结果目录与文件格式见 docs/EXPERIMENTS_FORMAT.md。
"""

import argparse
import asyncio
import datetime
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time

import orjson

# 让脚本能 import flowlens 与 tools 里的兄弟模块（loadgen / flowgen / exact）
sys.path.insert(0, str(os.path.dirname(os.path.abspath(__file__))))

import exact  # noqa: E402
import loadgen  # noqa: E402

# ---- 默认参数（可被命令行覆盖）----
DEFAULT_PORT = 9000                 # 避开 WSL2 保留的 80xx 段
DEFAULT_BATCH = 5000
S1_RATES = [50_000, 100_000, 150_000, 200_000, 250_000, 300_000]
S1_LEVEL_SEC = 20                   # S1 每级持续秒数
S2_SEC = 120                         # S2 混合负载持续秒数
S2_WRITE_FRAC = 0.70                 # S2 写入速率 = S1 最大可持续 × 0.7
S3_RATE = 30_000                     # S3 固定 3 万/秒
S3_SEC = 300                         # S3 5 分钟
S3_SAMPLES = 200                     # S3 结束时抽 200 个已定稿区间
S4_RATE = 30_000                     # S4 攻击流量：3 万/秒
S4_SEC = 600                         # S4 持续 10 分钟（对抗性流量下内存有界的直接证据）
SUST_RATE = 30_000                   # SUST 持续负载（C3 对比实验组 2~4 用）：长尾 3 万/秒
SUST_SEC = 300                       # SUST 5 分钟（与 C2 基线同参数）
SUST_TAIL = 60                       # SUST 停发后观察 60 秒（看回落）
S5_OVERLOAD_FRAC = 1.5               # S5 过载 = S1 最大可持续 × 1.5
S5_OVERLOAD_SEC = 60
S5_OBSERVE_SEC = 60

# 核的分配（写入 meta，供判读）。docker 目标下 --cpuset-cpus 用 WSL 宿主机核号。
# P1（多核）由 --svc-cpus/--load-cpus/--load-workers 覆盖（main() 里改这几个全局）。
SVC_CORE = "0"
LOAD_CORES = "1-6"
# 容器内存上限（--svc-mem 覆盖；P2c 的 512 MB 组用它）
SVC_MEM = "256m"
# 服务容器是否加 --cpuset-cpus（§7 多核对照：False = 不加、让服务看到宿主全部核，
# 与评测方最可能的启动方式一致）。main() 里由 --no-cpuset 置 False。
NO_CPUSET = False
AUX_CORE = "7"
LOAD_WORKERS = 4
# P2d-3 第零节第 2 条：预期前端数（--expect-fronts；None = 只记录不核对）。
# 第五/六节（自动判断、不注入 env）用它强制核对前端数确为 0。
EXPECT_FRONT: int | None = None


def expand_cpus(spec: str) -> list[int]:
    """把 "0,2,4,6" 或 "0-3,8" 这样的核规格展开成核号列表。"""
    out: list[int] = []
    for part in spec.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


def svc_ncpu() -> int:
    """服务容器可用的逻辑核数（用于 cgroup CPU 上限：多核时 CPU% 可超过 100）。"""
    return len(expand_cpus(SVC_CORE))

# 保留期（秒），与 timewin.RETENTION_SEC 一致——S3 抽样区间必须落在保留期内
RETENTION_SEC = 600
# 迟到上限（毫秒），与 timewin.LATENESS_MS 一致——"已定稿"秒的边界
LATENESS_MS = 60_000


# ===================== 纯函数（可单测） =====================

def pctl(values, p: float) -> float | None:
    """百分位（线性插值）；空列表返回 None。与 loadgen.pctl 同口径。"""
    if not values:
        return None
    import numpy as np
    return float(np.percentile(values, p))


def level_ok(level: dict) -> bool:
    """某一级是否"可持续"：接受率≥99%、无 503、写入 p99 < 500 ms。"""
    if level.get("accept_ratio", 0.0) < 0.99:
        return False
    if level.get("ratio_503", 0.0) > 0:
        return False
    p99 = level.get("lat_p99_ms")
    if p99 is None or p99 >= 500.0:
        return False
    return True


def max_sustainable(levels: list[dict]) -> int | None:
    """最大可持续吞吐：满足 level_ok 的最高发送速率（事件/秒）；没有则 None。"""
    best = None
    for lv in levels:
        if level_ok(lv):
            best = lv["rate"]
    return best


def mem_mb(b: int | None) -> float | None:
    return None if b is None else round(b / 1_048_576, 2)


def mem_timeline(healthz_samples: list[dict]) -> dict:
    """从逐秒 healthz 里提取工作集（memory_bytes）与原始读数（memory_current_bytes）
    的起始/峰值/结束值（MiB），以及降级级别是否变化、到过哪些级别。

    供汇报用（T10b 追加要求）：每个场景的工作集随时间变化 + 降级级别是否变化。
    """
    ws, cur, levels, threads = [], [], [], []
    for s in healthz_samples:
        hz = s.get("healthz") or {}
        if "memory_bytes" in hz:
            ws.append(hz["memory_bytes"])
        if "memory_current_bytes" in hz:
            cur.append(hz["memory_current_bytes"])
        lvl = (hz.get("degrade") or {}).get("level")
        if lvl is not None:
            levels.append(lvl)
        if s.get("threads") is not None:
            threads.append(s["threads"])
    def _trip(arr):
        if not arr:
            return {"start_mb": None, "peak_mb": None, "end_mb": None}
        return {"start_mb": mem_mb(arr[0]), "peak_mb": mem_mb(max(arr)),
                "end_mb": mem_mb(arr[-1])}
    return {
        "working_set": _trip(ws),
        "raw_current": _trip(cur),
        "degrade_changed": len(set(levels)) > 1,
        "degrade_levels_seen": sorted(set(levels)) if levels else [],
        "peak_degrade_level": max(levels) if levels else 0,
        # 主进程线程数（诊断可见核数→OpenBLAS 线程→内存）
        "threads_peak": max(threads) if threads else None,
    }


def proc_timeline(samples: list[dict]) -> dict:
    """从监控采样的 `procs` 字段汇总每类进程（main/各 parse 子进程）的 CPU 峰值与
    常驻内存峰值（MiB），P1 判"瓶颈在服务还是压测端"与 S4 报各进程常驻内存用。

    返回 {"main": {cpu_peak_pct, rss_peak_mb}, "parse": [每子进程 {pid, cpu_peak_pct,
    rss_peak_mb}], "parse_count": int}；无 procs 数据时返回空结构。
    """
    main = {"cpu_peak_pct": None, "rss_peak_mb": None}
    by_pid: dict[int, dict] = {}
    for s in samples:
        for p in (s.get("procs") or []):
            if p["name"] == "main":
                cpu = p.get("cpu_pct")
                rss = p.get("rss_mb")
                if cpu is not None and (main["cpu_peak_pct"] is None or cpu > main["cpu_peak_pct"]):
                    main["cpu_peak_pct"] = cpu
                if rss is not None and (main["rss_peak_mb"] is None or rss > main["rss_peak_mb"]):
                    main["rss_peak_mb"] = rss
            else:
                d = by_pid.setdefault(p["pid"], {"pid": p["pid"],
                                                 "cpu_peak_pct": None,
                                                 "rss_peak_mb": None})
                if p.get("cpu_pct") is not None and (
                        d["cpu_peak_pct"] is None or p["cpu_pct"] > d["cpu_peak_pct"]):
                    d["cpu_peak_pct"] = p["cpu_pct"]
                if p.get("rss_mb") is not None and (
                        d["rss_peak_mb"] is None or p["rss_mb"] > d["rss_peak_mb"]):
                    d["rss_peak_mb"] = p["rss_mb"]
    return {"main": main, "parse": sorted(by_pid.values(), key=lambda x: x["pid"]),
            "parse_count": len(by_pid)}


def cpu_window_pct(samples: list[dict]) -> dict:
    """整窗口（一段采样）的引擎（main）与前端合计（parse）CPU%（占单核 %）。

    与 run_sustained 每分钟行的算法同口径：逐秒 cpu_pct × 与上一采样的间隔累加成
    CPU 秒，再 ÷ 窗口时长。procs 里 name=="main" 是引擎、其余（name=="parse"，
    父进程号 = 主进程、排除 resource_tracker 与健康检查）是前端（P2d-1 口径）。
    单进程模式（前端数 0）时 front_cpu_pct 为 0.0。
    """
    main_s = front_s = 0.0
    prev_t = None
    for s in samples:
        procs = s.get("procs") or []
        main_cpu = sum(p["cpu_pct"] for p in procs
                       if p["name"] == "main" and p.get("cpu_pct") is not None)
        front_cpu = sum(p["cpu_pct"] for p in procs
                        if p["name"] != "main" and p.get("cpu_pct") is not None)
        if prev_t is not None:
            dt_s = (s["t"] - prev_t) / 1000.0
            main_s += main_cpu / 100.0 * dt_s
            front_s += front_cpu / 100.0 * dt_s
        prev_t = s["t"]
    span = (samples[-1]["t"] - samples[0]["t"]) / 1000.0 if len(samples) > 1 else 0.0
    return {"engine_cpu_pct": round(main_s / span * 100.0, 1) if span > 0 else None,
            "front_cpu_pct": round(front_s / span * 100.0, 1) if span > 0 else None}


def parse_loadgen_lines(lines: list[str]) -> tuple[dict | None, list[dict]]:
    """解析 loadgen 的标准输出：逐秒行（type=second）与汇总行（type=summary）。

    返回 (summary_dict_or_None, per_second_list)。loadgen 逐秒行已含
    sent/accepted/rejected/errors/lat_p50/p95/p99。
    """
    seconds, summary = [], None
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        try:
            d = orjson.loads(ln)
        except orjson.JSONDecodeError:
            continue
        if d.get("type") == "second":
            seconds.append(d)
        elif d.get("type") == "summary":
            summary = d
    return summary, seconds


def merge_timeline(healthz_samples: list[dict], loadgen_seconds: list[dict]
                   ) -> list[dict]:
    """把逐秒 healthz 采样与 loadgen 逐秒统计按"秒"合并成 timeline 行。

    每个在 loadgen 或 healthz 里出现过的秒都有一行：
    {"t": 秒, "sent", "accepted", "rejected", "errors", "lat_p50/p95/p99_ms",
     "healthz": {...}}（该秒的 healthz 完整内容；缺则 None）。
    """
    hz_by_sec = {s["t"] // 1000: s.get("healthz") for s in healthz_samples}
    rss_by_sec = {s["t"] // 1000: s for s in healthz_samples}
    secs = sorted(set(hz_by_sec) | {d["t"] for d in loadgen_seconds})
    lg_by_sec = {d["t"]: d for d in loadgen_seconds}
    out = []
    for sec in secs:
        lg = lg_by_sec.get(sec)
        src = rss_by_sec.get(sec) or {}
        # 逐秒的 main / 解析进程 CPU（占单核 %）与解析进程数，P1 判瓶颈在哪端
        procs = src.get("procs") or []
        main_cpu = sum(p["cpu_pct"] for p in procs if p["name"] == "main"
                       and p.get("cpu_pct") is not None)
        parse_cpu = sum(p["cpu_pct"] for p in procs if p["name"] == "parse"
                        and p.get("cpu_pct") is not None)
        parse_n = sum(1 for p in procs if p["name"] == "parse")
        row = {"t": sec, "sent": 0, "accepted": 0, "rejected": 0, "errors": 0,
               "lat_p50_ms": None, "lat_p95_ms": None, "lat_p99_ms": None,
               "rss_anon_mb": mem_mb(src.get("rss_anon_bytes")),
               "vm_rss_mb": mem_mb(src.get("vm_rss_bytes")),
               "main_cpu_pct": round(main_cpu, 1) or None,
               "parse_cpu_pct": round(parse_cpu, 1) or None,
               "parse_n": parse_n or None,
               "healthz": hz_by_sec.get(sec)}
        if lg is not None:
            row["sent"] = lg["sent"]; row["accepted"] = lg["accepted"]
            row["rejected"] = lg["rejected"]; row["errors"] = lg["errors"]
            row["lat_p50_ms"] = lg.get("lat_p50_ms")
            row["lat_p95_ms"] = lg.get("lat_p95_ms")
            row["lat_p99_ms"] = lg.get("lat_p99_ms")
        out.append(row)
    return out


# ===================== 服务生命周期 =====================

class Service:
    """一个服务实例（docker 容器或本机进程），带 stop()。"""

    def __init__(self, kind: str, port: int, *, container: str | None = None,
                 proc: asyncio.subprocess.Process | None = None,
                 pid: int | None = None):
        self.kind = kind            # "docker" | "local"
        self.port = port
        self.container = container
        self.proc = proc
        self.pid = pid              # local 目标的 python 进程 pid（读 /proc CPU 用）
        self._clk_tck = 100
        # P2d-3 第零节第 2 条：docker 目标启动后从 logs 读到的前端数（local 为 None）
        self.front_count: int | None = None
        self.front_reason: str = ""

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if self.kind == "docker" and self.container:
            await _run(["docker", "stop", "-t", "10", self.container])
        elif self.kind == "local" and self.proc is not None:
            try:
                self.proc.terminate()
                await asyncio.wait_for(self.proc.wait(), timeout=15)
            except (ProcessLookupError, asyncio.TimeoutError):
                self.proc.kill()
                await self.proc.wait()


async def _run(cmd: list[str]) -> str:
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    out, _ = await proc.communicate()
    return out.decode(errors="replace")


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def start_service(target: str, port: int, image: str, workdir: str,
                        extra_env: dict | None = None) -> Service:
    """启动一个全新服务，等 /healthz 就绪后返回。

    extra_env：额外注入服务进程的环境变量（C3 实验组用它开/关 malloc_trim、
    驻留、在途额度）。docker 目标翻译成 -e K=V，local 目标合进子进程 env。
    """
    extra_env = extra_env or {}
    env = dict(os.environ, PYTHONHASHSEED="0", NO_PROXY="localhost,127.0.0.1",
               **extra_env)
    if target == "docker":
        name = "flowlens-exp-" + datetime.datetime.now().strftime("%H%M%S%f")
        env_args = []
        for k, v in extra_env.items():
            env_args += ["-e", f"{k}={v}"]
        cpuset_args = [] if NO_CPUSET else [f"--cpuset-cpus={SVC_CORE}"]
        proc = await asyncio.create_subprocess_exec(
            "docker", "run", "--rm", "-d", "--name", name,
            f"--memory={SVC_MEM}", f"--memory-swap={SVC_MEM}",
            *cpuset_args,
            *env_args,
            "-p", f"{port}:8080", image,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await proc.wait()
        svc = await _wait_ready(Service("docker", port, container=name), env)
        # P2d-3 第零节第 2 条：启动后从 docker logs 确认真实前端数（记进 meta、供核对）
        n, why = await read_front_count(name)
        svc.front_count, svc.front_reason = n, why
        print(f"[start] 容器 {name} 前端数 {n}（{why}）", flush=True)
        return svc
    else:
        # local：taskset 固定到 SVC_CORE；__main__ 监听 --port
        cmd = (["taskset", "-c", SVC_CORE, sys.executable, "-m", "flowlens",
                "--port", str(port)] if _has("taskset")
               else [sys.executable, "-m", "flowlens", "--port", str(port)])
        proc = await asyncio.create_subprocess_exec(*cmd, cwd=workdir, env=env,
                                                    stdout=asyncio.subprocess.DEVNULL,
                                                    stderr=asyncio.subprocess.DEVNULL)
        svc = Service("local", port, proc=proc, pid=proc.pid)
        return await _wait_ready(svc, env)


async def _wait_ready(svc: Service, env: dict, timeout: float = 30.0) -> Service:
    """轮询 /healthz 直到就绪；docker 目标失败时打印容器日志便于排查。"""
    import aiohttp
    deadline = time.monotonic() + timeout
    async with aiohttp.ClientSession(
            trust_env=False, timeout=aiohttp.ClientTimeout(total=3)) as ses:
        while time.monotonic() < deadline:
            try:
                async with ses.get(svc.url + "/healthz") as r:
                    if r.status == 200:
                        d = await r.json()
                        if d.get("ok"):
                            return svc
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
            await asyncio.sleep(0.5)
    # 超时：docker 目标把日志带出来
    if svc.kind == "docker" and svc.container:
        log = await _run(["docker", "logs", svc.container])
        raise RuntimeError(f"服务未就绪（docker {svc.container}）：\n{log[-2000:]}")
    raise RuntimeError("服务未就绪（local）")


def _has(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def _svc_fronts(svc: Service, extra_env: dict) -> tuple[int | None, str]:
    """P2d-3 第零节第 2 条：核对真实前端数与预期，返回 (前端数, 原因) 供记进 meta。

    预期优先级：显式 --expect-fronts（EXPECT_FRONT 全局）> FLOWLENS_FRONTENDS
    环境变量 > 不核对（只记录）。真实数与预期不符、或读不到启动行（None）时，
    立即抛错停止该实验（并打印原因），避免拿不确定的配置跑实验。自动判断的场景
    （第五/六节）用 --expect-fronts 0 强制核对"前端数确为 0"。
    """
    fc, fr = svc.front_count, svc.front_reason
    if fc is None:
        raise RuntimeError(f"启动日志读不到前端数（末行：{fr!r}），停止该实验")
    exp = EXPECT_FRONT
    if exp is None:
        env_exp = (extra_env or {}).get("FLOWLENS_FRONTENDS")
        if env_exp is not None:
            exp = int(env_exp)
    if exp is not None and exp != fc:
        raise RuntimeError(f"前端数不符：期望 {exp}，实得 {fc}（{fr}），停止该实验")
    return fc, fr


def parse_front_count(log_text: str) -> tuple[int | None, str]:
    """从启动日志文本解析"前端数 N（原因）"这一行，返回 (N, 原因)。

    纯函数（供单测）。读不到该行时返回 (None, 原文末行)——调用方据此判定"读不到"
    并停止实验（P2d-3 第零节第 2 条）。
    """
    m = re.search(r"前端数\s+(\d+)（(.*?)）", log_text)
    if not m:
        return None, (log_text.strip().splitlines() or [""])[-1]
    return int(m.group(1)), m.group(2)


async def read_front_count(container: str) -> tuple[int | None, str]:
    """从 `docker logs` 读启动日志的前端数，返回 (N, 原因)。

    P2d-3 第零节第 2 条：每次实验启动容器后都要确认真实的前端数，记进结果文件；
    与预期不符时立即停止该实验并报告。
    """
    out = await _run(["docker", "logs", container])
    return parse_front_count(out)


async def sample_service_cpu(svc: Service, last: dict) -> float | None:
    """服务进程占用的 CPU%（占单核的比例；服务被固定在一个核上）。

    docker：读容器 cgroup v2 cpu.stat 的 usage_usec，两次采样差 / 间隔秒。
    local：读 /proc/<pid>/stat 的 utime+stime，两次差 / (clk_tck × 间隔秒)。
    """
    if svc.kind == "docker":
        out = await _run(["docker", "exec", svc.container, "cat",
                          "/sys/fs/cgroup/cpu.stat"])
        m = re.search(r"usage_usec\s+(\d+)", out)
        if not m:
            return None
        usage = int(m.group(1))
    else:
        try:
            with open(f"/proc/{svc.pid}/stat", encoding="utf-8") as f:
                fields = f.read().split()
            usage = int(fields[13]) + int(fields[14])  # utime + stime
        except (OSError, IndexError):
            return None
    now = time.monotonic()
    prev_usage, prev_t = last.get("usage"), last.get("t")
    last["usage"], last["t"] = usage, now
    if prev_t is None:
        return None
    dt = now - prev_t
    if dt <= 0:
        return None
    # docker：usage 单位是**微秒**（cgroup v2 cpu.stat 的 usage_usec），一个核满速是
    # 1e6 µs/秒；local：utime+stime 单位是 clock tick（clk_tck=100Hz）。
    frac = (usage - prev_usage) / (
        (1_000_000 if svc.kind == "docker" else svc._clk_tck) * dt)
    # 上限 = 服务核数（多核容器里服务 CPU% 可超过 100，如 4 核打满 = 400%）
    return round(min(frac, svc_ncpu()) * 100.0, 1)


# ===================== 负载 / 查询 驱动 =====================

async def _loadgen_args(svc: Service, *, rate: int, duration: float,
                        batch: int, gzip: bool, mode: str, seed: int,
                        archive: str | None = None, workers: int | None = None,
                        conc: int = 8, fast: bool = False) -> list[str]:
    if workers is None:
        workers = LOAD_WORKERS
    if fast and archive:
        raise ValueError("--fast 与存档互斥（loadgen 限制；精度实验 S3 用原模式）")
    args = [sys.executable, os.path.join(TOOLS, "loadgen.py"),
            "--url", svc.url, "--workers", str(workers), "--conc", str(conc),
            "--rate", str(rate), "--batch", str(batch),
            "--duration", str(duration), "--mode", mode, "--seed", str(seed)]
    if gzip:
        args.append("--gzip")
    if fast:
        args += ["--fast", "--fast-templates", "32"]
    if archive:
        args += ["--archive", archive]
    # 压测进程固定到其他核（子进程继承亲和性）
    if _has("taskset"):
        args = ["taskset", "-c", LOAD_CORES] + args
    return args


async def run_loadgen(svc: Service, *, rate: int, duration: float, batch: int,
                      gzip: bool, mode: str, seed: int,
                      archive: str | None = None,
                      fast: bool = False) -> tuple[dict | None, list[dict]]:
    """异步跑一次 loadgen（作为子进程），流式读它的逐秒/汇总行，返回它们。

    用 asyncio.create_subprocess_exec（非阻塞），这样监控与查询任务能在同一事件
    循环里并发跑。NO_PROXY 确保 loadgen 直连服务。fast=True 用 P2a 快速模式。
    """
    args = await _loadgen_args(svc, rate=rate, duration=duration, batch=batch,
                               gzip=gzip, mode=mode, seed=seed, archive=archive,
                               fast=fast)
    env = dict(os.environ, NO_PROXY="localhost,127.0.0.1")
    proc = await asyncio.create_subprocess_exec(
        *args, env=env, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL)
    lines = []
    assert proc.stdout is not None
    async for raw in proc.stdout:
        lines.append(raw.decode(errors="replace"))
    await proc.wait()
    return parse_loadgen_lines(lines)


async def _get(ses, url: str) -> tuple[int, float, dict | None]:
    """一次 GET：返回 (状态码, 延迟 ms, json 或 None)。"""
    t0 = time.perf_counter()
    try:
        async with ses.get(url) as r:
            dt = (time.perf_counter() - t0) * 1000.0
            try:
                return r.status, dt, await r.json()
            except Exception:
                await r.read()
                return r.status, dt, None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        return 0, (time.perf_counter() - t0) * 1000.0, {"error": repr(e)}


async def query_loop(svc: Service, duration: float, *, rate_per_type: float,
                     rng) -> list[dict]:
    """S2 查询进程：持续 duration 秒，按类型发查询，记录每条的状态码与延迟。

    类型：默认窗口 topk（url/user）、随机 at+window 回放 topk（url/user）、stats。
    返回 queries 记录列表（写 queries.jsonl）。
    """
    import aiohttp
    queries = []
    deadline = time.monotonic() + duration
    async with aiohttp.ClientSession(
            trust_env=False, timeout=aiohttp.ClientTimeout(total=5)) as ses:
        next_at = time.monotonic()
        while time.monotonic() < deadline:
            now_ms = int(time.time() * 1000)
            pick = rng.random()
            if pick < 0.25:
                qtype, by = "topk_default", "url"
            elif pick < 0.45:
                qtype, by = "topk_default", "user"
            elif pick < 0.70:
                qtype, by = "topk_replay", "url"
            elif pick < 0.90:
                qtype, by = "topk_replay", "user"
            else:
                qtype, by = "stats", None
            # at 取过去 10s~300s（仍在保留期），窗口 10~60s：保证区间起点
            # from = at − window ≥ now − 360s，落在保留期（600s）内，不会 422。
            at_offset_s = int(rng.integers(10, 301))
            window_s = int(rng.integers(10, 61))
            if qtype == "stats":
                url = f"{svc.url}/v1/stats?window={window_s}s"
            elif qtype == "topk_replay":
                at = now_ms - at_offset_s * 1000
                url = f"{svc.url}/v1/topk?by={by}&k=10&window={window_s}s&at={at}"
            else:  # topk_default
                url = f"{svc.url}/v1/topk?by={by}&k=10&window=60s"
            status, dt, body = await _get(ses, url)
            queries.append({"t_ms": now_ms, "type": qtype,
                             "params": _qparams(qtype, by, window_s,
                                                at if qtype == "topk_replay" else None),
                             "status": status, "lat_ms": round(dt, 3)})
            # 按类型速率控制：每种类型目标速率 rate_per_type 次/秒
            next_at = max(next_at, time.monotonic())
            await asyncio.sleep(max(0.0, 1.0 / max(rate_per_type, 0.1)
                                     - (time.monotonic() - next_at)))
    return queries


async def sse_loop(svc: Service, duration: float, *, groups: int,
                   per_group: int) -> dict:
    """S2 的 SSE 订阅：groups 组共 groups×per_group 个订阅，持续 duration 秒。

    用不同的 by 参数把订阅分到不同组（每组一个后台生产任务，模拟 T7 的共享计算）；
    同组内多个订阅者共用计算。统计收到的帧数与连接错误数（验证 SSE 在混合负载下
    正常、不掉线）。返回汇总。
    """
    import aiohttp
    frames = {"n": 0}
    errors = {"n": 0}
    deadline = time.monotonic() + duration

    async def one(sub_id: int) -> None:
        # group = sub_id % groups：每组 per_group 个订阅者用**相同**参数（→ 同组、
        # 共用一个生产任务）；不同组用不同 interval（→ 组身份不同、各自生产）。
        group = sub_id % groups
        interval_ms = 1000 + group * 1000  # 1s/2s/3s/4s：组间区分
        url = (f"{svc.url}/v1/topk/stream?window=60s&k=10&by=url"
               f"&interval={interval_ms}ms")
        async with aiohttp.ClientSession(trust_env=False) as ses:
            while time.monotonic() < deadline:
                try:
                    async with ses.get(
                            url, headers={"Accept": "text/event-stream"},
                            timeout=aiohttp.ClientTimeout(total=None, sock_read=20)
                    ) as r:
                        async for raw in r.content:
                            if b"event: topk" in raw:
                                frames["n"] += 1
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    errors["n"] += 1
                    await asyncio.sleep(1.0)

    tasks = [asyncio.create_task(one(i)) for i in range(groups * per_group)]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True),
                               timeout=duration + 15)
    except asyncio.TimeoutError:
        pass
    for t in tasks:
        if not t.done():
            t.cancel()
    return {"subscribers": groups * per_group, "groups": groups,
            "frames": frames["n"], "errors": errors["n"]}


async def s3_record_loop(svc: Service, duration: float) -> list[dict]:
    """S3 运行期：每秒记录默认窗口 topk（url/user, k=10）与 stats 的服务响应。

    供控制台回放做"估计与真实对比"。返回逐秒响应列表（写 accuracy.jsonl 的前段）。
    """
    import aiohttp
    rows = []
    deadline = time.monotonic() + duration
    async with aiohttp.ClientSession(
            trust_env=False, timeout=aiohttp.ClientTimeout(total=5)) as ses:
        while time.monotonic() < deadline:
            now_ms = int(time.time() * 1000)
            rec = {"t_ms": now_ms}
            st, dt, topk_url = await _get(ses, f"{svc.url}/v1/topk?by=url&k=10&window=60s")
            rec["topk_url"] = {"status": st, "lat_ms": round(dt, 2), "body": topk_url}
            st, dt, topk_user = await _get(ses, f"{svc.url}/v1/topk?by=user&k=10&window=60s")
            rec["topk_user"] = {"status": st, "lat_ms": round(dt, 2), "body": topk_user}
            st, dt, stats = await _get(ses, f"{svc.url}/v1/stats?window=60s")
            rec["stats"] = {"status": st, "lat_ms": round(dt, 2), "body": stats}
            rows.append(rec)
            await asyncio.sleep(1.0)
    return rows


def pick_finalized_intervals(n: int, archive_start_ms: int, archive_end_ms: int,
                             now_ms: int, window_ms: int, rng) -> list[tuple[int, int]]:
    """抽 n 个"已定稿"的毫秒区间 [from, to)（to 不晚于 now−迟到上限，仍在保留期内）。

    返回 [(from_ms, to_ms), ...]。区间必须完全落在存档的时间范围内、且 to 落在
    [archive_start, now−lateness]、from = to − window 不早于 archive_start。
    """
    lo = archive_start_ms + window_ms
    hi = min(archive_end_ms, now_ms - LATENESS_MS)
    out = []
    for _ in range(n):
        if hi <= lo:
            break
        to = int(rng.integers(lo, hi + 1))
        out.append((to - window_ms, to))
    return out


async def s3_sample_intervals(svc: Service, intervals: list[tuple[int, int]],
                              k: int) -> list[dict]:
    """对每个已定稿区间调 /v1/topk?at= 与 /v1/stats?at=，供与精确解比较。"""
    import aiohttp
    out = []
    async with aiohttp.ClientSession(
            trust_env=False, timeout=aiohttp.ClientTimeout(total=10)) as ses:
        for frm, to in intervals:
            rec = {"from_ms": frm, "to_ms": to}
            win_s = (to - frm) // 1000  # 区间长度（秒），= 服务的 window
            for by in ("url", "user"):
                st, dt, body = await _get(ses,
                                          f"{svc.url}/v1/topk?by={by}&k={k}&window={win_s}s&at={to}")
                rec[f"topk_{by}"] = {"status": st, "lat_ms": round(dt, 2), "body": body}
            st, dt, stats = await _get(ses, f"{svc.url}/v1/stats?window={win_s}s&at={to}")
            rec["stats"] = {"status": st, "lat_ms": round(dt, 2), "body": stats}
            out.append(rec)
    return out


def _qparams(qtype: str, by, window_s: int | None, at: int | None) -> dict:
    d = {"type": qtype}
    if by:
        d["by"] = by
    if window_s is not None:
        d["window_s"] = window_s
    if at is not None:
        d["at_ms"] = at
    return d


# ===================== 结果目录 =====================

TOOLS = os.path.dirname(os.path.abspath(__file__))


def _write_jsonl(path, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(orjson.dumps(r).decode() + "\n")


def _write_json(path, obj) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(orjson.dumps(obj).decode())


def machine_info() -> dict:
    cpu = platform.processor() or platform.machine()
    try:
        ncpu = os.cpu_count() or 0
    except Exception:
        ncpu = 0
    return {"cpu_model": cpu, "cpus": ncpu, "os": platform.platform(),
            "python": platform.python_version()}


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=TOOLS, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8", errors="replace").strip()
    except Exception:
        return "unknown"


def write_meta(outdir: str, *, scenario: str, target: str, port: int,
               cpu_alloc: dict, params: dict, image: str | None = None,
               front_count: int | None = None, front_reason: str = "") -> None:
    _write_json(os.path.join(outdir, "meta.json"), {
        "scenario": scenario, "git_commit": git_commit(),
        "machine": machine_info(),
        "service": {"target": target, "port": port,
                    "cpu_alloc": cpu_alloc, "image": image,
                    # P2d-3 第零节第 2 条：docker 启动日志读到的真实前端数与原因
                    "front_count": front_count, "front_reason": front_reason},
        "params": params,
        "created_ms": int(time.time() * 1000),
    })


def write_summary(outdir: str, obj: dict) -> None:
    _write_json(os.path.join(outdir, "summary.json"), obj)


# ===================== 场景 =====================

async def sample_service_rss(svc: Service) -> dict:
    """服务进程的 RssAnon / VmRSS（/proc/<pid>/status）。

    docker：容器内 PID 1 就是服务，`docker exec cat /proc/1/status`（与
    sample_service_cpu 的 cgroup 采样同一通道）；local：宿主 /proc/<svc.pid>/status。
    读不到返回 {}。用于"工作集（cgroup 口径）+ 进程堆（RssAnon）"双口径对照。
    """
    if svc.kind == "docker" and svc.container:
        out = await _run(["docker", "exec", svc.container, "cat", "/proc/1/status"])
    else:
        try:
            with open(f"/proc/{svc.pid}/status") as f:
                out = f.read()
        except OSError:
            return {}
    d = {}
    for line in out.splitlines():
        if line.startswith(("RssAnon:", "VmRSS:")):
            try:
                d[line.split()[0].rstrip(":")] = int(line.split()[1]) * 1024
            except (ValueError, IndexError):
                pass
        elif line.startswith("Threads:"):
            # 主进程线程数（/proc/<pid>/status）。诊断"可见核数→内存"用：
            # OpenBLAS 在 import 时按可见核数建线程，Threads 是那条链的直接读数。
            try:
                d["Threads"] = int(line.split()[1])
            except (ValueError, IndexError):
                pass
    return d


def _parse_proc_stat(text: str) -> tuple[int, int] | None:
    """从 /proc/<pid>/stat 读出 (utime+stime, 父 pid)。comm 可能带括号/空格，取最后一个 ')'。"""
    r = text.rfind(")")
    if r < 0:
        return None
    fields = text[r + 1:].split()
    if len(fields) < 14:  # state 起算；utime=field 11、stime=field 12、ppid=field 1（相对 fields）
        return None
    try:
        return (int(fields[11]) + int(fields[12]), int(fields[1]))
    except (ValueError, IndexError):
        return None


async def sample_proc_all(svc: Service, last: dict, dt: float) -> list[dict]:
    """容器内所有 python 进程的 CPU%（占单核）与 VmRSS（MiB）。

    用一次 `docker exec` 抓 /proc 下所有 stat（utime+stime、ppid）与 status（VmRSS），
    两次采样差算 CPU%，只保留服务进程（ppid==1）——即主进程 + 各解析子进程。
    多核容器里 CPU% 上限 = 核数 × 100（不再 clamp 到 100），P1 判"哪个进程跑满"用。
    返回 [{"pid":..,"name":"main|parse","cpu_pct":..,"rss_mb":..}, ...]；读不到返回 []。
    """
    if svc.kind != "docker" or not svc.container:
        return []
    out = await _run(["docker", "exec", svc.container, "sh", "-c",
                      "for p in /proc/[0-9]*; do pid=${p#/proc/}; "
                      "st=$(cat $p/stat 2>/dev/null); [ -z \"$st\" ] && continue; "
                      "rs=$(awk '/^VmRSS:/{print $2}' $p/status 2>/dev/null); "
                      "cl=$(tr '\\0' ' ' < $p/cmdline 2>/dev/null); "
                      "echo \"$pid|$st|$rs|$cl\"; done"])
    now = time.monotonic()
    prev = last.get("procs") or {}
    cur: dict[int, tuple] = {}
    res: list[dict] = []
    for line in out.splitlines():
        parts = line.split("|", 3)
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        parsed = _parse_proc_stat(parts[1])
        if parsed is None:
            continue
        utime, ppid = parsed
        try:
            rss_kb = int(parts[2]) if parts[2].strip() else 0
        except ValueError:
            rss_kb = 0
        # 只保留 ppid==1 的服务进程（主进程 pid 1 + 解析子进程）。健康检查
        # （python -c "…urlopen('…/healthz'…)"）也是 ppid==1，按 cmdline 排除。
        if ppid != 1 and pid != 1:
            continue
        cmdline = parts[3] if len(parts) > 3 else ""
        if "healthz" in cmdline or "urllib.request" in cmdline:
            continue
        cur[pid] = (utime, now, rss_kb)
    for pid, (utime, t, rss_kb) in cur.items():
        if pid in prev:
            pu, pt, _ = prev[pid]
            ddt = t - pt
            if ddt > 0:
                cpu = (utime - pu) / (svc._clk_tck * ddt) * 100.0
                name = "main" if pid == 1 else "parse"
                res.append({"pid": pid, "name": name,
                            "cpu_pct": round(min(cpu, 100 * svc_ncpu() + 1), 1),
                            "rss_mb": round(rss_kb / 1024.0, 1)})
    last["procs"] = cur
    return res


async def _monitor(svc: Service, stop_event: asyncio.Event,
                   samples: list[dict]) -> None:
    """每秒采样 /healthz、服务 CPU 与服务进程 RssAnon/VmRSS。"""
    import aiohttp
    last = {}
    last_t = time.monotonic()
    async with aiohttp.ClientSession(
            trust_env=False, timeout=aiohttp.ClientTimeout(total=5)) as ses:
        while not stop_event.is_set():
            st, _, hz = await _get(ses, svc.url + "/healthz")
            cpu = await sample_service_cpu(svc, last)
            rss = await sample_service_rss(svc)
            dt = time.monotonic() - last_t
            last_t = time.monotonic()
            procs = await sample_proc_all(svc, last, dt)
            samples.append({"t": int(time.time() * 1000),
                            "healthz": hz, "svc_cpu_pct": cpu,
                            "rss_anon_bytes": rss.get("RssAnon"),
                            "vm_rss_bytes": rss.get("VmRSS"),
                            "threads": rss.get("Threads"),
                            "procs": procs})
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass


async def _build_image(image: str, pip_index: str | None) -> None:
    # 明确构建 **app** 目标（正式服务镜像，带 CMD 启动服务）。Dockerfile 最后一个
    # 阶段是 test（跑测试、无 CMD），默认目标会建出测试镜像（直接退出），所以必须
    # 显式 --target app。测试镜像用 `docker build --target test`（收尾检查用）。
    cmd = ["docker", "build", "--target", "app", "-t", image]
    if pip_index:
        cmd += ["--build-arg", f"PIP_INDEX_URL={pip_index}"]
    cmd.append(".")
    print(f"[experiments] 构建服务镜像 {image}（--target app）...", flush=True)
    out = await _run(cmd)
    if "ERROR" in out or "error" in out.lower():
        raise RuntimeError(f"docker build 失败：\n{out[-3000:]}")


async def run_s1(svc: Service, outdir: str, *, gzip: bool, rates: list[int],
                 level_sec: int, batch: int, fast: bool = False) -> dict:
    """S1 吞吐爬坡：对每个速率跑 level_sec 秒，记录该级的接受率/503/延迟/CPU。"""
    samples: list[dict] = []
    stop = asyncio.Event()
    mon = asyncio.create_task(_monitor(svc, stop, samples))
    levels = []
    all_lg_seconds = []
    for rate in rates:
        print(f"[S1:{'gzip' if gzip else 'plain'}] rate={rate}", flush=True)
        summary, seconds = await run_loadgen(svc, rate=rate, duration=level_sec,
                                             batch=batch, gzip=gzip, mode="长尾",
                                             seed=100 + (1 if gzip else 0))
        all_lg_seconds.extend(seconds)
        if not summary:
            continue
        sent = summary.get("sent", 0)
        acc = summary.get("accepted", 0)
        rej = summary.get("rejected", 0)
        # 该级期间服务 CPU 中位数（用该级秒数对应的采样）
        sec_set = {d["t"] for d in seconds}
        cpu_in_level = [s["svc_cpu_pct"] for s in samples
                        if s["t"] // 1000 in sec_set and s["svc_cpu_pct"] is not None]
        levels.append({
            "rate": rate, "sent": sent, "accepted": acc, "rejected": rej,
            "accept_ratio": round(acc / sent, 4) if sent else 0.0,
            "ratio_503": round(rej / sent, 4) if sent else 0.0,
            "accepted_events_rate": summary.get("accepted_events_rate"),
            "lat_p50_ms": summary.get("lat_p50_ms"),
            "lat_p95_ms": summary.get("lat_p95_ms"),
            "lat_p99_ms": summary.get("lat_p99_ms"),
            "client_cpu_pct": summary.get("client_cpu_pct"),
            "svc_cpu_pct_median": (round(sorted(cpu_in_level)[len(cpu_in_level) // 2], 1)
                                   if cpu_in_level else None),
        })
        print(f"    accept={acc / sent if sent else 0:.1%} 503={rej / sent if sent else 0:.1%} "
              f"p99={summary.get('lat_p99_ms')}ms", flush=True)
    stop.set()
    await mon
    _write_jsonl(os.path.join(outdir, "timeline.jsonl"),
                 merge_timeline(samples, all_lg_seconds))
    ms = max_sustainable(levels)
    summary = {"scenario": "S1", "gzip": gzip, "levels": levels,
               "max_sustainable": ms, "memory": mem_timeline(samples),
               "procs": proc_timeline(samples)}
    write_summary(outdir, summary)
    return summary


async def run_s2(svc: Service, outdir: str, *, base_rate: int, duration: float,
                 batch: int, rng, write_rate: int | None = None,
                 fast: bool = False) -> dict:
    """S2 混合负载：写入 70% 最大可持续（或 --s2-rate 直接指定）+ 查询 + SSE。

    fast=True 时写入走 P2a 快速模式（P2c 第二节：14 万/20 万写入下量查询延迟）。
    """
    if write_rate is None:
        write_rate = int(base_rate * S2_WRITE_FRAC)
    samples: list[dict] = []
    stop = asyncio.Event()
    mon = asyncio.create_task(_monitor(svc, stop, samples))
    # 并行：loadgen（写入）+ query_loop + sse_loop
    qtasks = asyncio.gather(
        run_loadgen(svc, rate=write_rate, duration=duration, batch=batch,
                    gzip=False, mode="长尾", seed=200, fast=fast),
        query_loop(svc, duration, rate_per_type=20.0, rng=rng),
        sse_loop(svc, duration, groups=4, per_group=5),
    )
    (lg_summary, lg_seconds), queries, sse = await qtasks
    stop.set()
    await mon
    _write_jsonl(os.path.join(outdir, "timeline.jsonl"),
                 merge_timeline(samples, lg_seconds))
    _write_jsonl(os.path.join(outdir, "queries.jsonl"), queries)
    # 各查询类型延迟分位
    by_type: dict[str, list[float]] = {}
    for q in queries:
        by_type.setdefault(q["type"], []).append(q["lat_ms"])
    qstats = {t: {"n": len(v), "p50": pctl(v, 50), "p95": pctl(v, 95),
                  "p99": pctl(v, 99)} for t, v in by_type.items()}
    # P2d-3 二：整窗口的引擎（main）与前端合计 CPU%（占单核 %）
    cpu = cpu_window_pct(samples)
    summary = {"scenario": "S2", "write_rate": write_rate, "sse": sse,
               "query_stats": qstats, "load": lg_summary,
               "engine_cpu_pct": cpu["engine_cpu_pct"],
               "front_cpu_pct": cpu["front_cpu_pct"],
               "memory": mem_timeline(samples)}
    write_summary(outdir, summary)
    return summary


async def run_s3(svc: Service, outdir: str, *, rate: int, duration: float,
                 batch: int, mode: str, n_samples: int, k: int, rng) -> dict:
    """S3 精度：固定速率跑 duration 秒并存档；运行期每秒记录 topk/stats 响应；
    结束后抽 n_samples 个已定稿区间与精确解比较。"""
    archive = os.path.join(outdir, "archive")
    samples: list[dict] = []
    stop = asyncio.Event()
    mon = asyncio.create_task(_monitor(svc, stop, samples))
    now_start = int(time.time() * 1000)
    # 并行：loadgen（存档）+ s3_record_loop（每秒记录）
    (lg_summary, lg_seconds), record_rows = await asyncio.gather(
        run_loadgen(svc, rate=rate, duration=duration, batch=batch, gzip=False,
                    mode=mode, seed=300, archive=archive),
        s3_record_loop(svc, duration),
    )
    # 精确解（读存档）
    try:
        sol = exact.ExactSolution.from_dir(archive)
    except Exception as e:
        sol = None
        print(f"[S3] 精确解读取失败：{e}", flush=True)
    # 抽样区间比较（服务仍在运行、数据在保留期内）
    now_end = int(time.time() * 1000)
    window_ms = 60_000
    intervals = pick_finalized_intervals(
        n_samples, now_start, now_end, now_end, window_ms, rng)
    sampled = await s3_sample_intervals(svc, intervals, k=k)
    # 与精确解比较。降级第 1 级起回放查询（带 at）被暂停（503）；即使某些查询仍返
    # 回 200，若服务处于降级态（body 的 degraded=True）它返回的是空/不完整结果，
    # 不能拿来算精度（否则空响应对比会得到假的 recall）。因此逐条记录**状态码**与
    # **degraded** 字段，分三类单独统计：
    #   valid     —— 200 且非降级：真正可比的精度样本
    #   degraded  —— 200 但 degraded=True：降级期空/不完整响应，不算精度
    #   rejected  —— 非 200（503/422 等）：回放被拒
    comparisons = []
    n_valid = 0
    n_degraded = 0
    n_rejected = 0
    for rec in sampled:
        frm, to = rec["from_ms"], rec["to_ms"]
        tu = rec.get("topk_url") or {}
        tu_body = tu.get("body") or {}
        cmprec = {"from_ms": frm, "to_ms": to,
                  "status": tu.get("status"), "degraded": tu_body.get("degraded")}
        if tu.get("status") != 200:
            n_rejected += 1
            cmprec["bucket"] = "rejected"
            comparisons.append(cmprec)
            continue
        if tu_body.get("degraded"):
            n_degraded += 1
            cmprec["bucket"] = "degraded"
            comparisons.append(cmprec)
            continue
        n_valid += 1
        cmprec["bucket"] = "valid"
        if sol is not None:
            for by in ("url", "user"):
                svc_items = (rec.get(f"topk_{by}", {}) or {}).get("body") or {}
                items = svc_items.get("items", [])
                # 服务返回 {key,count,err}（err 是半宽）→ lower=count−err、upper=count+err
                svc_list = [{"key": it["key"], "count": it["count"],
                             "lower": it["count"] - it.get("err", 0.0),
                             "upper": it["count"] + it.get("err", 0.0)} for it in items]
                exact_items = sol.topk(by, frm, to, k)
                cmprec[f"topk_{by}"] = exact.compare(svc_list, exact_items, k=k)
            stats_body = (rec.get("stats", {}) or {}).get("body") or {}
            cmprec["stats"] = {
                "distinct_users": {"service": stats_body.get("approx_distinct_users"),
                                   "exact": sol.distinct_users(frm, to)},
                "p95": {"service": stats_body.get("approx_p95_bytes"),
                        "exact": sol.quantile_bytes(frm, to, 0.95)},
            }
        comparisons.append(cmprec)
    stop.set()
    await mon
    _write_jsonl(os.path.join(outdir, "timeline.jsonl"),
                 merge_timeline(samples, lg_seconds))
    # accuracy.jsonl：前段是运行期逐秒响应，后段是抽样区间比较（用 type 字段区分）
    acc_rows = [{"type": "per_second", **r} for r in record_rows]
    acc_rows += [{"type": "sample_comparison", **r} for r in comparisons]
    _write_jsonl(os.path.join(outdir, "accuracy.jsonl"), acc_rows)
    summary = {"scenario": "S3", "rate": rate, "mode": mode,
               "load": lg_summary, "n_intervals": len(comparisons),
               "valid": n_valid, "degraded": n_degraded, "rejected": n_rejected,
               "archive_events": len(sol) if sol is not None else None,
               "memory": mem_timeline(samples)}
    write_summary(outdir, summary)
    return summary


async def run_sustained(svc: Service, outdir: str, *, rate: int,
                        duration: float, batch: int, mode: str,
                        seed: int, tail_sec: float = 60.0,
                        minute_mark: bool = True,
                        fast: bool = False) -> dict:
    """持续负载内存曲线：固定速率跑 duration 秒 + 停发后观察 tail_sec 秒。

    C3 的对比实验（组 2~4）与 S4 攻击流量都用它：长尾 3 万/秒 × 5 分钟（对比）
    或全随机 3 万/秒 × 10 分钟（S4）。记录工作集 / RssAnon 随时间、降级级别、
    接受率、malloc_trim 回收量（/healthz 的 malloc_trim 字段，组 2 起生效），
    以及"每分钟一行"的汇总表（工作集、降级、接受率、trim 累计回收 MiB）。
    P2c 起每分钟行另带主进程 CPU% 与每条 μs（该分钟 CPU 秒差 ÷ 该分钟接受条数）。

    返回 summary（含 memory、per_minute、load、degrade 级别序列）。
    """
    samples: list[dict] = []
    stop = asyncio.Event()
    mon = asyncio.create_task(_monitor(svc, stop, samples))
    total = duration + tail_sec
    # run_loadgen 返回 (summary, seconds) 元组；asyncio.sleep 返回 None。gather 按
    # 顺序返回两者，解包时要先接住 run_loadgen 的元组再拆开，别把 sleep 的 None
    # 当成 seconds（否则 merge_timeline 拿到 None 会崩）。
    (lg_summary, lg_seconds), _ = await asyncio.gather(
        run_loadgen(svc, rate=rate, duration=duration, batch=batch,
                    gzip=False, mode=mode, seed=seed, fast=fast),
        asyncio.sleep(total),  # 负载 + 停发观察尾巴
    )
    stop.set()
    await mon
    _write_jsonl(os.path.join(outdir, "timeline.jsonl"),
                 merge_timeline(samples, lg_seconds))

    # 降级级别随时间（t 秒 → 级别）
    level_seq = [(s["t"], (s.get("healthz") or {}).get("degrade", {}).get("level"))
                 for s in samples]
    # 在途/原始体拒绝总数与接受/发送总数（healthz 差值 + loadgen 汇总口径）
    hz0 = next((s.get("healthz") or {} for s in samples
                if s.get("healthz")), {})
    hzn = next((s.get("healthz") or {} for s in reversed(samples)
                if s.get("healthz")), {})
    infl_rej = int(hzn.get("ingest_rejected", 0)) - int(hz0.get("ingest_rejected", 0))
    raw_rej = int(hzn.get("raw_rejected", 0)) - int(hz0.get("raw_rejected", 0))
    # malloc_trim 累计调用次数与累计耗时（/healthz 的 malloc_trim 字段）
    trim_calls = [ (s.get("healthz") or {}).get("malloc_trim", {}).get("call_count", 0)
                   for s in samples if (s.get("healthz") or {}).get("malloc_trim") ]
    trim_total_ms = [ (s.get("healthz") or {}).get("malloc_trim", {}).get("total_ms", 0.0)
                      for s in samples if (s.get("healthz") or {}).get("malloc_trim") ]
    # 每分钟一行：工作集 MiB、降级级别、接受率、trim 累计回收
    per_minute = []
    if minute_mark:
        t0 = samples[0]["t"] if samples else 0
        # 该分钟的接受/发送条数：healthz 采样没有 accepted/sent 字段（旧版因此
        # 恒 0/None）；改用 merge_timeline 的逐秒行（loadgen 按秒归并的结果）
        tl_by_sec = {r["t"]: r for r in merge_timeline(samples, lg_seconds)}
        n_min = int(total // 60) + (1 if total % 60 else 0)
        for m in range(0, n_min):
            # 真分钟窗 [t0 + m*60, t0 + (m+1)*60)（P2c 起：逐分钟一行，
            # 末分钟可不足 60 s；负载尾段 m >= duration//60 属"停发观察期"）
            a = t0 // 1000 + m * 60
            b = t0 // 1000 + (m + 1) * 60
            win = [s for s in samples if a <= s["t"] // 1000 < b]
            if not win:
                continue
            ws = [(s.get("healthz") or {}).get("memory_bytes") for s in win]
            ws = [w for w in ws if w is not None]
            rss = [s.get("rss_anon_bytes") for s in win]
            rss = [r for r in rss if r is not None]
            # 该分钟的峰值降级级别（逐秒采样取最大，比"分钟末读数"更能暴露
            # 短暂进级——P2c 判定"全程第 0 级"用逐秒最大值）
            lvls = [(s.get("healthz") or {}).get("degrade", {}).get("level")
                    for s in win]
            lvls = [v for v in lvls if v is not None]
            lvl = max(lvls) if lvls else 0
            acc = [tl_by_sec[sec]["accepted"] for sec in range(a, b) if sec in tl_by_sec]
            sent = [tl_by_sec[sec]["sent"] for sec in range(a, b) if sec in tl_by_sec]
            # 主进程（引擎）与前端合计的 CPU 秒：逐秒 cpu_pct（占单核 %）× 与上一
            # 采样的间隔。procs 里 name=="main" 是引擎（容器主进程），name=="parse"
            # 是前端（父进程号 = 主进程、排除 resource_tracker 与健康检查，P2d-1
            # 口径，sample_proc_all 已按此识别）。
            main_cpu_s = 0.0
            front_cpu_s = 0.0
            prev_t = None
            for s in win:
                procs = s.get("procs") or []
                main_cpu = sum(p["cpu_pct"] for p in procs
                               if p["name"] == "main" and p.get("cpu_pct") is not None)
                front_cpu = sum(p["cpu_pct"] for p in procs
                                if p["name"] != "main" and p.get("cpu_pct") is not None)
                if prev_t is not None:
                    dt_s = (s["t"] - prev_t) / 1000.0
                    main_cpu_s += main_cpu / 100.0 * dt_s
                    front_cpu_s += front_cpu / 100.0 * dt_s
                prev_t = s["t"]
            macc = sum(acc)
            tm = (win[-1].get("healthz") or {}).get("malloc_trim") or {}
            # 前端合计 CPU%（占单核 %，3 前端可 >100）：本分钟前端 CPU 秒 ÷ 分钟时长
            win_span = (win[-1]["t"] - win[0]["t"]) / 1000.0
            front_cpu_pct = (round(front_cpu_s / win_span * 100.0, 1)
                             if win_span > 0 else None)
            per_minute.append({
                "minute": m,
                "ws_peak_mb": mem_mb(max(ws)) if ws else None,
                "rss_anon_peak_mb": mem_mb(max(rss)) if rss else None,
                "level": lvl,
                "accepted": macc, "sent": sum(sent),
                "accept_ratio": round(macc / sum(sent), 4) if sum(sent) else None,
                "main_cpu_s": round(main_cpu_s, 1),
                "main_us_ev": round(main_cpu_s / macc * 1e6, 2) if macc else None,
                "front_cpu_pct": front_cpu_pct,
                "trim_call_count": tm.get("call_count", 0),
                "trim_total_ms": tm.get("total_ms", 0.0),
                "trim_freed_total_mb": tm.get("total_freed_mb", 0.0),
            })
    # RssAnon（进程堆，/proc 口径）的起始/峰值/结束，与工作集（cgroup 口径）对照
    rss_anon = [s["rss_anon_bytes"] for s in samples
                if s.get("rss_anon_bytes") is not None]
    rss_trip = ({"start_mb": mem_mb(rss_anon[0]), "peak_mb": mem_mb(max(rss_anon)),
                 "end_mb": mem_mb(rss_anon[-1])} if rss_anon else
                {"start_mb": None, "peak_mb": None, "end_mb": None})
    trim_freed_mb = [(s.get("healthz") or {}).get("malloc_trim", {}).get("total_freed_mb", 0.0)
                     for s in samples if (s.get("healthz") or {}).get("malloc_trim")]
    summary = {
        "scenario": "SUSTAINED", "rate": rate, "mode": mode,
        "duration_s": duration, "tail_s": tail_sec,
        "load": lg_summary, "memory": mem_timeline(samples),
        "procs": proc_timeline(samples),
        "rss_anon": rss_trip,
        "trim_calls_total": trim_calls[-1] if trim_calls else 0,
        "trim_total_ms": trim_total_ms[-1] if trim_total_ms else 0.0,
        "trim_freed_total_mb": trim_freed_mb[-1] if trim_freed_mb else 0.0,
        "inflight_rejected": infl_rej,   # healthz ingest_rejected 全程差（P2c 一）
        "raw_rejected": raw_rej,         # healthz raw_rejected 全程差
        "per_minute": per_minute,
        "degrade_level_seq": level_seq,
    }
    write_summary(outdir, summary)
    return summary


async def run_s4(svc: Service, outdir: str, *, rate: int, duration: float,
                 batch: int) -> dict:
    """S4 攻击流量：全随机分布（每个 URL 与用户都不重复）持续 duration 秒。

    这是题目"对抗性流量下内存有界"的直接证据：驻留表在随机流量下每个值都不
    重复、表只涨命中不到，若整体清空策略生效，驻留表自身内存应钉死在 cap 以内，
    不随事件数增长。"""
    s = await run_sustained(svc, outdir, rate=rate, duration=duration,
                            batch=batch, mode="随机", seed=400)
    s["scenario"] = "S4"
    write_summary(outdir, s)
    return s


async def run_s5(svc: Service, outdir: str, *, base_rate: int,
                 overload_sec: float, observe_sec: float, batch: int) -> dict:
    """S5 过载与恢复：以 1.5× 最大可持续发送 overload_sec 秒，停发后观察 observe_sec 秒。"""
    rate = int(base_rate * S5_OVERLOAD_FRAC)
    samples: list[dict] = []
    stop = asyncio.Event()
    mon = asyncio.create_task(_monitor(svc, stop, samples))
    overload_start = int(time.time() * 1000)
    lg_summary, lg_seconds = await run_loadgen(
        svc, rate=rate, duration=overload_sec, batch=batch, gzip=False,
        mode="长尾", seed=500)
    overload_end = int(time.time() * 1000)
    # 停发后观察恢复
    await asyncio.sleep(observe_sec)
    stop.set()
    await mon
    _write_jsonl(os.path.join(outdir, "timeline.jsonl"),
                 merge_timeline(samples, lg_seconds))
    # 恢复时间：过载结束后第一个回到 0 级的时刻
    recover_s = None
    for s in samples:
        if s["t"] >= overload_end:
            lvl = (s.get("healthz") or {}).get("degrade", {}).get("level", 0)
            if lvl == 0:
                recover_s = round((s["t"] - overload_end) / 1000.0, 1)
                break
    levels_seq = [(s["t"], (s.get("healthz") or {}).get("degrade", {}).get("level"))
                  for s in samples]
    summary = {"scenario": "S5", "overload_rate": rate,
               "overload_start_ms": overload_start, "overload_end_ms": overload_end,
               "recover_seconds": recover_s, "load": lg_summary,
               "degrade_level_seq": levels_seq,
               "memory": mem_timeline(samples)}
    write_summary(outdir, summary)
    return summary


# ===================== 编排 =====================

async def run_scenarios(names: list[str], *, target: str, port: int,
                        image: str, workdir: str, batch: int,
                        rates: list[int], s1_level_sec: int,
                        mode: str, pip_index: str | None,
                        results_root: str, extra_env: dict | None = None,
                        no_build: bool = False,
                        fast: bool = False,
                        s2_rate: int | None = None,
                        svc_mem: str = SVC_MEM,
                        sust_rate: int = SUST_RATE,
                        sust_sec: int = SUST_SEC) -> dict:
    global SVC_MEM
    SVC_MEM = svc_mem
    # 快速模式不支持存档（loadgen 限制）：S3/S4 与 --fast 互斥
    if fast and ({"S3", "S4"} & set(names)):
        raise ValueError("--fast 与 S3/S4 互斥（快速模式不支持 --archive，"
                         "精度与攻击流量实验用原模式）")
    rng = __import__("numpy").random.default_rng(1234)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    root = os.path.join(results_root, stamp)
    cpu_alloc = {"service": SVC_CORE if not NO_CPUSET else "no-cpuset",
                 "loadgen": LOAD_CORES, "aux": AUX_CORE}
    out: dict = {}
    if target == "docker":
        if no_build:
            print(f"[experiments] --no-build：不重建，直接用现成镜像 {image}", flush=True)
        else:
            await _build_image(image, pip_index)

    s1_plain_max = None
    if "S1" in names:
        # --fast 时只跑 plain：快速模式的每批 gzip 压缩把压测端 CPU 拉回 ~30 μs/条
        # （P2b 的 e2e 也是 plain 口径），与 gzip 组失去可比性
        for gz in ((False,) if fast else (False, True)):
            d = os.path.join(root, f"S1_{'gzip' if gz else 'plain'}")
            os.makedirs(d, exist_ok=True)
            svc = await start_service(target, port, image, workdir, extra_env=extra_env)
            try:
                fc, fr = _svc_fronts(svc, extra_env)
                write_meta(d, scenario="S1", target=target, port=port,
                           cpu_alloc=cpu_alloc,
                           params={"gzip": gz, "rates": rates,
                                   "level_sec": s1_level_sec, "batch": batch,
                                   "mode": mode, "fast": fast},
                           image=image if target == "docker" else None,
                           front_count=fc, front_reason=fr)
                r = await run_s1(svc, d, gzip=gz, rates=rates,
                                 level_sec=s1_level_sec, batch=batch, fast=fast)
            finally:
                await svc.stop()
            out[f"S1_{ 'gzip' if gz else 'plain'}"] = r
            if not gz:
                s1_plain_max = r["max_sustainable"]
    base = s1_plain_max or (rates[-1] if rates else 30_000)
    print(f"[experiments] S1 最大可持续 = {s1_plain_max}（混合/过载场景以其为基准）",
          flush=True)

    if "S2" in names:
        d = os.path.join(root, "S2")
        os.makedirs(d, exist_ok=True)
        svc = await start_service(target, port, image, workdir, extra_env=extra_env)
        try:
            fc, fr = _svc_fronts(svc, extra_env)
            write_meta(d, scenario="S2", target=target, port=port, cpu_alloc=cpu_alloc,
                       params={"write_frac": S2_WRITE_FRAC, "duration_s": S2_SEC,
                               "batch": batch, "base_rate": base, "fast": fast,
                               "s2_rate": s2_rate},
                       image=image if target == "docker" else None,
                       front_count=fc, front_reason=fr)
            r = await run_s2(svc, d, base_rate=base, duration=S2_SEC,
                             batch=batch, rng=rng, write_rate=s2_rate, fast=fast)
        finally:
            await svc.stop()
        out["S2"] = r

    if "S3" in names:
        d = os.path.join(root, "S3")
        os.makedirs(d, exist_ok=True)
        svc = await start_service(target, port, image, workdir, extra_env=extra_env)
        try:
            fc, fr = _svc_fronts(svc, extra_env)
            write_meta(d, scenario="S3", target=target, port=port, cpu_alloc=cpu_alloc,
                       params={"rate": S3_RATE, "duration_s": S3_SEC, "batch": batch,
                               "mode": mode, "n_samples": S3_SAMPLES, "k": 10},
                       image=image if target == "docker" else None,
                       front_count=fc, front_reason=fr)
            r = await run_s3(svc, d, rate=S3_RATE, duration=S3_SEC, batch=batch,
                             mode=mode, n_samples=S3_SAMPLES, k=10, rng=rng)
        finally:
            await svc.stop()
        out["S3"] = r

    if "S5" in names:
        d = os.path.join(root, "S5")
        os.makedirs(d, exist_ok=True)
        svc = await start_service(target, port, image, workdir, extra_env=extra_env)
        try:
            fc, fr = _svc_fronts(svc, extra_env)
            write_meta(d, scenario="S5", target=target, port=port, cpu_alloc=cpu_alloc,
                       params={"overload_frac": S5_OVERLOAD_FRAC,
                               "overload_s": S5_OVERLOAD_SEC,
                               "observe_s": S5_OBSERVE_SEC, "base_rate": base},
                       image=image if target == "docker" else None,
                       front_count=fc, front_reason=fr)
            r = await run_s5(svc, d, base_rate=base,
                             overload_sec=S5_OVERLOAD_SEC,
                             observe_sec=S5_OBSERVE_SEC, batch=batch)
        finally:
            await svc.stop()
        out["S5"] = r

    if "S4" in names:
        d = os.path.join(root, "S4")
        os.makedirs(d, exist_ok=True)
        svc = await start_service(target, port, image, workdir, extra_env=extra_env)
        try:
            fc, fr = _svc_fronts(svc, extra_env)
            write_meta(d, scenario="S4", target=target, port=port, cpu_alloc=cpu_alloc,
                       params={"rate": S4_RATE, "duration_s": S4_SEC, "batch": batch,
                               "mode": "随机", "tail_s": 60},
                       image=image if target == "docker" else None,
                       front_count=fc, front_reason=fr)
            r = await run_s4(svc, d, rate=S4_RATE, duration=S4_SEC, batch=batch)
        finally:
            await svc.stop()
        out["S4"] = r

    if "SUST" in names:
        # C3 对比实验组（2~4）：长尾 3 万/秒 × 5 分钟持续负载，行为由 --extra-env 切换
        # （trim / 驻留 / 在途额度）。每组一个新容器，跑完停掉，结果各存一个子目录。
        d = os.path.join(root, "SUST")
        os.makedirs(d, exist_ok=True)
        svc = await start_service(target, port, image, workdir, extra_env=extra_env)
        try:
            fc, fr = _svc_fronts(svc, extra_env)
            write_meta(d, scenario="SUST", target=target, port=port, cpu_alloc=cpu_alloc,
                       params={"rate": sust_rate, "duration_s": sust_sec, "batch": batch,
                               "mode": "长尾", "tail_s": SUST_TAIL,
                               "fast": fast,
                               "extra_env": extra_env or {}},
                       image=image if target == "docker" else None,
                       front_count=fc, front_reason=fr)
            r = await run_sustained(svc, d, rate=sust_rate, duration=sust_sec,
                                    batch=batch, mode="长尾", seed=350,
                                    tail_sec=SUST_TAIL, fast=fast)
        finally:
            await svc.stop()
        out["SUST"] = r

    return {"root": root, "results": out}


def main() -> None:
    global SVC_CORE, LOAD_CORES, LOAD_WORKERS
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(description="FlowLens 端到端实验（T10b 第二节）")
    p.add_argument("--target", choices=["docker", "local"], default="docker")
    p.add_argument("--scenarios", nargs="+", default=["S1", "S2", "S3", "S5"])
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--image", default="flowlens:latest")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    p.add_argument("--s1-rates", type=int, nargs="*", default=S1_RATES)
    p.add_argument("--s1-level-sec", type=int, default=S1_LEVEL_SEC)
    p.add_argument("--mode", choices=["长尾", "均匀", "随机"], default="长尾")
    p.add_argument("--pip-index", default=None,
                   help="docker build 的 PIP_INDEX_URL（可选，走国内源）")
    p.add_argument("--results", default="results", help="结果根目录")
    p.add_argument("--svc-cpus", default=SVC_CORE,
                   help="服务容器的 --cpuset-cpus（WSL 宿主核号），如 '0,2,4,6'")
    p.add_argument("--load-cpus", default=LOAD_CORES,
                   help="压测 loadgen 的 taskset 核（WSL 宿主核号），如 '8-15'")
    p.add_argument("--load-workers", type=int, default=LOAD_WORKERS,
                   help="loadgen 的 --workers 进程数（默认 = --load-cpus 的逻辑核数）")
    p.add_argument("--extra-env", action="append", default=[],
                   metavar="K=V",
                   help="额外注入服务进程的环境变量（可多次）。C3 实验组用它"
                        "开/关 malloc_trim（FLOWLENS_MALLOC_TRIM_SEC）、驻留"
                        "（FLOWLENS_INTERN_USER_CAP/URL_CAP=0）、在途额度"
                        "（FLOWLENS_MAX_PENDING_BYTES）。")
    p.add_argument("--no-cpuset", action="store_true",
                   help="服务容器不加 --cpuset-cpus（让服务看到宿主全部核；"
                        "§7 多核对照用，与评测方最可能的启动方式一致）。")
    p.add_argument("--no-build", action="store_true",
                   help="不重建镜像，直接用 --image 指定的现成镜像（§7 用"
                        "预先从 origin/main 构建的 flowlens:submitted）。")
    p.add_argument("--fast", action="store_true",
                   help="S1/S2/SUST 的写入用 loadgen 快速模式（P2a）。与 S3/S4 "
                        "互斥（快速模式不支持存档）；S1 只跑 plain 组。")
    p.add_argument("--s2-rate", type=int, default=0,
                   help="S2 直接指定写入速率（事件/秒），覆盖 70%% 最大可持续"
                        "的推算（P2c：14 万/20 万两档）。")
    p.add_argument("--svc-mem", default=SVC_MEM,
                   help="服务容器内存上限（默认 256m；P2c 的 A512 组用 512m）。")
    p.add_argument("--expect-fronts", type=int, default=None,
                   help="P2d-3 第零节第 2 条：预期前端数，启动后从 docker logs 核对，"
                        "不符立即停止该实验。第五/六节（自动判断）用 0 强制核对；"
                        "缺省时按注入的 FLOWLENS_FRONTENDS 核对、未注入则只记录。")
    p.add_argument("--sust-rate", type=int, default=SUST_RATE,
                   help="SUST 持续速率（默认 3 万/秒；P2c 第一节用 20 万）。")
    p.add_argument("--sust-sec", type=int, default=SUST_SEC,
                   help="SUST 持续秒数（默认 300；P2c 的 10 分钟长跑用 600）。")
    args = p.parse_args()

    # P1 多核核分配：把命令行覆盖写进全局（start_service/_loadgen_args 在调用时读）
    global NO_CPUSET, EXPECT_FRONT
    NO_CPUSET = args.no_cpuset
    EXPECT_FRONT = args.expect_fronts
    SVC_CORE = args.svc_cpus
    LOAD_CORES = args.load_cpus
    if args.load_workers > 0:
        LOAD_WORKERS = args.load_workers
    else:
        LOAD_WORKERS = len(expand_cpus(LOAD_CORES))  # 默认 = 压测核的逻辑核数

    extra_env = {}
    for kv in args.extra_env:
        if "=" not in kv:
            p.error(f"--extra-env 需要 K=V 形式，收到 {kv!r}")
        k, v = kv.split("=", 1)
        extra_env[k] = v

    workdir = os.path.dirname(TOOLS)
    t0 = time.monotonic()
    result = asyncio.run(run_scenarios(
        args.scenarios, target=args.target, port=args.port, image=args.image,
        workdir=workdir, batch=args.batch, rates=args.s1_rates,
        s1_level_sec=args.s1_level_sec, mode=args.mode, pip_index=args.pip_index,
        results_root=args.results, extra_env=extra_env, no_build=args.no_build,
        fast=args.fast, s2_rate=args.s2_rate or None, svc_mem=args.svc_mem,
        sust_rate=args.sust_rate, sust_sec=args.sust_sec))
    dt = time.monotonic() - t0
    print(f"\n[experiments] 完成，用时 {dt:.0f}s，结果目录：{result['root']}", flush=True)
    for name, r in result["results"].items():
        extra = ""
        if r.get("scenario") == "S1":
            extra = f"  max_sustainable={r.get('max_sustainable')}"
        if r.get("scenario") == "S5":
            extra = f"  recover={r.get('recover_seconds')}s"
        mem = r.get("memory", {})
        ws = mem.get("working_set", {})
        extra += (f"  WS start/peak/end="
                  f"{ws.get('start_mb')}/{ws.get('peak_mb')}/{ws.get('end_mb')} MiB"
                  f"  degrade_changed={mem.get('degrade_changed')}")
        print(f"  {name}{extra}", flush=True)


if __name__ == "__main__":
    main()
