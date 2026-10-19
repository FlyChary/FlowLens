"""C2（T9b 支线）内存增长探测：在容器内跑真实服务，逐时刻采样内存构成。

目标：查清"容器里 3 万/秒持续写入下工作集涨到 ~229 MiB、而 C1 进程内模拟只涨
~97 MB"这 ~90 MB 差额的来源。C1 测的是**进程内**（无 HTTP 并发、无网络栈、无
aiohttp 连接对象、模拟工具自驱），容器里多了：真实 TCP 连接与 aiohttp 协议对象、
多连接并发的瞬时解析峰值、以及 glibc 分配器"释放后不还操作系统"的常驻。本脚本
把这三层都采下来，用数据区分：
  1. 内存碎片（分配器不还）→ RssAnon 持续涨而 Python 层（getallocatedblocks /
     gc 对象数）平稳；
  2. 并发瞬时峰值叠加 → 只在多连接并发时涨，单连接时不涨，负载停后回落；
  3. 泄漏（某结构持续增长）→ Python 层对象数与 RssAnon 一起涨，负载停后不回落。

运行方式（在容器内、作为 PID 1 之外的探针）：
  用已构建的正式镜像，把仓库 tools/ 挂进去，跑本脚本。本脚本用 create_app 在进程
  内启动**与 python -m flowlens 相同配置、真实 memstat** 的服务（监听 --port），
  然后每 --sample-interval 秒采一次、每 --walk-interval 秒用 C1 的 MemWalker 记
  一次账本，逐行 JSON 写到标准输出（服务自身日志走 stderr，不混入）。

四种运行方式（--mode 只是标签，真正的差异来自环境变量 / --malloc-trim）：
  baseline   —— 与生产相同（不额外设置）
  arena2     —— 启动容器时带 -e MALLOC_ARENA_MAX=2
  trim       —— 本脚本加 --malloc-trim：每采样周期 ctypes 调 libc.malloc_trim(0)
                并记录调用前后的 RssAnon（用于判断"分配器保留的已释放页"有多少能还）
  pymalloc   —— 启动容器时带 -e PYTHONMALLOC=malloc（绕过 Python 小对象分配器）

用法（在容器内）：
  python /probe/mem_probe.py --port 8080 --duration 340 --mode baseline
  python /probe/mem_probe.py --port 8080 --duration 340 --mode trim --malloc-trim

采样行字段（type=sample，每 10 秒一行）：
  t               相对采样起点的毫秒
  cgroup          memory.current / anon / file / active_file / inactive_file /
                  working_set(=current−inactive_file，降级口径) / limit
  proc            VmRSS / RssAnon（/proc/self/status，字节）
  py.allocated_blocks   sys.getallocatedblocks()（CPython 分配器管理的块数，廉价）
  degrade         当前降级级别 + 控制器看到的 memory_bytes / occupancy / tick_lag
  events          已写入事件数（与负载对齐用）
  每 30 秒额外：py.gc_top10（对象数最多的 10 种类型及数量）、walk（C1 MemWalker 各
  结构与缓存总量 + 本次统计自身耗时 walk_ms，用于确认统计不影响结论）
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import gc
import json
import os
import platform
import sys
import time
from collections import Counter
from pathlib import Path

# 容器内 tools/ 挂载在 /probe，本脚本在 /probe/mem_probe.py：把 /probe 放进
# sys.path 才能 import 同目录的 mem_report（C1 的 MemWalker 记账器）。site-packages
# 里的 flowlens 由正常 import 解析。
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from aiohttp import web  # noqa: E402

from flowlens.api.app import create_app, make_engine  # noqa: E402
from flowlens.timewin import Clock  # noqa: E402

if os.environ.get("PYTHONHASHSEED") != "0":
    print("警告：PYTHONHASHSEED 未固定为 0", file=sys.stderr)


# ---------------- cgroup / 进程 / Python 层读数 ----------------
# 这些读数函数带 root 参数（默认 "/"），与 flowlens.memstat 的 root 惯用法一致：
# 生产读真实 /sys 与 /proc，测试可指向伪造目录。只读、不抛异常。

_V1_NO_LIMIT = 1 << 60  # cgroup v1 无上限的哨兵（与 memstat 同值）


def _first_int(path: Path, col: int) -> int | None:
    try:
        with open(path, "rb") as f:
            data = f.read(128)
    except OSError:
        return None
    try:
        return int(data.split(None, col + 1)[col])
    except (ValueError, IndexError):
        return None


def _stat_field(path: Path, name: str) -> int | None:
    """读 cgroup memory.stat 的某个字段（v1 的 "key = value" 也兼容）。"""
    try:
        with open(path, "rb") as f:
            data = f.read(8192)
    except OSError:
        return None
    for line in data.split(b"\n"):
        if not line:
            continue
        parts = line.split(None, 1)
        key = parts[0].rstrip(b"=").decode("ascii", "ignore")
        if key == name and len(parts) > 1:
            try:
                return int(parts[1].split()[0])
            except (ValueError, IndexError):
                return None
    return None


def cgroup_memory(root: str = "/") -> dict:
    """读当前 cgroup（v2 优先，v1 兜底）的内存构成。

    返回 current/anon/file/active_file/inactive_file/working_set/limit（读不到的
    字段为 None）。working_set = current − inactive_file，与降级控制器同口径
    （memstat.memory_bytes），是与 T10b 实测可直接对照的数。
    """
    out: dict = {}
    v2 = Path(root) / "sys" / "fs" / "cgroup"
    cur = _first_int(v2 / "memory.current", 0)
    if cur is not None:
        out["current"] = cur
        for f in ("anon", "file", "active_file", "inactive_file"):
            out[f] = _stat_field(v2 / "memory.stat", f)
        mx = None
        try:
            txt = (v2 / "memory.max").read_text().strip()
            mx = None if txt == "max" else int(txt)
        except (OSError, ValueError):
            mx = None
        out["limit"] = mx
    else:
        # cgroup v1 兜底
        v1 = Path(root) / "sys" / "fs" / "cgroup" / "memory"
        cur = _first_int(v1 / "memory.usage_in_bytes", 0)
        if cur is not None:
            out["current"] = cur
            for f in ("anon", "file", "active_file"):
                out[f] = _stat_field(v1 / "memory.stat", f)
            # v1 的不活跃文件缓存是 total_inactive_file（与 memstat 同口径）
            out["inactive_file"] = _stat_field(v1 / "memory.stat", "total_inactive_file")
            lim = _first_int(v1 / "memory.limit_in_bytes", 0)
            out["limit"] = None if (lim is None or lim >= _V1_NO_LIMIT) else lim
    ws = out.get("current")
    inact = out.get("inactive_file")
    if ws is not None and inact is not None:
        out["working_set"] = max(0, ws - inact)
    return out


def proc_status(root: str = "/") -> dict:
    """/proc/self/status 的 VmRSS 与 RssAnon（字节）。RssAnon 是堆（anon）部分——
    碎片/泄漏判断的核心信号。读不到（如 Windows）返回空 dict。"""
    out: dict = {}
    try:
        with open(Path(root) / "proc" / "self" / "status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    out["VmRSS_bytes"] = int(line.split()[1]) * 1024
                elif line.startswith("RssAnon:"):
                    out["RssAnon_bytes"] = int(line.split()[1]) * 1024
    except OSError:
        pass
    return out


def rss_anon() -> int:
    """当前 RssAnon（字节），供 malloc_trim 前后对照。"""
    return proc_status().get("RssAnon_bytes", 0)


def _gc_top10() -> tuple[list, int]:
    """gc 中对象数最多的前 10 种类型。返回 ([(类型名, 数量), ...], 总对象数)。

    昂贵（遍历全部 GC 对象），只在 --walk-interval 周期里做一次。"""
    counts: Counter = Counter()
    for obj in gc.get_objects():
        counts[type(obj).__name__] += 1
    return counts.most_common(10), sum(counts.values())


# ---------------- C1 MemWalker 记账（每 30 秒） ----------------

def _walk_ledger(engine, query) -> dict:
    """用 C1 的 build_ledger 统计引擎各结构与缓存总量，并记录本次统计自身耗时。

    放在事件循环里同步跑（只读遍历，不改状态）；耗时记入 walk_ms，用于确认这次
    统计本身对服务/内存的影响可忽略。失败时返回 {"error": ...}，不影响主采样。
    """
    try:
        from mem_report import build_ledger
    except Exception as e:  # 导入失败不该让整个探针崩
        return {"error": f"import mem_report 失败: {e!r}"}
    t0 = time.perf_counter()
    try:
        ledger = build_ledger(engine, query)
    except Exception as e:
        return {"error": f"build_ledger 失败: {e!r}"}
    walk_ms = (time.perf_counter() - t0) * 1000.0
    return {"rows": ledger["rows"], "walked_bytes": ledger["walked_bytes"],
            "walk_ms": round(walk_ms, 1)}


# ---------------- malloc_trim（--malloc-trim 模式） ----------------

_LIBC: ctypes.CDLL | None = None


def _malloc_trim() -> dict:
    """ctypes 调 libc.malloc_trim(0)：请求分配器把可归还的堆顶页还给操作系统。
    记录调用前后 RssAnon 与耗时，用于量化"分配器保留的已释放页"有多少可回收。"""
    global _LIBC
    if _LIBC is None:
        try:
            _LIBC = ctypes.CDLL("libc.so.6")
        except OSError:
            return {"error": "无法加载 libc.so.6"}
    before = rss_anon()
    t0 = time.perf_counter()
    ret = _LIBC.malloc_trim(0)
    dt_us = (time.perf_counter() - t0) * 1e6
    after = rss_anon()
    return {"ret": int(ret), "before_rssanon": before, "after_rssanon": after,
            "freed_bytes": before - after, "us": round(dt_us)}


# ---------------- 采样主体 ----------------

def _mb(x) -> float | None:
    return None if x is None else round(x / 1048576, 3)


async def run(port: int, duration: float, sample_interval: float,
              walk_interval: float, mode: str, malloc_trim: bool) -> None:
    # 与 __main__ 相同的装配：真实 Engine + IngestService + QueryService，
    # create_app 默认 memory_reader=memstat（真实读数）。lingering_time/auto_
    # decompress 与 __main__ 一致。
    engine, ingest, query = make_engine(Clock())
    app = create_app(engine, ingest, query)
    runner = web.AppRunner(app, auto_decompress=False, lingering_time=1.0)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(json.dumps({"type": "listening", "port": port}, ensure_ascii=False),
          flush=True)

    ctrl = engine.controller
    walk_every = max(1, int(round(walk_interval / sample_interval)))
    cg0 = cgroup_memory()
    emit = {
        "type": "meta", "mode": mode, "port": port,
        "python": platform.python_version(),
        "env": {"MALLOC_ARENA_MAX": os.environ.get("MALLOC_ARENA_MAX"),
                "PYTHONMALLOC": os.environ.get("PYTHONMALLOC")},
        "malloc_trim": malloc_trim,
        "cgroup_limit": cg0.get("limit"),
        "cgroup_current_at_start": cg0.get("current"),
        "start_ms": int(time.time() * 1000),
    }
    print(json.dumps(emit, ensure_ascii=False), flush=True)

    t0 = time.monotonic()
    n = 0
    limit = cg0.get("limit") or 256 * 1024 * 1024
    # 记账安全阈值：memory.current 超过上限的 80% 时跳过 gc 扫描与 MemWalker
    # 记账。原因（baseline 实测）：满负载下服务 anon-rss 已爬到 ~238 MiB、
    # memory.current 峰值 243 MiB，此时 gc.get_objects() + 深度遍历的瞬时对象
    # 会把 current 推过 256 MiB 上限 → 内核 OOM 杀掉探针本身（baseline 在 t≈264s
    # 被 OOM）。跳过记账不影响逐 10s 的核心采样（cgroup/proc/allocated_blocks），
    # 只损失峰值时刻的 walk/gc 明细——而那正是最危险的时刻，保命优先。
    WALK_HEADROOM = 0.80
    try:
        while time.monotonic() - t0 < duration:
            cg = cgroup_memory()
            rec: dict = {
                "type": "sample",
                "t_ms": int((time.monotonic() - t0) * 1000),
                "cgroup": cg,
                "proc": proc_status(),
                "py": {"allocated_blocks": sys.getallocatedblocks()},
                "degrade": {
                    "level": ctrl.level,
                    "memory_bytes": ctrl.memory_bytes,
                    "occupancy": ctrl.occupancy,
                    "tick_lag_ms": ctrl.tick_lag_ms,
                },
                "events_ingested": engine.counters.events_ingested,
            }
            if malloc_trim:
                rec["malloc_trim"] = _malloc_trim()
            cur = cg.get("current")
            near_limit = (cur is not None and cur > WALK_HEADROOM * limit)
            if n % walk_every == 0 and not near_limit:
                g0 = time.perf_counter()
                top, total = _gc_top10()
                rec["py"]["gc_top10"] = top
                rec["py"]["gc_total_objects"] = total
                rec["py"]["gc_scan_ms"] = round((time.perf_counter() - g0) * 1000, 1)
                rec["walk"] = _walk_ledger(engine, query)
            elif near_limit:
                rec["walk"] = {"skipped": "near memory limit",
                               "current": cur, "limit": limit}
            print(json.dumps(rec, ensure_ascii=False, default=str), flush=True)
            n += 1
            await asyncio.sleep(sample_interval)
    finally:
        try:
            await runner.cleanup()
        except Exception:
            pass


def main() -> None:
    p = argparse.ArgumentParser(description="C2 内存增长探测（容器内）")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--duration", type=float, default=340.0,
                   help="采样总时长（秒）：建议 = 负载时长 + 一段空载尾巴")
    p.add_argument("--sample-interval", type=float, default=10.0)
    p.add_argument("--walk-interval", type=float, default=30.0,
                   help="MemWalker 记账周期（秒），同时采 gc 对象类型")
    p.add_argument("--mode", default="baseline",
                   help="运行方式标签：baseline/arena2/trim/pymalloc")
    p.add_argument("--malloc-trim", action="store_true",
                   help="每采样周期调用 libc.malloc_trim(0) 并记录前后 RssAnon")
    args = p.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    asyncio.run(run(args.port, args.duration, args.sample_interval,
                    args.walk_interval, args.mode, args.malloc_trim))


if __name__ == "__main__":
    main()
