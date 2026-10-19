"""T11a 端到端 CPU 剖析工具（两种模式，任务书见 docs/instructions/T11a-2.md）。

两次固定速率测量：10 万/秒（接近单核上限，测处理能力）与 6 万/秒（较低负载，
测事件循环延迟与垃圾回收停顿），均为每批 5000 条、不 gzip、预热 30 s + 测量
30 s。测量窗口按墙钟截止时间（time.monotonic）计满 measure 秒；loadgen 时长
= 预热 + 测量 + 3 s 启动延迟 + 10 s 余量，全程覆盖测量窗口。回答"在不受内存
降级干扰的情况下，服务处理每条事件的 CPU 时间花在了哪里"。

垃圾回收单独计账：每次回收的起止墙钟时刻记进 GC_EVENTS；各阶段按"调用时段与
回收时段的交叠"扣除回收耗时（gc_us），阶段数字才反映其自身开销。URL 归一化
缓存的命中率在运行时包装 flowlens.model.normalize_url / _normalize_url 统计。

serve 模式（容器内运行）：
  - 与 flowlens/__main__.py 完全相同的方式创建真实服务（make_engine + create_app），
    只是 memory_reader 传固定假读数（50 MB / 256 MB 上限），保证测量期间永不因内存
    进入降级
  - 对写入路径与 tick 的函数做"包装计时"：替换模块属性 / 类方法为带计时的包装
    （只加计时、不改行为），分阶段同时累计 wall 与 CPU 时间。顶层阶段的 CPU 之和
    应接近进程 CPU 时间（time.process_time 差值），据此校验包装的完整性
  - gc.callbacks 记录每次回收的代数、耗时、回收对象数（回调阶段是字符串
    'start'/'stop'，不是 gc.START/gc.FINISH——那两个名字在 CPython 里不存在）
  - 每 100 ms 调度一个小任务测事件循环延迟（直接决定查询 p99）
  - 每秒读一次真实工作集（memstat.memory_bytes，不是那个假读数）；超过 220 MB
    提前结束测量（假读数会让内存降级失效，必须防真实超限）
  - 相位由 run 模式驱动：服务就绪（ready）→ run 模式起好 loadgen 后 POST
    /prof/start → 预热 → 清零计数开始测量 → 写 JSON 退出。这样测量窗口与
    loadgen 稳态对齐（预热期 loadgen 已在发，测量期 loadgen 仍在发）

run 模式（宿主上运行）：
  - 启动容器（serve 模式、--memory=256m、固定 1 个服务核），等 /prof/state 就绪，
    用 taskset 在压测核上后台起 loadgen，再通知 serve 开始预热/测量，docker wait
    容器退出后读取 JSON 并汇总
  - 路径均可移植：仓库根按本文件位置推算；结果默认写仓库内 results/T11a
    （--out-root 可覆盖）；loadgen 解释器默认当前 Python（--loadgen-py 或
    环境变量 FLOWLENS_LOADGEN_PY 可覆盖）

核的分配与 T10b 相同：服务 0 号核，压测 1-6 号核。宿主端口 9000（本机 WSL2 的
80xx 段被保留，见 docs/PROGRESS.md 决策）。容器以非 root（uid 10001）运行，输出
目录须事先在宿主建好并 chmod 777。
"""

import argparse
import asyncio
import bisect
import gc
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

# ===================== serve 模式的统计容器（模块全局，包装函数闭包读写） =====================

# 阶段名 -> [累计 wall ns, 调用次数, 累计 cpu ns]。顶层阶段之和（cpu 口径）应接近
# 进程 CPU 时间；ingest 的细分阶段（classify/hash/dedup/groupby/dim_write）嵌套在
# ingest 之内，报告中单独列出、从 ingest 剩余里扣掉。
STAGES: dict[str, list] = {}
# 事件循环延迟探针采到的延迟（微秒）：任务从"计划执行"到"实际执行"的间隔
LOOP_LAT_US: list[float] = []
# 每次 gc 回收的事件：{"gen", "t"(相对测量起点，秒), "us", "collected",
# "s"(回收开始的墙钟 monotonic), "e"(回收结束的墙钟 monotonic)}
GC_EVENTS: list[dict] = []
# 每次回收的墙钟区间 [(start, end)]，按时间顺序追加、单调递增、互不重叠。各阶段
# "扣除 GC"用它：阶段调用时段 [t0, t1] 与回收区间的交叠之和就是"该调用里事件循环
# 正被回收占住"的时间——它同样烧进程 CPU、process_time 差值里含它，但不是该阶段
# 自己的活。GC_ENDS 是 end 的平行列表（同序），供 bisect 二分定位候选区间，把每次
# 调用的扣除开销压到 O(log n)（回收稀疏，绝大多数调用与任何回收都不相交）。
GC_SPANS: list[tuple[float, float]] = []
GC_ENDS: list[float] = []
# URL 归一化缓存统计（包装 flowlens.model.normalize_url / _normalize_url 累计，
# 测量起点清零）：[总调用次数, 未命中次数, 未命中累计耗时 ns, 未命中中带 ID 段
# 的原始 URL 数]
NORM_STATS: list = [0, 0, 0, 0]
# 查询延迟探针采到的查询端到端延迟（毫秒）：每 250 ms 走一次真实
# QueryService.topk（url 维、60 s 窗口、k=10），wall 口径。直接回答"低负载下
# 查询 p99 能否低于 50 ms"（T11a-2 第三节第 5 条）
QUERY_LAT_MS: list[float] = []
# 每秒的真实工作集（MiB）
MEM_MB: list[float] = []
# 测量起点（monotonic）与起点时的进程 CPU 时间（秒）
MEASURE_START = [0.0]
MEASURE_CPU0 = [0.0]
# 测量起点时的引擎计数（accepted/deduped/late/invalid），用于算"测量窗口内"的
# 事件数——预热期也在写入，若用累计计数会把预热算进来、把每条 CPU 时间稀释
MEASURE_EVENTS0 = [0, 0, 0, 0]
# 相位（/prof/state 用）
PHASE = {"phase": "starting"}

# 顶层阶段（报告表格顺序）
TOP_STAGES = ["read_body", "decode", "load_array", "validate", "ingest",
              "tick_finalize", "tick_snapshot"]
# ingest 的细分阶段（嵌套在 ingest 内）
INGEST_SUBS = ["classify", "hash", "dedup", "groupby", "dim_write"]
ALL_STAGES = TOP_STAGES + INGEST_SUBS
# 等待 IO 的阶段：异步函数体里真正在等网络/让出事件循环，它测得的 process_time 差值
# 会包含"该协程挂起期间事件循环干的其他活"（别的请求、Ticker、探针），不能当成它自己
# 烧的 CPU。故 CPU 口径只统计同步阶段，这类阶段单独报 wall（衡量流水线在 IO 上的停顿）。
IO_BOUND_STAGES = {"read_body"}


def _record(name: str, wall_ns: int, cpu_ns: int, gc_ns: int = 0) -> None:
    # STAGES[name] = [累计 wall ns, 调用次数, 累计 cpu ns, 累计 gc ns（扣除项）]
    e = STAGES.get(name)
    if e is None:
        e = STAGES[name] = [0, 0, 0, 0]
    e[0] += wall_ns
    e[1] += 1
    e[2] += cpu_ns
    e[3] += gc_ns


def _gc_overlap_ns(t0: float, t1: float) -> int:
    """调用时段 [t0, t1]（monotonic 秒）与所有回收区间的交叠之和（ns）。

    回收与调用都在事件循环单线程里发生，交叠部分烧的进程 CPU 既算进该阶段的
    process_time 差值、也不是它自己的活——从该阶段 wall 与 cpu 里各扣一次
    （单线程下这段时间两者相等）。

    候选区间只有"end > t0 且 start < t1"的那些；end 单调递增，用 bisect 定位
    end > t0 的第一个下标，再顺序走到 start >= t1 为止（区间互不重叠，候选数
    通常 ≤2）。回收稀疏时绝大多数调用在此直接返回 0。
    """
    i = bisect.bisect_right(GC_ENDS, t0)
    total = 0
    while i < len(GC_SPANS):
        s, e = GC_SPANS[i]
        if s >= t1:
            break
        lo, hi = max(t0, s), min(t1, e)
        if hi > lo:
            total += int((hi - lo) * 1e9)
        i += 1
    return total


def _wrap_sync(stage: str, fn):
    """同步函数的包装：计时累加到 stage，返回值原样传回（不改行为）。

    同时记 wall（perf_counter）与 cpu（process_time）：同步调用基本是纯 CPU 计算，
    两者接近；cpu 口径才是与服务核 CPU 时间对齐的量。再记该调用时段内发生的
    垃圾回收耗时（_gc_overlap_ns），报告时从阶段数字里扣除。异常也照常计时并抛回。
    """
    def wrapper(*a, **kw):
        t0 = time.monotonic()
        c0 = time.process_time_ns()
        try:
            return fn(*a, **kw)
        finally:
            t1 = time.monotonic()
            _record(stage, (t1 - t0) * 1e9, time.process_time_ns() - c0,
                    _gc_overlap_ns(t0, t1))
    wrapper.__wraps_stage__ = stage
    wrapper.__wrapped__ = fn
    return wrapper


def _wrap_async(stage: str, fn):
    """异步函数的包装：计时累加到 stage，返回值原样传回。

    注意这里**不是** async def——它是工厂，返回一个 async 包装函数；若写成 async
    def 则调用它得到的是协程对象，替掉模块里的名字后服务就坏了。

    wall 含等待 IO 的时间（读请求体的收包等待），cpu 只算真正烧核的部分——两者之差
    就是"读阶段里事件循环让出去等网络"的时间。异常也照常计时并抛回。
    """
    async def wrapper(*a, **kw):
        t0 = time.perf_counter_ns()
        c0 = time.process_time_ns()
        try:
            return await fn(*a, **kw)
        finally:
            _record(stage, time.perf_counter_ns() - t0, time.process_time_ns() - c0)
    wrapper.__wraps_stage__ = stage
    wrapper.__wrapped__ = fn
    return wrapper


def _install_ingest_sub() -> None:
    """给 ingest 内部调用的函数装计时包装（模块级 / 类方法，替换即生效）。

    这些函数只被 Engine.ingest 调用（见 flowlens/engine.py）：
      - classify、key_hashes：engine 里 `from ... import` 进来的，替换 flowlens.engine
        模块里的名字
      - GenerationalBloom.check_and_set：self.bloom 实例调用，替换类上的
      - DimBucket.observe：b.dims[d].observe 调用，替换类上的
    哈希阶段（key_hashes）同时覆盖去重用的 event_id 哈希与两维度的 key 哈希。
    """
    import flowlens.engine as eng
    import flowlens.ring as ringmod
    from flowlens.dedup import GenerationalBloom

    eng.classify = _wrap_sync("classify", eng.classify)
    eng.key_hashes = _wrap_sync("hash", eng.key_hashes)
    GenerationalBloom.check_and_set = _wrap_sync(
        "dedup", GenerationalBloom.check_and_set)
    ringmod.DimBucket.observe = _wrap_sync(
        "dim_write", ringmod.DimBucket.observe)
    # groupby（按秒分组）：engine.ingest 里对 np.argsort 调一次。全项目 ingest 是
    # np.argsort 的唯一写入热路径调用点（bounds/top_k 是查询路径、不在本采样里），
    # 故给 numpy.argsort 计时即可归到 groupby。
    import numpy as np
    np.argsort = _wrap_sync("groupby", np.argsort)


# ---- URL 归一化缓存命中率统计（T11a-2，T11b 适配 _normalize_url_impl） -------------
#
# 只包装 flowlens.model 自己的两个函数（不改 flowlens/ 下任何文件）：
#   - normalize_url：每条事件调用一次（validate 内）→ 总调用次数
#   - _normalize_url_impl：只在缓存未命中时被调用 → 未命中次数 + 未命中耗时
# validate 的包装（"validate" 阶段）在它们外层，未命中耗时同时进 validate 阶段
# 与 NORM_STATS——后者只是前者的一部分，不存在双重计数问题。
#
# 缓存键是剥掉 ?/# 之后的路径部分；键里含 {id} 的 URL（约一半流量）每次的路径段
# 都是随机数字/UUID/长十六进制，路径本身都不同、缓存必未命中。T11b 起带 ID 替换的
# 路径不再写缓存，所以这类键每次都是未命中——本统计仍能正确计"每次未命中"。未命中
# 时顺手判断原始 URL 的路径部分是否含 ID 段（_id_seg_like），量化"带 ID 路径导致
# 未命中"的占比。判断口径与 flowgen 的 {id} 归一化规则一致（纯数字 / UUID /
# ≥16 位十六进制）：只查键、不真做归一化，每条未命中多花约 1 微秒，只测未命中路径。


def _id_seg_like(seg: str) -> bool:
    """一个路径段是否会被归一化成 {id}（纯数字 / UUID / ≥16 位十六进制）。

    口径与 flowlens.model._is_id_segment 相同，但只做判断、不做替换（少一次
    字符串切片与正则）。
    """
    if not seg.isascii():
        return False
    if seg.isdigit():
        return True
    if len(seg) == 36 and seg[8] == seg[13] == seg[18] == seg[23] == "-":
        return all(c in "0123456789abcdefABCDEF"
                   for c in seg if c != "-")
    return len(seg) >= 16 and all(c in "0123456789abcdefABCDEF" for c in seg)


def _norm_head(raw: str) -> str:
    """剥掉第一个 ? 或 #（取更早者）及其之后的部分——与归一化缓存的键同口径。"""
    q, h = raw.find("?"), raw.find("#")
    if q == -1 or (h != -1 and h < q):
        q = h
    if q == -1:
        return raw
    return raw[:q]


def _install_norm_stats() -> None:
    """给 flowlens.model 的归一化入口装上命中率统计（只加计数、不改行为）。

    包装 model.normalize_url（总调用）与 model._normalize_url_impl（未命中才调，
    T11b 起返回 (结果, 是否发生 ID 替换)）。
    口径与 model 内部一致：命中 = _norm_cache 里查得到剥掉 ?/# 后的键（T10b 起
    缓存键是路径部分）。"带 ID 段"的判断需要原始 raw，而 _normalize_url_impl 只
    拿到键——用 _norm_pending 记录每次 normalize_url 调用的 raw，未命中时弹出配对
    （validate 同步执行、同一事件循环线程内配对不会错位）。
    """
    import flowlens.model as model

    orig_norm = model.normalize_url
    orig_inner = model._normalize_url_impl
    pending: list[str] = []

    def norm_wrapper(raw):
        NORM_STATS[0] += 1
        head = _norm_head(raw)
        # 同步单线程、无 await 间隔，此处的缓存状态与 orig_norm 内部查表一致
        if model._norm_cache.get(head) is None:
            pending.append(raw)
        return orig_norm(raw)

    def inner_wrapper(head):
        c0 = time.perf_counter_ns()
        out = orig_inner(head)
        dt = time.perf_counter_ns() - c0
        NORM_STATS[1] += 1
        NORM_STATS[2] += dt
        if pending:
            raw = pending.pop()
            if any(_id_seg_like(p) for p in _norm_head(raw).split("/")):
                NORM_STATS[3] += 1
        return out

    norm_wrapper.__wraps_stage__ = "norm"
    norm_wrapper.__wrapped__ = orig_norm
    inner_wrapper.__wraps_stage__ = "norm_miss"
    inner_wrapper.__wrapped__ = orig_inner
    model.normalize_url = norm_wrapper
    model._normalize_url_impl = inner_wrapper


def install_wrappers(engine) -> None:
    """给写入路径与 tick 路径的全部包装计时装好。"""
    import flowlens.api.app as appmod
    import flowlens.ingest as ingestmod
    from flowlens.engine import Engine

    # 顶层：读请求体（异步，含收包等待）
    appmod._read_raw_body = _wrap_async("read_body", appmod._read_raw_body)
    # 顶层：解压 / 解析 / 校验（ingest.py 里 from ... import 进来，替换 flowlens.ingest
    # 的名字——注意 IngestService 引用的也是这些模块全局名）
    ingestmod.decode_body = _wrap_sync("decode", ingestmod.decode_body)
    ingestmod.load_array = _wrap_sync("load_array", ingestmod.load_array)
    ingestmod.validate = _wrap_sync("validate", ingestmod.validate)
    # 顶层：引擎写入（整体），细分见 _install_ingest_sub
    Engine.ingest = _wrap_sync("ingest", Engine.ingest)
    _install_ingest_sub()
    # 顶层：每秒 tick——定稿（advance）与快照分开
    Engine.advance = _wrap_sync("tick_finalize", Engine.advance)
    # 快照的 CPU 在同步的 _dim_tops / _snapshot_stats 里（refresh_snapshot_async 是
    # async，两个维度之间 await sleep(0) 让出，用 process_time 测它会连"挂起期间
    # 事件循环干的 ingest 活"一起算，虚高）。这两个方法只被快照路径调用，且不 yield，
    # process_time 干净，合并记为 tick_snapshot。
    Engine._dim_tops = _wrap_sync("tick_snapshot", Engine._dim_tops)
    Engine._snapshot_stats = _wrap_sync("tick_snapshot", Engine._snapshot_stats)
    # URL 归一化缓存命中率（T11a-2）
    _install_norm_stats()


# ===================== serve 模式的后台探针 =====================

async def _loop_latency_probe(interval: float, stop: "asyncio.Event") -> None:
    """每 interval 秒调度一个小任务，记录"计划执行时刻"到"实际执行时刻"的延迟。

    主协程睡到下一个计划时刻 t，call_soon 一个立即执行的回调，再 await 它完成；
    回调实际跑到的时刻与 t 之差就是事件循环被长任务 / gc / tick 占住的额外延迟。
    空闲时接近 0，被独占时飙高——正是决定查询 p99 的量。
    """
    loop = asyncio.get_running_loop()
    next_t = time.monotonic() + interval
    while not stop.is_set():
        delay = next_t - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        if stop.is_set():
            break
        planned = time.monotonic()
        done = asyncio.Event()

        def _task():
            if MEASURE_START[0] > 0:
                LOOP_LAT_US.append((time.monotonic() - planned) * 1e6)
            done.set()

        loop.call_soon(_task)
        await done.wait()
        next_t = planned + interval


# gc 回收计时：回调阶段是字符串 'start'（某代回收开始前）/ 'stop'（整个回收结束后），
# 不是 gc.START/gc.FINISH（这两个名字在 CPython 里根本不存在，比较会抛 AttributeError、
# 被 gc 回调静默吞掉）。一次回收可能跨多代（每代一个 start，最后一个 stop），
# 这里把一次回收记为一条：从第一个 start 到 stop 的总耗时，代数取最高代。
# 同时记墙钟区间（monotonic），供各阶段"扣除 GC"算交叠（见 _gc_overlap_ns）。
_gc_t0 = [0]
_gc_w0 = [0.0]
_gc_gen = [0]
_gc_active = [False]


def _gc_callback(phase, info) -> None:
    if phase == "start":
        if not _gc_active[0]:
            _gc_t0[0] = time.perf_counter_ns()
            _gc_w0[0] = time.monotonic()
            _gc_active[0] = True
        _gc_gen[0] = max(_gc_gen[0], info.get("generation", 0))
    elif phase == "stop" and _gc_active[0]:
        _gc_active[0] = False
        w_end = time.monotonic()
        if MEASURE_START[0] > 0:
            GC_EVENTS.append({
                "gen": _gc_gen[0],
                "t": round(w_end - MEASURE_START[0], 4),
                "us": round((time.perf_counter_ns() - _gc_t0[0]) / 1000.0, 2),
                "collected": info.get("collected", 0),
                "s": _gc_w0[0],
                "e": w_end,
            })
            GC_SPANS.append((_gc_w0[0], w_end))
            GC_ENDS.append(w_end)
        _gc_gen[0] = 0


async def _query_probe(query_service, interval: float, stop: "asyncio.Event") -> None:
    """每 interval 秒发一次真实查询（默认 60 s 窗口、k=10，走快照路径），
    记端到端 wall 延迟（毫秒）。

    直接回答"低负载下查询 p99 能否低于 50 ms"（T11a-2 第三节第 5 条）。探针本身
    占一点 CPU（一次 60 s 窗口的快照 topk，毫秒级），只在需要测查询延迟的那一组
    （6 万/秒）开启——它会轻微压低该组的吞吐数字，报告里注明。
    """
    from flowlens.dims import Dim
    while not stop.is_set():
        await asyncio.sleep(interval)
        if stop.is_set() or MEASURE_START[0] <= 0:
            continue
        try:
            t0 = time.monotonic()
            await query_service.topk(Dim.URL, 60_000, 10)
            QUERY_LAT_MS.append((time.monotonic() - t0) * 1000.0)
        except Exception:
            pass  # 查询失败不影响测量主体，跳过该次


async def _mem_watch(stop: "asyncio.Event") -> None:
    """每秒读一次真实工作集（memstat.memory_bytes，非假读数），记录并查 220 MB 上限。"""
    from flowlens import memstat
    while not stop.is_set():
        try:
            mb = memstat.memory_bytes() / (1024 * 1024)
        except Exception:
            mb = 0.0
        if MEASURE_START[0] > 0:
            MEM_MB.append(round(mb, 1))
            if mb > 220.0:
                PHASE["phase"] = "stopping_mem"
                print(f"[profile] 真实工作集 {mb:.1f} MB 超过 220 MB，提前结束测量",
                      flush=True)
                stop.set()
                return
        await asyncio.sleep(1.0)


# ===================== serve 模式 =====================

def _percentile(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def _gc_summary() -> dict:
    """垃圾回收的单独汇总（T11a-2）：总耗时、按代分列（次数/总耗时/最长一次）、
    占核比例。GC 单独计账后，各阶段表里报的是"扣除 GC 后"的自身开销。"""
    total_us = round(sum(g["us"] for g in GC_EVENTS), 1)
    dur = (time.monotonic() - MEASURE_START[0]) if MEASURE_START[0] > 0 else 0.0
    cpu_s = (time.process_time() - MEASURE_CPU0[0]) if MEASURE_START[0] > 0 else 0.0
    per_gen = {}
    for gen in (0, 1, 2):
        evs = [g for g in GC_EVENTS if g["gen"] == gen]
        per_gen[str(gen)] = {
            "count": len(evs),
            "total_us": round(sum(g["us"] for g in evs), 1),
            "max_us": round(max((g["us"] for g in evs), default=0.0), 2),
        }
    return {
        "total": len(GC_EVENTS),
        "per_sec": round(len(GC_EVENTS) / dur, 2) if dur else 0.0,
        "total_us": total_us,
        # GC 烧的 CPU 占服务核的比例：GC 也是单核上的活，占多少核就多少"核占比"
        "pct_of_core": round(total_us / 1e6 / cpu_s * 100.0, 2) if cpu_s > 0 else 0.0,
        "per_gen": per_gen,
    }


def _norm_summary() -> dict:
    """URL 归一化缓存的命中统计（T11a-2）。"""
    total, miss, miss_ns, idlike = NORM_STATS
    return {
        "calls": total,
        "misses": miss,
        "hit_rate": round(1 - miss / total, 4) if total else None,
        # 未命中时的平均耗时（微秒/次）：含正则、ID 段判断与缓存回填
        "miss_us_avg": round(miss_ns / 1000.0 / miss, 3) if miss else None,
        # 未命中里"路径带 ID 段（归一化后必是 {id}、路径本身不同、缓存必未命中）"
        # 的占比——量化"未命中是由随机 ID 路径造成"的程度
        "miss_with_id": idlike,
        "miss_with_id_rate": round(idlike / miss, 4) if miss else None,
    }


def _collect_report(engine) -> dict:
    """把全局统计汇总成一份报告 dict。

    垃圾回收单独计账（T11a-2）：各阶段的 wall_us / cpu_us 是**扣除 GC 后**的值
    （该调用时段内发生的回收耗时，gc_us，已从里面减掉）；顶层"阶段之和"校验用
    "扣除后阶段之和 + GC 总耗时"对比进程 CPU（GC 本身也烧进程 CPU，只是不归任何
    阶段）。
    """
    dur = (time.monotonic() - MEASURE_START[0]) if MEASURE_START[0] > 0 else 0.0
    cpu_s = (time.process_time() - MEASURE_CPU0[0]) if MEASURE_START[0] > 0 else 0.0
    c = engine.counters
    # 测量窗口内的事件数（累计值减去测量起点时的值）——预热期也在写入，用累计值
    # 会把预热算进来、把每条 CPU 时间稀释
    win = [c.events_ingested - MEASURE_EVENTS0[0],
           c.deduped - MEASURE_EVENTS0[1],
           c.late - MEASURE_EVENTS0[2],
           c.invalid - MEASURE_EVENTS0[3]]
    win_accepted = max(0, win[0])
    gc_total_us = sum(g["us"] for g in GC_EVENTS)
    stages = {}
    for name in ALL_STAGES:
        e = STAGES.get(name)
        if e:
            wall_us = e[0] / 1000.0
            cpu_us = e[2] / 1000.0
            gc_us = e[3] / 1000.0
            io_bound = name in IO_BOUND_STAGES
            # 扣除 GC：单线程下回收烧的 CPU 同时体现在 wall 与 cpu 里（相等），
            # 两边各扣一次
            wall_net_us = max(0.0, wall_us - gc_us)
            cpu_net_us = max(0.0, cpu_us - gc_us)
            # IO 等待阶段：测得的 process_time 差值不可归为它自己的 CPU（含挂起期间
            # 事件循环干的其他活），CPU 口径按 0 计，只报 wall。
            attr_cpu_us = 0.0 if io_bound else cpu_net_us
            stages[name] = {
                "wall_us": round(wall_net_us, 1),
                "cpu_us": round(attr_cpu_us, 1),
                # 扣除项：该阶段调用时段内发生的 GC 耗时
                "gc_us": round(gc_us, 1),
                "cpu_us_measured": round(cpu_us, 1) if io_bound else None,
                "io_bound": io_bound,
                "count": e[1],
                "us_per_call": round(wall_net_us / e[1], 4) if e[1] else 0.0,
                # 每条被接受事件的该阶段 CPU 时间（微秒），按测量窗口的接受数；
                # IO 阶段记 0（见 cpu_us）
                "us_per_event": round(attr_cpu_us / win_accepted, 4)
                if win_accepted else 0.0,
                # 占进程 CPU 时间比例（cpu 口径，扣除 GC 后，IO 阶段恒 0）
                "cpu_pct": round(attr_cpu_us / 1e6 / cpu_s * 100.0, 2)
                if cpu_s > 0 else 0.0,
            }
        else:
            stages[name] = {"wall_us": 0, "cpu_us": 0, "gc_us": 0,
                            "cpu_us_measured": None,
                            "io_bound": name in IO_BOUND_STAGES, "count": 0,
                            "us_per_call": 0.0, "us_per_event": 0.0, "cpu_pct": 0.0}
    lat = LOOP_LAT_US
    # 顶层阶段之和（wall / cpu，均扣除 GC 后；IO 阶段的 cpu_us 恒 0）
    top_wall_us = sum(stages[s]["wall_us"] for s in TOP_STAGES)
    top_cpu_us = sum(stages[s]["cpu_us"] for s in TOP_STAGES)
    # 校验口径：扣除后的阶段之和 + GC 总耗时 ≈ 进程 CPU（GC 烧的 CPU 已归 GC，
    # 不在任何阶段里）
    frac = (top_cpu_us + gc_total_us) / 1e6 / cpu_s if cpu_s > 0 else 0.0
    return {
        "measure_sec": round(dur, 2),
        "phase": PHASE["phase"],
        # 进程 CPU 时间（秒，time.process_time 差值）：校验"各阶段之和 + GC ≈
        # 服务核 CPU 时间"的参照。服务固定单核，所以 cpu_s / dur ≈ 该核的占用比例。
        "cpu_time_s": round(cpu_s, 3),
        "cpu_pct_of_core": round(cpu_s / dur * 100.0, 1) if dur else 0.0,
        # 测量窗口内的事件数（不是容器生命周期累计——预热期也在写入）
        "events": {
            "accepted": win[0],
            "deduped": win[1],
            "late": win[2],
            "invalid": win[3],
        },
        # 实际接受速率（条/秒）与每条被接受事件的平均 CPU 时间（微秒），均按测量窗口
        "accepted_rate": round(win[0] / dur, 1) if dur else 0.0,
        "cpu_us_per_accepted_event": round(
            cpu_s / win[0] * 1e6, 3) if win[0] > 0 else 0.0,
        "stages": stages,
        # 顶层阶段之和（wall / cpu，微秒，均扣除 GC 后）
        "top_stages_wall_us": round(top_wall_us, 1),
        "top_stages_cpu_us": round(top_cpu_us, 1),
        # 校验：（扣除后阶段之和 + GC）占进程 CPU 的比例，应接近 1.0；< 1 说明还有
        # 没包到的 Python 时间——如 aiohttp 协议处理、JSON 响应序列化、定时器回调
        "top_stages_frac_of_cpu": round(frac, 3),
        "norm_cache": _norm_summary(),
        "loop_latency_us": {
            "count": len(lat),
            "p50": _percentile(lat, 0.5),
            "p99": _percentile(lat, 0.99),
            "max": round(max(lat), 2) if lat else 0.0,
        },
        # 查询延迟（仅开启 --query-probe 的那组有值；60 s 窗口、k=10 快照查询，
        # 每 250 ms 一次，wall 毫秒）
        "query_latency_ms": {
            "count": len(QUERY_LAT_MS),
            "p50": _percentile(QUERY_LAT_MS, 0.5),
            "p99": _percentile(QUERY_LAT_MS, 0.99),
            "max": round(max(QUERY_LAT_MS), 2) if QUERY_LAT_MS else 0.0,
        },
        "gc": _gc_summary(),
        "mem_mib": {
            "peak": max(MEM_MB, default=0.0),
            "end": MEM_MB[-1] if MEM_MB else 0.0,
            "series": MEM_MB,
        },
    }


async def _serve(args) -> None:
    os.environ.setdefault("PYTHONHASHSEED", "0")

    from aiohttp import web
    from flowlens.api.app import create_app, make_engine
    from flowlens.timewin import Clock

    # 与 __main__ 完全相同的创建方式，只是 memory_reader 用假读数（50 MB / 256 MB）
    engine, ingest_service, query_service = make_engine(Clock())
    app = create_app(engine, ingest_service, query_service,
                     memory_reader=lambda: (50 * 2**20, 256 * 2**20))

    install_wrappers(engine)
    gc.callbacks.append(_gc_callback)

    start_ev = asyncio.Event()   # run 模式起好 loadgen 后 POST /prof/start
    stop_ev = asyncio.Event()    # 提前结束（内存超限或 run 模式 POST /prof/stop）

    async def _prof_state(request):
        return web.json_response({
            "phase": PHASE["phase"],
            "measure_started": MEASURE_START[0] > 0,
            "accepted": engine.counters.events_ingested,
        })

    async def _prof_start(request):
        if not start_ev.is_set():
            PHASE["phase"] = "warming"
            print(f"[profile] 收到 start，预热 {args.warmup}s", flush=True)
            start_ev.set()
        return web.json_response({"ok": True})

    async def _prof_stop(request):
        PHASE["phase"] = "stopped_by_client"
        stop_ev.set()
        return web.json_response({"ok": True})

    app.router.add_get("/prof/state", _prof_state)
    app.router.add_post("/prof/start", _prof_start)
    app.router.add_post("/prof/stop", _prof_stop)

    runner = web.AppRunner(app, auto_decompress=False, lingering_time=1.0)
    await runner.setup()
    site = web.TCPSite(runner, args.host, args.port)
    await site.start()
    PHASE["phase"] = "ready"
    print(f"[profile] 服务就绪，等待 run 模式起 loadgen 后发 /prof/start；"
          f"预热 {args.warmup}s + 测量 {args.measure}s", flush=True)

    loop_task = asyncio.create_task(_loop_latency_probe(0.1, stop_ev))
    mem_task = asyncio.create_task(_mem_watch(stop_ev))
    # 查询延迟探针（默认关，只在 6 万/秒那组开——那组专门测查询延迟）
    qtask = (asyncio.create_task(_query_probe(query_service, 0.25, stop_ev))
             if getattr(args, "query_probe", False) else None)
    tasks = [t for t in (loop_task, mem_task, qtask) if t is not None]
    try:
        # 等 run 模式确认 loadgen 已起（保证预热期就有流量）
        await start_ev.wait()
        # 预热（期间 loadgen 已在发，让缓存 / 数据结构热起来）
        await asyncio.sleep(args.warmup)
        if stop_ev.is_set():
            PHASE["phase"] = PHASE["phase"] or "aborted_warmup"
        else:
            # 清零并开始测量
            for k in list(STAGES):
                STAGES[k] = [0, 0, 0, 0]
            LOOP_LAT_US.clear()
            GC_EVENTS.clear()
            GC_SPANS.clear()
            GC_ENDS.clear()
            NORM_STATS[:] = [0, 0, 0, 0]
            QUERY_LAT_MS.clear()
            MEM_MB.clear()
            MEASURE_START[0] = time.monotonic()
            MEASURE_CPU0[0] = time.process_time()
            _ce = engine.counters
            MEASURE_EVENTS0[:] = [_ce.events_ingested, _ce.deduped, _ce.late,
                                  _ce.invalid]
            PHASE["phase"] = "measuring"
            print("[profile] 测量开始", flush=True)
            # 测量：按墙钟截止时间跑满 measure 秒（T11a-2 修正）。上一版用
            # sleep(0.5)×N 累计名义时长——满载下每个 sleep 都被事件循环延迟触发
            # （实测循环延迟 p50 68 ms），名义 30 s 拖成 47 s，而 loadgen 只发到
            # 70 s，窗口尾部 ~10 s 没流量，把 accepted_rate 与核占比稀释了。改为
            # deadline = monotonic() + measure，窗口长度与负载无关。
            deadline = time.monotonic() + args.measure
            while time.monotonic() < deadline and not stop_ev.is_set():
                await asyncio.sleep(0.1)
            if not stop_ev.is_set():
                PHASE["phase"] = "done"
            print(f"[profile] 测量结束（相位 {PHASE['phase']}）", flush=True)
    finally:
        stop_ev.set()
        for t in tasks:
            t.cancel()
        report = _collect_report(engine)
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        print(f"[profile] 统计已写入 {out}"
              f"（accepted={report['events']['accepted']}）", flush=True)
        await runner.cleanup()


def serve_entry(args) -> None:
    """serve 入口：启动服务、装计时、跑完预热+测量后写 JSON 退出。"""
    asyncio.run(_serve(args))


# ===================== run 模式 =====================

SVC_CORE = "0"
LOAD_CORES = "1-6"
HOST_PORT = 9000
# 仓库根目录按本文件位置推算（tools/ 的上一级），不写死任何机器上的路径
REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_TOOLS = REPO_ROOT / "tools"
IMAGE = "flowlens:latest"
# 结果默认落到仓库下的 results/T11a，可用 --out-root 覆盖
DEFAULT_OUT_ROOT = REPO_ROOT / "results" / "T11a"


def _has(cmd):
    return shutil.which(cmd) is not None


def _run(cmd, **kw):
    # Windows 中文系统默认 GBK 解码 UTF-8 输出会报 UnicodeDecodeError，显式 utf-8
    return subprocess.run(cmd, capture_output=True, text=True,
                           encoding="utf-8", errors="replace", **kw)


# run 模式自己起的 /prof 探针对 localhost 发 HTTP：WSL 的 autoProxy 可能把
# 127.0.0.1 也走代理（T8b 决策），故建一个禁用代理的 opener，保证直连服务。
_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

# T11a-2 的两次固定速率测量（任务书第二节）：
#   - 10 万/秒：接近单核上限（每条 ~10.7 μs CPU 的倒数约 9.3 万/秒），测处理能力
#   - 6 万/秒：较低负载，测事件循环延迟与垃圾回收停顿
# 其余参数与上次相同：每批 5000 条、不 gzip、长尾、预热 30 s + 测量 30 s。
# 不再跑 20 万/秒——那是过载，测的是过载行为而非处理能力。
RUN_BATCH = 5_000
RUN_GZIP = False
RUN_WARMUP = 30.0
RUN_MEASURE = 30.0
# (run_name, rate, query_probe)：query_probe 只在 6 万/秒那组开（那组专门测查询
# 延迟，探针的少量 CPU 开销可接受）；10 万/秒组不开，避免探针干扰处理能力测量。
RUNS = [
    ("run_100k", 100_000, False),
    ("run_60k", 60_000, True),
]
# 测量窗口前的启动延迟：run 模式起好 loadgen 后等 3 s 建连才通知 serve 预热。
# loadgen 的时长必须覆盖"这 3 s + 预热 + 测量"全程，再多留 10 s 余量（任务书
# 第一节第 1 条）——上一版只留了 10 s、没算启动延迟，窗口尾部没流量。
LOADGEN_TAIL_SEC = 10
STARTUP_DELAY_SEC = 3.0


def _http_get(url, timeout=2):
    with _NO_PROXY_OPENER.open(url, timeout=timeout) as r:
        return json.loads(r.read())


def _http_post(url, body=None):
    data = json.dumps(body).encode() if body is not None else b""
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    with _NO_PROXY_OPENER.open(req, timeout=5) as r:
        return json.loads(r.read())


def _wait_phase(port, want, timeout=60.0):
    """轮询 /prof/state 直到相位为 want（就绪）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            st = _http_get(f"http://127.0.0.1:{port}/prof/state")
            if st.get("phase") == want:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def _loadgen_python(override: str | None = None) -> str:
    """loadgen 的 Python 解释器：默认当前解释器（sys.executable）。

    优先级：命令行 --loadgen-py > 环境变量 FLOWLENS_LOADGEN_PY > sys.executable。
    覆盖的路径不存在时回退到当前解释器，保证换机器不报错。
    """
    candidate = (override or os.environ.get("FLOWLENS_LOADGEN_PY")
                 or sys.executable)
    return candidate if os.path.exists(candidate) else sys.executable


def _parse_loadgen_summary(stdout: str) -> dict | None:
    """从 loadgen 输出里抓 type=summary 那行 JSON。"""
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        if d.get("type") == "summary":
            return d
    return None


def _loadgen_base_cmd(rate: int, loadgen_py: str | None = None) -> list[str]:
    """loadgen 基础命令（不含时长），压测进程固定到压测核。"""
    loadgen = _loadgen_python(loadgen_py)
    cmd = [loadgen, "tools/loadgen.py", "--url", f"http://127.0.0.1:{HOST_PORT}",
           "--workers", "4", "--conc", "8", "--mode", "长尾",
           "--rate", str(rate), "--batch", str(RUN_BATCH)]
    if _has("taskset"):
        cmd = ["taskset", "-c", LOAD_CORES] + cmd
    if RUN_GZIP:
        cmd.append("--gzip")
    return cmd


async def _run_once(rate: int, run_name: str, query_probe: bool, warmup, measure,
                    dry=False, out_root: str | None = None,
                    loadgen_py: str | None = None) -> dict:
    """单次固定速率测量（rate / RUN_BATCH / 不 gzip）。

    起容器（serve）→ 等 ready → 起 loadgen（跑 启动延迟+warmup+measure+余量）→
    通知 serve 预热 → docker wait 容器退出 → 收 loadgen 汇总 → 读 serve 的 JSON。
    loadgen 时长覆盖整个预热与测量窗口并多留 10 s 余量（任务书第一节第 1 条）。
    query_probe 为 True 时给 serve 加 --query-probe（开查询延迟探针）。
    """
    import uuid
    cname = f"flowlens-probe-{uuid.uuid4().hex[:8]}"
    outdir = os.path.join(out_root or DEFAULT_OUT_ROOT, run_name)
    os.makedirs(outdir, exist_ok=True)
    try:
        os.chmod(outdir, 0o777)
    except OSError:
        pass
    out_json = f"/out/{run_name}.json"
    report: dict = {"name": run_name, "container": cname}

    if not dry:
        docker = ["docker", "run", "--rm", "-d", "--name", cname,
                  "--memory=256m", "--memory-swap=256m",
                  f"--cpuset-cpus={SVC_CORE}",
                  "-p", f"{HOST_PORT}:8080",
                  "-v", f"{REPO_TOOLS.as_posix()}:/probe:ro",
                  "-v", f"{outdir}:/out",
                  IMAGE,
                  "python", "/probe/profile_server.py", "serve",
                  "--host", "0.0.0.0", "--port", "8080",
                  "--warmup", str(warmup), "--measure", str(measure),
                  "--out", out_json]
        if query_probe:
            docker.append("--query-probe")
        r = _run(docker)
        if r.returncode != 0:
            report["error"] = f"docker run 失败: {r.stderr[-500:]}"
            return report
        ok = await asyncio.to_thread(_wait_phase, HOST_PORT, "ready", 60.0)
        if not ok:
            logs = _run(["docker", "logs", cname])
            _run(["docker", "rm", "-f", cname])
            report["error"] = f"服务未就绪:\n{logs.stderr[-800:]}"
            return report

    # loadgen 时长 = 启动延迟 + 预热 + 测量 + 余量：预热与测量全程都有流量
    loadgen_dur = STARTUP_DELAY_SEC + warmup + measure + LOADGEN_TAIL_SEC
    env = dict(os.environ, NO_PROXY="localhost,127.0.0.1", PYTHONPATH=".",
               PYTHONHASHSEED="0")
    proc = None
    try:
        cmd = _loadgen_base_cmd(rate, loadgen_py) + ["--duration", str(loadgen_dur)]
        print(f"[run] {run_name} rate={rate} batch={RUN_BATCH} "
              f"loadgen_dur={loadgen_dur}s", flush=True)
        if not dry:
            proc = subprocess.Popen(cmd, cwd=REPO_ROOT, env=env,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True,
                                    encoding="utf-8", errors="replace")
            # 起好（留 STARTUP_DELAY_SEC 建连）后通知 serve 开始预热
            await asyncio.sleep(STARTUP_DELAY_SEC)
            await asyncio.to_thread(
                _http_post, f"http://127.0.0.1:{HOST_PORT}/prof/start")
    finally:
        # 异常时兜底杀掉 loadgen；正常路径要等容器退出后再收
        if proc is not None and "error" in report:
            proc.kill()
            proc = None

    loadgen_summary = None
    if not dry:
        # 等容器退出（serve 在测量结束后自行退出）
        await asyncio.to_thread(_run, ["docker", "wait", cname])
        if proc is not None:
            # 容器已退出，收 loadgen 剩余输出（它跑满 loadgen_dur）
            try:
                out, _ = proc.communicate(timeout=LOADGEN_TAIL_SEC + 20)
                loadgen_summary = _parse_loadgen_summary(out or "")
            except subprocess.TimeoutExpired:
                proc.kill()
                out, _ = proc.communicate()
                loadgen_summary = _parse_loadgen_summary(out or "")
            proc = None
    host_json = os.path.join(outdir, run_name + ".json")
    if os.path.exists(host_json):
        report["server"] = json.loads(Path(host_json).read_text(encoding="utf-8"))
    report["loadgen"] = loadgen_summary
    return report


async def run_main(warmup, measure, dry=False, only: str | None = None,
                   out_root: str | None = None,
                   loadgen_py: str | None = None) -> None:
    """跑 T11a-2 的两组（10 万/秒、6 万/秒），依次各起一个全新容器。

    每组独立：全新容器（serve）、全新 loadgen、结果各自落盘。--only 可只跑其中
    一组（按 run_name 前缀过滤），便于单独重跑某组。out_root 为 None 时用
    默认的仓库内 results/T11a。
    """
    out_root = out_root or str(DEFAULT_OUT_ROOT)
    if not dry:
        chk = _run(["docker", "image", "inspect", IMAGE])
        if chk.returncode != 0:
            print(f"[run] 镜像 {IMAGE} 不存在，构建中（--target app）...", flush=True)
            b = _run(["docker", "build", "--target", "app", "-t", IMAGE,
                      str(REPO_ROOT)])
            if b.returncode != 0:
                raise RuntimeError(f"构建失败: {b.stderr[-500:]}")
    runs = [(n, r, q) for n, r, q in RUNS if only is None or n == only]
    os.makedirs(out_root, exist_ok=True)
    for run_name, rate, query_probe in runs:
        r = await _run_once(rate, run_name, query_probe, warmup, measure, dry=dry,
                            out_root=out_root, loadgen_py=loadgen_py)
        status = "OK" if "error" not in r else r["error"][:120]
        print(f"[run] {run_name} 完成: {status}", flush=True)
        out_path = os.path.join(out_root, f"{run_name}.json")
        Path(out_path).write_text(json.dumps(r, ensure_ascii=False, indent=2),
                                  encoding="utf-8")
        print(f"[run] 结果写入 {out_path}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(
        description="T11a 端到端 CPU 剖析（10 万/秒 与 6 万/秒 两组固定速率）")
    p.add_argument("mode", choices=["serve", "run"])
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--warmup", type=float, default=RUN_WARMUP)
    p.add_argument("--measure", type=float, default=RUN_MEASURE)
    p.add_argument("--out", default=None, help="serve 模式输出 JSON 路径")
    p.add_argument("--query-probe", action="store_true",
                   help="serve：开查询延迟探针（60s 窗口 k=10，每 250ms 一次）")
    p.add_argument("--only", default=None,
                   choices=[n for n, _, _ in RUNS],
                   help="run：只跑指定的一组（默认两组都跑）")
    p.add_argument("--out-root", default=None,
                   help=f"run：结果根目录（默认 {DEFAULT_OUT_ROOT}）")
    p.add_argument("--loadgen-py", default=None,
                   help="run：loadgen 用的 Python 解释器"
                        f"（默认当前解释器，也可用环境变量 FLOWLENS_LOADGEN_PY）")
    p.add_argument("--dry-run", action="store_true", help="run：只打印，不真正跑")
    args = p.parse_args()

    if args.mode == "serve":
        if not args.out:
            p.error("serve 模式需要 --out")
        serve_entry(args)
    else:
        asyncio.run(run_main(args.warmup, args.measure, dry=args.dry_run,
                             only=args.only, out_root=args.out_root,
                             loadgen_py=args.loadgen_py))


if __name__ == "__main__":
    main()
