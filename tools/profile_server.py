"""T11a 端到端 CPU 剖析工具（两种模式，任务书见 docs/instructions/T11a.md）。

只做一次固定速率测量：速率 20 万/秒、每批 5000 条、不 gzip，预热 30 s + 测量
30 s（loadgen 时长 = 预热 + 测量 + 收尾余量）。回答"在不受内存降级干扰的情况下，
服务处理每条事件的 CPU 时间花在了哪里"。

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

run 模式（WSL 宿主上运行）：
  - 启动容器（serve 模式、--memory=256m、固定 1 个服务核），等 /prof/state 就绪，
    用 taskset 在压测核上后台起 loadgen，再通知 serve 开始预热/测量，docker wait
    容器退出后读取 JSON 并汇总

核的分配与 T10b 相同：服务 0 号核，压测 1-6 号核。宿主端口 9000（本机 WSL2 的
80xx 段被保留，见 docs/PROGRESS.md 决策）。容器以非 root（uid 10001）运行，输出
目录须事先在宿主建好并 chmod 777。
"""

import argparse
import asyncio
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
# 每次 gc 回收的事件：{"gen", "t"(相对测量起点，秒), "us", "collected"}
GC_EVENTS: list[dict] = []
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


def _record(name: str, wall_ns: int, cpu_ns: int) -> None:
    # STAGES[name] = [累计 wall ns, 调用次数, 累计 cpu ns]
    e = STAGES.get(name)
    if e is None:
        e = STAGES[name] = [0, 0, 0]
    e[0] += wall_ns
    e[1] += 1
    e[2] += cpu_ns


def _wrap_sync(stage: str, fn):
    """同步函数的包装：计时累加到 stage，返回值原样传回（不改行为）。

    同时记 wall（perf_counter）与 cpu（process_time）：同步调用基本是纯 CPU 计算，
    两者接近；cpu 口径才是与服务核 CPU 时间对齐的量。异常也照常计时并抛回。
    """
    def wrapper(*a, **kw):
        t0 = time.perf_counter_ns()
        c0 = time.process_time_ns()
        try:
            return fn(*a, **kw)
        finally:
            _record(stage, time.perf_counter_ns() - t0, time.process_time_ns() - c0)
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
_gc_t0 = [0]
_gc_gen = [0]
_gc_active = [False]


def _gc_callback(phase, info) -> None:
    if phase == "start":
        if not _gc_active[0]:
            _gc_t0[0] = time.perf_counter_ns()
            _gc_active[0] = True
        _gc_gen[0] = max(_gc_gen[0], info.get("generation", 0))
    elif phase == "stop" and _gc_active[0]:
        _gc_active[0] = False
        if MEASURE_START[0] > 0:
            GC_EVENTS.append({
                "gen": _gc_gen[0],
                "t": round(time.monotonic() - MEASURE_START[0], 4),
                "us": round((time.perf_counter_ns() - _gc_t0[0]) / 1000.0, 2),
                "collected": info.get("collected", 0),
            })
        _gc_gen[0] = 0


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


def _collect_report(engine) -> dict:
    """把全局统计汇总成一份报告 dict。"""
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
    stages = {}
    for name in ALL_STAGES:
        e = STAGES.get(name)
        if e:
            wall_us = e[0] / 1000.0
            cpu_us = e[2] / 1000.0
            io_bound = name in IO_BOUND_STAGES
            # IO 等待阶段：测得的 process_time 差值不可归为它自己的 CPU（含挂起期间
            # 事件循环干的其他活），CPU 口径按 0 计，只报 wall。
            attr_cpu_us = 0.0 if io_bound else cpu_us
            stages[name] = {
                "wall_us": round(wall_us, 1),
                "cpu_us": round(attr_cpu_us, 1),
                "cpu_us_measured": round(cpu_us, 1) if io_bound else None,
                "io_bound": io_bound,
                "count": e[1],
                "us_per_call": round(wall_us / e[1], 4) if e[1] else 0.0,
                # 每条被接受事件的该阶段 CPU 时间（微秒），按测量窗口的接受数；
                # IO 阶段记 0（见 cpu_us）
                "us_per_event": round(attr_cpu_us / win_accepted, 4)
                if win_accepted else 0.0,
                # 占进程 CPU 时间比例（cpu 口径，IO 阶段恒 0）
                "cpu_pct": round(attr_cpu_us / 1e6 / cpu_s * 100.0, 2)
                if cpu_s > 0 else 0.0,
            }
        else:
            stages[name] = {"wall_us": 0, "cpu_us": 0, "cpu_us_measured": None,
                            "io_bound": name in IO_BOUND_STAGES, "count": 0,
                            "us_per_call": 0.0, "us_per_event": 0.0, "cpu_pct": 0.0}
    lat = LOOP_LAT_US
    gen2 = [g for g in GC_EVENTS if g["gen"] == 2]
    top_wall_us = sum(STAGES[s][0] for s in TOP_STAGES if s in STAGES) / 1000.0
    # CPU 口径的顶层阶段之和：只统计非 IO 阶段（IO 阶段的 process_time 不可归属）
    top_cpu_us = sum(STAGES[s][2] for s in TOP_STAGES
                     if s in STAGES and s not in IO_BOUND_STAGES) / 1000.0
    return {
        "measure_sec": round(dur, 2),
        "phase": PHASE["phase"],
        # 进程 CPU 时间（秒，time.process_time 差值）：校验"各阶段之和 ≈ 服务核 CPU
        # 时间"的参照。服务固定单核，所以 cpu_s / dur ≈ 该核的占用比例（0~1）。
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
        # 顶层阶段之和（wall / cpu，微秒）
        # 各阶段之和（wall / cpu，微秒）。cpu 口径只含非 IO 阶段（read_body 等异步
        # 阶段的 process_time 差值含"挂起期间事件循环干的其他活"，不能归属，已排除）。
        "top_stages_wall_us": round(top_wall_us, 1),
        "top_stages_cpu_us": round(top_cpu_us, 1),
        # 非 IO 阶段 CPU 之和占进程 CPU 时间的比例（校验用，应接近 1.0；< 1 说明还有
        # 没包到的 Python 时间——如 aiohttp 协议处理、JSON 响应序列化、定时器回调）
        "top_stages_frac_of_cpu": round(top_cpu_us / 1e6 / cpu_s, 3) if cpu_s > 0 else 0.0,
        "loop_latency_us": {
            "count": len(lat),
            "p50": _percentile(lat, 0.5),
            "p99": _percentile(lat, 0.99),
            "max": round(max(lat), 2) if lat else 0.0,
        },
        "gc": {
            "total": len(GC_EVENTS),
            "per_sec": round(len(GC_EVENTS) / dur, 2) if dur else 0.0,
            "gen2_count": len(gen2),
            "gen2_max_us": max((g["us"] for g in gen2), default=0.0),
            "gen2_total_us": round(sum(g["us"] for g in gen2), 1),
            "all_max_us": max((g["us"] for g in GC_EVENTS), default=0.0),
        },
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
                STAGES[k] = [0, 0, 0]
            LOOP_LAT_US.clear()
            GC_EVENTS.clear()
            MEM_MB.clear()
            MEASURE_START[0] = time.monotonic()
            MEASURE_CPU0[0] = time.process_time()
            _ce = engine.counters
            MEASURE_EVENTS0[:] = [_ce.events_ingested, _ce.deduped, _ce.late,
                                  _ce.invalid]
            PHASE["phase"] = "measuring"
            print("[profile] 测量开始", flush=True)
            # 测量：正常跑满 measure 秒；内存超限（mem_watch 置 stop_ev 并改相位为
            # stopping_mem）或 run 模式 POST /prof/stop（第 4 组跑完）时提前结束
            remaining = args.measure
            while remaining > 0 and not stop_ev.is_set():
                step = min(0.5, remaining)
                await asyncio.sleep(step)
                remaining -= step
            if not stop_ev.is_set():
                PHASE["phase"] = "done"
            print(f"[profile] 测量结束（相位 {PHASE['phase']}）", flush=True)
    finally:
        stop_ev.set()
        for t in (loop_task, mem_task):
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
REPO_ROOT = "/mnt/d/dhy/code/flowlens"
REPO_TOOLS = f"{REPO_ROOT}/tools"
IMAGE = "flowlens:latest"
OUT_ROOT = "/tmp/flowlens_t11a"
LOADGEN_PY = "/home/user/flowlens-venv/bin/python"
# 单次固定速率测量的参数（任务要求）：20 万/秒、每批 5000、不 gzip、预热 30 + 测量 30
RUN_RATE = 200_000
RUN_BATCH = 5_000
RUN_GZIP = False
RUN_WARMUP = 30.0
RUN_MEASURE = 30.0
RUN_NAME = "run_200k"
# loadgen 的时长 = 预热 + 测量 + 余量（容器退出后 loadgen 会被 terminate）
LOADGEN_TAIL_SEC = 10


def _has(cmd):
    return shutil.which(cmd) is not None


def _run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


# run 模式自己起的 /prof 探针对 localhost 发 HTTP：WSL 的 autoProxy 可能把
# 127.0.0.1 也走代理（T8b 决策），故建一个禁用代理的 opener，保证直连服务。
_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


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


def _loadgen_python():
    return LOADGEN_PY if os.path.exists(LOADGEN_PY) else sys.executable


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


def _loadgen_base_cmd() -> list[str]:
    """loadgen 基础命令（不含速率/批量/时长），压测进程固定到压测核。"""
    loadgen = _loadgen_python()
    cmd = [loadgen, "tools/loadgen.py", "--url", f"http://127.0.0.1:{HOST_PORT}",
           "--workers", "4", "--conc", "8", "--mode", "长尾",
           "--rate", str(RUN_RATE), "--batch", str(RUN_BATCH)]
    if _has("taskset"):
        cmd = ["taskset", "-c", LOAD_CORES] + cmd
    if RUN_GZIP:
        cmd.append("--gzip")
    return cmd


async def _run_once(warmup, measure, dry=False) -> dict:
    """单次固定速率测量（RUN_RATE / RUN_BATCH / 不 gzip）。

    起容器（serve）→ 等 ready → 起 loadgen（跑 warmup+measure+余量）→ 通知 serve
    预热 → docker wait 容器退出 → 收 loadgen 汇总 → 读 serve 的 JSON。
    """
    import uuid
    name = f"flowlens-probe-{uuid.uuid4().hex[:8]}"
    outdir = os.path.join(OUT_ROOT, RUN_NAME)
    os.makedirs(outdir, exist_ok=True)
    try:
        os.chmod(outdir, 0o777)
    except OSError:
        pass
    out_json = f"/out/{RUN_NAME}.json"
    report: dict = {"name": RUN_NAME, "container": name}

    if not dry:
        docker = ["docker", "run", "--rm", "-d", "--name", name,
                  "--memory=256m", "--memory-swap=256m",
                  f"--cpuset-cpus={SVC_CORE}",
                  "-p", f"{HOST_PORT}:8080",
                  "-v", f"{REPO_TOOLS}:/probe:ro",
                  "-v", f"{outdir}:/out",
                  IMAGE,
                  "python", "/probe/profile_server.py", "serve",
                  "--host", "0.0.0.0", "--port", "8080",
                  "--warmup", str(warmup), "--measure", str(measure),
                  "--out", out_json]
        r = _run(docker)
        if r.returncode != 0:
            report["error"] = f"docker run 失败: {r.stderr[-500:]}"
            return report
        ok = await asyncio.to_thread(_wait_phase, HOST_PORT, "ready", 60.0)
        if not ok:
            logs = _run(["docker", "logs", name])
            _run(["docker", "rm", "-f", name])
            report["error"] = f"服务未就绪:\n{logs.stderr[-800:]}"
            return report

    env = dict(os.environ, NO_PROXY="localhost,127.0.0.1", PYTHONPATH=".",
               PYTHONHASHSEED="0")
    proc = None
    try:
        cmd = _loadgen_base_cmd() + ["--duration",
                                     str(warmup + measure + LOADGEN_TAIL_SEC)]
        print(f"[run] {RUN_NAME} rate={RUN_RATE} batch={RUN_BATCH} "
              f"duration={warmup + measure + LOADGEN_TAIL_SEC}s", flush=True)
        if not dry:
            proc = subprocess.Popen(cmd, cwd=REPO_ROOT, env=env,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True,
                                    encoding="utf-8", errors="replace")
            # 起好（留 3 s 建连）后通知 serve 开始预热
            await asyncio.sleep(3.0)
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
        await asyncio.to_thread(_run, ["docker", "wait", name])
        if proc is not None:
            # 容器已退出，收 loadgen 剩余输出（它跑满 warmup+measure+tail）
            try:
                out, _ = proc.communicate(timeout=LOADGEN_TAIL_SEC + 20)
                loadgen_summary = _parse_loadgen_summary(out or "")
            except subprocess.TimeoutExpired:
                proc.kill()
                out, _ = proc.communicate()
                loadgen_summary = _parse_loadgen_summary(out or "")
            proc = None
    host_json = os.path.join(outdir, RUN_NAME + ".json")
    if os.path.exists(host_json):
        report["server"] = json.loads(Path(host_json).read_text(encoding="utf-8"))
    report["loadgen"] = loadgen_summary
    return report


async def run_main(warmup, measure, dry=False) -> None:
    if not dry:
        chk = _run(["docker", "image", "inspect", IMAGE])
        if chk.returncode != 0:
            print(f"[run] 镜像 {IMAGE} 不存在，构建中（--target app）...", flush=True)
            b = _run(["docker", "build", "--target", "app", "-t", IMAGE,
                      REPO_ROOT])
            if b.returncode != 0:
                raise RuntimeError(f"构建失败: {b.stderr[-500:]}")
    r = await _run_once(warmup, measure, dry=dry)
    status = "OK" if "error" not in r else r["error"][:120]
    print(f"[run] {RUN_NAME} 完成: {status}", flush=True)
    os.makedirs(OUT_ROOT, exist_ok=True)
    out_path = os.path.join(OUT_ROOT, f"{RUN_NAME}.json")
    Path(out_path).write_text(json.dumps(r, ensure_ascii=False, indent=2),
                              encoding="utf-8")
    print(f"[run] 结果写入 {out_path}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description="T11a 端到端 CPU 剖析（单次固定速率）")
    p.add_argument("mode", choices=["serve", "run"])
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--warmup", type=float, default=RUN_WARMUP)
    p.add_argument("--measure", type=float, default=RUN_MEASURE)
    p.add_argument("--out", default=None, help="serve 模式输出 JSON 路径")
    p.add_argument("--dry-run", action="store_true", help="run：只打印，不真正跑")
    args = p.parse_args()

    if args.mode == "serve":
        if not args.out:
            p.error("serve 模式需要 --out")
        serve_entry(args)
    else:
        asyncio.run(run_main(args.warmup, args.measure, dry=args.dry_run))


if __name__ == "__main__":
    main()
