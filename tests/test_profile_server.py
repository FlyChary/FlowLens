"""T11a 剖析工具自身的测试（不起 docker、不压测）。

覆盖可离线验证的部分：百分位、loadgen 汇总解析、包装计时（记录 + 不改行为）、
以及 _collect_report 的字段形状。install_wrappers 会替换 flowlens 里的名字，
测试里用完即还原，避免污染同进程其他测试。
"""

import sys
import time
from pathlib import Path

import pytest

_TOOLS = Path(__file__).resolve().parent.parent / "tools"
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

import profile_server as ps


# ---------------- 一、百分位 ----------------

def test_percentile_linear_interpolation():
    assert ps._percentile([], 0.5) is None
    assert ps._percentile([10], 0.5) == 10
    # [1,2,3,4]：p50 = 2.5（线性插值），p0=1，p99 接近 4
    assert ps._percentile([1, 2, 3, 4], 0.5) == 2.5
    assert ps._percentile([1, 2, 3, 4], 0.0) == 1
    assert ps._percentile([1, 2, 3, 4], 1.0) == 4
    # 与 numpy 口径一致（线性插值，percentile 默认 linear）
    import numpy as np
    assert ps._percentile(list(range(100)), 0.99) == pytest.approx(
        float(np.percentile(list(range(100)), 99)))


# ---------------- 二、loadgen 汇总解析 ----------------

def test_parse_loadgen_summary_picks_summary_line():
    out = '\n'.join([
        '{"type": "second", "t": 1, "accepted": 500}',
        '{"type": "second", "t": 2, "accepted": 500}',
        'some noise line',
        '{"type": "summary", "wall_s": 10.0, "accepted": 5000, '
        '"accepted_events_rate": 500.1}',
    ])
    s = ps._parse_loadgen_summary(out)
    assert s is not None
    assert s["type"] == "summary"
    assert s["accepted_events_rate"] == 500.1


def test_parse_loadgen_summary_none_when_absent():
    assert ps._parse_loadgen_summary('{"type": "second", "t": 1}\n') is None
    assert ps._parse_loadgen_summary("") is None


# ---------------- 三、包装计时：记录 + 不改行为 ----------------

def test_wrap_sync_records_and_preserves():
    ps.STAGES.clear()

    def add(a, b):
        return a + b

    wrapped = ps._wrap_sync("add", add)
    assert wrapped(2, 3) == 5
    assert wrapped(0, 0) == 0
    e = ps.STAGES.get("add")
    assert e is not None
    assert e[1] == 2                 # 调用次数
    assert e[0] >= 0 and e[2] >= 0   # wall / cpu 都非负


def test_wrap_sync_preserves_exception_and_none():
    ps.STAGES.clear()

    def boom():
        raise ValueError("x")

    wrapped = ps._wrap_sync("boom", boom)
    with pytest.raises(ValueError):
        wrapped()
    # 计时仍应记录（异常路径也计时）
    assert ps.STAGES["boom"][1] == 1


async def _no_op(a=1):
    return a * 2


def test_wrap_async_records_and_preserves():
    import asyncio
    ps.STAGES.clear()
    wrapped = ps._wrap_async("no_op", _no_op)
    r = asyncio.run(wrapped(21))
    assert r == 42
    assert ps.STAGES["no_op"][1] == 1


# ---------------- 四、install_wrappers：装包不改变写入结果 ----------------

def _make_batch(n=50):
    """构造一个合法的 EventBatch（全 URL 维可读）。"""
    from flowlens.model import EventBatch
    import numpy as np
    base_ts = 1_750_000_000_000
    return EventBatch(
        ids=[f"e{i}" for i in range(n)],
        users=[f"u{i % 5}" for i in range(n)],
        urls=[f"/api/item/x{i % 7}" for i in range(n)],
        ts=np.arange(n, dtype=np.int64) * 7 + base_ts,
        nbytes=np.full(n, 2048, dtype=np.int64),
        invalid=0,
    )


def test_install_wrappers_preserves_ingest_result():
    """装上包装后，engine.ingest 的结果（accepted 等）应与不装包装完全一致，
    且包装确实在 STAGES 里记了 ingest / 细分阶段的次数。

    用两个独立的全新 engine（同起点假时钟）：一个裸跑做基准、一个装包装，
    喂同样的批，结果必须一致（否则说明包装改变了行为）。
    """
    from flowlens.config import Config
    from flowlens.engine import Engine
    from flowlens.timewin import FakeClock

    T = 1_750_000_000_000
    base = Engine(Config(), FakeClock(T)).ingest(_make_batch())

    ps.STAGES.clear()
    eng1 = Engine(Config(), FakeClock(T))
    ps.install_wrappers(eng1)
    try:
        res = eng1.ingest(_make_batch())
    finally:
        # 还原所有被替换的名字，避免污染其他测试
        _unwrap()

    assert res.accepted == base.accepted
    assert res.deduped == base.deduped
    assert res.late == base.late
    assert res.invalid == base.invalid
    # 包装确实记到了 ingest 与若干细分阶段
    assert ps.STAGES["ingest"][1] >= 1
    assert ps.STAGES["classify"][1] >= 1
    assert ps.STAGES["hash"][1] >= 1
    assert ps.STAGES["dedup"][1] >= 1
    assert ps.STAGES["dim_write"][1] >= 1


# ---------------- 六、gc 回调：'start'/'stop' 字符串阶段 ----------------

def test_gc_callback_records_a_collection():
    """gc 回调的阶段是字符串 'start'/'stop'（不是 gc.START/gc.FINISH——那两个名字
    在 CPython 里不存在）。造一次真实回收，验证 _gc_callback 确实记了一条。"""
    import gc
    ps.GC_EVENTS.clear()
    ps.MEASURE_START[0] = time.monotonic()  # 让"测量期"判定通过
    cb = ps._gc_callback
    gc.callbacks.append(cb)
    try:
        trash = [object() for _ in range(20000)]
        del trash
        gc.collect(2)   # 强制一次完整回收
    finally:
        gc.callbacks.remove(cb)
    assert len(ps.GC_EVENTS) >= 1
    ev = ps.GC_EVENTS[0]
    assert ev["us"] >= 0
    assert ev["gen"] in (0, 1, 2)
    assert "collected" in ev


def _unwrap() -> None:
    """把 install_wrappers 替换过的名字还原（wrapper 上有 __wrapped__）。"""
    import flowlens.api.app as appmod
    import flowlens.ingest as ingestmod
    import flowlens.engine as eng
    import flowlens.ring as ringmod
    import numpy as np
    from flowlens.engine import Engine
    from flowlens.dedup import GenerationalBloom

    for obj in (appmod, ingestmod, eng, ringmod, np):
        for attr in list(vars(obj)):
            v = getattr(obj, attr)
            if hasattr(v, "__wrapped__"):
                setattr(obj, attr, v.__wrapped__)
    # 类方法
    for cls, attr in ((Engine, "ingest"), (Engine, "advance"),
                      (Engine, "_dim_tops"), (Engine, "_snapshot_stats"),
                      (GenerationalBloom, "check_and_set"),
                      (ringmod.DimBucket, "observe")):
        v = getattr(cls, attr)
        if hasattr(v, "__wrapped__"):
            setattr(cls, attr, v.__wrapped__)


# ---------------- 五、_collect_report 字段形状 ----------------

def test_collect_report_shape():
    """用假 engine 验证报告字段齐全、口径自洽。"""
    ps.MEASURE_START[0] = 100.0
    ps.MEASURE_CPU0[0] = 0.5
    # 测量起点时的计数基线为 0 → 窗口事件数 == 容器累计（本用例无预热）
    ps.MEASURE_EVENTS0[:] = [0, 0, 0, 0]
    ps.PHASE["phase"] = "done"
    # 造一点计数
    ps.STAGES.clear()
    ps.STAGES["ingest"] = [2_000_000, 10, 1_800_000]   # wall / count / cpu (ns)
    ps.STAGES["read_body"] = [1_000_000, 10, 400_000]
    ps.LOOP_LAT_US.clear()
    ps.LOOP_LAT_US.extend([1.0, 2.0, 3.0])
    ps.GC_EVENTS.clear()
    ps.GC_EVENTS.append({"gen": 2, "t": 1.0, "us": 500.0, "collected": 10})
    ps.MEM_MB.clear()
    ps.MEM_MB.extend([100.0, 120.0, 110.0])

    class _Eng:
        class counters:
            events_ingested = 1000
            deduped = 5
            late = 2
            invalid = 1

    rep = ps._collect_report(_Eng())
    for key in ("measure_sec", "cpu_time_s", "cpu_pct_of_core", "events",
                "accepted_rate", "cpu_us_per_accepted_event", "stages",
                "top_stages_wall_us", "top_stages_cpu_us",
                "top_stages_frac_of_cpu", "loop_latency_us", "gc", "mem_mib"):
        assert key in rep, key
    # 事件数按测量窗口：本用例无预热基线，窗口 == 累计 1000
    assert rep["events"]["accepted"] == 1000
    # 每条事件 ingest 阶段 CPU 时间 = 1_800_000ns /1000 /1000 事件 = 1.8us
    assert rep["stages"]["ingest"]["us_per_event"] == pytest.approx(1.8)
    # read_body 是 IO 等待阶段：归因 CPU 按 0 计，但保留测得的 cpu_us_measured
    assert rep["stages"]["read_body"]["io_bound"] is True
    assert rep["stages"]["read_body"]["cpu_us"] == 0.0
    assert rep["stages"]["read_body"]["cpu_us_measured"] == pytest.approx(400.0)
    assert rep["stages"]["ingest"]["io_bound"] is False
    # 事件循环延迟 p50 = 2.0
    assert rep["loop_latency_us"]["p50"] == pytest.approx(2.0)
    # 内存峰值
    assert rep["mem_mib"]["peak"] == 120.0
    # 顶层 CPU 之和只算非 IO 阶段 = ingest 的 1.8e6 ns = 1800us（read_body 已排除）
    assert rep["top_stages_cpu_us"] == pytest.approx(1800.0)
