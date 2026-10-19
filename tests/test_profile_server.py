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
    ps.GC_SPANS.clear()
    ps.GC_ENDS.clear()
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
    # T11a-2：墙钟区间也记了，供各阶段"扣除 GC"算交叠
    assert ev["s"] <= ev["e"]
    assert len(ps.GC_SPANS) >= 1
    s, e = ps.GC_SPANS[-1]
    assert (s, e) == (ev["s"], ev["e"])
    assert ps.GC_ENDS[-1] == ev["e"]


def test_gc_overlap_partial_and_full():
    """_gc_overlap_ns：调用时段与回收区间的交叠（按 ns）。

    造三段互不重叠的回收区间（[1,2]、[3,4]、[6,7]，monotonic 秒），验证：
    不相交 → 0；部分交叠 → 交叠部分；完全覆盖 → 区间总长；bisect 定位不靠全扫。
    """
    ps.GC_SPANS.clear()
    ps.GC_ENDS.clear()
    for s, e in ((1.0, 2.0), (3.0, 4.0), (6.0, 7.0)):
        ps.GC_SPANS.append((s, e))
        ps.GC_ENDS.append(e)
    try:
        assert ps._gc_overlap_ns(0.0, 0.5) == 0               # 早于所有区间
        assert ps._gc_overlap_ns(2.5, 3.0) == 0               # 区间之间的空隙
        assert ps._gc_overlap_ns(1.5, 2.5) == 500_000_000     # 与 [1,2] 交 [1.5,2] = 0.5s
        assert ps._gc_overlap_ns(1.5, 3.5) == 1_000_000_000   # 交 [1,2] 0.5s + [3,4] 0.5s
        assert ps._gc_overlap_ns(0.0, 10.0) == 3_000_000_000  # 覆盖全部三个区间（各 1s）
        assert ps._gc_overlap_ns(6.2, 6.4) == 200_000_000     # 与 [6,7] 交 0.2s
        assert ps._gc_overlap_ns(3.0, 4.0) == 1_000_000_000   # 恰好盖满一个区间
    finally:
        ps.GC_SPANS.clear()
        ps.GC_ENDS.clear()


# ---------------- 六b、URL 归一化缓存统计 ----------------

def test_id_seg_like_matches_model_rules():
    """_id_seg_like 的口径与 flowlens.model._is_id_segment 一致（纯数字 / UUID /
    ≥16 位十六进制），但不做替换。"""
    from flowlens.model import _is_id_segment
    segs = ["12345", "abc", "x0", "X1", "hello",
            "0123456789abcdef",            # 16 位十六进制
            "0123456789abcdeg",            # 16 位但含 g → 否
            "0123456789abc",               # 15 位十六进制 → 否
            "550e8400-e29b-41d4-a716-446655440000",   # UUID
            "550E8400-E29B-41D4-A716-446655440000",   # UUID 大写
            "550e8400e29b41d4a716446655440000",       # 32 位十六进制
            "550e8400-e29b-41d4-a716-4466554400001",  # 37 位：不是 UUID 也不是纯数字
            "中文段",                      # 非 ASCII
            ""]                            # 空段（根路径的开头段）
    for s in segs:
        assert ps._id_seg_like(s) == _is_id_segment(s), s


def test_norm_wrapper_counts_and_preserves():
    """包装 normalize_url / _normalize_url 后：结果与不包装完全一致，命中/未命中
    计数正确，未命中里"带 ID 段"的判定正确。用完还原（_unwrap）。"""
    import flowlens.model as model
    ps.NORM_STATS[:] = [0, 0, 0, 0]
    urls = [
        "/api/item/x0?ref=1",        # 稳定路径：首次未命中、之后命中
        "/api/item/x0?ref=2",        # 同路径不同 query → 命中
        f"/api/order/{'a' * 32}/x1",           # 路径带 32 位十六进制 → 未命中 + 带 ID
        "/api/order/550e8400-e29b-41d4-a716-446655440000",  # 带 UUID → 未命中 + 带 ID
        "/api/item/x0",              # 命中
    ]
    # 归一化结果与缓存命中与否无关（命中/未命中都返回同一个归一化串），所以
    # base 用任意缓存状态算都行；但命中/未命中"计数"依赖缓存状态，故跑包装前
    # 清空缓存，让计数确定。
    model.clear_norm_cache()
    base = [model.normalize_url(u) for u in urls]
    model.clear_norm_cache()

    ps._install_norm_stats()
    try:
        got = [model.normalize_url(u) for u in urls]
    finally:
        _unwrap()

    assert got == base, "包装改变了归一化结果"
    total, miss, miss_ns, idlike = ps.NORM_STATS
    assert total == 5
    # 清空后：/api/item/x0 首次未命中、两条 ID 路径各自未命中、其余命中
    assert miss == 3
    assert idlike == 2
    assert miss_ns > 0
    model.clear_norm_cache()  # 收尾，别把本用例的条目留给别的测试


def _unwrap() -> None:
    """把 install_wrappers 替换过的名字还原（wrapper 上有 __wrapped__）。"""
    import flowlens.api.app as appmod
    import flowlens.ingest as ingestmod
    import flowlens.engine as eng
    import flowlens.ring as ringmod
    import flowlens.model as modelmod
    import numpy as np
    from flowlens.engine import Engine
    from flowlens.dedup import GenerationalBloom

    for obj in (appmod, ingestmod, eng, ringmod, modelmod, np):
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
    """用假 engine 验证报告字段齐全、口径自洽（含 T11a-2 的 GC 扣除与 norm_cache）。"""
    ps.MEASURE_START[0] = 100.0
    ps.MEASURE_CPU0[0] = 0.5
    # 测量起点时的计数基线为 0 → 窗口事件数 == 容器累计（本用例无预热）
    ps.MEASURE_EVENTS0[:] = [0, 0, 0, 0]
    ps.PHASE["phase"] = "done"
    # 造一点计数（STAGES 第 4 位是"调用时段内发生的 GC 耗时"，ns）
    ps.STAGES.clear()
    ps.STAGES["ingest"] = [2_000_000, 10, 1_800_000, 200_000]   # wall / count / cpu / gc
    ps.STAGES["read_body"] = [1_000_000, 10, 400_000, 0]
    ps.LOOP_LAT_US.clear()
    ps.LOOP_LAT_US.extend([1.0, 2.0, 3.0])
    ps.GC_EVENTS.clear()
    ps.GC_EVENTS.append({"gen": 2, "t": 1.0, "us": 500.0, "collected": 10,
                         "s": 1.0, "e": 1.0005})
    ps.NORM_STATS[:] = [100, 40, 40_000_000, 39]
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
                "top_stages_frac_of_cpu", "loop_latency_us", "gc", "mem_mib",
                "norm_cache"):
        assert key in rep, key
    # 事件数按测量窗口：本用例无预热基线，窗口 == 累计 1000
    assert rep["events"]["accepted"] == 1000
    # ingest 扣除 GC 后：cpu 1_800_000 − 200_000 = 1_600_000 ns
    # 每条事件 CPU = 1_600_000 /1000 = 1.6us
    assert rep["stages"]["ingest"]["cpu_us"] == pytest.approx(1600.0)
    assert rep["stages"]["ingest"]["gc_us"] == pytest.approx(200.0)
    assert rep["stages"]["ingest"]["us_per_event"] == pytest.approx(1.6)
    # read_body 是 IO 等待阶段：归因 CPU 按 0 计，但保留测得的 cpu_us_measured
    assert rep["stages"]["read_body"]["io_bound"] is True
    assert rep["stages"]["read_body"]["cpu_us"] == 0.0
    assert rep["stages"]["read_body"]["cpu_us_measured"] == pytest.approx(400.0)
    assert rep["stages"]["ingest"]["io_bound"] is False
    # 事件循环延迟 p50 = 2.0
    assert rep["loop_latency_us"]["p50"] == pytest.approx(2.0)
    # 内存峰值
    assert rep["mem_mib"]["peak"] == 120.0
    # 顶层 CPU 之和只算非 IO 阶段 = ingest 扣 GC 后 1_600_000 ns = 1600us
    assert rep["top_stages_cpu_us"] == pytest.approx(1600.0)
    # 校验口径的分子 =（扣 GC 后阶段之和 + GC 总耗时）= 1600 + 500 = 2100us；
    # 比值再除以进程 CPU。cpu_s 在测试里是真实 process_time 差值且两处都四舍五入到
    # 3 位小数，反解误差大，故只验证 frac 非负（GC 扣除的数值正确性已由上面的
    # top_stages_cpu_us==1600 与 stages.ingest.gc_us 直接锁住）。
    assert rep["top_stages_frac_of_cpu"] >= 0.0
    # 归一化缓存：命中率 = 1 − 40/100 = 0.6；未命中平均耗时 = 40_000_000ns/40 = 1000us
    assert rep["norm_cache"]["calls"] == 100
    assert rep["norm_cache"]["misses"] == 40
    assert rep["norm_cache"]["hit_rate"] == pytest.approx(0.6)
    assert rep["norm_cache"]["miss_us_avg"] == pytest.approx(1000.0)
    assert rep["norm_cache"]["miss_with_id_rate"] == pytest.approx(39 / 40)
    # GC 汇总：总耗时 500us、第 2 代 1 次（占核比例依赖真实 cpu_s，只验证总耗时与
    # 代计数，不断言比值）
    assert rep["gc"]["total"] == 1
    assert rep["gc"]["total_us"] == pytest.approx(500.0)
    assert rep["gc"]["per_gen"]["2"]["count"] == 1
