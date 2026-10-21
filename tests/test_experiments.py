"""experiments 的纯函数单测（分位数、最大可持续吞吐判定、工作集时间线、
loadgen 行解析、timeline 合并、已定稿区间抽样）。不启动服务，秒级跑完。

慢的冒烟测试（进程内起服务、验证结果目录与文件格式）在 tests/e2e/ 下、标 slow。
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

import numpy as np
import pytest

import experiments as ex
import summarize as sm


# ---- pctl ----

def test_pctl_percentile():
    assert ex.pctl([], 95) is None
    assert ex.pctl([1, 2, 3, 4, 5], 50) == 3.0
    assert abs(ex.pctl(list(range(1, 101)), 95) - 95.05) < 1e-6


# ---- level_ok / max_sustainable ----

def _lv(rate, acc=1.0, r503=0.0, p99=100.0):
    return {"rate": rate, "accept_ratio": acc, "ratio_503": r503, "lat_p99_ms": p99}


def test_level_ok_criteria():
    assert ex.level_ok(_lv(100000, acc=0.995, p99=300.0)) is True
    # 接受率 < 99%
    assert ex.level_ok(_lv(100000, acc=0.98)) is False
    # 有 503
    assert ex.level_ok(_lv(100000, r503=0.01)) is False
    # p99 >= 500
    assert ex.level_ok(_lv(100000, p99=500.0)) is False
    # p99 缺失
    assert ex.level_ok(_lv(100000, p99=None)) is False


def test_max_sustainable_highest_qualifying():
    levels = [_lv(50000, acc=1.0, p99=80.0),
              _lv(100000, acc=0.995, p99=300.0),
              _lv(150000, acc=0.98, p99=800.0),   # 不接受
              _lv(200000, acc=0.5, p99=9000.0)]
    assert ex.max_sustainable(levels) == 100000


def test_max_sustainable_none_when_none_qualify():
    assert ex.max_sustainable([_lv(50000, acc=0.9), _lv(100000, r503=0.5)]) is None


# ---- mem_timeline（工作集 + 降级级别）----

def test_mem_timeline_trip_and_degrade():
    samples = [
        {"t": 1000, "healthz": {"memory_bytes": 100, "memory_current_bytes": 200,
                                "degrade": {"level": 0}}},
        {"t": 2000, "healthz": {"memory_bytes": 150, "memory_current_bytes": 210,
                                "degrade": {"level": 0}}},
        {"t": 3000, "healthz": {"memory_bytes": 120, "memory_current_bytes": 205,
                                "degrade": {"level": 3}}},
    ]
    m = ex.mem_timeline(samples)
    assert m["working_set"]["start_mb"] == ex.mem_mb(100)
    assert m["working_set"]["peak_mb"] == ex.mem_mb(150)
    assert m["working_set"]["end_mb"] == ex.mem_mb(120)
    assert m["raw_current"]["peak_mb"] == ex.mem_mb(210)
    assert m["degrade_changed"] is True
    assert m["degrade_levels_seen"] == [0, 3]
    assert m["peak_degrade_level"] == 3


def test_mem_timeline_empty_and_constant():
    m = ex.mem_timeline([])
    assert m["working_set"]["peak_mb"] is None
    assert m["degrade_changed"] is False
    assert m["peak_degrade_level"] == 0
    # 全程 0 级：无变化
    m2 = ex.mem_timeline([{"t": i, "healthz": {"memory_bytes": 10,
                                               "degrade": {"level": 0}}}
                          for i in (1, 2, 3)])
    assert m2["degrade_changed"] is False


# ---- parse_loadgen_lines ----

def test_parse_loadgen_lines():
    import orjson
    lines = [
        orjson.dumps({"type": "second", "t": 100, "sent": 5000, "accepted": 5000,
                      "rejected": 0, "errors": 0, "lat_p50_ms": 90.0,
                      "lat_p95_ms": 120.0, "lat_p99_ms": 130.0}).decode(),
        orjson.dumps({"type": "second", "t": 101, "sent": 5000, "accepted": 4900,
                      "rejected": 100, "errors": 0, "lat_p50_ms": 95.0,
                      "lat_p95_ms": 125.0, "lat_p99_ms": 135.0}).decode(),
        orjson.dumps({"type": "summary", "sent": 10000, "accepted": 9900,
                      "rejected": 100, "errors": 0, "accept_rate": 0.99,
                      "accepted_events_rate": 4950.0, "lat_p99_ms": 135.0,
                      "client_cpu_pct": 40.0, "wall_s": 2.0}).decode(),
        "not json, skip",
    ]
    summary, seconds = ex.parse_loadgen_lines(lines)
    assert summary is not None and summary["accepted"] == 9900
    assert len(seconds) == 2 and seconds[0]["t"] == 100
    assert seconds[1]["rejected"] == 100


# ---- merge_timeline ----

def test_merge_timeline_joins_by_second():
    samples = [{"t": 1000, "healthz": {"memory_bytes": 100,
                                       "degrade": {"level": 0}}},
               {"t": 2000, "healthz": {"memory_bytes": 110,
                                       "degrade": {"level": 0}}}]
    seconds = [{"t": 1, "sent": 5000, "accepted": 5000, "rejected": 0, "errors": 0,
                "lat_p50_ms": 90.0, "lat_p95_ms": 120.0, "lat_p99_ms": 130.0},
               {"t": 2, "sent": 5000, "accepted": 4900, "rejected": 100, "errors": 0,
                "lat_p50_ms": 95.0, "lat_p95_ms": 125.0, "lat_p99_ms": 135.0}]
    merged = ex.merge_timeline(samples, seconds)
    assert len(merged) == 2
    assert merged[0]["t"] == 1 and merged[0]["sent"] == 5000
    assert merged[0]["healthz"]["memory_bytes"] == 100
    assert merged[1]["rejected"] == 100
    # 某秒只有 healthz、没有 loadgen：sent 补 0
    merged2 = ex.merge_timeline([{"t": 5, "healthz": {"memory_bytes": 5}}], [])
    assert merged2[0]["sent"] == 0 and merged2[0]["accepted"] == 0


# ---- cpu_window_pct（P2d-3 二：整窗口引擎/前端合计 CPU%）----

def _sample(t, main_cpu, *fronts):
    procs = []
    if main_cpu is not None:
        procs.append({"pid": 1, "name": "main", "cpu_pct": main_cpu})
    for i, f in enumerate(fronts):
        procs.append({"pid": i + 2, "name": "parse", "cpu_pct": f})
    return {"t": t, "procs": procs}


def test_cpu_window_pct_sums_main_and_front():
    # 窗口 10s：引擎每采样 100%（=1 核满）→ 10 CPU 秒 → 100%；
    # 3 前端各 50%（合计 150%）→ 15 CPU 秒 → 150%
    samples = [_sample(i * 1000, 100.0, 50.0, 50.0, 50.0) for i in range(11)]
    c = ex.cpu_window_pct(samples)
    assert c["engine_cpu_pct"] == 100.0
    assert c["front_cpu_pct"] == 150.0


def test_cpu_window_pct_single_process_front_zero():
    # 单进程（前端数 0）：前端合计 0.0
    samples = [_sample(i * 1000, 40.0) for i in range(5)]
    c = ex.cpu_window_pct(samples)
    assert c["engine_cpu_pct"] == 40.0
    assert c["front_cpu_pct"] == 0.0


def test_cpu_window_pct_empty():
    assert ex.cpu_window_pct([]) == {"engine_cpu_pct": None, "front_cpu_pct": None}


# ---- parse_front_count / _svc_fronts（P2d-3 第零节第 2 条）----

def test_parse_front_count():
    assert ex.parse_front_count("... 前端数 3（FLOWLENS_FRONTENDS=3） ...") == \
        (3, "FLOWLENS_FRONTENDS=3")
    assert ex.parse_front_count("前端数 0（内存上限 256 MB < 384 MB）") == \
        (0, "内存上限 256 MB < 384 MB")
    # 读不到该行：None + 原文末行
    n, last = ex.parse_front_count("no front line here\nlast line")
    assert n is None and last == "last line"


def test_svc_fronts_stops_on_mismatch(monkeypatch):
    class _S:
        front_count = 2
        front_reason = "FLOWLENS_FRONTENDS=2"
    svc = _S()
    # 无预期（未注入 env）：只记录、不核对
    assert ex._svc_fronts(svc, {}) == (2, "FLOWLENS_FRONTENDS=2")
    # 注入 FLOWLENS_FRONTENDS 相符：核对通过
    assert ex._svc_fronts(svc, {"FLOWLENS_FRONTENDS": "2"}) == (2, "FLOWLENS_FRONTENDS=2")
    # 注入 FLOWLENS_FRONTENDS 不符：抛错停止
    with pytest.raises(RuntimeError):
        ex._svc_fronts(svc, {"FLOWLENS_FRONTENDS": "3"})
    # 显式 --expect-fronts 优先且不符：抛错
    monkeypatch.setattr(ex, "EXPECT_FRONT", 0)
    try:
        with pytest.raises(RuntimeError):
            ex._svc_fronts(svc, {})
    finally:
        monkeypatch.setattr(ex, "EXPECT_FRONT", None)


def test_svc_fronts_stop_when_unreadable():
    class _S:
        front_count = None
        front_reason = "docker 还没起来"
    with pytest.raises(RuntimeError):
        ex._svc_fronts(_S(), {})


# ---- pick_finalized_intervals ----

def test_pick_finalized_intervals_in_retention():
    rng = np.random.default_rng(0)
    start, end, now = 1_000_000_000_000, 1_000_000_300_000, 1_000_000_320_000
    window_ms = 60_000
    ivs = ex.pick_finalized_intervals(50, start, end, now, window_ms, rng)
    assert len(ivs) == 50
    for frm, to in ivs:
        # 区间在存档范围内、to 已定稿（不晚于 now − 迟到上限）、长度为 window
        assert frm >= start
        assert to <= end
        assert to <= now - ex.LATENESS_MS
        assert to - frm == window_ms


# ---- summarize._read_accuracy（S3 抽样区间分桶：valid / degraded / rejected）----

def _cmp(bucket, **extra):
    r = {"type": "sample_comparison", "bucket": bucket, "from_ms": 0, "to_ms": 60000}
    r.update(extra)
    return r


def _write_acc(tmp_path, rows):
    import orjson
    p = tmp_path / "accuracy.jsonl"
    p.write_text("".join(orjson.dumps(r).decode() + "\n" for r in rows),
                 encoding="utf-8")
    return str(tmp_path)


def test_read_accuracy_buckets_new_format(tmp_path):
    rows = [
        _cmp("valid", topk_url={"recall": 1.0, "mean_rel_err": 0.02,
                                "in_bounds": True},
             topk_user={"recall": 0.9, "mean_rel_err": 0.03, "in_bounds": True},
             stats={"distinct_users": {"service": 100, "exact": 110},
                    "p95": {"service": 1000.0, "exact": 1050.0}}),
        _cmp("valid", topk_url={"recall": 0.8, "mean_rel_err": 0.04,
                                "in_bounds": False},
             topk_user={"recall": 1.0, "mean_rel_err": 0.01, "in_bounds": True}),
        _cmp("degraded", status=200, degraded=True),   # 200 但降级：不计精度
        _cmp("rejected", status=503, degraded=False),  # 回放被拒
    ]
    out = sm._read_accuracy(_write_acc(tmp_path, rows))
    text = "\n".join(out)
    # 分桶计数：有效 2 / 降级 1 / 拒绝 1
    assert "有效 2 / 降级 1 / 拒绝 1" in text
    # topk_url 平均召回只由 valid 计算：(1.0 + 0.8)/2 = 0.9
    assert "90.0%" in text
    # 降级 / 拒绝 的告警行
    assert "降级态" in text and "503/422" in text
    # stats 只由 valid 计算：去重 |100−110|/110、p95 |1000−1050|/1050
    assert "stats 去重用户数" in text and "stats p95 字节" in text


def test_read_accuracy_old_format_flagged_unreliable(tmp_path):
    # 旧格式：无 bucket 字段，但有 topk_url 结果 → 归"有效"，却标注不可信
    rows = [
        {"type": "sample_comparison", "from_ms": 0, "to_ms": 60000,
         "topk_url": {"recall": 1.0, "mean_rel_err": 0.0, "in_bounds": True},
         "topk_user": {"recall": 1.0, "mean_rel_err": 0.0, "in_bounds": True},
         "stats": {"distinct_users": {"service": None, "exact": 100},
                   "p95": {"service": None, "exact": 1000.0}}},
    ]
    out = sm._read_accuracy(_write_acc(tmp_path, rows))
    text = "\n".join(out)
    assert "旧格式未记录降级标记" in text
    assert "不可信" in text
