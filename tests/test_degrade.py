"""降级控制器（T8）单元测试：每级进入/退出、滞回、取最高级、sample_weight、内存上限为 None。"""

import logging

from flowlens.degrade import DegradeController, DEFAULT_MEMORY_LIMIT

MB = 1024 * 1024
LIMIT = 256 * MB


def ctrl(**kw) -> DegradeController:
    c = DegradeController()
    for k, v in kw.items():
        setattr(c, k, v)
    return c


def upd(c, occ=0.0, lag=0.0, mem=0, limit=LIMIT):
    c.update(occ, lag, mem, limit)
    return c.level


# ---- 第 1 级：occupancy 或 tick 延迟触发 ----

def test_l1_enter_by_occupancy():
    c = ctrl()
    assert upd(c, occ=0.5) == 0
    assert upd(c, occ=0.61) == 1
    assert c.degraded and c.replay_paused and c.sse_slowdown
    assert not c.tighten_budgets and not c.reject_ingest


def test_l1_enter_by_tick_lag():
    c = ctrl()
    assert upd(c, lag=1500) == 1  # 延迟 > 1 s
    # 进入后退出阈值更低（滞回）：延迟回落到 500 以下才退出
    assert upd(c, lag=999) == 1   # 仍未到退出线（>= 500）
    assert upd(c, lag=499) == 0
    # 从未进入时：延迟在 (500, 1000] 不触发第 1 级
    c2 = ctrl()
    assert upd(c2, lag=999) == 0
    assert upd(c2, occ=0.1, lag=1001) == 1


def test_l1_exit_needs_both_occupancy_and_lag_low():
    c = ctrl()
    assert upd(c, occ=0.7) == 1
    # occupancy 回落到退出线以下、但延迟仍高 → 保持第 1 级（退出要求两者都低）
    assert upd(c, occ=0.3, lag=800) == 1
    assert upd(c, occ=0.3, lag=600) == 1
    assert upd(c, occ=0.3, lag=400) == 0


# ---- 第 2 级：occupancy 触发 ----

def test_l2_enter_exit():
    c = ctrl()
    assert upd(c, occ=0.7) == 1      # 只有第 1 级
    assert upd(c, occ=0.85) == 2     # 进入第 2 级（取最高）
    assert upd(c, occ=0.7) == 2      # 回落：第 2 级未退出（0.7 > 0.5）→ 仍第 2 级
    assert upd(c, occ=0.45) == 1     # 第 2 级退出（0.45 < 0.5）、第 1 级仍活跃（0.45 > 0.4）
    assert upd(c, occ=0.3) == 0      # 第 1 级退出（0.3 < 0.4）


def test_l2_hysteresis_band():
    c = ctrl()
    assert upd(c, occ=0.9) == 2
    # 滞回区间 [0.5, 0.8]：保持不变（既不进入也不退出第 2 级）
    assert upd(c, occ=0.75) == 2     # 第 1 级活跃（> 0.4），第 2 级未退出 → 最高 2
    assert upd(c, occ=0.5) == 2      # 恰在退出线：occupancy < 0.5 才退出
    assert upd(c, occ=0.49) == 1     # 第 2 级退出，第 1 级仍活跃
    assert upd(c, occ=0.3) == 0


# ---- 第 3 / 4 级：内存阈值（按上限比例）----

def test_l3_l4_enter_exit_by_memory():
    c = ctrl()
    assert upd(c, mem=int(0.77 * LIMIT)) == 0
    assert upd(c, mem=int(0.79 * LIMIT)) == 3          # 进入第 3 级
    assert c.tighten_budgets
    assert upd(c, mem=int(0.71 * LIMIT)) == 3          # 第 3 级未退出（>= 0.70）
    assert upd(c, mem=int(0.85 * LIMIT)) == 3          # 未到第 4 级进入线
    assert upd(c, mem=int(0.91 * LIMIT)) == 4          # 进入第 4 级
    assert c.reject_ingest
    assert upd(c, mem=int(0.85 * LIMIT)) == 4          # 第 4 级未退出（>= 0.82）
    assert upd(c, mem=int(0.75 * LIMIT)) == 3          # 第 4 级退出（< 0.82），第 3 级保持
    assert upd(c, mem=int(0.69 * LIMIT)) == 0          # 第 3 级退出


def test_memory_threshold_scales_with_limit():
    """阈值按上限比例，不写死 200/230 MB：换 512 MB 上限时进入线也按比例翻倍。"""
    c = ctrl()
    lim = 512 * MB
    assert upd(c, occ=0, mem=int(0.79 * lim), limit=lim) == 3
    assert upd(c, occ=0, mem=int(0.91 * lim), limit=lim) == 4


def test_memory_limit_none_defaults_to_256mb():
    c = ctrl()
    # 上限为 None 时按 256 MB 计算
    assert DEFAULT_MEMORY_LIMIT == 256 * MB
    assert upd(c, occ=0, mem=int(0.79 * DEFAULT_MEMORY_LIMIT), limit=None) == 3
    assert upd(c, occ=0, mem=int(0.91 * DEFAULT_MEMORY_LIMIT), limit=None) == 4
    # 输入值原样保存，供 /healthz 展示
    assert c.memory_limit_bytes is None
    assert c.memory_bytes == int(0.91 * DEFAULT_MEMORY_LIMIT)


# ---- 多个条件同时满足时取最高级 ----

def test_highest_level_wins_when_multiple_conditions():
    c = ctrl()
    # 同时满足第 1 级（占用 0.9）、第 2 级（占用 0.9）、第 3 级（内存 0.8）、第 4 级（内存 0.95）
    assert upd(c, occ=0.9, lag=2000, mem=int(0.95 * LIMIT)) == 4
    # 占用拉高但内存低：只触发 1、2 级
    assert upd(c, occ=0.95, lag=0, mem=0) == 2
    # 占用中等但内存高：只触发 3、4 级
    assert upd(c, occ=0.5, lag=0, mem=int(0.92 * LIMIT)) == 4


# ---- sample_weight 随 occupancy / 级别变化 ----

def test_sample_weight_by_level_and_occupancy():
    c = ctrl()
    assert c.sample_weight == 1                    # level 0
    upd(c, occ=0.7)
    assert c.level == 1 and c.sample_weight == 1   # 第 1 级不采样
    upd(c, occ=0.85)
    assert c.level == 2 and c.sample_weight == 2   # 第 2 级采样 ×2
    upd(c, occ=0.96)
    assert c.sample_weight == 4                    # 占用 > 0.95 → ×4
    assert isinstance(c.sample_weight, int)
    upd(c, occ=0.9)
    assert c.level == 2 and c.sample_weight == 2   # 占用回落到 0.95 以下 → 回到 ×2
    upd(c, occ=0.3)
    assert c.level == 0 and c.sample_weight == 1


def test_sample_weight_4_requires_high_occupancy_even_at_l2():
    """占用 0.95 以上才 ×4，即便已在第 2 级（0.8~0.95 之间只 ×2）。"""
    c = ctrl()
    upd(c, occ=0.9)
    assert c.level == 2 and c.sample_weight == 2
    upd(c, occ=0.951)
    assert c.sample_weight == 4


# ---- 滞回区间内保持不变 ----

def test_level_stable_within_hysteresis_band():
    c = ctrl()
    assert upd(c, occ=0.9) == 2
    prev = c.level
    # 在 [0.5, 0.8] 内小幅抖动，级别不应来回跳
    for occ in (0.7, 0.75, 0.66, 0.72, 0.55, 0.79):
        assert upd(c, occ=occ) == 2 == prev, f"occ={occ} 时级别不应变"


# ---- 级别变化时记录日志 ----

def test_logs_on_level_change(capsys):
    c = ctrl()
    records = []

    class _H(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger("flowlens.degrade")
    old = logger.handlers
    logger.handlers = [_H()]
    logger.setLevel(logging.INFO)
    try:
        assert upd(c, occ=0.9) == 2
        assert upd(c, occ=0.3) == 0
    finally:
        logger.handlers = old
    # 两次级别变化（0→2、2→0）各记一行，含新旧级别
    assert any(r.levelno == logging.INFO and "降级级别" in r.getMessage() for r in records)


# ---- pin：测试固定级别；None 恢复按阈值判定 ----

def test_pin_holds_level_and_none_restores():
    c = ctrl()
    c.pin(3)
    assert c.level == 3
    upd(c, occ=0.0, lag=0, mem=0)  # 低负载也不变
    assert c.level == 3
    c.pin(None)
    assert upd(c, occ=0.0) == 0
    assert upd(c, occ=0.9) == 2


# ---- on_change 回调：只在级别实际变化时触发 ----

def test_on_change_called_once_per_change():
    calls = []
    c = DegradeController()
    c.on_change = lambda old, new: calls.append((old, new))
    assert upd(c, occ=0.7) == 1
    assert upd(c, occ=0.7) == 1  # 未变化，不回调
    assert upd(c, occ=0.9) == 2
    assert calls == [(0, 1), (1, 2)]
    assert upd(c, occ=0.3) == 0
    assert calls == [(0, 1), (1, 2), (2, 0)]


# ---- 只读属性与 status ----

def test_status_fields_and_readonly_inputs():
    c = ctrl()
    upd(c, occ=0.7, lag=120, mem=12345, limit=LIMIT)
    st = c.status()
    assert st == {
        "level": c.level, "sample_weight": c.sample_weight,
        "occupancy": 0.7, "tick_lag_ms": 120.0,
        "memory_bytes": 12345, "memory_limit_bytes": LIMIT,
    }
    assert c.occupancy == 0.7 and c.tick_lag_ms == 120.0
    assert c.memory_bytes == 12345 and c.memory_limit_bytes == LIMIT
