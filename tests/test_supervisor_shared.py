"""P2d-1 二：共享状态（SharedState）、前端侧 SlotAdmission、引擎侧 FrontsBudgetView、
前端侧 DegradeView。

sharedctypes 的 Value/Array 在单进程里可直接读写，这里不用真的跨进程——验证的是
"镜像/汇总/标志翻转"这套接口逻辑，跨进程正确性由端到端用例覆盖（第六节）。
"""

import multiprocessing as mp

from flowlens.supervisor import (DegradeView, FrontsBudgetView, SharedState,
                                 SlotAdmission)


def _shared(n: int = 2) -> SharedState:
    return SharedState(mp.get_context("spawn"), n)


# ---- SharedState ----

def test_reset_slot_zeroes_pending_keeps_rejected():
    s = _shared(3)
    s.pending[1] = 100
    s.raw_pending[1] = 50
    s.ingest_rejected[1] = 7
    s.raw_rejected[1] = 9
    s.reset_slot(1)
    assert s.pending[1] == 0
    assert s.raw_pending[1] == 0
    assert s.ingest_rejected[1] == 7   # 拒绝计数保留
    assert s.raw_rejected[1] == 9


def test_other_slots_untouched_on_reset():
    s = _shared(3)
    s.pending[0] = 11
    s.pending[2] = 22
    s.reset_slot(1)
    assert s.pending[0] == 11 and s.pending[2] == 22


# ---- SlotAdmission：镜像占用与拒绝到共享数组 ----

def test_slot_mirrors_pending_and_rejected():
    s = _shared(2)
    adm = SlotAdmission(limit=100, pending_arr=s.pending,
                        rejected_arr=s.ingest_rejected, slot=1)
    assert adm.pending == 0
    assert adm.try_reserve(40)
    assert s.pending[1] == 40
    assert adm.try_reserve(40)
    assert s.pending[1] == 80
    # 放不下 → 拒绝，镜像到共享数组
    assert not adm.try_reserve(40)
    assert s.ingest_rejected[1] == 1
    adm.release(80)
    assert s.pending[1] == 0


def test_slot_rejected_setter_mirrors():
    """_ingest_expect 走 `budget.rejected += 1`：读属性→写属性，落到共享槽位。"""
    s = _shared(2)
    adm = SlotAdmission(limit=100, pending_arr=s.pending,
                        rejected_arr=s.raw_rejected, slot=0)
    assert adm.rejected == 0
    adm.rejected += 1          # expect 路径的拒绝（发请求体之前）
    assert s.raw_rejected[0] == 1
    assert adm.rejected == 1


def test_slot_limit_get_set():
    s = _shared(2)
    adm = SlotAdmission(limit=100, pending_arr=s.pending,
                        rejected_arr=s.ingest_rejected, slot=0)
    assert adm.limit == 100
    adm.limit = 50
    assert adm.limit == 50
    # 收紧后超额的预留失败
    adm.try_reserve(60)
    assert not adm.try_reserve(60)


def test_slot_occupancy():
    s = _shared(2)
    adm = SlotAdmission(limit=100, pending_arr=s.pending,
                        rejected_arr=s.ingest_rejected, slot=0)
    adm.try_reserve(50)
    assert abs(adm.occupancy - 0.5) < 1e-9


# ---- FrontsBudgetView：引擎侧汇总 ----

def test_view_sums_slots():
    s = _shared(3)
    s.pending[0], s.pending[1], s.pending[2] = 10, 20, 30
    s.ingest_rejected[0], s.ingest_rejected[2] = 1, 2
    view = FrontsBudgetView(s, s.pending, s.ingest_rejected,
                            normal_total=300, tight_total=150)
    assert view.pending == 60
    assert view.rejected == 3
    assert view.limit == 300          # tight 未置位
    assert abs(view.occupancy - 0.2) < 1e-9


def test_view_limit_setter_toggles_tight():
    s = _shared(2)
    view = FrontsBudgetView(s, s.pending, s.ingest_rejected,
                            normal_total=300, tight_total=150)
    assert s.tight.value == 0
    assert view.limit == 300
    # _make_degrade_change_hook 设 limit = tight_adm（< normal）→ tight 置 1
    view.limit = 150
    assert s.tight.value == 1
    assert view.limit == 150
    # 恢复 normal → tight 清零
    view.limit = 300
    assert s.tight.value == 0
    assert view.limit == 300


def test_view_limit_setter_boundary():
    """value < normal 才置 tight；等于 normal 视为正常（不收紧）。"""
    s = _shared(1)
    view = FrontsBudgetView(s, s.pending, s.ingest_rejected,
                            normal_total=300, tight_total=150)
    view.limit = 300
    assert s.tight.value == 0
    view.limit = 299
    assert s.tight.value == 1


def test_view_zero_limit_occupancy():
    s = _shared(1)
    view = FrontsBudgetView(s, s.pending, s.ingest_rejected,
                            normal_total=0, tight_total=0)
    assert view.occupancy == 0.0


# ---- DegradeView：前端侧 ----

def test_degrade_view_reject_ingest():
    s = _shared(2)
    v = DegradeView(s)
    assert v.level == 0
    assert v.reject_ingest is False
    s.level.value = 3
    assert v.level == 3
    assert v.reject_ingest is False   # 第 3 级不收拒写入
    s.level.value = 4
    assert v.reject_ingest is True    # 第 4 级才拒
