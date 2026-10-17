"""性能改动的等价性测试：新 top_k / HeavyHitters.part 与改动前的参考实现逐项相同。

参考实现是改动前代码的原样复制（只改了对象接收者的写法），作为对照答案。
覆盖三种满载窗口（长尾、接近均匀、全随机，各 61 秒、两维名单写满）与带边缘秒
（frac < 1）的区间；k 取 1、10、100 与大于候选总数。
"""

import numpy as np
import orjson
import pytest

from flowlens.bounds import Answer, Part, Result, top_k
from flowlens.config import Config
from flowlens.dims import ALL_DIMS, Dim
from flowlens.engine import Engine
from flowlens.model import parse_events
from flowlens.timewin import FakeClock, split_range

N_URL = 20_000
N_USER = 200_000
EVENTS_PER_SEC = 1500  # 长尾下足以让每秒两维名单都写满 512
K_VALUES = (1, 10, 100, 5000)  # 5000 > 候选总数


# ---- 改动前实现的原样复制（对照答案）----

def _top_k_reference(parts: list[Part], k: int) -> Answer:
    u_all = float(sum(p.u for p in parts))
    if k <= 0 or not any(len(p.hashes) for p in parts):
        return Answer([], u_all)
    H = np.concatenate([p.hashes for p in parts])
    full = [p.frac >= 1 for p in parts]
    L = np.concatenate([p.lower if f else np.zeros(len(p.hashes)) for p, f in zip(parts, full)])
    Up = np.concatenate([p.upper for p in parts])
    P = np.concatenate([p.upper if f else p.frac * p.upper for p, f in zip(parts, full)])
    Upart = np.concatenate([np.full(len(p.hashes), float(p.u)) for p in parts])
    keys_all = [k_ for p in parts for k_ in p.keys]

    uniq, first, inv = np.unique(H, return_index=True, return_inverse=True)
    lower = np.bincount(inv, weights=L)
    upper = np.bincount(inv, weights=Up) + (u_all - np.bincount(inv, weights=Upart))
    point = np.bincount(inv, weights=P)

    threshold = 0.0
    if len(uniq) >= k:
        threshold = float(np.partition(lower, len(lower) - k)[len(lower) - k])
    alive = upper >= threshold
    for p in parts:
        if p.refine is None or not alive.any():
            continue
        idx = np.flatnonzero(alive)
        absent = idx[~np.isin(uniq[idx], p.hashes)]
        if len(absent):
            r = np.minimum(p.refine(uniq[absent]).astype(np.float64), p.u)
            upper[absent] -= p.u - r
        alive = upper >= threshold
    idx = np.flatnonzero(alive)
    res = [Result(int(uniq[i]), keys_all[first[i]], float(point[i]), float(lower[i]), float(upper[i])) for i in idx]
    res.sort(key=lambda r: (-r.count, r.key))
    return Answer(res[:k], u_all)


def _part_reference(hh, frac: float) -> Part:
    es = hh.top.entries()
    return Part(
        hashes=np.fromiter((e.hash for e in es), dtype=np.uint64, count=len(es)),
        lower=np.fromiter((e.lower for e in es), dtype=np.float64, count=len(es)),
        upper=np.fromiter((e.count for e in es), dtype=np.float64, count=len(es)),
        keys=[e.key for e in es], u=hh.u(), frac=frac, refine=hh.refine,
    )


# ---- 构造满载窗口 ----

def _urls_users(mode: str, seed: int, j0: int, j1: int, rng):
    n = j1 - j0
    if mode == "长尾":
        w = 1.0 / np.arange(1, N_URL + 1) ** 1.1
        idx = rng.choice(N_URL, size=n, p=w / w.sum())
        urls = [f"/api/item/x{u}" for u in idx.tolist()]
        users = [f"u{u}" for u in rng.integers(0, N_USER, n).tolist()]
    elif mode == "接近均匀":
        idx = rng.integers(0, N_URL, n)
        urls = [f"/api/item/x{u}" for u in idx.tolist()]
        users = [f"u{u}" for u in rng.integers(0, N_USER, n).tolist()]
    else:  # 全随机
        urls = [f"/r/x{j}" for j in range(j0, j1)]
        users = [f"u{j}" for j in range(j0, j1)]
    return urls, users


def _fill_window(eng, cur: int, mode: str, seed: int) -> None:
    """第 cur-60..cur 秒各写满 1500 条（cur 为当前秒，环覆盖 [cur-60, cur+6)）。"""
    j = 0
    rng = np.random.default_rng(seed)
    for s in range(cur - 60, cur + 1):
        urls, users = _urls_users(mode, seed, j, j + EVENTS_PER_SEC, rng)
        ts = [s * 1000 + int(x) for x in rng.integers(0, 1000, EVENTS_PER_SEC)]
        evs = [{
            "event_id": f"e{seed}-{j + i}",
            "user_id": users[i],
            "url": urls[i],
            "ts": ts[i],
            "bytes": 1000 + (i % 100),
        } for i in range(EVENTS_PER_SEC)]
        eng.ingest(parse_events(orjson.dumps(evs)))
        j += EVENTS_PER_SEC


def _assert_full(eng, s0: int) -> None:
    for s in (s0, s0 + 30, s0 + 60):
        b = eng.ring.get(s)
        assert b is not None
        assert all(len(bb.hh.top) == 512 for bb in b.dims), f"第 {s} 秒名单未满"


# ---- 满载窗口只在模块级构造一次（61 秒写满是测试里最贵的准备步骤）----

_CUR = 1_735_689_600_000 // 1000
_windows: dict = {}


def _window_part() -> Engine:
    """长尾满载窗口（part 对比用），只构造一次。"""
    if "part" not in _windows:
        eng = Engine(Config(), FakeClock(_CUR * 1000))
        _fill_window(eng, _CUR, "长尾", 42)
        _assert_full(eng, _CUR - 60)
        _windows["part"] = eng
    return _windows["part"]


def _window_mode(mode: str, seed: int) -> Engine:
    """某分布的满载窗口（topk 对比用），每种分布只构造一次。"""
    key = (mode, seed)
    if key not in _windows:
        eng = Engine(Config(), FakeClock(_CUR * 1000))
        _fill_window(eng, _CUR, mode, seed)
        _assert_full(eng, _CUR - 60)
        _windows[key] = eng
    return _windows[key]


def _assert_answers_same(new: Answer, old: Answer, ctx: str) -> None:
    assert new.unseen_upper == old.unseen_upper, ctx
    assert len(new.items) == len(old.items), ctx
    for a, b in zip(new.items, old.items):
        assert a == b, f"{ctx}: {a} != {b}"


def _assert_parts_same(new: Part, old: Part, ctx: str) -> None:
    assert np.array_equal(new.hashes, old.hashes), ctx
    assert np.array_equal(new.lower, old.lower), ctx
    assert np.array_equal(new.upper, old.upper), ctx
    assert new.keys == old.keys, ctx
    assert new.u == old.u and new.frac == old.frac, ctx


@pytest.mark.slow
def test_part_matches_reference():
    # part 新旧实现：每个秒桶、两个维度、整秒与边缘 frac 的数组逐项相同
    eng = _window_part()
    for s in range(_CUR - 60, _CUR + 1):
        b = eng.ring.get(s)
        for d in ALL_DIMS:
            for frac in (1.0, 0.37):
                _assert_parts_same(b.dims[d].part(frac), _part_reference(b.dims[d].hh, frac),
                                   f"sec={s} dim={d} frac={frac}")


@pytest.mark.slow
def test_topk_matches_reference_all_modes():
    for mode, seed in (("长尾", 1), ("接近均匀", 2), ("全随机", 3)):
        eng = _window_mode(mode, seed)
        # 整秒窗口 [cur-60, cur)（全在环内、全有数据）
        r = split_range((_CUR - 60) * 1000, _CUR * 1000)
        for d in ALL_DIMS:
            parts = eng.collect_parts(d, r)
            for k in K_VALUES:
                _assert_answers_same(top_k(parts, k), _top_k_reference(parts, k),
                                     f"{mode} dim={d} k={k}")
        # 带边缘秒（左右 frac 都 < 1，右边缘 < 0.5 走 lower=0 路径）
        r2 = split_range((_CUR - 60) * 1000 + 400, (_CUR - 10) * 1000 + 300)
        assert r2.edges[0].frac == 0.6 and r2.edges[1].frac == 0.3
        for d in ALL_DIMS:
            parts2 = eng.collect_parts(d, r2)
            for k in K_VALUES:
                _assert_answers_same(top_k(parts2, k), _top_k_reference(parts2, k),
                                     f"{mode} 边缘 dim={d} k={k}")


@pytest.mark.slow
def test_topk_matches_reference_small_window():
    # 候选数很少的窗口（k=5000 > 候选总数 走 len(uniq) < k 分支）
    eng = Engine(Config(), FakeClock(1_735_689_600_000))
    s0 = 1_735_689_600_000 // 1000
    rng = np.random.default_rng(9)
    ts = [s0 * 1000 + int(x) for x in rng.integers(0, 900, 50)]
    evs = [{"event_id": f"e{i}", "user_id": f"u{i % 7}", "url": f"/s/x{i % 11}",
            "ts": ts[i], "bytes": 50 + i} for i in range(50)]
    eng.ingest(parse_events(orjson.dumps(evs)))
    r = split_range((s0 - 60) * 1000, s0 * 1000 + 900)
    for d in ALL_DIMS:
        parts = eng.collect_parts(d, r)
        for k in K_VALUES:
            _assert_answers_same(top_k(parts, k), _top_k_reference(parts, k),
                                 f"小窗口 dim={d} k={k}")
