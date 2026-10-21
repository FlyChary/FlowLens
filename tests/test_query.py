"""T5 查询：默认窗口读快照、回放上下界、最终态 LRU 缓存、统计。

数据只经 engine.collect_parts / collect_stats / snapshot 获取。
200 个随机区间的上下界与召回对照用 10 分钟满载数据，较慢，标记为 slow（默认 pytest -q 跳过）。
"""

import asyncio
import collections

import numpy as np
import orjson
import pytest

from flowlens.config import Config
from flowlens.dims import Dim
from flowlens.engine import Engine
from flowlens.errors import BadRequest, TooOld
from flowlens.model import EventBatch
from flowlens.query import (CACHE_CAP, QueryService, is_snapshot_query,
                            stats_from_snapshot, topk_from_snapshot)
from flowlens.timewin import FakeClock

NOW = 1_735_689_600_000
N_URL = 20_000
N_USER = 200_000


def _batch_for(events, nbytes=100):
    """events: (urls, users, ts) 三个 list。返回可直接 ingest 的 EventBatch。"""
    urls, users, ts = events
    n = len(urls)
    return EventBatch(
        [f"e{i}" for i in range(n)], list(users), list(urls),
        np.asarray(ts, dtype=np.int64), np.full(n, nbytes, dtype=np.int64), 0,
    )


def _engine_with(events, now=NOW):
    eng = Engine(Config(), FakeClock(now))
    if events is not None:
        eng.ingest(_batch_for(events))
    return eng


def _run(coro):
    return asyncio.run(coro)


# ---- 参数校验 / 快照路径 / 缓存 / TooOld（小引擎，快）----

def test_topk_param_validation():
    async def main():
        qs = QueryService(_engine_with(None))
        for kw in (dict(window_ms=500), dict(window_ms=700_000),
                   dict(k=0), dict(k=101)):
            with pytest.raises(BadRequest):
                await qs.topk(Dim.URL, kw.get("window_ms", 60_000), kw.get("k", 10), NOW - 1000)
        # at 在未来
        with pytest.raises(BadRequest):
            await qs.topk(Dim.URL, 60_000, 10, NOW + 10_000)
        # stats 同样校验 window
        with pytest.raises(BadRequest):
            await qs.stats(900)
        with pytest.raises(BadRequest):
            await qs.stats(600_001)
    _run(main())


def test_too_old_raises():
    async def main():
        qs = QueryService(_engine_with(None))
        # from 早于保留期 600 秒
        with pytest.raises(TooOld):
            await qs.topk(Dim.URL, 60_000, 10, NOW - 700_000)
    _run(main())


def test_long_window_frames_never_too_old():
    """window=600s 的帧（interval 为 1s/1.5s/5s/60s）在一分钟内各时刻都不抛 TooOld。

    帧时刻 T 对齐到 interval 网格，往往比 now 早；区间起点 T − 600s 因此早于
    now − 600s，但实际数据仍在（线段树保留到"最新定稿秒往前 600s"，最新定稿秒
    约为 now − 60s）。旧口径会把长窗口帧大量误报 TooOld。
    """
    async def main():
        qs = QueryService(_engine_with(None))
        for interval in (1000, 1500, 5000, 60_000):
            for off in range(0, 60_000, 5_000):  # 一分钟内各个时刻
                now = NOW + off
                qs.engine.clock.ms = now
                t = (now // interval) * interval
                await qs.topk(Dim.URL, 600_000, 10, t)  # 不应抛 TooOld
    _run(main())


def test_long_window_boundary_too_old():
    """window=600s：at 为 60 秒前正常返回；起点早于 now − 660s 时 TooOld。"""
    async def main():
        qs = QueryService(_engine_with(None))
        # at = now − 60s：起点 = now − 660s，恰在可用范围内
        await qs.topk(Dim.URL, 600_000, 10, NOW - 60_000)
        # at = now − 61s：起点 = now − 661s，早于可用范围 → TooOld
        with pytest.raises(TooOld):
            await qs.topk(Dim.URL, 600_000, 10, NOW - 61_000)
    _run(main())


def test_default_window_reads_snapshot():
    async def main():
        urls = [f"/x{i % 40}" for i in range(500)]
        users = [f"u{i % 30}" for i in range(500)]
        ts = [NOW - 2000 + (i % 1000) for i in range(500)]  # 落在快照窗口 [now-60, now) 内
        eng = _engine_with((urls, users, ts))
        eng.tick()  # 刷新快照
        qs = QueryService(eng)
        a = await qs.topk(Dim.URL, 60_000, 10, None)
        snap = eng.snapshot
        assert a.final is False
        assert a.from_ms == snap.from_ms and a.to_ms == snap.to_ms
        assert a.items == snap.tops[Dim.URL][:10]
        assert a.unseen_upper == snap.unseen_upper[Dim.URL]
    _run(main())


def test_k_over_snapshot_topn_uses_replay():
    async def main():
        # 快照窗口 [S-60, S) 内有一批数据（S = NOW 整秒）；at 带 100ms 偏移时区间
        # 变成 [S-60+? ...]，不再是快照区间——若误走快照，from 会等于 S-60 整秒
        urls = [f"/x{i % 40}" for i in range(300)]
        users = [f"u{i % 20}" for i in range(300)]
        ts = [NOW - 1500 + (i % 1000) for i in range(300)]
        eng = _engine_with((urls, users, ts))
        eng.tick()
        qs = QueryService(eng)
        snap = eng.snapshot
        a = await qs.topk(Dim.URL, 60_000, 10, NOW - 100)  # 带 at 偏移 → 回放
        assert a.from_ms != snap.from_ms  # 区间是回放算的，不是快照的
    _run(main())


def test_final_range_cached_same_object():
    async def main():
        qs = QueryService(_engine_with(None))
        at = NOW - 200_000  # to_ms=at 早于 watermark，final
        b1 = await qs.topk(Dim.URL, 60_000, 10, at)
        b2 = await qs.topk(Dim.URL, 60_000, 10, at)
        assert b1.final is True
        assert b2 is b1  # 第二次返回缓存里的同一对象
    _run(main())


def test_non_final_range_not_cached():
    async def main():
        qs = QueryService(_engine_with(None))
        b1 = await qs.topk(Dim.URL, 60_000, 10, NOW - 1000)  # to_ms=at < watermark? 否，含当前秒
        b2 = await qs.topk(Dim.URL, 60_000, 10, NOW - 1000)
        assert b1.final is False
        assert b2 is not b1  # 非最终态不缓存，每次新建
    _run(main())


def test_cache_evicts_least_recently_used():
    async def main():
        qs = QueryService(_engine_with(None))
        at = NOW - 200_000  # 固定 at，窗口不同 → (d,from,to,k) 不同
        combos = [(Dim.URL, 10) for _ in range(CACHE_CAP)] + [(Dim.USER, 10)]
        for i, (d, k) in enumerate(combos):
            await qs.topk(d, 1000 + i * 1000, k, at)  # window 递增 → from 各不相同
        # 最先写入的（d=URL, window=1000）应被淘汰，最新的（USER, window=257000）仍在
        assert qs._cache_get((Dim.URL, at - 1000, at, 10)) is None
        assert qs._cache_get((Dim.USER, at - (1000 + (CACHE_CAP) * 1000), at, 10)) is not None
        assert len(qs._cache) == CACHE_CAP
    _run(main())


def test_stats_default_reads_snapshot():
    async def main():
        urls = ["/x" for _ in range(500)]
        users = [f"u{i % 12}" for i in range(500)]
        ts = [NOW - 2000 + (i % 1000) for i in range(500)]
        eng = _engine_with((urls, users, ts))
        eng.tick()
        qs = QueryService(eng)
        s = await qs.stats(60_000)
        snap = eng.snapshot
        assert s.from_ms == snap.from_ms and s.to_ms == snap.to_ms
        assert s.distinct_users == snap.distinct_users
        assert s.p95_bytes == snap.p95_bytes
    _run(main())


def test_snapshot_pure_functions_match_default_branch():
    """P2d-2 一：topk_from_snapshot / stats_from_snapshot 与 QueryService 默认
    分支（读快照、不做合并）的结果相同（引擎与前端共用这两个函数）。"""
    async def main():
        urls = [f"/x{i % 40}" for i in range(500)]
        users = [f"u{i % 30}" for i in range(500)]
        ts = [NOW - 2000 + (i % 1000) for i in range(500)]
        eng = _engine_with((urls, users, ts))
        eng.tick()
        qs = QueryService(eng)
        snap = eng.snapshot
        for d in (Dim.URL, Dim.USER):
            for k in (1, 10, 100):
                via_qs = await qs.topk(d, 60_000, k, None)
                via_fn = topk_from_snapshot(snap, d, k, 60_000)
                assert via_qs == via_fn, (d, k)
        s_via_qs = await qs.stats(60_000)
        s_via_fn = stats_from_snapshot(snap, 60_000)
        assert s_via_qs == s_via_fn

    _run(main())


def test_is_snapshot_query():
    """默认窗口判定：不带 at、window 60 秒；topk 时 k 不超过快照名单长度。"""
    assert is_snapshot_query(None, 60_000, None, 100)   # stats：k 为 None
    assert is_snapshot_query(None, 60_000, 10, 100)
    assert is_snapshot_query(None, 60_000, 100, 100)
    assert not is_snapshot_query(None, 30_000, 10, 100)  # window 不是 60 秒
    assert not is_snapshot_query(NOW - 1000, 60_000, 10, 100)  # 带 at
    assert not is_snapshot_query(None, 60_000, 101, 100)  # k 超过快照名单长度


def test_stats_other_window_approximate():
    async def main():
        # 300 个不同用户，bytes 固定 1000
        urls = ["/x" for _ in range(3000)]
        users = [f"u{i % 300}" for i in range(3000)]
        ts = [NOW - 2000 + (i % 1000) for i in range(3000)]
        eng = _engine_with(None)
        eng.ingest(_batch_for((urls, users, ts), nbytes=1000))
        eng.tick()
        qs = QueryService(eng)
        s = await qs.stats(30_000)
        assert abs(s.distinct_users - 300) / 300 < 0.05
        assert 900 <= s.p95_bytes <= 1100  # p95 ≈ 1000
    _run(main())


# ---- 10 分钟数据：200 个随机区间的上下界与召回（慢）----

@pytest.fixture(scope="module")
def dataset():
    """10 分钟、每秒 ~300 条长尾事件，逐秒推进时钟 + tick。返回 (engine, qs, urls, users, ts)。

    event_id 全局唯一（e{t}-{i}）：若各批都用 e0..e299，同一 10 秒 Bloom 代里相邻批次
    的同号 id 会被去重丢掉，精确计数（原始事件）就会比引擎多算。
    """
    clock = FakeClock(NOW)
    eng = Engine(Config(), clock)
    rng = np.random.default_rng(0)
    w = 1.0 / np.arange(1, N_URL + 1) ** 1.1
    w = w / w.sum()
    all_urls, all_users, all_ts = [], [], []
    for t in range(600):
        now = NOW + t * 1000
        n = 300
        idx = rng.choice(N_URL, size=n, p=w)
        urls = [f"/api/item/x{u}" for u in idx.tolist()]
        users = [f"u{u}" for u in rng.integers(0, N_USER, n).tolist()]
        ts = [now - int(x) for x in rng.integers(0, 1000, n)]
        eng.ingest(EventBatch(
            [f"e{t}-{i}" for i in range(n)], users, urls,
            np.asarray(ts, dtype=np.int64),
            np.array([1000 + (i % 100) for i in range(n)], dtype=np.int64), 0))
        all_urls.extend(urls); all_users.extend(users); all_ts.extend(ts)
        clock.advance(1000)
        eng.tick()
    qs = QueryService(eng)
    return eng, qs, all_urls, all_users, all_ts


@pytest.mark.slow
def test_dataset_built(dataset):
    eng, qs, urls, users, ts = dataset
    assert len(urls) == 600 * 300
    assert eng.trees[Dim.URL].newest is not None  # 早期数据已进树
    assert eng.snapshot is not None


@pytest.mark.slow
def test_random_ranges_bounds_and_recall(dataset):
    eng, qs, urls, users, ts = dataset
    urls_arr = np.asarray(urls)
    users_arr = np.asarray(users)
    ts_arr = np.asarray(ts, dtype=np.int64)
    s_start = NOW // 1000 + 1
    s_end = (NOW + 600 * 1000) // 1000  # 最后一秒
    rng = np.random.default_rng(42)

    def run(q):
        return asyncio.run(q)

    for _ in range(200):
        at = int(rng.integers(s_start * 1000 + 1, s_end * 1000))  # at 毫秒，含随机毫秒
        window = int(rng.integers(1, 601)) * 1000                 # 1~600 秒
        from_ms, to_ms = at - window, at
        # 保留期约束：from_ms 不早于 s_start 整秒
        if from_ms < s_start * 1000:
            from_ms = s_start * 1000
            at = to_ms = from_ms + window
        k = 10
        ans = run(qs.topk(Dim.URL, window, k, at))
        # 上下界：结果里每个 key 的真实次数必须落在 [lower, upper]
        sel = (ts_arr >= ans.from_ms) & (ts_arr < ans.to_ms)
        exact = collections.Counter(urls_arr[sel].tolist())
        for it in ans.items:
            real = exact.get(it.key, 0)
            assert it.lower <= real <= it.upper, f"{it.key}: real={real} 不在 [{it.lower},{it.upper}]"
        # 召回（并列不算错）：返回的 key 真实次数 ≥ 精确答案第 10 名的次数就算命中。
        # 窗口 ≥ 30 秒时逐条严格断言：命中不少于 9/10
        if window >= 30_000:
            ranked = exact.most_common(10)
            assert len(ranked) == 10  # 该窗口 key 数远多于 10
            tenth = ranked[-1][1]
            got = [it.key for it in ans.items[:10]]
            hit = sum(1 for key in got if exact.get(key, 0) >= tenth)
            assert hit >= 9, f"窗口 {window}ms 召回 {hit}/10：got={got} 精确={ranked}"
