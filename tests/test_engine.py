import math

import numpy as np
import pytest

from flowlens.bounds import for_keys, top_k
from flowlens.config import Config
from flowlens.dims import Dim
from flowlens.engine import Engine
from flowlens.errors import TooOld
from flowlens.hashing import key_hash, key_hashes
from flowlens.model import EventBatch, parse_events
from flowlens.timewin import FakeClock, split_range

NOW = 1_735_689_600_000
RETENTION = 600  # cfg.retention_sec


def make_batch(eids, users, urls, ts, nbytes):
    """直接构造 EventBatch，绕过 parse_events，便于精确控制每列。"""
    return EventBatch(
        list(eids), list(users), list(urls),
        np.array(ts, dtype=np.int64), np.array(nbytes, dtype=np.int64), 0,
    )


def new_engine():
    clock = FakeClock(NOW)
    return Engine(Config(), clock), clock


def exact_counts(values, ts, from_ms, to_ms):
    """按毫秒精确算各 key 在 [from_ms, to_ms) 内的真实次数。"""
    import collections
    ts = np.asarray(ts)
    sel = (ts >= from_ms) & (ts < to_ms)
    return collections.Counter(np.asarray(values)[sel].tolist())


def check_bounds_within(parts, values, ts, from_ms, to_ms):
    """断言：每个在区间内出现过的 key，真实次数落在 [lower, upper] 内。"""
    exact = exact_counts(values, ts, from_ms, to_ms)
    if not exact:
        return
    keys = list(exact)
    lo, hi = for_keys(parts, key_hashes(keys))
    for k, l, h in zip(keys, lo, hi):
        real = exact[k]
        assert l <= real <= h, f"{k}: real={real} 不在 [{l},{h}]"


# ---- 写入后 collect_parts 能查到，bounds 成立 ----

def test_write_then_query_bounds():
    eng, clock = new_engine()
    urls = [f"/api/item/{i % 200}" for i in range(500)]
    users = [f"u{i % 50}" for i in range(500)]
    ts = [NOW - 1000 + i for i in range(500)]
    eng.ingest(make_batch([f"e{i}" for i in range(500)], users, urls, ts, [100] * 500))
    parts = eng.collect_parts(Dim.URL, split_range(NOW - 60_000, NOW))
    check_bounds_within(parts, urls, ts, NOW - 60_000, NOW)
    # 最热 key 确实在结果里
    ans = top_k(parts, 10)
    assert any(it.key for it in ans.items)


def test_write_then_query_user_dim():
    eng, clock = new_engine()
    users = [f"u{i % 50}" for i in range(300)]
    ts = [NOW - 1000 + i for i in range(300)]
    eng.ingest(make_batch([f"e{i}" for i in range(300)], users, ["/x"] * 300, ts, [10] * 300))
    parts = eng.collect_parts(Dim.USER, split_range(NOW - 60_000, NOW))
    check_bounds_within(parts, users, ts, NOW - 60_000, NOW)


# ---- 推进 70 秒并 tick，数据进树；跨边界区间 bounds 仍成立 ----

def test_tick_moves_data_to_tree_and_cross_boundary():
    eng, clock = new_engine()
    urls = [f"/api/item/{i % 300}" for i in range(1000)]
    users = [f"u{i % 80}" for i in range(1000)]
    ts = [NOW - 1000 + i for i in range(1000)]
    eng.ingest(make_batch([f"e{i}" for i in range(1000)], users, urls, ts, [200] * 1000))
    clock.advance(70_000)
    eng.tick()
    assert eng.trees[Dim.URL].newest is not None  # 树里有数据了
    # 横跨树/环边界的区间：整段 [NOW-1000, NOW+60s]
    parts = eng.collect_parts(Dim.URL, split_range(NOW - 1000, NOW + 60_000))
    check_bounds_within(parts, urls, ts, NOW - 1000, NOW + 60_000)
    parts_u = eng.collect_parts(Dim.USER, split_range(NOW - 1000, NOW + 60_000))
    check_bounds_within(parts_u, users, ts, NOW - 1000, NOW + 60_000)


# ---- 随机 100 个带毫秒边界的区间：真实次数落在上下界内 ----

def test_random_millisecond_ranges():
    eng, clock = new_engine()
    rng = np.random.default_rng(0)
    n = 5000
    urls = [f"/api/item/{i % 300}" for i in range(n)]
    users = [f"u{i % 200}" for i in range(n)]
    # ts 分散在最近 55 秒内（落在环窗口内，写入时都不会被丢），带任意毫秒
    ts = [NOW - int(x) for x in rng.integers(1, 55_000, n)]
    eng.ingest(make_batch([f"e{i}" for i in range(n)], users, urls, ts, [50] * n))
    clock.advance(30_000)  # 让较旧的 30 秒定稿进树，最近的留在环里
    eng.tick()
    lo_all, hi_all = NOW - 55_000, NOW - 1
    for _ in range(100):
        a = int(rng.integers(lo_all, hi_all))
        b = int(rng.integers(a + 1, hi_all + 1))
        parts = eng.collect_parts(Dim.URL, split_range(a, b))
        check_bounds_within(parts, urls, ts, a, b)


# ---- 同批 / 跨批重复、迟到、未来、invalid 计数 ----

def test_dedup_within_batch():
    eng, clock = new_engine()
    ts = [NOW - 100] * 4
    b = make_batch(["a", "b", "a", "a"], ["u"] * 4, ["/x"] * 4, ts, [1] * 4)
    res = eng.ingest(b)
    assert res.accepted == 2 and res.deduped == 2 and res.late == 0 and res.invalid == 0
    # 重复只计一次
    parts = eng.collect_parts(Dim.URL, split_range(NOW - 60_000, NOW))
    from flowlens.bounds import for_key
    lo, hi = for_key(parts, key_hash("/x"))
    assert lo <= 2 <= hi and lo > 0  # 两次不同 key a,b → 2 次


def test_dedup_across_batches():
    eng, clock = new_engine()
    b1 = make_batch(["a", "a"], ["u", "u"], ["/x", "/x"], [NOW - 100, NOW - 100], [1, 1])
    r1 = eng.ingest(b1)
    b2 = make_batch(["a"], ["u"], ["/x"], [NOW - 100], [1])
    r2 = eng.ingest(b2)
    assert r1.accepted == 1 and r1.deduped == 1
    assert r2.accepted == 0 and r2.deduped == 1


def test_late_event():
    eng, clock = new_engine()
    b = make_batch(["a"], ["u"], ["/x"], [NOW - 61_000], [1])  # 早于 watermark(now-60s)
    res = eng.ingest(b)
    assert res.late == 1 and res.accepted == 0


def test_future_event_counts_invalid():
    eng, clock = new_engine()
    b = make_batch(["a"], ["u"], ["/x"], [NOW + 10_000], [1])  # 超过 now+5s
    res = eng.ingest(b)
    assert res.invalid == 1 and res.accepted == 0


def test_batch_invalid_passthrough():
    eng, clock = new_engine()
    b = make_batch(["a"], ["u"], ["/x"], [NOW - 100], [1])
    b.invalid = 3  # parse_events 已剔除了 3 条
    res = eng.ingest(b)
    assert res.invalid == 3 and res.accepted == 1


def test_counters_accumulate():
    eng, clock = new_engine()
    eng.ingest(make_batch(["a"], ["u"], ["/x"], [NOW - 100], [1]))
    eng.ingest(make_batch(["b", "b"], ["u", "u"], ["/y", "/y"], [NOW - 100, NOW - 100], [1, 1]))
    c = eng.counters
    assert c.events_ingested == 2
    assert c.deduped == 1
    assert c.invalid == 0
    assert c.late == 0


# ---- late_at_engine：把时钟往回拨，让事件秒已定稿离开环 ----

def test_late_at_engine_by_clock_stepback():
    # 正常单时钟下，ACCEPT 的秒永远 ≥ ring.next（next 跟着 watermark 走），
    # 所以 ring.slot 返回 None 只在时钟回拨后出现：先推进再回拨，让某秒"重新变 ACCEPT
    # 但已定稿离开环"。
    S0 = NOW // 1000
    eng, clock = new_engine()
    clock.advance(70_000)          # now = NOW+70s
    eng.tick()                      # 定稿到 S0+9，ring.next = S0+10
    assert eng.ring.next == S0 + 10
    clock.advance(-5_000)           # 回拨到 NOW+65s，watermark = NOW+5s
    res = eng.ingest(make_batch(["c"], ["u"], ["/y"], [NOW + 6_000], [1]))
    # ts=NOW+6s ≥ watermark(NOW+5s) → ACCEPT；秒 S0+6 < next(S0+10) → slot 返回 None
    assert res.accepted == 0
    assert res.late == 1
    assert res.deduped == 0 and res.invalid == 0
    assert eng.counters.late_at_engine == 1


# ---- 定稿幂等：两维写入中途失败，下一拍重试能恢复 ----

def test_on_finalize_idempotent_after_partial_failure():
    S0 = NOW // 1000
    eng, clock = new_engine()
    eng.ingest(make_batch([f"e{i}" for i in range(100)], ["u"] * 100, ["/x"] * 100,
                          [NOW - 100] * 100, [10] * 100))  # 秒 S0-1 有数据
    # 让 USER 维的 add_leaf 在写 S0-1 时第一次抛异常（模拟第二维定稿失败；
    # URL 维先写入成功，失败时 URL 树已含 S0-1、USER 树只到 S0-2）
    user_tree = eng.trees[Dim.USER]
    orig_add = user_tree.add_leaf
    calls = {}

    def flaky_add(inp):
        if inp.sec == S0 - 1 and "n" not in calls:
            calls["n"] = 1
            raise RuntimeError("模拟定稿中途失败")
        return orig_add(inp)

    user_tree.add_leaf = flaky_add
    clock.advance(61_000)  # S0-1 过 watermark，需要定稿
    with pytest.raises(RuntimeError):
        eng.advance()
    assert eng.trees[Dim.URL].newest == S0 - 1   # 第一维已写入
    assert user_tree.newest == S0 - 2            # 第二维还差 S0-1
    assert eng.ring.next == S0 - 1               # 异常中断，环未推进
    user_tree.add_leaf = orig_add                # 只第一次失败
    eng.advance()                                # 下一拍重试：URL 幂等跳过，USER 补 S0-1
    assert eng.ring.next == S0 + 1
    assert eng.trees[Dim.URL].newest == S0
    assert user_tree.newest == S0
    # 两棵树都包含 S0-1，且只写入一次（若重复写入，次数会翻倍成 200）
    leaf_u = eng.trees[Dim.URL].leaf(S0 - 1)
    leaf_r = user_tree.leaf(S0 - 1)
    assert leaf_u is not None and leaf_r is not None
    assert leaf_u.upper.sum() == 100 and leaf_r.upper.sum() == 100
    # 恢复后写入路径正常
    res = eng.ingest(make_batch(["z"], ["v"], ["/z"], [NOW + 60_900], [5]))
    assert res.accepted == 1


# ---- 保留期检查 + 空树不报错 ----

def test_too_old():
    eng, clock = new_engine()
    with pytest.raises(TooOld):
        eng.collect_parts(Dim.URL, split_range(NOW - (RETENTION + 100) * 1000, NOW - RETENTION * 1000))
    with pytest.raises(TooOld):
        eng.collect_stats(Dim.URL, split_range(NOW - (RETENTION + 100) * 1000, NOW - RETENTION * 1000))


def test_empty_tree_no_error():
    eng, clock = new_engine()
    # 没写入任何数据，线段树为空：查询不报错，返回空
    parts = eng.collect_parts(Dim.URL, split_range(NOW - 60_000, NOW))
    assert parts == [] or all(len(p.hashes) == 0 for p in parts)
    st = eng.collect_stats(Dim.URL, split_range(NOW - 60_000, NOW))
    assert st is not None


# ---- collect_stats：去重人数 / p95 在误差范围内，边缘秒 0.5 规则 ----

def test_collect_stats_hll_and_dd():
    eng, clock = new_engine()
    n = 2000
    users = [f"u{i % 300}" for i in range(n)]  # 300 个不同用户
    nbytes = [1000 + (i % 100) * 10 for i in range(n)]
    ts = [NOW - 1000 + (i % 500) for i in range(n)]
    eng.ingest(make_batch([f"e{i}" for i in range(n)], users, ["/x"] * n, ts, nbytes))
    # user 维度：去重人数
    st = eng.collect_stats(Dim.USER, split_range(NOW - 60_000, NOW))
    assert st.hll is not None and st.dd is None  # user 只有 HLL
    est = st.hll.estimate()
    assert abs(est - 300) / 300 < 0.05  # HLL 误差 ~1%
    # url 维度：p95
    stu = eng.collect_stats(Dim.URL, split_range(NOW - 60_000, NOW))
    assert stu.hll is None and stu.dd is not None  # url 只有 DDSketch
    p95 = stu.dd.quantile(0.95)
    exact_sorted = sorted(nbytes)
    exact_p95 = exact_sorted[int(0.95 * (n - 1))]
    assert abs(p95 - exact_p95) / exact_p95 < 0.05


def test_collect_stats_edge_half_rule():
    # 边缘秒 frac < 0.5 不计入 stats，frac >= 0.5 计入
    eng, clock = new_engine()
    # 只在一个秒的边缘写入
    ts = [NOW - 900] * 100  # 秒 NOW-1
    nbytes = [1000] * 100
    eng.ingest(make_batch([f"e{i}" for i in range(100)], ["u"] * 100, ["/x"] * 100, ts, nbytes))
    # 区间只覆盖该秒的前半 [NOW-1000, NOW-1500) 是错的；用 split_range 造一个 frac<0.5 的边缘
    # 取 [NOW-1100, NOW-900+400) 之类让边缘 frac < 0.5
    r = split_range(NOW - 1000, NOW - 1000 + 400)  # 覆盖 [NOW-1, NOW-1) 的前 0.4 秒
    assert r.edges and r.edges[0].sec == NOW // 1000 - 1
    st = eng.collect_stats(Dim.USER, r)
    # frac=0.4 < 0.5 → 不计入该秒
    assert st.hll.estimate() == 0
    r2 = split_range(NOW - 1000, NOW - 1000 + 600)  # 前 0.6 秒
    assert r2.edges and r2.edges[0].frac >= 0.5
    st2 = eng.collect_stats(Dim.USER, r2)
    assert st2.hll.estimate() > 0


# ---- 维度 Stats 只带对应概要 ----

def test_dim_stats_shape():
    eng, clock = new_engine()
    eng.ingest(make_batch(["a"], ["u"], ["/x"], [NOW - 100], [10]))
    su = eng.collect_stats(Dim.USER, split_range(NOW - 60_000, NOW))
    st = eng.collect_stats(Dim.URL, split_range(NOW - 60_000, NOW))
    assert su.hll is not None and su.dd is None
    assert st.hll is None and st.dd is not None


# ---- 推进 70 秒并 tick 后，对线段树里的区间 collect_stats ----

def test_collect_stats_on_tree():
    eng, clock = new_engine()
    n = 2000
    users = [f"u{i % 300}" for i in range(n)]     # 300 个不同用户
    nbytes = [1000 + (i % 100) * 10 for i in range(n)]
    ts = [NOW - 1000 - i for i in range(n)]        # 全在 NOW-1s 之前
    eng.ingest(make_batch([f"e{i}" for i in range(n)], users, ["/x"] * n, ts, nbytes))
    clock.advance(70_000)
    eng.tick()
    assert eng.trees[Dim.URL].newest is not None   # 数据已进树
    r = split_range(NOW - 60_000, NOW)
    su = eng.collect_stats(Dim.USER, r)
    assert su.hll is not None and abs(su.hll.estimate() - 300) / 300 < 0.05
    stu = eng.collect_stats(Dim.URL, r)
    p95 = stu.dd.quantile(0.95)
    exact = sorted(nbytes)
    exact_p95 = exact[int(0.95 * (n - 1))]
    assert abs(p95 - exact_p95) / exact_p95 < 0.05


def test_collect_stats_edge_in_tree_half_rule():
    # 数据全部落在某秒 S 内；推进后该秒定稿进树。
    # 边缘秒（落在树里的秒）覆盖 frac < 0.5 不计入、frac >= 0.5 计入。
    eng, clock = new_engine()
    S = NOW // 1000 - 1
    ts = [S * 1000 + 500] * 100
    eng.ingest(make_batch([f"e{i}" for i in range(100)], ["u"] * 100, ["/x"] * 100, ts, [10] * 100))
    clock.advance(70_000)
    eng.tick()
    assert eng.trees[Dim.USER].leaf(S) is not None  # 该秒已进树
    # frac = 0.4 < 0.5 → 不计入该秒
    r = split_range((S + 1) * 1000 - 400, (S + 1) * 1000)
    assert r.edges[0].sec == S and r.edges[0].frac == 0.4
    assert eng.collect_stats(Dim.USER, r).hll.estimate() == 0
    # frac = 0.6 >= 0.5 → 计入该秒
    r2 = split_range((S + 1) * 1000 - 600, (S + 1) * 1000)
    assert r2.edges[0].sec == S and r2.edges[0].frac == 0.6
    assert eng.collect_stats(Dim.USER, r2).hll.estimate() > 0


# ---- T3 快照 ----

def snapshot_range(now_ms):
    """refresh_snapshot 用的区间：最近 60 个整秒，不含当前秒。"""
    n = now_ms // 1000
    return split_range((n - 60) * 1000, n * 1000)


def test_snapshot_initially_not_none():
    eng, _ = new_engine()
    assert eng.snapshot is not None
    assert eng.snapshot.at_ms == NOW
    assert eng.snapshot.from_ms == (NOW // 1000 - 60) * 1000
    assert eng.snapshot.to_ms == (NOW // 1000) * 1000


def test_snapshot_matches_direct_query():
    # 快照与"对同一区间直接调用 collect_parts + top_k"的结果逐个相等
    eng, clock = new_engine()
    urls = [f"/api/item/{i % 200}" for i in range(2000)]
    users = [f"u{i % 120}" for i in range(2000)]
    ts = [NOW - 1000 - i for i in range(2000)]  # 全部落在快照区间内
    eng.ingest(make_batch([f"e{i}" for i in range(2000)], users, urls, ts, [100] * 2000))
    snap = eng.refresh_snapshot()
    r = snapshot_range(clock.now_ms())
    for d in (Dim.URL, Dim.USER):
        ans = top_k(eng.collect_parts(d, r), 100)
        assert snap.tops[d] == tuple(ans.items), f"维度 {d} 的 tops 与直接查询不一致"
        assert snap.unseen_upper[d] == ans.unseen_upper
    su = eng.collect_stats(Dim.USER, r)
    stu = eng.collect_stats(Dim.URL, r)
    assert snap.distinct_users == su.hll.estimate()
    assert snap.p95_bytes == stu.dd.quantile(0.95)


def test_snapshot_excludes_current_second():
    # 快照区间为 [now−60s, now)，当前秒还在持续写入，不能进快照。
    # NOW 恰好落在秒边界上：当前秒 = NOW//1000，写 NOW+500（未来 0.5 s，仍被接受）；
    # 上一秒 = NOW//1000−1，写 NOW−100。
    eng, clock = new_engine()
    eng.ingest(make_batch(["a"], ["u1"], ["/hot"], [NOW + 500], [5000]))   # 当前秒，不进快照
    eng.ingest(make_batch(["b"], ["u2"], ["/prev"], [NOW - 100], [10]))    # 上一秒，进快照
    snap = eng.refresh_snapshot()
    url_keys = [x.key for x in snap.tops[Dim.URL]]
    assert "/prev" in url_keys and "/hot" not in url_keys
    user_keys = [x.key for x in snap.tops[Dim.USER]]
    assert "u2" in user_keys and "u1" not in user_keys
    # p95 只由快照区间内的事件决定：若当前秒的 5000 字节被计入，分位数会跳到 ~5000
    assert 8 <= snap.p95_bytes <= 12
    est = snap.distinct_users
    assert est > 0 and est < 10  # 只有 u2，u1 不应被计入


def test_empty_engine_snapshot():
    eng, _ = new_engine()
    snap = eng.snapshot
    assert len(snap.tops) == 2
    assert all(t == () for t in snap.tops)
    assert all(u == 0.0 for u in snap.unseen_upper)
    assert snap.distinct_users == 0.0
    assert math.isnan(snap.p95_bytes)


def test_snapshot_after_advance_matches_replay():
    # 时钟推进、数据部分定稿进线段树后，快照依然与直接查询一致
    eng, clock = new_engine()
    urls = [f"/api/item/{i % 300}" for i in range(3000)]
    users = [f"u{i % 150}" for i in range(3000)]
    ts = [NOW - 1000 - i for i in range(3000)]  # 覆盖 [NOW-4.05s, NOW-1s)
    eng.ingest(make_batch([f"e{i}" for i in range(3000)], users, urls, ts, [50] * 3000))
    clock.advance(30_000)  # 较早的秒定稿进树，其余留在环里
    eng.tick()
    snap = eng.snapshot
    assert snap.at_ms == NOW + 30_000
    assert snap.from_ms == (NOW // 1000 + 30 - 60) * 1000
    assert snap.to_ms == (NOW // 1000 + 30) * 1000
    # 区间 [NOW-31s, NOW+30s) 横跨树与环边界（数据在 [NOW-4.05s, NOW-1s)）
    r = split_range(NOW - 31_000, NOW + 30_000)
    for d in (Dim.URL, Dim.USER):
        ans = top_k(eng.collect_parts(d, r), 100)
        assert snap.tops[d] == tuple(ans.items)
    assert snap.distinct_users == eng.collect_stats(Dim.USER, r).hll.estimate()
    assert snap.p95_bytes == eng.collect_stats(Dim.URL, r).dd.quantile(0.95)


def test_snapshot_immutability():
    # 生成新快照后，之前拿到的旧快照内容不变
    eng, clock = new_engine()
    eng.ingest(make_batch(["a"], ["u"], ["/x"], [NOW - 2000], [10]))
    old = eng.refresh_snapshot()  # 含 /x（在 60 秒窗口内）
    assert any(x.key == "/x" for x in old.tops[Dim.URL])
    assert 8 <= old.p95_bytes <= 12  # 单条 10 字节事件，DDSketch 近似分位数
    clock.advance(62_000)  # 数据所在秒滑出 60 秒窗口
    eng.tick()
    assert eng.snapshot is not old
    assert eng.snapshot.tops[Dim.URL] == ()
    # 旧快照不受后续刷新影响
    assert any(x.key == "/x" for x in old.tops[Dim.URL])
    assert 8 <= old.p95_bytes <= 12


# ---- ingest(weight=2)：真实次数 *2 落在上下界内 ----

def test_ingest_weight_doubles_counts():
    eng, clock = new_engine()
    urls = [f"/api/item/{i % 100}" for i in range(500)]
    ts = [NOW - 1000 + i for i in range(500)]
    eng.ingest(make_batch([f"e{i}" for i in range(500)], ["u"] * 500, urls, ts, [10] * 500), weight=2)
    parts = eng.collect_parts(Dim.URL, split_range(NOW - 60_000, NOW))
    import collections
    exact = collections.Counter(urls)
    lo, hi = for_keys(parts, key_hashes(list(exact)))
    for k, l, h in zip(exact, lo, hi):
        assert l <= exact[k] * 2 <= h, f"{k}: real*2={exact[k]*2} 不在 [{l},{h}]"


# ---- user_id 末尾带 '\x00'：用原始字符串的哈希能查到，次数正确 ----

def test_user_with_trailing_nul_char():
    eng, clock = new_engine()
    uid = "userA\x00"
    import orjson
    body = orjson.dumps([{"event_id": "e1", "user_id": uid, "url": "/x", "ts": NOW - 100, "bytes": 10}])
    batch = parse_events(body)
    assert len(batch) == 1 and batch.users[0] == uid  # 原样保留
    eng.ingest(batch)
    parts = eng.collect_parts(Dim.USER, split_range(NOW - 60_000, NOW))
    lo, hi = for_keys(parts, key_hashes([uid]))
    assert lo <= 1 <= hi and hi >= 1
