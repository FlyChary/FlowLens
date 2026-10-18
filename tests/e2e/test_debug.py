"""B1 e2e：调试接口 GET /debug/structure。

覆盖点（对应 B1.md 第二节）：
- 未设 FLOWLENS_DEBUG=1 时访问 /debug/structure 返回 404（与其他不存在路径一致）；
- 开启后字段齐全；
- 带 at 与 window 时，返回的拆分与 engine._plan 的结果一致；
- 节点的事件数之和与对应区间叶子的事件数之和一致（取 url 维叶子的 DDSketch 总计数）。

时钟用 FakeClock、数据用长尾 URL 逐秒写入并定稿进线段树。慢测试（满载 10 分钟历史
的耗时与响应体大小测量）标 @pytest.mark.slow，默认 pytest -q 跳过，需显式
pytest -q -m slow 运行。
"""

import asyncio
import os
import statistics
import time

import numpy as np
import orjson
import pytest
from aiohttp.test_utils import TestClient, TestServer

from flowlens.api.app import create_app
from flowlens.api.debug import build_structure
from flowlens.config import Config
from flowlens.dims import Dim
from flowlens.engine import Engine
from flowlens.ingest import Admission, IngestService
from flowlens.model import parse_events
from flowlens.query import QueryService
from flowlens.timewin import FakeClock, split_range

NOW = 1_735_689_600_000
LINGERING_TIME = 1  # 与生产 __main__.py 一致
N_USER = 200_000


def _fake_memory_reader() -> tuple[int, int | None]:
    """测试用假内存读数：恒 (0, None)，与运行环境真实内存解耦（同 test_api）。"""
    return 0, None


async def _get(client, url, *a, **kw):
    from aiohttp.client_exceptions import ClientOSError
    for _ in range(3):
        try:
            return await client.get(url, *a, **kw)
        except ClientOSError:
            await asyncio.sleep(0.05)
    raise RuntimeError("get 连续 3 次连接被重置")


def _make_second_body(s: int, n: int, url_n: int, seed: int) -> bytes:
    """第 s 秒内 n 条事件：长尾 URL（/api/item/xN，归一化后仍是多个不同 URL）。"""
    rng = np.random.default_rng(seed)
    w = 1.0 / np.arange(1, url_n + 1) ** 1.1
    idx = rng.choice(url_n, size=n, p=w / w.sum())
    urls = [f"/api/item/x{u}" for u in idx.tolist()]
    users = [f"u{u}" for u in rng.integers(0, N_USER, n).tolist()]
    ts = [s * 1000 + int(x) for x in rng.integers(0, 1000, n)]
    evs = [{"event_id": f"e{s}_{j}", "user_id": users[j], "url": urls[j],
            "ts": ts[j], "bytes": 1000 + (j % 100)} for j in range(n)]
    return orjson.dumps(evs)


def _load_tree(engine: Engine, clock: FakeClock, n_secs: int,
               per_sec: int = 1500, url_n: int = 20_000) -> int:
    """逐秒写入并定稿，使线段树积累 n_secs 秒历史。返回最新定稿秒。

    第 s 秒：时钟放到第 s 秒内（事件判为 ACCEPT）写入，再把时钟推进到 watermark
    越过 s、调 engine.advance() 让该秒定稿进树。时钟只前进，符合活跃环的语义。
    """
    base = clock.now_ms() // 1000
    for t in range(1, n_secs + 1):
        s = base + t - 1
        clock.ms = s * 1000 + 500
        engine.ingest(parse_events(_make_second_body(s, per_sec, url_n, 1000 + s)))
        clock.ms = (s + 1) * 1000 + 60_000 + 1  # watermark 越过 s
        engine.advance()
    return base + n_secs - 1


def _serve(run, *, debug_enabled: bool, preload=None):
    """起真实服务器；debug_enabled 控制是否设 FLOWLENS_DEBUG=1；await run 后关闭。"""
    async def main():
        old = os.environ.get("FLOWLENS_DEBUG")
        if debug_enabled:
            os.environ["FLOWLENS_DEBUG"] = "1"
        elif old is not None:
            del os.environ["FLOWLENS_DEBUG"]
        try:
            clock = FakeClock(NOW)
            engine = Engine(Config(), clock)
            if preload is not None:
                preload(engine, clock)
            app = create_app(engine, IngestService(engine, Admission()),
                             QueryService(engine), tick_period=1000.0,
                             memory_reader=_fake_memory_reader)
            server = TestServer(app)
            client = TestClient(server)
            await server.start_server(auto_decompress=False, lingering_time=LINGERING_TIME)
            try:
                await run(client, engine, clock, app)
            finally:
                await client.close()
        finally:
            if old is None:
                os.environ.pop("FLOWLENS_DEBUG", None)
            else:
                os.environ["FLOWLENS_DEBUG"] = old
    asyncio.run(main())


# ---- 1. 未开启时返回 404 ----

def test_debug_disabled_returns_404():
    async def run(client, engine, clock, app):
        r = await _get(client, "/debug/structure")
        assert r.status == 404
        body = await r.json()
        assert "error" in body  # 与其他不存在的路径一致的 JSON 错误体
    _serve(run, debug_enabled=False)


def test_debug_other_unknown_path_also_404():
    async def run(client, engine, clock, app):
        # 对照：一个确实不存在的路径也是 404，验证 /debug/structure 未注册时与之一致
        r = await _get(client, "/definitely/not/here")
        assert r.status == 404
    _serve(run, debug_enabled=False)


# ---- 2. 开启后字段齐全 ----

def test_debug_fields_complete():
    async def run(client, engine, clock, app):
        r = await _get(client, "/debug/structure")
        assert r.status == 200
        b = await r.json()
        # 顶层字段
        for f in ("now_ms", "now_sec", "ring_next", "latest_finalized_sec",
                  "retention_sec", "retention_range", "ring", "tree"):
            assert f in b, f"缺字段 {f}"
        assert b["now_ms"] == NOW
        assert b["now_sec"] == NOW // 1000
        # 空引擎：ring.advance 把 next 设为 watermark 的秒号 = 当前秒 - 60
        assert b["ring_next"] == NOW // 1000 - 60
        assert b["latest_finalized_sec"] is None  # 尚未定稿
        assert b["retention_sec"] == engine.cfg.retention_sec
        assert isinstance(b["ring"], list) and b["ring"] == []  # 空引擎无数据秒桶
        # tree 结构字段
        for f in ("by", "newest", "retention_range", "nodes", "builders"):
            assert f in b["tree"], f"tree 缺字段 {f}"
        assert b["tree"]["by"] == "url"
        assert b["tree"]["newest"] is None
        assert b["tree"]["nodes"] == [] and b["tree"]["builders"] == []
        # by=user 也能工作
        r = await _get(client, "/debug/structure", params={"by": "user"})
        assert r.status == 200
        assert (await r.json())["tree"]["by"] == "user"
    _serve(run, debug_enabled=True)


def test_debug_ring_bucket_fields():
    """写入后环内有数据：每个秒桶带 秒号/事件数/两维名单摘要（含前 3 key）。"""
    def preload(engine, clock):
        clock.ms = NOW
        engine.ingest(parse_events(_make_second_body(NOW // 1000, 200, 20000, 7)))

    async def run(client, engine, clock, app):
        r = await _get(client, "/debug/structure", params={"by": "url"})
        assert r.status == 200
        b = await r.json()
        assert len(b["ring"]) == 1
        slot = b["ring"][0]
        assert slot["sec"] == NOW // 1000
        assert slot["events"] == 200  # url 维 DDSketch 总计数
        for d in ("url", "user"):
            ds = slot["dims"][d]
            for f in ("len", "cap", "u", "top"):
                assert f in ds, f"名单摘要缺字段 {f}"
            assert ds["cap"] == 512
            assert ds["len"] <= 512
            assert len(ds["top"]) <= 3
            for key, cnt in ds["top"]:
                assert isinstance(key, str) and cnt > 0
    _serve(run, debug_enabled=True, preload=preload)


# ---- 3. 带 at 与 window 时，拆分与 engine._plan 一致 ----

def test_debug_plan_matches_engine_plan():
    def preload(engine, clock):
        _load_tree(engine, clock, 40, per_sec=600, url_n=5000)

    async def run(client, engine, clock, app):
        # 固定一个查询时刻与窗口（非整秒，保证两端有边缘秒）
        at_ms = NOW - 1234
        window_ms = 100_000
        r = await _get(client, "/debug/structure",
                       params={"by": "url", "at": str(at_ms), "window": "100s"})
        assert r.status == 200
        b = await r.json()
        assert "plan" in b
        got = b["plan"]
        assert got["from_ms"] == at_ms - window_ms
        assert got["to_ms"] == at_ms
        # 用真实查询的唯一划分入口独立复算
        d = Dim.URL
        plan = engine._plan(d, split_range(at_ms - window_ms, at_ms))
        assert got["ring_secs"] == [bk.sec for bk in plan.ring_buckets]
        exp_nodes = []
        if plan.history is not None:
            s0, e0 = plan.history
            exp_nodes = [{"level": n.level, "start": n.start, "end": n.end}
                         for n in engine._cover(d, s0, e0, stats_only=False)]
        assert got["tree_nodes"] == exp_nodes
        exp_edges = [{"sec": e.sec, "frac": e.frac, "in_ring": e.in_ring} for e in plan.edges]
        assert got["edges"] == exp_edges
        # 本用例：窗口横跨已定稿的树历史 → 至少有一个树节点
        assert len(got["tree_nodes"]) >= 1
        # 非整秒边界 → 有边缘秒
        assert len(got["edges"]) >= 1
    _serve(run, debug_enabled=True, preload=preload)


# ---- 4. 节点事件数之和 = 对应区间叶子事件数之和 ----

def test_debug_node_events_match_leaves():
    """对每个带统计概要（level 0 或 >= stats_min_level）的已冻结节点，其事件数应等于
    该节点覆盖范围内所有秒的 url 维叶子事件数之和（DDSketch 合并时 n 累加）。"""

    def preload(engine, clock):
        _load_tree(engine, clock, 40, per_sec=1500, url_n=20000)

    async def run(client, engine, clock, app):
        r = await _get(client, "/debug/structure", params={"by": "url"})
        assert r.status == 200
        b = await r.json()
        # 独立取 url 维叶子事件数
        leaf_events = {s: leaf.stats.dd.n for s, leaf in
                       engine.trees[Dim.URL].levels[0].items()
                       if leaf.stats is not None and leaf.stats.dd is not None}
        checked = 0
        for node in b["tree"]["nodes"]:
            # 只对带 DDSketch 的层校验（level 0 叶子恒带；level >= stats_min_level 带）
            has_stats = node["level"] == 0 or node["level"] >= engine.cfg.stats_min_level
            if not has_stats:
                continue
            expected = sum(leaf_events.get(s, 0) for s in range(node["start"], node["end"]))
            assert node["events"] == expected, (
                f"level {node['level']} 节点 [{node['start']},{node['end']}) "
                f"事件数 {node['events']} != 叶子和 {expected}")
            checked += 1
        assert checked >= 1, "没有可校验的带统计节点"
    _serve(run, debug_enabled=True, preload=preload)


# ---- 5. 满载 10 分钟历史的耗时与响应体大小（慢测试，默认跳过）----

@pytest.mark.slow
def test_debug_perf_full_tree():
    """满载窗口（两维名单写满 512、线段树完整 10 分钟历史）下，
    测量单次 build_structure 与 HTTP /debug/structure 的耗时与响应体大小。"""

    def preload(engine, clock):
        _load_tree(engine, clock, 660, per_sec=1500, url_n=20000)

    async def run(client, engine, clock, app):
        # 独立复算叶子事件数之和，作为事件数正确性的对照
        leaf_total = sum(leaf.stats.dd.n for leaf in
                         engine.trees[Dim.URL].levels[0].values()
                         if leaf.stats is not None and leaf.stats.dd is not None)

        # 纯 build_structure + orjson 序列化（确定性部分）
        times = []
        last_body = b""
        for _ in range(21):
            t0 = time.perf_counter()
            data = build_structure(engine, Dim.URL)
            last_body = orjson.dumps(data)
            times.append((time.perf_counter() - t0) * 1000)
        med_pure = statistics.median(times)

        # 通过真实 HTTP 端点测单次调用
        http_times = []
        last_status = 0
        for _ in range(21):
            t0 = time.perf_counter()
            r = await _get(client, "/debug/structure?by=url")
            last_status = r.status
            body = await r.read()
            http_times.append((time.perf_counter() - t0) * 1000)
        med_http = statistics.median(http_times)

        print(f"[B1perf] nodes={_node_count(engine)} leaf_events={leaf_total}")
        print(f"[B1perf] build+serialize median={med_pure:.3f}ms body={len(last_body)}B")
        print(f"[B1perf] http median={med_http:.3f}ms status={last_status}")
        assert last_status == 200
        assert med_pure < 5.0, f"build_structure median {med_pure:.3f}ms over 5ms target"

    _serve(run, debug_enabled=True, preload=preload)


def _node_count(engine: Engine) -> int:
    n = 0
    for lvl in engine.trees[Dim.URL].levels:
        n += len(lvl)
    return n
