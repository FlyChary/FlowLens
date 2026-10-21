"""T8 集成测试：用可注入（pin）的降级控制器固定级别，验证各级动作。

- 第 1 级：回放查询 503 + Retry-After，默认窗口查询照常；SSE 重算间隔变为 5 秒、帧照常推。
- 第 2 级：带权采样生效（Top-K 估计围绕真实值）；HLL 全量（去重人数与不采样时完全相同）；
  同一批事件在两个独立引擎里采样取舍一致；响应带 degraded=true 与 sample_rate。
- 第 3 级：Admission/RawBudget 上限降低、退出后恢复；新 SSE 订阅 503、已有订阅保持；两缓存清空。
- 第 4 级：写入 503（读体之前），之后同批重试被正常接受、不判重。
- level 0 时全部现有测试原样通过（另见 test_api / test_stream / test_engine）。

控制器 pin() 到某一级后，因 app 的 Ticker 与 engine/query/stream 共享同一个控制器实例
（create_app 装配），采样、暂停回放、收紧预算、拒写会同时按该级生效。
"""

import asyncio
import json

from aiohttp.test_utils import TestClient, TestServer

from flowlens.api.app import (create_app, degrade_controller_key,
                              ingest_service_key, query_service_key,
                              raw_budget_key, stream_manager_key, ticker_key)
from flowlens.config import Config
from flowlens.dims import Dim
from flowlens.engine import Engine
from flowlens.ingest import Admission, IngestService
import flowlens.model as model
from flowlens.query import QueryService
from flowlens.timewin import FakeClock

NOW = 1_735_689_600_000
MB = 1024 * 1024
# 与生产/e2e 一致（见 docs/PROGRESS.md 决策）
LINGERING_TIME = 1


def _strict_json(text: str):
    def _reject(const):
        raise ValueError(f"响应里出现非标准 JSON 常量: {const}")
    return json.loads(text, parse_constant=_reject)


def _fake_memory_reader() -> tuple[int, int | None]:
    """测试用假内存读数（T8b）：恒 (0, None)，与真实内存解耦（见 test_api.py 同名函数）。"""
    return 0, None


class MemFeed:
    """可控制内存读数的假 reader（T8b）：测试随时改 .mem / .limit，Ticker 每拍读到当前值。

    用来端到端验证"内存读数 → Ticker → 降级控制器"这条链路：先喂低值，再喂超过
    阈值的值，观察第 3、4 级的进入与退出（Windows 上 memstat 读不到真实内存，这条
    链路本地一次都没真正跑过，见 docs/instructions/T8b.md 背景）。
    """

    def __init__(self, mem: int = 0, limit: int | None = None):
        self.mem = mem
        self.limit = limit
        self.calls = 0

    def __call__(self) -> tuple[int, int | None]:
        self.calls += 1
        return self.mem, self.limit


def _serve(run, tick_period: float = 1000.0,
           memory_reader=None):
    async def main():
        clock = FakeClock(NOW)
        engine = Engine(Config(), clock)
        app = create_app(engine, IngestService(engine, Admission()),
                         QueryService(engine), tick_period=tick_period,
                         memory_reader=memory_reader if memory_reader is not None
                         else _fake_memory_reader)
        server = TestServer(app)
        client = TestClient(server)
        await server.start_server(auto_decompress=False, lingering_time=LINGERING_TIME)
        try:
            await run(client, engine, clock, app)
        finally:
            await client.close()
    asyncio.run(main())


async def _post(client, url, *a, **kw):
    from aiohttp.client_exceptions import ClientOSError
    for _ in range(3):
        try:
            return await client.post(url, *a, **kw)
        except ClientOSError:
            await asyncio.sleep(0.05)
    raise RuntimeError("post 连接被重置")


async def _get(client, url, *a, **kw):
    from aiohttp.client_exceptions import ClientOSError
    for _ in range(3):
        try:
            return await client.get(url, *a, **kw)
        except ClientOSError:
            await asyncio.sleep(0.05)
    raise RuntimeError("get 连接被重置")


# ---- 第 1 级：暂停回放（HTTP），SSE 降频不暂停 ----

def test_level1_pause_replay_and_slow_sse():
    async def run(client, engine, clock, app):
        ctrl = app[degrade_controller_key]
        ctrl.pin(1)
        try:
            # 写入数据并刷新快照（默认窗口查询读快照）
            evs = [{"event_id": f"e{i}", "user_id": f"u{i % 10}", "url": f"/x{i % 5}",
                    "ts": NOW - 1000, "bytes": 100} for i in range(50)]
            r = await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
            assert r.status == 202
            engine.refresh_snapshot()

            # 默认窗口查询（无 at、60s、k<=100）：读快照，照常 200
            r = await _get(client, "/v1/topk", params={"window": "60s", "k": "10", "by": "url"})
            assert r.status == 200, (r.status, await r.text())
            # 带 at 的回放查询：503 + Retry-After
            r = await _get(client, "/v1/topk",
                           params={"window": "60s", "k": "10", "by": "url", "at": str(NOW - 60_000)})
            assert r.status == 503
            assert r.headers.get("Retry-After") == "1"
            # window 不是 60s 的回放查询：503
            r = await _get(client, "/v1/topk", params={"window": "120s", "k": "10", "by": "url"})
            assert r.status == 503 and r.headers.get("Retry-After") == "1"
            # k 超过快照名单长度（100）：也走回放 → 503
            r = await _get(client, "/v1/topk", params={"window": "60s", "k": "100", "by": "url"})
            assert r.status == 200  # k=100 恰好等于 snapshot_top_n，仍读快照
            # 非 60s 的 stats：503；60s 的 stats：200
            r = await _get(client, "/v1/stats", params={"window": "120s"})
            assert r.status == 503 and r.headers.get("Retry-After") == "1"
            r = await _get(client, "/v1/stats", params={"window": "60s"})
            assert r.status == 200

            # SSE 降频：订阅成功（第 1 级不拒订），组的最小重算间隔变为 5 秒
            mgr = app[stream_manager_key]
            resp = await client.get("/v1/topk/stream?window=60s&k=10&by=url&interval=1s")
            assert resp.status == 200
            try:
                group = next(iter(mgr.groups.values()))
                assert group.recompute_interval_ms() == 5000.0
                # SSE 不暂停：仍能收到数据帧
                while True:
                    raw = await asyncio.wait_for(resp.content.readuntil(b"\n\n"), timeout=20)
                    text = raw.decode()
                    if "data:" in text:
                        frame = _strict_json(text.split("data:", 1)[1].strip())
                        assert frame["degraded"] is True
                        break
            finally:
                resp.close()
        finally:
            ctrl.pin(None)
    _serve(run)


# ---- 第 2 级：带权采样 ----

def _l2_batch():
    """100 个 URL 各 100 次、50 个用户，全部落在窗口内同一秒附近。"""
    nkeys, per = 100, 100
    sec0 = (NOW // 1000 - 2) * 1000
    return [{"event_id": f"e{i}", "user_id": f"u{i % 50}", "url": f"/x{i % nkeys}",
             "ts": sec0 + (i % 900), "bytes": 100} for i in range(nkeys * per)]


def test_level2_sampling_hll_full_and_response_fields():
    """第 2 级：Top-K 走采样、响应带 degraded 与 sample_rate；HLL 全量（去重人数与不采样相同）。"""
    from flowlens.model import parse_events

    async def run(client, engine, clock, app):
        ctrl = app[degrade_controller_key]
        ctrl.pin(2)  # 第 2 级，sample_weight=2（占用 0，不到 0.95 的 4）
        try:
            assert ctrl.sample_weight == 2
            evs = _l2_batch()
            body = json.dumps(evs).encode()
            await _post(client, "/v1/ingest", data=body)
            engine.refresh_snapshot()

            # 响应带 degraded=true 与 sample_rate=0.5
            r = await _get(client, "/v1/topk", params={"window": "60s", "k": "10", "by": "url"})
            assert r.status == 200
            t = _strict_json(await r.text())
            assert t["degraded"] is True
            assert abs(t["sample_rate"] - 0.5) < 1e-9

            # 不采样的对照引擎（weight=1，默认控制器不采样）
            eng_b = Engine(Config(), FakeClock(NOW))
            eng_b.ingest(parse_events(body))
            eng_b.refresh_snapshot()

            # HLL 全量：采样引擎与对照引擎的去重人数估计完全相同
            assert engine.snapshot.distinct_users == eng_b.snapshot.distinct_users
            # 且确实用到了全部 50 个用户（不是采样后的子集）
            assert engine.snapshot.distinct_users >= 40

            # SSE 帧同样带 degraded 与 sample_rate（采样生效时）
            resp = await client.get("/v1/topk/stream?window=60s&k=10&by=url&interval=1s")
            assert resp.status == 200
            try:
                while True:
                    raw = await asyncio.wait_for(resp.content.readuntil(b"\n\n"), timeout=20)
                    text = raw.decode()
                    if "data:" in text:
                        frame = _strict_json(text.split("data:", 1)[1].strip())
                        assert frame["degraded"] is True
                        assert abs(frame["sample_rate"] - 0.5) < 1e-9
                        break
            finally:
                resp.close()
        finally:
            ctrl.pin(None)
    _serve(run)


def test_level2_sampling_unbiased_and_deterministic():
    """Top-K 加权估计围绕真实值；同批事件在两个独立引擎里采样取舍一致（与到达时间无关）。"""
    from flowlens.model import parse_events
    from flowlens.bounds import for_keys
    from flowlens.hashing import key_hashes
    from flowlens.timewin import split_range
    import collections

    evs = _l2_batch()
    batch = parse_events(json.dumps(evs).encode())
    assert len(batch) == 10000

    # 引擎 A：采样（weight=2）
    eng_a = Engine(Config(), FakeClock(NOW))
    eng_a.controller.pin(2)
    eng_a.ingest(batch)
    parts_a = eng_a.collect_parts(Dim.URL, split_range(NOW - 60_000, NOW))
    exact = collections.Counter(batch.urls)  # 每 URL 100 次
    assert all(c == 100 for c in exact.values())
    lo, hi = for_keys(parts_a, key_hashes(list(exact)))
    for k, l, h in zip(exact, lo, hi):
        c = exact[k]  # = 100
        # 加权次数围绕真实次数 c（无偏）；采样方差 + CMS 高估 → 宽松区间
        assert 0.7 * c <= h <= 1.3 * c, f"{k}: 加权估计 {h} 偏离真实 {c}"

    # 引擎 A2：独立、同样采样、同批事件 → 取舍一致（确定性，与到达时间无关）
    eng_a2 = Engine(Config(), FakeClock(NOW))
    eng_a2.controller.pin(2)
    eng_a2.ingest(batch)
    eng_a.refresh_snapshot()
    eng_a2.refresh_snapshot()
    ta = [(it.key, it.count) for it in eng_a.snapshot.tops[Dim.URL][:10]]
    ta2 = [(it.key, it.count) for it in eng_a2.snapshot.tops[Dim.URL][:10]]
    assert ta == ta2, f"两个独立采样引擎结果不一致: {ta} vs {ta2}"


# ---- 第 3 级：收紧内存、拒绝新订阅、清缓存 ----

def test_level3_tighten_budgets_and_caches():
    async def run(client, engine, clock, app):
        ctrl = app[degrade_controller_key]
        raw_budget = app[raw_budget_key]
        admission = app[ingest_service_key].admission
        qs = app[query_service_key]

        # 预先填充两个缓存
        model.normalize_url("http://example.com/a//b?x=1")  # 填 URL 归一化缓存
        assert len(model._norm_cache) > 0
        # 填查询结果缓存：一个已定稿的历史区间（final=True 才进缓存）
        evs = [{"event_id": f"e{i}", "user_id": "u", "url": "/x", "ts": NOW - 1000, "bytes": 10}
               for i in range(20)]
        await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
        clock.advance(90_000)  # 让 [NOW-60s, NOW) 定稿
        engine.refresh_snapshot()
        await _get(client, "/v1/topk",
                   params={"window": "60s", "k": "10", "by": "url", "at": str(NOW)})
        assert len(qs._cache) > 0

        # 进入第 3 级
        ctrl.pin(3)
        # 预算收紧
        assert admission.limit == 2 * MB
        assert raw_budget.limit == 4 * MB
        # 两个缓存被清空（只在进入那一刻清一次）
        assert len(model._norm_cache) == 0
        assert len(qs._cache) == 0

        # 先（第 3 级时）尝试新订阅 → 被拒
        mgr = app[stream_manager_key]
        stream_url = "/v1/topk/stream?window=60s&k=10&by=url&interval=1s"
        resp_rej = await client.get(stream_url)
        assert resp_rej.status == 503
        assert resp_rej.headers.get("Retry-After") == "1"
        resp_rej.close()
        assert mgr.subscriber_count() == 0

        # 退到第 2 级（不拒订）：建立"已有订阅"
        ctrl.pin(2)
        resp_old = await client.get(stream_url)
        assert resp_old.status == 200
        assert mgr.subscriber_count() == 1

        # 再回到第 3 级：已有订阅保持（仍 1 个），新订阅仍被拒
        ctrl.pin(3)
        assert mgr.subscriber_count() == 1  # 已有订阅保持
        resp_new = await client.get(stream_url)
        assert resp_new.status == 503
        assert mgr.subscriber_count() == 1  # 新订阅没进去
        resp_new.close()

        # 退出第 3 级：预算恢复（缓存不清、重新填充）
        ctrl.pin(0)
        assert admission.limit == 4 * MB
        assert raw_budget.limit == 8 * MB
        resp_old.close()
    _serve(run)


# ---- 第 4 级：拒写在读体之前，重试不判重 ----

def test_level4_reject_ingest_before_body_and_retry_ok():
    async def run(client, engine, clock, app):
        ctrl = app[degrade_controller_key]
        ctrl.pin(4)
        try:
            evs = [{"event_id": f"e{i}", "user_id": f"u{i % 5}", "url": "/x",
                    "ts": NOW - 1000, "bytes": 100} for i in range(30)]
            body = json.dumps(evs).encode()
            r = await _post(client, "/v1/ingest", data=body)
            assert r.status == 503
            assert r.headers.get("Retry-After") == "1"
            # 被拒时未写入任何去重状态：事件计数不变
            assert engine.counters.events_ingested == 0
            assert engine.counters.deduped == 0
            # 退出降级后原样重试：正常接受、不判重
            ctrl.pin(0)
            r = await _post(client, "/v1/ingest", data=body)
            assert r.status == 202
            out = await r.json()
            assert out["accepted"] == 30 and out["deduped"] == 0
        finally:
            ctrl.pin(None)
    _serve(run)


# ---- 内存读数驱动：假 reader 控制读数，走 Ticker → 控制器的真实链路 ----
# （不用 pin：验证的是"读数变化如何推动级别"，而不是固定级别后的动作）

def test_memory_reader_drives_levels():
    """内存读数超过/回落阈值时，Ticker 每拍喂给控制器，级别相应进入/退出。

    链路：memory_reader() → Ticker._update_degrade → controller.update → 级别。
    先喂 0.8×limit（第 3 级）、再喂 0.95×limit（第 4 级）、再喂 0（退出），
    每一档都验证副作用（预算收紧/恢复、拒写）与 /healthz 的 degrade 字段。
    """
    feed = MemFeed(mem=0, limit=256 * MB)

    async def run(client, engine, clock, app):
        ctrl = app[degrade_controller_key]
        raw_budget = app[raw_budget_key]
        admission = app[ingest_service_key].admission
        ticker = app[ticker_key]

        # Ticker 确实在跑（周期 0.1s）：等到至少一拍
        for _ in range(50):
            if ticker.tick_count >= 1:
                break
            await asyncio.sleep(0.02)
        assert ticker.tick_count >= 1
        # 初始：内存 0 → 不降级
        assert ctrl.level == 0

        # 第 3 级：内存升到 0.8 × 256MB（> 0.78）
        feed.mem = int(0.8 * 256 * MB)
        for _ in range(50):
            if ctrl.level >= 3:
                break
            await asyncio.sleep(0.02)
        assert ctrl.level == 3, ctrl.level
        assert admission.limit == 2 * MB and raw_budget.limit == 4 * MB
        assert ctrl.tighten_budgets
        assert not ctrl.reject_ingest

        # 第 4 级：内存再升到 0.95 × 256MB（> 0.90）
        feed.mem = int(0.95 * 256 * MB)
        for _ in range(50):
            if ctrl.level >= 4:
                break
            await asyncio.sleep(0.02)
        assert ctrl.level == 4, ctrl.level
        assert ctrl.reject_ingest
        # 第 4 级拒写：读体之前 503
        evs = [{"event_id": "e0", "user_id": "u", "url": "/x",
                "ts": NOW - 1000, "bytes": 100}]
        r = await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
        assert r.status == 503
        assert r.headers.get("Retry-After") == "1"

        # 退出：内存回落到 0 → 全部退出
        feed.mem = 0
        for _ in range(50):
            if ctrl.level == 0:
                break
            await asyncio.sleep(0.02)
        assert ctrl.level == 0, ctrl.level
        assert admission.limit == 4 * MB and raw_budget.limit == 8 * MB
        # 退出后写入恢复
        r = await _post(client, "/v1/ingest", data=json.dumps(evs).encode())
        assert r.status == 202
        # 内存读数确实来自 feed（被 Ticker 调过、/healthz 与 degrade 字段一致）
        assert feed.calls > 0
        r = await _get(client, "/healthz")
        hz = await r.json()
        assert hz["degrade"]["memory_bytes"] == 0
        assert hz["memory_bytes"] == 0  # 来自 memory_reader，不是 memstat 真实值
    _serve(run, tick_period=0.1, memory_reader=feed)


def test_entering_level3_calls_malloc_trim_once(monkeypatch):
    """P1c 一.4（修"降级棘轮"）：进入第 3 级的那一刻，on_change 回调里立即调一次
    malloc_trim 归还 glibc 碎片（第 4 级拒写不降内存，负载不撤就退不出降级）。

    用可控内存读数让控制器进入第 3 级；monkeypatch app 模块里的 malloc_trim
    计数（生产回调引用的是模块全局名，patch 模块属性即可命中）。验证：
    进入第 3 级时调用恰好一次、级别不变不重复调、退出后再进再调一次。
    """
    import flowlens.api.app as app_mod
    calls = []
    monkeypatch.setattr(app_mod, "malloc_trim", lambda: calls.append(1) or True)

    feed = MemFeed(mem=0, limit=256 * MB)

    async def run(client, engine, clock, app):
        ctrl = app[degrade_controller_key]
        ticker = app[ticker_key]
        # Ticker 在跑（周期 0.1s）
        for _ in range(50):
            if ticker.tick_count >= 1:
                break
            await asyncio.sleep(0.02)

        # 进入第 3 级（0.8 × 256MB > 0.78）
        feed.mem = int(0.8 * 256 * MB)
        for _ in range(50):
            if ctrl.level >= 3:
                break
            await asyncio.sleep(0.02)
        assert ctrl.level == 3, ctrl.level
        # 等一拍，确认"只在进入那一刻调一次"（后续拍级别不变、不再调）
        await asyncio.sleep(0.3)
        assert len(calls) == 1, f"进入第 3 级应恰好 trim 一次，实际 {len(calls)}"

        # 退出（内存回落 0）→ 再进入：再调一次
        feed.mem = 0
        for _ in range(50):
            if ctrl.level == 0:
                break
            await asyncio.sleep(0.02)
        assert ctrl.level == 0
        feed.mem = int(0.8 * 256 * MB)
        for _ in range(50):
            if ctrl.level >= 3:
                break
            await asyncio.sleep(0.02)
        assert ctrl.level == 3
        await asyncio.sleep(0.3)
        assert len(calls) == 2, f"再次进入应再 trim 一次，实际 {len(calls)}"
    _serve(run, tick_period=0.1, memory_reader=feed)
