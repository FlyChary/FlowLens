"""T4 写入处理：decode_body、IngestService（按解压后字节预留/去重顺序/分块/释放）、Ticker。"""

import asyncio
import json
import zlib

import numpy as np
import pytest

import flowlens.ingest as ing_mod
from flowlens.config import Config
from flowlens.dims import Dim
from flowlens.engine import Engine
from flowlens.errors import BadRequest, Overloaded, PayloadTooLarge, UnsupportedEncoding
from flowlens.ingest import Admission, IngestService, MAX_DECOMPRESSED_BYTES, MAX_PENDING_BYTES
from flowlens.model import EventBatch
from flowlens.ticker import Ticker
from flowlens.timewin import FakeClock

NOW = 1_735_689_600_000


def _gzip(data: bytes) -> bytes:
    c = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    return c.compress(data) + c.flush()


def _body(evs: list) -> bytes:
    return json.dumps(evs).encode()


def _ev(i, ts=NOW - 100):
    return {"event_id": f"e{i}", "user_id": f"u{i % 10}", "url": f"/x{i % 5}",
            "ts": ts, "bytes": 10}


def _padded_body(total: int) -> bytes:
    """合法 JSON（[]）后用空白填充到恰好 total 字节（orjson 容忍顶层值后的空白）。"""
    core = b"[]"
    return core + b" " * (total - len(core))


def _svc(limit=None):
    eng = Engine(Config(), FakeClock(NOW))
    adm = Admission(limit) if limit is not None else Admission()
    return eng, IngestService(eng, adm)


# ---- decode_body ----

def test_decode_identity_and_empty():
    from flowlens.ingest import decode_body
    assert decode_body(b"payload", None) == b"payload"
    assert decode_body(b"payload", "identity") == b"payload"


def test_decode_gzip_and_case_insensitive():
    from flowlens.ingest import decode_body
    assert decode_body(_gzip(b"hello" * 100), "gzip") == b"hello" * 100
    assert decode_body(_gzip(b"abc"), "GZip") == b"abc"      # 大小写不敏感
    assert decode_body(_gzip(b"abc"), "GZIP") == b"abc"


def test_decode_unsupported_encoding():
    from flowlens.ingest import decode_body
    with pytest.raises(UnsupportedEncoding):
        decode_body(b"x", "br")
    with pytest.raises(UnsupportedEncoding):
        decode_body(b"x", "deflate")


def test_decode_body_too_large():
    from flowlens.ingest import decode_body, MAX_BODY_BYTES
    with pytest.raises(PayloadTooLarge):
        decode_body(b"\0" * (MAX_BODY_BYTES + 1), None)


def test_decode_exactly_4mb_gzip_ok():
    # 解压后恰好 4 MB 的 gzip 请求应被接受（解压后 == 上限）
    from flowlens.ingest import decode_body
    data = (b"x" * 1024) * 4096  # 恰好 4 MB
    assert len(data) == MAX_DECOMPRESSED_BYTES
    out = decode_body(_gzip(data), "gzip")
    assert len(out) == MAX_DECOMPRESSED_BYTES


def test_decode_gzip_over_limit():
    # 解压后超过 4 MB（这里 60 MB）必须抛 PayloadTooLarge，且过程不真分配 60 MB
    from flowlens.ingest import decode_body
    data = (b"\0" * 1024) * (60 * 1024)
    with pytest.raises(PayloadTooLarge):
        decode_body(_gzip(data), "gzip")


def test_decode_gzip_bomb():
    # 200MB 的零压缩后仅约 200KB；解压时必须抛 PayloadTooLarge，且过程中不真正分配 200MB
    from flowlens.ingest import decode_body
    bomb = _gzip(b"\0" * (200 * 1024 * 1024))
    assert len(bomb) < 1024 * 1024  # 压缩后确实很小
    with pytest.raises(PayloadTooLarge):
        decode_body(bomb, "gzip")


def test_decode_corrupt_gzip():
    from flowlens.ingest import decode_body
    with pytest.raises(BadRequest):
        decode_body(b"\x1f\x8b\x08\x00garbage-not-really-deflate", "gzip")


def test_decode_truncated_gzip():
    from flowlens.ingest import decode_body
    full = _gzip(b"hello world " * 1000)
    with pytest.raises(BadRequest):
        decode_body(full[: len(full) // 2], "gzip")


# ---- IngestService 计数与错误 ----

def test_ingest_empty_array():
    async def main():
        eng, svc = _svc()
        r = await svc.ingest(_body([]), None)
        assert (r.accepted, r.deduped, r.late, r.invalid) == (0, 0, 0, 0)
        assert svc.admission.pending == 0
    asyncio.run(main())


def test_ingest_normal_batch_counts():
    async def main():
        eng, svc = _svc()
        r = await svc.ingest(_body([_ev(i) for i in range(12000)]), None)  # 跨 3 个块
        assert r.accepted == 12000 and r.deduped == 0 and r.late == 0 and r.invalid == 0
        assert svc.admission.pending == 0  # 处理完已释放
    asyncio.run(main())


def test_ingest_gzip_body():
    async def main():
        eng, svc = _svc()
        r = await svc.ingest(_gzip(_body([_ev(i) for i in range(100)])), "gzip")
        assert r.accepted == 100
    asyncio.run(main())


def test_ingest_within_batch_dup():
    async def main():
        eng, svc = _svc()
        evs = [{"event_id": "a", "user_id": "u", "url": "/x", "ts": NOW - 100, "bytes": 1},
               {"event_id": "a", "user_id": "u", "url": "/x", "ts": NOW - 100, "bytes": 1}]
        r = await svc.ingest(_body(evs), None)
        assert r.accepted == 1 and r.deduped == 1
    asyncio.run(main())


def test_ingest_across_batch_dup():
    async def main():
        eng, svc = _svc()
        r1 = await svc.ingest(_body([_ev(1)]), None)
        r2 = await svc.ingest(_body([_ev(1)]), None)
        assert r1.accepted == 1 and r1.deduped == 0
        assert r2.accepted == 0 and r2.deduped == 1
    asyncio.run(main())


def test_ingest_late_and_future():
    async def main():
        eng, svc = _svc()
        late = {"event_id": "l", "user_id": "u", "url": "/x", "ts": NOW - 61_000, "bytes": 1}
        fut = {"event_id": "f", "user_id": "u", "url": "/x", "ts": NOW + 10_000, "bytes": 1}
        r = await svc.ingest(_body([late, fut]), None)
        assert r.late == 1 and r.invalid == 1 and r.accepted == 0
    asyncio.run(main())


def test_ingest_exactly_4mb_accepted():
    # 解压后恰好 4 MB 的请求应被接受
    async def main():
        eng, svc = _svc()
        r = await svc.ingest(_padded_body(MAX_DECOMPRESSED_BYTES), None)
        assert svc.admission.pending == 0  # 已释放
    asyncio.run(main())


def test_ingest_over_4mb_rejected():
    # 超过 4 MB 抛 PayloadTooLarge，且不应动在途预留
    async def main():
        eng, svc = _svc()
        with pytest.raises(PayloadTooLarge):
            await svc.ingest(_padded_body(MAX_DECOMPRESSED_BYTES + 1), None)
        assert svc.admission.pending == 0
    asyncio.run(main())


# ---- 关键：先按字节预留、再去重，拒绝的请求重试不被判重 ----

def test_overloaded_then_retry_not_deduped():
    async def main():
        eng, svc = _svc()
        # 在途字节占满
        assert svc.admission.try_reserve(MAX_PENDING_BYTES)
        assert svc.admission.occupancy == 1.0
        try:
            await svc.ingest(_body([_ev(1000)]), None)
            raise AssertionError("应抛 Overloaded")
        except Overloaded as e:
            assert e.retry_after == 1
        # 被拒的 e1000 不应写进 Bloom（预留失败在解析/去重之前）。释放后重试必须被正常接受
        svc.admission.release(MAX_PENDING_BYTES)
        r = await svc.ingest(_body([_ev(1000)]), None)
        assert r.accepted == 1 and r.deduped == 0
    asyncio.run(main())


# ---- 分块让出事件循环 ----

def test_chunking_yields_to_event_loop():
    async def main():
        eng, svc = _svc()
        loop = asyncio.get_running_loop()
        probe_moments = []  # 记录并发任务每次执行的时刻

        async def probe():
            while True:
                probe_moments.append(loop.time())
                await asyncio.sleep(0)

        probe_task = asyncio.create_task(probe())
        await svc.ingest(_body([_ev(i) for i in range(20000)]), None)  # 4 个块，块间让出
        ingest_end = loop.time()
        probe_task.cancel()
        try:
            await probe_task
        except asyncio.CancelledError:
            pass
        # 若 ingest 不分块让出，并发任务只能在请求"之后"才执行；分块让出则它在请求
        # 进行中就执行过——某次执行时刻早于请求结束，即证明处理期间事件循环被让出
        assert any(m < ingest_end for m in probe_moments)
    asyncio.run(main())


# ---- 异常时也会释放预留 ----

def test_exception_releases_reservation():
    async def main():
        eng, svc = _svc()
        calls = []
        orig = eng.ingest

        def flaky(batch):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("模拟某块处理失败")
            return orig(batch)

        eng.ingest = flaky
        with pytest.raises(RuntimeError):
            await svc.ingest(_body([_ev(i) for i in range(12000)]), None)  # 第 2 块抛异常
        assert svc.admission.pending == 0  # finally 里已释放
    asyncio.run(main())


# ---- 分块处理把已处理的 arr 元素置 None，尽早释放 ----

def test_arr_elements_released_after_processing():
    async def main():
        eng, svc = _svc()
        captured = {}
        real_load = ing_mod.load_array

        def spy_load(raw):
            arr = real_load(raw)
            captured["arr"] = arr
            return arr

        ing_mod.load_array = spy_load
        try:
            await svc.ingest(_body([_ev(i) for i in range(12000)]), None)
        finally:
            ing_mod.load_array = real_load
        arr = captured["arr"]
        assert len(arr) == 12000
        assert all(x is None for x in arr), "全部已处理元素应已置 None 以便释放"
    asyncio.run(main())


# ---- Ticker ----

def _fill_batch(n):
    return EventBatch(
        [f"e{i}" for i in range(n)], [f"u{i % 10}" for i in range(n)],
        [f"/x{i % 5}" for i in range(n)],
        np.array([NOW - 1000 + i for i in range(n)], dtype=np.int64),
        np.array([10] * n, dtype=np.int64), 0,
    )


def test_ticker_ticks_and_finalizes():
    async def main():
        clock = FakeClock(NOW)
        eng = Engine(Config(), clock)
        eng.ingest(_fill_batch(200))  # 数据在 [NOW-1s, NOW)
        ticker = Ticker(eng, period=0.05)
        task = asyncio.create_task(ticker.run())
        clock.advance(61_000)  # 让数据过 watermark 定稿
        await asyncio.sleep(0.2)
        assert ticker.tick_count >= 1
        assert eng.trees[Dim.URL].newest is not None  # 数据已定稿进树
        assert eng.snapshot.at_ms == clock.now_ms()   # 快照已刷新
        assert ticker.last_tick_duration_ms >= 0
        ticker.stop()
        await task  # stop 后干净结束
        assert not task.cancelled()
    asyncio.run(main())


def test_ticker_cancel_cleans_up():
    async def main():
        eng = Engine(Config(), FakeClock(NOW))
        ticker = Ticker(eng, period=0.05)
        task = asyncio.create_task(ticker.run())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(main())


def test_ticker_error_counts_and_continues():
    async def main():
        eng = Engine(Config(), FakeClock(NOW))
        calls = {"n": 0}
        orig = eng.advance

        def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("模拟某拍失败")
            return orig()

        eng.advance = flaky
        ticker = Ticker(eng, period=0.02)
        task = asyncio.create_task(ticker.run())
        await asyncio.sleep(0.1)
        ticker.stop()
        await task
        assert ticker.error_count == 1, "第一拍应计一次错误"
        assert ticker.tick_count >= 1, "错误后应继续拍"
        assert ticker.error_count < ticker.tick_count + 1  # 没有每拍都失败
    asyncio.run(main())


# ---- 异步快照与同步快照结果完全相同 ----

def test_refresh_snapshot_async_matches_sync():
    async def main():
        clock = FakeClock(NOW)
        eng = Engine(Config(), clock)
        eng.ingest(_fill_batch(500))
        s_sync = eng.refresh_snapshot()
        s_async = await eng.refresh_snapshot_async()
        assert s_sync == s_async  # frozen dataclass，逐字段相等
        assert s_sync.tops[Dim.URL] == s_async.tops[Dim.URL]
        assert s_sync.p95_bytes == s_async.p95_bytes
        assert s_sync.distinct_users == s_async.distinct_users
    asyncio.run(main())
