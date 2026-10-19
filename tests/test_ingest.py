"""T4 写入处理：decode_body、IngestService（按解压后字节预留/去重顺序/分块/释放）、Ticker。"""

import asyncio
import json
import os
import random
import sys
import zlib

import numpy as np
import pytest

import flowlens.ingest as ing_mod
from flowlens.config import Config
from flowlens.dims import Dim
from flowlens.engine import Engine, IngestResult
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


# ---- 在途上限的环境变量覆盖（C3 实验变量）----

def test_pending_limit_env_override(monkeypatch):
    """FLOWLENS_MAX_PENDING_BYTES 覆盖在途上限：合法值生效，非法/空退回默认。"""
    from flowlens.ingest import _read_pending_limit, DEFAULT_PENDING_BYTES
    monkeypatch.setenv("FLOWLENS_MAX_PENDING_BYTES", "2097152")  # 2 MB
    assert _read_pending_limit() == 2 * 1024 * 1024
    monkeypatch.setenv("FLOWLENS_MAX_PENDING_BYTES", "not-a-number")
    assert _read_pending_limit() == DEFAULT_PENDING_BYTES
    monkeypatch.setenv("FLOWLENS_MAX_PENDING_BYTES", "-5")
    assert _read_pending_limit() == DEFAULT_PENDING_BYTES
    monkeypatch.delenv("FLOWLENS_MAX_PENDING_BYTES")
    assert _read_pending_limit() == DEFAULT_PENDING_BYTES


def test_pooled_pending_limit_env_override(monkeypatch):
    """P1c 一.3：进程池开启时在途额度默认 8 MB；FLOWLENS_MAX_PENDING_BYTES 仍可覆盖。"""
    from flowlens.ingest import pooled_pending_limit, POOLED_PENDING_BYTES
    assert POOLED_PENDING_BYTES == 8 * 1024 * 1024  # 默认 8 MB
    monkeypatch.delenv("FLOWLENS_MAX_PENDING_BYTES", raising=False)
    assert pooled_pending_limit() == 8 * 1024 * 1024
    monkeypatch.setenv("FLOWLENS_MAX_PENDING_BYTES", "6291456")  # 6 MB
    assert pooled_pending_limit() == 6 * 1024 * 1024
    monkeypatch.setenv("FLOWLENS_MAX_PENDING_BYTES", "not-a-number")
    assert pooled_pending_limit() == 8 * 1024 * 1024
    monkeypatch.setenv("FLOWLENS_MAX_PENDING_BYTES", "-5")
    assert pooled_pending_limit() == 8 * 1024 * 1024
    monkeypatch.delenv("FLOWLENS_MAX_PENDING_BYTES")


def test_raw_budget_limit_env_override(monkeypatch):
    """FLOWLENS_RAW_BUDGET_BYTES 覆盖原始请求体额度上限：合法值生效，非法/空/非正退回默认 8 MB。"""
    from flowlens.api.app import _read_raw_budget_limit, DEFAULT_RAW_BUDGET_LIMIT
    assert DEFAULT_RAW_BUDGET_LIMIT == 8 * 1024 * 1024  # 默认 8 MB
    monkeypatch.setenv("FLOWLENS_RAW_BUDGET_BYTES", "16777216")  # 16 MB
    assert _read_raw_budget_limit() == 16 * 1024 * 1024
    monkeypatch.setenv("FLOWLENS_RAW_BUDGET_BYTES", "not-a-number")
    assert _read_raw_budget_limit() == DEFAULT_RAW_BUDGET_LIMIT
    monkeypatch.setenv("FLOWLENS_RAW_BUDGET_BYTES", "-5")
    assert _read_raw_budget_limit() == DEFAULT_RAW_BUDGET_LIMIT
    monkeypatch.delenv("FLOWLENS_RAW_BUDGET_BYTES")
    assert _read_raw_budget_limit() == DEFAULT_RAW_BUDGET_LIMIT


def test_admission_limit_2mb_rejects_3mb():
    """在途上限 2 MB 时，3 MB 请求预留不下（503 路径），2 MB 可预留。"""
    adm = Admission(limit=2 * 1024 * 1024)
    assert adm.can_reserve(3 * 1024 * 1024) is False
    assert adm.try_reserve(3 * 1024 * 1024) is False
    assert adm.pending == 0
    assert adm.try_reserve(2 * 1024 * 1024) is True
    assert adm.pending == 2 * 1024 * 1024


# ---- IngestService 计数与错误 ----

def test_ingest_empty_array():
    async def main():
        eng, svc = _svc()
        r = await svc.ingest(_body([]), None)
        assert (r.accepted, r.deduped, r.late, r.invalid) == (0, 0, 0, 0)
        assert svc.admission.pending == 0
    asyncio.run(main())


def test_ingest_all_invalid_chunk_counted():
    # F2 回归：整块全非法（validate 后 batch 为空）时 invalid 仍要计入。
    # 此前 `if batch:` 跳过 engine.ingest 会丢掉这块的 invalid 计数。
    async def main():
        eng, svc = _svc()
        # NDJSON 全非法：两行都不是事件对象（非 dict）
        r = await svc.ingest(b'"not-an-object"\n[1,2,3]\n', None)
        assert (r.accepted, r.deduped, r.late, r.invalid) == (0, 0, 0, 2)
        # 纯 JSON 数组全非对象：同样要计数（该路径与 NDJSON 无关，一并守护）
        r2 = await svc.ingest(b'[5, "str", null]', None)
        assert (r2.accepted, r2.invalid) == (0, 3)
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


# ---- P1：解析进程路径的等价性与正确性（决定一/五）----

def _hash_seed_fixed() -> bool:
    s = os.environ.get("PYTHONHASHSEED", "")
    return bool(s) and s != "random"


requires_seed = pytest.mark.skipif(
    not _hash_seed_fixed(), reason="真子进程用例需要固定 PYTHONHASHSEED")


def _body_rand(n: int, seed: int = 0) -> bytes:
    """长尾 + query 不同的 body（跨块、含归一化路径）。"""
    r = random.Random(seed)
    evs = [{"event_id": f"e{seed * 100000 + i}", "user_id": f"u{r.randrange(50)}",
            "url": f"/api/item/x{r.randrange(30)}?q={i}",
            "ts": NOW - 1000 - (i % 900), "bytes": 10 + (i % 40)} for i in range(n)]
    return json.dumps(evs).encode()


def _snap_sig(eng) -> tuple:
    s = eng.refresh_snapshot()
    return (s.tops, s.distinct_users, s.p95_bytes)


def _cnt_sig(eng) -> tuple:
    c = eng.counters
    return (c.events_ingested, c.deduped, c.late, c.invalid)


def test_ingest_parsed_equals_ingest():
    """P1 §3.1 核心（不依赖子进程）：同一 body，IngestService 单进程路径 vs 进程池路径
    的"已解析结果"，写两个全新引擎，topk(url/user)/stats/计数器逐项相等。

    这里直接用 IngestService 的两条路径（in-proc 与 ParsedBatch），等价于"同一请求体
    分别走进程池与单进程"（解析部分逻辑一致，只是 in-proc 用 validate、pooled 用
    parse_body——两者产出同构，见 test_parse_pool）。覆盖长尾 / 全随机 / 含非法 / 批内重复。
    """
    from flowlens.parse_pool import parse_body
    from flowlens.ingest import decode_body as _decode
    # 本用例验证的是"写入核心"：同一解析结果走 in-proc（validate→ingest）与进程池
    # （parse_body→ingest_parsed）是否等价。两条路径**用同一块大小切块**——in-proc
    # 内部用 CHUNK_EVENTS（2000），这里手动切块也取它。之所以不取 POOLED_CHUNK_EVENTS
    # （10000）：Top-K 名单是近似结构、堆成员对"逐秒 observe 的分批顺序"敏感，块大小
    # 不同会在高基数流量的名单深尾部产生少量差异（sketch 固有性质，非 bug），那会污染
    # 本用例想验证的写入核心等价性。块大小是性能旋钮、不是写入核心的正确性不变量。
    CE = ing_mod.CHUNK_EVENTS

    def run(body):
        e1 = Engine(Config(), FakeClock(NOW))
        s1 = IngestService(e1, Admission())
        e2 = Engine(Config(), FakeClock(NOW))
        # 进程池路径 = 本进程 parse_body + ingest_parsed（与子进程 parse_body 同一函数）
        parsed = parse_body(_decode(body, None))

        async def both():
            r1 = await s1.ingest(body, None)
            # 进程池写入核心口径：ingest_parsed + 与 in-proc 相同的块大小切块，整批
            # invalid 放首块，让引擎计数器也计到
            chunks = [parsed.chunk(i, i + CE) for i in range(0, len(parsed), CE)]
            if not chunks:
                chunks = [parsed.chunk(0, 0)]
            chunks[0].invalid = parsed.invalid
            total = IngestResult(0, 0, 0, 0)
            for ch in chunks:
                total = IngestService._add(total, e2.ingest_parsed(ch))
            return r1, total
        r1, r2 = asyncio.run(both())
        assert r1 == r2, f"计数不一致 {r1} vs {r2}"
        assert _snap_sig(e1) == _snap_sig(e2)
        assert _cnt_sig(e1) == _cnt_sig(e2)

    run(_body_rand(6000))                       # 长尾，跨 3 个块
    r = random.Random(3)                        # 全随机不重复
    rnd = [{"event_id": f"r{r.getrandbits(60)}", "user_id": f"ru{r.getrandbits(20)}",
            "url": f"/r/{r.getrandbits(30)}", "ts": NOW - 1000, "bytes": 100} for _ in range(4000)]
    run(json.dumps(rnd).encode())
    bad = json.loads(_body_rand(3000))          # 含非法 + 批内重复
    bad[7] = {"user_id": "x", "url": "/x", "ts": NOW - 100, "bytes": 1}   # 缺 event_id
    bad[8] = dict(bad[0])
    run(json.dumps(bad).encode())
    # 全非法
    run(json.dumps([5, "str", None]).encode())


def test_ingest_pooled_503_then_retry_not_deduped():
    """P1 §3.2 / 规则 2：进程池路径下在途额度满 → 503（预留在池提交之前），释放后重试不判重。"""
    eng = Engine(Config(), FakeClock(NOW))
    adm = Admission()
    svc = IngestService(eng, adm, parse_pool=_FakePool())
    body = _body_rand(100)

    async def main():
        assert adm.try_reserve(MAX_PENDING_BYTES)
        try:
            await svc.ingest(body, None)
            raise AssertionError("应抛 Overloaded")
        except Overloaded as e:
            assert e.retry_after == 1
        # 被拒的 e* 不应写进 Bloom（预留在解析与池提交之前）。释放后重试必须被正常接受
        adm.release(MAX_PENDING_BYTES)
        r = await svc.ingest(body, None)
        assert r.accepted == 100 and r.deduped == 0
    asyncio.run(main())


def _FakePool():
    """一个恒返回单进程 parse 结果的"进程池"替身（不真起子进程），让 svc 走 _ingest_pooled。"""
    from flowlens.parse_pool import parse_body

    class _FakePool:
        available = True
        num_workers = 1

        def __init__(self):
            self.rebuilt = False

        def submit(self, raw):
            import concurrent.futures as cf
            f = cf.Future()
            f.set_result(parse_body(raw))
            return f

        def rebuild(self):
            self.rebuilt = True

        def close(self):
            pass

    return _FakePool()


def test_pool_path_full_equivalence(monkeypatch):
    """P1 §3.1（经 IngestService 的完整路径）：同一请求体分别走进程池路径（替身池，走
    decode_body→parse_body→ingest_parsed→POOLED_CHUNK_EVENTS 切块→invalid 放首块）与单进程
    路径（decode_body→validate→ingest），写两个全新引擎，topk(url/user)/stats/计数器逐项相等。
    覆盖 P1 点名的长尾、gzip、NDJSON、全随机。

    两条路径**用同一块大小切块**才能逐项相等：进程池路径用 POOLED_CHUNK_EVENTS（10000），
    单进程路径用 CHUNK_EVENTS（默认 2000）。块大小不同会在高基数流量的 Top-K 名单深尾部
    产生差异（TopK 堆成员对"逐秒 observe 分批顺序"敏感，sketch 固有性质、非 bug）。本用例
    验证的是"路径"（in-proc vs 进程池）等价，故临时把 CHUNK_EVENTS 对齐到
    POOLED_CHUNK_EVENTS，把块大小这个性能旋钮固定住、只留路径变量。
    """
    monkeypatch.setattr(ing_mod, "CHUNK_EVENTS", ing_mod.POOLED_CHUNK_EVENTS)
    eng, svc = _svc()
    svc._pool = _FakePool()
    svc._parse_inproc = False          # 强制走 _ingest_pooled（替身池）
    eng2 = Engine(Config(), FakeClock(NOW))
    s2 = IngestService(eng2, Admission())  # 单进程路径

    async def run(body, enc):
        r_pooled = await svc.ingest(body, enc)
        r_inproc = await s2.ingest(body, enc)
        assert r_pooled == r_inproc
        assert _snap_sig(eng) == _snap_sig(eng2)
        assert _cnt_sig(eng) == _cnt_sig(eng2)

    async def main():
        plain = _body_rand(6000)
        await run(plain, None)                                   # 长尾，跨 3 个块
        await run(_gzip(plain), "gzip")                          # gzip（解压后等价）
        # NDJSON（逐行）
        evs = json.loads(plain)
        ndjson = b"\n".join(json.dumps(e).encode() for e in evs)
        await run(ndjson, None)
        r = random.Random(7)                                     # 全随机不重复
        rnd = [{"event_id": f"r{r.getrandbits(60)}", "user_id": f"ru{r.getrandbits(20)}",
                "url": f"/r/{r.getrandbits(30)}", "ts": NOW - 1000, "bytes": 100} for _ in range(4000)]
        await run(json.dumps(rnd).encode(), None)
    asyncio.run(main())


def test_engine_write_semaphore_serializes():
    """决定五：写入引擎受 Semaphore(1) 保护——两个并发请求的"写引擎"区间不重叠。"""
    eng = Engine(Config(), FakeClock(NOW))
    svc = IngestService(eng, Admission())
    overlaps = {"n": 0}
    active = {"n": 0}
    orig = eng.ingest

    def spy(batch, *a, **k):
        active["n"] += 1
        if active["n"] > 1:
            overlaps["n"] += 1
        try:
            return orig(batch, *a, **k)
        finally:
            active["n"] -= 1

    eng.ingest = spy

    async def main():
        body = _body_rand(3000)
        await asyncio.gather(svc.ingest(body, None), svc.ingest(body, None))
    asyncio.run(main())
    assert overlaps["n"] == 0, "Semaphore(1) 应串行化写引擎，两请求写区间不重叠"


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


# ---- 定期 malloc_trim（C3）----

def test_malloc_trim_non_linux_no_error(monkeypatch):
    """非 Linux（或找不到 libc）时 malloc_trim() 什么都不做、不报错。

    Windows 上真实路径即非 Linux；Linux 上则把 _LIBC 置成加载失败来模拟。
    两种情况下都应返回 False、不抛异常。"""
    import flowlens.ticker as tk
    if os.name == "posix" and not sys.platform.startswith("win"):
        # Linux：模拟 libc 加载失败（置 _LIBC=False，让 _load_libc 返回 None）
        monkeypatch.setattr(tk, "_LIBC", False)
    else:
        # Windows：os.name != "posix" 直接短路，无需改 _LIBC
        pass
    assert tk.malloc_trim() is False


def test_malloc_trim_linux_calls_libc(monkeypatch):
    """Linux：malloc_trim 成功时返回 True，且确实调用了 libc.malloc_trim(0)。"""
    if not (os.name == "posix" and not sys.platform.startswith("win")):
        pytest.skip("仅 Linux 有 libc.malloc_trim")
    import flowlens.ticker as tk
    calls = []

    class _FakeLib:
        def malloc_trim(self, arg):
            calls.append(arg)
            return 1

    monkeypatch.setattr(tk, "_LIBC", _FakeLib())
    assert tk.malloc_trim() is True
    assert calls == [0]


def test_malloc_trim_no_symbol(monkeypatch):
    """加载到的 libc 没有 malloc_trim 符号（如 musl）时返回 False、不报错。"""
    import flowlens.ticker as tk
    monkeypatch.setattr(tk, "_LIBC", object())  # 没有 malloc_trim 属性
    assert tk.malloc_trim() is False


def test_ticker_trim_disabled_on_windows():
    """Windows 上 trim.enabled 恒为 False（平台不支持 malloc_trim）。"""
    if os.name == "posix" and not sys.platform.startswith("win"):
        pytest.skip("仅 Windows 上验证 trim 被平台关闭")
    eng = Engine(Config(), FakeClock(NOW))
    ticker = Ticker(eng, period=1.0, trim_period=10.0)
    assert ticker.trim.enabled is False


def test_ticker_trim_respects_period(monkeypatch):
    """trim_period 之内不重复触发：第一拍设基准、未到点跳过、到点后触发一次。

    直接驱动 _maybe_trim（墙钟参数），不真等 10 秒；强制 enabled 让非 Linux
    平台也走触发逻辑，打桩 malloc_trim 与 _rss_anon_bytes 记录调用。"""
    import flowlens.ticker as tk
    eng = Engine(Config(), FakeClock(NOW))
    ticker = Ticker(eng, period=1.0, trim_period=10.0)
    monkeypatch.setattr(ticker.trim, "enabled", True)
    calls = {"n": 0}
    monkeypatch.setattr(tk, "malloc_trim", lambda: calls.__setitem__("n", calls["n"] + 1) or True)
    monkeypatch.setattr(tk, "_rss_anon_bytes", lambda: 0)

    ticker._maybe_trim(0.0)       # 首次：设基准，不 trim
    assert calls["n"] == 0
    ticker._maybe_trim(5.0)       # 5s < 10s，未到期
    assert calls["n"] == 0
    ticker._maybe_trim(11.0)      # 11s ≥ 10s，到期 → 触发一次
    assert calls["n"] == 1
    ticker._maybe_trim(12.0)      # 距上次(11)仅 1s < 10s，不重复
    assert calls["n"] == 1
    assert ticker.trim.call_count == 1
    assert ticker.trim.total_ms >= 0.0
