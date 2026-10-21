"""P2d-1 三：写入通道消息格式（分帧、编解码往返、空批）。

分帧用伪造的 StreamReader（把一条消息的字节拆成多段喂进去）；编解码用真实
parse_body 产出的 ParsedBatch（长尾大量重复 + 高基数 + 空批）。
"""

import asyncio
import pickle
import random

import numpy as np
import orjson
import pytest

from flowlens.intern import Intern
from flowlens.ipc import (_HDR, MSG_ACK, MSG_BATCH, WireBatch, _dedupe,
                          decode_to_parsed, encode_parsed, read_msg, send_msg)
from flowlens.parse_pool import parse_body

NOW = 1_735_689_600_000


def _longtail_body(n: int, seed: int, nkeys: int = 30) -> bytes:
    """长尾：少量 URL 大量重复（归一化后去重效果明显）。"""
    r = random.Random(seed)
    evs = [{"event_id": f"e{seed}-{i}", "user_id": f"u{r.randrange(nkeys // 2)}",
            "url": f"/api/item/{r.randrange(nkeys)}?q={i}",
            "ts": NOW - 1000 - (i % 900), "bytes": 10 + (i % 40)} for i in range(n)]
    return orjson.dumps(evs)


def _random_body(n: int, seed: int) -> bytes:
    """高基数：每个 URL 一个不同的 40 位十进制 ID 段（>16 位数字，归一化成 {id}）。

    注意：纯随机十六进制会被 normalize_url 归一成 {id} 而全部坍缩成 1 个，故用十进制
    大数字（同样触发 {id} 替换，但保留"每个 URL 不同"的意图——这里验证的是去重/展开
    逻辑，归一化坍缩反而是高基数的极端情形，单独一条覆盖即可）。
    """
    r = random.Random(seed)
    evs = [{"event_id": f"r{r.getrandbits(50)}",
            "user_id": f"ru{r.getrandbits(40)}",
            "url": f"/item/{r.getrandbits(130)}",
            "ts": NOW - 1000, "bytes": 5} for _ in range(n)]
    return orjson.dumps(evs)


# ---- 分帧：一条消息的字节拆成多段、分多次喂给 readexactly，能还原 ----

class _ChunkedReader:
    """按预切好的段逐次返回的 readexactly 替身（模拟字节乱序/分段到达）。"""

    def __init__(self, chunks: list[bytes]):
        self._buf = bytearray()
        self._chunks = list(chunks)

    async def readexactly(self, n: int) -> bytes:
        while len(self._buf) < n:
            if not self._chunks:
                raise asyncio.IncompleteReadError(bytes(self._buf), n)
            self._buf.extend(self._chunks.pop(0))
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out


def _split_random(data: bytes, seed: int, pieces: int) -> list[bytes]:
    """把 data 随机切成 pieces 段（含可能的空段）。"""
    r = random.Random(seed)
    cuts = sorted(r.randrange(len(data) + 1) for _ in range(pieces))
    out, prev = [], 0
    for c in cuts:
        out.append(data[prev:c])
        prev = c
    out.append(data[prev:])
    return out


def _wire_batch_sample(n: int = 5, seed: int = 0) -> WireBatch:
    r = random.Random(seed)
    uniq = [f"k{i}" for i in range(max(1, n // 2))]
    idx = np.array([r.randrange(len(uniq)) for _ in range(n)], dtype=np.int32)
    return WireBatch(1, np.arange(n, dtype=np.int64), np.arange(n, dtype=np.int64),
                     np.arange(n, dtype=np.uint64), np.arange(n, dtype=np.uint64),
                     np.arange(n, dtype=np.uint64),
                     uniq, idx, ["u0", "u1"], idx, 0)


def _wb_equal(a: WireBatch, b: WireBatch) -> bool:
    """WireBatch 含 numpy 数组，不能用 dataclass 的 ==（数组真值不明确），逐字段比。"""
    if (a.req_id, a.url_uniq, a.user_uniq, a.invalid) != \
       (b.req_id, b.url_uniq, b.user_uniq, b.invalid):
        return False
    return (np.array_equal(a.ts, b.ts) and np.array_equal(a.nbytes, b.nbytes)
            and np.array_equal(a.h_id, b.h_id) and np.array_equal(a.h_url, b.h_url)
            and np.array_equal(a.h_user, b.h_user)
            and np.array_equal(a.url_idx, b.url_idx)
            and np.array_equal(a.user_idx, b.user_idx))


@pytest.mark.parametrize("pieces", [1, 3, 11])
def test_read_msg_framed_in_multiple_chunks(pieces):
    w = _wire_batch_sample()
    blob = _HDR.pack(len(pickle.dumps(w, protocol=5)), MSG_BATCH) \
        + pickle.dumps(w, protocol=5)
    reader = _ChunkedReader(_split_random(blob, seed=pieces, pieces=pieces * 2 + 1))
    mtype, obj = asyncio.run(read_msg(reader))
    assert mtype == MSG_BATCH
    assert _wb_equal(obj, w)


def test_read_msg_ack_roundtrip():
    ack = (7, "ok", 100, 2, 3, 4)
    blob = _HDR.pack(len(pickle.dumps(ack, protocol=5)), MSG_ACK) \
        + pickle.dumps(ack, protocol=5)
    reader = _ChunkedReader(_split_random(blob, seed=1, pieces=5))
    mtype, obj = asyncio.run(read_msg(reader))
    assert mtype == MSG_ACK and obj == ack


def test_pack_msg_one_frame():
    """pack_msg 把头 + 负载合成一整帧，read_msg 能还原（快照广播用：一次序列化发多端）。"""
    from flowlens.ipc import MSG_SNAPSHOT, pack_msg
    obj = {"a": 1, "b": [2, 3]}
    frame = pack_msg(MSG_SNAPSHOT, obj)
    assert frame[:_HDR.size] == _HDR.pack(len(frame) - _HDR.size, MSG_SNAPSHOT)
    reader = _ChunkedReader(_split_random(frame, seed=2, pieces=4))
    mtype, out = asyncio.run(read_msg(reader))
    assert mtype == MSG_SNAPSHOT and out == obj


def test_read_msg_incomplete_raises():
    """连接在消息中途关闭：readexactly 抛 IncompleteReadError（调用方处理）。"""
    w = _wire_batch_sample(3)
    blob = _HDR.pack(len(pickle.dumps(w, protocol=5)), MSG_BATCH) \
        + pickle.dumps(w, protocol=5)
    reader = _ChunkedReader([blob[:3]])   # 只给 3 个字节（头都没齐）
    with pytest.raises(asyncio.IncompleteReadError):
        asyncio.run(read_msg(reader))


def test_send_msg_then_read():
    w = _wire_batch_sample(8, seed=3)

    class _W:
        def __init__(self):
            self.data = bytearray()
        def write(self, b):
            self.data.extend(b)
        async def drain(self):
            pass

    wtr = _W()
    asyncio.run(send_msg(wtr, MSG_BATCH, w))
    reader = _ChunkedReader(_split_random(bytes(wtr.data), seed=9, pieces=6))
    mtype, obj = asyncio.run(read_msg(reader))
    assert mtype == MSG_BATCH and _wb_equal(obj, w)


# ---- _dedupe ----

def test_dedupe_indices():
    uniq, idx = _dedupe(["a", "b", "a", "c", "b", "a"])
    assert uniq == ["a", "b", "c"]
    assert idx.tolist() == [0, 1, 0, 2, 1, 0]
    u0, i0 = _dedupe([])
    assert u0 == [] and i0.dtype == np.int32 and len(i0) == 0


def test_dedupe_interned_strings_same_object():
    """驻留表命中时，同一内容已是同一对象，下标仍按首次出现计。"""
    tab = Intern(100)
    a = tab.intern("x")
    b = tab.intern("x")
    assert a is b
    uniq, idx = _dedupe([a, b, "y"])
    assert uniq == ["x", "y"] and idx.tolist() == [0, 0, 1]


# ---- 编解码往返 ----

def _assert_parsed_equal(p1, p2):
    assert len(p1) == len(p2)
    assert (p1.ts == p2.ts).all() and (p1.nbytes == p2.nbytes).all()
    assert (p1.h_id == p2.h_id).all() and (p1.h_url == p2.h_url).all()
    assert (p1.h_user == p2.h_user).all()
    assert p1.urls == p2.urls and p1.users == p2.users
    assert p1.invalid == p2.invalid


def test_roundtrip_longtail():
    """长尾（大量重复字符串）：去重后 uniq 数 << 事件数，展开后逐项相等。"""
    p = parse_body(_longtail_body(3000, seed=1))
    w = encode_parsed(42, p)
    assert len(w.url_uniq) < len(p.urls)   # 确实去重了
    p2 = decode_to_parsed(w)
    _assert_parsed_equal(p, p2)
    assert w.req_id == 42


def test_roundtrip_high_cardinality():
    """高基数（每个 URL 不同）：uniq 数 == 事件数，仍要还原。"""
    p = parse_body(_random_body(800, seed=2))
    # 十进制大数字段被归一成 {id}，但 event_id/user_id 各不相同，验证展开逻辑
    w = encode_parsed(1, p)
    assert len(w.user_uniq) == len(p.users)   # user_id 全不同
    p2 = decode_to_parsed(w)
    _assert_parsed_equal(p, p2)


def test_roundtrip_truly_distinct_urls():
    """URL 全不同：uniq 数 == 事件数。直接构造 ParsedBatch（不走 parse_body），
    避开 normalize_url 的 {id} 坍缩，纯粹验证"无重复时展开"这条路径。"""
    from flowlens.parse_pool import ParsedBatch
    n = 500
    urls = [f"/x/tok{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(n)]
    users = [f"u{i % 50}" for i in range(n)]
    p = ParsedBatch(np.arange(n, dtype=np.int64), np.ones(n, dtype=np.int64),
                    np.arange(n, dtype=np.uint64), np.arange(n, dtype=np.uint64),
                    np.arange(n, dtype=np.uint64), urls, users, 0)
    assert len(set(urls)) == n
    w = encode_parsed(3, p)
    assert len(w.url_uniq) == n
    p2 = decode_to_parsed(w)
    _assert_parsed_equal(p, p2)


def test_roundtrip_with_intern():
    """带驻留：引擎侧只对 uniq 驻留，展开后内容相同，且重复内容共享同一对象。"""
    u_tab, r_tab = Intern(10000), Intern(10000)
    p = parse_body(_longtail_body(1500, seed=4))
    w = encode_parsed(9, p)
    p2 = decode_to_parsed(w, intern_url=u_tab.intern, intern_user=r_tab.intern)
    _assert_parsed_equal(p, p2)
    # 驻留生效：还原列表里同一 URL 的两处是同一个对象（长尾数据必有重复）
    assert len(set(p.urls)) < len(p.urls)
    seen: dict[str, int] = {}
    shared = False
    for i, s in enumerate(p2.urls):
        if s in seen:
            shared = p2.urls[seen[s]] is s
            assert shared
            break
        seen[s] = i
    assert shared, "应有两处相同 URL 是同一驻留对象"


def test_roundtrip_empty_batch():
    """空批（0 条有效、只有 invalid）也能往返。"""
    p = parse_body(b"[5, \"str\", null]")
    assert len(p) == 0 and p.invalid == 3
    w = encode_parsed(5, p)
    p2 = decode_to_parsed(w)
    assert len(p2) == 0 and p2.invalid == 3
    assert p2.urls == [] and p2.users == []


def test_roundtrip_all_valid_empty():
    """合法空数组：0 条、invalid=0。"""
    p = parse_body(b"[]")
    w = encode_parsed(6, p)
    p2 = decode_to_parsed(w)
    assert len(p2) == 0 and p2.invalid == 0
