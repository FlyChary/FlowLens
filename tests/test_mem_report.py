"""C1 内存账本工具的测试（tools/mem_report.py）。

覆盖深度遍历统计方法（MemWalker）：已知大小的对象计数正确、共享对象只计一次、
numpy 数组按 nbytes、key 字符串单列归 C_KEYS；以及驻留节省与 Bloom 误判率
两个纯函数的已知值核对。
"""

import sys
from pathlib import Path

import numpy as np
import pytest

_TOOLS = Path(__file__).resolve().parent.parent / "tools"
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

from mem_report import (C_KEYS, C_MISC, _bloom_halved, _intern_savings,  # noqa: E402
                        _skip_expand, MemWalker)


# ---------------- MemWalker：计数正确性 ----------------

def test_shared_object_counted_once():
    """同一个对象被两个组件引用时只计一次（归先遍历的组件）。"""
    shared = "s" * 1000                      # 一个较大的共享字符串
    shared_arr = np.zeros(1024, dtype=np.uint64)
    a = [shared, shared_arr]
    b = [shared, shared_arr, "only-in-b"]
    w = MemWalker(keystring_ids=set())
    w.walk([a], "compA")
    w.walk([b], "compB")
    assert w.n["compA"] == 3                # a 本身 + shared + shared_arr
    assert w.b["compA"] == sys.getsizeof(a) + sys.getsizeof(shared) + shared_arr.nbytes
    assert w.n["compB"] == 2                # b 本身 + "only-in-b"（共享的不计）
    assert w.b["compB"] == sys.getsizeof(b) + sys.getsizeof("only-in-b")
    # 总量 = 五个对象各计一次
    assert w.total() == sys.getsizeof(a) + sys.getsizeof(shared) + shared_arr.nbytes \
        + sys.getsizeof(b) + sys.getsizeof("only-in-b")


def test_numpy_array_counted_by_nbytes_not_sizeof():
    arr = np.zeros(4096, dtype=np.float64)
    w = MemWalker(keystring_ids=set())
    w.walk([arr], "comp")
    assert w.b["comp"] == arr.nbytes        # 32768，而不是 getsizeof(arr) 的对象头
    assert w.n["comp"] == 1


def test_keystring_goes_to_its_own_row():
    """id 在 keystring_ids 里的字符串一律归 C_KEYS，无论从哪个组件可达。"""
    s1, s2 = "key-A", "key-B"
    bucket = [s1, s2, "not-a-key"]
    w = MemWalker(keystring_ids={id(s1), id(s2)})
    w.walk([bucket], "ring")
    assert w.n[C_KEYS] == 2
    assert w.b[C_KEYS] == sys.getsizeof(s1) + sys.getsizeof(s2)
    assert w.n["ring"] == 2                 # bucket 本身 + "not-a-key"
    assert w.b["ring"] == sys.getsizeof(bucket) + sys.getsizeof("not-a-key")


def test_scalar_leaves_not_expanded():
    """int/float/None 是叶子：只计自身，不向引用展开。"""
    obj = [42, 3.5, None]
    w = MemWalker(keystring_ids=set())
    w.walk([obj], "comp")
    assert w.n["comp"] == 4
    assert w.b["comp"] == sys.getsizeof(obj) + sys.getsizeof(42) \
        + sys.getsizeof(3.5) + sys.getsizeof(None)


def test_engine_graph_ledger_covers_all_rows():
    """在真实（迷你）Engine 上建账本：所有行字节之和 = 遍历总量，且 Bloom 被归到
    它自己的行（它的 numpy 位数组不在任何其他组件下可达）。"""
    from flowlens.config import Config
    from flowlens.engine import Engine
    from flowlens.model import EventBatch
    from flowlens.query import QueryService
    from flowlens.timewin import FakeClock
    from mem_report import BASE_NOW_MS, build_ledger

    import os
    os.environ["PYTHONHASHSEED"] = "0"      # 哈希一致性只在跨进程有意义，这里仅作记录
    engine = Engine(Config(), FakeClock(BASE_NOW_MS))
    qsvc = QueryService(engine)
    batch = EventBatch(["e1", "e2"], ["u1", "u2"], ["/api/item/x1", "/api/item/x2"],
                       np.array([BASE_NOW_MS, BASE_NOW_MS + 100], np.int64),
                       np.array([1000, 2000], np.int64), 0)
    engine.ingest(batch)
    engine.tick()

    led = build_ledger(engine, qsvc)
    rows = led["rows"]
    assert sum(v["b"] for v in rows.values()) == led["walked_bytes"]
    # Bloom 的位数组：8 代 × (1<<21)/8 字节（默认配置 bits=21）
    expected_bloom_words = 8 * ((1 << engine.bloom.bits) // 64 * 8)
    assert rows["去重 Bloom filter"]["b"] >= expected_bloom_words
    assert rows[C_KEYS]["n"] >= 4           # 4 个 key 字符串（可能部分同对象）


def test_skip_expand_covers_scalars_and_builtins():
    assert _skip_expand("x")
    assert _skip_expand(1)
    assert _skip_expand(np.zeros(3))
    assert _skip_expand(int)                # 类型对象
    assert _skip_expand(len)                # builtin
    assert not _skip_expand([1])
    assert not _skip_expand({"a": 1})
    assert not _skip_expand(object())


# ---------------- 驻留节省：已知大小核对 ----------------

def test_intern_savings_known_sizes():
    # 同值字符串的 3 个不同对象 + 另一个值（用运行时运算构造，避开常量折叠）
    sa = [chr(97) * 50, chr(97) * 50, chr(97) * 50]
    assert len({id(s) for s in sa}) == 3
    sb = chr(98) * 50
    inv = {"url": {"a" * 50: {id(s) for s in sa}, "b" * 50: {id(sb)}}}
    r = _intern_savings(inv)["url"]
    sa_sz = sys.getsizeof(sa[0])
    sb_sz = sys.getsizeof(sb)
    assert sa_sz == sb_sz                  # 同长度字符串 sizeof 相同
    assert r["distinct_values"] == 2
    assert r["current_objects"] == 4
    assert r["dup_objects"] == 2
    assert r["dup_waste_bytes"] == 2 * sa_sz
    assert r["interned_bytes"] == sa_sz + sb_sz
    assert r["table_cost"] == 2 * 56
    assert r["saved_bytes"] == 2 * sa_sz - 2 * 56


# ---------------- Bloom 减半：误判率 ----------------

def test_bloom_halved_formula():
    from flowlens.config import Config
    r = _bloom_halved(Config())
    cur, half = r["cur"], r["half"]
    assert half["m"] == cur["m"] // 2
    assert half["k"] == max(1, cur["k"] // 2)
    # 设计容量下：当前配置误判率 ≤ 0.1%，减半后显著上升
    assert cur["fp_at_capacity"] <= 0.001 + 1e-9
    assert half["fp_at_capacity"] > cur["fp_at_capacity"] * 10
    # 150% 容量时更糟
    assert half["fp_at_150pct"] > half["fp_at_capacity"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
