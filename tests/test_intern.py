"""驻留表（flowlens/intern.py）：上限与整体清空、相同内容返回同一对象。"""

import pytest

from flowlens.intern import Intern


def _fresh(s: str) -> str:
    """造一个与字面量不是同一对象的等价字符串（绕过 CPython 的字面量驻留）。"""
    return "u" + s


def test_same_content_returns_same_object():
    it = Intern(100)
    a = it.intern(_fresh("1"))
    b = it.intern(_fresh("1"))
    assert a is b, "相同内容应返回同一个对象"
    # 内容与传入一致
    assert a == "u1"


def test_distinct_content_distinct_objects():
    it = Intern(100)
    a = it.intern(_fresh("1"))
    b = it.intern(_fresh("2"))
    assert a is not b
    assert a == "u1" and b == "u2"


def test_fill_to_cap_then_clears():
    it = Intern(4)
    for i in range(4):
        it.intern(_fresh(str(i)))
    assert len(it) == 4
    # 第 5 个触发整体清空：旧的驻留项全部丢弃，只留下新的
    it.intern(_fresh("4"))
    assert len(it) == 1
    assert list(it._table) == ["u4"]


def test_after_clear_old_content_not_reused():
    it = Intern(2)
    x = it.intern(_fresh("0"))
    it.intern(_fresh("1"))
    # 触发整体清空后，"u0" 不再是驻留对象（重新 intern 会得到新对象）
    it.intern(_fresh("2"))
    assert "u0" not in it._table
    y = it.intern(_fresh("0"))
    assert y is not x, "清空后重新驻留应是新对象"


def test_len_and_clear():
    it = Intern(10)
    it.intern("a")
    it.intern("b")
    assert len(it) == 2
    it.clear()
    assert len(it) == 0
    # clear 后重新驻留
    assert it.intern("a") == "a"


def test_invalid_cap():
    with pytest.raises(ValueError):
        Intern(0)
    with pytest.raises(ValueError):
        Intern(-1)
