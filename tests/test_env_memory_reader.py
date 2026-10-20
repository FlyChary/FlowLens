"""P2d-2 五：_env_memory_reader 只在调试模式（FLOWLENS_DEBUG=1）下读 FLOWLENS_TEST_MEM。

未开调试却设了 FLOWLENS_TEST_MEM 时忽略（返回真实读数）；开调试且合法时覆盖；
格式非法时忽略。通过 monkeypatch FLOWLENS_DEBUG / FLOWLENS_TEST_MEM 与真实
_default_memory_reader 解耦来断言分支。
"""

import pytest

from flowlens import __main__ as m
from flowlens.api.app import _default_memory_reader


@pytest.fixture
def reader(monkeypatch):
    """把真实内存读数钉成一个可辨认的哨兵，断言"是否被覆盖"。

    _env_memory_reader 在函数体内 `from .api.app import _default_memory_reader`
    （局部导入），所以要在 api.app 模块上打补丁。"""
    import flowlens.api.app as appmod
    monkeypatch.setattr(appmod, "_default_memory_reader",
                        lambda: (7_777_777, None))
    return m._env_memory_reader


def test_no_test_mem_returns_real(reader, monkeypatch):
    monkeypatch.delenv("FLOWLENS_TEST_MEM", raising=False)
    monkeypatch.delenv("FLOWLENS_DEBUG", raising=False)
    assert reader() == (7_777_777, None)


def test_test_mem_without_debug_ignored(reader, monkeypatch):
    """设了 FLOWLENS_TEST_MEM 但 FLOWLENS_DEBUG 未开：忽略、返回真实读数。"""
    monkeypatch.setenv("FLOWLENS_TEST_MEM", "100,200")
    monkeypatch.delenv("FLOWLENS_DEBUG", raising=False)
    assert reader() == (7_777_777, None)


def test_test_mem_with_debug_overrides(reader, monkeypatch):
    """FLOWLENS_DEBUG=1 + 合法 FLOWLENS_TEST_MEM：覆盖为设定值。"""
    monkeypatch.setenv("FLOWLENS_TEST_MEM", "251658240,268435456")
    monkeypatch.setenv("FLOWLENS_DEBUG", "1")
    assert reader() == (251658240, 268435456)


def test_test_mem_with_debug_no_limit(reader, monkeypatch):
    """只有 mem（limit 留空）：limit 为 None。"""
    monkeypatch.setenv("FLOWLENS_TEST_MEM", "123456,")
    monkeypatch.setenv("FLOWLENS_DEBUG", "1")
    assert reader() == (123456, None)


def test_test_mem_with_debug_bad_format_ignored(reader, monkeypatch):
    """开调试但格式非法：忽略、返回真实读数。"""
    monkeypatch.setenv("FLOWLENS_TEST_MEM", "not-a-number")
    monkeypatch.setenv("FLOWLENS_DEBUG", "1")
    assert reader() == (7_777_777, None)


def test_debug_flag_is_the_gate(monkeypatch):
    """debug.is_enabled 只在 FLOWLENS_DEBUG=1 时为 True（本函数据此判门）。"""
    from flowlens.api import debug
    monkeypatch.delenv("FLOWLENS_DEBUG", raising=False)
    assert debug.is_enabled() is False
    monkeypatch.setenv("FLOWLENS_DEBUG", "1")
    assert debug.is_enabled() is True
    monkeypatch.setenv("FLOWLENS_DEBUG", "0")
    assert debug.is_enabled() is False
    # 真实 _default_memory_reader 存在且可调（回归：没被我 patch 掉的地方仍可用）
    assert callable(_default_memory_reader)
