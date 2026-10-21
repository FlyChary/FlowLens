"""P2d-1 一：前端数计算（resolve_front_count）各分支。

分支：环境变量（含 0 与负数、非 Linux 平台被忽略）/ 非 Linux 平台 / 核数不足 /
内存上限过低 / 自动开（内存上限有或无）。
"""

import pytest

import flowlens.supervisor as sup


def _patch(monkeypatch, platform="linux", cores=4, limit=None):
    monkeypatch.setattr(sup, "sys", type("P", (), {"platform": platform})())
    monkeypatch.setattr(sup, "available_cpus", lambda: cores)
    monkeypatch.setattr(sup, "memory_limit_bytes", lambda: limit)
    monkeypatch.delenv("FLOWLENS_FRONTENDS", raising=False)


MB = 1024 * 1024


# ---- 环境变量优先 ----

def test_env_explicit(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("FLOWLENS_FRONTENDS", "2")
    assert sup.resolve_front_count() == (2, "FLOWLENS_FRONTENDS=2")


def test_env_zero_disables(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("FLOWLENS_FRONTENDS", "0")
    assert sup.resolve_front_count() == (0, "FLOWLENS_FRONTENDS=0")


def test_env_negative_clamped_to_zero(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("FLOWLENS_FRONTENDS", "-3")
    assert sup.resolve_front_count() == (0, "FLOWLENS_FRONTENDS=-3")


def test_env_ignored_on_non_linux(monkeypatch):
    _patch(monkeypatch, platform="win32")
    monkeypatch.setenv("FLOWLENS_FRONTENDS", "2")
    n, why = sup.resolve_front_count()
    assert n == 0
    assert "非 Linux" in why


def test_env_whitespace_stripped(monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("FLOWLENS_FRONTENDS", " 2 ")
    assert sup.resolve_front_count() == (2, "FLOWLENS_FRONTENDS=2")


# ---- 无环境变量：平台 / 核数 / 内存 ----

def test_non_linux_auto_zero(monkeypatch):
    _patch(monkeypatch, platform="win32")
    n, why = sup.resolve_front_count()
    assert n == 0 and "SO_REUSEPORT" in why


def test_linux_single_core_zero(monkeypatch):
    _patch(monkeypatch, cores=1, limit=None)
    n, why = sup.resolve_front_count()
    assert n == 0 and "核数" in why


def test_linux_low_memory_limit_zero(monkeypatch):
    _patch(monkeypatch, cores=8, limit=256 * MB)
    n, why = sup.resolve_front_count()
    assert n == 0
    assert "256 MB" in why and "384 MB" in why


def test_linux_auto_with_memory_limit(monkeypatch):
    _patch(monkeypatch, cores=8, limit=512 * MB)
    n, why = sup.resolve_front_count()
    assert n == 3  # min(3, 8-1)
    assert "512" in why


def test_linux_auto_no_memory_limit(monkeypatch):
    _patch(monkeypatch, cores=8, limit=None)
    n, why = sup.resolve_front_count()
    assert n == 3
    assert "无" in why


def test_linux_auto_two_cores(monkeypatch):
    _patch(monkeypatch, cores=2, limit=None)
    n, _ = sup.resolve_front_count()
    assert n == 1


def test_linux_memory_limit_boundary(monkeypatch):
    # 恰好等于下限（384 MB）：允许开
    _patch(monkeypatch, cores=4, limit=sup.MIN_MEMORY_FOR_FRONTS)
    n, _ = sup.resolve_front_count()
    assert n == 3
