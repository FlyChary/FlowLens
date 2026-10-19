"""C2 内存探测工具的测试（tools/mem_probe.py）。

只测纯函数/可注入读数的部分（cgroup、/proc 解析，用伪造 root 目录），不在 Windows
上依赖真实 /sys、/proc。模式标签与 malloc_trim 的 ctypes 调用在容器里实测，单测只
覆盖"标签透传 + 缺 libc 时不抛"。
"""

import ctypes
import sys
from pathlib import Path

import pytest

_TOOLS = Path(__file__).resolve().parent.parent / "tools"
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

import mem_probe  # noqa: E402


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# ---------------- cgroup_memory ----------------

def test_cgroup_v2(tmp_path):
    """cgroup v2：memory.current + memory.stat 四字段 + memory.max 上限 + 工作集。"""
    cg = tmp_path / "sys" / "fs" / "cgroup"
    _write(cg / "memory.current", "250000000\n")
    _write(cg / "memory.max", "268435456\n")          # 256 MB
    _write(cg / "memory.stat",
           "anon 160000000\n"
           "file 80000000\n"
           "active_file 17500000\n"
           "inactive_file 62500000\n"
           "sock 5000\n")
    d = mem_probe.cgroup_memory(str(tmp_path))
    assert d["current"] == 250_000_000
    assert d["limit"] == 268_435_456
    assert d["anon"] == 160_000_000
    assert d["file"] == 80_000_000
    assert d["active_file"] == 17_500_000
    assert d["inactive_file"] == 62_500_000
    # 工作集 = current − inactive_file（降级口径）
    assert d["working_set"] == 250_000_000 - 62_500_000


def test_cgroup_v2_max_unlimited(tmp_path):
    """memory.max 为 'max' 时上限记 None（无上限）。"""
    cg = tmp_path / "sys" / "fs" / "cgroup"
    _write(cg / "memory.current", "1000000\n")
    _write(cg / "memory.max", "max\n")
    _write(cg / "memory.stat", "inactive_file 100000\n")
    d = mem_probe.cgroup_memory(str(tmp_path))
    assert d["limit"] is None
    assert d["current"] == 1_000_000
    assert d["working_set"] == 900_000


def test_cgroup_v1_fallback(tmp_path):
    """无 v2（memory.current 缺失）时退回 cgroup v1 的 memory/ 目录。"""
    v1 = tmp_path / "sys" / "fs" / "cgroup" / "memory"
    _write(v1 / "memory.usage_in_bytes", "200000000\n")
    _write(v1 / "memory.limit_in_bytes", "268435456\n")
    _write(v1 / "memory.stat",
           "total_inactive_file 60000000\n"
           "inactive_anon 50000\n"
           "anon 150000000\n")
    d = mem_probe.cgroup_memory(str(tmp_path))
    assert d["current"] == 200_000_000
    assert d["limit"] == 268_435_456
    assert d["working_set"] == 200_000_000 - 60_000_000
    assert d["anon"] == 150_000_000


def test_cgroup_v1_no_limit(tmp_path):
    """v1 上限 ≥ 2^60 视为无上限（None）。"""
    v1 = tmp_path / "sys" / "fs" / "cgroup" / "memory"
    _write(v1 / "memory.usage_in_bytes", "1000\n")
    _write(v1 / "memory.limit_in_bytes", str(1 << 62) + "\n")
    d = mem_probe.cgroup_memory(str(tmp_path))
    assert d["limit"] is None


def test_cgroup_missing_all(tmp_path):
    """什么 cgroup 文件都没有时返回空 dict（不抛）。"""
    d = mem_probe.cgroup_memory(str(tmp_path))
    assert d == {}


# ---------------- proc_status ----------------

def test_proc_status(tmp_path):
    """/proc/self/status 的 VmRSS / RssAnon（KiB → 字节）。"""
    _write(tmp_path / "proc" / "self" / "status",
           "Name:   flowlens\n"
           "VmRSS:      190000 kB\n"
           "RssAnon:    160000 kB\n"
           "RssFile:      30000 kB\n")
    d = mem_probe.proc_status(str(tmp_path))
    assert d["VmRSS_bytes"] == 190_000 * 1024
    assert d["RssAnon_bytes"] == 160_000 * 1024


def test_proc_status_missing(tmp_path):
    """读不到 /proc/self/status（如 Windows）返回空 dict。"""
    assert mem_probe.proc_status(str(tmp_path)) == {}


def test_rss_anon_missing(tmp_path, monkeypatch):
    """读不到时 rss_anon 返回 0（供 malloc_trim 前后对照，不抛）。"""
    monkeypatch.setattr(mem_probe, "proc_status", lambda root="/": {})
    assert mem_probe.rss_anon() == 0


# ---------------- malloc_trim ----------------

def test_malloc_trim_no_libc(monkeypatch):
    """加载不了 libc 时返回错误、不抛（Windows/非 glibc 环境）。"""
    def _boom(name):
        raise OSError("no libc")
    monkeypatch.setattr(ctypes, "CDLL", _boom, raising=True)
    mem_probe._LIBC = None
    r = mem_probe._malloc_trim()
    assert "error" in r


# ---------------- gc_top10 ----------------

def test_gc_top10_shape():
    """gc_top10 返回 [(类型名, 数量)] 前 10 + 总数，且数量降序。"""
    top, total = mem_probe._gc_top10()
    assert len(top) <= 10
    assert total > 0
    assert all(isinstance(name, str) and cnt > 0 for name, cnt in top)
    # 降序
    counts = [c for _, c in top]
    assert counts == sorted(counts, reverse=True)

