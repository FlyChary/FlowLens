"""memstat：有 cgroup 上限时用 cgroup 读数、无上限退回 statm，v1 与 v2 都覆盖。"""

import sys
from pathlib import Path

import pytest

import flowlens.memstat as mem
from flowlens.memstat import memory_bytes, memory_limit_bytes


def _write(root: Path, rel: str, content: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="ascii")


# ---- memory_limit_bytes ----

def test_v2_limit(tmp_path):
    _write(tmp_path, "sys/fs/cgroup/memory.max", "256\n")  # "max" 才是无上限；数字是上限
    assert memory_limit_bytes(str(tmp_path)) == 256


def test_v2_no_limit_when_max(tmp_path):
    # memory.max 内容为 "max"：没有数字，视为无上限
    _write(tmp_path, "sys/fs/cgroup/memory.max", "max\n")
    assert memory_limit_bytes(str(tmp_path)) is None


def test_v1_limit(tmp_path):
    # v1 没有 memory.max；limit_in_bytes 是具体数值
    _write(tmp_path, "sys/fs/cgroup/memory/memory.limit_in_bytes", "104857600\n")
    assert memory_limit_bytes(str(tmp_path)) == 104857600


def test_v1_no_limit_when_huge(tmp_path):
    # v1 默认上限是 2^60 左右巨大值，不小于 2^60 视为无上限
    _write(tmp_path, "sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n")
    assert memory_limit_bytes(str(tmp_path)) is None


def test_no_limit_anywhere_returns_none(tmp_path):
    assert memory_limit_bytes(str(tmp_path)) is None
    assert memory_limit_bytes(str(tmp_path / "不存在")) is None


# ---- memory_bytes：有上限用 cgroup ----

def test_v2_with_limit_uses_cgroup(tmp_path):
    _write(tmp_path, "sys/fs/cgroup/memory.max", "256\n")
    _write(tmp_path, "sys/fs/cgroup/memory.current", "123456789\n")
    # statm 也写一个值，确认没被采用
    _write(tmp_path, "proc/self/statm", "5000 1234 200 0 0 1000 0\n")
    assert memory_bytes(str(tmp_path)) == 123456789


def test_v1_with_limit_uses_cgroup(tmp_path):
    # v1：有 limit、有 usage，且没有 v2 文件
    _write(tmp_path, "sys/fs/cgroup/memory/memory.limit_in_bytes", "104857600\n")
    _write(tmp_path, "sys/fs/cgroup/memory/memory.usage_in_bytes", "987654321\n")
    _write(tmp_path, "proc/self/statm", "5000 1234 200 0 0 1000 0\n")
    assert memory_bytes(str(tmp_path)) == 987654321


def test_v2_beats_v1_when_both_have_limit(tmp_path):
    # v2 有上限时优先用 v2 读数，v1 被抢先
    _write(tmp_path, "sys/fs/cgroup/memory.max", "256\n")
    _write(tmp_path, "sys/fs/cgroup/memory.current", "111\n")
    _write(tmp_path, "sys/fs/cgroup/memory/memory.limit_in_bytes", "104857600\n")
    _write(tmp_path, "sys/fs/cgroup/memory/memory.usage_in_bytes", "222\n")
    assert memory_bytes(str(tmp_path)) == 111


# ---- memory_bytes：无上限退回 statm ----

def test_v2_no_limit_falls_back_to_statm(tmp_path, monkeypatch):
    # 有 memory.max 但内容是 "max"（无上限）→ 退回 statm
    _write(tmp_path, "sys/fs/cgroup/memory.max", "max\n")
    _write(tmp_path, "sys/fs/cgroup/memory.current", "999999\n")  # 不应被采用
    _write(tmp_path, "proc/self/statm", "5000 1234 200 0 0 1000 0\n")
    monkeypatch.setattr(mem, "_page_size", lambda: 4096)
    assert memory_bytes(str(tmp_path)) == 1234 * 4096


def test_no_cgroup_at_all_uses_statm(tmp_path, monkeypatch):
    # 完全没有 cgroup 文件（例如 Windows）→ statm
    _write(tmp_path, "proc/self/statm", "5000 1234 200 0 0 1000 0\n")
    monkeypatch.setattr(mem, "_page_size", lambda: 4096)
    assert memory_bytes(str(tmp_path)) == 1234 * 4096


def test_limit_but_usage_missing_falls_back_to_statm(tmp_path, monkeypatch):
    # 有上限但对应 usage 读不到：仍退回 statm（不返回 0，避免"有上限却读不到占用"误判）
    _write(tmp_path, "sys/fs/cgroup/memory.max", "256\n")  # 有上限
    # 没有 memory.current / memory.usage_in_bytes
    _write(tmp_path, "proc/self/statm", "5000 777 200 0 0 1000 0\n")
    monkeypatch.setattr(mem, "_page_size", lambda: 4096)
    assert memory_bytes(str(tmp_path)) == 777 * 4096


# ---- 健壮性：全缺返回 0、乱码不抛 ----

def test_all_missing_no_fake_data(tmp_path):
    # 全缺时不返回伪造值：非 Windows 没有 WorkingSet 兜底 → 0；Windows 退回真实的
    # WorkingSetSize（> 0，T8b）——不能断言 == 0，否则把"Windows 恒 0"的旧 bug 锁死。
    if sys.platform == "win32":
        v1, v2 = mem._windows_working_set(), mem._windows_working_set()
    else:
        v1 = v2 = 0
    assert memory_bytes(str(tmp_path)) == v1
    assert memory_bytes(str(tmp_path / "不存在")) == v2


def test_garbage_files_do_not_raise(tmp_path):
    # 上限与占用文件都存在但内容不是数字：limit 判不出 → statm 也乱码 → 0，全程不抛
    _write(tmp_path, "sys/fs/cgroup/memory.max", "not-a-number\n")
    _write(tmp_path, "sys/fs/cgroup/memory.current", "zzz\n")
    _write(tmp_path, "sys/fs/cgroup/memory/memory.limit_in_bytes", "\x00\x01\n")
    _write(tmp_path, "sys/fs/cgroup/memory/memory.usage_in_bytes", "???\n")
    _write(tmp_path, "proc/self/statm", "xx yy\n")
    # 乱码文件全读不出：非 Windows → 0；Windows 退回真实 WorkingSetSize（> 0）
    expected = mem._windows_working_set() if sys.platform == "win32" else 0
    assert memory_bytes(str(tmp_path)) == expected
    assert memory_limit_bytes(str(tmp_path)) is None


def test_empty_file_skipped(tmp_path):
    # 空的上限文件：读不出数字 → 视为无上限
    (tmp_path / "sys" / "fs" / "cgroup").mkdir(parents=True)
    (tmp_path / "sys" / "fs" / "cgroup" / "memory.max").write_bytes(b"")
    assert memory_limit_bytes(str(tmp_path)) is None


# ---- T8b：cgroup 与 statm 全缺时退回 Windows WorkingSetSize ----

def test_all_missing_falls_back_to_windows_working_set(tmp_path, monkeypatch):
    # cgroup 与 statm 都读不到时，memory_bytes 应退回 _windows_working_set（Windows
    # 开发机拿真实值）。跨平台可测：把 _windows_working_set 换成返回固定值的替身，
    # 验证"全缺 → 退回该读数"这条调用链（不管实际在哪个平台跑）。
    monkeypatch.setattr(mem, "_windows_working_set", lambda platform=None: 7_000_000)
    assert memory_bytes(str(tmp_path)) == 7_000_000
    # 有 statm 时不退回 Windows 分支（statm 优先）
    _write(tmp_path, "proc/self/statm", "5000 1234 200 0 0 1000 0\n")
    monkeypatch.setattr(mem, "_page_size", lambda: 4096)
    assert memory_bytes(str(tmp_path)) == 1234 * 4096


@pytest.mark.skipif(sys.platform != "win32", reason="只在 Windows 上验证真实读数")
def test_windows_working_set_reads_real_value():
    # Windows 开发机：memory_bytes 读到真实的 WorkingSetSize（> 0、量级为进程实际
    # 内存）。此前 memstat 在 Windows 上恒返回 0，这条路径本地从没跑过（T8b 背景）。
    v = mem._windows_working_set()
    assert isinstance(v, int)
    assert v > 0, "Windows 上应读到正的 WorkingSetSize"
    # 进程内存量级：Python 解释器 + numpy 约几十 MB，不该是 KB 级或 TB 级
    assert 1 * 1024 * 1024 < v < 4 * 1024 * 1024 * 1024
    # 全缺路径（无 cgroup、无 /proc）也走到它
    assert memory_bytes(str("C:/不存在的路径")) == v
