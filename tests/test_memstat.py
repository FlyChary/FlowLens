"""memstat：cgroup v2 / v1 / statm 的读取顺序与换算，全缺返回 0，永不抛异常。"""

from pathlib import Path

import flowlens.memstat as mem
from flowlens.memstat import memory_bytes


def _write(root: Path, rel: str, content: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="ascii")


def test_cgroup_v2(tmp_path):
    _write(tmp_path, "sys/fs/cgroup/memory.current", "123456789\n")
    _write(tmp_path, "sys/fs/cgroup/memory/memory.usage_in_bytes", "1\n")  # v1 存在但必须被 v2 抢先
    assert memory_bytes(str(tmp_path)) == 123456789


def test_cgroup_v1_when_no_v2(tmp_path):
    _write(tmp_path, "sys/fs/cgroup/memory/memory.usage_in_bytes", "987654321\n")
    assert memory_bytes(str(tmp_path)) == 987654321


def test_statm_when_no_cgroup(tmp_path, monkeypatch):
    # 伪造 statm：第一列 size、第二列 resident=1234
    _write(tmp_path, "proc/self/statm", "5000 1234 200 0 0 1000 0\n")
    monkeypatch.setattr(mem, "_page_size", lambda: 4096)
    assert memory_bytes(str(tmp_path)) == 1234 * 4096


def test_all_missing_returns_zero(tmp_path):
    # 目录存在但三个来源都没有
    assert memory_bytes(str(tmp_path)) == 0
    # 连 root 目录都不存在也不抛异常
    assert memory_bytes(str(tmp_path / "不存在")) == 0


def test_garbage_files_do_not_raise(tmp_path):
    # 文件存在但内容不是数字：逐层跳过，不抛异常
    _write(tmp_path, "sys/fs/cgroup/memory.current", "not-a-number\n")
    _write(tmp_path, "sys/fs/cgroup/memory/memory.usage_in_bytes", "\x00\x01\n")
    _write(tmp_path, "proc/self/statm", "xx yy\n")
    assert memory_bytes(str(tmp_path)) == 0


def test_empty_file_skipped(tmp_path):
    (tmp_path / "sys" / "fs" / "cgroup").mkdir(parents=True)
    (tmp_path / "sys" / "fs" / "cgroup" / "memory.current").write_bytes(b"")
    assert memory_bytes(str(tmp_path)) == 0
