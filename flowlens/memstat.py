"""内存读数：优先 cgroup，退回进程 RSS，再退回 0。

读取顺序（root 参数只给测试伪造目录用，生产一律默认 "/"）：
1. cgroup v2：root/sys/fs/cgroup/memory.current（字节数）
2. cgroup v1：root/sys/fs/cgroup/memory/memory.usage_in_bytes（字节数）
3. 进程常驻集：root/proc/self/statm 第二列（常驻页数）× 系统页大小

任何一步读不到（例如 Windows 没有这些文件）都静默进入下一步，
全部读不到时返回 0；本模块任何情况下都不抛异常——它是 /stats 与
/healthz 的读数来源，读内存失败不该让查询接口报错。
"""

import os
from pathlib import Path


def _first_int(path: Path, col: int) -> int | None:
    """读文本文件的第一行、第 col 列（0 起）整数；读不到或不是数字返回 None。"""
    try:
        with open(path, "rb") as f:
            data = f.read(128)
    except OSError:
        return None
    try:
        return int(data.split(None, col + 1)[col])
    except (ValueError, IndexError):
        return None


def _page_size() -> int:
    """系统页大小（字节）；拿不到（例如 Windows 没有 os.sysconf）返回 0。"""
    try:
        return int(os.sysconf("SC_PAGE_SIZE"))
    except (ValueError, OSError, AttributeError):
        return 0


def memory_bytes(root: str = "/") -> int:
    """当前内存占用（字节）。读不到时返回 0，不抛异常。"""
    p = Path(root)
    # cgroup v2
    v = _first_int(p / "sys" / "fs" / "cgroup" / "memory.current", 0)
    if v is not None:
        return v
    # cgroup v1
    v = _first_int(p / "sys" / "fs" / "cgroup" / "memory" / "memory.usage_in_bytes", 0)
    if v is not None:
        return v
    # /proc/self/statm：第二列是常驻集页数（resident），乘页大小换成字节
    pgs = _first_int(p / "proc" / "self" / "statm", 1)
    if pgs is not None:
        return pgs * _page_size()
    return 0
