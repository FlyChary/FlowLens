"""内存读数：优先 cgroup（有上限时），退回进程常驻集，全缺返回 0。

读取顺序（root 参数只给测试伪造目录用，生产一律默认 "/"）：
1. cgroup v2：上限看 memory.max、读数看 memory.current
2. cgroup v1：上限看 memory.limit_in_bytes、读数看 memory.usage_in_bytes
3. 进程常驻集：/proc/self/statm 第二列（常驻页数）× 系统页大小

只有当前进程所在的 cgroup 设置了内存上限时才采用 cgroup 读数，否则回退到进程
常驻集（statm）。原因：WSL 里 /sys/fs/cgroup 是整个 Linux 发行版（所有进程共享），
读到的是全部进程的总和（实测 427 MB，而本进程仅约 109 MB），降级控制器会因此误判；
目标 Docker 容器用 --memory 启动，一定带上限，cgroup 读数才准。

任何一步读不到（例如 Windows 没有这些文件）都静默进入下一步，
全部读不到时返回 0；本模块任何情况下都不抛异常——它是 /stats 与
/healthz 的读数来源，读内存失败不该让查询接口报错。
"""

import os
from pathlib import Path

# cgroup v1 没有显式"无上限"标记，内核默认上限是 2^60 左右的巨大值；
# 读到不小于它的数视为没有设置上限。
_V1_NO_LIMIT = 1 << 60


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


def _proc_rss_bytes(root: str) -> int:
    """进程常驻集（/proc/self/statm 第二列 × 页大小）；读不到返回 0。"""
    pgs = _first_int(Path(root) / "proc" / "self" / "statm", 1)
    if pgs is None:
        return 0
    return pgs * _page_size()


def memory_limit_bytes(root: str = "/") -> int | None:
    """当前进程所在 cgroup 的内存上限（字节）；没有上限或读不到时返回 None。

    - cgroup v2：memory.max，内容为 "max"（或读不到数字）视为没有上限；
    - cgroup v1：memory.limit_in_bytes，数值不小于 2^60 视为没有上限。
    两级都拿不到确定的上限时返回 None（例如 Windows）。
    """
    p = Path(root)
    # cgroup v2：memory.max 内容为 "max" 表示无上限
    v2 = _first_int(p / "sys" / "fs" / "cgroup" / "memory.max", 0)
    if v2 is not None:
        return v2
    # cgroup v1：memory.limit_in_bytes 不小于 2^60 视为无上限
    v1 = _first_int(p / "sys" / "fs" / "cgroup" / "memory" / "memory.limit_in_bytes", 0)
    if v1 is not None and v1 < _V1_NO_LIMIT:
        return v1
    return None


def memory_bytes(root: str = "/") -> int:
    """当前内存占用（字节）。有 cgroup 上限时用 cgroup 读数，否则用进程常驻集。

    读不到时返回 0，不抛异常。
    """
    limit = memory_limit_bytes(root)
    if limit is not None:
        # 有上限：读对应的 cgroup 占用（v2 优先，v1 次之）
        p = Path(root)
        v = _first_int(p / "sys" / "fs" / "cgroup" / "memory.current", 0)
        if v is not None:
            return v
        v = _first_int(p / "sys" / "fs" / "cgroup" / "memory" / "memory.usage_in_bytes", 0)
        if v is not None:
            return v
    # 无上限（或未识别出 cgroup）：退回进程常驻集
    return _proc_rss_bytes(root)
