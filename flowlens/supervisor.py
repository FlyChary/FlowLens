"""前端进程架构（P2d-1）：前端数的计算与前端进程的管理。

引擎与前端之间通过共享内存交换降级状态与额度占用（见第二节 SharedState），
前端进程由 FrontSupervisor 用 spawn 上下文拉起并监控（见第五节 5.4）。
"""

import logging
import os
import sys

from .memstat import memory_limit_bytes
from .parse_pool import available_cpus

log = logging.getLogger("flowlens.supervisor")

# P2c 第四节：256 MB 下多进程反而更差（每个 Python 进程几十 MB 基础内存），
# 低于该内存上限时不开前端
MIN_MEMORY_FOR_FRONTS = 384 * 1024 * 1024
# 前端数上限：引擎本身占一个核，前端数 = min(上限, 可用核数 − 1)
MAX_FRONTS = 3


def resolve_front_count() -> tuple[int, str]:
    """前端进程数与原因（原因写进启动日志）。

    优先级：环境变量 FLOWLENS_FRONTENDS（显式值，0 表示禁用）> 自动判断
    （Linux + 至少 2 核 + 内存上限不低于 384 MB 时开 min(3, 核数 − 1)）。
    SO_REUSEPORT 的 TCPSite 依赖 Linux，非 Linux 平台一律 0。
    """
    env = os.environ.get("FLOWLENS_FRONTENDS", "").strip()
    if env:
        n = max(0, int(env))
        if n > 0 and not sys.platform.startswith("linux"):
            return 0, "FLOWLENS_FRONTENDS 已设置，但非 Linux 平台不支持 SO_REUSEPORT"
        return n, f"FLOWLENS_FRONTENDS={env}"
    if not sys.platform.startswith("linux"):
        return 0, "非 Linux 平台（不支持 SO_REUSEPORT）"
    cores = available_cpus()
    if cores < 2:
        return 0, f"可用核数 {cores} < 2"
    limit = memory_limit_bytes()
    if limit is not None and limit < MIN_MEMORY_FOR_FRONTS:
        return 0, f"内存上限 {limit >> 20} MB < {MIN_MEMORY_FOR_FRONTS >> 20} MB"
    return min(MAX_FRONTS, cores - 1), \
        f"可用核数 {cores}，内存上限 {'无' if limit is None else limit >> 20}"
