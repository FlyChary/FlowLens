"""内存读数：优先 cgroup（有上限时），退回进程常驻集，全缺返回 0。

读取顺序（root 参数只给测试伪造目录用，生产一律默认 "/"）：
1. cgroup v2：上限看 memory.max、读数看 memory.current
2. cgroup v1：上限看 memory.limit_in_bytes、读数看 memory.usage_in_bytes
3. 进程常驻集：/proc/self/statm 第二列（常驻页数）× 系统页大小
4. （仅 Windows）上述全缺时，用 ctypes 调 psapi 的 GetProcessMemoryInfo 读
   本进程 WorkingSetSize（T8b）：让 Windows 开发机 /healthz 显示真实值、内存相关
   逻辑（降级控制器第 3/4 级）在本地也能真正被触发——此前 memstat 在 Windows 上
   恒返回 0，这些逻辑本地一次都没执行过。

只有当前进程所在的 cgroup 设置了内存上限时才采用 cgroup 读数，否则回退到进程
常驻集（statm）。原因：WSL 里 /sys/fs/cgroup 是整个 Linux 发行版（所有进程共享），
读到的是全部进程的总和（实测 427 MB，而本进程仅约 109 MB），降级控制器会因此误判；
目标 Docker 容器用 --memory 启动，一定带上限，cgroup 读数才准。

任何一步读不到都静默进入下一步，全部读不到时返回 0；本模块任何情况下都不抛
异常——它是 /stats 与 /healthz 的读数来源，读内存失败不该让查询接口报错。
"""

import ctypes
import os
import sys
from ctypes import c_ulong
from pathlib import Path

# cgroup v1 没有显式"无上限"标记，内核默认上限是 2^60 左右的巨大值；
# 读到不小于它的数视为没有设置上限。
_V1_NO_LIMIT = 1 << 60

# Windows 的 PROCESS_MEMORY_COUNTERS（ctypes 结构），首次用时才定义（见
# _process_memory_counters）；非 Windows 上保持 None、永不使用。
_PROCESS_MEMORY_COUNTERS = None


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


def _process_memory_counters():
    """Windows 的 PROCESS_MEMORY_COUNTERS 结构（ctypes），只在首次用时定义一次。

    顶层 import 不能直接定义它：ctypes.windll 只存在于 Windows，非 Windows 上
    导入本模块（生产与测试都会 import memstat）不能因此报错。延迟到真正调用
    （仅 Windows 全缺时）才定义，且缓存住不重复分配。
    """
    global _PROCESS_MEMORY_COUNTERS
    if _PROCESS_MEMORY_COUNTERS is None:
        class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", c_ulong),
                ("PageFaultCount", c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]
    return _PROCESS_MEMORY_COUNTERS


def _windows_working_set(platform: str = sys.platform) -> int:
    """Windows：本进程的 WorkingSetSize（字节）；任何异常都吞掉、返回 0。

    ctypes 调 psapi 的 GetProcessMemoryInfo。只有 cgroup 与 statm 都读不到时才用
    （即纯 Windows 开发机），让 /healthz 与降级控制器拿到真实内存而非恒 0。
    platform 参数只给测试伪造用（在非 Windows 上强制走这条路径）。
    """
    if platform != "win32":
        return 0
    try:
        k32 = ctypes.windll.kernel32
        # GetCurrentProcess 返回伪句柄（64 位 0xFFFF...FF）。必须声明 restype 为
        # c_void_p，否则 ctypes 按 32 位有符号 int 截断成 -1，调用静默失败返回 0。
        k32.GetCurrentProcess.restype = ctypes.c_void_p
        psapi = ctypes.windll.psapi
        # 同样要声明 argtypes：伪句柄 2^64-1 超出默认 C int 范围，不声明会在
        # 传参时抛 OverflowError。声明成 c_void_p 才能按 64 位传。
        psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, c_ulong]
        proc = k32.GetCurrentProcess()
        Ctr = _process_memory_counters()
        ctr = Ctr()
        ctr.cb = ctypes.sizeof(Ctr)
        ok = psapi.GetProcessMemoryInfo(proc, ctypes.byref(ctr), ctypes.sizeof(Ctr))
        return int(ctr.WorkingSetSize) if ok else 0
    except Exception:
        return 0


def memory_bytes(root: str = "/") -> int:
    """当前内存占用（字节）。有 cgroup 上限时用 cgroup 读数，否则用进程常驻集。

    读不到时：Windows 退回 GetProcessMemoryInfo 的 WorkingSetSize，仍读不到返回
    0；不抛异常。
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
    v = _proc_rss_bytes(root)
    if v:
        return v
    # 全缺（纯 Windows 开发机：既没有 cgroup 也没有 /proc）：读本进程
    # WorkingSetSize，让本地 /healthz 显示真实值（T8b）
    return _windows_working_set()
