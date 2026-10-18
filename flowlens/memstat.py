"""内存读数：原始读数（memory_current_bytes）与降级用的"工作集"（memory_bytes）。

两个公开读数：
- memory_current_bytes(root)：原始读数。有 cgroup 上限时读 cgroup
  （v2 memory.current / v1 memory.usage_in_bytes），无上限退回进程常驻集
  （/proc/self/statm 第二列 × 页大小），（仅 Windows）全缺时读本进程
  WorkingSetSize（T8b），全缺返回 0。供 /healthz 的 memory_current_bytes 展示，便于对照。
- memory_bytes(root)：工作集 = 原始读数减去不活跃文件缓存
  （v2 memory.stat 的 inactive_file / v1 的 total_inactive_file），降级判断与
  /healthz 的 memory_bytes 用它。这与 docker stats 显示一致，也是 Kubernetes
  判断驱逐的口径：不活跃的文件缓存在内存紧张时会被内核优先回收，不会导致进程
  OOM 被杀。读不到 memory.stat 时退回原始读数；无 cgroup 上限时直接用进程读数
  （没有文件缓存可扣）。

读取顺序（root 参数只给测试伪造目录用，生产一律默认 "/"）：
1. cgroup v2：上限看 memory.max、原始读数看 memory.current
2. cgroup v1：上限看 memory.limit_in_bytes、原始读数看 memory.usage_in_bytes
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


def _stat_field(path: Path, name: str) -> int | None:
    """读 cgroup 的 memory.stat / memory.stat 里某个字段的整数值。

    memory.stat 是每行 "键 值" 的文本（v1 是 "键 值" 或 "键 = 值"）。只读到
    前若干行即可（字段名按首字母分布，inactive_file 等在开头附近），读不到该
    字段返回 None。
    """
    try:
        with open(path, "rb") as f:
            data = f.read(8192)
    except OSError:
        return None
    for line in data.split(b"\n"):
        if not line:
            continue
        parts = line.split(None, 1)
        # v1 的 "key = value" 形式：键里可能带 "="
        key = parts[0].rstrip(b"=").decode("ascii", "ignore")
        if key == name and len(parts) > 1:
            try:
                return int(parts[1].split()[0])
            except (ValueError, IndexError):
                return None
    return None


def _cgroup_inactive_file_bytes(root: str) -> int | None:
    """当前 cgroup 的不活跃文件缓存字节数（用于扣出工作集）。

    v2 读 memory.stat 的 inactive_file；v1 读 memory.stat 的
    total_inactive_file。都读不到返回 None（工作集退回原始读数）。
    """
    p = Path(root) / "sys" / "fs" / "cgroup"
    v2 = _stat_field(p / "memory.stat", "inactive_file")
    if v2 is not None:
        return v2
    return _stat_field(p / "memory" / "memory.stat", "total_inactive_file")


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


def memory_current_bytes(root: str = "/") -> int:
    """原始内存读数（字节）：cgroup 占用、进程常驻集或 Windows WorkingSetSize。

    有 cgroup 上限时用 cgroup 读数（v2 memory.current 优先，v1
    memory.usage_in_bytes 次之）；无上限（或未识别出 cgroup）退回进程常驻集；
    全缺（纯 Windows 开发机）退回本进程 WorkingSetSize（T8b），仍读不到返回 0。
    不抛异常。/healthz 的 memory_current_bytes 字段用它，便于和工作集对照。
    """
    if memory_limit_bytes(root) is not None:
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


def memory_bytes(root: str = "/") -> int:
    """降级判断与 /healthz 用的"工作集"读数（字节）= 原始读数 − 不活跃文件缓存。

    有 cgroup 上限时，工作集 = cgroup 占用 − memory.stat 的 inactive_file
    （v1 为 total_inactive_file）：不活跃的文件缓存在内存紧张时会被内核优先
    回收，不会导致进程被杀，把它算进内存读数会让降级控制器虚高触发（T10b 实测容器
    --memory=256m：空闲 memory.current ≈ 169 MiB、工作集 ≈ 108 MiB；5 万/秒写入 30
    秒后 ≈ 248 MiB / ≈ 184 MiB，其中不活跃文件缓存约 60 MiB 基本恒定）。这与
    docker stats 的显示一致。读不到 memory.stat 时退回原始读数；无 cgroup 上限
    （进程读数本身不含可回收的文件缓存）直接返回原始读数。
    """
    cur = memory_current_bytes(root)
    if memory_limit_bytes(root) is None:
        return cur
    inactive = _cgroup_inactive_file_bytes(root)
    if inactive is None:
        return cur
    return max(0, cur - inactive)
