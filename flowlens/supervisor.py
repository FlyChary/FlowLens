"""前端进程架构（P2d-1）：前端数的计算、共享状态与前端进程的管理。

引擎与前端之间通过 multiprocessing 的共享内存交换"降级状态"与"各前端的额度
占用"，不需要消息往返（第二节 SharedState）：
- 引擎写 level（降级级别）与 tight（第 3 级起收紧额度的标志）；
- 前端 i 只写自己的第 i 个槽位（在途/原始体的预留字节与拒绝计数）；
- 每槽只有一个写者，读者读到的是某一时刻的值即可，所有共享对象 lock=False。

前端进程由 FrontSupervisor 用 spawn 上下文拉起并监控（见第五节 5.4）。
"""

import logging
import os
import sys

from .ingest import Admission
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


class SharedState:
    """引擎与各前端之间的共享内存（引擎创建，作为 Process 参数传给 spawn 出的前端；
    spawn 下 sharedctypes 可以这样跨进程传递）。

    lock=False：每个槽位只有一个写者（引擎写 level/tight，前端 i 只写自己的第 i 个
    槽位），读者读到的是某一时刻的值即可，不需要锁。
    """

    def __init__(self, ctx, n: int):
        self.n = n
        self.level = ctx.Value("i", 0, lock=False)        # 引擎写：当前降级级别
        self.tight = ctx.Value("b", 0, lock=False)        # 引擎写：1 表示第 3 级起收紧额度
        self.pending = ctx.Array("q", n, lock=False)      # 前端 i 写：在途额度已预留的字节
        self.raw_pending = ctx.Array("q", n, lock=False)  # 前端 i 写：原始体额度已预留的字节
        self.ingest_rejected = ctx.Array("q", n, lock=False)  # 前端 i 写：在途额度拒绝累计
        self.raw_rejected = ctx.Array("q", n, lock=False)     # 前端 i 写：原始体额度拒绝累计

    def reset_slot(self, i: int) -> None:
        """前端 i 意外退出后，它的预留不会再释放，由引擎在重启它之前清零。拒绝计数保留。"""
        self.pending[i] = 0
        self.raw_pending[i] = 0


class DegradeView:
    """前端侧的降级视图：_ingest 与 _ingest_expect 只用到 reject_ingest。

    参考代码"需要确认"项的实测结论：DegradeController.reject_ingest 的条件
    确实是 level >= 4（degrade.py 的 reject_ingest 属性），按实际条件实现。
    不继承 DegradeController（那是有状态机、每秒被 Ticker 驱动的对象，前端进程
    里没有任何东西驱动它）；实测 aiohttp 3.14 对 AppKey 的取值不做类型检查
    （web_app.Application.__setitem__ 只查 key 是否 AppKey），放 DegradeView
    进 degrade_controller_key 不告警、不报错。
    """

    def __init__(self, shared: "SharedState"):
        self._shared = shared

    @property
    def level(self) -> int:
        return self._shared.level.value

    @property
    def reject_ingest(self) -> bool:
        return self._shared.level.value >= 4


class SlotAdmission(Admission):
    """前端本地的额度（继承 Ingest 的 Admission），同时把占用与拒绝次数镜像到
    共享数组的第 slot 个槽位，引擎侧（FrontsBudgetView / /healthz）据此汇总。

    镜像方式（参考代码给了两选一，选这个）：占用在 try_reserve/release 后写；
    拒绝计数改成只读属性、由基类在 try_reserve 拒绝时直接写共享数组。这样
    _ingest_expect 的 `budget.rejected += 1`（原始体额度在发请求体之前的拒绝，
    走不到任何请求处理函数的 finally）也能被镜像——若改成"请求结束时 finally 里
    同步 rejected"，expect 路径的拒绝就丢了。
    """

    def __init__(self, limit: int, pending_arr, rejected_arr, slot: int):
        # _r/_i 必须在 super().__init__ 之前就位：Admission.__init__ 会执行
        # self.rejected = 0，命中的是本类的属性 setter（写共享数组）
        self._p = pending_arr
        self._r = rejected_arr
        self._i = slot
        super().__init__(limit=limit)

    def try_reserve(self, n: int) -> bool:
        ok = super().try_reserve(n)
        self._p[self._i] = self.pending
        return ok

    def release(self, n: int) -> None:
        super().release(n)
        self._p[self._i] = self.pending

    @property
    def rejected(self) -> int:
        """拒绝计数镜像到共享数组（基类 try_reserve 失败时写），这里读回。"""
        return self._r[self._i]

    # 基类的 rejected 是普通字段（dataclass 字段默认 0），这里把它换成共享数组的
    # 读回：_ingest_expect 执行 `budget.rejected += 1` 时是"读属性 → 写属性"，
    # 需要一个可写的 setter（直接写共享槽位，见上）。
    @rejected.setter
    def rejected(self, value: int) -> None:
        self._r[self._i] = value


class FrontsBudgetView:
    """引擎侧：把各前端的同类额度汇总成一个 Admission 的样子。

    Ticker 读 occupancy（降级判定）、_make_degrade_change_hook 设 limit（第 3 级收紧/
    恢复）、_healthz 读 rejected。数据全部来自共享数组（各前端自己写自己的槽位）。

    normal_total / tight_total 是**全部前端合计**的额度；设置 limit 时不真正改每个
    前端的额度，只翻转共享的 tight 标志——每个前端在每次处理请求时按 tight 标志自行
    选 normal / tight（见 frontend.FrontIngestService.ingest），这样"第 3 级收紧额度"
    不需要引擎去改 N 个进程内的对象。
    """

    def __init__(self, shared: "SharedState", pending_arr, rejected_arr,
                 normal_total: int, tight_total: int):
        self._s = shared
        self._p = pending_arr
        self._r = rejected_arr
        self._normal = normal_total
        self._tight = tight_total

    @property
    def limit(self) -> int:
        """当前（合计）在途额度上限：tight 标志置位时为 tight_total，否则 normal_total。"""
        return self._tight if self._s.tight.value else self._normal

    @limit.setter
    def limit(self, value: int) -> None:
        # _make_degrade_change_hook 执行 admission.limit = tight/normal；这里只翻转
        # 共享 tight 标志，实际的每前端额度由前端按 tight 自行选择
        self._s.tight.value = 1 if value < self._normal else 0

    @property
    def pending(self) -> int:
        return sum(self._p)

    @property
    def occupancy(self) -> float:
        return self.pending / self.limit if self.limit else 0.0

    @property
    def rejected(self) -> int:
        return sum(self._r)
