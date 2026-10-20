"""前端进程架构（P2d-1）：前端数的计算、共享状态与前端进程的管理。

引擎与前端之间通过 multiprocessing 的共享内存交换"降级状态"与"各前端的额度
占用"，不需要消息往返（第二节 SharedState）：
- 引擎写 level（降级级别）与 tight（第 3 级起收紧额度的标志）；
- 前端 i 只写自己的第 i 个槽位（在途/原始体的预留字节与拒绝计数）；
- 每槽只有一个写者，读者读到的是某一时刻的值即可，所有共享对象 lock=False。

前端进程由 FrontSupervisor 用 spawn 上下文拉起并监控（第五节 5.4）：意外退出的
前端由 monitor 任务在重启前清零它的额度槽位（它的预留不会再释放，不清零会占死
引擎侧的合计额度）。
"""

import asyncio
import logging
import multiprocessing as mp
import os
import sys
import time

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
        # 各前端额度的**实际合计**（普通属性、只给引擎进程用；各前端进程不需要，
        # 每前端的额度作为 Process 参数单独传）。第 3 级起"正常/收紧"两种口径的
        # 合计都要有——引擎侧视图（FrontsBudgetView）按 tight 标志选口径。
        self.adm_total = 0
        self.adm_tight_total = 0
        self.raw_total = 0
        self.raw_tight_total = 0

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


class RawSlotAdmission(SlotAdmission):
    """前端原始请求体额度：limit 随共享 tight 标志动态（第 3 级起用收紧口径，
    与单进程模式 _make_degrade_change_hook 把 raw_budget.limit 从 8 MB 降到
    4 MB 的语义一致）。_ingest_expect（发请求体前的 503 判断）与 _read_raw_body
    每次使用都现读 limit，所以动态 limit 天然生效，不需要改 app.py。

    参考代码骨架在 create_front_app 里放的是普通 SlotAdmission（limit 恒为
    raw_normal），那会让第 3 级收紧对原始体额度不生效——按单进程语义补上。
    """

    def __init__(self, normal: int, tight: int, pending_arr, rejected_arr,
                 slot: int, shared: "SharedState"):
        self._s = shared
        self._tight_limit = tight
        self._normal_limit = normal
        super().__init__(normal, pending_arr, rejected_arr, slot)

    @property
    def limit(self) -> int:
        return self._tight_limit if self._s.tight.value else self._normal_limit

    @limit.setter
    def limit(self, value: int) -> None:
        self._normal_limit = value


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


def per_front_limit(total: int, n: int, floor: int) -> int:
    """总额度按前端数平均，但每个前端的正常额度不能低于单请求上限 floor，
    否则一个合法的大请求（最大 floor 字节）永远预留不到。"""
    return max(total // n, floor)


class FrontSupervisor:
    """前端进程的启动与监控（第五节 5.4）：spawn 拉起 n 个前端，monitor 每秒检查，
    意外退出的先清零额度槽位再重新拉起；stop 在停机时 terminate→kill。

    每前端的额度（正常/收紧、在途/原始体）在这里按 per_front_limit 算好，作为
    Process 参数传给前端（spawn 子进程不继承主进程内存，但 sharedctypes 与基本
    类型可以 pickle 传递）。因为有 floor 下限，前端数较多时**合计**会略高于设定值
    （例如在途 16 MB ÷ 4 前端 < 4 MB 时，每前端抬到 4 MB、合计 16 MB 不变；
    16 MB ÷ 6 前端 < 4 MB 时每前端 4 MB、合计 24 MB）——启动日志与 P2-notes 记录
    实际合计。
    """

    def __init__(self, n: int, host: str, port: int, ingest_sock: str, query_sock: str,
                 lingering: float,
                 adm_total: int, adm_tight_total: int,
                 raw_total: int, raw_tight_total: int,
                 adm_floor: int, raw_floor: int):
        self.ctx = mp.get_context("spawn")
        self.shared = SharedState(self.ctx, n)
        self.n = n
        self._args = (host, port, ingest_sock, query_sock)
        self._lingering = lingering
        self.procs: list = [None] * n
        self._stopping = False
        # 每前端额度（第 4.3 节）：正常 = per_front_limit(合计, n, 单请求上限)，
        # 收紧 = 收紧合计 // n（不设下限——降级期间"单个请求上限暂时降低"的语义，
        # 与单进程模式一致）
        self.adm_normal = per_front_limit(adm_total, n, adm_floor)
        self.adm_tight = adm_tight_total // n
        self.raw_normal = per_front_limit(raw_total, n, raw_floor)
        self.raw_tight = raw_tight_total // n
        # 各前端的**实际合计**（供 create_app 建 FrontsBudgetView 与启动日志）。
        # 存在 shared 上（普通属性，不进 pickle、只给引擎进程用；各前端进程拿到的是
        # 同一 sharedctypes 但不需要这些标量）。
        self.shared.adm_total = self.adm_normal * n
        self.shared.adm_tight_total = self.adm_tight * n
        self.shared.raw_total = self.raw_normal * n
        self.shared.raw_tight_total = self.raw_tight * n

    def start_all(self) -> None:
        for i in range(self.n):
            self._start(i)

    def _start(self, i: int) -> None:
        from .frontend import front_main
        p = self.ctx.Process(
            target=front_main, name=f"flowlens-front{i}",
            args=(i, *self._args, self.shared,
                  self.adm_normal, self.adm_tight,
                  self.raw_normal, self.raw_tight, self._lingering))
        p.start()
        self.procs[i] = p

    async def monitor(self) -> None:
        """每秒检查一次：意外退出的前端清零它的预留（它不会再释放，不清零会占死
        引擎侧的合计额度），再重新拉起。"""
        while not self._stopping:
            await asyncio.sleep(1)
            for i, p in enumerate(self.procs):
                if p is not None and not p.is_alive() and not self._stopping:
                    log.warning("前端 %d 退出（exitcode=%s），重新启动", i, p.exitcode)
                    self.shared.reset_slot(i)
                    self._start(i)

    def stop(self, timeout: float = 3.0) -> None:
        self._stopping = True
        for p in self.procs:
            if p is not None and p.is_alive():
                p.terminate()                       # SIGTERM
        deadline = time.monotonic() + timeout
        for p in self.procs:
            if p is not None:
                p.join(max(0.0, deadline - time.monotonic()))
                if p.is_alive():
                    p.kill()
                    p.join(1)
