"""SSE 实时推送：GET /v1/topk/stream。

参数相同（by、window、k、interval）的订阅者属于同一个"订阅组"，共用一个后台生产
任务与同一份计算结果——计算量只随组数增长，不随订阅者数增长。

帧的时间按 interval 对齐到时间网格 T = ⌊now/interval⌋×interval，每一帧的内容等于
QueryService.topk(d, window, k, at=T) 的结果，即区间 [T − window, T)。同一组每秒
最多重新计算一次（interval 小于 1 秒时在同一秒内复用上一次的结果、只更新帧时刻），
否则几个"200 ms 间隔、600 s 窗口"的订阅就能把回放 CPU 占满。

每个订阅者只有一个"最新帧"位置：容量为 1 的 asyncio.Queue，满了先取出旧帧再放新帧，
被丢的帧数计入订阅者计数（背压不产生内存增长，只丢帧）。单帧写入超过写超时未完成的
订阅者被断开。

断线重连带 Last-Event-ID 时补发错过的帧（最多 30 帧），再无缝接上实时帧。补帧优先用
组里保留的最近 30 帧（生产者每产一帧就记一份 (帧时刻, payload)，上限 30、内存有界）：
同组订阅者的帧完全相同，不需要每个重连者各自重算；只有不在缓存里的帧才现场 topk
计算，并且全局串行化（同一时刻只有一个补帧计算在跑），网络抖动大量订阅者同时重连
时不会并发打满回放。补发与实时共用同一个单调游标 next_tid，id 连续、严格递增、
不重复、不重叠。

没有 Last-Event-ID 的订阅者加入已有组时，若组里已有最近一帧就立即补发它（最多一帧，
之后照常跟随实时帧），不用等下一个网格点。应用关闭时向每个订阅者队列放入结束标记，
pump 收到后立即返回，所有 SSE 处理函数及时退出（不等 aiohttp 的 60 秒强制取消）。
"""

import asyncio
import logging
from collections import deque

import orjson

from ..dims import Dim
from ..engine import Engine
from ..errors import Overloaded, TooOld
from ..query import QueryService, TopKAnswer
from ..timewin import LATENESS_MS

log = logging.getLogger(__name__)

# 断线重连最多补发的帧数（组里帧缓存的上限）
CATCHUP_MAX_FRAMES = 30
# 保留期检查的额外余量（秒）：与 engine._plan 一致（最新定稿秒约 now − 迟到上限）
_RETENTION_MARGIN_SEC = LATENESS_MS // 1000
# 默认上限（测试时可调小）
DEFAULT_MAX_GROUPS = 8
DEFAULT_MAX_SUBSCRIBERS = 256
DEFAULT_MIN_RECOMPUTE_SEC = 1.0   # 同一组每秒最多重新计算一次
DEFAULT_HEARTBEAT_SEC = 15.0      # 每 15 秒发一行 ": ping" 心跳
DEFAULT_WRITE_TIMEOUT_SEC = 5.0   # 单帧写入超过 5 秒未完成就断开该订阅者
# 第 1 级降级（SSE 降频）时，同一组的最小重算间隔从 1 秒放宽到 5 秒
SLOWDOWN_MIN_RECOMPUTE_SEC = 5.0


def topk_data(ans: TopKAnswer, *, degraded: bool = False,
              sample_weight: int = 1) -> dict:
    """把 TopKAnswer 序列化成 /v1/topk 响应同构的 dict（/v1/topk 与 SSE 共用）。

    SSE 的 data 字段必须与 /v1/topk 的响应体完全相同，因此两处都从这里取，不写两份。
    degraded / sample_weight 由调用方按当前降级状态传入（T8）：采样生效时（权重 > 1）
    额外带 sample_rate 字段（1 / 权重）。
    """
    def fmt(x: float):
        # 整数输出整数，否则保留两位小数（与 /v1/topk 一致）
        return int(x) if float(x).is_integer() else round(float(x), 2)

    data = {
        "window_ms": ans.window_ms,
        "from": ans.from_ms,
        "to": ans.to_ms,
        "by": str(ans.dim),
        "k": ans.k,
        "final": ans.final,
        "degraded": degraded,
        "unseen_upper": ans.unseen_upper,
        "items": [{"key": it.key, "count": fmt(it.count), "err": fmt(it.err)}
                  for it in ans.items],
    }
    if sample_weight > 1:
        data["sample_rate"] = 1.0 / sample_weight
    return data


def fmt_frame(tid: int, payload: dict) -> bytes:
    """一帧 SSE 报文的字节：id / event / data 三行加一个空行。"""
    return f"id: {tid}\nevent: topk\ndata: {orjson.dumps(payload).decode()}\n\n".encode()


def compute_next_tid(last_id_raw: str | None, now_ms: int, interval_ms: int,
                     window_ms: int, retention_ms: int) -> int:
    """算出订阅者要交付的起始帧时刻（游标），保证 [游标, ...) 的每帧都算得出、没重复。

    - 无 Last-Event-ID，或格式不对、晚于当前时间：从当前网格点开始（实时帧）。
    - 有合法 Last-Event-ID：从它**下一格**开始（已见过的 last_id 不重发），再向上
      对齐到 interval 网格（客户端换了 interval 时 last_id 可能不在当前网格上，
      对齐后补帧与实时帧才都落在网格上），并受两个下限约束、取最近者（游标只会更大）：
      * 最旧可算帧：其窗口 [t−window, t) 仍在保留期内（t ≥ now−retention−迟到上限
        +window，向上对齐到网格），否则该帧 topk 会抛 TooOld；口径与 engine._plan 一致
        （最新定稿秒约 now−迟到上限，实际能查到比 now−retention 更早的数据）；
      * 最近 CATCHUP_MAX_FRAMES 帧（以当前网格为基准，再早的忽略、直接补到当前）。
    返回值是 interval 的整数倍（帧时刻都在网格上）。
    """
    grid = (now_ms // interval_ms) * interval_ms
    if last_id_raw is None:
        return grid
    s = last_id_raw.strip()
    if not s.isdigit():
        return grid  # 格式不对（负数、小数、非数字）→ 从实时帧开始
    last_id = int(s)
    if last_id > now_ms:
        return grid  # 晚于当前时间 → 忽略
    # 已见过的 last_id 不重发，从其下一格开始；再向上对齐到网格（off-grid 时对齐）
    target = -((last_id + interval_ms) // -interval_ms) * interval_ms
    # 最旧可算帧：窗口 [t-window, t) 需在保留期 [now-retention-迟到上限, now) 内
    raw_min = now_ms - retention_ms - _RETENTION_MARGIN_SEC * 1000 + window_ms
    t_min_valid = -(-raw_min // interval_ms) * interval_ms  # 向上对齐到网格
    max_catchup_floor = grid - CATCHUP_MAX_FRAMES * interval_ms
    return max(target, t_min_valid, max_catchup_floor)


# 放进订阅者队列的结束标记：应用关闭时 pump 收到它立即返回，SSE 处理函数及时退出
STOP = object()


class SubscriptionGroup:
    """一个订阅组：相同参数（by、window、k、interval）共用一个生产任务与同一份结果。

    组里保留最近 CATCHUP_MAX_FRAMES 帧的 (帧时刻, payload)：同组订阅者的帧完全相同，
    补帧直接从这里取，重连者不必各自重算 topk。上限 30，内存有界。
    """

    def __init__(self, query_service: QueryService, engine: Engine, *,
                 by: Dim, window_ms: int, k: int, interval_ms: int,
                 min_recompute: float):
        self.qs = query_service
        self.engine = engine
        self.by = by
        self.window_ms = window_ms
        self.k = k
        self.interval_ms = interval_ms
        self.min_recompute = min_recompute
        # 降级控制器（T8）：第 1 级时最小重算间隔放宽（降频）；SSE 不暂停（只降频）
        self.controller = query_service.controller
        self.subscribers: dict[int, "Subscriber"] = {}
        self.task: asyncio.Task | None = None
        self.frames: deque[tuple[int, dict]] = deque(maxlen=CATCHUP_MAX_FRAMES)

    def recompute_interval_ms(self) -> float:
        """当前生效的最小重算间隔（毫秒）。第 1 级降级时从 1 秒放宽到 5 秒。"""
        if self.controller.sse_slowdown:
            return SLOWDOWN_MIN_RECOMPUTE_SEC * 1000.0
        return self.min_recompute * 1000.0

    def grid(self) -> int:
        """当前时刻对齐到 interval 网格上的帧时刻 T = ⌊now/interval⌋×interval。"""
        return (self.engine.clock.now_ms() // self.interval_ms) * self.interval_ms

    async def producer(self) -> None:
        """网格步进生产者：每到一个网格点产一帧，同一组每秒最多重新计算一次。

        计算只在"本帧时刻距上次计算帧时刻 ≥ min_recompute"时真正调用 topk，否则复用
        上一次的结果（interval 小于 1 秒时同一秒内的多个网格点共享同一份 payload、只
        更新帧时刻）。按**帧时刻**（而非墙钟）判断节流：interval 大于等于 1 秒时每帧
        间隔都 ≥ min_recompute，保证每一帧都重新计算（data.to 等于帧 id）；interval 小
        于 1 秒时同一秒的多个网格点复用一份计算。每产出一帧先追加进组的帧缓存（供补帧
        不重算、新订阅者立即收帧用），再交给组里每个订阅者的最新帧队列。被取消（最后一
        个订阅者离开或应用关闭）时干净退出。
        """
        interval = self.interval_ms
        payload: dict | None = None
        last_compute_g: int | None = None  # 上次真正计算的那一帧的帧时刻
        g = self.grid()
        while True:
            # 最小重算间隔在运行时可变：第 1 级降级时放宽到 5 秒（降频，帧照常推）
            if payload is None or (g - last_compute_g) >= self.recompute_interval_ms():
                try:
                    ans = await self.qs.topk(self.by, self.window_ms, self.k, g,
                                             from_stream=True)
                    # 降级状态与采样权重随帧带上（degraded、采样时 sample_rate）
                    payload = topk_data(ans, degraded=self.controller.degraded,
                                        sample_weight=self.controller.sample_weight)
                    last_compute_g = g
                except Exception:
                    # 单帧计算失败（例如该帧区间恰好早于保留期）：记日志、跳过这一帧，
                    # 不让后台任务因一次失败就永久退出（与 Ticker 单拍出错的处理一致）。
                    log.exception("SSE 组 %s 帧 %s 计算失败，跳过", self.by, g)
            if payload is not None:
                self.frames.append((g, payload))
                for sub in list(self.subscribers.values()):
                    sub.offer(g, payload)
            g += interval
            # 睡到下一个网格点；按绝对时刻对齐，避免误差累积漂移
            await asyncio.sleep(max(0.0, (g - self.engine.clock.now_ms()) / 1000.0))


class Subscriber:
    """一个订阅者：最新帧队列（容量 1）+ 被丢弃帧计数。"""

    __slots__ = ("queue", "dropped")

    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self.dropped = 0

    def offer(self, tid: int, payload: dict) -> None:
        """放入一帧；队列满了先取出旧帧（丢弃计数 +1）再放新帧。"""
        if self.queue.full():
            try:
                self.queue.get_nowait()
                self.dropped += 1
            except asyncio.QueueEmpty:
                pass
        try:
            self.queue.put_nowait((tid, payload))
        except asyncio.QueueFull:
            self.dropped += 1  # 兜底（满则丢）

    def stop(self) -> None:
        """应用关闭：放入结束标记，pump 收到后立即返回。"""
        try:
            self.queue.put_nowait(STOP)
        except asyncio.QueueFull:
            # 队列里还有未交付的一帧：丢弃它（计丢弃数）再放标记。
            # 应用正在关闭，这一帧不必再交付，丢弃计数不再被 leave 结算也没关系。
            try:
                self.queue.get_nowait()
                self.dropped += 1
            except asyncio.QueueEmpty:
                pass
            self.queue.put_nowait(STOP)


class StreamManager:
    """SSE 订阅管理器：订阅组与订阅者的注册、上限与生命周期。挂在 app 上（web.AppKey）。"""

    def __init__(self, query_service: QueryService, *,
                 max_groups: int = DEFAULT_MAX_GROUPS,
                 max_subscribers: int = DEFAULT_MAX_SUBSCRIBERS,
                 min_recompute: float = DEFAULT_MIN_RECOMPUTE_SEC,
                 heartbeat_period: float = DEFAULT_HEARTBEAT_SEC,
                 write_timeout: float = DEFAULT_WRITE_TIMEOUT_SEC,
                 compute_lock: "asyncio.Semaphore | None" = None):
        self.qs = query_service
        self.engine = query_service.engine
        self.max_groups = max_groups
        self.max_subscribers = max_subscribers
        self.min_recompute = min_recompute
        self.heartbeat_period = heartbeat_period
        self.write_timeout = write_timeout
        self.groups: dict[tuple, SubscriptionGroup] = {}
        self.total_dropped = 0
        # 补帧"现场计算"的全局串行锁：多个订阅者（甚至不同组）同时重连、都要现场算
        # 漏掉的帧时，同一时刻只有一个在算，避免并发打满回放。3.11 的信号量在使用
        # 时才绑定事件循环，可在任意时刻创建；测试可注入自己的信号量。
        self.compute_lock: "asyncio.Semaphore" = compute_lock or asyncio.Semaphore(1)

    # ---- 上限（在开始发送流之前检查，失败抛 Overloaded → 503 + Retry-After）----
    def check_limits(self) -> None:
        # 第 3 级降级：收紧内存，拒绝新的 SSE 订阅（已有订阅保持）
        if self.qs.controller.tighten_budgets:
            raise Overloaded("降级期间暂停新的 SSE 订阅", retry_after=1)
        if len(self.groups) >= self.max_groups:
            raise Overloaded("订阅组数已达上限", retry_after=1)
        if self.subscriber_count() >= self.max_subscribers:
            raise Overloaded("SSE 连接数已达上限", retry_after=1)

    def subscriber_count(self) -> int:
        return sum(len(g.subscribers) for g in self.groups.values())

    def stats(self) -> dict:
        """供 /healthz 的 sse 字段：当前组数、订阅者数、累计丢弃帧数。"""
        return {
            "groups": len(self.groups),
            "subscribers": self.subscriber_count(),
            "dropped_frames": self.total_dropped,
        }

    def subscribe(self, *, by: Dim, window_ms: int, k: int, interval_ms: int,
                  raw) -> tuple[SubscriptionGroup, Subscriber]:
        """登记一个新订阅者；超上限抛 Overloaded。返回（组，订阅者）。

        同参数组已存在时复用，否则新建并启动生产任务。raw 用 id 作下标区分订阅者。
        """
        self.check_limits()
        key = (by, window_ms, k, interval_ms)
        group = self.groups.get(key)
        if group is None:
            group = SubscriptionGroup(
                self.qs, self.engine, by=by, window_ms=window_ms, k=k,
                interval_ms=interval_ms, min_recompute=self.min_recompute)
            self.groups[key] = group
            group.task = asyncio.create_task(group.producer())
        sub = Subscriber()
        group.subscribers[id(raw)] = sub
        return group, sub

    def leave(self, key: tuple, raw) -> None:
        """订阅者离开（断开或写完）：从组里移除；最后一个离开时结束组的生产任务。"""
        group = self.groups.get(key)
        if group is None:
            return
        sub = group.subscribers.pop(id(raw), None)
        if sub is not None:
            self.total_dropped += sub.dropped
        if not group.subscribers:
            self.groups.pop(key, None)
            if group.task is not None:
                group.task.cancel()

    async def close(self) -> None:
        """应用关闭：立即结束所有 SSE 流，再结束所有订阅组的生产任务。

        先向每个订阅者队列放入结束标记：pump 收到后自行返回，SSE 处理函数正常
        退出（不靠 aiohttp 关连接时取消、也不用等它的 60 秒强制取消窗口），
        finally 里的 leave() 结算订阅者。之后取消生产任务（帧缓存不再需要）。
        先取快照再改动，避免与"连接断开时 handler 的 finally 调 leave()"并发
        改动 self.groups（一边迭代一边被 pop 会 RuntimeError）。
        """
        groups = list(self.groups.values())
        for group in groups:
            for sub in list(group.subscribers.values()):
                sub.stop()
        for group in groups:
            if group.task is not None and not group.task.done():
                group.task.cancel()
        for group in groups:
            if group.task is not None and not group.task.done():
                try:
                    await group.task
                except (asyncio.CancelledError, Exception):
                    pass


async def _timed_write(response, data: bytes, timeout: float) -> None:
    """带超时的写：超过 timeout 未完成抛 asyncio.TimeoutError（调用方断开该订阅者）。"""
    await asyncio.wait_for(response.write(data), timeout=timeout)


async def _catchup(response, group: SubscriptionGroup, cursor: int, end_tid: int,
                   write_timeout: float, compute_lock: "asyncio.Semaphore") -> int:
    """补发 [cursor, end_tid) 的帧，返回交付后的新游标。

    优先用组里保留的最近帧（group.frames，生产者已算好、同组共享）——补帧不重算，
    网络抖动大量订阅者同时重连也不会各自重算；不在缓存里的帧（通常只有最老的那一
    帧，被 30 帧缓存挤出）才现场 topk 计算，并用 compute_lock 串行化，避免并发打满
    回放。
    """
    interval = group.interval_ms
    cached = {t: p for (t, p) in group.frames if cursor <= t < end_tid}
    t = cursor
    while t < end_tid:
        await asyncio.sleep(0)  # 补发帧间让出事件循环
        if t in cached:
            payload = cached[t]
        else:
            # 现场计算（全局串行）。补发比 compute_next_tid 晚 1~2s，保留期下限随
            # now 上移，最老的补发帧可能刚好掉到下限之外 → TooOld：跳过该帧（与原
            # 行为一致），游标照常前进。
            async with compute_lock:  # 现场计算全局串行
                try:
                    ans = await group.qs.topk(group.by, group.window_ms, group.k, t,
                                              from_stream=True)
                except TooOld:
                    t += interval
                    continue
                payload = topk_data(ans, degraded=group.controller.degraded,
                                    sample_weight=group.controller.sample_weight)
        await _timed_write(response, fmt_frame(t, payload), write_timeout)
        t += interval
    # 游标不后退：已追平（end_tid < cursor）时保持 cursor
    return max(cursor, end_tid)


async def pump(response, sub: Subscriber, group: SubscriptionGroup, *,
              next_tid: int, immediate: bool, heartbeat: float, write_timeout: float,
              compute_lock: "asyncio.Semaphore") -> None:
    """把一个订阅者的帧（含心跳）持续写入 response，保证 id 连续、严格递增。

    新订阅者（immediate=True，没有 Last-Event-ID）：若组里已有最近一帧，先立即补发
    它（最多一帧），不用等下一个网格点（interval=60s 时最多省 60s），之后照常跟随。
    断线重连：首个实时帧到来前，把 [游标, 首帧) 逐帧补发（优先用组缓存，不重算），
    再交付首帧——补发与实时无缝衔接，不重不漏。之后逐帧跟随队列；某帧比游标跳得远
    说明中间的帧被慢客户端丢弃，直接跳到该帧（丢弃已在 offer 计数）。队列在 heartbeat
    内没有新帧时发 ": ping" 心跳。收到 STOP（应用关闭）立即返回，让 SSE 处理函数
    及时退出。单帧写入超过 write_timeout 未完成时抛 asyncio.TimeoutError，由调用方
    断开；客户端断开时 aiohttp 取消本任务（CancelledError），调用方负责 leave。
    """
    interval = group.interval_ms
    cursor = next_tid  # 下一个要交付的帧时刻（游标），随交付单调前进
    # 新订阅者：组里已有最近一帧就立即补发它，游标推到它之后（id 仍连续、严格递增）
    if immediate and group.frames:
        lid, lpay = group.frames[-1]
        await _timed_write(response, fmt_frame(lid, lpay), write_timeout)
        cursor = lid + interval
    catchup_done = False  # 首个实时帧前做一次补发（断线重连的缺口）；新订阅者无缺口
    while True:
        try:
            item = await asyncio.wait_for(sub.queue.get(), timeout=heartbeat)
        except asyncio.TimeoutError:
            await _timed_write(response, b": ping\n\n", write_timeout)
            continue
        if item is STOP:  # 应用关闭：立即结束该流，让 SSE 处理函数及时退出
            return
        tid, payload = item
        if not catchup_done:
            cursor = await _catchup(response, group, cursor, tid, write_timeout,
                                    compute_lock)
            catchup_done = True
        # 实时帧：tid < cursor（队列积压的旧帧，已交付/已丢弃）跳过；否则交付该帧，
        # 中间被丢的帧直接跳过（丢弃）。
        if tid >= cursor:
            await _timed_write(response, fmt_frame(tid, payload), write_timeout)
            cursor = tid + interval
