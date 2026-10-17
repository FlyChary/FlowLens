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

断线重连带 Last-Event-ID 时补发错过的帧（最多 30 帧），再无缝接上实时帧。补发与实时
共用同一个单调游标 next_tid：补发把 [next_tid, 首帧) 逐帧用 topk 算出来，之后逐帧
跟随队列，因此 id 连续、严格递增、不重复、不重叠。
"""

import asyncio
import logging

import orjson

from ..dims import Dim
from ..engine import Engine
from ..errors import Overloaded, TooOld
from ..query import QueryService, TopKAnswer

log = logging.getLogger(__name__)

# 断线重连最多补发的帧数
CATCHUP_MAX_FRAMES = 30
# 默认上限（测试时可调小）
DEFAULT_MAX_GROUPS = 8
DEFAULT_MAX_SUBSCRIBERS = 256
DEFAULT_MIN_RECOMPUTE_SEC = 1.0   # 同一组每秒最多重新计算一次
DEFAULT_HEARTBEAT_SEC = 15.0      # 每 15 秒发一行 ": ping" 心跳
DEFAULT_WRITE_TIMEOUT_SEC = 5.0   # 单帧写入超过 5 秒未完成就断开该订阅者


def topk_data(ans: TopKAnswer) -> dict:
    """把 TopKAnswer 序列化成 /v1/topk 响应同构的 dict（/v1/topk 与 SSE 共用）。

    SSE 的 data 字段必须与 /v1/topk 的响应体完全相同，因此两处都从这里取，不写两份。
    """
    def fmt(x: float):
        # 整数输出整数，否则保留两位小数（与 /v1/topk 一致）
        return int(x) if float(x).is_integer() else round(float(x), 2)

    return {
        "window_ms": ans.window_ms,
        "from": ans.from_ms,
        "to": ans.to_ms,
        "by": str(ans.dim),
        "k": ans.k,
        "final": ans.final,
        "degraded": False,  # T8 降级控制器接入后替换
        "unseen_upper": ans.unseen_upper,
        "items": [{"key": it.key, "count": fmt(it.count), "err": fmt(it.err)}
                  for it in ans.items],
    }


def fmt_frame(tid: int, payload: dict) -> bytes:
    """一帧 SSE 报文的字节：id / event / data 三行加一个空行。"""
    return f"id: {tid}\nevent: topk\ndata: {orjson.dumps(payload).decode()}\n\n".encode()


def compute_next_tid(last_id_raw: str | None, now_ms: int, interval_ms: int,
                     window_ms: int, retention_ms: int) -> int:
    """算出订阅者要交付的起始帧时刻（游标），保证 [游标, ...) 的每帧都算得出、没重复。

    - 无 Last-Event-ID，或格式不对、晚于当前时间：从当前网格点开始（实时帧）。
    - 有合法 Last-Event-ID：从它**下一格**开始（已见过的 last_id 不重发），受两个下限
      约束、取最近者（游标只会更大）：
      * 最旧可算帧：其窗口 [t−window, t) 仍在保留期内（t ≥ now−retention+window，
        向上对齐到网格），否则该帧 topk 会抛 TooOld；
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
    target = last_id + interval_ms  # 已见过的 last_id 不重发，从其下一格开始
    # 最旧可算帧：窗口 [t-window, t) 需在保留期 [now-retention, now) 内
    raw_min = now_ms - retention_ms + window_ms
    t_min_valid = -(-raw_min // interval_ms) * interval_ms  # 向上对齐到网格
    max_catchup_floor = grid - CATCHUP_MAX_FRAMES * interval_ms
    return max(target, t_min_valid, max_catchup_floor)


class SubscriptionGroup:
    """一个订阅组：相同参数（by、window、k、interval）共用一个生产任务与同一份结果。"""

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
        self.subscribers: dict[int, "Subscriber"] = {}
        self.task: asyncio.Task | None = None

    def grid(self) -> int:
        """当前时刻对齐到 interval 网格上的帧时刻 T = ⌊now/interval⌋×interval。"""
        return (self.engine.clock.now_ms() // self.interval_ms) * self.interval_ms

    async def producer(self) -> None:
        """网格步进生产者：每到一个网格点产一帧，同一组每秒最多重新计算一次。

        计算只在"距上次计算 ≥ min_recompute"时真正调用 topk，否则复用上一次的结果
        （interval 小于 1 秒时同一秒内的多个网格点共享同一份 payload、只更新帧时刻）。
        产出的 (帧时刻, payload) 交给组里每个订阅者的最新帧队列。被取消（最后一个
        订阅者离开或应用关闭）时干净退出。
        """
        interval = self.interval_ms
        payload: dict | None = None
        last_compute: int | None = None
        g = self.grid()
        while True:
            t0 = self.engine.clock.now_ms()
            if payload is None or (t0 - last_compute) >= self.min_recompute * 1000:
                try:
                    ans = await self.qs.topk(self.by, self.window_ms, self.k, g)
                    payload = topk_data(ans)
                    last_compute = t0
                except Exception:
                    # 单帧计算失败（例如该帧区间恰好早于保留期）：记日志、跳过这一帧，
                    # 不让后台任务因一次失败就永久退出（与 Ticker 单拍出错的处理一致）。
                    log.exception("SSE 组 %s 帧 %s 计算失败，跳过", self.by, g)
            if payload is not None:
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


class StreamManager:
    """SSE 订阅管理器：订阅组与订阅者的注册、上限与生命周期。挂在 app 上（web.AppKey）。"""

    def __init__(self, query_service: QueryService, *,
                 max_groups: int = DEFAULT_MAX_GROUPS,
                 max_subscribers: int = DEFAULT_MAX_SUBSCRIBERS,
                 min_recompute: float = DEFAULT_MIN_RECOMPUTE_SEC,
                 heartbeat_period: float = DEFAULT_HEARTBEAT_SEC,
                 write_timeout: float = DEFAULT_WRITE_TIMEOUT_SEC):
        self.qs = query_service
        self.engine = query_service.engine
        self.max_groups = max_groups
        self.max_subscribers = max_subscribers
        self.min_recompute = min_recompute
        self.heartbeat_period = heartbeat_period
        self.write_timeout = write_timeout
        self.groups: dict[tuple, SubscriptionGroup] = {}
        self.total_dropped = 0

    # ---- 上限（在开始发送流之前检查，失败抛 Overloaded → 503 + Retry-After）----
    def check_limits(self) -> None:
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
        """应用关闭：结束所有订阅组的生产任务。

        先取快照再取消/等待，避免与"连接断开时 handler 的 finally 调 leave()"并发
        改动 self.groups（一边迭代一边被 pop 会 RuntimeError）。
        """
        groups = list(self.groups.values())
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


async def pump(response, sub: Subscriber, group: SubscriptionGroup, *,
              next_tid: int, heartbeat: float, write_timeout: float) -> None:
    """把一个订阅者的帧（含心跳）持续写入 response，保证 id 连续、严格递增。

    维护单调游标 next_tid（要交付的下一帧时刻）：
    - 首个实时帧到来时，先把 [next_tid, 首帧) 逐帧用 topk 补发出来（断线重连的补帧，
      帧间让出事件循环；终态区间命中缓存），再交付首帧——这样补发与实时无缝衔接，
      不重不漏；
    - 之后逐帧跟随队列交付。若某帧比 next_tid 跳得远，说明中间的帧在队列里被丢弃
      （慢客户端），直接跳到该帧、不重算（丢帧已在 Subscriber.offer 计数）。
    - 队列在 heartbeat 内没有新帧时发一行 ": ping" 心跳。
    单帧写入超过 write_timeout 未完成时抛 asyncio.TimeoutError，由调用方断开。
    客户端断开时 aiohttp 取消本任务（CancelledError），调用方负责 leave。
    """
    interval = group.interval_ms
    cursor = next_tid  # 下一个要交付的帧时刻（游标），随交付单调前进
    initial = True     # 只有首帧做补发（断线重连的缺口）；之后的实时缺口一律丢弃、不重算
    while True:
        try:
            tid, payload = await asyncio.wait_for(sub.queue.get(), timeout=heartbeat)
        except asyncio.TimeoutError:
            await _timed_write(response, b": ping\n\n", write_timeout)
            continue
        if initial:
            # 首帧：补发 [cursor, tid) 的缺口（断线重连）。cursor 已含 Last-Event-ID 的
            # "下一格"下限，已见过的帧不会重发；补发与首帧 id 连续、严格递增、不重不漏。
            # 缺口受 CATCHUP_MAX_FRAMES 约束（compute_next_tid 已保证），TooOld 的旧帧跳过。
            t = cursor
            guard = 0
            while t < tid and guard <= CATCHUP_MAX_FRAMES + 4:
                guard += 1
                await asyncio.sleep(0)  # 补发帧间让出事件循环
                try:
                    ans = await group.qs.topk(group.by, group.window_ms, group.k, t)
                    await _timed_write(response, fmt_frame(t, topk_data(ans)), write_timeout)
                except TooOld:
                    pass  # 这一帧太旧（早于保留期）：忽略，继续往后
                t += interval
            # 客户端已追平（tid < cursor）时游标不后退：保留"下一格"下限，跳过旧帧
            cursor = max(cursor, tid)
            initial = False
        # 实时帧：只交付最新帧。tid < cursor（队列里积压着游标之前的旧帧，已交付/已丢弃）
        # 时跳过不重发；正常/落后时（tid ≥ cursor）交付该帧，中间被丢的帧直接跳过（丢弃）。
        if tid >= cursor:
            await _timed_write(response, fmt_frame(tid, payload), write_timeout)
            cursor = tid + interval
