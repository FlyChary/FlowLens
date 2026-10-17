"""后台每秒任务：推进环与去重、刷新默认窗口快照。

放在独立模块而不是 ingest.py：Ticker 是长生命周期的后台驱动（每秒一拍），
ingest.py 是一次性请求的处理，两者职责不同，合在一个模块里会让"请求处理"混进
一个永不结束的任务。
"""

import asyncio
import logging

from .engine import Engine

log = logging.getLogger(__name__)


class Ticker:
    """按固定周期执行 engine.advance() + refresh_snapshot_async()。

    记录每次 tick 的耗时，以及实际开始时间相对预定时间的延迟（降级控制器 T8 要用，
    用属性对外提供）。单拍抛普通异常时记日志、计入 error_count 并继续下一拍，
    不让后台任务因一次失败就永久退出。
    """

    def __init__(self, engine: Engine, period: float = 1.0):
        self.engine = engine
        self.period = period
        self.tick_count = 0              # 已完成的 tick 次数
        self.error_count = 0             # tick 抛异常的累计次数
        self.last_tick_duration_ms = 0.0  # 最近一次 tick（advance + 快照）耗时
        self.last_tick_delay_ms = 0.0     # 最近一次 tick 实际开始相对预定时间的延迟
        self._stop = asyncio.Event()

    def stop(self) -> None:
        """请求停止。run() 会在当前一拍结束后干净退出。"""
        self._stop.set()

    async def run(self) -> None:
        """循环执行直到 stop()。被取消时干净退出（不吞 CancelledError）。

        按绝对时刻调度：每拍睡到"预定时刻"，而不是"工作完再睡 period"，这样一拍
        的耗时不会累积成越来越晚的漂移（只要单拍耗时 < period）。
        """
        loop = asyncio.get_running_loop()
        next_at = loop.time()  # 第一拍立即执行
        while not self._stop.is_set():
            start = loop.time()
            # 延迟 = 实际开始相对预定时刻（next_at）的偏移；早于预定为负
            self.last_tick_delay_ms = (start - next_at) * 1000.0
            try:
                t0 = loop.time()
                self.engine.advance()
                await asyncio.sleep(0)
                await self.engine.refresh_snapshot_async()
                self.last_tick_duration_ms = (loop.time() - t0) * 1000.0
                self.tick_count += 1
            except Exception:
                # 单拍失败不退出后台任务：记完整异常、计数，继续下一拍。
                # 只捕获 Exception——CancelledError（3.11+ 属 BaseException）照常向上传播
                self.error_count += 1
                log.exception("tick 失败，继续下一拍")
            if self._stop.is_set():
                break
            next_at += self.period
            await self._wait_until(next_at)

    async def _wait_until(self, at: float) -> None:
        """睡到绝对时刻 at（已过期则只让出一次）；期间若 stop() 被调用立即返回。"""
        delay = at - asyncio.get_running_loop().time()
        if self._stop.is_set() or delay <= 0:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
