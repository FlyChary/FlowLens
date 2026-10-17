"""时间相关的常量与规则：watermark、可接收范围、定稿判断、区间拆分。

所有时间都是 epoch 毫秒；秒号 = ts // 1000（Python 的 // 对负数也是向下取整）。
"""

import time
from dataclasses import dataclass, field

import numpy as np

LATENESS_MS = 60_000  # 允许迟到不超过一个窗口
FUTURE_SKEW_MS = 5_000  # 时钟偏差容差
WINDOW_SEC = 60
RING_SLOTS = 66  # 60 秒窗口 + 当前秒 + 5 秒未来容差
RETENTION_SEC = 600

ACCEPT, LATE, FUTURE = 0, 1, 2


class Clock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


class FakeClock(Clock):
    """测试用的假时钟。"""

    def __init__(self, ms: int):
        self.ms = ms

    def now_ms(self) -> int:
        return self.ms

    def advance(self, d: int) -> None:
        self.ms += d


def sec(ts_ms: int) -> int:
    """ 下取整返回秒数 """
    return ts_ms // 1000


def watermark(now_ms: int) -> int:
    """早于它的事件视为迟到。"""
    return now_ms - LATENESS_MS


def classify(ts_ms: np.ndarray, now_ms: int) -> np.ndarray:
    """逐条判断事件时间：ACCEPT / LATE / FUTURE。恰好等于 watermark 的被接收。"""
    out = np.full(ts_ms.shape, ACCEPT, dtype=np.int8)
    out[ts_ms < watermark(now_ms)] = LATE
    out[ts_ms > now_ms + FUTURE_SKEW_MS] = FUTURE
    return out


def finalizable(s: int, now_ms: int) -> bool:
    """秒桶 s 是否已不可能再收到事件（整秒早于 watermark）。"""
    return (s + 1) * 1000 <= watermark(now_ms)


@dataclass(frozen=True)
class Edge:
    sec: int
    frac: float


@dataclass(frozen=True)
class Range:
    """
    毫秒区间拆分后的结果：整秒部分 [start, stop)，以及两端不满一秒的边缘秒。
    此处返回绝对秒数，与Unix epoch：1970 年 1 月 1 日 00:00:00 UTC 的秒差
    """

    start: int = 0
    stop: int = 0
    edges: tuple[Edge, ...] = field(default_factory=tuple)


def split_range(from_ms: int, to_ms: int) -> Range:
    """把 [from_ms, to_ms) 拆成整秒与边缘秒。topk 回放、stats、SSE 补帧都用它，不要另写。"""
    if to_ms <= from_ms:
        return Range()
    # 区间在1秒内
    if sec(from_ms) == sec(to_ms - 1) and (from_ms % 1000 or to_ms % 1000):
        s = sec(from_ms)
        return Range(s, s, (Edge(s, (to_ms - from_ms) / 1000),))
    # 这里//是向下取整，转成负数就是向上取整
    start, stop = -(-from_ms // 1000), sec(to_ms)
    edges = []

    # 判断左边有没有剩余段
    if (rem := start * 1000 - from_ms) > 0:
        edges.append(Edge(start - 1, rem / 1000))
    # 判断右边有没有剩余段
    if (rem := to_ms - stop * 1000) > 0:
        edges.append(Edge(stop, rem / 1000))
    return Range(start, stop, tuple(edges))
