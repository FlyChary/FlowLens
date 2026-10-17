"""查询参数解析与校验：把请求的 query string 变成一个 QueryParams。

校验在这里完成一遍；engine / query 层还会各自做第二道防线。只抛异常，
不自己写状态码（由 api 的中间件统一转成 HTTP 响应）。
"""

import re
from dataclasses import dataclass

from aiohttp import web

from ..dims import Dim
from ..errors import BadRequest

# window 格式：纯数字（秒）、Ns、Nm；合法范围 1~600 秒
_WINDOW_RE = re.compile(r"^\s*(\d+)\s*(s|m)?\s*$")
_WINDOW_SEC_MIN = 1
_WINDOW_SEC_MAX = 600
K_MIN, K_MAX = 1, 100

# interval（SSE 订阅周期）合法范围：200 ms ~ 60 s，默认 1 s
INTERVAL_MIN_MS = 200
INTERVAL_MAX_MS = 60_000
INTERVAL_DEFAULT_MS = 1000


@dataclass(frozen=True)
class QueryParams:
    """一个查询请求的参数（窗口、k、维度、at、interval）。"""

    window_ms: int
    k: int
    by: Dim
    at_ms: int | None
    interval_ms: int = INTERVAL_DEFAULT_MS

    @classmethod
    def from_request(cls, request: web.Request, now_ms: int,
                     *, need_k: bool = True, need_by: bool = True) -> "QueryParams":
        q = request.query
        window_ms = _parse_window(q.get("window", "60s"))
        k = _parse_k(q["k"]) if need_k and "k" in q else 10
        by = _parse_by(q["by"]) if need_by and "by" in q else Dim.URL
        at_ms = _parse_at(q.get("at"), now_ms)
        interval_ms = _parse_interval(q.get("interval"))
        return cls(window_ms, k, by, at_ms, interval_ms)


def _parse_window(s: str) -> int:
    m = _WINDOW_RE.match(s or "")
    if not m:
        raise BadRequest(f"window 格式非法（应为纯数字秒、Ns 或 Nm）: {s!r}")
    n = int(m.group(1))
    sec = n * 60 if m.group(2) == "m" else n
    if not _WINDOW_SEC_MIN <= sec <= _WINDOW_SEC_MAX:
        raise BadRequest(f"window 超出范围 {_WINDOW_SEC_MIN}~{_WINDOW_SEC_MAX} 秒: {s!r}")
    return sec * 1000


def _parse_k(s: str) -> int:
    if not (s or "").strip().isdigit():
        raise BadRequest(f"k 必须是正整数: {s!r}")
    k = int(s)
    if not K_MIN <= k <= K_MAX:
        raise BadRequest(f"k 超出范围 {K_MIN}~{K_MAX}: {s!r}")
    return k


def _parse_by(s: str) -> Dim:
    try:
        return Dim.parse(s)
    except ValueError as e:
        raise BadRequest(str(e)) from None


def _parse_at(s: str | None, now_ms: int) -> int | None:
    if s is None or s.strip() == "":
        return None
    if not s.strip().isdigit():  # 只接受非负整数（负数、小数、带符号都 400）
        raise BadRequest(f"at 必须是非负整数（epoch 毫秒）: {s!r}")
    at = int(s)
    if at > now_ms:
        raise BadRequest("at 在未来")
    return at


def _parse_interval(s: str | None) -> int:
    """interval：'500ms'、'1s'、'2'（纯数字按秒）等，转成毫秒。"""
    if s is None or s.strip() == "":
        return INTERVAL_DEFAULT_MS
    m = re.match(r"^\s*(\d+)\s*(ms|s)?\s*$", s)
    if not m:
        raise BadRequest(f"interval 格式非法（应为毫秒数、'Ns' 或 'Nms'）: {s!r}")
    n = int(m.group(1))
    unit = m.group(2) or "s"
    ms = n if unit == "ms" else n * 1000
    if not INTERVAL_MIN_MS <= ms <= INTERVAL_MAX_MS:
        raise BadRequest(
            f"interval 超出范围 {INTERVAL_MIN_MS}ms~{INTERVAL_MAX_MS // 1000}s: {s!r}")
    return ms
