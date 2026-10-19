"""事件模型：JSON 解析、逐条校验、URL 归一化。

解析与校验分两步：load_array（orjson 解析出顶层数组）+ validate（逐条校验与归一化），
parse_events = validate(load_array(body))。这样大请求可以解析一次后按块逐块 validate，
不必一次性占住事件循环。validate 返回按列存放的 EventBatch；单条不合法只计入 invalid
并剔除，不影响同批其他事件。只有顶层不是合法 JSON 数组时才抛 BadRequest。
"""

import re
from dataclasses import dataclass

import numpy as np
import orjson

from .errors import BadRequest
from .intern import url_intern

# event_id 长度上限（字节），超过视为非法
EVENT_ID_MAX_BYTES = 256
# url / user_id 归一化、截断后的长度上限（字节）
KEY_MAX_BYTES = 128
# ts / bytes 是 int64 列，超出 2^63−1 会在转 numpy 时抛 OverflowError，逐条判非法
INT64_MAX = (1 << 63) - 1
# 归一化结果缓存上限；同一个 URL 会反复出现，满了整体清空
NORM_CACHE_CAP = 100_000
# 缓存键是未截断的原始 URL，长度不受限；超过这个字符数就不写缓存，防止极端长 URL 撑爆内存
NORM_CACHE_MAX_KEY_LEN = 1024

# scheme://host 或 //host（协议相对形式），去掉 scheme 和主机只留路径
_SCHEME_HOST = re.compile(r"^(?:[A-Za-z][A-Za-z0-9+.\-]*:)?//[^/?#]*")
_MULTI_SLASH = re.compile(r"/{2,}")
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")

# 归一化缓存：raw url -> 归一化结果
_norm_cache: dict[str, str] = {}


def clear_norm_cache() -> None:
    """清空 URL 归一化缓存（降级第 3 级收紧内存时调用）。"""
    _norm_cache.clear()


@dataclass
class EventBatch:
    """一批通过校验的事件，按列存放，各列对齐。"""

    ids: list[str]
    users: list[str]
    urls: list[str]
    ts: np.ndarray      # 毫秒，int64
    nbytes: np.ndarray  # 响应字节数，int64
    invalid: int = 0    # 被剔除的不合法条数

    def __len__(self) -> int:
        return len(self.ids)


def load_array(body: bytes) -> list:
    """orjson 解析请求体为顶层数组。顶层不是数组或 JSON 不合法时抛 BadRequest。

    大请求分块处理时，先解析出数组、再按块逐块 validate，避免一次性占住事件循环。
    """
    try:
        arr = orjson.loads(body)
    except orjson.JSONDecodeError:
        raise BadRequest("请求体不是合法 JSON") from None
    if not isinstance(arr, list):
        raise BadRequest("顶层必须是 JSON 数组")
    return arr


def parse_events(body: bytes) -> EventBatch:
    """解析并校验一批事件。顶层不是 JSON 数组或 JSON 不合法时抛 BadRequest。"""
    return validate(load_array(body))


def validate(objs: list) -> EventBatch:
    """逐条校验与归一化（parse_events 去掉解析后的部分）。单条不合法只计入 invalid 并剔除。"""
    ids: list[str] = []
    users: list[str] = []
    urls: list[str] = []
    ts: list[int] = []
    nbytes: list[int] = []
    invalid = 0
    for obj in objs:
        if not isinstance(obj, dict):
            invalid += 1
            continue
        eid = obj.get("event_id")
        uid = obj.get("user_id")
        url = obj.get("url")
        t = obj.get("ts")
        b = obj.get("bytes")
        # 逐字段校验，任一不合法即整条剔除
        if not isinstance(eid, str) or not eid:
            invalid += 1
            continue
        if len(eid) > EVENT_ID_MAX_BYTES or (
                not eid.isascii() and len(eid.encode("utf-8")) > EVENT_ID_MAX_BYTES):
            invalid += 1
            continue
        if not isinstance(uid, str) or not uid:
            invalid += 1
            continue
        if not isinstance(url, str) or not url:
            invalid += 1
            continue
        if not _is_int(t) or not _is_int(b):
            # 非整数（含 float）。orjson 把 >= 2^64 的整数转成 float，与普通小数无法区分，
            # 一并按单条非法处理，不影响同批其他事件
            invalid += 1
            continue
        # 此时 t、b 都是 int；超过 2^63−1 放不进 int64 列，判这一条非法
        if t <= 0 or t > INT64_MAX or b < 0 or b > INT64_MAX:
            invalid += 1
            continue
        ids.append(eid)
        users.append(_truncate_utf8(uid))
        urls.append(normalize_url(url))
        ts.append(t)
        nbytes.append(b)

    return EventBatch(
        ids, users, urls,
        np.array(ts, dtype=np.int64), np.array(nbytes, dtype=np.int64), invalid,
    )


def _is_int(x) -> bool:
    """JSON 整数；bool 是 int 的子类，true/false 不算整数。"""
    return isinstance(x, int) and not isinstance(x, bool)


def _truncate_utf8(s: str) -> str:
    """按 UTF-8 截断到 128 字节，不截断多字节字符中间。"""
    if s.isascii():
        return s[:KEY_MAX_BYTES]
    b = s.encode("utf-8")
    if len(b) > KEY_MAX_BYTES:
        s = b[:KEY_MAX_BYTES].decode("utf-8", "ignore")
    return s


def normalize_url(raw: str) -> str:
    """URL 归一化（规则见 TASKS.md T1），结果缓存 10 万条，满了整体清空。

    T10b：缓存键先用 _strip_query_fragment 截掉 ?/# 之后的部分再查——真实流量里
    query 几乎每次都不同，若以整条原始 URL 为键几乎命中不了、每条都要走正则。
    截掉 query/fragment 后的"路径部分"是稳定的缓存键；scheme://host 的去除仍在
    缓存未命中时（_normalize_url 内部）进行。归一化结果与改动前逐字节一致
    （tests/test_model.py 有修改前实现的参考对照测试）。

    路径部分超过 NORM_CACHE_MAX_KEY_LEN 字符时照常归一化，但不写入缓存
    （键是无界的路径串，缓存它们可能占用大量内存）。

    返回前对归一化结果做驻留（intern.url_intern）：路径带不同 ID 的原始 URL
    （如 /order/1 与 /order/2）归一化后内容相同，但若不驻留每次都会生成新的
    字符串对象，各秒桶、叶子、节点里会存很多份。驻留只改"是否同一对象"，
    不改内容，归一化逻辑本身不变。
    """
    head = _strip_query_fragment(raw)
    hit = _norm_cache.get(head)
    if hit is not None:
        return hit
    out = _normalize_url(head)
    if url_intern is not None:
        out = url_intern.intern(out)
    if len(head) <= NORM_CACHE_MAX_KEY_LEN:
        if len(_norm_cache) >= NORM_CACHE_CAP:
            _norm_cache.clear()
        _norm_cache[head] = out
    return out


def _strip_query_fragment(raw: str) -> str:
    """截掉第一个 ? 或 #（取更早者）及其之后的部分，返回路径部分（含 scheme://host）。

    与 _normalize_url 内部"取 ? 与 # 中更早的一个"的口径一致；两者都没有时原样返回。
    用作缓存键前调用，让 query/fragment 不同的同一路径命中同一缓存项。
    """
    q, h = raw.find("?"), raw.find("#")
    if q == -1 or (h != -1 and h < q):
        q = h
    if q == -1:
        return raw
    return raw[:q]


def _normalize_url(raw: str) -> str:
    # 缓存未命中才走这里，可以用正则
    s = _SCHEME_HOST.sub("", raw, count=1)
    q, h = s.find("?"), s.find("#")
    if q == -1 or (h != -1 and h < q):
        q = h
    if q != -1:
        s = s[:q]
    s = _MULTI_SLASH.sub("/", s)
    if not s:
        s = "/"
    elif s[0] != "/":
        s = "/" + s
    if len(s) > 1:
        s = s.rstrip("/")
    parts = s.split("/")
    for i in range(1, len(parts)):
        if _is_id_segment(parts[i]):
            parts[i] = "{id}"
    s = "/".join(parts)
    b = s.encode("utf-8")
    if len(b) > KEY_MAX_BYTES:
        s = b[:KEY_MAX_BYTES].decode("utf-8", "ignore")
        # 截断可能恰好切在一个 "/" 上，末尾这个 "/"（根路径除外）再去掉
        if len(s) > 1 and s.endswith("/"):
            s = s.rstrip("/")
    return s


def _is_id_segment(seg: str) -> bool:
    """纯数字、UUID（8-4-4-4-12 十六进制，不区分大小写）、长度不少于 16 的十六进制。"""
    if not seg.isascii():
        return False
    if seg.isdigit():
        return True
    if len(seg) == 36 and seg[8] == seg[13] == seg[18] == seg[23] == "-":
        return (_is_hex(seg[:8]) and _is_hex(seg[9:13]) and _is_hex(seg[14:18])
                and _is_hex(seg[19:23]) and _is_hex(seg[24:]))
    return len(seg) >= 16 and _is_hex(seg)


def _is_hex(s: str) -> bool:
    return all(c in _HEX_DIGITS for c in s)
