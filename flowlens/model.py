"""事件模型：JSON 解析、逐条校验、URL 归一化。

解析与校验分两步：load_array（请求体解析成元素列表：顶层 JSON 数组或 NDJSON）+
validate（逐条校验与归一化），parse_events = validate(load_array(body))。这样大请求
可以解析一次后按块逐块 validate，不必一次性占住事件循环。validate 返回按列存放的
EventBatch；单条不合法只计入 invalid 并剔除，不影响同批其他事件。只有既不是合法
JSON 数组、也没有任何一行是合法 JSON（NDJSON）时才抛 BadRequest。
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
# ID 段：纯数字、UUID（8-4-4-4-12 十六进制）、或长度不少于 16 的十六进制。
# 字符类显式写 [0-9]/[0-9a-fA-F]，不用 \d——\d 会匹配其他语言的数字字符（如
# 阿拉伯-印度数字），与既有规则的 isascii() 前置判断不一致。
_ID_SEG = re.compile(
    r"[0-9]+"
    r"|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r"|[0-9a-fA-F]{16,}"
)
# 保留：_is_id_segment（profile_server 的 _id_seg_like 对照口径、参考实现）仍逐字符判断
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
    """请求体解析成元素列表：顶层是 JSON 数组时按数组解析（现有路径），否则按
    NDJSON（每行一条）解析。两种格式都没有可解析内容时抛 BadRequest。

    NDJSON 规则：跳过开头的空白；按换行切分、跳过空行；每行 orjson 解析一个
    元素。某一行不是合法 JSON 时放入占位值 None——由 validate 计为 invalid，
    不影响其他行，不抛 BadRequest。单个 JSON 对象（不是数组、也不是多行）
    就是一个单行 NDJSON 文档。整个请求体既不是数组、也没有任何一行是合法
    JSON 时，仍抛 BadRequest。

    大请求分块处理时，先解析出元素列表、再按块逐块 validate，避免一次性占住
    事件循环。
    """
    body = body.lstrip()
    if body[:1] == b"[":  # JSON 数组（现有路径，行为不变）
        try:
            arr = orjson.loads(body)
        except orjson.JSONDecodeError:
            raise BadRequest("请求体不是合法 JSON") from None
        if not isinstance(arr, list):
            raise BadRequest("顶层必须是 JSON 数组")
        return arr
    # NDJSON：按换行切分（rstrip 掉行尾的 \r，兼容 \r\n），跳过空行，逐行解析。
    # 只要有一行是合法 JSON 就接受（坏行放 None 占位、由 validate 计 invalid）；
    # 一行都解析不出来（整个请求体没有任何合法 JSON）才抛 BadRequest。
    out: list = []
    any_valid = False
    for line in body.split(b"\n"):
        line = line.rstrip(b"\r")
        if not line.strip():
            continue
        try:
            out.append(orjson.loads(line))
            any_valid = True
        except orjson.JSONDecodeError:
            out.append(None)  # 占位：validate 把它计为 invalid
    if not any_valid:
        raise BadRequest("请求体不是合法 JSON")
    return out


def parse_events(body: bytes) -> EventBatch:
    """解析并校验一批事件（JSON 数组或 NDJSON）。无可解析内容时抛 BadRequest。"""
    return validate(load_array(body))


def validate(objs: list) -> EventBatch:
    """校验与归一化（parse_events 去掉解析后的部分）。单条不合法只计入 invalid 并剔除。

    T11b：先一次性取出五列并整列做类型/范围检查；整块全部合法时走列式快路径
    （_validate_fast），跳过逐条的重复 isinstance 判断与分支。只要有一条不合法
    就退回逐条校验（_validate_one_by_one）——保留原实现，行为不变。
    两路径对任何输入产出逐字节相同的 EventBatch（tests/test_model.py 有修改前
    参考实现的对照测试）。
    """
    if not isinstance(objs, list):
        return _validate_one_by_one(objs)
    n = len(objs)
    if n == 0:
        return EventBatch([], [], [],
                          np.array([], dtype=np.int64), np.array([], dtype=np.int64), 0)
    # 有任一元素不是 dict 就直接走逐条慢路径（列式取列依赖 .get，对非 dict 不安全）
    if any(type(o) is not dict for o in objs):
        return _validate_one_by_one(objs)
    # 一次性取出五列（dict.get）
    eids = [o.get("event_id") for o in objs]
    uids = [o.get("user_id") for o in objs]
    urls = [o.get("url") for o in objs]
    tss = [o.get("ts") for o in objs]
    byts = [o.get("bytes") for o in objs]
    # 整列类型检查：字符串列非空、整数列 type is int（天然排除 bool）
    ok = True
    for eid, uid, url, t, b in zip(eids, uids, urls, tss, byts):
        if (type(eid) is not str or not eid
                or type(uid) is not str or not uid
                or type(url) is not str or not url
                or type(t) is not int or type(b) is not int):
            ok = False
            break
    # 范围检查：event_id 长度、ts/bytes 落在 int64 内
    if ok:
        for eid, t, b in zip(eids, tss, byts):
            if len(eid) > EVENT_ID_MAX_BYTES or (
                    not eid.isascii() and len(eid.encode("utf-8")) > EVENT_ID_MAX_BYTES):
                ok = False
                break
            if t <= 0 or t > INT64_MAX or b < 0 or b > INT64_MAX:
                ok = False
                break
    if ok:
        return _validate_fast(eids, uids, urls, tss, byts)
    return _validate_one_by_one(objs)


def _validate_fast(eids: list, uids: list, urls: list,
                   tss: list, byts: list) -> EventBatch:
    """列式快路径：五列已确认全部合法（类型与范围），只做归一化与（必要时）截断。"""
    key_max = KEY_MAX_BYTES
    truncate = _truncate_utf8
    norm = normalize_url
    # user_id 只在确实超长时才截断。截断判的是**字节**长度：ASCII 串字符数即字节数，
    # len <= 128 时 _truncate_utf8 原样返回，可省掉那次调用；非 ASCII 必须走
    # 按字节截断的 _truncate_utf8（字符数短、字节数长的串也会被截）。
    users = [u if (u.isascii() and len(u) <= key_max) else truncate(u) for u in uids]
    return EventBatch(
        eids, users, [norm(u) for u in urls],
        np.array(tss, dtype=np.int64), np.array(byts, dtype=np.int64), 0,
    )


def _validate_one_by_one(objs: list) -> EventBatch:
    """逐条校验与归一化（T11b 前的原实现）。任一条不合法即计入 invalid 并剔除。"""
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

    T11b：缓存策略——归一化过程中发生了 ID 段替换的路径（几乎每条都不同、几乎
    不会再次出现），不写入缓存：这类键写进去只会把能命中的稳定路径挤出去。
    稳定路径（无 ID 段）照常缓存，命中时跳过归一化。
    """
    head = _strip_query_fragment(raw)
    hit = _norm_cache.get(head)
    if hit is not None:
        return hit
    out, replaced = _normalize_url_impl(head)
    if url_intern is not None:
        out = url_intern.intern(out)
    # 发生 ID 段替换的路径不写缓存（见 docstring）；超长键也不写
    if not replaced and len(head) <= NORM_CACHE_MAX_KEY_LEN:
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


def _normalize_url_impl(raw: str) -> tuple[str, bool]:
    """归一化本体（缓存未命中才调用）。返回 (归一化结果, 是否发生了 ID 段替换)。

    T11b 快路径（与修改前 _normalize_url 的输出逐字节一致，
    tests/test_model.py 有修改前实现的参考对照测试）：
    - raw 中没有 "//" 时，scheme://host 与连续斜杠两个正则都不可能匹配（它们
      都要求字面 "//"），跳过；后续处理只删字符、不会造出 "//"，一次判断即可
      同时跳过两个正则；
    - 路径段是否为 ID 用预编译正则 _ID_SEG fullmatch 一次判断，替代逐字符的
      Python 循环（_is_hex）。
    """
    has_slashes = "//" in raw
    s = _SCHEME_HOST.sub("", raw, count=1) if has_slashes else raw
    q, h = s.find("?"), s.find("#")
    if q == -1 or (h != -1 and h < q):
        q = h
    if q != -1:
        s = s[:q]
    if has_slashes:
        s = _MULTI_SLASH.sub("/", s)
    if not s:
        s = "/"
    elif s[0] != "/":
        s = "/" + s
    if len(s) > 1:
        s = s.rstrip("/")
    parts = s.split("/")
    replaced = False
    fullmatch = _ID_SEG.fullmatch
    for i in range(1, len(parts)):
        if fullmatch(parts[i]) is not None:
            parts[i] = "{id}"
            replaced = True
    s = "/".join(parts)
    b = s.encode("utf-8")
    if len(b) > KEY_MAX_BYTES:
        s = b[:KEY_MAX_BYTES].decode("utf-8", "ignore")
        # 截断可能恰好切在一个 "/" 上，末尾这个 "/"（根路径除外）再去掉
        if len(s) > 1 and s.endswith("/"):
            s = s.rstrip("/")
    return s, replaced


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
