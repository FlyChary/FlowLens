"""写入通道的消息格式（P2d-1 第三节）：前端 → 引擎的批次、引擎 → 前端的 ACK。

一条消息 = 5 字节头 + pickle 负载。头是 struct "<IB"（小端、无对齐）：
I 是负载长度（无符号 32 位，上限约 4 GB，够用）、B 是消息类型。分帧靠
readexactly——一条消息的字节无论被拆成多少段到达，都能还原。

WireBatch 的字符串列只传本批中**不重复**的那些（url_uniq / user_uniq），再附一个
逐条下标数组（url_idx / user_idx），还原时按下标展开。长尾流量里大量事件共用
同一归一化 URL，去重后线上体量比逐条传字符串小得多。
"""

import asyncio
import pickle
import struct

import numpy as np
from dataclasses import dataclass

MSG_BATCH = 1   # 前端 → 引擎：一个写入请求的解析结果
MSG_ACK = 2     # 引擎 → 前端：写入结果或错误
MSG_SNAPSHOT = 3   # 引擎 → 前端：最新快照（P2d-2 快照广播）
_HDR = struct.Struct("<IB")   # 负载长度（字节）+ 消息类型


def pack_msg(mtype: int, obj) -> bytes:
    """序列化成一整帧（头 + 负载）。快照广播用它：一次序列化、发给多个连接，
    且"一次 write 写出整帧"保证不同消息在字节流中不交错（见 hub.py）。"""
    data = pickle.dumps(obj, protocol=5)
    return _HDR.pack(len(data), mtype) + data


async def send_msg(writer: asyncio.StreamWriter, mtype: int, obj) -> None:
    """把 (mtype, obj) 序列化成一条消息写入 writer（头与体连续写出，再 drain）。"""
    data = pickle.dumps(obj, protocol=5)
    writer.write(_HDR.pack(len(data), mtype))
    writer.write(data)
    await writer.drain()


async def read_msg(reader: asyncio.StreamReader):
    """读一条消息，返回 (类型, 对象)。

    连接在消息中途关闭时 readexactly 抛 asyncio.IncompleteReadError，由调用方处理。
    """
    hdr = await reader.readexactly(_HDR.size)
    n, mtype = _HDR.unpack(hdr)
    return mtype, pickle.loads(await reader.readexactly(n))


@dataclass
class WireBatch:
    """一个写入请求的解析结果的线上格式。

    ts / nbytes / h_id / h_url / h_user 与 url_idx / user_idx 逐行对齐（长度都是
    事件数）；url_uniq / user_uniq 是本批不重复的字符串，url_idx[j] / user_idx[j]
    是第 j 条事件对应的下标。invalid 是被剔除的不合法条数。
    """

    req_id: int
    ts: np.ndarray       # int64
    nbytes: np.ndarray   # int64
    h_id: np.ndarray     # uint64
    h_url: np.ndarray    # uint64
    h_user: np.ndarray   # uint64
    url_uniq: list
    url_idx: np.ndarray  # int32，长度 = 事件数
    user_uniq: list
    user_idx: np.ndarray
    invalid: int


def _dedupe(seq: list) -> tuple[list, np.ndarray]:
    """把字符串序列拆成 (不重复列表, 逐条下标)。下标即该串首次出现时的序号。"""
    d: dict = {}
    # setdefault 的第二个参数在插入之前求值，所以新字符串得到的下标就是插入前的
    # len(d)；重复串直接命中已存的下标。
    idx = np.fromiter((d.setdefault(s, len(d)) for s in seq), dtype=np.int32,
                      count=len(seq))
    return list(d), idx


def encode_parsed(req_id: int, p) -> WireBatch:
    """把 ParsedBatch 编码成 WireBatch（字符串去重）。"""
    url_uniq, url_idx = _dedupe(p.urls)
    user_uniq, user_idx = _dedupe(p.users)
    return WireBatch(req_id, p.ts, p.nbytes, p.h_id, p.h_url, p.h_user,
                     url_uniq, url_idx, user_uniq, user_idx, p.invalid)


def decode_to_parsed(w: WireBatch, intern_url=None, intern_user=None):
    """引擎侧：把 WireBatch 还原成 ParsedBatch。

    只对**不重复**的字符串做驻留（intern_url / intern_user 传驻留表的 intern 方法，
    为 None 时不驻留），再按下标展开成逐条列表。驻留在引擎进程做才有意义（驻留表
    只在引擎进程内共享，见 intern.py）；前端进程里 parse_body 做的驻留是各进程一份、
    跨进程无意义，真正的共享驻留在这里补做。
    """
    from .parse_pool import ParsedBatch

    def expand(uniq: list, idx: np.ndarray, intern) -> list:
        if intern is not None:
            uniq = [intern(s) for s in uniq]
        arr = np.empty(len(uniq), dtype=object)
        if uniq:
            arr[:] = uniq
        return arr[idx].tolist()

    return ParsedBatch(
        ts=w.ts, nbytes=w.nbytes, h_id=w.h_id, h_url=w.h_url, h_user=w.h_user,
        urls=expand(w.url_uniq, w.url_idx, intern_url),
        users=expand(w.user_uniq, w.user_idx, intern_user),
        invalid=w.invalid,
    )
