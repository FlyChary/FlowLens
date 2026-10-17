"""写入请求处理：解码请求体、按解压后字节预留容量、分块交给引擎。

顺序固定（CLAUDE.md 规则 2）：先解码，再按**解压后字节数**为整个请求预留容量，
预留成功后才解析、去重、写入。任何一步失败都不写 Bloom，调用方重试不会被当成
重复。

按字节而不是按条数预留的原因：orjson 解析后的 Python 对象约为原始 JSON 的 3.4 倍，
条数上限挡不住"解析本身"的内存；而解压后字节数在解析前就已确定，能覆盖解析的
峰值内存（见 docs/TASKS.md T4）。
"""

import asyncio
import zlib
from dataclasses import dataclass

from .errors import BadRequest, Overloaded, PayloadTooLarge, UnsupportedEncoding
from .engine import Engine, IngestResult
from .model import load_array, validate

MAX_BODY_BYTES = 4 * 1024 * 1024             # 请求体（压缩后）上限
MAX_DECOMPRESSED_BYTES = 4 * 1024 * 1024     # 单个请求解压后上限，超过提示拆分
MAX_PENDING_BYTES = 4 * 1024 * 1024          # 在途请求的解压后字节数之和的上限
CHUNK_EVENTS = 5000                          # 每次让出事件循环前处理的事件数

# gzip 解压分块：输入每次喂这么多压缩字节，输出用 max_length 限制，
# 避免把整个解压结果一次性放进内存（解压炸弹防护）。
_GZIP_INPUT_CHUNK = 64 * 1024
_GZIP_OUTPUT_MAX = 256 * 1024


def decode_body(raw: bytes, content_encoding: str | None) -> bytes | bytearray:
    """按 Content-Encoding 解码请求体（纯函数，不依赖 aiohttp）。

    只接受空 / identity / gzip（不区分大小写）；body 超限抛 PayloadTooLarge；
    gzip 分块解压，累计超过上限立即抛 PayloadTooLarge，不先把整个结果放进内存；
    数据损坏或流未完整结束（被截断）抛 BadRequest。
    返回 bytes（identity，零拷贝）或 bytearray（gzip，避免再复制一份）。
    """
    enc = (content_encoding or "").strip().lower()
    if enc not in ("", "identity", "gzip"):
        raise UnsupportedEncoding(f"不支持的 Content-Encoding: {content_encoding!r}")
    if len(raw) > MAX_BODY_BYTES:
        raise PayloadTooLarge(f"请求体超过 {MAX_BODY_BYTES} 字节")
    if enc == "gzip":
        return _gunzip(raw)
    return raw


def _gunzip(raw: bytes) -> bytearray:
    """分块 gzip 解压，边解压边检查大小，超上限立即停止。数据损坏/截断抛 BadRequest。

    返回 bytearray：调用方（orjson.loads）直接接受，免去一次整块 bytes 拷贝。
    """
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)  # 16 + MAX_WBITS = gzip 头
    out = bytearray()
    fed = 0
    try:
        while fed < len(raw):
            chunk = raw[fed:fed + _GZIP_INPUT_CHUNK]
            fed += len(chunk)
            while True:
                piece = d.decompress(chunk, _GZIP_OUTPUT_MAX)
                out += piece
                if len(out) > MAX_DECOMPRESSED_BYTES:
                    raise PayloadTooLarge(f"解压后超过 {MAX_DECOMPRESSED_BYTES} 字节，请拆分")
                chunk = d.unconsumed_tail
                if not chunk:
                    break
        tail = d.flush()
    except zlib.error as e:
        raise BadRequest(f"gzip 数据损坏: {e}") from None
    if not d.eof:
        raise BadRequest("gzip 流未完整结束（被截断）")
    if tail:
        out += tail
        if len(out) > MAX_DECOMPRESSED_BYTES:
            raise PayloadTooLarge(f"解压后超过 {MAX_DECOMPRESSED_BYTES} 字节，请拆分")
    return out


@dataclass
class Admission:
    """在途请求的解压后字节数计数：已预留、尚未处理完的字节之和。单写者，无锁。"""

    limit: int = MAX_PENDING_BYTES
    _pending: int = 0

    @property
    def pending(self) -> int:
        """在途（已预留未释放）的解压后字节数。"""
        return self._pending

    @property
    def occupancy(self) -> float:
        """占用率 pending / limit，降级控制器（T8）要用。"""
        return self._pending / self.limit if self.limit else 0.0

    def try_reserve(self, n: int) -> bool:
        """预留 n 字节；放不下返回 False（调用方应整体拒绝，不写任何去重状态）。"""
        if self._pending + n > self.limit:
            return False
        self._pending += n
        return True

    def release(self, n: int) -> None:
        self._pending = max(0, self._pending - n)


class IngestService:
    """写入请求处理：decode → 按解压后字节预留 → 解析 → 分块 validate + engine.ingest。"""

    def __init__(self, engine: Engine, admission: Admission):
        self.engine = engine
        self.admission = admission

    async def ingest(self, body: bytes, content_encoding: str | None) -> IngestResult:
        # 1. 解码（含 gzip 解压与大小检查），得到解压后的字节
        raw = decode_body(body, content_encoding)
        # 2. 按解压后字节数预留容量：必须在解析和任何去重之前（CLAUDE.md 规则 2），
        #    否则解析本身的内存（约 3.4 倍）挡不住
        n_bytes = len(raw)
        if not self.admission.try_reserve(n_bytes):
            raise Overloaded(retry_after=1)
        try:
            # 3. 解析出顶层数组（不合法抛 BadRequest）
            arr = load_array(raw)
            n = len(arr)
            # 4. 分块处理，块间让出事件循环
            total = IngestResult(0, 0, 0, 0)
            for i in range(0, n, CHUNK_EVENTS):
                j = min(i + CHUNK_EVENTS, n)
                batch = validate(arr[i:j])
                if batch:
                    res = self.engine.ingest(batch)
                    total = IngestResult(
                        total.accepted + res.accepted,
                        total.deduped + res.deduped,
                        total.late + res.late,
                        total.invalid + res.invalid,
                    )
                # 已处理的元素置 None（等长切片赋值），让这些 dict 尽早释放
                arr[i:j] = [None] * (j - i)
                if j < n:
                    await asyncio.sleep(0)  # 让出事件循环
            return total
        finally:
            self.admission.release(n_bytes)
