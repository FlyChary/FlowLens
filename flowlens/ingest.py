"""写入请求处理：解码请求体、按解压后字节预留容量、分块交给引擎。

顺序固定（CLAUDE.md 规则 2）：先解码，再按**解压后字节数**为整个请求预留容量，
预留成功后才解析、去重、写入。任何一步失败都不写 Bloom，调用方重试不会被当成
重复。

按字节而不是按条数预留的原因：orjson 解析后的 Python 对象约为原始 JSON 的 3.4 倍，
条数上限挡不住"解析本身"的内存；而解压后字节数在解析前就已确定，能覆盖解析的
峰值内存（见 docs/TASKS.md T4）。
"""

import asyncio
import os
import zlib
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass

from .errors import BadRequest, Overloaded, PayloadTooLarge, UnsupportedEncoding
from .engine import Engine, IngestResult
from .model import load_array, validate
from .parse_pool import ParsePool, ParsedBatch

MAX_BODY_BYTES = 4 * 1024 * 1024             # 请求体（压缩后）上限
MAX_DECOMPRESSED_BYTES = 4 * 1024 * 1024     # 单个请求解压后上限，超过提示拆分
# 在途请求的解压后字节数之和的上限（默认 4 MB）。C3 实验用环境变量覆盖：
# 降到 2 MB 可压低满载在途内存，代价是更大的 503 比例。
DEFAULT_PENDING_BYTES = 4 * 1024 * 1024


def _read_pending_limit() -> int:
    """从 FLOWLENS_MAX_PENDING_BYTES 读在途上限（字节）。

    缺省/非法/非正数都退回默认 4 MB。容器实验用它把在途额度降到 2 MB 做对比
    （组 4）。生产不设该变量，行为与原来一致。
    """
    raw = os.environ.get("FLOWLENS_MAX_PENDING_BYTES", "").strip()
    if not raw:
        return DEFAULT_PENDING_BYTES
    try:
        v = int(raw)
    except ValueError:
        return DEFAULT_PENDING_BYTES
    return v if v > 0 else DEFAULT_PENDING_BYTES


MAX_PENDING_BYTES = _read_pending_limit()
# 每次让出事件循环前写入引擎的事件数（P1 第 2 节 / 用户决定五）：从 5000 降到 2000。
# 依据：写入引擎每条约 2.5 μs，单块 2000 条约 5 ms；混合负载下查询 p99 高的原因是
# 查询排在多个写入块之后，缩短单块同步耗时能压低事件循环被独占的时间。
CHUNK_EVENTS = 2000
# "写入引擎"这一步同时进行的请求数上限（用户决定五）：写入引擎是单写者，串行化避免
# 多个写入块背靠背独占事件循环、拖高查询 p99。
DEFAULT_ENGINE_WRITE_CONCURRENCY = 1

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

    def can_reserve(self, n: int) -> bool:
        """能否再预留 n 字节（只读判断，不改状态）。

        Expect: 100-continue 的拒绝路径在发请求体之前就要用这个判断（不能真的
        预留，否则请求被拒后没有对应的释放点）；真正的预留仍是 try_reserve。
        """
        return self._pending + n <= self.limit

    def try_reserve(self, n: int) -> bool:
        """预留 n 字节；放不下返回 False（调用方应整体拒绝，不写任何去重状态）。"""
        if not self.can_reserve(n):
            return False
        self._pending += n
        return True

    def release(self, n: int) -> None:
        self._pending = max(0, self._pending - n)


class IngestService:
    """写入请求处理（P1 起）：decode → 按解压后字节预留 → 解析（进程池或本进程）→
    分块写入引擎（信号量保护）。

    顺序固定（CLAUDE.md 规则 2 + 用户决定一）：
      decode_body（解压，主进程）→ 按**解压后字节数** try_reserve（在解析与去重之前）
      → 解析（进程池可用则交给子进程，否则本进程 validate）→ 分块写引擎 → finally release。
    预留失败 / 解析异常都**不写任何去重状态**，调用方重试不会被判重。

    解析进程只算"纯计算"部分（load_array、validate、三列哈希）；**去重、采样、写桶全部
    只在主进程**的 engine._ingest_core 里做，单写者、无锁、语义与单进程完全一致。
    """

    def __init__(self, engine: Engine, admission: Admission,
                 parse_pool: ParsePool | None = None,
                 engine_write_concurrency: int = DEFAULT_ENGINE_WRITE_CONCURRENCY):
        self.engine = engine
        self.admission = admission
        self._pool = parse_pool
        self._parse_inproc = parse_pool is None or not parse_pool.available
        self._write_sem = None  # 延迟到事件循环里创建（见 _get_write_sem）
        self._write_concurrency = max(1, engine_write_concurrency)

    def _get_write_sem(self) -> asyncio.Semaphore:
        """'写入引擎'这一步的信号量（决定五）。单写者串行化，避免多个写入块背靠背
        独占事件循环。Semaphore 必须在事件循环里创建，这里按当前 loop 延迟初始化。"""
        if self._write_sem is None:
            self._write_sem = asyncio.Semaphore(self._write_concurrency)
        return self._write_sem

    async def ingest(self, body: bytes, content_encoding: str | None) -> IngestResult:
        # 1. 解码（含 gzip 解压与大小检查），得到解压后的字节
        raw = decode_body(body, content_encoding)
        # 2. 按解压后字节数预留容量：必须在解析和任何去重之前（CLAUDE.md 规则 2）
        n_bytes = len(raw)
        if not self.admission.try_reserve(n_bytes):
            # 第 3 级把在途上限从 4 MB 收紧到 2 MB：这期间解压后 2~4 MB 的请求预留
            # 不下，走这里的 503（而不是 413）——有意的、暂时性的，退出降级即可成功。
            if self.admission.limit < MAX_PENDING_BYTES:
                mb = self.admission.limit // (1024 * 1024)
                raise Overloaded(
                    f"降级期间单个请求的上限暂时为 {mb} MB，请稍后重试", retry_after=1)
            raise Overloaded(retry_after=1)
        try:
            if self._parse_inproc:
                return await self._ingest_inproc(raw)
            return await self._ingest_pooled(raw)
        finally:
            self.admission.release(n_bytes)

    async def _ingest_inproc(self, raw: bytes) -> IngestResult:
        """单进程路径（现有路径）：本进程 load_array + 分块 validate + engine.ingest。

        结构与改动前一致（逐块 validate→ingest→释放 dict→块间让出），只是块大小改为
        2000（CHUNK_EVENTS），且"写入引擎"这段受信号量保护（决定五：串行化、避免多块
        背靠背独占事件循环）。validate 是纯函数、每块各带 invalid 计数，逐块累加。
        """
        arr = load_array(raw)
        n = len(arr)
        sem = self._get_write_sem()
        total = IngestResult(0, 0, 0, 0)
        async with sem:
            for i in range(0, n, CHUNK_EVENTS):
                j = min(i + CHUNK_EVENTS, n)
                batch = validate(arr[i:j])
                # 总是调用 engine.ingest：整块全非法时 batch 为空，engine.ingest
                # 仍会返回 batch.invalid（其 invalid = batch.invalid + 未来事件），
                # 用 `if batch:` 跳过会丢掉这块的 invalid 计数
                total = self._add(total, self.engine.ingest(batch))
                # 已处理的元素置 None（等长切片赋值），让这些 dict 尽早释放
                arr[i:j] = [None] * (j - i)
                if j < n:
                    await asyncio.sleep(0)  # 让出事件循环
        return total

    async def _ingest_pooled(self, raw: bytes) -> IngestResult:
        """进程池路径：把**解压后**的字节交给子进程解析（load_array+validate+三列哈希），
        拿回 ParsedBatch 后按 CHUNK_EVENTS 切块、分块写引擎（信号量保护、块间让出）。

        解压已在主进程完成（决定一）；子进程只做纯计算，异常（BadRequest 等）原样传回。
        子进程崩溃（BrokenProcessPool）时重建进程池、当前请求 503（此时**尚未去重**，
        重试安全，P1 §1.5 / 规则 2）。
        """
        try:
            fut = self._pool.submit(raw)  # 池已坏时 submit 会**同步**抛 BrokenProcessPool
            parsed = await asyncio.wrap_future(fut)  # 或等待期间抛（子进程处理中崩溃）
        except BrokenProcessPool:
            # 子进程崩溃（submit 或处理期间）→ 重建进程池；此时**尚未去重**，当前请求
            # 返回 503 + Retry-After（P1 §1.5 / 规则 2），调用方重发安全。
            self._pool.rebuild()
            raise Overloaded("解析进程不可用，请重试", retry_after=1) from None
        # 切块写引擎。整批 invalid 必须让**引擎计数器**也计到（与单进程路径
        # engine.ingest 更新 counters.invalid 口径一致）：把它放在第一个块上（chunk 默认
        # invalid=0，这里给首块赋上整批 invalid）。全非法请求体 parsed 为空、切不出块，
        # 用单块承载 invalid，保证计数器仍计到（test_ingest_all_invalid_chunked_counted）。
        chunks = [parsed.chunk(i, i + CHUNK_EVENTS)
                  for i in range(0, len(parsed), CHUNK_EVENTS)]
        if not chunks:
            chunks = [parsed.chunk(0, 0)]
        chunks[0].invalid = parsed.invalid
        sem = self._get_write_sem()
        total = IngestResult(0, 0, 0, 0)
        async with sem:
            for idx, chunk in enumerate(chunks):
                total = self._add(total, self.engine.ingest_parsed(chunk))
                if idx + 1 < len(chunks):
                    await asyncio.sleep(0)  # 块间让出事件循环
        return total

    @staticmethod
    def _add(a: IngestResult, b: IngestResult) -> IngestResult:
        """累加两次写入的计数。"""
        return IngestResult(a.accepted + b.accepted, a.deduped + b.deduped,
                            a.late + b.late, a.invalid + b.invalid)
