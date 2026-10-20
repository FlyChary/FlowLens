"""快照广播（P2d-2 二）：引擎侧把最新快照发给所有前端的写入通道。

引擎每次替换快照后（Engine.on_snapshot）调用 publish：把快照序列化成一整帧
（ipc.pack_msg，一次序列化），安排一次异步发送。只保留"最新一帧"——上一次广播
还没发完又来了新快照时，丢弃旧的、只发最新的（前端的默认窗口查询对 1 秒级的
快照滞后不敏感，落后一帧可接受，省掉积压的重复快照）。

**为什么不需要锁**：asyncio 是单线程的。写入通道上"头 + 负载"的写出
（ipc.send_msg）与整帧的写出（SnapshotHub._flush 对每个 writer 一次 write）
在字节流里都不会与其他消息交错——write 调用是同步的、write 与 drain 之间
没有 await 点能让别的协程往同一 writer 写。ACK 与快照帧因此不会互相穿插，
前端的 read_msg（按 5 字节头读长度再读负载）总能完整还原一条消息。
"""

import asyncio
from typing import Callable

from .ipc import MSG_SNAPSHOT, pack_msg


class SnapshotHub:
    """维护各前端写入通道的 writer，把最新快照广播出去。

    只保留"最新一帧"：如果上一次广播还没发完又来了新快照，就丢弃旧的、只发
    最新的（_flush 的循环每次重新读 _pending）。
    """

    def __init__(self) -> None:
        self._writers: set = set()
        self._latest: bytes | None = None   # 最新一帧（已序列化），新连接加入时立即发
        self._pending: bytes | None = None  # 等待发送的帧
        self._task: asyncio.Task | None = None

    def add(self, writer) -> None:
        """登记一条前端连接；已有最新快照时立即发给它（新前端不必等下一个 tick）。"""
        self._writers.add(writer)
        if self._latest is not None:
            writer.write(self._latest)

    def discard(self, writer) -> None:
        """移除一条前端连接（连接断开时调用）。"""
        self._writers.discard(writer)

    def publish(self, snap) -> None:
        """Engine.on_snapshot 回调。同步执行：序列化一次，安排异步发送。"""
        frame = pack_msg(MSG_SNAPSHOT, snap)
        self._latest = self._pending = frame
        if self._task is None or self._task.done():
            self._task = asyncio.get_running_loop().create_task(self._flush())

    async def _flush(self) -> None:
        while self._pending is not None:
            frame, self._pending = self._pending, None
            ws = list(self._writers)
            for w in ws:
                try:
                    w.write(frame)   # 一次 write 写出整帧（头 + 负载不交错）
                except Exception:
                    self._writers.discard(w)
            await asyncio.gather(*(w.drain() for w in ws), return_exceptions=True)

    async def close(self) -> None:
        """停机时结束未完成的广播任务。"""
        if self._task is not None and not self._task.done():
            self._task.cancel()
        if self._task is not None:
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None


def make_on_snapshot(hub: SnapshotHub, shared, controller, memory_reader) -> Callable:
    """Engine.on_snapshot 的工厂：同时做"发布共享状态"与"广播快照"。

    前端回答默认窗口查询需要引擎的降级状态与内存读数（degraded / sample_weight
    / memory_bytes），这里随每次快照（每秒一次）把它们写进共享内存：
    - degraded / level：取控制器的当前级别（与前端 DegradeView 读的 shared.level
      同一口径）；
    - sample_weight：采样权重（degrade.py 的 sample_weight 属性：占用率 > 0.95
      时为 4、第 2 级起为 2，其余 1）——同一级别内也会随占用率在 1/2/4 之间变，
      所以必须每次快照都写（最多滞后 1 秒）；
    - memory_bytes：用 app 的 memory_reader 读，与 /v1/stats 的 memory_bytes
      同一读数来源。

    级别变化时（on_change 钩子）也会立即写一次 shared.level，让"前端据此拒写"
    不受 1 秒滞后影响（见 __main__ 的说明）；这里的写保证前端**回答查询用的
    状态**与它手里的快照一致（同一时刻读）。
    """
    def on_snapshot(snap) -> None:
        shared.degraded.value = 1 if controller.degraded else 0
        shared.level.value = controller.level
        shared.sample_weight.value = controller.sample_weight
        shared.mem_bytes.value = memory_reader()[0]
        hub.publish(snap)
    return on_snapshot
