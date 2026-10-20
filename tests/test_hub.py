"""P2d-2 二：快照广播（SnapshotHub / pack_msg / FrontState / make_on_snapshot）。

不真起进程、跨平台可跑：writer 用记录字节的替身；快照帧直接经
flowlens.ipc.read_msg 的同一分帧路径还原，验证"一次 write 写出整帧"后
前端读循环能正确拆开 ACK 与快照帧。
"""

import asyncio
import multiprocessing as mp
import time

import pytest

from flowlens.engine import Engine, Snapshot
from flowlens.frontend import FrontState
from flowlens.hub import SnapshotHub, make_on_snapshot
from flowlens.ipc import MSG_ACK, MSG_SNAPSHOT, pack_msg, read_msg
from flowlens.supervisor import SharedState

NOW = 1_735_689_600_000


class _Writer:
    """StreamWriter 替身：记录 write 的字节；drain 可配置挂起一拍。"""

    def __init__(self, hang: bool = False):
        self.frames = []
        self.hang = hang

    def write(self, b: bytes):
        self.frames.append(b)

    async def drain(self):
        if self.hang:
            await asyncio.sleep(0.01)


class _Reader:
    """从预置字节流读消息（模拟前端读循环的 read_msg 源）。"""

    def __init__(self, blob: bytes):
        self._buf = bytearray(blob)

    async def readexactly(self, n: int) -> bytes:
        if len(self._buf) < n:
            raise asyncio.IncompleteReadError(bytes(self._buf), n)
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out


def _snap(tag: int) -> Snapshot:
    return Snapshot(NOW, NOW - 60_000, NOW, (), (0.0, 0.0), 12.0, 100.0 + tag)


def test_hub_publish_writes_full_frame_to_all():
    """publish 后每个已登记 writer 收到一整帧；read_msg 能还原出快照。"""
    async def go():
        hub = SnapshotHub()
        w1, w2 = _Writer(), _Writer()
        hub.add(w1)
        hub.add(w2)
        s = _snap(1)
        hub.publish(s)
        await hub._task
        for w in (w1, w2):
            mtype, out = await read_msg(_Reader(b"".join(w.frames)))
            assert mtype == MSG_SNAPSHOT and out == s
        await hub.close()
    asyncio.run(go())


def test_hub_late_connection_gets_latest():
    """连接在第一次 publish 之后加入：add 时立即拿到当前最新帧。"""
    async def go():
        hub = SnapshotHub()
        s = _snap(7)
        hub.publish(s)
        await hub._task
        w = _Writer()
        hub.add(w)
        assert len(w.frames) == 1
        mtype, out = await read_msg(_Reader(b"".join(w.frames)))
        assert mtype == MSG_SNAPSHOT and out == s
        await hub.close()
    asyncio.run(go())


def test_hub_only_keeps_latest():
    """一次广播还没发出（任务未开始运行）又来新快照：旧的被丢弃、只发最新的
    （_pending 被覆盖、不另起任务）。"""
    async def go():
        hub = SnapshotHub()
        w = _Writer(hang=True)   # drain 挂起，保证发送慢于 publish
        hub.add(w)
        hub.publish(_snap(1))    # 任务已创建、尚未运行（没有 await 过）
        hub.publish(_snap(2))    # 立刻新快照：_pending 被覆盖
        await hub._task
        await hub.close()
        # 只写出最新一帧（tag=2），tag=1 被丢弃
        mtype, out = await read_msg(_Reader(b"".join(w.frames)))
        assert mtype == MSG_SNAPSHOT
        assert out.p95_bytes == 100.0 + 2
    asyncio.run(go())


def test_hub_discard_stops_delivering():
    async def go():
        hub = SnapshotHub()
        w1, w2 = _Writer(), _Writer()
        hub.add(w1)
        hub.add(w2)
        hub.discard(w1)
        hub.publish(_snap(3))
        await hub._task
        assert w1.frames == [] and len(w2.frames) == 1
        await hub.close()
    asyncio.run(go())


def test_snapshot_frame_and_ack_interleave_on_stream():
    """ACK 帧与快照帧在同一字节流里先后出现，read_msg 逐条都能还原
    （"write 与 drain 之间没有 await 让出点"的字节流不交错，见 hub.py 注释）。"""
    async def go():
        ack = pack_msg(MSG_ACK, (1, "ok", 5, 1, 2, 3))
        snap = pack_msg(MSG_SNAPSHOT, _snap(4))
        blob = ack + snap + ack
        # 从同一流连续读三条（ACK、快照、ACK）
        r = _Reader(blob)
        m1, o1 = await read_msg(r)
        m2, o2 = await read_msg(r)
        m3, o3 = await read_msg(r)
        assert (m1, o1) == (MSG_ACK, (1, "ok", 5, 1, 2, 3))
        assert m2 == MSG_SNAPSHOT and o2.p95_bytes == 104.0
        assert (m3, o3) == (MSG_ACK, (1, "ok", 5, 1, 2, 3))
    asyncio.run(go())


def test_front_state_freshness():
    shared = SharedState(mp.get_context("spawn"), 1)
    st = FrontState(shared)
    assert st.fresh_snapshot() is None       # 还没收到快照
    s = _snap(1)
    st.snapshot = s
    st.snapshot_recv_ms = time.monotonic() * 1000
    assert st.fresh_snapshot() is s          # 刚收到
    st.snapshot_recv_ms -= 3001              # 3 秒前收到 → 过期
    assert st.fresh_snapshot() is None
    st.snapshot_recv_ms = time.monotonic() * 1000 - 2999
    assert st.fresh_snapshot() is s          # 3 秒内（含边界附近）仍新鲜


class _FakeController:
    """make_on_snapshot 只读 degraded / level / sample_weight 三个属性。"""
    degraded = True
    level = 3
    sample_weight = 2


class _FakeMemory:
    def __call__(self):
        return (251_658_240, 268_435_456)


def test_make_on_snapshot_publishes_shared_state():
    """快照回调把 degraded / level / sample_weight / mem_bytes 写进共享内存，
    并经 hub 广播（采样权重在同一级别内也会变，所以每次快照都写）。"""
    async def go():
        shared = SharedState(mp.get_context("spawn"), 1)
        hub = SnapshotHub()
        w = _Writer()
        hub.add(w)
        on_snap = make_on_snapshot(hub, shared, _FakeController(), _FakeMemory())
        on_snap(_snap(9))
        await hub._task
        assert shared.degraded.value == 1
        assert shared.level.value == 3
        assert shared.sample_weight.value == 2
        assert shared.mem_bytes.value == 251_658_240
        mtype, snap = await read_msg(_Reader(b"".join(w.frames)))
        assert mtype == MSG_SNAPSHOT and snap.p95_bytes == 109.0
        await hub.close()
    asyncio.run(go())


def test_engine_on_snapshot_hook_fires():
    """Engine.refresh_snapshot 整体替换快照后触发 on_snapshot（同步回调）。"""
    from flowlens.config import Config
    from flowlens.timewin import FakeClock
    got = []
    eng = Engine(Config(), FakeClock(NOW))
    eng.on_snapshot = got.append
    s = eng.refresh_snapshot()
    assert got == [s] and got[0] is eng.snapshot
    # 异步入口同样触发
    got.clear()
    s2 = asyncio.run(eng.refresh_snapshot_async())
    assert got == [s2] and got[0] is eng.snapshot
