"""P2d-3 二.3：事件循环延迟探针（FLOWLENS_DEBUG 门控）的测试。

核心保证：未设置 FLOWLENS_DEBUG 时探针**不启用**——`make_loop_probe()` 返回
enabled=False，`start()` 不创建任何后台任务、`record_tick()` 为空操作（零开销）。
启用时能采到延迟、每分钟汇总（p50/p99/max）。
"""

import asyncio

import pytest

import flowlens.api.debug as debug
from flowlens.loop_probe import LoopProbe, _pctl, make_loop_probe


# ---- _pctl 纯函数 ----

def test_pctl_percentiles():
    assert _pctl([], 50) == 0.0
    assert _pctl([1, 2, 3, 4, 5], 50) == 3.0
    assert abs(_pctl(list(range(1, 101)), 99) - 99.01) < 1e-6


# ---- 未启用：不创建任务（P2d-3 二.3 要求 4）----

def test_disabled_probe_creates_no_task(monkeypatch):
    """未设置 FLOWLENS_DEBUG：make_loop_probe 未启用，start 不建任务、record_tick 空操作。"""
    monkeypatch.delenv("FLOWLENS_DEBUG", raising=False)
    probe = make_loop_probe()
    assert probe.enabled is False

    async def _run():
        created = set(asyncio.all_tasks())
        probe.start()                      # 未启用：应不创建任何任务
        await asyncio.sleep(0)
        assert set(asyncio.all_tasks()) == created, "未启用探针不应创建后台任务"
        probe.record_tick(1.0, 2.0)        # 未启用：空操作、不抛
        assert probe.final_top() == ""

    asyncio.run(_run())


def test_make_loop_probe_enabled_when_debug(monkeypatch):
    """FLOWLENS_DEBUG=1：探针启用。"""
    monkeypatch.setenv("FLOWLENS_DEBUG", "1")
    assert make_loop_probe().enabled is True
    # 复位，避免影响其他测试
    monkeypatch.delenv("FLOWLENS_DEBUG", raising=False)


# ---- 启用：能采到延迟并汇总 ----

def test_enabled_probe_starts_task(monkeypatch):
    """启用探针：start 创建探针的后台任务，close 后任务结束。

    （不数 all_tasks() 的个数——探针内部 wait_for(stop.wait()) 会派生临时任务，
    精确计数是竞态；只断言探针自己的任务被创建并完成。）
    """
    probe = LoopProbe(enabled=True, interval=0.05, report_every=60.0)

    async def _run():
        assert probe._task is None
        probe.start()
        await asyncio.sleep(0)
        assert probe._task is not None and isinstance(probe._task, asyncio.Task)
        await probe.close()
        assert probe._task.done()

    asyncio.run(_run())


def test_enabled_probe_reports_and_top(monkeypatch, caplog):
    """启用探针：喂入已知延迟样本，汇总算对 p50/p99/max 并留最长几次；record_tick 记日志。"""
    probe = LoopProbe(enabled=True, interval=0.05, report_every=60.0)
    # 直接喂样本（延迟 μs 与时刻 ms 一一对应），验证 _report 的统计口径
    probe._sample_us = [1.0, 2.0, 3.0, 4.0, 1000.0]
    probe._sample_ts = [10, 11, 12, 13, 14]
    probe._report()
    # final_top 保留最长几次（这里 1000μs 那次应在最前）
    top = probe.final_top()
    assert "1000" in top and "@14" in top
    with caplog.at_level("INFO", logger="flowlens.loop_probe"):
        probe._sample_us = [5.0]
        probe._sample_ts = [20]
        probe._report()
    assert any("事件循环延迟" in r.message and "本分钟" in r.message
               for r in caplog.records)
    # record_tick 启用时记一行日志（定稿/快照分解）
    with caplog.at_level("INFO", logger="flowlens.loop_probe"):
        probe.record_tick(3.0, 4.0)
    assert any("tick 分解" in r.message for r in caplog.records)

