import asyncio

import pytest

from shared.memory_monitor import MemoryMonitor


async def test_monitor_fires_kill_when_limit_breached_for_grace():
    fired = asyncio.Event()

    async def on_breach():
        fired.set()

    mon = MemoryMonitor(
        limit_bytes=100,  # intentionally tiny → always over budget
        soft_limit_pct=80,
        grace_seconds=0.1,
        poll_interval_seconds=0.02,
        on_breach=on_breach,
    )
    await mon.start()
    await asyncio.wait_for(fired.wait(), timeout=1.0)
    await mon.stop()


async def test_monitor_does_not_fire_below_limit():
    fired = asyncio.Event()

    async def on_breach():
        fired.set()

    mon = MemoryMonitor(
        limit_bytes=10 * 1024 * 1024 * 1024,
        soft_limit_pct=80,
        grace_seconds=0.1,
        poll_interval_seconds=0.02,
        on_breach=on_breach,
    )
    await mon.start()
    await asyncio.sleep(0.3)
    await mon.stop()
    assert not fired.is_set()
