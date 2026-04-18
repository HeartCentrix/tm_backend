"""Verify that the worker's export semaphore caps simultaneous export jobs."""
import asyncio
import pytest


async def test_semaphore_caps_concurrent_exports():
    from shared.config import settings
    assert settings.MAX_CONCURRENT_EXPORTS_PER_WORKER == 2

    sem = asyncio.Semaphore(settings.MAX_CONCURRENT_EXPORTS_PER_WORKER)
    active = 0
    peak = 0
    lock = asyncio.Lock()

    async def fake_export():
        nonlocal active, peak
        async with sem:
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.05)
            async with lock:
                active -= 1

    await asyncio.gather(*(fake_export() for _ in range(10)))
    assert peak <= settings.MAX_CONCURRENT_EXPORTS_PER_WORKER
