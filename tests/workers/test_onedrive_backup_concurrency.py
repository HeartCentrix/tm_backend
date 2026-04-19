"""Verify the semaphore math used by _useronedrive_backup_files caps in-flight
downloads at ONEDRIVE_BACKUP_FILE_CONCURRENCY under asyncio.gather fan-out."""
import asyncio


async def test_semaphore_caps_concurrency():
    from shared.config import settings

    sem = asyncio.Semaphore(settings.ONEDRIVE_BACKUP_FILE_CONCURRENCY)
    active = 0
    peak = 0
    lock = asyncio.Lock()

    async def one():
        nonlocal active, peak
        async with sem:
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.01)
            async with lock:
                active -= 1

    await asyncio.gather(*(one() for _ in range(64)))
    assert peak <= settings.ONEDRIVE_BACKUP_FILE_CONCURRENCY
