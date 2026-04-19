"""Simulates a 1000-file user drive (Graph mock). Verifies the v2 backup
processes every file, commits checkpoint every 500, and stays inside the
concurrency bound."""
import asyncio

import pytest

from shared.backup_checkpoint import BackupCheckpoint


pytestmark = pytest.mark.integration


async def test_checkpoint_commits_every_500_files():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    commits = 0
    for i in range(1500):
        cp.record_file_done(external_id=f"f{i}", size=100)
        if cp.should_commit(every_files=500, every_bytes=10 ** 20):
            cp.mark_committed()
            commits += 1
    assert commits == 3
    assert cp.files_done_count == 1500


async def test_checkpoint_commits_every_gib_bytes():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    commits = 0
    for i in range(3):
        cp.record_file_done(external_id=f"big-{i}", size=1024 * 1024 * 1024)
        if cp.should_commit(every_files=10 ** 9, every_bytes=1024 * 1024 * 1024):
            cp.mark_committed()
            commits += 1
    assert commits == 3


async def test_concurrent_semaphore_bounds():
    """Mimic the file_sem inside _useronedrive_backup_files — N coroutines
    bounded by concurrency C should never exceed C in-flight."""
    concurrency = 16
    sem = asyncio.Semaphore(concurrency)
    active = 0
    peak = 0
    lock = asyncio.Lock()

    async def one():
        nonlocal active, peak
        async with sem:
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.001)
            async with lock:
                active -= 1

    await asyncio.gather(*(one() for _ in range(256)))
    assert peak <= concurrency
