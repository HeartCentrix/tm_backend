"""Load test: 5 k synthetic files through the checkpoint + concurrency pipeline."""
import asyncio
import time

import pytest

from shared.backup_checkpoint import BackupCheckpoint


pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def test_5k_files_checkpoint_and_concurrency():
    cp = BackupCheckpoint.empty(resource_id="r", drive_id="d")
    commits: list = []
    cp_lock = asyncio.Lock()
    sem = asyncio.Semaphore(16)

    async def _one(i):
        async with sem:
            await asyncio.sleep(0)  # yield; models I/O scheduling
            async with cp_lock:
                cp.record_file_done(external_id=f"f-{i}", size=1024)
                if cp.should_commit(every_files=500, every_bytes=10 ** 20):
                    cp.mark_committed()
                    commits.append(i)

    t0 = time.monotonic()
    await asyncio.gather(*(_one(i) for i in range(5000)))
    elapsed = time.monotonic() - t0

    assert cp.files_done_count == 5000
    assert len(commits) == 10
    assert elapsed < 10, f"5k-file sim took {elapsed:.2f}s"
