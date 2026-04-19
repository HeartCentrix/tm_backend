"""Load test: 500 files x 2 MB = ~1 GB export. Verifies runtime + memory bound."""
import time
import tracemalloc

import pytest

from shared.azure_storage import AzureStorageShard
from shared.export_manifest import ExportManifestBuilder
from tests.workers.workers_restore_worker_shim import FileExportOrchestrator


pytestmark = [pytest.mark.integration, pytest.mark.slow]


class _Item:
    def __init__(self, ext_id, name, folder, blob_path, size):
        self.external_id = ext_id
        self.name = name
        self.folder_path = folder
        self.blob_path = blob_path
        self.content_size = size
        self.snapshot_id = "snap-1"
        self.tenant_id = "tenant-1"
        self.resource_id = "res-1"
        self.shard_index = 0
        self.extra_data = {}


class _Manager:
    def __init__(self, shard):
        self._shard = shard

    def get_shard_by_index(self, i):
        return self._shard

    def get_shard_for_item(self, item):
        return self._shard

    def get_default_shard(self):
        return self._shard


async def _seed_load(shard, count=500):
    await shard.ensure_container("backup-files-tenantl")
    await shard.ensure_container("backup-exports-tenantl")
    body = b"X" * (2 * 1024 * 1024)
    items = []
    for i in range(count):
        ext_id = f"id-{i}"
        path = f"snap-1/ts/{ext_id}"
        await shard.upload_blob("backup-files-tenantl", path, body)
        items.append(_Item(ext_id, f"f-{i}.bin", "Documents", path, len(body)))
    return items


async def test_load_500_files(azure_test_connection_string):
    shard = AzureStorageShard.from_connection_string(azure_test_connection_string)
    try:
        items = await _seed_load(shard)
        manifest = ExportManifestBuilder(job_id="job-L", snapshot_ids=["snap-1"])

        tracemalloc.start()
        t0 = time.monotonic()
        orch = FileExportOrchestrator(
            job_id="job-L",
            snapshot_ids=["snap-1"],
            items=items,
            shard_manager=_Manager(shard),
            source_container="backup-files-tenantl",
            dest_container="backup-exports-tenantl",
            parallelism=12,
            block_size=4 * 1024 * 1024,
            fetch_batch_size=50,
            export_format="ZIP",
            missing_policy="skip",
            max_file_bytes=10 ** 12,
            path_max_len=260,
            sanitize_chars='<>:"/\\|?*',
            manifest=manifest,
        )
        result = await orch.run()
        elapsed = time.monotonic() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert result["exported_count"] == 500
        # Azurite is the bottleneck locally; real Azure is much faster. Keep a
        # generous cap that still fails if something regresses catastrophically.
        assert elapsed < 600, f"load test {elapsed:.1f}s exceeded 600s"
        assert peak < 500 * 1024 * 1024, f"peak {peak} exceeded 500 MB"
        print(
            f"[load] 500 x 2 MB export in {elapsed:.1f}s, "
            f"peak {peak / 1024 / 1024:.1f} MB"
        )
    finally:
        await shard.close()
