"""End-to-end multi-file export: seeds files across a folder tree, runs the
orchestrator, verifies ZIP structure + manifest + tracemalloc memory bound."""
import io
import tracemalloc
import zipfile

import pytest

from shared.azure_storage import AzureStorageShard
from shared.export_manifest import ExportManifestBuilder
from tests.workers.workers_restore_worker_shim import FileExportOrchestrator


pytestmark = pytest.mark.integration


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


async def _seed(shard, per_folder=10):
    await shard.ensure_container("backup-files-tenanty")
    await shard.ensure_container("backup-exports-tenanty")
    items = []
    for folder in ("Documents", "Documents/Q1", "Photos"):
        for i in range(per_folder):
            ext_id = f"{folder.replace('/', '_')}-{i}"
            name = f"file-{i}.txt"
            path = f"snap-1/ts/{ext_id}"
            body = f"body-{ext_id}".encode() * 64
            await shard.upload_blob("backup-files-tenanty", path, body)
            items.append(_Item(ext_id, name, folder, path, len(body)))
    items.append(_Item("pending", "pending.docx", "Documents", None, 1024))
    return items


async def test_multi_file_zip_end_to_end(azure_test_connection_string):
    shard = AzureStorageShard.from_connection_string(azure_test_connection_string)
    try:
        items = await _seed(shard)
        manifest = ExportManifestBuilder(job_id="job-z", snapshot_ids=["snap-1"])

        tracemalloc.start()
        orch = FileExportOrchestrator(
            job_id="job-z",
            snapshot_ids=["snap-1"],
            items=items,
            shard_manager=_Manager(shard),
            source_container="backup-files-tenanty",
            dest_container="backup-exports-tenanty",
            parallelism=4,
            block_size=64 * 1024,
            fetch_batch_size=10,
            export_format="ZIP",
            missing_policy="skip",
            max_file_bytes=10 ** 12,
            path_max_len=260,
            sanitize_chars='<>:"/\\|?*',
            manifest=manifest,
        )
        result = await orch.run()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert result["output_mode"] == "zip"
        assert result["exported_count"] == 30
        assert result["failed_count"] == 1
        assert peak < 300 * 1024 * 1024, f"peak {peak} exceeded 300 MB"

        zip_bytes = await shard.download_blob("backup-exports-tenanty", result["blob_path"])
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        names = zf.namelist()
        assert "_MANIFEST.json" in names
        assert any(n.startswith("Documents/Q1/file-0.txt") for n in names)
        assert any(n.startswith("Photos/file-") for n in names)
    finally:
        await shard.close()
