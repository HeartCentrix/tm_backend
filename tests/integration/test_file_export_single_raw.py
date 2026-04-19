"""End-to-end single-file ORIGINAL export: FileExportOrchestrator writes
raw_single Job.result. Verifies against real Azure / Azurite via
AZURE_TEST_CONNECTION_STRING."""
import pytest

from shared.azure_storage import AzureStorageShard
from shared.export_manifest import ExportManifestBuilder
from tests.workers.workers_restore_worker_shim import FileExportOrchestrator


pytestmark = pytest.mark.integration


class _Item:
    def __init__(self, ext_id, name, folder, blob_path, size, content_type):
        self.external_id = ext_id
        self.name = name
        self.folder_path = folder
        self.blob_path = blob_path
        self.content_size = size
        self.snapshot_id = "snap-1"
        self.tenant_id = "tenant-1"
        self.resource_id = "res-1"
        self.shard_index = 0
        self.extra_data = {"raw": {"file": {"mimeType": content_type}}}


class _Manager:
    def __init__(self, shard):
        self._shard = shard

    def get_shard_by_index(self, i):
        return self._shard

    def get_shard_for_item(self, item):
        return self._shard

    def get_default_shard(self):
        return self._shard


async def test_raw_single_end_to_end(azure_test_connection_string):
    shard = AzureStorageShard.from_connection_string(azure_test_connection_string)
    try:
        await shard.ensure_container("backup-files-tenantx")
        payload = b"HELLO-FILE-BYTES" * 100
        await shard.upload_blob("backup-files-tenantx", "snap-1/ts/item-1", payload)

        manifest = ExportManifestBuilder(job_id="job-rs", snapshot_ids=["snap-1"])
        item = _Item(
            "item-1", "Hello.txt", "Documents", "snap-1/ts/item-1",
            len(payload), "text/plain",
        )

        orch = FileExportOrchestrator(
            job_id="job-rs",
            snapshot_ids=["snap-1"],
            items=[item],
            shard_manager=_Manager(shard),
            source_container="backup-files-tenantx",
            dest_container="backup-exports-tenantx",
            parallelism=2,
            block_size=4096,
            fetch_batch_size=10,
            export_format="ORIGINAL",
            missing_policy="skip",
            max_file_bytes=10 ** 12,
            path_max_len=260,
            sanitize_chars='<>:"/\\|?*',
            manifest=manifest,
        )
        result = await orch.run()
        assert result["output_mode"] == "raw_single"
        assert result["source_container"] == "backup-files-tenantx"
        assert result["source_blob_path"] == "snap-1/ts/item-1"
        assert result["original_name"] == "Hello.txt"
        assert result["content_type"] == "text/plain"
        assert result["size_bytes"] == len(payload)
    finally:
        await shard.close()
