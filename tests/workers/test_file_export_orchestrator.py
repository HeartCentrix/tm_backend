"""Orchestrator-level tests: single-file raw-stream shortcut + multi-file ZIP."""
import io
import zipfile
from collections import defaultdict

from tests.workers.workers_restore_worker_shim import FileExportOrchestrator
from shared.export_manifest import ExportManifestBuilder


class FakeShard:
    def __init__(self, index=0):
        self.index = index
        self.shard_index = index
        self.blobs: dict = {}
        self._pending = defaultdict(dict)

    async def stage_block(self, c, p, bid, data):
        self._pending[(c, p)][bid] = data

    async def commit_block_list_manual(self, c, p, bids, metadata=None):
        staged = self._pending.get((c, p), {})
        self.blobs[(c, p)] = b"".join(staged[b] for b in bids)

    async def put_block_from_url(self, c, p, bid, src_url):
        _, rest = src_url.split("://", 1)
        _shard, container, path = rest.split("/", 2)
        self._pending[(c, p)][bid] = self.blobs[(container, path)]

    async def upload_blob(self, c, p, content, metadata=None):
        self.blobs[(c, p)] = content
        return {"success": True}

    async def download_blob(self, c, p):
        return self.blobs.get((c, p))

    async def download_blob_stream(self, c, p, chunk_size=4 * 1024 * 1024):
        data = self.blobs.get((c, p), b"")
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    async def get_blob_properties(self, c, p):
        data = self.blobs.get((c, p))
        if data is None:
            return None
        return {"size": len(data)}

    async def get_blob_url(self, c, p):
        return f"fake://shard{self.index}/{c}/{p}"

    async def get_blob_sas_url(self, c, p, valid_for_hours=6):
        return f"fake://shard{self.index}/{c}/{p}"

    async def list_blobs(self, c):
        for (cc, p) in self.blobs:
            if cc == c:
                yield p


class FakeManager:
    def __init__(self):
        self._shards = {0: FakeShard(0)}

    def get_shard_by_index(self, i):
        return self._shards[i]

    def get_shard_for_item(self, item):
        return self._shards[0]

    def get_default_shard(self):
        return self._shards[0]


class _Item:
    def __init__(self, external_id, name, folder_path, blob_path, size=100, content_type=None):
        self.external_id = external_id
        self.name = name
        self.folder_path = folder_path
        self.blob_path = blob_path
        self.content_size = size
        self.snapshot_id = "snap-1"
        self.tenant_id = "tenant-1"
        self.resource_id = "res-1"
        self.shard_index = 0
        self.extra_data = {"raw": {"file": {"mimeType": content_type}} if content_type else {}}


async def test_single_file_original_raw_stream_shortcut():
    mgr = FakeManager()
    shard = mgr.get_default_shard()
    await shard.upload_blob("backup-files-tenant", "snap/ts/id-only", b"FILE-BYTES")

    items = [_Item("id-only", "Report.xlsx", "Docs", "snap/ts/id-only", size=10,
                   content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")]
    manifest = ExportManifestBuilder(job_id="job-x", snapshot_ids=["snap-1"])

    orch = FileExportOrchestrator(
        job_id="job-x",
        snapshot_ids=["snap-1"],
        items=items,
        shard_manager=mgr,
        source_container="backup-files-tenant",
        dest_container="backup-exports-tenant",
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
    assert result["source_container"] == "backup-files-tenant"
    assert result["source_blob_path"] == "snap/ts/id-only"
    assert result["original_name"] == "Report.xlsx"
    assert result["content_type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert result["size_bytes"] == 10
    assert result["exported_count"] == 1


async def test_multi_file_zip_assembly():
    mgr = FakeManager()
    shard = mgr.get_default_shard()
    for i in range(3):
        await shard.upload_blob("backup-files-tenant", f"snap/ts/id-{i}", f"BYTES-{i}".encode())

    items = [
        _Item(f"id-{i}", f"file-{i}.txt", f"Folder/{i % 2}", f"snap/ts/id-{i}", size=7)
        for i in range(3)
    ]
    manifest = ExportManifestBuilder(job_id="job-z", snapshot_ids=["snap-1"])

    orch = FileExportOrchestrator(
        job_id="job-z",
        snapshot_ids=["snap-1"],
        items=items,
        shard_manager=mgr,
        source_container="backup-files-tenant",
        dest_container="backup-exports-tenant",
        parallelism=2,
        block_size=4096,
        fetch_batch_size=10,
        export_format="ZIP",
        missing_policy="skip",
        max_file_bytes=10 ** 12,
        path_max_len=260,
        sanitize_chars='<>:"/\\|?*',
        manifest=manifest,
    )
    result = await orch.run()
    assert result["output_mode"] == "zip"
    assert result["exported_count"] == 3
    zip_bytes = await shard.download_blob("backup-exports-tenant", result["blob_path"])
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()
    assert "_MANIFEST.json" in names
    assert any("file-0.txt" in n for n in names)
