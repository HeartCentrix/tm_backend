"""Verify shard-aware routing: one FolderExportTask per (folder, shard) pair."""
import io
import json
import zipfile
from collections import defaultdict

import pytest

from tests.workers.workers_restore_worker_shim import MailExportOrchestrator
from shared.export_manifest import ExportManifestBuilder


_REG = {}


class FakeShard:
    def __init__(self, index):
        self.index = index
        self.shard_index = index
        self.blobs = {}
        self._pending = defaultdict(dict)

    async def stage_block(self, c, p, bid, data):
        self._pending[(c, p)][bid] = data

    async def commit_block_list_manual(self, c, p, bids, metadata=None):
        staged = self._pending[(c, p)]
        self.blobs[(c, p)] = b"".join(staged[b] for b in bids)

    async def put_block_from_url(self, c, p, bid, src_url):
        _, rest = src_url.split("://", 1)
        shard_id, container, path = rest.split("/", 2)
        src_shard = _REG[int(shard_id.replace("shard", ""))]
        self._pending[(c, p)][bid] = src_shard.blobs[(container, path)]

    async def upload_blob(self, c, p, content, metadata=None):
        self.blobs[(c, p)] = content
        return {"success": True}

    async def download_blob(self, c, p):
        return self.blobs.get((c, p))

    async def download_blob_stream(self, c, p, chunk_size=4 * 1024 * 1024):
        data = self.blobs.get((c, p), b"")
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    async def get_blob_url(self, c, p):
        return f"fake://shard{self.index}/{c}/{p}"

    async def get_blob_sas_url(self, c, p, valid_for_hours=6):
        return f"fake://shard{self.index}/{c}/{p}"

    async def get_blob_properties(self, c, p):
        data = self.blobs.get((c, p))
        if data is None:
            return None
        return {"size": len(data)}


class FakeShardManager:
    def __init__(self):
        global _REG
        _REG = {0: FakeShard(0), 1: FakeShard(1)}
        self._shards = _REG

    def get_shard_by_index(self, i: int):
        return self._shards[i]

    def get_shard_for_item(self, item):
        return self._shards[getattr(item, "shard_index", 0)]

    def get_default_shard(self):
        return self._shards[0]


class _Item:
    def __init__(self, external_id, name, folder, blob_path, shard_index):
        self.external_id = external_id
        self.name = name
        self.folder_path = folder
        self.blob_path = blob_path
        self.content_size = 100
        self.attachment_blob_paths = []
        self.shard_index = shard_index


async def test_multi_shard_export_assembles_single_zip():
    mgr = FakeShardManager()

    for shard_index, folder, mid in [(0, "Inbox", "m1"), (1, "Sent", "m2")]:
        msg = {
            "id": mid, "subject": f"msg-{mid}",
            "from": {"emailAddress": {"address": "a@x.com"}},
            "toRecipients": [{"emailAddress": {"address": "b@x.com"}}],
            "body": {"contentType": "text", "content": "b"},
            "sentDateTime": "2026-04-10T12:00:00Z",
        }
        await mgr.get_shard_by_index(shard_index).upload_blob(
            "mailbox", f"{mid}.json", json.dumps(msg).encode()
        )

    items = [
        _Item("m1", "m1", "Inbox", "m1.json", shard_index=0),
        _Item("m2", "m2", "Sent", "m2.json", shard_index=1),
    ]
    manifest = ExportManifestBuilder(job_id="job-s", snapshot_ids=["s"])

    orch = MailExportOrchestrator(
        job_id="job-s", snapshot_ids=["s"], items=items,
        shard=None,
        shard_manager=mgr,
        source_container="mailbox", dest_container="exports",
        parallelism=2, split_bytes=10**9, block_size=1024,
        fetch_batch_size=5, queue_maxsize=5,
        format="MBOX", include_attachments=False, manifest=manifest,
    )
    result = await orch.run()
    assert result["exported_count"] == 2
    dest_shard_index = result["dest_shard_index"]
    dest_shard = mgr.get_shard_by_index(dest_shard_index)

    zip_bytes = await dest_shard.download_blob("exports", result["blob_path"])
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()
    assert any("Inbox" in n for n in names)
    assert any("Sent" in n for n in names)
    assert "_MANIFEST.json" in names
