"""Integration-ish test exercising the full MailExportOrchestrator with FakeShard."""
import io
import json
import zipfile

import pytest

from tests.workers.workers_restore_worker_shim import MailExportOrchestrator
from shared.export_manifest import ExportManifestBuilder


class FakeShard:
    def __init__(self):
        self.blobs = {}
        self._pending = {}

    async def stage_block(self, c, p, bid, data):
        self._pending.setdefault((c, p), {})[bid] = data

    async def commit_block_list_manual(self, c, p, bids, metadata=None):
        staged = self._pending.get((c, p), {})
        self.blobs[(c, p)] = b"".join(staged[b] for b in bids)

    async def put_block_from_url(self, c, p, bid, src_url):
        src_container, src_path = src_url.split("://", 1)[1].split("/", 1)
        self._pending.setdefault((c, p), {})[bid] = self.blobs[(src_container, src_path)]

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
        return f"fake://{c}/{p}"


class _Item:
    def __init__(self, external_id, name, folder_path, blob_path):
        self.external_id = external_id
        self.name = name
        self.folder_path = folder_path
        self.blob_path = blob_path
        self.content_size = 200
        self.attachment_blob_paths = []


async def test_orchestrator_emits_zip_with_per_folder_mbox():
    shard = FakeShard()
    for i, (folder, mid) in enumerate([("Inbox", "m1"), ("Inbox", "m2"), ("Sent", "m3")]):
        msg = {
            "id": mid,
            "subject": f"msg {i}",
            "from": {"emailAddress": {"address": "a@x.com"}},
            "toRecipients": [{"emailAddress": {"address": "b@x.com"}}],
            "body": {"contentType": "text", "content": f"body {i}"},
            "sentDateTime": "2026-04-10T12:00:00Z",
        }
        await shard.upload_blob("mailbox", f"{mid}.json", json.dumps(msg).encode())

    items = [
        _Item("m1", "msg 0", "Inbox", "m1.json"),
        _Item("m2", "msg 1", "Inbox", "m2.json"),
        _Item("m3", "msg 2", "Sent", "m3.json"),
    ]

    manifest = ExportManifestBuilder(job_id="job-x", snapshot_ids=["snap-1"])

    orch = MailExportOrchestrator(
        job_id="job-x",
        snapshot_ids=["snap-1"],
        items=items,
        shard=shard,
        source_container="mailbox",
        dest_container="exports",
        parallelism=2,
        split_bytes=10**9,
        block_size=1024,
        fetch_batch_size=10,
        queue_maxsize=5,
        format="MBOX",
        include_attachments=False,
        manifest=manifest,
    )
    final = await orch.run()
    assert final["blob_path"].endswith(".zip")
    assert final["exported_count"] == 3
    assert final["failed_count"] == 0

    zip_bytes = await shard.download_blob("exports", final["blob_path"])
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()
    assert any(n.startswith("Inbox") and n.endswith(".mbox") for n in names)
    assert any(n.startswith("Sent") and n.endswith(".mbox") for n in names)
    assert "_MANIFEST.json" in names


async def test_preflight_logs_warning_for_huge_export():
    shard = FakeShard()
    items = []
    for i in range(10):
        msg = {
            "id": f"m{i}",
            "subject": "x",
            "from": {"emailAddress": {"address": "a@x.com"}},
            "toRecipients": [{"emailAddress": {"address": "b@x.com"}}],
            "body": {"contentType": "text", "content": "x"},
            "sentDateTime": "2026-04-10T12:00:00Z",
        }
        await shard.upload_blob("mailbox", f"m{i}.json", json.dumps(msg).encode())
        it = _Item(f"m{i}", f"msg {i}", "Inbox", f"m{i}.json")
        it.content_size = 15 * 1024 * 1024 * 1024  # 15 GB each → 150 GB total
        items.append(it)

    from shared.export_manifest import ExportManifestBuilder
    manifest = ExportManifestBuilder(job_id="job-huge", snapshot_ids=["s"])
    orch = MailExportOrchestrator(
        job_id="job-huge", snapshot_ids=["s"], items=items, shard=shard,
        source_container="mailbox", dest_container="exports", parallelism=1,
        split_bytes=10**9, block_size=1024, fetch_batch_size=5, queue_maxsize=5,
        format="MBOX", include_attachments=True, manifest=manifest,
    )
    warnings = orch.preflight()
    assert any("large export" in w for w in warnings)
