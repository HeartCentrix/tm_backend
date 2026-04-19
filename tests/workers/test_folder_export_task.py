"""Unit-level tests for FolderExportTask, using a fake shard that captures bytes."""
import asyncio
import json
import mailbox
from collections import defaultdict

import pytest

from tests.workers.workers_restore_worker_shim import FolderExportTask


class FakeShard:
    """In-memory stand-in for AzureStorageShard — stores staged blocks per blob
    and 'commits' them in the order requested so the resulting bytes can be
    inspected."""
    def __init__(self):
        self.blobs: dict = {}
        self._pending: dict = defaultdict(dict)

    async def stage_block(self, container, path, bid, data):
        self._pending[(container, path)][bid] = data

    async def commit_block_list_manual(self, container, path, bids, metadata=None):
        staged = self._pending[(container, path)]
        self.blobs[(container, path)] = b"".join(staged[b] for b in bids)
        staged.clear()

    async def upload_blob(self, container, path, content, metadata=None):
        self.blobs[(container, path)] = content
        return {"success": True}

    async def download_blob(self, container, path):
        return self.blobs.get((container, path))

    async def download_blob_stream(self, container, path, chunk_size=4 * 1024 * 1024):
        data = self.blobs.get((container, path), b"")
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]


class _Item:
    def __init__(self, ext_id, blob_path, name):
        self.external_id = ext_id
        self.blob_path = blob_path
        self.name = name
        self.content_size = 200
        self.attachment_blob_paths = []


async def _seed(shard):
    msg1 = {
        "id": "m1", "subject": "Hello",
        "from": {"emailAddress": {"address": "a@x.com"}},
        "toRecipients": [{"emailAddress": {"address": "b@x.com"}}],
        "body": {"contentType": "text", "content": "one"},
        "sentDateTime": "2026-04-10T12:00:00Z",
    }
    msg2 = {
        "id": "m2", "subject": "World",
        "from": {"emailAddress": {"address": "a@x.com"}},
        "toRecipients": [{"emailAddress": {"address": "b@x.com"}}],
        "body": {"contentType": "text", "content": "two"},
        "sentDateTime": "2026-04-10T12:01:00Z",
    }
    await shard.upload_blob("mailbox", "m1.json", json.dumps(msg1).encode())
    await shard.upload_blob("mailbox", "m2.json", json.dumps(msg2).encode())


async def test_folder_export_writes_valid_mbox(tmp_path):
    shard = FakeShard()
    await _seed(shard)

    items = [_Item("m1", "m1.json", "Hello"), _Item("m2", "m2.json", "World")]
    task = FolderExportTask(
        folder_name="Inbox",
        items=items,
        shard=shard,
        source_container="mailbox",
        dest_container="exports",
        dest_blob_prefix="job-1/Inbox",
        split_bytes=10**9,
        block_size=1024,
        fetch_batch_size=10,
        queue_maxsize=5,
        format="MBOX",
        include_attachments=False,
        manifest=None,
    )
    result = await task.run()

    assert len(result.produced_blobs) == 1
    produced = await shard.download_blob("exports", result.produced_blobs[0])
    fp = tmp_path / "Inbox.mbox"
    fp.write_bytes(produced)
    mbox = mailbox.mbox(str(fp))
    subjects = sorted(m["Subject"] for m in mbox)
    assert subjects == ["Hello", "World"]
