"""Unit tests for the standalone fetch_message / gather_attachments helpers in
``workers/restore-worker/mail_fetch.py``.

These functions used to live as private methods on ``FolderExportTask``;
extracting them lets PST writers reuse the exact same fetch logic.  The
tests below mock a minimal ``shard`` interface (``download_blob``,
``download_blob_stream``, ``list_blobs``) and verify each priority path.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

# mail_fetch.py lives under workers/restore-worker which is a hyphenated dir
# and not a Python package — manually expose it.
_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(
    os.path.join(_HERE, "..", "..", "workers", "restore-worker")
)
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)

# Also make repo-root importable for `shared.mime_builder`.
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import mail_fetch  # noqa: E402
from shared.mime_builder import AttachmentRef  # noqa: E402


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class _Item:
    """Minimal SnapshotItem-like object."""

    def __init__(
        self,
        external_id: str = "ext-1",
        blob_path=None,
        extra_data=None,
        snapshot_id="snap-1",
    ):
        self.external_id = external_id
        self.blob_path = blob_path
        self.extra_data = extra_data
        self.snapshot_id = snapshot_id


class _Shard:
    """In-memory shard recording each call so tests can assert against it."""

    def __init__(self, blobs=None, list_blobs_data=None):
        self._blobs = blobs or {}
        self._list_data = list_blobs_data or {}
        self.download_calls = []
        self.download_stream_calls = []
        self.list_calls = []

    async def download_blob(self, container, path):
        self.download_calls.append((container, path))
        return self._blobs.get((container, path))

    async def download_blob_stream(self, container, path, chunk_size=4 * 1024 * 1024):
        self.download_stream_calls.append((container, path))
        data = self._blobs.get((container, path), b"")
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    async def list_blobs(self, container):
        self.list_calls.append(container)
        for name in self._list_data.get(container, []):
            yield name


# ---------------------------------------------------------------------------
# fetch_message
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_message_returns_inline_extra_data_raw():
    raw = {"id": "m1", "subject": "hi", "body": {"contentType": "text", "content": "x"}}
    item = _Item(extra_data={"raw": raw})
    shard = _Shard()

    result = await mail_fetch.fetch_message(item, shard, "mail-c")

    assert result == raw
    # Inline path must NOT touch the shard
    assert shard.download_calls == []
    assert shard.list_calls == []


@pytest.mark.asyncio
async def test_fetch_message_downloads_from_blob_path():
    msg = {"id": "m2", "subject": "blob-load"}
    blob_bytes = json.dumps(msg).encode("utf-8")
    shard = _Shard(blobs={("mail-c", "snap1/m2.json"): blob_bytes})
    item = _Item(blob_path="snap1/m2.json", extra_data=None)

    result = await mail_fetch.fetch_message(item, shard, "mail-c")

    assert result == msg
    assert shard.download_calls == [("mail-c", "snap1/m2.json")]
    # No legacy fallback list when blob_path is present
    assert shard.list_calls == []


@pytest.mark.asyncio
async def test_fetch_message_returns_none_when_no_blob_and_legacy_finds_nothing():
    # No inline raw, no blob_path, list returns no matching blob
    shard = _Shard(list_blobs_data={"mail-c": ["snap1/other.json", "snap1/another.json"]})
    item = _Item(external_id="missing-id", blob_path=None, extra_data=None, snapshot_id="snap1")

    result = await mail_fetch.fetch_message(item, shard, "mail-c")

    assert result is None
    # The legacy fallback should have at least attempted to list
    assert shard.list_calls == ["mail-c"]
    # No download should happen because nothing matched
    assert shard.download_calls == []


@pytest.mark.asyncio
async def test_fetch_message_legacy_fallback_recovers_blob():
    """When blob_path is null but the legacy list-and-match finds the blob."""
    msg = {"id": "found", "subject": "recovered"}
    blob_path = "tenant/res/snap1/2026/found-id"
    shard = _Shard(
        blobs={("mail-c", blob_path): json.dumps(msg).encode("utf-8")},
        list_blobs_data={"mail-c": [blob_path, "snap1/other"]},
    )
    item = _Item(external_id="found-id", blob_path=None, extra_data=None, snapshot_id="snap1")

    result = await mail_fetch.fetch_message(item, shard, "mail-c")

    assert result == msg
    assert shard.list_calls == ["mail-c"]
    assert shard.download_calls == [("mail-c", blob_path)]


# ---------------------------------------------------------------------------
# gather_attachments
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gather_attachments_returns_empty_when_include_disabled():
    shard = _Shard()
    out = await mail_fetch.gather_attachments(
        ["snap1/atts/a.bin"], shard, "mail-c", include_attachments=False
    )
    assert out == []
    assert shard.download_stream_calls == []


@pytest.mark.asyncio
async def test_gather_attachments_returns_empty_when_paths_empty():
    shard = _Shard()
    out = await mail_fetch.gather_attachments([], shard, "mail-c", include_attachments=True)
    assert out == []
    assert shard.download_stream_calls == []


@pytest.mark.asyncio
async def test_gather_attachments_returns_attachmentref_per_path():
    payload = b"PK\x03\x04binary-bytes"
    paths = [
        "snap1/atts/file1.docx",
        "snap1/atts/file2.pdf",
    ]
    shard = _Shard(
        blobs={("mail-c", paths[0]): payload, ("mail-c", paths[1]): payload + b"-2"}
    )

    out = await mail_fetch.gather_attachments(paths, shard, "mail-c", include_attachments=True)

    assert len(out) == 2
    assert all(isinstance(a, AttachmentRef) for a in out)
    assert out[0].name == "file1.docx"
    assert out[1].name == "file2.pdf"
    assert all(a.content_type == "application/octet-stream" for a in out)
    # data_stream is an async generator — confirm it actually pulls from the shard
    chunks_a = []
    async for ch in out[0].data_stream:
        chunks_a.append(ch)
    chunks_b = []
    async for ch in out[1].data_stream:
        chunks_b.append(ch)
    assert b"".join(chunks_a) == payload
    assert b"".join(chunks_b) == payload + b"-2"
    # And the stream method was called for each path (in order)
    assert shard.download_stream_calls == [
        ("mail-c", paths[0]),
        ("mail-c", paths[1]),
    ]
