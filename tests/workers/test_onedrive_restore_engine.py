import sys
import os
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

_WORKER_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "workers", "restore-worker")
)
if _WORKER_DIR not in sys.path:
    sys.path.insert(0, _WORKER_DIR)

from onedrive_restore import (  # noqa: E402
    OneDriveRestoreEngine, Mode, SMALL_FILE_MAX_BYTES,
)


def _mk_engine(graph):
    source = SimpleNamespace(id="res1", tenant_id="t1", external_id="source-user")
    return OneDriveRestoreEngine(
        graph_client=graph,
        source_resource=source,
        target_drive_user_id="target-user",
        tenant_id="t1",
        mode=Mode.OVERWRITE,
        worker_id="wt",
    )


def _mk_item(name, size, folder_path=None, blob_path="tenant/res/snap/itm", ext_id="e"):
    return SimpleNamespace(
        id="i", external_id=ext_id, name=name,
        folder_path=folder_path, content_size=size, blob_path=blob_path,
        extra_data={"raw": {"file": {"mimeType": "application/octet-stream"}}},
    )


@pytest.mark.asyncio
async def test_small_file_uses_simple_put():
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock(return_value={"id": "drv-1", "name": "a.txt"})
    graph.upload_large_file_to_drive = AsyncMock(return_value={"id": "drv-ignored"})
    graph.patch_drive_item_file_system_info = AsyncMock()

    engine = _mk_engine(graph)
    engine._read_blob_bytes = AsyncMock(return_value=b"x" * (SMALL_FILE_MAX_BYTES - 1))

    outcome = await engine.upload_one(_mk_item("a.txt", SMALL_FILE_MAX_BYTES - 1))

    assert outcome.outcome == "overwritten"
    assert graph.upload_small_file_to_drive.await_count == 1
    assert graph.upload_large_file_to_drive.await_count == 0


@pytest.mark.asyncio
async def test_large_file_uses_upload_session():
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock(return_value={"id": "drv-bad"})
    graph.upload_large_file_to_drive = AsyncMock(return_value={"id": "drv-2", "name": "big.bin"})
    graph.patch_drive_item_file_system_info = AsyncMock()

    big = SMALL_FILE_MAX_BYTES + 10
    engine = _mk_engine(graph)
    engine._read_blob_bytes = AsyncMock(return_value=b"x" * big)

    outcome = await engine.upload_one(_mk_item("big.bin", big))

    assert outcome.outcome == "overwritten"
    assert graph.upload_small_file_to_drive.await_count == 0
    assert graph.upload_large_file_to_drive.await_count == 1


@pytest.mark.asyncio
async def test_missing_blob_is_skipped_not_failed():
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock()
    graph.upload_large_file_to_drive = AsyncMock()
    graph.patch_drive_item_file_system_info = AsyncMock()

    engine = _mk_engine(graph)
    engine._read_blob_bytes = AsyncMock(return_value=None)

    item = _mk_item("gone.txt", 100, blob_path=None)
    outcome = await engine.upload_one(item)

    assert outcome.outcome == "skipped"
    assert outcome.reason == "blob_missing"
    assert graph.upload_small_file_to_drive.await_count == 0
    assert graph.upload_large_file_to_drive.await_count == 0


@pytest.mark.asyncio
async def test_upload_patches_file_system_info_from_raw():
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock(return_value={"id": "drv-9", "name": "a.txt"})
    graph.upload_large_file_to_drive = AsyncMock()
    patched = {}

    async def patch_fsi(drive_id, drive_item_id, created_iso, modified_iso):
        patched.update(dict(drive=drive_id, item=drive_item_id,
                            created=created_iso, modified=modified_iso))
    graph.patch_drive_item_file_system_info = patch_fsi

    engine = _mk_engine(graph)
    engine._read_blob_bytes = AsyncMock(return_value=b"x" * 50)

    item = _mk_item("a.txt", 50)
    item.extra_data = {"raw": {
        "fileSystemInfo": {
            "createdDateTime": "2024-01-01T00:00:00Z",
            "lastModifiedDateTime": "2024-06-01T12:00:00Z",
        },
    }}

    outcome = await engine.upload_one(item)
    assert outcome.outcome == "overwritten"
    assert patched["item"] == "drv-9"
    assert patched["drive"] == "target-user"
    assert patched["created"] == "2024-01-01T00:00:00Z"
    assert patched["modified"] == "2024-06-01T12:00:00Z"


@pytest.mark.asyncio
async def test_run_drains_items_and_aggregates():
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock(side_effect=[
        {"id": "a", "name": "a.txt"},
        {"id": "b", "name": "b.txt"},
    ])
    graph.upload_large_file_to_drive = AsyncMock()
    graph.patch_drive_item_file_system_info = AsyncMock()

    engine = _mk_engine(graph)
    engine._read_blob_bytes = AsyncMock(side_effect=[b"1" * 10, b"2" * 10])

    items = [_mk_item("a.txt", 10, ext_id="a"), _mk_item("b.txt", 10, ext_id="b")]
    summary = await engine.run(items)

    assert summary["overwritten"] == 2
    assert summary["failed"] == 0
    assert len(summary["items"]) == 2
    assert graph.upload_small_file_to_drive.await_count == 2
