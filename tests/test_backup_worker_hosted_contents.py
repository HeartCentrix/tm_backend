"""Tests for BackupWorker._userchats_backup_hosted_contents inline-image capture."""
import importlib.util
import pathlib
import sys

import pytest
from unittest.mock import AsyncMock, MagicMock


def _load_backup_worker_module():
    """The backup-worker directory has a hyphen, so it isn't importable via
    the normal `workers.backup_worker.main` path. Load it via importlib so the
    tests can reach `BackupWorker` without renaming the on-disk directory."""
    here = pathlib.Path(__file__).resolve().parent
    main_path = here.parent / "workers" / "backup-worker" / "main.py"
    spec = importlib.util.spec_from_file_location(
        "workers_backup_worker_main", str(main_path)
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_bw_mod = _load_backup_worker_module()
BackupWorker = _bw_mod.BackupWorker


@pytest.mark.asyncio
async def test_userchats_backup_hosted_contents_creates_one_item_per_image(monkeypatch):
    worker = BackupWorker.__new__(BackupWorker)  # skip __init__
    worker.graph = MagicMock()

    async def fake_iter():
        yield b"PNGDATA"
    worker.graph.get_hosted_content = AsyncMock(return_value=(fake_iter(), "image/png", 7))

    worker._exists_hosted_item = AsyncMock(return_value=False)
    uploaded = []
    async def fake_upload(blob_path, stream, **kw):
        uploaded.append((blob_path, stream))
    worker._upload_stream = AsyncMock(side_effect=fake_upload)
    inserted = []
    async def fake_insert(**kw):
        inserted.append(kw)
    worker._insert_snapshot_item = AsyncMock(side_effect=fake_insert)

    items, bytes_ = await worker._userchats_backup_hosted_contents(
        snapshot_id="snap",
        user_id="u",
        chat_id="c",
        message_id="m",
        hosted_contents=[{"id": "h1"}, {"id": "h2"}],
    )
    assert items == 2
    assert bytes_ == 14
    assert len(inserted) == 2
    assert inserted[0]["item_type"] == "CHAT_HOSTED_CONTENT"
    assert inserted[0]["external_id"] == "m:h1"
    assert inserted[0]["parent_external_id"] == "m"
    assert inserted[0]["blob_path"] == "users/u/chats/c/messages/m/hosted/h1"


@pytest.mark.asyncio
async def test_userchats_backup_hosted_contents_is_idempotent():
    worker = BackupWorker.__new__(BackupWorker)
    worker.graph = MagicMock()
    worker._exists_hosted_item = AsyncMock(return_value=True)
    worker._upload_stream = AsyncMock()
    worker._insert_snapshot_item = AsyncMock()

    items, bytes_ = await worker._userchats_backup_hosted_contents(
        snapshot_id="snap",
        user_id="u",
        chat_id="c",
        message_id="m",
        hosted_contents=[{"id": "h1"}, {"id": "h2"}],
    )
    assert items == 0 and bytes_ == 0
    worker._upload_stream.assert_not_awaited()
    worker._insert_snapshot_item.assert_not_awaited()
