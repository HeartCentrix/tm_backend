"""Unit tests for fetch_snapshot_items folder-scope branch.

Mocks the resolver so we can verify the dispatch logic of
fetch_snapshot_items without depending on Postgres.
"""
from __future__ import annotations

import pathlib
import sys
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Make sibling modules inside `workers/restore-worker/` importable by
# their bare names (the worker process cd's into that dir, so its code
# uses `from mail_restore import ...`).
_WORKER_DIR = pathlib.Path(__file__).resolve().parent.parent / "workers" / "restore-worker"
if str(_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKER_DIR))

import tests.conftest  # noqa: F401,E402 — triggers worker alias registration
from workers.restore_worker.main import RestoreWorker  # noqa: E402


def _bare_worker() -> RestoreWorker:
    """Bypass __init__ — we only exercise fetch_snapshot_items."""
    return RestoreWorker.__new__(RestoreWorker)


@pytest.mark.asyncio
async def test_folder_paths_route_through_resolver():
    worker = _bare_worker()
    session = MagicMock()
    snap_id = str(uuid.uuid4())

    fake_rows = [MagicMock(id=uuid.uuid4()), MagicMock(id=uuid.uuid4())]
    with patch("shared.folder_resolver.resolve_selection",
               new=AsyncMock(return_value=fake_rows)) as mock_resolve:
        result = await worker.fetch_snapshot_items(
            session,
            snapshot_ids=[snap_id],
            item_ids=[],
            folder_paths=["/Shared Documents/Q4"],
            excluded_item_ids=[],
        )

    assert result == fake_rows
    mock_resolve.assert_awaited_once()
    kwargs = mock_resolve.await_args.kwargs
    assert kwargs["snapshot_id"] == snap_id
    assert kwargs["folder_paths"] == ["/Shared Documents/Q4"]
    assert kwargs["excluded_item_ids"] == []


@pytest.mark.asyncio
async def test_excluded_item_ids_alone_routes_through_resolver():
    """Even with no folder_paths but excluded_item_ids set, the resolver
    branch is chosen — the legacy id-only path can't express exclusion."""
    worker = _bare_worker()
    session = MagicMock()
    snap_id = str(uuid.uuid4())

    with patch("shared.folder_resolver.resolve_selection",
               new=AsyncMock(return_value=[])) as mock_resolve:
        await worker.fetch_snapshot_items(
            session,
            snapshot_ids=[snap_id],
            item_ids=[str(uuid.uuid4())],
            folder_paths=[],
            excluded_item_ids=[str(uuid.uuid4())],
        )

    mock_resolve.assert_awaited_once()


@pytest.mark.asyncio
async def test_item_ids_only_uses_legacy_path():
    """No folder scope + only item_ids → original strict lookup path.
    Resolver must NOT be called."""
    worker = _bare_worker()
    # Build a session that returns a scalars().all() chain with one row.
    row = MagicMock(id=uuid.uuid4())
    scalars = MagicMock()
    scalars.all.return_value = [row]
    exec_result = MagicMock()
    exec_result.scalars.return_value = scalars
    session = MagicMock()
    session.execute = AsyncMock(return_value=exec_result)

    with patch("shared.folder_resolver.resolve_selection",
               new=AsyncMock()) as mock_resolve:
        result = await worker.fetch_snapshot_items(
            session,
            snapshot_ids=[str(uuid.uuid4())],
            item_ids=[str(uuid.uuid4())],
        )

    mock_resolve.assert_not_awaited()
    assert result == [row]


@pytest.mark.asyncio
async def test_folder_paths_with_empty_snapshot_ids_returns_empty():
    worker = _bare_worker()
    session = MagicMock()
    with patch("shared.folder_resolver.resolve_selection",
               new=AsyncMock(return_value=["should-not-see-me"])) as mock_resolve:
        result = await worker.fetch_snapshot_items(
            session,
            snapshot_ids=[],
            item_ids=[],
            folder_paths=["/Foo"],
            excluded_item_ids=[],
        )
    assert result == []
    mock_resolve.assert_not_awaited()
