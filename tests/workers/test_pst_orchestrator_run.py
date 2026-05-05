"""Tests for ``PstExportOrchestrator.run()`` — writer dispatch, ZIP
bundling, blob upload, cleanup, and progress reporting.

The orchestrator's lazy ``upload_blob_with_retry_from_file`` import
happens inside ``run()``; we patch it with ``patch.object`` on the live
``shared.azure_storage`` module loaded via the standard import machinery.

We mock the per-type writers by patching attributes on ``pst_export``
itself — the orchestrator looks them up in a local ``writer_map`` that
references the module-level names, so patching the module attribute
substitutes our fakes cleanly.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Path setup — make worker dir + repo root importable, and pre-load the
# pst_writers package so pst_export's top-level imports resolve.
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(
    os.path.join(_HERE, "..", "..", "workers", "restore-worker")
)
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# pst_convert is the bundled CLI now; production writers shell out to it
# but tests stub the writer classes themselves so the binary is never
# invoked here.


# Pre-register pst_writers as a real importable package so pst_export's
# top-level imports succeed when we load it via spec_from_file_location.
_PST_WRITERS_DIR = os.path.join(_WORKER, "pst_writers")
if "pst_writers" not in sys.modules:
    _pkg = types.ModuleType("pst_writers")
    _pkg.__path__ = [_PST_WRITERS_DIR]
    sys.modules["pst_writers"] = _pkg

if "pst_writers.base" not in sys.modules:
    _base_spec = importlib.util.spec_from_file_location(
        "pst_writers.base", os.path.join(_PST_WRITERS_DIR, "base.py")
    )
    _base_mod = importlib.util.module_from_spec(_base_spec)
    sys.modules["pst_writers.base"] = _base_mod
    _base_spec.loader.exec_module(_base_mod)


def _ensure_writer_module(name: str, filename: str):
    full = f"pst_writers.{name}"
    if full in sys.modules:
        return sys.modules[full]
    spec = importlib.util.spec_from_file_location(
        full, os.path.join(_PST_WRITERS_DIR, filename)
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[full] = mod
    spec.loader.exec_module(mod)
    return mod


_ensure_writer_module("mail", "mail.py")
_ensure_writer_module("calendar", "calendar.py")
_ensure_writer_module("contact", "contact.py")


# ---------------------------------------------------------------------------
# Load pst_export.py as a top-level module named ``pst_export``.
# ---------------------------------------------------------------------------

_PST_EXPORT_PATH = os.path.join(_WORKER, "pst_export.py")
if "pst_export" not in sys.modules:
    _spec = importlib.util.spec_from_file_location("pst_export", _PST_EXPORT_PATH)
    _pst_export_mod = importlib.util.module_from_spec(_spec)
    sys.modules["pst_export"] = _pst_export_mod
    _spec.loader.exec_module(_pst_export_mod)
else:
    _pst_export_mod = sys.modules["pst_export"]

PstExportOrchestrator = _pst_export_mod.PstExportOrchestrator
PstWriteResult = sys.modules["pst_writers.base"].PstWriteResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Item:
    """Minimal SnapshotItem-like shim — only the fields the planner uses."""

    def __init__(self, snapshot_id, external_id, item_type, folder_path="Inbox"):
        self.snapshot_id = snapshot_id
        self.external_id = external_id
        self.item_type = item_type
        self.folder_path = folder_path
        self.name = ""


_SNAP = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000000")


def _make_writer_mock(pst_path: Path):
    """Return a writer class whose .write() returns a fixed PstWriteResult.

    The orchestrator instantiates the class then awaits ``.write(...)``.
    We model that with a class whose instances expose an async ``write``
    AsyncMock, then have it create the PST file on disk so the ZIP step
    can find it.
    """

    async def fake_write(group, workdir, split_gb, shard, source_container):
        workdir = Path(workdir)
        workdir.mkdir(parents=True, exist_ok=True)
        target = workdir / pst_path.name
        target.write_bytes(b"fake-pst-bytes")
        return PstWriteResult(
            pst_paths=[target],
            item_count=1,
            failed_count=0,
        )

    class _FakeWriter:
        def __init__(self):
            self.write = AsyncMock(side_effect=fake_write)

    return _FakeWriter


@pytest.fixture
def patch_writers(tmp_path):
    """Patch all three writer classes in pst_export to file-creating fakes."""
    mail_cls = _make_writer_mock(Path("mail.pst"))
    cal_cls = _make_writer_mock(Path("calendar.pst"))
    contact_cls = _make_writer_mock(Path("contacts.pst"))
    with patch.object(_pst_export_mod, "MailPstWriter", mail_cls), \
         patch.object(_pst_export_mod, "CalendarPstWriter", cal_cls), \
         patch.object(_pst_export_mod, "ContactPstWriter", contact_cls):
        yield {
            "EMAIL": mail_cls,
            "CALENDAR_EVENT": cal_cls,
            "USER_CONTACT": contact_cls,
        }


@pytest.fixture
def fake_shard():
    shard = MagicMock(name="AzureStorageShard")
    shard.ensure_container = AsyncMock(return_value=None)
    return shard


@pytest.fixture
def fake_upload_success():
    """Default upload mock — returns success."""
    import shared.azure_storage as az
    mock = AsyncMock(return_value={"success": True, "blob_path": "ok"})
    with patch.object(az, "upload_blob_with_retry_from_file", new=mock):
        yield mock


@pytest.fixture
def fake_upload_failure():
    import shared.azure_storage as az
    mock = AsyncMock(return_value={"success": False, "error": "boom"})
    with patch.object(az, "upload_blob_with_retry_from_file", new=mock):
        yield mock


@pytest.fixture(autouse=True)
def isolate_workdir(tmp_path, monkeypatch):
    """Redirect the orchestrator's workdir to tmp_path so we don't pollute
    /tmp during tests and so the ZIP lands in a writable directory."""
    # Patch the Path constructor used in __init__ via attribute swap.
    real_init = PstExportOrchestrator.__init__

    def patched_init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        self.workdir = tmp_path / "pst-export" / self.job_id

    monkeypatch.setattr(PstExportOrchestrator, "__init__", patched_init)
    yield


# ---------------------------------------------------------------------------
# Test 1 — happy path returns done + blob_path + counts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_returns_done_with_blob_path_and_counts(
    patch_writers, fake_shard, fake_upload_success
):
    items = [
        _Item(_SNAP, "e1", "EMAIL"),
        _Item(_SNAP, "c1", "CALENDAR_EVENT"),
        _Item(_SNAP, "ct1", "USER_CONTACT"),
    ]
    orch = PstExportOrchestrator(
        job_id="job-123",
        items=items,
        spec={"pstGranularity": "MAILBOX"},
        storage_shard=fake_shard,
        dest_container="my-exports",
        source_container="src",
        tenant_id="tenant-x",
    )
    result = await orch.run()

    assert result["status"] == "done"
    assert result["job_id"] == "job-123"
    assert result["blob_path"] == "pst-exports/job-123/job-123.zip"
    assert result["container"] == "my-exports"
    assert result["pst_count"] == 3
    assert sorted(result["pst_files"]) == sorted(
        ["mail.pst", "calendar.pst", "contacts.pst"]
    )
    assert result["item_counts_by_type"] == {
        "EMAIL": 1,
        "CALENDAR_EVENT": 1,
        "USER_CONTACT": 1,
    }
    assert result["failed_counts_by_type"] == {
        "EMAIL": 0,
        "CALENDAR_EVENT": 0,
        "USER_CONTACT": 0,
    }
    assert result["granularity"] == "MAILBOX"
    assert result["total_size_bytes"] > 0


# ---------------------------------------------------------------------------
# Test 2 — upload_blob_with_retry_from_file called with correct args
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_calls_upload_with_correct_container_and_blob_path(
    patch_writers, fake_shard, fake_upload_success
):
    orch = PstExportOrchestrator(
        job_id="job-upload",
        items=[_Item(_SNAP, "e1", "EMAIL")],
        spec={"pstGranularity": "MAILBOX"},
        storage_shard=fake_shard,
        dest_container="custom-exports",
    )
    await orch.run()

    fake_upload_success.assert_awaited_once()
    _, kwargs = fake_upload_success.call_args
    assert kwargs["container_name"] == "custom-exports"
    assert kwargs["blob_path"] == "pst-exports/job-upload/job-upload.zip"
    assert kwargs["shard"] is fake_shard
    assert kwargs["metadata"] == {
        "job_id": "job-upload",
        "granularity": "MAILBOX",
    }


# ---------------------------------------------------------------------------
# Test 4 — update_progress callback called with monotonically non-decreasing
#          values across the 5 → 100 range
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_invokes_update_progress_callback_with_increasing_values(
    patch_writers, fake_shard, fake_upload_success
):
    seen: list = []

    async def progress(pct):
        seen.append(pct)

    items = [
        _Item(_SNAP, "e1", "EMAIL"),
        _Item(_SNAP, "c1", "CALENDAR_EVENT"),
    ]
    orch = PstExportOrchestrator(
        job_id="job-prog",
        items=items,
        spec={},
        storage_shard=fake_shard,
        dest_container="exports-p",
        update_progress=progress,
    )
    await orch.run()

    # Check first / last and bounds
    assert seen[0] == 5
    assert seen[-1] == 100
    assert 95 in seen
    # All in 0..100, monotonically non-decreasing
    for a, b in zip(seen, seen[1:]):
        assert a <= b
    # Some pct should have landed in the writer-progress band 5..80
    assert any(5 < p <= 80 for p in seen)


# ---------------------------------------------------------------------------
# Test 5 — upload failure → status="upload_failed" and blob_path=None
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_returns_upload_failed_when_upload_fails(
    patch_writers, fake_shard, fake_upload_failure
):
    orch = PstExportOrchestrator(
        job_id="job-fail",
        items=[_Item(_SNAP, "e1", "EMAIL")],
        spec={},
        storage_shard=fake_shard,
        dest_container="exports-f",
    )
    result = await orch.run()

    assert result["status"] == "upload_failed"
    assert result["blob_path"] is None
    # Container is still recorded so callers can re-attempt against it.
    assert result["container"] == "exports-f"


# ---------------------------------------------------------------------------
# Test 6 — empty items → pst_count=0, status=done (empty zip uploaded)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_empty_items_yields_zero_psts_but_still_done(
    patch_writers, fake_shard, fake_upload_success
):
    orch = PstExportOrchestrator(
        job_id="job-empty",
        items=[],
        spec={},
        storage_shard=fake_shard,
        dest_container="exports-e",
    )
    result = await orch.run()

    assert result["pst_count"] == 0
    assert result["pst_files"] == []
    assert result["item_counts_by_type"] == {}
    assert result["failed_counts_by_type"] == {}
    assert result["status"] == "done"
    # An empty zip was still uploaded
    fake_upload_success.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test 7 — _cleanup_dir invoked on workdir after upload
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_cleans_up_workdir_after_upload(
    patch_writers, fake_shard, fake_upload_success
):
    orch = PstExportOrchestrator(
        job_id="job-clean",
        items=[_Item(_SNAP, "e1", "EMAIL")],
        spec={},
        storage_shard=fake_shard,
        dest_container="exports-cl",
    )
    expected_workdir = orch.workdir

    with patch.object(_pst_export_mod, "_cleanup_dir") as cleanup_mock:
        await orch.run()

    cleanup_mock.assert_called_once_with(expected_workdir)


# ---------------------------------------------------------------------------
# Test 8 — when storage_shard is None, falls back to azure_storage_manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_falls_back_to_azure_storage_manager_when_shard_none(
    patch_writers, fake_upload_success
):
    fake_shard = MagicMock(name="default_shard")
    fake_shard.ensure_container = AsyncMock(return_value=None)

    fake_mgr = MagicMock()
    fake_mgr.get_default_shard = MagicMock(return_value=fake_shard)
    fake_mgr.get_container_name = MagicMock(return_value="backup-exports-abcd1234")

    import shared.azure_storage as az
    with patch.object(az, "azure_storage_manager", fake_mgr):
        orch = PstExportOrchestrator(
            job_id="job-mgr",
            items=[_Item(_SNAP, "e1", "EMAIL")],
            spec={},
            tenant_id="tenant-z",
            # leave dest_container at default "exports" so fallback triggers
        )
        result = await orch.run()

    fake_mgr.get_default_shard.assert_called_once()
    fake_mgr.get_container_name.assert_called_once_with("tenant-z", "exports")
    fake_shard.ensure_container.assert_awaited_once_with("backup-exports-abcd1234")
    assert result["container"] == "backup-exports-abcd1234"
    assert result["status"] == "done"
