"""Unit tests for files_export_or_restore validation + family resolver.

The endpoint calls into trigger_restore for the actual queueing, which is
covered by existing integration tests. Here we exercise:
  * _resource_family collapses the Files family to a single bucket.
  * Validation returns the expected 400/404 surface for the payload
    shapes the frontend can send.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

# The job-service lives in a hyphenated dir and its main.py uses bare
# sibling imports (`from chat_export import ...`). Add the services dir
# to sys.path, then import as `main`. The workers' own main.py modules
# are loaded under `workers.restore_worker.main` (full dotted path), so
# there's no name collision so long as we don't shadow them here.
import importlib
import pathlib
import sys

_JOB_DIR = pathlib.Path(__file__).resolve().parent.parent / "services" / "job-service"
if str(_JOB_DIR) not in sys.path:
    sys.path.insert(0, str(_JOB_DIR))


def _load_job_service_main():
    # Move the job-service dir to the FRONT of sys.path so it wins over
    # any `workers/*-worker/` entries that a prior test may have
    # prepended. Without this, `import main` resolves to whichever
    # worker's main.py happens to be earlier on sys.path.
    try:
        sys.path.remove(str(_JOB_DIR))
    except ValueError:
        pass
    sys.path.insert(0, str(_JOB_DIR))

    # Drop any cached `main` that isn't the job-service one.
    existing = sys.modules.get("main")
    if existing is not None:
        existing_file = getattr(existing, "__file__", "") or ""
        if "services/job-service" not in existing_file:
            sys.modules.pop("main", None)

    import main  # type: ignore[import-not-found]
    return main


def test_resource_family_collapses_files():
    mod = _load_job_service_main()
    assert mod._resource_family("ONEDRIVE") == "FILES"
    assert mod._resource_family("USER_ONEDRIVE") == "FILES"
    assert mod._resource_family("SHAREPOINT_SITE") == "FILES"
    assert mod._resource_family("TEAMS_CHANNEL") == "FILES"
    assert mod._resource_family("M365_GROUP") == "FILES"
    assert mod._resource_family("onedrive") == "FILES"  # case-insensitive
    assert mod._resource_family("MAILBOX") == "MAILBOX"
    assert mod._resource_family("POWER_BI") == "POWER_BI"


def _fake_resource(rtype: str, tid: uuid.UUID = None):
    r = MagicMock()
    r.type = MagicMock()
    r.type.value = rtype
    r.tenant_id = tid or uuid.uuid4()
    r.id = uuid.uuid4()
    return r


@pytest.mark.asyncio
async def test_non_file_workload_returns_400():

    mod = _load_job_service_main()
    db = MagicMock()
    db.get = AsyncMock(return_value=_fake_resource("MAILBOX"))
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(uuid.uuid4()), {"restoreType": "EXPORT_ZIP"}, db)
    assert ei.value.status_code == 400
    assert "not a file workload" in ei.value.detail


@pytest.mark.asyncio
async def test_missing_snapshot_id_returns_400():

    mod = _load_job_service_main()
    db = MagicMock()
    db.get = AsyncMock(return_value=_fake_resource("SHAREPOINT_SITE"))
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(uuid.uuid4()), {"restoreType": "EXPORT_ZIP"}, db)
    assert ei.value.status_code == 400
    assert ei.value.detail == "snapshotId is required"


@pytest.mark.asyncio
async def test_in_place_without_conflict_mode_returns_400():

    mod = _load_job_service_main()
    db = MagicMock()
    db.get = AsyncMock(return_value=_fake_resource("SHAREPOINT_SITE"))
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(uuid.uuid4()), {
            "restoreType": "IN_PLACE",
            "snapshotId": str(uuid.uuid4()),
            "folderPaths": ["/Shared Documents"],
        }, db)
    assert ei.value.status_code == 400
    assert "conflictMode" in ei.value.detail


@pytest.mark.asyncio
async def test_empty_selection_returns_400():

    mod = _load_job_service_main()
    db = MagicMock()
    db.get = AsyncMock(return_value=_fake_resource("SHAREPOINT_SITE"))
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(uuid.uuid4()), {
            "restoreType": "EXPORT_ZIP",
            "snapshotId": str(uuid.uuid4()),
        }, db)
    assert ei.value.status_code == 400
    assert "Select at least one" in ei.value.detail


@pytest.mark.asyncio
async def test_cross_resource_workload_mismatch_returns_400():

    mod = _load_job_service_main()
    tenant_id = uuid.uuid4()
    source = _fake_resource("SHAREPOINT_SITE", tid=tenant_id)
    target = _fake_resource("MAILBOX", tid=tenant_id)  # wrong family
    db = MagicMock()
    db.get = AsyncMock(side_effect=[source, target])
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(source.id), {
            "restoreType": "CROSS_RESOURCE",
            "snapshotId": str(uuid.uuid4()),
            "folderPaths": ["/Shared Documents"],
            "targetResourceId": str(target.id),
        }, db)
    assert ei.value.status_code == 400
    assert "workload family mismatch" in ei.value.detail


@pytest.mark.asyncio
async def test_cross_resource_cross_tenant_returns_400():

    mod = _load_job_service_main()
    source = _fake_resource("SHAREPOINT_SITE", tid=uuid.uuid4())
    target = _fake_resource("SHAREPOINT_SITE", tid=uuid.uuid4())  # different tenant
    db = MagicMock()
    db.get = AsyncMock(side_effect=[source, target])
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(source.id), {
            "restoreType": "CROSS_RESOURCE",
            "snapshotId": str(uuid.uuid4()),
            "folderPaths": ["/Shared Documents"],
            "targetResourceId": str(target.id),
        }, db)
    assert ei.value.status_code == 400
    assert "Cross-tenant" in ei.value.detail


@pytest.mark.asyncio
async def test_happy_path_delegates_to_trigger_restore():

    mod = _load_job_service_main()
    source = _fake_resource("SHAREPOINT_SITE")
    db = MagicMock()
    db.get = AsyncMock(return_value=source)
    with patch.object(mod, "trigger_restore",
                      new=AsyncMock(return_value={"jobId": "abc", "status": "QUEUED"})) as tr:
        out = await mod.files_export_or_restore(str(source.id), {
            "restoreType": "EXPORT_ZIP",
            "snapshotId": str(uuid.uuid4()),
            "folderPaths": ["/Shared Documents/Q4"],
        }, db)

    assert out == {"jobId": "abc", "status": "QUEUED"}
    body = tr.await_args.args[0]
    assert body["restoreType"] == "EXPORT_ZIP"
    assert body["folderPaths"] == ["/Shared Documents/Q4"]
    assert body["preserveTree"] is True  # folderPaths non-empty → default True
    assert body["exportFormat"] == "ZIP"
