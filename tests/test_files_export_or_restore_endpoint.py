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

# The module's path is hyphenated — load it the same way the rest of
# the tests do (by adding the services dir to sys.path).
import pathlib
import sys
_JOB_DIR = pathlib.Path(__file__).resolve().parent.parent / "services" / "job-service"
if str(_JOB_DIR) not in sys.path:
    sys.path.insert(0, str(_JOB_DIR))


def test_resource_family_collapses_files():
    from importlib import import_module
    mod = import_module("main")  # services/job-service/main.py
    assert mod._resource_family("ONEDRIVE") == "FILES"
    assert mod._resource_family("SHAREPOINT") == "FILES"
    assert mod._resource_family("TEAMS") == "FILES"
    assert mod._resource_family("GROUP") == "FILES"
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
    from importlib import import_module
    mod = import_module("main")
    db = MagicMock()
    db.get = AsyncMock(return_value=_fake_resource("MAILBOX"))
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(uuid.uuid4()), {"restoreType": "EXPORT_ZIP"}, db)
    assert ei.value.status_code == 400
    assert "not a file workload" in ei.value.detail


@pytest.mark.asyncio
async def test_missing_snapshot_id_returns_400():
    from importlib import import_module
    mod = import_module("main")
    db = MagicMock()
    db.get = AsyncMock(return_value=_fake_resource("SHAREPOINT"))
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(uuid.uuid4()), {"restoreType": "EXPORT_ZIP"}, db)
    assert ei.value.status_code == 400
    assert ei.value.detail == "snapshotId is required"


@pytest.mark.asyncio
async def test_in_place_without_conflict_mode_returns_400():
    from importlib import import_module
    mod = import_module("main")
    db = MagicMock()
    db.get = AsyncMock(return_value=_fake_resource("SHAREPOINT"))
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
    from importlib import import_module
    mod = import_module("main")
    db = MagicMock()
    db.get = AsyncMock(return_value=_fake_resource("SHAREPOINT"))
    with pytest.raises(HTTPException) as ei:
        await mod.files_export_or_restore(str(uuid.uuid4()), {
            "restoreType": "EXPORT_ZIP",
            "snapshotId": str(uuid.uuid4()),
        }, db)
    assert ei.value.status_code == 400
    assert "Select at least one" in ei.value.detail


@pytest.mark.asyncio
async def test_cross_resource_workload_mismatch_returns_400():
    from importlib import import_module
    mod = import_module("main")
    tenant_id = uuid.uuid4()
    source = _fake_resource("SHAREPOINT", tid=tenant_id)
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
    from importlib import import_module
    mod = import_module("main")
    source = _fake_resource("SHAREPOINT", tid=uuid.uuid4())
    target = _fake_resource("SHAREPOINT", tid=uuid.uuid4())  # different tenant
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
    from importlib import import_module
    mod = import_module("main")
    source = _fake_resource("SHAREPOINT")
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
