"""Tests for GET /api/v1/resources/snapshots/{id}/contact-folders."""
import importlib.util
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from httpx import AsyncClient, ASGITransport


def _load_service_main():
    name = "snapshot_service_main"
    if name in sys.modules:
        return sys.modules[name]
    svc_dir = Path(__file__).resolve().parents[2] / "services" / "snapshot-service"
    if str(svc_dir) not in sys.path:
        sys.path.insert(0, str(svc_dir))
    spec = importlib.util.spec_from_file_location(name, svc_dir / "main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def app():
    return _load_service_main().app


def _patch_db(folders):
    """Patch get_db to yield a session whose execute().scalars().all() returns folders."""
    result = MagicMock()
    result.scalars.return_value.all.return_value = folders
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)

    async def _fake_get_db():
        yield session
    return _fake_get_db, session


@pytest.mark.asyncio
async def test_returns_distinct_folders_with_well_known_first(app):
    snap_id = str(uuid.uuid4())
    fake_get_db, _ = _patch_db(
        ["Deleted Items", "Custom A", "Recipient Cache", "Contacts", "Custom B"]
    )
    svc = _load_service_main()
    app.dependency_overrides[svc.get_db] = fake_get_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.get(f"/api/v1/resources/snapshots/{snap_id}/contact-folders")
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 200
    body = r.json()
    assert body["folders"] == [
        "Contacts", "Recipient Cache", "Deleted Items",
        "Custom A", "Custom B",
    ]


@pytest.mark.asyncio
async def test_empty_snapshot_returns_empty_list(app):
    snap_id = str(uuid.uuid4())
    fake_get_db, _ = _patch_db([])
    svc = _load_service_main()
    app.dependency_overrides[svc.get_db] = fake_get_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.get(f"/api/v1/resources/snapshots/{snap_id}/contact-folders")
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 200
    assert r.json() == {"folders": []}


@pytest.mark.asyncio
async def test_invalid_snapshot_id_returns_400(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/api/v1/resources/snapshots/not-a-uuid/contact-folders")
    assert r.status_code in (400, 422)
