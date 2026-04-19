"""Extended multi_app_manager: per-app token bucket, is_throttled, APP_1-16 loader."""
import importlib
import pytest


def test_loader_supports_apps_1_through_16(monkeypatch):
    for i in range(1, 17):
        monkeypatch.setenv(f"APP_{i}_CLIENT_ID", f"cid-{i}")
        monkeypatch.setenv(f"APP_{i}_CLIENT_SECRET", f"sec-{i}")
        monkeypatch.setenv(f"APP_{i}_TENANT_ID", f"tid-{i}")
    from shared import config
    importlib.reload(config)
    from shared import multi_app_manager as mam
    importlib.reload(mam)
    apps = mam.multi_app_manager.apps
    assert len(apps) == 16
    assert apps[0].client_id == "cid-1"
    assert apps[15].client_id == "cid-16"


def test_is_throttled_method_reflects_throttled_until():
    import time
    from shared import multi_app_manager as mam
    importlib.reload(mam)
    if not mam.multi_app_manager.apps:
        pytest.skip("no apps configured")
    app_id = mam.multi_app_manager.apps[0].client_id
    mam.multi_app_manager.mark_throttled(app_id, retry_after_seconds=30)
    assert mam.multi_app_manager.is_app_throttled(app_id) is True
    mam.multi_app_manager.apps[0].throttled_until = time.time() - 1
    assert mam.multi_app_manager.is_app_throttled(app_id) is False


@pytest.mark.asyncio
async def test_per_app_bucket_paces(monkeypatch):
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "10.0")
    monkeypatch.setenv("APP_1_CLIENT_ID", "cid-1")
    monkeypatch.setenv("APP_1_CLIENT_SECRET", "sec-1")
    monkeypatch.setenv("APP_1_TENANT_ID", "tid-1")
    from shared import config
    importlib.reload(config)
    from shared import multi_app_manager as mam
    importlib.reload(mam)
    if not mam.multi_app_manager.apps:
        pytest.skip("no apps configured")
    app_id = mam.multi_app_manager.apps[0].client_id
    import asyncio
    t0 = asyncio.get_event_loop().time()
    await mam.multi_app_manager.acquire_app_token(app_id)
    await mam.multi_app_manager.acquire_app_token(app_id)
    elapsed = asyncio.get_event_loop().time() - t0
    assert 0.08 < elapsed < 0.25
