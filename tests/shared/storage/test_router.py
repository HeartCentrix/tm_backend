"""StorageRouter unit tests — mocked backends, no IO."""
from unittest.mock import MagicMock

import pytest

from shared.storage.errors import (
    BackendNotFoundError,
    TransitionInProgressError,
)
from shared.storage.router import StorageRouter


def _mock_store(backend_id: str, name: str):
    s = MagicMock()
    s.backend_id = backend_id
    s.name = name
    s.kind = "mock"
    return s


@pytest.mark.asyncio
async def test_router_returns_active_store_when_stable():
    r = StorageRouter()
    az = _mock_store("az", "azure")
    sw = _mock_store("sw", "seaweedfs")
    r._backends = {"az": az, "sw": sw}
    r._active_backend_id = "az"
    r._transition_state = "stable"
    assert r.get_active_store() is az


@pytest.mark.asyncio
async def test_router_blocks_active_store_during_draining():
    r = StorageRouter()
    r._backends = {"az": _mock_store("az", "azure")}
    r._active_backend_id = "az"
    r._transition_state = "draining"
    with pytest.raises(TransitionInProgressError):
        r.get_active_store()


@pytest.mark.asyncio
async def test_router_get_by_id_ignores_transition():
    r = StorageRouter()
    az = _mock_store("az", "azure")
    r._backends = {"az": az}
    r._active_backend_id = "az"
    r._transition_state = "flipping"
    assert r.get_store_by_id("az") is az


@pytest.mark.asyncio
async def test_router_get_unknown_id_raises():
    r = StorageRouter()
    r._backends = {}
    with pytest.raises(BackendNotFoundError):
        r.get_store_by_id("nope")


@pytest.mark.asyncio
async def test_router_get_store_for_item():
    r = StorageRouter()
    sw = _mock_store("sw", "seaweedfs")
    r._backends = {"sw": sw}
    item = MagicMock()
    item.backend_id = "sw"
    assert r.get_store_for_item(item) is sw


@pytest.mark.asyncio
async def test_router_writable_reflects_state():
    r = StorageRouter()
    r._transition_state = "stable"
    assert r.writable() is True
    r._transition_state = "draining"
    assert r.writable() is False
    r._transition_state = "flipping"
    assert r.writable() is False
