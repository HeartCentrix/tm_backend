from unittest.mock import AsyncMock, MagicMock

import pytest

from services.storage_toggle_worker.preflight import run_preflight


@pytest.mark.asyncio
async def test_preflight_happy_path():
    router = MagicMock()
    target = MagicMock()
    info = MagicMock()
    info.etag = "abc"
    target.upload = AsyncMock(return_value=info)
    target.delete = AsyncMock()
    target.name = "target"
    router.get_store_by_id = MagicMock(return_value=target)

    db = AsyncMock()
    db.fetchval = AsyncMock(return_value=0.5)

    result = await run_preflight(
        router=router, db=db,
        target_backend_id="target",
        cooldown_elapsed=True,
        transition_state="stable",
    )
    assert result.ok is True
    names = [c.name for c in result.checks]
    assert "target_reachable" in names
    assert "db_replica_lag_ok" in names
    assert "no_inflight_transition" in names
    assert "cooldown_elapsed" in names


@pytest.mark.asyncio
async def test_preflight_fails_on_cooldown_and_replica_lag():
    router = MagicMock()
    target = MagicMock()
    target.upload = AsyncMock(side_effect=Exception("unreachable"))
    router.get_store_by_id = MagicMock(return_value=target)

    db = AsyncMock()
    db.fetchval = AsyncMock(return_value=120.0)  # 2 min lag

    result = await run_preflight(
        router=router, db=db,
        target_backend_id="target",
        cooldown_elapsed=False,
        transition_state="stable",
    )
    assert result.ok is False
    by_name = {c.name: c for c in result.checks}
    assert by_name["db_replica_lag_ok"].ok is False
    assert by_name["cooldown_elapsed"].ok is False
    assert by_name["target_reachable"].ok is False
