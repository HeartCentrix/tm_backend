"""Verify _get uses RateLimitPolicy when GRAPH_HARDENING_ENABLED=true."""
import importlib
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_get_honors_retry_after_header(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_THROTTLE_BACKOFF_SECONDS", "1,1,1")
    from shared import config
    importlib.reload(config)
    from shared import graph_client
    importlib.reload(graph_client)

    responses = [
        MagicMock(
            status_code=429, headers={"Retry-After": "0"},
            json=MagicMock(return_value={}),
        ),
        MagicMock(
            status_code=200, headers={},
            json=MagicMock(return_value={"value": [{"id": "a"}]}),
        ),
    ]
    responses[0].raise_for_status = MagicMock()
    responses[1].raise_for_status = MagicMock()

    async def fake_get(*a, **kw):
        return responses.pop(0)

    gc = graph_client.GraphClient(
        client_id="cid", client_secret="sec", tenant_id="tid",
    )
    gc._get_token = AsyncMock(return_value="faketoken")

    with patch("shared.graph_client.httpx.AsyncClient") as MockCli:
        cli = MockCli.return_value.__aenter__.return_value
        cli.get = AsyncMock(side_effect=fake_get)
        out = await gc._get("https://graph.microsoft.com/v1.0/users")
    assert out.get("value") == [{"id": "a"}]


@pytest.mark.asyncio
async def test_get_exhausts_cumulative_cap_and_raises(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_THROTTLE_BACKOFF_SECONDS", "10")
    monkeypatch.setenv("GRAPH_MAX_CUMULATIVE_WAIT_SECONDS", "5")
    monkeypatch.setenv("GRAPH_POST_THROTTLE_BRAKE_MS", "0")
    from shared import config
    importlib.reload(config)
    from shared import graph_client
    importlib.reload(graph_client)

    resp = MagicMock(status_code=429, headers={})
    resp.raise_for_status = MagicMock()

    async def fake_get(*a, **kw):
        return resp

    gc = graph_client.GraphClient(
        client_id="cid", client_secret="sec", tenant_id="tid",
    )
    gc._get_token = AsyncMock(return_value="faketoken")

    with patch("shared.graph_client.httpx.AsyncClient") as MockCli:
        cli = MockCli.return_value.__aenter__.return_value
        cli.get = AsyncMock(side_effect=fake_get)
        from shared.graph_ratelimit import GraphRetryExhaustedError
        with pytest.raises(GraphRetryExhaustedError):
            await gc._get("https://graph.microsoft.com/v1.0/users")
