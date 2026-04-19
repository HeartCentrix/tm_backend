"""_iter_pages: sticky app rotation + is_throttled check before return."""
import importlib
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_iter_pages_survives_429_then_yields(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_THROTTLE_BACKOFF_SECONDS", "0,0")
    monkeypatch.setenv("GRAPH_POST_THROTTLE_BRAKE_MS", "0")
    from shared import config
    importlib.reload(config)
    from shared import graph_client
    importlib.reload(graph_client)

    calls = []
    page_429 = MagicMock(status_code=429, headers={"Retry-After": "0"})
    page_ok = MagicMock(
        status_code=200, headers={},
        json=MagicMock(return_value={"value": [{"id": "x"}]}),
    )
    page_429.raise_for_status = MagicMock()
    page_ok.raise_for_status = MagicMock()

    async def fake_get(url, **kw):
        calls.append(url)
        return page_429 if len(calls) == 1 else page_ok

    gc = graph_client.GraphClient(
        client_id="app-1", client_secret="sec", tenant_id="tid",
    )
    gc._get_token = AsyncMock(return_value="tok")

    with patch("shared.graph_client.httpx.AsyncClient") as MockCli:
        cli = MockCli.return_value.__aenter__.return_value
        cli.get = AsyncMock(side_effect=fake_get)
        pages = []
        async for p in gc._iter_pages("https://graph.microsoft.com/v1.0/users"):
            pages.append(p)
    assert pages == [{"value": [{"id": "x"}]}]
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_iter_pages_cumulative_cap_raises(monkeypatch):
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
        client_id="app-1", client_secret="sec", tenant_id="tid",
    )
    gc._get_token = AsyncMock(return_value="tok")

    with patch("shared.graph_client.httpx.AsyncClient") as MockCli:
        cli = MockCli.return_value.__aenter__.return_value
        cli.get = AsyncMock(side_effect=fake_get)
        from shared.graph_ratelimit import GraphRetryExhaustedError
        with pytest.raises(GraphRetryExhaustedError):
            async for _ in gc._iter_pages("https://graph.microsoft.com/v1.0/users"):
                pass
