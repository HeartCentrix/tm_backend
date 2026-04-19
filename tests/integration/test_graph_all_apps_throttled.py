"""All apps throttled — cumulative cap raises GraphRetryExhaustedError."""
import importlib
import pytest

from tests.integration.fake_graph_server import FakeGraphServer, ScriptedResponse


pytestmark = pytest.mark.integration


async def _fake_token():
    return "t"


@pytest.mark.asyncio
async def test_exhaustion_raises_graph_retry_exhausted(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    # Force immediate cumulative exhaustion: one 60s backoff step, cap 30s.
    monkeypatch.setenv("GRAPH_THROTTLE_BACKOFF_SECONDS", "60")
    monkeypatch.setenv("GRAPH_MAX_CUMULATIVE_WAIT_SECONDS", "30")
    monkeypatch.setenv("GRAPH_POST_THROTTLE_BRAKE_MS", "0")
    from shared import config
    importlib.reload(config)
    from shared import multi_app_manager as mam
    importlib.reload(mam)
    from shared import graph_client
    importlib.reload(graph_client)

    srv = FakeGraphServer()
    base = await srv.start()
    try:
        for _ in range(5):
            srv.queue(ScriptedResponse(status=429))
        gc = graph_client.GraphClient(
            client_id="cid", client_secret="sec", tenant_id="tid",
        )
        gc._get_token = _fake_token

        from shared.graph_ratelimit import GraphRetryExhaustedError
        with pytest.raises(GraphRetryExhaustedError):
            async for _ in gc._iter_pages(f"{base}/v1.0/users"):
                pass
    finally:
        await srv.stop()
