"""End-to-end: fake Graph returns 429 sequences; hardened client completes."""
import importlib
import pytest

from tests.integration.fake_graph_server import FakeGraphServer, ScriptedResponse


pytestmark = pytest.mark.integration


async def _fake_token():
    return "faketoken"


@pytest.mark.asyncio
async def test_stream_survives_retry_after_0_throttle(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_THROTTLE_BACKOFF_SECONDS", "0,0")
    monkeypatch.setenv("GRAPH_POST_THROTTLE_BRAKE_MS", "0")
    from shared import config
    importlib.reload(config)
    from shared import graph_client
    importlib.reload(graph_client)

    srv = FakeGraphServer()
    base = await srv.start()
    try:
        srv.queue(ScriptedResponse(status=429, headers={"Retry-After": "0"}))
        srv.queue(ScriptedResponse(status=429, headers={"Retry-After": "0"}))
        srv.queue(ScriptedResponse(
            status=200, body={"value": [{"id": "m1"}, {"id": "m2"}]},
        ))

        gc = graph_client.GraphClient(
            client_id="cid", client_secret="sec", tenant_id="tid",
        )
        gc._get_token = _fake_token

        msgs = []
        async for page in gc._iter_pages(f"{base}/v1.0/users"):
            msgs.extend(page.get("value", []))
        assert [m["id"] for m in msgs] == ["m1", "m2"]
        assert len(srv.calls) == 3
    finally:
        await srv.stop()


@pytest.mark.asyncio
async def test_stream_parses_http_date_retry_after(monkeypatch):
    import email.utils
    import time
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_POST_THROTTLE_BRAKE_MS", "0")
    from shared import config
    importlib.reload(config)
    from shared import graph_client
    importlib.reload(graph_client)

    srv = FakeGraphServer()
    base = await srv.start()
    try:
        future = email.utils.formatdate(time.time() + 0.1, usegmt=True)
        srv.queue(ScriptedResponse(status=429, headers={"Retry-After": future}))
        srv.queue(ScriptedResponse(status=200, body={"value": []}))

        gc = graph_client.GraphClient(
            client_id="cid", client_secret="sec", tenant_id="tid",
        )
        gc._get_token = _fake_token

        msgs = []
        async for page in gc._iter_pages(f"{base}/v1.0/users"):
            msgs.extend(page.get("value", []))
        assert msgs == []
        assert len(srv.calls) == 2
    finally:
        await srv.stop()
