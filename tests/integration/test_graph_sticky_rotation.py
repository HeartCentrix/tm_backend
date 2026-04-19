"""Sticky rotation: stream pinned to app-1, 429s, migrates to app-2."""
import importlib
import pytest

from tests.integration.fake_graph_server import FakeGraphServer, ScriptedResponse


pytestmark = pytest.mark.integration


async def _fake_token():
    return "t"


@pytest.mark.asyncio
async def test_migrate_on_throttle(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_STICKY_PAGES_BEFORE_RETURN", "2")
    monkeypatch.setenv("GRAPH_THROTTLE_BACKOFF_SECONDS", "0")
    monkeypatch.setenv("GRAPH_POST_THROTTLE_BRAKE_MS", "0")
    for i in (1, 2, 3):
        monkeypatch.setenv(f"APP_{i}_CLIENT_ID", f"app-{i}")
        monkeypatch.setenv(f"APP_{i}_CLIENT_SECRET", f"sec-{i}")
        monkeypatch.setenv(f"APP_{i}_TENANT_ID", f"tid-{i}")
    from shared import config
    importlib.reload(config)
    from shared import multi_app_manager as mam
    importlib.reload(mam)
    from shared import graph_client
    importlib.reload(graph_client)

    srv = FakeGraphServer()
    base = await srv.start()
    try:
        # Page 1 ok, page 2 → 429 triggers migration, then 2 pages ok,
        # terminal page closes the stream.
        srv.queue(ScriptedResponse(
            status=200,
            body={"value": [{"id": "m1"}],
                  "@odata.nextLink": f"{base}/v1.0/next"},
        ))
        srv.queue(ScriptedResponse(status=429, headers={"Retry-After": "0"}))
        srv.queue(ScriptedResponse(
            status=200,
            body={"value": [{"id": "m2"}],
                  "@odata.nextLink": f"{base}/v1.0/next"},
        ))
        srv.queue(ScriptedResponse(status=200, body={"value": [{"id": "m3"}]}))

        gc = graph_client.GraphClient(
            client_id="app-1", client_secret="sec-1", tenant_id="tid-1",
        )
        gc._get_token = _fake_token

        ids = []
        async for page in gc._iter_pages(f"{base}/v1.0/users"):
            ids.extend(m["id"] for m in page.get("value", []))
        assert ids == ["m1", "m2", "m3"]
    finally:
        await srv.stop()
