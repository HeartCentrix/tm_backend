"""5 k-user simulation — 32 streams × 16 fake apps, throttle-proof verify."""
import asyncio
import importlib
import random
import time
import pytest

from tests.integration.fake_graph_server import FakeGraphServer, ScriptedResponse


pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def _fake_token():
    return "t"


@pytest.mark.asyncio
async def test_32_streams_with_20pct_throttle(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    # Realistic-ish pacing for the soak: 2 rps per stream, 5 rps per app.
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "2.0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "5.0")
    monkeypatch.setenv("GRAPH_THROTTLE_BACKOFF_SECONDS", "0,0,0")
    monkeypatch.setenv("GRAPH_MAX_CUMULATIVE_WAIT_SECONDS", "60")
    monkeypatch.setenv("GRAPH_POST_THROTTLE_BRAKE_MS", "0")
    for i in range(1, 17):
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
        # Script ~20% throttles + 80% successful single-item pages.
        for _ in range(32 * 15):
            if random.random() < 0.2:
                srv.queue(ScriptedResponse(
                    status=429, headers={"Retry-After": "0"},
                ))
            else:
                srv.queue(ScriptedResponse(
                    status=200, body={"value": [{"id": "m"}]},
                ))

        async def one_stream(sid: int):
            gc = graph_client.GraphClient(
                client_id=f"app-{(sid % 16) + 1}",
                client_secret="sec",
                tenant_id=f"tid-{(sid % 16) + 1}",
            )
            gc._get_token = _fake_token
            out = []
            try:
                async for page in gc._iter_pages(f"{base}/v1.0/users"):
                    out.extend(page.get("value", []))
                    if len(out) >= 10:
                        break
            except Exception:
                pass
            return len(out)

        t0 = time.monotonic()
        results = await asyncio.gather(*[one_stream(i) for i in range(32)])
        elapsed = time.monotonic() - t0
        assert all(n >= 1 for n in results), f"streams produced nothing: {results}"
        assert elapsed < 60, f"soak took {elapsed:.1f}s"
    finally:
        await srv.stop()
