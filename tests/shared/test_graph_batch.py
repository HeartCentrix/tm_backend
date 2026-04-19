"""BatchClient: chunking, 429 sub-response retry, paginated-endpoint rejection."""
import importlib
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_batch_chunks_at_20(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    from shared import config
    importlib.reload(config)
    from shared.graph_batch import BatchRequest, BatchClient
    sent_batches = []

    async def fake_post(url, json, **kw):
        sent_batches.append(json)
        responses = [
            {"id": r["id"], "status": 200, "body": {"echo": r["id"]}}
            for r in json["requests"]
        ]
        return MagicMock(
            status_code=200,
            json=MagicMock(return_value={"responses": responses}),
        )

    gc = MagicMock()
    gc._get_token = AsyncMock(return_value="tok")
    gc.client_id = "app-1"
    from shared.graph_ratelimit import RateLimitPolicy
    gc._policy = RateLimitPolicy(
        stream_rate=0.0, app_rate=0.0,
        throttle_sequence=[1], transient_sequence=[1],
        jitter_ratio=0.0, cumulative_cap_s=60,
    )

    client = BatchClient(gc)
    requests = [BatchRequest(id=f"r{i}", method="GET", url=f"/users/{i}")
                for i in range(45)]
    with patch("shared.graph_batch.httpx.AsyncClient") as M:
        cli = M.return_value.__aenter__.return_value
        cli.post = AsyncMock(side_effect=fake_post)
        result = await client.batch(requests)
    assert len(sent_batches) == 3
    assert len(result) == 45


@pytest.mark.asyncio
async def test_batch_retries_429_sub_responses(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    monkeypatch.setenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "0")
    monkeypatch.setenv("GRAPH_APP_PACE_REQS_PER_SEC", "0")
    from shared import config
    importlib.reload(config)
    from shared.graph_batch import BatchRequest, BatchClient
    call_count = {"n": 0}

    async def fake_post(url, json, **kw):
        call_count["n"] += 1
        if call_count["n"] == 1:
            responses = []
            for i, r in enumerate(json["requests"]):
                if i % 2 == 0:
                    responses.append({
                        "id": r["id"], "status": 429,
                        "headers": {"Retry-After": "0"}, "body": {},
                    })
                else:
                    responses.append({
                        "id": r["id"], "status": 200,
                        "body": {"echo": r["id"]},
                    })
            return MagicMock(
                status_code=200,
                json=MagicMock(return_value={"responses": responses}),
            )
        return MagicMock(
            status_code=200,
            json=MagicMock(return_value={"responses": [
                {"id": r["id"], "status": 200, "body": {"echo": r["id"]}}
                for r in json["requests"]
            ]}),
        )

    gc = MagicMock()
    gc._get_token = AsyncMock(return_value="tok")
    gc.client_id = "app-1"
    from shared.graph_ratelimit import RateLimitPolicy
    gc._policy = RateLimitPolicy(
        stream_rate=0.0, app_rate=0.0,
        throttle_sequence=[1], transient_sequence=[1],
        jitter_ratio=0.0, cumulative_cap_s=60,
    )

    client = BatchClient(gc)
    with patch("shared.graph_batch.httpx.AsyncClient") as M:
        cli = M.return_value.__aenter__.return_value
        cli.post = AsyncMock(side_effect=fake_post)
        requests = [BatchRequest(id=f"r{i}", method="GET", url=f"/users/{i}")
                    for i in range(10)]
        result = await client.batch(requests)
    assert len(result) == 10
    assert all(r.status == 200 for r in result.values())


def test_batch_rejects_paginated_endpoint():
    from shared.graph_batch import BatchClient, BatchRequest
    with pytest.raises(ValueError, match="paginated"):
        BatchClient.validate_requests([
            BatchRequest(id="r1", method="GET", url="/users/delta"),
        ])
