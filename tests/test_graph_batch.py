"""Tests for the shared Graph $batch helper."""
from unittest.mock import AsyncMock
import pytest

from shared.graph_batch import batch_requests, BatchRequest


@pytest.mark.asyncio
async def test_batch_requests_chunks_into_20_per_call():
    """Graph's /$batch endpoint caps at 20 ops per request. The helper
    must chunk silently so callers can pass any list size."""
    # 45 ops → 3 chunks: 20, 20, 5.
    post = AsyncMock(side_effect=[
        {"responses": [{"id": str(i), "status": 200, "body": {"v": i}} for i in range(20)]},
        {"responses": [{"id": str(i), "status": 200, "body": {"v": i}} for i in range(20, 40)]},
        {"responses": [{"id": str(i), "status": 200, "body": {"v": i}} for i in range(40, 45)]},
    ])

    reqs = [BatchRequest(id=str(i), method="GET", url=f"/users/{i}") for i in range(45)]
    results = await batch_requests(post, reqs)

    assert post.await_count == 3
    assert len(results) == 45
    assert results[0]["body"]["v"] == 0
    assert results[44]["body"]["v"] == 44


@pytest.mark.asyncio
async def test_batch_requests_preserves_input_order_on_out_of_order_responses():
    """Graph may return /$batch responses in any order — keyed by id.
    Caller relies on list-order = input-order."""
    post = AsyncMock(return_value={"responses": [
        {"id": "2", "status": 200, "body": {"v": 2}},
        {"id": "0", "status": 200, "body": {"v": 0}},
        {"id": "1", "status": 404, "body": {"error": {"code": "NotFound"}}},
    ]})
    reqs = [
        BatchRequest(id="0", method="GET", url="/a"),
        BatchRequest(id="1", method="GET", url="/b"),
        BatchRequest(id="2", method="GET", url="/c"),
    ]
    results = await batch_requests(post, reqs)
    assert results[0]["body"]["v"] == 0
    assert results[1]["status"] == 404
    assert results[2]["body"]["v"] == 2


@pytest.mark.asyncio
async def test_batch_requests_empty_input_is_noop():
    post = AsyncMock()
    results = await batch_requests(post, [])
    assert results == []
    post.assert_not_called()
