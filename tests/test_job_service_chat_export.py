"""Integration tests for /api/v1/exports/chat/*.

Gated behind INTEGRATION=1. Requires:
  - Postgres reachable per shared.config settings
  - A `client` (httpx.AsyncClient) fixture hooked into the job-service FastAPI app
  - `auth_header` / `other_auth_header` fixtures producing Bearer tokens scoped
    to the `seeded` tenant + another tenant respectively

None of those fixtures exist in the top-level conftest.py yet — T22 wires
them. Until then, tests collect-skip with a clear message. The assertions
below are the spec for what T22 must make pass.
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("INTEGRATION"),
    reason="job-service HTTP fixtures (client/auth_header) not yet wired; needs T22",
)

# Opt-in fixture import — only loaded when INTEGRATION=1 above hasn't skipped us.
pytest_plugins = ("tests.conftest_chat_export",)


@pytest.mark.asyncio
async def test_post_chat_export_rejects_multi_thread(client, auth_header, seeded):
    r = await client.post(
        "/api/v1/exports/chat",
        headers=auth_header,
        json={
            "resourceId": seeded["resource_id"],
            "snapshotIds": [seeded["snapshot_id"]],
            "threadPath": None,
            "itemIds": seeded["multi_thread_ids"],
            "exportFormat": "HTML",
            "includeAttachments": True,
        },
    )
    assert r.status_code == 400
    body = r.json()
    # FastAPI nests custom dict payloads under "detail" when raised via HTTPException.
    err_payload = body.get("detail", body)
    err = err_payload.get("error") if isinstance(err_payload, dict) else None
    assert err == "MULTI_THREAD_NOT_SUPPORTED_YET"


@pytest.mark.asyncio
async def test_post_chat_export_accepts_single_thread(client, auth_header, seeded):
    r = await client.post(
        "/api/v1/exports/chat",
        headers=auth_header,
        json={
            "resourceId": seeded["resource_id"],
            "snapshotIds": [seeded["snapshot_id"]],
            "threadPath": seeded["thread_path"],
            "itemIds": [],
            "exportFormat": "HTML",
            "includeAttachments": True,
        },
    )
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "PENDING"
    assert body["estimatedMessages"] == seeded["message_count"]


@pytest.mark.asyncio
async def test_estimate_returns_size(client, auth_header, seeded):
    r = await client.post(
        "/api/v1/exports/chat/estimate",
        headers=auth_header,
        json={
            "resourceId": seeded["resource_id"],
            "snapshotIds": [seeded["snapshot_id"]],
            "threadPath": seeded["thread_path"],
            "itemIds": [],
            "exportFormat": "HTML",
            "includeAttachments": True,
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["messages"] == seeded["message_count"]
    assert body["layoutMode"] == "single_thread"


@pytest.mark.asyncio
async def test_post_chat_export_403_when_not_owner(client, seeded, other_auth_header):
    r = await client.post(
        "/api/v1/exports/chat",
        headers=other_auth_header,
        json={
            "resourceId": seeded["resource_id"],
            "snapshotIds": [seeded["snapshot_id"]],
            "threadPath": seeded["thread_path"],
            "itemIds": [],
            "exportFormat": "HTML",
            "includeAttachments": True,
        },
    )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_idempotency_key_dedupes(client, auth_header, seeded):
    body = {
        "resourceId": seeded["resource_id"],
        "snapshotIds": [seeded["snapshot_id"]],
        "threadPath": seeded["thread_path"],
        "itemIds": [],
        "exportFormat": "HTML",
        "includeAttachments": True,
    }
    h = {**auth_header, "Idempotency-Key": "abc-123"}
    r1 = await client.post("/api/v1/exports/chat", headers=h, json=body)
    r2 = await client.post("/api/v1/exports/chat", headers=h, json=body)
    assert r1.json()["jobId"] == r2.json()["jobId"]
