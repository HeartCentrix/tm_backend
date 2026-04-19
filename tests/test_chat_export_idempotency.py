import os, pytest
pytestmark = pytest.mark.skipif(not os.environ.get("INTEGRATION"), reason="INTEGRATION=1 required")


@pytest.mark.asyncio
async def test_same_idempotency_key_returns_same_job(client, auth_header, seeded):
    body = {"resourceId": seeded["resource_id"], "snapshotIds": [seeded["snapshot_id"]],
            "threadPath": seeded["thread_path"], "itemIds": [],
            "exportFormat": "HTML", "includeAttachments": True}
    h = {**auth_header, "Idempotency-Key": "same-key"}
    r1 = await client.post("/api/v1/exports/chat", headers=h, json=body)
    r2 = await client.post("/api/v1/exports/chat", headers=h, json=body)
    assert r1.json()["jobId"] == r2.json()["jobId"]
