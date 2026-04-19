import os, pytest, asyncio
pytestmark = pytest.mark.skipif(not os.environ.get("INTEGRATION"), reason="INTEGRATION=1 required")


@pytest.mark.asyncio
async def test_cancel_before_consume(client, auth_header, seeded):
    r = await client.post("/api/v1/exports/chat", headers=auth_header,
        json={"resourceId": seeded["resource_id"], "snapshotIds": [seeded["snapshot_id"]],
              "threadPath": seeded["thread_path"], "itemIds": [],
              "exportFormat": "HTML", "includeAttachments": True})
    jid = r.json()["jobId"]
    c = await client.post(f"/api/v1/exports/chat/{jid}/cancel", headers=auth_header)
    assert c.status_code == 202
    await asyncio.sleep(3)
    s = await client.get(f"/api/v1/exports/chat/{jid}", headers=auth_header)
    assert s.json()["status"] in ("CANCELLED", "CANCELLING")
