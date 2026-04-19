import os, pytest, asyncio
pytestmark = pytest.mark.skipif(not os.environ.get("INTEGRATION"), reason="INTEGRATION=1 required")


@pytest.mark.asyncio
async def test_missing_attachment_becomes_placeholder(client, auth_header, seeded_missing_att):
    r = await client.post("/api/v1/exports/chat", headers=auth_header,
        json={"resourceId": seeded_missing_att["resource_id"],
              "snapshotIds": [seeded_missing_att["snapshot_id"]],
              "threadPath": seeded_missing_att["thread_path"], "itemIds": [],
              "exportFormat": "HTML", "includeAttachments": True})
    assert r.status_code == 202
    jid = r.json()["jobId"]
    for _ in range(60):
        s = await client.get(f"/api/v1/exports/chat/{jid}", headers=auth_header)
        if s.json().get("status") in ("COMPLETED", "FAILED"): break
        await asyncio.sleep(1)
    # Export still succeeds — attachment soft-fails to placeholder
    assert s.json()["status"] == "COMPLETED"
