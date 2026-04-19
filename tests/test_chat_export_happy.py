"""End-to-end happy-path test for chat export.

Requires docker-compose up -d (rabbitmq, postgres, redis, azurite,
job-service, chat-export-worker). Gate with INTEGRATION=1 env var.
"""
import os, io, zipfile, asyncio
import pytest
from httpx import AsyncClient


pytestmark = pytest.mark.skipif(not os.environ.get("INTEGRATION"), reason="INTEGRATION=1 required")


@pytest.mark.asyncio
async def test_full_flow_single_thread_html(client: AsyncClient, auth_header, seeded):
    r = await client.post("/api/v1/exports/chat", headers=auth_header,
        json={"resourceId": seeded["resource_id"], "snapshotIds": [seeded["snapshot_id"]],
              "threadPath": seeded["thread_path"], "itemIds": [],
              "exportFormat": "HTML", "includeAttachments": True})
    assert r.status_code == 202
    job_id = r.json()["jobId"]

    for _ in range(60):
        s = await client.get(f"/api/v1/exports/chat/{job_id}", headers=auth_header)
        if s.json().get("status") in ("COMPLETED", "FAILED"): break
        await asyncio.sleep(1)
    body = s.json()
    assert body["status"] == "COMPLETED"
    assert body["download"]["supportsRange"] is True

    zr = await client.get(body["download"]["url"])
    assert zr.status_code == 200
    z = zipfile.ZipFile(io.BytesIO(zr.content))
    names = z.namelist()
    assert "styles.css" in names
    assert any(n.endswith(".html") for n in names)
    assert "_meta/activity.log" in names
    assert "_meta/manifest.json" in names
    for n in names:
        assert not n.startswith("/")
