import pytest
from unittest.mock import AsyncMock

from shared.graph_client import GraphClient


@pytest.fixture
def client():
    c = GraphClient("cid", "secret", "tenant")
    c._get_token = AsyncMock(return_value="fake-token")
    return c


class _FakeResp:
    def __init__(self, status=201, body=None, headers=None, text=""):
        self.status_code = status
        self._body = body or {}
        self.headers = headers or {"content-type": "application/json"}
        self.text = text

    def json(self):
        return self._body


@pytest.mark.asyncio
async def test_upload_small_file_builds_correct_url(client, monkeypatch):
    calls = {}

    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def put(self, url, headers=None, content=None):
            calls["url"] = url
            calls["headers"] = headers
            calls["body_len"] = len(content) if content else 0
            return _FakeResp(201, {"id": "drv-itm-1", "name": "report.docx"})

    monkeypatch.setattr("shared.graph_client.httpx.AsyncClient", FakeClient)

    result = await client.upload_small_file_to_drive(
        user_id="user-1",
        drive_path="Projects/Q1/report.docx",
        body=b"x" * 1024,
        conflict_behavior="replace",
    )

    assert result["id"] == "drv-itm-1"
    # conflictBehavior moves to query string because httpx forbids `@`
    # in header names.
    assert "/users/user-1/drive/root:/Projects/Q1/report.docx:/content" in calls["url"]
    assert "@microsoft.graph.conflictBehavior=replace" in calls["url"]
    assert "@microsoft.graph.conflictBehavior" not in calls["headers"]
    assert calls["body_len"] == 1024


@pytest.mark.asyncio
async def test_upload_large_file_chunks_with_content_range(client, monkeypatch):
    calls = []

    session_body = {"uploadUrl": "https://upload/session?token=abc"}
    final_resp = {"id": "drv-2", "name": "big.bin"}

    class FakeClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, headers=None, json=None):
            calls.append(("POST", url, dict(headers or {}), json))
            return _FakeResp(200, session_body)

        async def put(self, url, headers=None, content=None):
            calls.append(("PUT", url, dict(headers or {}), len(content or b"")))
            is_last = "Content-Range" in headers and headers["Content-Range"].endswith("/25")
            if is_last:
                return _FakeResp(201, final_resp)
            return _FakeResp(202, {"status": 202}, headers={})

    monkeypatch.setattr("shared.graph_client.httpx.AsyncClient", FakeClient)

    result = await client.upload_large_file_to_drive(
        user_id="user-1",
        drive_path="bigdir/big.bin",
        body=b"A" * 25,
        total_size=25,
        chunk_size=10,
        conflict_behavior="replace",
    )

    assert result["id"] == "drv-2"
    assert calls[0][0] == "POST" and "createUploadSession" in calls[0][1]
    chunk_calls = [c for c in calls if c[0] == "PUT"]
    assert len(chunk_calls) == 3
    ranges = [c[2]["Content-Range"] for c in chunk_calls]
    assert ranges == ["bytes 0-9/25", "bytes 10-19/25", "bytes 20-24/25"]


@pytest.mark.asyncio
async def test_patch_file_system_info_normalises_plus_zero(client):
    calls = {}

    async def fake_patch(url, payload):
        calls["url"] = url
        calls["payload"] = payload
        return {"id": "drv-3"}

    client._patch = fake_patch

    await client.patch_drive_item_file_system_info(
        user_id="user-1",
        drive_item_id="drv-3",
        created_iso="2024-01-15T10:00:00+00:00",
        modified_iso="2024-02-20T12:30:00Z",
    )

    assert calls["url"].endswith("/users/user-1/drive/items/drv-3")
    fsi = calls["payload"]["fileSystemInfo"]
    assert fsi["createdDateTime"] == "2024-01-15T10:00:00Z"
    assert fsi["lastModifiedDateTime"] == "2024-02-20T12:30:00Z"


@pytest.mark.asyncio
async def test_patch_file_system_info_noop_on_empty_inputs(client):
    called = {"n": 0}

    async def fake_patch(url, payload):
        called["n"] += 1
        return {}

    client._patch = fake_patch

    result = await client.patch_drive_item_file_system_info(
        user_id="u", drive_item_id="i", created_iso=None, modified_iso=None,
    )

    assert result is None
    assert called["n"] == 0
