"""Tests for GraphClient.get_hosted_content streaming download."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from shared.graph_client import GraphClient


class _FakeResp:
    def __init__(self):
        self.headers = {"Content-Type": "image/png", "Content-Length": "10"}

    def raise_for_status(self):
        return None

    async def aiter_bytes(self, _n):
        yield b"hello"
        yield b"world"


class _AsyncCM:
    def __init__(self, value):
        self._value = value
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self._value

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True
        return None


@pytest.mark.asyncio
async def test_get_hosted_content_streams_bytes_and_ctype():
    gc = GraphClient(client_id="cid", client_secret="sec", tenant_id="tid")

    resp = _FakeResp()
    stream_cm = _AsyncCM(resp)

    mock_client = MagicMock()
    mock_client.stream = MagicMock(return_value=stream_cm)
    client_cm = _AsyncCM(mock_client)

    with patch("shared.graph_client.httpx.AsyncClient", return_value=client_cm), \
         patch.object(GraphClient, "_get_token", AsyncMock(return_value="tok")):
        stream, ctype, size = await gc.get_hosted_content("cid", "mid", "hid")
        assert ctype == "image/png"
        assert size == 10
        buf = b""
        async for chunk in stream:
            buf += chunk
        assert buf == b"helloworld"

    # Verify contexts were closed after consuming the stream.
    assert stream_cm.exited is True
    assert client_cm.exited is True
    # Verify the URL that was requested.
    mock_client.stream.assert_called_once()
    args, kwargs = mock_client.stream.call_args
    assert args[0] == "GET"
    assert args[1].endswith(
        "/chats/cid/messages/mid/hostedContents/hid/$value"
    )
    assert kwargs["headers"]["Authorization"] == "Bearer tok"
