"""Tests streaming blob download against Azurite."""
import pytest

from shared.azure_storage import AzureStorageShard


pytestmark = pytest.mark.integration


@pytest.fixture
async def shard(azurite_connection_string):
    s = AzureStorageShard.from_connection_string(azurite_connection_string, shard_index=0)
    yield s
    await s.close()


async def test_download_blob_stream_yields_chunks(shard):
    payload = b"A" * (5 * 1024 * 1024)
    await shard.ensure_container("test-stream")
    await shard.upload_blob("test-stream", "foo.bin", payload)

    chunks = []
    async for chunk in shard.download_blob_stream("test-stream", "foo.bin", chunk_size=1 * 1024 * 1024):
        chunks.append(chunk)
        assert len(chunk) <= 1 * 1024 * 1024

    assert b"".join(chunks) == payload
    assert len(chunks) >= 5


async def test_download_blob_stream_missing_blob_returns_nothing(shard):
    await shard.ensure_container("test-stream-missing")
    chunks = [c async for c in shard.download_blob_stream("test-stream-missing", "doesnotexist.bin")]
    assert chunks == []
