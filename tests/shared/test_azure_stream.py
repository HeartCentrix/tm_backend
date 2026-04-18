"""Tests streaming blob download against Azurite."""
import pytest

from shared.azure_storage import AzureStorageShard


pytestmark = pytest.mark.integration


@pytest.fixture
async def shard(azure_test_connection_string):
    s = AzureStorageShard.from_connection_string(azure_test_connection_string, shard_index=0)
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


async def test_stage_and_commit_blocks(shard):
    """Manually stage 3 blocks of 1 MB each, then commit in order."""
    await shard.ensure_container("test-blocks")

    block_a = ("blk-01", b"A" * 1_048_576)
    block_b = ("blk-02", b"B" * 1_048_576)
    block_c = ("blk-03", b"C" * 1_048_576)

    await shard.stage_block("test-blocks", "multipart.bin", block_a[0], block_a[1])
    await shard.stage_block("test-blocks", "multipart.bin", block_b[0], block_b[1])
    await shard.stage_block("test-blocks", "multipart.bin", block_c[0], block_c[1])

    await shard.commit_block_list_manual(
        "test-blocks", "multipart.bin",
        [block_a[0], block_b[0], block_c[0]],
    )

    result = await shard.download_blob("test-blocks", "multipart.bin")
    assert result[:10] == b"A" * 10
    assert result[1_048_576 : 1_048_576 + 10] == b"B" * 10
    assert result[2 * 1_048_576 : 2 * 1_048_576 + 10] == b"C" * 10
    assert len(result) == 3 * 1_048_576


async def test_put_block_from_url_roundtrip(shard):
    """Server-side copy via put_block_from_url stitches source blobs into destination."""
    await shard.ensure_container("test-ssc")

    await shard.upload_blob("test-ssc", "src-a.bin", b"X" * 1_048_576)
    await shard.upload_blob("test-ssc", "src-b.bin", b"Y" * 1_048_576)

    src_a_url = await shard.get_blob_url("test-ssc", "src-a.bin")
    src_b_url = await shard.get_blob_url("test-ssc", "src-b.bin")

    await shard.put_block_from_url("test-ssc", "dst.bin", "b01", src_a_url)
    await shard.put_block_from_url("test-ssc", "dst.bin", "b02", src_b_url)
    await shard.commit_block_list_manual("test-ssc", "dst.bin", ["b01", "b02"])

    result = await shard.download_blob("test-ssc", "dst.bin")
    assert result[:10] == b"X" * 10
    assert result[1_048_576 : 1_048_576 + 10] == b"Y" * 10
    assert len(result) == 2 * 1_048_576
