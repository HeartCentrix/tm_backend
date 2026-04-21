"""Contract tests — same test body runs against both AzureBlobStore and
SeaweedStore to guarantee behavior parity. Any new method added to the
BackendStore protocol should get a matching test here."""
import os
import uuid

import aiohttp
import pytest
import pytest_asyncio

from shared.storage.azure_blob import AzureBlobStore
from shared.storage.seaweedfs import SeaweedStore

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
)


@pytest_asyncio.fixture(params=["azure", "seaweedfs"])
async def store_ctx(request):
    if request.param == "azure":
        s = AzureBlobStore.from_connection_string(
            AZURITE_CONN,
            backend_id="00000000-0000-0000-0000-000000000001",
            name="azurite-contract",
        )
        container = f"contract-{uuid.uuid4().hex[:8]}"
        await s.ensure_container(container)
    else:
        s = SeaweedStore(
            backend_id="00000000-0000-0000-0000-000000000002",
            name="seaweedfs-contract",
            endpoint=os.getenv("SEAWEEDFS_TEST_ENDPOINT", "http://localhost:8333"),
            access_key="testaccess", secret_key="testsecret",
            buckets=["tmvault-test-0"], verify_tls=False,
        ).shard_for("tenant-contract", "res-contract")
        container = f"contract-{uuid.uuid4().hex[:8]}"
    yield s, container, request.param
    await s.close()


@pytest.mark.asyncio
async def test_upload_download(store_ctx):
    s, c, _ = store_ctx
    info = await s.upload(c, "k.txt", b"payload")
    assert info.size == 7
    assert info.backend_id
    got = await s.download(c, "k.txt")
    assert got == b"payload"


@pytest.mark.asyncio
async def test_upload_metadata_roundtrip(store_ctx):
    s, c, _ = store_ctx
    await s.upload(c, "m.txt", b"m", metadata={"kind": "meta", "tag": "t1"})
    props = await s.get_properties(c, "m.txt")
    assert props is not None
    # Azure normalizes metadata keys to lowercase when returned.
    md = {k.lower(): v for k, v in (props.metadata or {}).items()}
    assert md.get("kind") == "meta"


@pytest.mark.asyncio
async def test_missing_blob_returns_none(store_ctx):
    s, c, _ = store_ctx
    got = await s.download(c, "absent.txt")
    assert got is None


@pytest.mark.asyncio
async def test_list_blobs(store_ctx):
    s, c, _ = store_ctx
    await s.upload(c, "a.txt", b"a")
    await s.upload(c, "b.txt", b"b")
    names = []
    async for n in s.list_blobs(c):
        names.append(n)
    assert "a.txt" in names
    assert "b.txt" in names


@pytest.mark.asyncio
async def test_delete(store_ctx):
    s, c, _ = store_ctx
    await s.upload(c, "del.txt", b"x")
    await s.delete(c, "del.txt")
    assert await s.download(c, "del.txt") is None


@pytest.mark.asyncio
async def test_presigned_url_is_get_readable(store_ctx):
    s, c, kind = store_ctx
    await s.upload(c, "p.txt", b"presigned")
    url = await s.presigned_url(c, "p.txt", valid_hours=1)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            assert resp.status == 200
            assert await resp.read() == b"presigned"


@pytest.mark.asyncio
async def test_shard_for_is_deterministic(store_ctx):
    s, _, _ = store_ctx
    a = s.shard_for("t1", "r1")
    b = s.shard_for("t1", "r1")
    # Same inputs pick same shard; different inputs may differ.
    assert a.name == b.name
