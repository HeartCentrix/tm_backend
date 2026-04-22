"""SeaweedStore functional tests against a local SeaweedFS container.

Requires SeaweedFS running on localhost:8333 (the `test` compose profile
starts it via tests/shared/storage/conftest.py).
"""
import os
import uuid

import pytest
import pytest_asyncio

from shared.storage.seaweedfs import SeaweedStore

ENDPOINT = os.getenv("SEAWEEDFS_TEST_ENDPOINT", "http://localhost:8333")
ACCESS = "testaccess"
SECRET = "testsecret"


@pytest_asyncio.fixture
async def sw_store():
    store = SeaweedStore(
        backend_id="00000000-0000-0000-0000-000000000002",
        name="seaweedfs-test",
        endpoint=ENDPOINT, access_key=ACCESS, secret_key=SECRET,
        buckets=["tmvault-test-0"], region="us-east-1",
        verify_tls=False,
    ).shard_for("tenant-t0", "res-r0")  # force single bucket for test
    yield store
    await store.close()


@pytest.mark.asyncio
async def test_seaweedfs_upload_download_roundtrip(sw_store):
    path = f"sw-{uuid.uuid4().hex[:8]}.txt"
    body = b"hello seaweed"
    info = await sw_store.upload("contract", path, body, metadata={"kind": "test"})
    assert info.size == len(body)
    got = await sw_store.download("contract", path)
    assert got == body
    await sw_store.delete("contract", path)


@pytest.mark.asyncio
async def test_seaweedfs_missing_returns_none(sw_store):
    got = await sw_store.download("contract", "absent-key.txt")
    assert got is None
