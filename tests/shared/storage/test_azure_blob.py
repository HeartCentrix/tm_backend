"""AzureBlobStore functional tests against Azurite (local emulator).

Requires Azurite running on localhost:10000 (the `test` compose profile
starts it via tests/shared/storage/conftest.py).
"""
import uuid

import pytest
import pytest_asyncio

from shared.storage.azure_blob import AzureBlobStore

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
)


@pytest_asyncio.fixture
async def azure_store():
    store = AzureBlobStore.from_connection_string(
        conn_str=AZURITE_CONN,
        backend_id="00000000-0000-0000-0000-000000000001",
        name="azurite-test",
    )
    yield store
    await store.close()


@pytest.mark.asyncio
async def test_azure_upload_download_roundtrip(azure_store: AzureBlobStore):
    container = f"test-{uuid.uuid4().hex[:8]}"
    await azure_store.ensure_container(container)
    path = "hello.txt"
    body = b"hello tmvault"

    info = await azure_store.upload(container, path, body, metadata={"kind": "test"})
    assert info.backend_id == "00000000-0000-0000-0000-000000000001"
    assert info.size == len(body)

    got = await azure_store.download(container, path)
    assert got == body


@pytest.mark.asyncio
async def test_azure_download_missing_returns_none(azure_store: AzureBlobStore):
    container = f"test-{uuid.uuid4().hex[:8]}"
    await azure_store.ensure_container(container)
    got = await azure_store.download(container, "absent.txt")
    assert got is None
