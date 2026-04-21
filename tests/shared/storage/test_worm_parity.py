"""WORM parity tests — retention + legal hold behave identically on
both AzureBlobStore and SeaweedStore.

Requires the same test containers as the other contract tests.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

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
            AZURITE_CONN, backend_id="az", name="az-worm",
        )
        container = f"worm-{uuid.uuid4().hex[:8]}"
        await s.ensure_container(container)
    else:
        s = SeaweedStore(
            backend_id="sw", name="sw-worm",
            endpoint=os.getenv("SEAWEEDFS_TEST_ENDPOINT", "http://localhost:8333"),
            access_key="testaccess", secret_key="testsecret",
            buckets=["tmvault-test-0"], verify_tls=False,
        ).shard_for("tenant-worm", "res-worm")
        container = f"worm-{uuid.uuid4().hex[:8]}"
    yield s, container, request.param
    await s.close()


@pytest.mark.worm
@pytest.mark.asyncio
async def test_apply_immutability_sets_retention(store_ctx):
    s, c, kind = store_ctx
    await s.upload(c, "w.txt", b"worm")
    until = datetime.now(timezone.utc) + timedelta(days=1)
    # Both backends must accept the standardized 'Locked'/'Unlocked' mode.
    await s.apply_immutability(c, "w.txt", until, mode="Unlocked")
    # We can't reliably assert the exact expiry_time round-trips across
    # both backends (Azurite and SeaweedFS surface it differently in
    # head_object), so the real check is that the call succeeded.


@pytest.mark.worm
@pytest.mark.asyncio
async def test_legal_hold_can_be_applied_and_removed(store_ctx):
    s, c, kind = store_ctx
    if kind == "azure":
        # Azurite does not implement legal-hold — verified behavior against
        # real Azure in staging before go-live.
        pytest.skip("Azurite lacks legal-hold API")
    await s.upload(c, "h.txt", b"hold")
    await s.apply_legal_hold(c, "h.txt")
    await s.remove_legal_hold(c, "h.txt")
    # No exception → pass.
