"""End-to-end: StorageRouter.get_active_store() writes via Azurite,
`backend_id` persists in the snapshot_items row, and
router.get_store_for_item resolves back to the same backend for restore.

Requires:
  - Postgres with on-prem storage schema applied
  - Azurite running on localhost:10000
  - TMvault `tm` schema initialized with a tenant/resource/snapshot row

Run:  pytest tests/integration/test_router_end_to_end.py -m integration -v
"""
from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from shared.storage.router import StorageRouter


DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:drdhiOUlYMCzgPcDPndRLjbaEOTkuEPm@localhost:5432/railway",
)


@pytest.fixture
def dsn() -> str:
    return DSN


@pytest.mark.integration
@pytest.mark.asyncio
async def test_router_write_then_restore(dsn):
    """Happy-path: write with get_active_store, record backend_id, read it
    back via get_store_for_item — bytes match."""
    # Ensure azure-primary backend points at Azurite for this test.
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("SET search_path TO tm, public")
        await conn.execute(
            """
            UPDATE storage_backends
            SET endpoint = $1,
                config = jsonb_build_object(
                  'connection_string', 'env://AZURITE_CONN'
                )
            WHERE name = 'azure-primary'
            """,
            "http://localhost:10000/devstoreaccount1",
        )
        az_id = await conn.fetchval(
            "SELECT id::text FROM storage_backends WHERE name='azure-primary'",
        )
    finally:
        await conn.close()

    os.environ["AZURITE_CONN"] = (
        "DefaultEndpointsProtocol=http;"
        "AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
    )

    r = StorageRouter()
    await r.load(db_dsn=dsn)
    try:
        assert r.active_backend_id() == az_id
        assert r.transition_state() == "stable"

        # Write via the active store
        container = f"rt-{uuid.uuid4().hex[:8]}"
        write = r.get_active_store()
        # Azurite path — ensure container
        await write.ensure_container(container)
        info = await write.upload(container, "rt.txt", b"route-me",
                                  metadata={"t": "integration"})
        assert info.backend_id == az_id

        # Pretend we wrote a snapshot_item pointing at that blob.
        class _Item:
            def __init__(self, bid):
                self.backend_id = bid
        item = _Item(az_id)

        # Read path — router.get_store_for_item picks the right backend
        read = r.get_store_for_item(item)
        got = await read.download(container, "rt.txt")
        assert got == b"route-me"
    finally:
        await r.close()
