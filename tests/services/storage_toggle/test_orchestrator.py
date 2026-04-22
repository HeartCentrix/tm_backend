"""End-to-end orchestrator happy path.

Spins a real toggle (azure→seaweedfs) using Azurite + SeaweedFS + local
Postgres. Strategies are set to `noop` so the toggle runs entirely in
the app layer.
"""
from __future__ import annotations

import json
import os
import uuid

import asyncpg
import pytest

os.environ.setdefault("PG_PROMOTE_STRATEGY", "noop")
os.environ.setdefault("DNS_FLIP_STRATEGY", "noop")
os.environ.setdefault("WORKER_RESTART_STRATEGY", "noop")


DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:drdhiOUlYMCzgPcDPndRLjbaEOTkuEPm@localhost:5432/railway",
)

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
)


async def _seed_backends() -> tuple[str, str]:
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute("SET search_path TO tm, public")
        # Azure — point at Azurite via connection_string
        await conn.execute(
            """
            UPDATE storage_backends
            SET endpoint = 'http://localhost:10000/devstoreaccount1',
                config = jsonb_build_object(
                  'connection_string', 'env://AZURITE_CONN'
                ),
                is_enabled = true
            WHERE name = 'azure-primary'
            """
        )
        az_id = await conn.fetchval(
            "SELECT id::text FROM storage_backends WHERE name='azure-primary'",
        )

        # SeaweedFS — seed a test backend row
        sw_id = await conn.fetchval(
            """
            INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config)
            VALUES ('seaweedfs','seaweedfs-test',
                    'http://localhost:8333',
                    'env://SEAWEED_TEST_SECRET',
                    $1::jsonb)
            ON CONFLICT (name) DO UPDATE SET endpoint = EXCLUDED.endpoint
            RETURNING id::text
            """,
            json.dumps({
                "buckets": ["tmvault-test-0"],
                "region": "us-east-1",
                "verify_tls": False,
                "access_key_env": "SEAWEED_TEST_ACCESS",
            }),
        )

        # Reset system_config to azure-primary + stable
        await conn.execute(
            "UPDATE system_config SET active_backend_id=$1, transition_state='stable', "
            "cooldown_until=NULL WHERE id=1",
            uuid.UUID(az_id),
        )
        return az_id, sw_id
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_toggle_happy_path():
    os.environ["AZURITE_CONN"] = AZURITE_CONN
    os.environ["SEAWEED_TEST_ACCESS"] = "testaccess"
    os.environ["SEAWEED_TEST_SECRET"] = "testsecret"

    az_id, sw_id = await _seed_backends()

    from shared.storage.router import router
    await router.load(db_dsn=DSN)
    try:
        # Insert a toggle event + run the orchestrator directly (skip RMQ)
        event_id = uuid.uuid4()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute("SET search_path TO tm, public")
            await conn.execute(
                "INSERT INTO storage_toggle_events "
                "(id, actor_id, from_backend_id, to_backend_id, reason, status) "
                "VALUES ($1,$2,$3,$4,$5,'started')",
                event_id, uuid.UUID(int=1), uuid.UUID(az_id), uuid.UUID(sw_id),
                "test",
            )
        finally:
            await conn.close()

        from services.storage_toggle_worker.orchestrator import run_toggle
        await run_toggle({
            "event_id": str(event_id),
            "from_id": az_id,
            "to_id": sw_id,
            "actor_id": "1",
            "reason": "test",
        })

        # Assert final state
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute("SET search_path TO tm, public")
            row = await conn.fetchrow(
                "SELECT status, completed_at FROM storage_toggle_events WHERE id=$1",
                event_id,
            )
            assert row["status"] == "completed", f"got {row['status']}"
            sc = await conn.fetchrow(
                "SELECT active_backend_id::text, transition_state, cooldown_until "
                "FROM system_config WHERE id=1"
            )
            assert sc["active_backend_id"] == sw_id
            assert sc["transition_state"] == "stable"
            assert sc["cooldown_until"] is not None
        finally:
            await conn.close()
    finally:
        # Flip back so the test isn't destructive to future runs
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute("SET search_path TO tm, public")
            await conn.execute(
                "UPDATE system_config SET active_backend_id=$1, transition_state='stable', "
                "cooldown_until=NULL WHERE id=1",
                uuid.UUID(az_id),
            )
        finally:
            await conn.close()
        await router.close()
