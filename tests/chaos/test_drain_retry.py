"""Chaos drill — simulate inflight jobs mid-toggle; drain must mark them
aborted_by_toggle and create retry rows with pre_toggle_job_id pointing
at the original."""
from __future__ import annotations

import os
import uuid
from datetime import datetime

import asyncpg
import pytest

DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:drdhiOUlYMCzgPcDPndRLjbaEOTkuEPm@localhost:5432/railway",
)


async def _db():
    conn = await asyncpg.connect(DSN)
    await conn.execute("SET search_path TO tm, public")
    return conn


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_drain_marks_stragglers_and_creates_retries():
    """Insert two RUNNING jobs, run drain with max_wait_s=0, assert they
    get CANCELLED and two new QUEUED jobs appear with pre_toggle_job_id
    pointing back."""
    # Need a tenant + resource to satisfy FK constraints on jobs table.
    conn = await _db()
    try:
        tenant = await conn.fetchrow(
            "SELECT id FROM tenants LIMIT 1"
        )
        if tenant is None:
            pytest.skip("no tenant seed row — run backup scheduler smoke first")
        tenant_id = tenant["id"]

        # Insert two RUNNING jobs
        job_ids = [uuid.uuid4(), uuid.uuid4()]
        for j in job_ids:
            await conn.execute(
                "INSERT INTO jobs (id, type, tenant_id, status, spec, created_at, updated_at) "
                "VALUES ($1, 'BACKUP', $2, 'RUNNING', '{}'::jsonb, NOW(), NOW())",
                j, tenant_id,
            )
    finally:
        await conn.close()

    # Drain with immediate timeout
    from services.storage_toggle_worker.drain import drain_inflight
    db = await _db()
    try:
        result = await drain_inflight(db, event_id="chaos-1",
                                       poll_interval_s=0, max_wait_s=0)
        assert result.retried_count >= 2, f"retried_count={result.retried_count}"
    finally:
        await db.close()

    # Verify
    conn = await _db()
    try:
        for j in job_ids:
            row = await conn.fetchrow(
                "SELECT status, retry_reason FROM jobs WHERE id=$1", j,
            )
            assert row["status"] == "CANCELLED", f"{j}: status={row['status']}"
            assert "aborted_by_toggle" in (row["retry_reason"] or ""), \
                f"{j}: retry_reason={row['retry_reason']}"
            retry = await conn.fetchrow(
                "SELECT id, status FROM jobs "
                "WHERE pre_toggle_job_id=$1 AND status='QUEUED'", j,
            )
            assert retry is not None, f"no retry row for {j}"
    finally:
        # Cleanup retry rows + original rows
        await conn.execute(
            "DELETE FROM jobs WHERE pre_toggle_job_id = ANY($1::uuid[])",
            job_ids,
        )
        await conn.execute(
            "DELETE FROM jobs WHERE id = ANY($1::uuid[])",
            job_ids,
        )
        await conn.close()
