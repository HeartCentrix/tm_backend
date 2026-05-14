"""SQL safety harness for the backup-batch-row redesign.

Runs EXPLAIN against every raw SQL string the redesign introduces.
Fails non-zero exit if any query errors. Run before deploy; rerun
after every Postgres / asyncpg upgrade.

Usage:
    DATABASE_URL=postgres://... python tm_backend/scripts/sql_smoke_test.py
"""
from __future__ import annotations

import asyncio
import os
import sys

import asyncpg


# Each entry: (label, sql, args).  Args use $1, $2... (asyncpg style).
QUERIES = [
    (
        "finalizer.gate1_missing_tier2",
        """
        SELECT s.user_id
          FROM unnest($1::uuid[]) s(user_id)
          LEFT JOIN tm_vault.resources r
            ON r.parent_resource_id = s.user_id
           AND r.archived_at IS NULL
         GROUP BY s.user_id
         HAVING COUNT(r.id) = 0
        """,
        ["{00000000-0000-0000-0000-000000000000}"],
    ),
    (
        "finalizer.gate23_pending_leaves",
        """
        WITH leaves AS (
            SELECT cast(unnest($1::uuid[]) AS uuid) AS rid
        ),
        latest AS (
            SELECT DISTINCT ON (s.resource_id)
                   s.resource_id, s.status::text AS status
              FROM tm_vault.snapshots s
              JOIN leaves l ON s.resource_id = l.rid
             WHERE s.created_at > $2::timestamp
             ORDER BY s.resource_id, s.created_at DESC
        )
        SELECT l.rid
          FROM leaves l
          LEFT JOIN latest la ON la.resource_id = l.rid
         WHERE la.resource_id IS NULL
            OR la.status NOT IN ('COMPLETED','PARTIAL','FAILED')
        """,
        ["{00000000-0000-0000-0000-000000000000}", "2026-01-01"],
    ),
    (
        "finalizer.gate4_inflight_partitions",
        """
        SELECT 1
          FROM tm_vault.snapshot_partitions sp
          JOIN tm_vault.snapshots s ON s.id = sp.snapshot_id
         WHERE s.resource_id = ANY($1::uuid[])
           AND s.created_at > $2::timestamp
           AND sp.status::text IN ('QUEUED','IN_PROGRESS')
         LIMIT 1
        """,
        ["{00000000-0000-0000-0000-000000000000}", "2026-01-01"],
    ),
    (
        "audit.activity_feed_v2",
        """
        SELECT
          b.id::text AS batch_id,
          b.created_at,
          b.completed_at,
          b.status,
          b.source,
          b.actor_email,
          b.scope_user_ids,
          b.bytes_expected,
          (SELECT COALESCE(SUM(s.bytes_added), 0)
             FROM tm_vault.snapshots s
             JOIN tm_vault.jobs j ON j.id = s.job_id
            WHERE COALESCE(j.spec::jsonb->>'batch_id','') = b.id::text
              AND s.status::text IN ('COMPLETED','PARTIAL','IN_PROGRESS')
              AND s.created_at > b.created_at)        AS bytes_done,
          (SELECT array_agg(DISTINCT j.id)
             FROM tm_vault.jobs j
            WHERE COALESCE(j.spec::jsonb->>'batch_id','') = b.id::text) AS job_ids
          FROM tm_vault.backup_batches b
         WHERE b.tenant_id = $1::uuid
         ORDER BY b.created_at DESC
         LIMIT 50
        """,
        ["00000000-0000-0000-0000-000000000000"],
    ),
    (
        "scheduler.sweep_inflight_window",
        """
        SELECT id FROM tm_vault.backup_batches
         WHERE status = 'IN_PROGRESS'
           AND created_at > NOW() - make_interval(hours => $1)
         ORDER BY created_at ASC
         LIMIT 500
        """,
        [24],
    ),
    (
        "scheduler.sweep_stalled_batches",
        """
        SELECT b.id,
               EXISTS(
                   SELECT 1 FROM tm_vault.snapshots s
                     JOIN tm_vault.jobs j ON j.id = s.job_id
                    WHERE COALESCE(j.spec::jsonb->>'batch_id','') = b.id::text
                      AND s.created_at > b.created_at
               ) AS has_any_snapshot
          FROM tm_vault.backup_batches b
         WHERE b.status = 'IN_PROGRESS'
           AND b.created_at < NOW() - make_interval(hours => $1)
         LIMIT 500
        """,
        [24],
    ),
    (
        "drilldown.scope_user_ids_lookup",
        """
        SELECT scope_user_ids FROM tm_vault.backup_batches
         WHERE id = $1::uuid
        """,
        ["00000000-0000-0000-0000-000000000000"],
    ),
    (
        "drilldown.latest_snapshot_per_resource",
        """
        SELECT DISTINCT ON (s.resource_id)
               s.resource_id, s.status::text, s.item_count, s.bytes_added
          FROM tm_vault.snapshots s
         WHERE s.resource_id = ANY($1::uuid[])
         ORDER BY s.resource_id, s.created_at DESC
        """,
        ["{00000000-0000-0000-0000-000000000000}"],
    ),
]


async def _explain_one(conn: asyncpg.Connection, label: str, sql: str, args: list) -> bool:
    try:
        # asyncpg `prepare` validates SQL without executing — cheaper than EXPLAIN
        # for inserts/updates we never do here, and works for the SELECTs above.
        await conn.execute("EXPLAIN " + sql, *args)
        print(f"OK   {label}")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"FAIL {label}: {type(e).__name__}: {e}")
        return False


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set", file=sys.stderr)
        return 2
    # Match production: asyncpg statement_cache_size=0 (bug #55).
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    try:
        results = []
        for label, sql, args in QUERIES:
            results.append(await _explain_one(conn, label, sql, args))
    finally:
        await conn.close()
    if not all(results):
        print(f"\n{sum(1 for r in results if not r)} query(ies) failed.")
        return 1
    print(f"\nAll {len(QUERIES)} queries parse cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
