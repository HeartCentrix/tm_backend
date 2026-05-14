-- ====================================================================
-- Backfill stuck partitioned snapshots after bug #152 fix
-- ====================================================================
--
-- Before commit fixing main.py:8058 (`:fs::json` → `CAST(:fs AS JSONB)`),
-- every call to _mark_partition_terminal raised PostgresSyntaxError. Both
-- COMPLETED and FAILED transitions were affected, so every shard's data
-- work landed in `snapshot_items` correctly, but its `snapshot_partitions`
-- row stayed `IN_PROGRESS` forever — and the parent `snapshots` row stayed
-- IN_PROGRESS because `_finalize_partitioned_snapshot` only fires when all
-- shards are terminal.
--
-- After deploying the fix, run this script ONCE against the live DB to
-- unstick anything stranded. Safe to re-run: every UPDATE has a guard so
-- replays are no-ops.
--
-- Operator: run via
--   railway run --service backup_worker -- psql "$DATABASE_URL" \
--     -f scripts/2026_05_14_backfill_stuck_partition_snapshots.sql
--
-- Verify before-after:
--   SELECT status, COUNT(*) FROM snapshot_partitions GROUP BY status;
--   SELECT status, COUNT(*) FROM snapshots
--     WHERE (extra_data->>'partitioned')::bool = true GROUP BY status;
-- ====================================================================

\set ON_ERROR_STOP on
BEGIN;

-- ---- Step 1: flip IN_PROGRESS partition rows to COMPLETED -----------
-- Anything that's been IN_PROGRESS for > 5 minutes pre-dates the fix
-- (after the fix, terminal-flip is sub-second). The shards already ran
-- their data work — we just couldn't record their terminal state.
-- bytes_uploaded / files_uploaded stay 0 here; the snapshot-level
-- reconcile in Step 2 derives the real totals from snapshot_items.
WITH stuck AS (
    SELECT id, snapshot_id
    FROM snapshot_partitions
    WHERE status = 'IN_PROGRESS'
      AND started_at IS NOT NULL
      AND started_at < NOW() - INTERVAL '5 minutes'
)
UPDATE snapshot_partitions p
   SET status = 'COMPLETED',
       completed_at = COALESCE(p.completed_at, NOW()),
       failure_state = COALESCE(p.failure_state, '{}'::jsonb)
                       || jsonb_build_object(
                            'backfilled_by', '2026_05_14_fs_cast_fix',
                            'note', 'data work was persisted; terminal flip blocked by SQL bug'
                          )
  FROM stuck s
 WHERE p.id = s.id;

-- ---- Step 2: reconcile parent snapshot bytes/items from snapshot_items
-- Mirrors the DB-grounded reconcile in
-- workers/backup-worker/main.py:_finalize_partitioned_snapshot lines
-- 8151-8180. Child item types (CHAT_ATTACHMENT / EMAIL_ATTACHMENT /
-- CHAT_HOSTED_CONTENT) are pointer rows whose blob storage is already
-- counted by the parent items; excluded here.
WITH stuck_snaps AS (
    SELECT DISTINCT s.id AS snapshot_id
    FROM snapshots s
    WHERE s.status = 'IN_PROGRESS'
      AND (s.extra_data->>'partitioned')::bool = true
      -- All this snapshot's partitions are now terminal (Step 1 just
      -- flipped them; this guard also catches replays).
      AND NOT EXISTS (
          SELECT 1 FROM snapshot_partitions p2
          WHERE p2.snapshot_id = s.id
            AND p2.status NOT IN ('COMPLETED', 'FAILED')
      )
),
totals AS (
    SELECT
        si.snapshot_id,
        COUNT(*) FILTER (
            WHERE si.item_type NOT IN (
                'CHAT_ATTACHMENT',
                'EMAIL_ATTACHMENT',
                'CHAT_HOSTED_CONTENT'
            )
        ) AS persisted_count,
        COALESCE(SUM(si.content_size), 0) AS persisted_bytes
    FROM snapshot_items si
    JOIN stuck_snaps ss ON ss.snapshot_id = si.snapshot_id
    GROUP BY si.snapshot_id
)
UPDATE snapshots s SET
    status = (CASE
        -- All shards FAILED → snapshot FAILED. Otherwise COMPLETED.
        -- (PARTIAL is only set when SOME shards failed; if any single
        -- shard succeeded we record the work and call it complete.)
        WHEN EXISTS (
            SELECT 1 FROM snapshot_partitions p3
             WHERE p3.snapshot_id = s.id
               AND p3.status = 'COMPLETED'
        ) THEN
            CASE WHEN EXISTS (
                SELECT 1 FROM snapshot_partitions p4
                 WHERE p4.snapshot_id = s.id
                   AND p4.status = 'FAILED'
            ) THEN 'PARTIAL'::snapshotstatus
            ELSE 'COMPLETED'::snapshotstatus END
        ELSE 'FAILED'::snapshotstatus
    END),
    completed_at = COALESCE(s.completed_at, NOW()),
    duration_secs = CASE
        WHEN s.started_at IS NOT NULL THEN
            EXTRACT(EPOCH FROM (COALESCE(s.completed_at, NOW()) - s.started_at))::int
        ELSE s.duration_secs
    END,
    item_count = t.persisted_count,
    new_item_count = t.persisted_count,
    bytes_added = t.persisted_bytes,
    bytes_total = t.persisted_bytes
FROM totals t
WHERE s.id = t.snapshot_id
  AND s.status = 'IN_PROGRESS';

-- ---- Step 3: roll bytes_total up to parent Job -----------------------
-- Mirror _finalize_bulk_parent_if_complete's effect: jobs.bytes_processed
-- and jobs.items_processed should reflect the sum of their snapshots'
-- new totals. Without this, the Activity row stays stuck at the
-- pre-backfill values.
WITH updated_snaps AS (
    SELECT DISTINCT s.job_id, s.id AS snapshot_id, s.bytes_added, s.item_count
    FROM snapshots s
    WHERE s.completed_at >= NOW() - INTERVAL '5 minutes'
      AND s.job_id IS NOT NULL
      AND (s.extra_data->>'partitioned')::bool = true
),
job_totals AS (
    SELECT job_id,
           COALESCE(SUM(bytes_added), 0) AS total_bytes,
           COALESCE(SUM(item_count), 0)  AS total_items
    FROM updated_snaps
    GROUP BY job_id
)
UPDATE jobs j SET
    bytes_processed = GREATEST(COALESCE(j.bytes_processed, 0), jt.total_bytes),
    items_processed = GREATEST(COALESCE(j.items_processed, 0), jt.total_items)
FROM job_totals jt
WHERE j.id = jt.job_id;

-- ---- Step 4: report the damage we just undid -------------------------
SELECT
    'partition_rows_backfilled' AS metric,
    COUNT(*) AS n
FROM snapshot_partitions
WHERE (failure_state->>'backfilled_by') = '2026_05_14_fs_cast_fix'
UNION ALL
SELECT
    'snapshots_unstuck' AS metric,
    COUNT(*) AS n
FROM snapshots
WHERE status IN ('COMPLETED', 'PARTIAL', 'FAILED')
  AND completed_at >= NOW() - INTERVAL '5 minutes'
  AND (extra_data->>'partitioned')::bool = true;

COMMIT;
