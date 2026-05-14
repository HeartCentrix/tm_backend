-- ============================================================================
-- One-time backfill: historic ONEDRIVE_FILE rows with wrong content_size
-- ============================================================================
--
-- Issue: prior to the fix at backup-worker/main.py:5100, every ONEDRIVE_FILE
-- row was INSERTed with content_size = len(metadata_json_body) instead of
-- the real file size. The flusher at main.py:6981 only sometimes overwrote
-- this with the actual size, so any snapshot whose flush never ran (i.e.,
-- the file was already in blob from a prior snapshot, smart-skip path) kept
-- the JSON-metadata size forever. End result: Activity row showed ~200 MB
-- when the actual backup was ~14 GB.
--
-- Run this on the demo Railway database AFTER deploying the worker fix.
-- The new INSERTs already use the right size; this just rewrites the bad
-- history.
--
-- Idempotent: rewriting from metadata->raw->size to the same value is a
-- no-op. Safe to re-run.
--
-- Run via Railway → Postgres → Query, or psql $DATABASE_URL < this_file.

BEGIN;

-- Step 1: rewrite snapshot_items.content_size from extra_data.raw.size when
-- the stored content_size disagrees with the real Graph-reported file size.
-- The new-INSERT path only does this for ONEDRIVE_FILE; that's the only
-- item_type affected by the bug.
WITH fixed AS (
  UPDATE snapshot_items si
     SET content_size = (extra_data->'raw'->>'size')::bigint
   WHERE si.item_type = 'ONEDRIVE_FILE'
     AND extra_data ? 'raw'
     AND (extra_data->'raw'->>'size') IS NOT NULL
     AND (extra_data->'raw'->>'size') ~ '^[0-9]+$'
     AND si.content_size <> (extra_data->'raw'->>'size')::bigint
  RETURNING si.id, si.snapshot_id, si.content_size
)
SELECT COUNT(*)             AS rows_rewritten,
       SUM(content_size)    AS bytes_after_rewrite,
       MIN(snapshot_id)     AS sample_snapshot_id
  FROM fixed;

-- Step 2: re-roll snapshots.bytes_added / bytes_total from the
-- now-corrected snapshot_items.content_size. Mirrors the worker's
-- finalize-block bytes reconcile but applied historically across every
-- snapshot. CHILD_ITEM_TYPES are excluded from the bytes_added sum the
-- worker uses (attachments + hosted content count against their parent's
-- bytes_added on read).
UPDATE snapshots s
   SET bytes_added = sub.b,
       bytes_total = sub.b
  FROM (
    SELECT snapshot_id,
           SUM(content_size) AS b
      FROM snapshot_items
     WHERE item_type NOT IN (
             'CHAT_ATTACHMENT', 'EMAIL_ATTACHMENT', 'CHAT_HOSTED_CONTENT'
           )
     GROUP BY snapshot_id
  ) sub
 WHERE s.id = sub.snapshot_id
   AND COALESCE(s.bytes_added, 0) <> sub.b;

-- Step 3: re-roll jobs.bytes_processed from the sum of all snapshots'
-- bytes_added under each job (matches what the Activity row reads).
UPDATE jobs j
   SET bytes_processed = sub.b
  FROM (
    SELECT job_id,
           SUM(COALESCE(bytes_added, 0)) AS b
      FROM snapshots
     WHERE job_id IS NOT NULL
     GROUP BY job_id
  ) sub
 WHERE j.id = sub.job_id
   AND COALESCE(j.bytes_processed, 0) <> sub.b;

-- ---------------------------------------------------------------------------
-- Verification queries — run after COMMIT:
--
--   SELECT id, type, status, bytes_added, bytes_total, item_count, completed_at
--     FROM snapshots
--    WHERE bytes_added IS NOT NULL
--    ORDER BY completed_at DESC NULLS LAST
--    LIMIT 20;
--
--   SELECT id, type, status, bytes_processed, completed_at
--     FROM jobs
--    WHERE type = 'BACKUP' AND status = 'COMPLETED'
--    ORDER BY completed_at DESC
--    LIMIT 10;
-- ---------------------------------------------------------------------------

COMMIT;
