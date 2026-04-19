-- Teams chat export v1 — schema prerequisites
-- Run BEFORE 2026_04_20_chat_export_indexes.sql
-- Adds the minimal columns + enum values the chat-export pipeline needs,
-- keeps all other behaviour on existing `spec`/`result`/`extra_data` JSONB.

-- 1) snapshot_items: link attachments + hosted content to their parent msg
ALTER TABLE snapshot_items
  ADD COLUMN IF NOT EXISTS parent_external_id VARCHAR;

-- 2) jobs: export flow needs a PENDING state distinct from QUEUED (reserved
--    for in-worker consumption) and a CANCELLING transient for graceful stop.
--    Enum ADD VALUE can't run inside a transaction — apply each separately.
--    Postgres 12+: IF NOT EXISTS supported.
ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'PENDING';
ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'CANCELLING';

-- 3) JobType: chat exports reuse existing EXPORT; no new enum value.
--    Discriminator lives in jobs.spec->>'kind' = 'chat_export_thread'.

-- Idempotent: safe to re-run.
