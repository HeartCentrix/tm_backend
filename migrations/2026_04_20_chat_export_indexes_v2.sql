-- Teams chat export v1 indexes (v2 — corrected to actual schema)
-- Supersedes 2026_04_20_chat_export_indexes.sql for deploy.
-- Run after 2026_04_20_chat_export_schema.sql.
--
-- Real schema: snapshot_items has no resource_id column (JOIN snapshots),
-- no last_modified_at/deleted_at (use is_deleted + extra_data timestamps),
-- no jobs.parent_id/job_type (jobs.type is the column).

-- Scope-resolution: thread-path filter + message type.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_items_folder_type
  ON tm_vault.snapshot_items (folder_path, item_type);

-- Attachment/hosted joins — parent_external_id added by schema migration.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_items_parent_ext
  ON tm_vault.snapshot_items (parent_external_id)
  WHERE item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT');

-- Snapshot-scoped message enumeration.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_items_snapshot_type
  ON tm_vault.snapshot_items (snapshot_id, item_type);

-- Tenant concurrency cap query on jobs.
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_type_status
  ON tm_vault.jobs (tenant_id, type, status)
  WHERE status IN ('QUEUED','PENDING','RUNNING');

-- created_at scan for chat messages (scope resolver orders by this).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_items_chat_created
  ON tm_vault.snapshot_items (snapshot_id, folder_path, created_at)
  WHERE item_type IN ('TEAMS_CHAT_MESSAGE','TEAMS_MESSAGE','TEAMS_MESSAGE_REPLY');
