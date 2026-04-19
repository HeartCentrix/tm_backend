-- Teams chat export v1 indexes
-- Required for scope-resolution queries at 5k-user / millions-of-msg scale.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_items_resource_type_folder
  ON snapshot_items (resource_id, item_type, folder_path);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_items_parent_ext
  ON snapshot_items (parent_external_id)
  WHERE item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT');

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshot_items_timestamps
  ON snapshot_items (resource_id, created_at, last_modified_at, deleted_at)
  WHERE item_type IN ('TEAMS_CHAT_MESSAGE','TEAMS_MESSAGE','TEAMS_MESSAGE_REPLY');

CREATE INDEX IF NOT EXISTS idx_jobs_parent_type_status
  ON jobs (parent_id, job_type, status);

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_type_status
  ON jobs (tenant_id, job_type, status)
  WHERE status IN ('PENDING','RUNNING');
