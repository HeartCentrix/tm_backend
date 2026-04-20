-- Files folder-select v2: composite index for the folder-scope resolver.
-- Speeds up `WHERE snapshot_id = :s AND folder_path LIKE ANY(:prefixes)`
-- used by shared/folder_resolver.py. text_pattern_ops makes LIKE with a
-- trailing wildcard index-supported.

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_snapshot_items_snapshot_folder_path
  ON snapshot_items (snapshot_id, folder_path text_pattern_ops);
