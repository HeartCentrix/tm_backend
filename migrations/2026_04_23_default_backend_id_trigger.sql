-- Follow-up to 2026_04_21_onprem_storage_schema.sql
-- Adds a BEFORE INSERT trigger on snapshots + snapshot_items that fills
-- backend_id with system_config.active_backend_id when the inserter
-- (legacy worker code still using azure_storage_manager directly) omits
-- it. Without this, every snapshot insert fails the NOT NULL constraint
-- until the per-worker codemod migrates call sites to StorageRouter.

SET search_path TO tm, public;

CREATE OR REPLACE FUNCTION default_snapshot_backend_id()
RETURNS trigger AS $$
BEGIN
  IF NEW.backend_id IS NULL THEN
    SELECT active_backend_id INTO NEW.backend_id FROM system_config WHERE id = 1;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_snapshots_default_backend ON snapshots;
CREATE TRIGGER trg_snapshots_default_backend
  BEFORE INSERT ON snapshots
  FOR EACH ROW EXECUTE FUNCTION default_snapshot_backend_id();

DROP TRIGGER IF EXISTS trg_snapshot_items_default_backend ON snapshot_items;
CREATE TRIGGER trg_snapshot_items_default_backend
  BEFORE INSERT ON snapshot_items
  FOR EACH ROW EXECUTE FUNCTION default_snapshot_backend_id();
