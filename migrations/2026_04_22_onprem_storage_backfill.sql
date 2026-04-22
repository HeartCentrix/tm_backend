-- migrations/2026_04_22_onprem_storage_backfill.sql
-- Run AFTER 2026_04_21_onprem_storage_schema.sql is applied AND the
-- storage_backends seed row exists. Backfills backend_id on all existing
-- snapshots + items to azure-primary, then sets NOT NULL.

BEGIN;

SET LOCAL search_path TO tm, public;

DO $$
DECLARE
  az_id UUID;
BEGIN
  SELECT id INTO az_id FROM storage_backends WHERE name = 'azure-primary' LIMIT 1;
  IF az_id IS NULL THEN
    RAISE EXCEPTION 'azure-primary backend not seeded — run 2026_04_21_onprem_storage_schema.sql first';
  END IF;

  UPDATE snapshots      SET backend_id = az_id WHERE backend_id IS NULL;
  UPDATE snapshot_items SET backend_id = az_id WHERE backend_id IS NULL;
END $$;

ALTER TABLE snapshots      ALTER COLUMN backend_id SET NOT NULL;
ALTER TABLE snapshot_items ALTER COLUMN backend_id SET NOT NULL;

COMMIT;
