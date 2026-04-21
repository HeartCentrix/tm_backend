-- migrations/2026_04_21_onprem_storage_schema.sql
-- On-prem storage backend + toggle schema. Idempotent; re-runnable.
-- Follow-up: 2026_04_22_onprem_storage_backfill.sql (backfills backend_id
-- on existing snapshots/items and sets NOT NULL).

BEGIN;

-- All TMvault relations live in the `tm` schema (see shared/config DB_SCHEMA).
SET LOCAL search_path TO tm, public;

-- 1. Storage backend registry
CREATE TABLE IF NOT EXISTS storage_backends (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind         TEXT NOT NULL CHECK (kind IN ('azure_blob','seaweedfs')),
  name         TEXT NOT NULL UNIQUE,
  endpoint     TEXT NOT NULL,
  config       JSONB NOT NULL DEFAULT '{}'::jsonb,
  secret_ref   TEXT NOT NULL,
  is_enabled   BOOLEAN NOT NULL DEFAULT true,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Singleton runtime state
CREATE TABLE IF NOT EXISTS system_config (
  id                 SMALLINT PRIMARY KEY CHECK (id = 1),
  active_backend_id  UUID NOT NULL REFERENCES storage_backends(id),
  transition_state   TEXT NOT NULL DEFAULT 'stable'
                     CHECK (transition_state IN ('stable','draining','flipping')),
  last_toggle_at     TIMESTAMPTZ,
  cooldown_until     TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Audit / orchestration log
CREATE TABLE IF NOT EXISTS storage_toggle_events (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_id             UUID NOT NULL,
  actor_ip             INET,
  from_backend_id      UUID NOT NULL REFERENCES storage_backends(id),
  to_backend_id        UUID NOT NULL REFERENCES storage_backends(id),
  reason               TEXT,
  status               TEXT NOT NULL DEFAULT 'started',
  started_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  drain_completed_at   TIMESTAMPTZ,
  flip_completed_at    TIMESTAMPTZ,
  completed_at         TIMESTAMPTZ,
  error_message        TEXT,
  pre_flight_checks    JSONB,
  drained_job_count    INTEGER,
  retried_job_count    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_toggle_events_actor
  ON storage_toggle_events(actor_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_toggle_events_status
  ON storage_toggle_events(status)
  WHERE status NOT IN ('completed','aborted','failed');

-- 4. Add backend_id to snapshots + snapshot_items (nullable until backfill)
ALTER TABLE snapshots
  ADD COLUMN IF NOT EXISTS backend_id UUID REFERENCES storage_backends(id);
ALTER TABLE snapshot_items
  ADD COLUMN IF NOT EXISTS backend_id UUID REFERENCES storage_backends(id);

CREATE INDEX IF NOT EXISTS idx_snapshots_backend ON snapshots(backend_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_items_backend ON snapshot_items(backend_id);

-- 5. Job retry plumbing
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS retry_reason TEXT;
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS pre_toggle_job_id UUID REFERENCES jobs(id);

-- 6. Seed azure-primary backend + initial system_config row
-- Real endpoint/account values come from env; placeholders here get
-- overwritten via a follow-up UPDATE in the deployment script (see
-- ops/runbooks/railway-pilot-setup.md step 7).
INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config)
SELECT 'azure_blob', 'azure-primary',
       'https://PLACEHOLDER.blob.core.windows.net',
       'env://AZURE_STORAGE_ACCOUNT_KEY',
       jsonb_build_object(
         'shards', jsonb_build_array(
           jsonb_build_object(
             'account', 'PLACEHOLDER',
             'key_ref', 'env://AZURE_STORAGE_ACCOUNT_KEY'
           )
         )
       )
WHERE NOT EXISTS (SELECT 1 FROM storage_backends WHERE name = 'azure-primary');

INSERT INTO system_config (id, active_backend_id)
SELECT 1, (SELECT id FROM storage_backends WHERE name = 'azure-primary')
WHERE NOT EXISTS (SELECT 1 FROM system_config WHERE id = 1);

-- 7. NOTIFY trigger on system_config changes (router LISTENs on this)
CREATE OR REPLACE FUNCTION notify_system_config_changed()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('system_config_changed', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_system_config_notify ON system_config;
CREATE TRIGGER trg_system_config_notify
  AFTER UPDATE OR INSERT ON system_config
  FOR EACH ROW
  EXECUTE FUNCTION notify_system_config_changed();

COMMIT;
