SET search_path TO tm_vault, public;

CREATE TABLE IF NOT EXISTS storage_backends (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind VARCHAR NOT NULL,
    name VARCHAR NOT NULL UNIQUE,
    endpoint VARCHAR NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref VARCHAR NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_config (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    active_backend_id UUID NOT NULL REFERENCES storage_backends(id),
    transition_state VARCHAR NOT NULL DEFAULT 'stable',
    last_toggle_at TIMESTAMPTZ,
    cooldown_until TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS storage_toggle_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id UUID NOT NULL,
    actor_ip INET,
    from_backend_id UUID NOT NULL REFERENCES storage_backends(id),
    to_backend_id UUID NOT NULL REFERENCES storage_backends(id),
    reason VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'started',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    drain_completed_at TIMESTAMPTZ,
    flip_completed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS backend_id UUID REFERENCES storage_backends(id);
ALTER TABLE snapshot_items ADD COLUMN IF NOT EXISTS backend_id UUID REFERENCES storage_backends(id);

SELECT 'storage_backends=' || to_regclass('tm_vault.storage_backends')::text || '  system_config=' || to_regclass('tm_vault.system_config')::text AS status;
