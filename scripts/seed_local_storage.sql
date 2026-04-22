SET search_path TO tm_vault, public;

-- Azure backend row (so existing snapshots/items with NULL backend_id can be
-- back-stamped and routed via AzureBlobStore for restore).
INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config, is_enabled)
VALUES (
    'azure_blob',
    'azure-primary',
    'https://stamitkmishr369164905478.blob.core.windows.net',
    'env://AZURE_STORAGE_ACCOUNT_KEY',
    jsonb_build_object(
        'shards', jsonb_build_array(
            jsonb_build_object('account', 'stamitkmishr369164905478', 'key_ref', 'env://AZURE_STORAGE_ACCOUNT_KEY')
        )
    ),
    TRUE
)
ON CONFLICT (name) DO NOTHING;

-- Local SeaweedFS backend row (the new primary).
INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config, is_enabled)
VALUES (
    'seaweedfs',
    'local-seaweedfs',
    'http://seaweedfs:8333',
    'env://ONPREM_S3_SECRET_KEY',
    jsonb_build_object(
        'buckets', jsonb_build_array('tmvault-shard-0'),
        'region', 'us-east-1',
        'verify_tls', false,
        'access_key_env', 'ONPREM_S3_ACCESS_KEY',
        'upload_concurrency', 8,
        'multipart_threshold_mb', 100
    ),
    TRUE
)
ON CONFLICT (name) DO NOTHING;

-- Flip active backend to local seaweedfs.
INSERT INTO system_config (id, active_backend_id, transition_state)
SELECT 1, sb.id, 'stable'
FROM storage_backends sb
WHERE sb.name = 'local-seaweedfs'
ON CONFLICT (id) DO UPDATE
SET active_backend_id = EXCLUDED.active_backend_id,
    transition_state = 'stable',
    updated_at = NOW();

-- Back-stamp existing snapshots/items that have NULL backend_id to point at the
-- Azure row (so old data keeps restoring from Azure after the flip).
UPDATE snapshots
SET backend_id = (SELECT id FROM storage_backends WHERE name = 'azure-primary')
WHERE backend_id IS NULL;

UPDATE snapshot_items
SET backend_id = (SELECT id FROM storage_backends WHERE name = 'azure-primary')
WHERE backend_id IS NULL;

-- Show final state.
SELECT id, kind, name, endpoint, is_enabled FROM storage_backends ORDER BY kind, name;
SELECT id, active_backend_id, transition_state FROM system_config;
