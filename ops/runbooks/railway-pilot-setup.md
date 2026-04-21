# Railway Pilot Setup

Stand up the on-prem pilot on Railway in a day. Full client-DC deployment
is documented separately in the design spec Appendix B.

## Prereqs

- Railway account with billing enabled
- `railway` CLI: `npm install -g @railway/cli`
- `aws` CLI
- Read access to the existing `tmvault-azure` Railway project

## 1. Create the pilot project

```bash
railway login
railway init tmvault-onprem-pilot
cd tmvault-onprem-pilot
railway environment create pilot
railway use pilot
```

## 2. Deploy SeaweedFS

Create `services/seaweedfs/railway.toml`:

```toml
[deploy]
startCommand = "weed server -dir=/data -ip=0.0.0.0 -s3 -s3.config=/config/s3.json -filer -master.volumeSizeLimitMB=1024"

[build]
image = "chrislusf/seaweedfs:3.71"

[[volumes]]
mountPath = "/data"
size = "30Gi"

[[volumes]]
mountPath = "/config"
size = "100Mi"
```

Upload a `/config/s3.json` into the volume with your chosen pilot creds and
Object Lock enabled:

```json
{
  "identities": [
    {
      "name": "tmvault-pilot",
      "credentials": [
        { "accessKey": "PILOT_ACCESS", "secretKey": "PILOT_SECRET" }
      ],
      "actions": ["Admin"]
    }
  ],
  "buckets": { "objectLockEnabled": true, "versioning": "Enabled" }
}
```

Deploy:

```bash
railway up services/seaweedfs
```

Note the internal URL (`seaweedfs.railway.internal:8333`).

## 3. Create the pilot bucket with Object Lock

```bash
export ENDPOINT=http://seaweedfs.railway.internal:8333   # or public railway URL
python tm_backend/scripts/create_seaweedfs_buckets.py \
  --endpoint $ENDPOINT \
  --access-key PILOT_ACCESS \
  --secret-key PILOT_SECRET \
  --buckets tmvault-shard-0 \
  --default-retention-days 30 \
  --default-retention-mode GOVERNANCE
```

**Known issue** — SeaweedFS 3.71 returns `BucketAlreadyExists` for
`put_object_lock_configuration` on re-runs. The script tolerates this.
Per-object retention (`put_object_retention`) still works and is what
TMvault uses on every backup.

## 4. Add a Postgres replica

```bash
railway add postgresql
railway variables   # note DATABASE_URL
```

Set up logical replication from the `tmvault-azure` primary:

```sql
-- On Azure primary
ALTER SYSTEM SET wal_level = logical;
SELECT pg_reload_conf();
CREATE PUBLICATION tmvault_pub FOR ALL TABLES IN SCHEMA tm;

-- On pilot replica
CREATE SUBSCRIPTION tmvault_sub
  CONNECTION 'host=<azure-host> port=5432 dbname=tmvault user=tmvault_repl password=<pw>'
  PUBLICATION tmvault_pub;
```

## 5. Deploy worker pool + storage-toggle-worker

```bash
railway add --template backup-worker
railway variables set \
  ONPREM_S3_ENDPOINT=http://seaweedfs.railway.internal:8333 \
  ONPREM_S3_ACCESS_KEY=PILOT_ACCESS \
  ONPREM_S3_SECRET_KEY=PILOT_SECRET \
  ONPREM_S3_BUCKETS=tmvault-shard-0 \
  ONPREM_S3_VERIFY_TLS=false \
  DATABASE_URL=<pilot-replica-url> \
  RABBITMQ_URL=<federated-or-shared-rmq-url>
```

Repeat for `restore-worker` and a new service built from
`services/storage_toggle_worker/Dockerfile` for the toggle orchestrator.

Set toggle-worker strategy env to `noop` for the pilot (no real
kubectl/patroni/DNS in play):

```bash
railway variables set \
  PG_PROMOTE_STRATEGY=noop \
  DNS_FLIP_STRATEGY=noop \
  WORKER_RESTART_STRATEGY=noop
```

## 6. Apply migrations

```bash
docker run --rm -i -e PGPASSWORD=<pw> postgres:16-alpine \
  psql -h <pilot-pg-host> -U postgres -d railway \
  < tm_backend/migrations/2026_04_21_onprem_storage_schema.sql

docker run --rm -i -e PGPASSWORD=<pw> postgres:16-alpine \
  psql -h <pilot-pg-host> -U postgres -d railway \
  < tm_backend/migrations/2026_04_22_onprem_storage_backfill.sql
```

## 7. Seed the SeaweedFS backend row

```bash
psql -h <pilot-pg-host> -U postgres -d railway <<'SQL'
SET search_path TO tm, public;

INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config)
VALUES ('seaweedfs', 'onprem-pilot',
        'http://seaweedfs.railway.internal:8333',
        'env://ONPREM_S3_SECRET_KEY',
        jsonb_build_object(
          'buckets', jsonb_build_array('tmvault-shard-0'),
          'region', 'us-east-1',
          'verify_tls', false,
          'access_key_env', 'ONPREM_S3_ACCESS_KEY'
        ))
ON CONFLICT (name) DO UPDATE
SET endpoint = EXCLUDED.endpoint, config = EXCLUDED.config;
SQL
```

## 8. Smoke test

```bash
PYTHONPATH=tm_backend python tm_backend/scripts/smoke_onprem_pilot.py \
  --endpoint http://seaweedfs.railway.internal:8333 \
  --access-key PILOT_ACCESS --secret-key PILOT_SECRET \
  --bucket tmvault-shard-0
```

Expected: steps 1–3, 5, 6 print `ok`. **Step 4** should print
`ok — WORM correctly blocked delete`. If it prints `!!! delete
unexpectedly succeeded !!!`, your SeaweedFS build/version doesn't
enforce GOVERNANCE retention on objects. Options:
- Upgrade to a SeaweedFS release with Object Lock enforcement.
- Set `default-retention-mode=COMPLIANCE` (stricter).
- Accept the limitation for pilot but do **not** ship to clients
  claiming WORM until enforcement is verified on the chosen build.

## 9. First toggle via UI

1. Log into the TMvault web UI as org-admin.
2. Navigate to **Settings → Storage**.
3. Verify `Active: azure-primary`.
4. Click **Switch to onprem-pilot**.
5. All preflight checks green. Enter reason `Railway pilot initial cutover`.
6. Type `TMVAULT-ORG`. Submit.
7. Watch the SSE stream — expect `completed` in 5–10 min.
8. Run a small tenant backup. Verify a blob lands in the SeaweedFS bucket
   and that `tm.snapshot_items.backend_id` on the new row equals the
   `onprem-pilot` backend id.

## 10. Toggle back

Run the toggle in reverse. Measure phase timings in `storage_toggle_events`
and note any preflight drift.

## Teardown

```bash
railway environment delete pilot
```

Or keep it deployed and scale services to 0 for regression testing.

## Pilot limitations (don't ship as prod)

- **Single-container SeaweedFS** — no HA. Container restart = brief outage.
- **Volume size capped** by Railway plan (~100 GB on Pro).
- **No real cross-region replication** — logical only.
- **Perf not representative** of a real 4–8 node cluster.
- **Object Lock enforcement** depends on SeaweedFS build; verify via the
  smoke script before trusting WORM claims.
