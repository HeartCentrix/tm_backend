# On-Prem Backup Store with Azure-↔-On-Prem Toggle — Design

**Status:** Draft for review
**Author:** Rohit Sharma
**Date:** 2026-04-21
**Branch:** `on-prem`
**Spec location:** `tm_backend/docs/superpowers/specs/2026-04-21-onprem-backup-store-design.md`

---

## 1. Goal

Add an on-premise object storage backend (SeaweedFS) alongside existing Azure Blob, with an org-admin-driven toggle that cuts over the **entire** backup/restore pipeline — storage, workers, control plane — between the two. Pilot runs on Railway with minimal infra; client deployment uses real on-prem DC hardware. Application code is identical in both; only deployment topology and env vars differ.

### Non-goals

- Selling WORM-certified compliance to external customers (internal use only)
- Dual-write (both backends simultaneously) — explicitly a single-active-backend system
- Multi-region within on-prem (single DC assumed per client)
- Auto-toggle / automated failover (toggle is always human-triggered)

## 2. Decision summary

| # | Area | Decision | Rationale |
|---|------|----------|-----------|
| 1 | On-prem backend engine | **SeaweedFS distributed** (Apache 2.0) | S3-compat → minimal code change; Apache 2.0 → no AGPL/licensing drama |
| 2 | Toggle semantics | **Global complete cutover** (one active backend at a time) | User requirement; simpler than per-SLA or dual-write |
| 3 | Worker placement | **Follow the toggle** (Azure-mode workers run in cloud; on-prem-mode workers run in DC) | Preserves Azure SSC fast path in Azure mode; LAN-speed in on-prem mode |
| 4 | Existing data on toggle | **Passthrough read** (old snapshots stay on old backend, readable via `backend_id`) + **optional** background migration (deferred) | Instant toggle, zero data movement cost at flip time |
| 5 | Control plane | **Follow the toggle** — Postgres streaming replication + promote-replica; RabbitMQ federation; DNS flip | Workers talk to local DB/queue at LAN latency in both modes |
| 6 | WORM | **Full parity** via SeaweedFS Object Lock + legal hold | Ransomware protection on both backends |
| 7 | Toggle UX | **Org-admin**, Admin UI (Settings → Storage), graceful drain, retry in-flight on new backend, type-org-name confirm, audit table, 30-min cooldown, pre-flight health check | User-selected answers |
| 8 | Network link (prod client) | **Site-to-site VPN** (IPsec) — start here; ExpressRoute optional later | Cheapest option that covers DB replication + passthrough restores |
| 9 | Retention | **Per-SLA-policy** (reuse `sla_policies.retention_days`) | No new config, leverages existing model |
| 10 | Backup cadence | **3×/day (existing SLAs)**, unchanged by toggle | Perf parity requirement |
| 11 | Pilot env | **Railway** (single-container SeaweedFS + 2nd Postgres + small worker pool ~$30-60/mo) | Validates abstraction + toggle state machine without infra spend |

## 3. Architecture

### 3.1 Two deployment zones, one active

```
┌─ Azure Cloud Zone ────────────────┐       ┌─ On-Prem DC Zone ─────────────┐
│                                   │       │                               │
│  api-gateway                      │       │  api-gateway                  │
│  snapshot-service                 │       │  snapshot-service             │
│  backup-scheduler                 │       │  backup-scheduler             │
│  job-service                      │       │  job-service                  │
│  restore-worker pool              │  VPN  │  restore-worker pool          │
│  backup-worker pool               │◄════►│  backup-worker pool           │
│  azure-workload-worker            │       │  azure-workload-worker        │
│  chat-export-worker               │       │  chat-export-worker           │
│  dr-replication-worker            │       │  dr-replication-worker        │
│  storage-toggle-worker            │       │  storage-toggle-worker        │
│                                   │       │                               │
│  Postgres (primary OR replica)    │       │  Postgres (replica OR primary)│
│  RabbitMQ cluster                 │◄════►│  RabbitMQ cluster (federated) │
│  Azure Blob Storage (N shards)    │       │  SeaweedFS cluster (N shards) │
│                                   │       │                               │
└───────────────────────────────────┘       └───────────────────────────────┘
        ▲                                             ▲
        │                                             │
        └──── Active side handles all traffic ────────┘
        └──── Inactive side: DB replica, stopped workers ────
```

### 3.2 Runtime state machine (singleton in Postgres)

States: `stable → draining → flipping → stable`. Persisted in `system_config` table (single row, CHECK-enforced). Postgres `NOTIFY system_config_changed` fires on every update → all services pick up live via `LISTEN` without restart.

### 3.3 Component inventory

| Component | Type | Change scope |
|-----------|------|--------------|
| `shared/storage/base.py` | NEW | `BackendStore` protocol, `BlobInfo`/`BlobProps` dataclasses |
| `shared/storage/azure_blob.py` | NEW (extracted) | Azure impl of the protocol; wraps existing `AzureStorageShard` logic |
| `shared/storage/seaweedfs.py` | NEW | SeaweedFS/S3 impl via `aioboto3` |
| `shared/storage/router.py` | NEW | `StorageRouter` singleton; routes by `active_backend` for writes and `item.backend_id` for reads |
| `shared/azure_storage.py` | DEPRECATE | Keep file for backward-compat during migration; remove after all call sites moved |
| `shared/models.py` | MODIFY | Add `StorageBackend`, `SystemConfig`, `StorageToggleEvent`, `StorageMigrationJob` ORM classes; add `backend_id` FK on `Snapshot`, `SnapshotItem`; add `retry_reason`, `pre_toggle_job_id` on `Job` |
| `shared/schemas.py` | MODIFY | Pydantic DTOs for toggle API |
| `shared/config.py` | MODIFY | Add `ONPREM_S3_*` env vars |
| All 26 files using `azure_storage_manager` (214 call sites) | MODIFY (codemod) | Route through `StorageRouter` |
| `api-gateway` | MODIFY | Admin endpoints: `POST /api/admin/storage/toggle`, `GET /api/admin/storage/status`, `GET /api/admin/storage/events`, `POST /api/admin/storage/toggle/:id/abort` |
| `tm_vault` frontend | MODIFY | Settings → Storage page (state card, preflight, history, toggle modal w/ live SSE updates) |
| `services/storage-toggle-worker` | NEW microservice | Orchestrates 8-phase toggle flow |
| `shared/retention.py` | NEW | `compute_retention_until(sla, created_at)` + mode mapping helpers |
| `shared/retention_cleanup.py` | MODIFY | Use router; skip expected `ImmutableBlob` errors |

## 4. Data model

### 4.1 New tables

```sql
CREATE TABLE storage_backends (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind         TEXT NOT NULL CHECK (kind IN ('azure_blob','seaweedfs')),
  name         TEXT NOT NULL UNIQUE,
  endpoint     TEXT NOT NULL,
  config       JSONB NOT NULL DEFAULT '{}',   -- shards/buckets, region, tls_verify, ca_bundle ref
  secret_ref   TEXT NOT NULL,                  -- pointer into secret manager
  is_enabled   BOOLEAN NOT NULL DEFAULT true,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE system_config (
  id                 SMALLINT PRIMARY KEY CHECK (id = 1),   -- singleton
  active_backend_id  UUID NOT NULL REFERENCES storage_backends(id),
  transition_state   TEXT NOT NULL DEFAULT 'stable'
                      CHECK (transition_state IN ('stable','draining','flipping')),
  last_toggle_at     TIMESTAMPTZ,
  cooldown_until     TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE storage_toggle_events (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_id             UUID NOT NULL REFERENCES users(id),
  actor_ip             INET,
  from_backend_id      UUID NOT NULL REFERENCES storage_backends(id),
  to_backend_id        UUID NOT NULL REFERENCES storage_backends(id),
  reason               TEXT,
  status               TEXT NOT NULL DEFAULT 'started',
                       -- started | drain_started | drain_completed | db_promoted |
                       -- dns_flipped | workers_restarted | smoke_passed |
                       -- completed | aborted | failed
  started_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  drain_completed_at   TIMESTAMPTZ,
  flip_completed_at    TIMESTAMPTZ,
  completed_at         TIMESTAMPTZ,
  error_message        TEXT,
  pre_flight_checks    JSONB,
  drained_job_count    INTEGER,
  retried_job_count    INTEGER
);

CREATE INDEX idx_toggle_events_actor  ON storage_toggle_events(actor_id, started_at DESC);
CREATE INDEX idx_toggle_events_status ON storage_toggle_events(status)
  WHERE status NOT IN ('completed','aborted','failed');

-- DEFERRED to phase 8 (background migration feature)
CREATE TABLE storage_migration_jobs (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  from_backend_id    UUID NOT NULL REFERENCES storage_backends(id),
  to_backend_id      UUID NOT NULL REFERENCES storage_backends(id),
  filter             JSONB,
  status             TEXT NOT NULL DEFAULT 'pending',
  total_bytes        BIGINT,
  copied_bytes       BIGINT DEFAULT 0,
  total_items        BIGINT,
  copied_items       BIGINT DEFAULT 0,
  throughput_bytes_s BIGINT,
  started_by         UUID REFERENCES users(id),
  started_at         TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ DEFAULT NOW(),
  completed_at       TIMESTAMPTZ,
  error_message      TEXT
);
```

### 4.2 Modified tables

```sql
ALTER TABLE snapshots       ADD COLUMN backend_id UUID REFERENCES storage_backends(id);
ALTER TABLE snapshot_items  ADD COLUMN backend_id UUID REFERENCES storage_backends(id);
ALTER TABLE jobs            ADD COLUMN retry_reason TEXT;
ALTER TABLE jobs            ADD COLUMN pre_toggle_job_id UUID REFERENCES jobs(id);

-- Backfill + enforce NOT NULL for backend_id in single Alembic migration:
UPDATE snapshots       SET backend_id = <azure_backend_id> WHERE backend_id IS NULL;
UPDATE snapshot_items  SET backend_id = <azure_backend_id> WHERE backend_id IS NULL;
ALTER TABLE snapshots      ALTER COLUMN backend_id SET NOT NULL;
ALTER TABLE snapshot_items ALTER COLUMN backend_id SET NOT NULL;

CREATE INDEX idx_snapshots_backend      ON snapshots(backend_id);
CREATE INDEX idx_snapshot_items_backend ON snapshot_items(backend_id);
```

### 4.3 Seed data (in migration)

```sql
INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config)
VALUES
  ('azure_blob','azure-primary',  '<AZURE_STORAGE_BLOB_ENDPOINT>',
   'env://AZURE_STORAGE_ACCOUNT_KEY',
   '{"shards":[{"account":"<name>","key_ref":"env://AZURE_STORAGE_ACCOUNT_KEY"}]}'),
  ('seaweedfs',  'onprem-dc1',    '<ONPREM_S3_ENDPOINT>',
   'env://ONPREM_S3_SECRET_KEY',
   '{"buckets":["tmvault-shard-0"],"region":"us-east-1","verify_tls":true}');

INSERT INTO system_config (id, active_backend_id)
VALUES (1, (SELECT id FROM storage_backends WHERE name='azure-primary'));
```

### 4.4 Key invariants

- `snapshot_items.backend_id` is **permanent** and authoritative for reads. Toggle never rewrites it.
- Exactly one row in `system_config` (CHECK `id = 1`).
- `storage_toggle_events` is append-only; status transitions forward only.
- Foreign keys on `backend_id` prevent a backend row from being deleted while items reference it (must disable + archive instead).

## 5. Storage backend abstraction

### 5.1 `BackendStore` protocol (`shared/storage/base.py`)

Complete method list (see Section 3.1 of brainstorming discussion for full signatures):

- **Upload / download:** `upload`, `upload_from_file`, `download`, `download_stream`
- **Multipart / block:** `stage_block`, `commit_blocks`, `put_block_from_url`
- **Server-side copy:** `server_side_copy` (same-backend only; caller falls back to stream copy otherwise)
- **Listing / metadata:** `list_blobs`, `list_with_props`, `get_properties`, `delete`
- **Presigned URL:** `presigned_url(valid_hours=6)`
- **WORM:** `apply_immutability(until, mode)`, `apply_legal_hold`, `remove_legal_hold`
- **Lifecycle:** `apply_lifecycle(hot_days, cool_days, archive_days)`
- **Infra:** `ensure_container`, `close`
- **Sharding:** `shard_for(tenant_id, resource_id) -> BackendStore`

Two dataclasses: `BlobInfo` (carries `backend_id` → persisted on `SnapshotItem`) and `BlobProps`.

### 5.2 `AzureBlobStore` implementation

Wraps existing `AzureStorageShard` + `AzureStorageManager`. Same behavior as today; merely moved behind the protocol. File `shared/azure_storage.py` is retained as a shim during migration and deleted after all 214 call sites route via `StorageRouter`.

### 5.3 `SeaweedStore` implementation

Built on `aioboto3`. Feature mapping:

| `BackendStore` method | S3 API call |
|----------------------|-------------|
| `upload` | `put_object` |
| `upload_from_file` | `upload_fileobj` (multipart) |
| `download` | `get_object.read()` |
| `download_stream` | iterate `get_object` body |
| `stage_block` | `upload_part` |
| `commit_blocks` | `complete_multipart_upload` |
| `put_block_from_url` | `upload_part_copy` (with `x-amz-copy-source`) |
| `server_side_copy` | `copy_object` (same-cluster) |
| `list_blobs` / `list_with_props` | `list_objects_v2` |
| `get_properties` | `head_object` |
| `delete` | `delete_object` |
| `presigned_url` | `generate_presigned_url('get_object', ...)` |
| `apply_immutability` | `put_object_retention(Mode='COMPLIANCE'/'GOVERNANCE', RetainUntilDate)` |
| `apply_legal_hold` | `put_object_legal_hold(Status='ON')` |
| `remove_legal_hold` | `put_object_legal_hold(Status='OFF')` |
| `apply_lifecycle` | `put_bucket_lifecycle_configuration` |

Mode translation (caller passes `'Locked'`/`'Unlocked'`, backend translates):
- Azure `Locked` → SeaweedFS `COMPLIANCE`
- Azure `Unlocked` → SeaweedFS `GOVERNANCE`

Key layout in SeaweedFS:
```
bucket = tmvault-shard-{idx}   (created with ObjectLockEnabledForBucket + versioning)
key    = {workload}/{tenant_id}/{resource_id}/{snapshot_id}/{timestamp}/{item_id}
```

### 5.4 `StorageRouter` (`shared/storage/router.py`)

Singleton per process. Primary API:

- `get_active_store() -> BackendStore` — for NEW writes. Raises 503 if `transition_state != 'stable'`.
- `get_store_by_id(backend_id) -> BackendStore` — for reads. Item's `backend_id` is the source of truth.
- `get_store_for_item(item) -> BackendStore` — convenience over `backend_id`.
- `get_store_for_snapshot(snapshot) -> BackendStore` — same.
- `writable() -> bool` — checked by api-gateway middleware.
- `list_backends()`, `preflight_check(target_id)` — admin API.

Lifecycle:
- Loaded at service startup (`await router.load()`)
- Background task subscribes to Postgres `LISTEN system_config_changed`, calls `router.reload_on_notify()` when active backend or transition state flips
- Both backends instantiated in every process (so cross-backend passthrough reads work without lazy init latency)

Call-site conversion pattern (codemod in `scripts/migrate_to_router.py`):

```python
# Before
shard = azure_storage_manager.get_shard_for_resource(res_id, tenant_id)
await shard.upload_blob(container, path, content, metadata=meta)

# After (writes)
store = router.get_active_store().shard_for(tenant_id, res_id)
info = await store.upload(container, path, content, metadata=meta)
snapshot_item.backend_id = store.backend_id

# After (reads/restores)
store = router.get_store_for_item(snapshot_item).shard_for(
    snapshot_item.tenant_id, snapshot_item.resource_id)
async for chunk in store.download_stream(container, path):
    ...
```

Codemod handles ~180 sites mechanically; remainder (tests + edge cases) done per-worker in focused PRs.

### 5.5 Cross-backend passthrough

Rule: **`item.backend_id` always wins** over `active_backend`.

Scenario: active=onprem, user restores a pre-toggle Azure snapshot.
- Worker runs in DC zone
- `router.get_store_for_item(item)` returns `AzureBlobStore` (because `item.backend_id = azure_id`)
- Both backends already initialized in router → worker streams Azure blob → DC over VPN → user

Bandwidth path chosen automatically by `backend_id`. No conditional logic in workers.

### 5.6 Error handling

| Condition | Behavior |
|-----------|----------|
| Upload fail on active backend | Existing retry logic (exponential backoff) unchanged |
| Read fail on specific `backend_id` | Surface to user as "Source storage unreachable. Retry later." No fallback — the data is NOT on any other backend. |
| During `draining` | Writes return 503; reads allowed by any `backend_id`; in-flight writes finish or time out |
| During `flipping` | Writes return 503; reads allowed |
| Preflight failure | Toggle aborted, no state change |

## 6. Toggle workflow (orchestrated by `storage-toggle-worker`)

### 6.1 Phase sequence (azure → onprem; reverse is symmetric)

| Phase | Action | Event status emitted |
|-------|--------|----------------------|
| 0 | UI: admin submits toggle form with target + reason + org-name confirmation | — |
| 1 | Lock + preflight: acquire PG advisory lock, check cooldown, insert event row, run all preflight checks | `started` |
| 2 | Drain: `UPDATE system_config SET transition_state='draining'`, NOTIFY. API gateways return 503 for new jobs. Poll `jobs` every 5s until in-flight complete (max 15 min). Mark stragglers `aborted_by_toggle`; insert retry job rows with `pre_toggle_job_id` + `retry_reason` | `drain_started`, `drain_completed` |
| 3 | Postgres promote: verify lag < 10s once more. `pg_promote()` on DC replica. Azure primary demoted via `pg_rewind` or recovery.conf → becomes replica of DC | `db_promoted` |
| 4 | DNS flip: update internal CNAME `tmvault.internal` → DC ingress. TTL 30s → propagation ~1 min | `dns_flipped` |
| 5 | Worker restart: SIGTERM Azure-side worker pods. Scale DC pods to full capacity. RMQ federation stops consuming on old side, starts on DC | `workers_restarted` |
| 6 | Cutover: `UPDATE system_config SET active_backend_id=<onprem>, transition_state='flipping'`, NOTIFY | — |
| 7 | Smoke test: canary backup job on small test tenant → verify. Canary restore from canary snapshot. Canary passthrough restore from pre-toggle Azure snapshot | `smoke_passed` |
| 8 | Open for business: `transition_state='stable'`, `cooldown_until = NOW() + 30min`. NOTIFY. Retry jobs pull from queue. Release advisory lock. Event `completed` | `completed` |

**Target wall time: 5-15 minutes.** Dominated by drain (phase 2).

### 6.2 Abort + rollback

| Failure phase | Auto rollback | Manual needed |
|---------------|---------------|----------------|
| 1 (preflight) | No state change | No |
| 2 (drain) | Revert `transition_state=stable`, drop retry marks, re-open API | No |
| 3 (DB promote) | `pg_rewind` back to old primary | Possibly |
| 4 (DNS) | Flip DNS back | No |
| 5 (workers) | Restart old-side, scale down new-side | No |
| 6 (cutover) | Revert + all above | Possibly |
| 7 (smoke) | Keep degraded, alert, admin decides | **Yes** |

Rollback paths write `status='failed'` + `error_message` + `pre_flight_checks` snapshot to the toggle event row for forensics.

### 6.3 Concurrency guards

- `pg_try_advisory_lock(<toggle_lock_id>)` acquired phase 1, released phase 8. Prevents two admins flipping simultaneously even across multiple api-gateway replicas.
- `transition_state` machine guards writers — any service checking `router.writable()` returns 503 during `draining`/`flipping`.
- In-flight jobs: marked `aborted_by_toggle`, new retry job row created with `pre_toggle_job_id` link, partial blobs on old backend cleaned by nightly retention GC (tracked via `snapshot_status='aborted'`).

### 6.4 `storage-toggle-worker` service

- Dedicated microservice; own container
- Deployed to **both** zones; only one runs at a time (via heartbeat-backed PG advisory lock)
- Consumes `storage.toggle` RMQ queue
- Message shape: `{event_id, from_id, to_id, actor_id, reason}`
- Executes phases 2-8; updates `storage_toggle_events` row per transition
- Crash recovery: on restart if `transition_state != 'stable'`, resumes from last phase `status` or initiates rollback

### 6.5 API endpoints (`api-gateway`)

```
POST   /api/admin/storage/toggle
GET    /api/admin/storage/status
GET    /api/admin/storage/events?limit=20
GET    /api/admin/storage/events/:id
POST   /api/admin/storage/toggle/:id/abort   # only if phase ≤ 2
```

- RBAC: `role='org_admin'` required for all
- Audit: every request logged in `audit_log` regardless of outcome
- Live updates during toggle: SSE stream endpoint `GET /api/admin/storage/events/:id/stream`

### 6.6 Frontend (`tm_vault` → Settings → Storage)

Three cards on one page:

- **Current state:** active backend pill, stats (last toggle, cooldown, active jobs), per-backend summary (used / capacity / last write)
- **Preflight checklist:** each check ✓/✗ with detail; retry button
- **History:** table of past events; row click → detail modal with per-phase timeline + errors

Toggle modal:
- Target backend dropdown
- Reason textarea (required, min 10 chars)
- Confirmation field: "Type TMVAULT-ORG to confirm"
- Submit → live SSE status updates ("Draining... 3/247 jobs complete")

### 6.7 Observability

Metrics:
- `tmvault_toggle_phase_duration_seconds{phase,from,to}` histogram
- `tmvault_toggle_drain_jobs_remaining{event_id}` gauge
- `tmvault_toggle_replica_lag_seconds{backend}` gauge
- `tmvault_active_backend{name}` info gauge (1/0)
- `tmvault_toggle_events_total{status}` counter

Alerts:
- `ToggleStuckInDraining` — draining > 20 min
- `ToggleFailedSmoke` — phase 7 failed
- `ReplicaLagHigh` — lag > 30s (blocks future toggles)
- `BackendDown{backend}` — paging
- `WORMDeleteAttempted` — audit, non-paging

## 7. WORM / Object Lock

### 7.1 Parity table

| Guarantee | Azure Blob | SeaweedFS (S3 Object Lock) |
|-----------|-----------|---------------------------|
| Time-based immutability | `set_immutability_policy(expiry_time, mode='Unlocked'/'Locked')` | `put_object_retention(Mode='GOVERNANCE'/'COMPLIANCE', RetainUntilDate)` |
| Legal hold | `set_legal_hold(True/False)` | `put_object_legal_hold(Status='ON'/'OFF')` |
| Lifecycle | ARM `management_policies` | `put_bucket_lifecycle_configuration` |
| Bucket prep | None required | **Object Lock + versioning enabled at bucket creation** (non-retrofittable) |

### 7.2 Retention-until calculation (shared)

`shared/retention.py`:

```python
def compute_retention_until(sla_policy, created_at: datetime) -> datetime:
    return created_at + timedelta(days=sla_policy.retention_days)

def compute_immutability_mode(sla_policy) -> str:
    return 'Locked' if sla_policy.retention_mode == 'locked' else 'Unlocked'
```

Backend translates `Locked`/`Unlocked` internally.

### 7.3 Apply WORM on upload

```
backup-worker uploads blob
  → store.upload(...) succeeds
  → compute retention_until from sla_policy + snapshot.created_at
  → if sla_policy.worm_enabled:  store.apply_immutability(container, path, retention_until, mode)
  → if sla_policy.legal_hold_enabled:  store.apply_legal_hold(container, path, tag=f'sla-{sla_id}')
  → persist snapshot_item (backend_id, retention_until)
```

Sequential — immutability must land after object is durable.

### 7.4 SeaweedFS bucket prep (one-time, infra)

Critical: Object Lock requires bucket-level flag at creation. Runbook: `ops/seaweedfs/init-buckets.yml` (Ansible playbook), idempotent.

```bash
aws s3api --endpoint-url <sw> create-bucket \
  --bucket tmvault-shard-0 --object-lock-enabled-for-bucket
aws s3api --endpoint-url <sw> put-bucket-versioning \
  --bucket tmvault-shard-0 --versioning-configuration Status=Enabled
aws s3api --endpoint-url <sw> put-object-lock-configuration \
  --bucket tmvault-shard-0 \
  --object-lock-configuration 'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=GOVERNANCE,Days=30}}'
```

### 7.5 Retention cleanup

`shared/retention_cleanup.py` updated to use `StorageRouter`; skips expected `ImmutableBlob` errors during locked retention. Deletes DB rows only after backend delete succeeds.

### 7.6 WORM state snapshot in toggle events

Each `storage_toggle_events` row carries:

```json
"worm_state_snapshot": {
  "from_backend": {"immutable_blobs_count": ..., "oldest_retention_until": "...", "legal_hold_count": ...},
  "to_backend":   {"bucket_count": ..., "object_lock_enabled": true, "default_retention_days": ...}
}
```

Proves to auditors that toggle preserved immutability invariants.

### 7.7 Defense in depth

1. App layer: `delete()` respects WORM errors, never bypasses
2. Backend tier: Azure immutability + SeaweedFS Object Lock enforce independently of app
3. COMPLIANCE mode (Locked): not even storage-tier admin can shorten retention; `x-amz-bypass-governance-retention` header is never emitted by TMvault code and any attempt logged

## 8. Deployment topology

### 8.1 Pilot (Railway) — minimal cost

**Purpose:** validate abstraction + toggle state machine + WORM semantics + passthrough. Not a perf-at-scale validation.

```
Railway Project: tmvault-azure              Railway Project: tmvault-onprem-pilot
  (your existing production)                  (new)

  api-gateway, workers, etc.                   SeaweedFS (single container, dev mode)
  Postgres (primary)                           Postgres (replica, streams from primary)
  RabbitMQ (existing)                          Small worker pool (1 backup + 1 restore)
  → AZURE backend (current)                    storage-toggle-worker
                                               → SEAWEEDFS backend
```

Rough spend: ~$30-60/mo incremental.

SeaweedFS container spec:
```yaml
image: chrislusf/seaweedfs:latest
cmd: weed server -dir=/data -s3 -s3.config=/config/s3.json -filer -master.volumeSizeLimitMB=1024
volumes: [/data] # 20-50 GB Railway volume
```

`/config/s3.json` enables `objectLockEnabled: true` + `versioning: Enabled`.

App env (pilot):
```
ONPREM_S3_ENDPOINT=http://seaweedfs.railway.internal:8333
ONPREM_S3_ACCESS_KEY=<test-key>
ONPREM_S3_SECRET_KEY=<test-secret>
ONPREM_S3_BUCKETS=tmvault-shard-0
ONPREM_S3_VERIFY_TLS=false       # dev only
ONPREM_UPLOAD_CONCURRENCY=4
ONPREM_MULTIPART_THRESHOLD_MB=100
ONPREM_RETRY_MAX=3
```

### 8.2 Pilot simplifications vs client production

| Pilot (Railway) | Client prod (real DC) |
|-----------------|----------------------|
| 1 SeaweedFS container, no HA | 4-8 node distributed, EC:4 |
| 1 shard bucket | 4-8 shard buckets |
| Single Postgres replica (same Railway region) | Cross-zone replica over VPN |
| Railway internal DNS | Custom internal DNS + scripted flip |
| No VPN | IPsec site-to-site |
| Simple SQL `pg_promote()` | Patroni / repmgr orchestration |
| Perf NOT representative of scale | Perf matched to current Azure deployment |

**Code path is identical.** Only env vars + deployment size differ.

### 8.3 Client production (later — configurable)

Scale tiers (ops team picks based on budget):

| Tier | Nodes | Raw | Usable (EC:4) | Use when |
|------|-------|-----|--------------|----------|
| Starter | 4 | ~400 TB | ~260 TiB | Pilot graduation, <300 TiB, tight budget |
| Standard | 6 | ~900 TB | ~600 TiB | 400 TiB + growth |
| Growth | 8-12 | 1.5-3 PB | 1-2 PiB | Multi-PB, rack-fault tolerance |

Application requires only:
1. SeaweedFS S3 endpoint reachable from both zones
2. Object Lock + versioning enabled at bucket creation (non-retrofittable)
3. TLS on S3 endpoint
4. ≥3 masters (raft quorum)
5. NTP-synced clocks (Object Lock retention comparisons)
6. Postgres replica reachable from DC workers

Everything else is a deployment-time knob. Hardware spec lives in the client's infra repo, not the app repo.

### 8.4 Network (client prod)

- Site-to-site VPN over Internet (dual-tunnel IPsec, BGP failover) — ~$140/mo Azure VPN Gateway + existing DC edge
- Intra-DC: 2× 25 Gbps TOR per rack, MLAG
- DNS: internal zone `tmvault.internal`, 30s TTL on flipping services (`api`, `pg`, `amqp`)
- TLS: internal CA bundle distributed via ConfigMap

### 8.5 Control-plane replication (client prod)

- Postgres: streaming replication + replication slots + Patroni/repmgr for promote orchestration
- `synchronous_commit = remote_apply` for same-zone hot standby; async across zones
- RabbitMQ: federation upstream both directions (durable queues + publisher confirms → no loss during toggle)

### 8.6 Secrets

Storage backend credentials stored as references in `storage_backends.secret_ref` (never raw). Vault or Azure Key Vault provides actual values. Rotation schedule: 90 days (S3) / 180 days (Azure storage keys).

## 9. Testing strategy

| Layer | Test | Environment |
|-------|------|-------------|
| Unit | Each `BackendStore` impl against its SDK mock | Local (mocked) |
| Contract | One test suite run against both Azure + SeaweedFS impls — verifies identical behavior | Azurite + SeaweedFS docker-compose |
| Router | Mocked backends; all (active × backend_id) routing combinations | Unit |
| Integration | End-to-end backup + restore via router on both backends | docker-compose |
| Toggle rehearsal | Full drain → promote → DNS → workers → smoke on staging | Staging (mini SeaweedFS + PG + workers) |
| WORM compliance | Delete attempts during retention; attempts to shorten `retention_until`; legal hold blocks delete past retention | Both backends |
| Chaos | Kill worker mid-backup; verify toggle retry creates new job with `pre_toggle_job_id` | docker-compose |
| Load (deferred) | 3×/day backup burst on SeaweedFS | Staging @ 10% prod scale |
| DR drill (quarterly) | Full prod toggle rehearsal on staging, measure per-phase timings | Staging |

## 10. Rollout phases

| Phase | Scope | Effort |
|-------|-------|--------|
| **0. Infra prep** (client only; for pilot = skip) | Hardware, rack, VPN, OS, networking, SeaweedFS cluster init, PG replica streaming, RMQ federation | Ops-owned, 2-3 weeks |
| **1. Abstraction layer** | `BackendStore` protocol, `AzureBlobStore`, `SeaweedStore`, `StorageRouter`; unit + contract tests. No production behavior change. | 3 weeks / 1 eng |
| **2. Schema + call-site migration** | Alembic migration, seed rows, codemod routes 214 call sites → router | 2 weeks / 1 eng |
| **3. Toggle machinery** | `storage-toggle-worker`, api endpoints, frontend Settings page, runbook | 3 weeks / 1 eng |
| **4. WORM parity** | `apply_immutability` + `apply_legal_hold` on SeaweedStore; contract tests | 1 week / 1 eng (parallel with phase 3) |
| **5. Staging dry run** | Full toggle rehearsal on staging; chaos drills; team runbook walkthrough | 1 week |
| **6. Pilot on Railway** | Deploy on Railway with minimal SeaweedFS + PG replica; toggle multiple times; measure + debug | 1 week |
| **7. Production cutover (per client)** | Scheduled maintenance window; toggle to on-prem for N hours; monitor; toggle back | 1 day active + 30 days observation |
| **8. (Deferred) Migration tooling** | `storage_migration_jobs` background worker to bulk-copy historical snapshots between backends | Separate effort |

**Total eng time (phases 1-6): ~11 weeks for one eng, ~6-7 weeks for two engineers working in parallel.**

### Go/no-go gates

Each phase gated on:
- Unit + contract + integration tests all green
- Staging rehearsal passes (where applicable)
- Runbook reviewed by ops + on-call
- Preflight checks for next phase return all-green
- Metric thresholds (e.g., replica lag < 10s for 24h before any production toggle)

## 11. Risks + mitigations

| Risk | Mitigation |
|------|-----------|
| SeaweedFS Object Lock bug post-go-live | Chaos tests + audit scans; Azure primary retains all data; can toggle back anytime |
| VPN flap → replica lag spike | Dual-tunnel IPsec + BGP failover; alert on lag > 30s blocks future toggles |
| Postgres promote fails mid-toggle | Advisory lock + phase state machine auto-rollback; manual runbook for stuck state |
| DC power/cooling failure | Toggle back to Azure is the DR path; Azure side is always a hot replica |
| DNS TTL caches stale during flip | Low TTL pre-set + client retry logic handles transient |
| Mail-export ZIP assembly (`put_block_from_url`) across backends | Router sees both src + dst `backend_id`; when they differ, falls back to worker-streamed copy; stays zero-copy when same |
| Compliance audit fails on SeaweedFS WORM | Pilot is internal-only; client compliance needs validated before their production toggle; Azure primary as safety net |
| Codemod misses an `azure_storage_manager` call site | Contract tests + pre-commit grep for stray usages; staging catches functional regressions |
| Toggle flap (accidental re-toggle) | 30-min cooldown enforced in preflight |
| Worker in DC can't reach Graph API (corp firewall rules) | Phase 0 checklist: whitelist Graph API endpoints; explicit preflight check before go-live |

## 12. Open questions (to resolve during implementation)

- Exact SeaweedFS S3 lifecycle rule support for archive-tier (verify hot→cool→archive tiers work; may require tiering to external cold-store via SeaweedFS remote-store feature)
- Precise SLA mapping for legal hold: is it per-SLA-policy boolean today, or per-resource override? Reconcile with `SlaPolicy` model during phase 4
- RabbitMQ federation loop-prevention: confirm message `x-federation-headers` strip policy to avoid repeat-delivery on round trips during transition
- Railway volume size limits for SeaweedFS container (pilot only) — confirm Railway paid plan volume caps

## 13. Appendix — Reference files

- `tm_backend/shared/azure_storage.py` — source of truth for current Azure behavior; target of extraction
- `tm_backend/shared/config.py` — add `ONPREM_S3_*` env vars here
- `tm_backend/shared/models.py` — add new ORM classes + columns here
- `tm_backend/shared/retention_cleanup.py` — update to use router
- `tm_backend/workers/backup-worker/main.py` — one of 26 call sites; representative for codemod patterns
- `tm_backend/workers/restore-worker/main.py` — primary passthrough read consumer
- `tm_backend/workers/restore-worker/mail_export.py` — critical `put_block_from_url` callsite (same-backend-only path)

---

## Appendix A — Railway Pilot Setup Runbook

Step-by-step to stand up the on-prem pilot on Railway. Assumes existing TMvault production project on Railway is `tmvault-azure`.

### A.1 Prerequisites

- Railway account with billing enabled (pilot services add ~$30-60/mo)
- `railway` CLI installed locally: `npm install -g @railway/cli`
- `aws` CLI installed locally (used for creating SeaweedFS buckets over S3 API)
- Access to existing `tmvault-azure` Railway project

### A.2 Create pilot project

```bash
railway login
railway init tmvault-onprem-pilot
cd tmvault-onprem-pilot
railway environment create pilot
railway use pilot
```

### A.3 Deploy SeaweedFS (single-container, dev mode)

Create `services/seaweedfs/railway.toml`:

```toml
[deploy]
startCommand = "weed server -dir=/data -ip=0.0.0.0 -s3 -s3.config=/config/s3.json -filer -master.volumeSizeLimitMB=1024"

[build]
image = "chrislusf/seaweedfs:latest"

[volumes]
"/data" = { size = "30Gi" }
"/config" = { size = "100Mi" }
```

Create `services/seaweedfs/config/s3.json`:

```json
{
  "identities": [
    {
      "name": "tmvault-pilot",
      "credentials": [
        { "accessKey": "PILOT_ACCESS_KEY_CHANGE_ME", "secretKey": "PILOT_SECRET_KEY_CHANGE_ME" }
      ],
      "actions": ["Admin"]
    }
  ],
  "buckets": {
    "objectLockEnabled": true,
    "versioning": "Enabled"
  }
}
```

Deploy:

```bash
railway up services/seaweedfs
railway service --name seaweedfs
# note the internal URL: seaweedfs.railway.internal:8333
```

### A.4 Create the pilot bucket with Object Lock

```bash
export AWS_ACCESS_KEY_ID=PILOT_ACCESS_KEY_CHANGE_ME
export AWS_SECRET_ACCESS_KEY=PILOT_SECRET_KEY_CHANGE_ME
export AWS_DEFAULT_REGION=us-east-1
export SW=http://seaweedfs.railway.internal:8333   # or tunneled public URL during setup

aws s3api --endpoint-url $SW create-bucket \
  --bucket tmvault-shard-0 --object-lock-enabled-for-bucket

aws s3api --endpoint-url $SW put-bucket-versioning \
  --bucket tmvault-shard-0 \
  --versioning-configuration Status=Enabled

aws s3api --endpoint-url $SW put-object-lock-configuration \
  --bucket tmvault-shard-0 \
  --object-lock-configuration \
    'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=GOVERNANCE,Days=30}}'

# Verify
aws s3api --endpoint-url $SW get-bucket-versioning --bucket tmvault-shard-0
aws s3api --endpoint-url $SW get-object-lock-configuration --bucket tmvault-shard-0
```

### A.5 Deploy Postgres replica

Option: use Railway's managed Postgres for the replica (simpler than running Postgres yourself).

```bash
railway add postgresql
railway variables  # note the DATABASE_URL
```

Then configure streaming replication FROM your existing production Postgres (`tmvault-azure` project). Two approaches:

**A.5.a. Logical replication (easier on Railway-managed PG)**
```sql
-- On primary (tmvault-azure project Postgres)
ALTER SYSTEM SET wal_level = logical;
SELECT pg_reload_conf();
CREATE PUBLICATION tmvault_pub FOR ALL TABLES;

-- On pilot replica
CREATE SUBSCRIPTION tmvault_sub
  CONNECTION 'host=<primary-host> port=5432 dbname=tmvault user=<repl_user> password=<pw>'
  PUBLICATION tmvault_pub;
```

**A.5.b. Physical streaming replication** — requires self-hosted PG on Railway; skip for pilot simplicity.

### A.6 Deploy pilot worker pool

Fork your existing worker Docker images. In the pilot project:

```bash
railway add --template backup-worker
railway variables set \
  ACTIVE_ZONE=pilot \
  ONPREM_S3_ENDPOINT=http://seaweedfs.railway.internal:8333 \
  ONPREM_S3_ACCESS_KEY=PILOT_ACCESS_KEY_CHANGE_ME \
  ONPREM_S3_SECRET_KEY=PILOT_SECRET_KEY_CHANGE_ME \
  ONPREM_S3_BUCKETS=tmvault-shard-0 \
  ONPREM_S3_VERIFY_TLS=false \
  ONPREM_UPLOAD_CONCURRENCY=4 \
  ONPREM_MULTIPART_THRESHOLD_MB=100 \
  ONPREM_RETRY_MAX=3 \
  DATABASE_URL=<pilot-replica-url> \
  RABBITMQ_URL=<same-as-azure-rmq-or-federated>
```

Start with 1 replica of backup-worker + 1 of restore-worker.

### A.7 Deploy storage-toggle-worker

Small new service (built in phase 3 of rollout):

```bash
railway add --dockerfile tm_backend/services/storage-toggle-worker/Dockerfile
railway variables set \
  DATABASE_URL=<pilot-replica-url> \
  RABBITMQ_URL=<same-as-above>
```

### A.8 Run Alembic migration (pilot)

From local dev machine:

```bash
cd tm_backend
DATABASE_URL=<pilot-replica-url> alembic upgrade head
```

Then seed the `storage_backends` + `system_config` rows (done by the migration for the Azure row; add SeaweedFS row manually for pilot):

```sql
INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config)
VALUES (
  'seaweedfs',
  'onprem-pilot',
  'http://seaweedfs.railway.internal:8333',
  'env://ONPREM_S3_SECRET_KEY',
  '{"buckets":["tmvault-shard-0"],"region":"us-east-1","verify_tls":false}'
);
```

### A.9 Smoke test — manual

```bash
# From a backup-worker shell (Railway exec)
python -c "
import asyncio
from shared.storage import router
async def main():
    await router.load()
    store = router.get_store_by_id('<pilot-seaweedfs-id>').shard_for('tenant-test','res-test')
    info = await store.upload('pilot-test','hello.txt', b'hi there', metadata={'test':'1'})
    print('uploaded:', info)
    b = await store.download('pilot-test','hello.txt')
    print('downloaded:', b)
asyncio.run(main())
"
```

Expect no errors + bytes round-trip.

### A.10 Toggle test

1. Open TMvault UI → Settings → Storage
2. Should show "Active: Azure Blob"
3. Click "Switch to On-Prem"
4. Preflight checklist shows all green (target reachable, replica lag <10s, workers healthy)
5. Enter reason "Pilot validation", type "TMVAULT-ORG", submit
6. Watch SSE stream: draining → db_promoted → dns_flipped → workers_restarted → smoke_passed → completed
7. Run a small tenant backup → verify blob in SeaweedFS bucket + `backend_id` in `snapshot_items`
8. Toggle back. Verify pre-toggle on-prem snapshot now read via passthrough

### A.11 Teardown (when done piloting)

```bash
railway environment delete pilot
# Or keep it for regression testing, just scale services to 0
```

### A.12 Railway pilot limits

- Single SeaweedFS container = no HA. Container restart = brief downtime
- Volume size limited by Railway plan (typically 100GB on Pro plan)
- No real cross-region replication — pilot uses logical replication only
- Perf not representative of real DC deployment; this is functional validation only

---

## Appendix B — Client On-Prem Deployment Runbook

Step-by-step for a client deploying real on-prem infrastructure. Written as ops-team-facing procedure.

### B.1 Prerequisites checklist

- [ ] Rack space with power + cooling (2 racks recommended for rack-fault tolerance)
- [ ] 4-8 commodity server chassis (spec depends on scale tier chosen — see Section 8.3)
- [ ] 2× TOR switches per rack with MLAG (25 Gbps or better)
- [ ] Internet link at DC (1 Gbps minimum for VPN + optional restore passthrough)
- [ ] Edge router with IPsec VPN capability (FortiGate, pfSense, Cisco ASA, etc.)
- [ ] Azure subscription with Azure VPN Gateway licensed
- [ ] Internal CA for TLS (or use Let's Encrypt if DC has public DNS)
- [ ] NTP servers reachable from all DC nodes
- [ ] DNS internal zone (e.g., `tmvault.internal`) — BIND, AD DNS, or Route53 Private Hosted Zone

### B.2 Infrastructure provisioning (phase 0)

**B.2.1 Rack + network**

1. Install servers in 2 racks, 4 servers per rack for Standard tier
2. Dual-home each server: 2× 25 Gbps to both TOR switches in its rack
3. MLAG between the two TOR switches
4. Spine: 2× 100 Gbps between TOR switches across racks
5. Out-of-band management network (1 Gbps) for BMC access

**B.2.2 Base OS**

All nodes: Ubuntu 22.04 LTS or RHEL 9.

```bash
# Common config
sudo apt update && sudo apt install -y chrony xfsprogs
sudo timedatectl set-ntp true
sudo systemctl enable --now chrony
# Format data drives as XFS
for d in /dev/sd{b,c,d,e,f,g,h,i,j,k,l,m}; do
  sudo mkfs.xfs -f "$d"
done
# Create data mounts
sudo mkdir -p /data/{1..12}
# (fstab entries for each drive)
```

**B.2.3 Kubernetes**

Install K3s (recommended for on-prem simplicity):

```bash
# Control-plane nodes (3)
curl -sfL https://get.k3s.io | sh -s - server --cluster-init

# Additional control-plane
curl -sfL https://get.k3s.io | sh -s - server --server https://<first-cp>:6443 --token <token>

# Worker nodes
curl -sfL https://get.k3s.io | sh -s - agent --server https://<vip>:6443 --token <token>
```

Or use vanilla k8s via kubeadm if preferred.

### B.3 SeaweedFS cluster deployment

**B.3.1 Via Helm (recommended)**

```bash
helm repo add seaweedfs https://seaweedfs.github.io/seaweedfs/helm
helm repo update

cat > seaweedfs-values.yaml <<EOF
master:
  replicas: 3
  resources:
    requests: { cpu: 2, memory: 8Gi }
  data:
    storageClass: local-ssd
    size: 200Gi

volume:
  replicas: 8
  dataDirs:
    - { name: data1, storageClass: local-hdd, size: 20Ti }
    # ... repeat for data2...data12 per node
  resources:
    requests: { cpu: 8, memory: 64Gi }

filer:
  replicas: 3
  resources:
    requests: { cpu: 4, memory: 16Gi }
  # Use Postgres as filer backend
  config: |
    [postgres]
    enabled = true
    hostname = "pg.tmvault.internal"
    port = 5432
    username = "seaweedfs"
    database = "seaweedfs_filer"

s3:
  enabled: true
  replicas: 3
  s3ConfigSecret: seaweedfs-s3-config   # pre-created with identities + object lock
  ingress:
    enabled: true
    host: s3.tmvault.internal
    tls:
      - hosts: [s3.tmvault.internal]
        secretName: s3-tls
EOF

kubectl create namespace seaweedfs

# Create the S3 config secret (with object lock enabled)
kubectl -n seaweedfs create secret generic seaweedfs-s3-config \
  --from-file=s3.json=<(cat <<'EOF'
{
  "identities": [
    {"name": "tmvault", "credentials": [{"accessKey": "<REDACTED>", "secretKey": "<REDACTED>"}], "actions": ["Admin"]}
  ],
  "buckets": { "objectLockEnabled": true, "versioning": "Enabled" }
}
EOF
)

helm install seaweedfs seaweedfs/seaweedfs -n seaweedfs -f seaweedfs-values.yaml
```

**B.3.2 Verify cluster health**

```bash
kubectl -n seaweedfs get pods
kubectl -n seaweedfs exec master-0 -- weed shell <<<'cluster.check'
kubectl -n seaweedfs exec master-0 -- weed shell <<<'volume.list'
```

**B.3.3 Create shard buckets with Object Lock**

```bash
export AWS_ACCESS_KEY_ID=<redacted>
export AWS_SECRET_ACCESS_KEY=<redacted>
export AWS_DEFAULT_REGION=us-east-1
export SW=https://s3.tmvault.internal

for i in 0 1 2 3 4 5 6 7; do
  aws s3api --endpoint-url $SW create-bucket \
    --bucket tmvault-shard-$i --object-lock-enabled-for-bucket
  aws s3api --endpoint-url $SW put-bucket-versioning \
    --bucket tmvault-shard-$i \
    --versioning-configuration Status=Enabled
  aws s3api --endpoint-url $SW put-object-lock-configuration \
    --bucket tmvault-shard-$i \
    --object-lock-configuration \
      'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=GOVERNANCE,Days=30}}'
done

# Verify
for i in 0 1 2 3 4 5 6 7; do
  aws s3api --endpoint-url $SW get-object-lock-configuration --bucket tmvault-shard-$i
done
```

**B.3.4 Set up lifecycle rules**

```bash
cat > lifecycle.json <<EOF
{
  "Rules": [
    {
      "ID": "tier-after-7-days",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "Transitions": [{"Days": 7, "StorageClass": "STANDARD_IA"}]
    }
  ]
}
EOF

for i in 0 1 2 3 4 5 6 7; do
  aws s3api --endpoint-url $SW put-bucket-lifecycle-configuration \
    --bucket tmvault-shard-$i \
    --lifecycle-configuration file://lifecycle.json
done
```

### B.4 Postgres streaming replica

**B.4.1 Deploy Postgres in DC**

Use Patroni for HA-managed Postgres:

```bash
# Use a production-grade helm chart like 'postgres-operator' or 'stackgres'
helm repo add postgres-operator-charts https://opensource.zalando.com/postgres-operator/charts/postgres-operator
helm install postgres-operator postgres-operator-charts/postgres-operator

# Create cluster
kubectl apply -f - <<EOF
apiVersion: acid.zalan.do/v1
kind: postgresql
metadata:
  name: tmvault-pg
spec:
  teamId: tmvault
  numberOfInstances: 2
  volume:
    size: 2Ti
    storageClass: local-nvme
  postgresql:
    version: "15"
  resources:
    requests: { cpu: "8", memory: 64Gi }
  standby:
    s3_wal_path: ""
    standby_host: <azure-pg-primary-host>
    standby_port: 5432
EOF
```

**B.4.2 Configure streaming from Azure primary**

On Azure primary:
```sql
-- Allow DC IP range in pg_hba.conf
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET wal_level = replica;
ALTER SYSTEM SET hot_standby = on;
ALTER SYSTEM SET wal_keep_size = '2GB';
SELECT pg_reload_conf();

CREATE ROLE tmvault_repl WITH REPLICATION PASSWORD '<pw>' LOGIN;

SELECT pg_create_physical_replication_slot('dc_replica_slot');
```

On DC replica, ensure `recovery.conf` / `primary_conninfo` points at Azure primary with the slot name.

Verify lag:
```sql
SELECT client_addr, state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) AS sent_lag,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_lag
FROM pg_stat_replication;
```

### B.5 RabbitMQ federation

**B.5.1 Deploy RMQ cluster in DC**

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install tmvault-rmq bitnami/rabbitmq \
  --set replicaCount=3 \
  --set persistence.enabled=true \
  --set persistence.size=50Gi \
  --set auth.username=tmvault \
  --set auth.password=<redacted>
```

**B.5.2 Set up federation**

Enable federation plugin on both sides:
```bash
kubectl exec -it tmvault-rmq-0 -- rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management
```

Configure federation upstream on DC side pointing at Azure RMQ:
```bash
rabbitmqctl set_parameter federation-upstream azure-primary \
  '{"uri":"amqps://tmvault:<pw>@rmq.azure.tmvault.example:5671","trust-user-id":false}'

rabbitmqctl set_policy --apply-to queues federate-all \
  '^(backup|restore|chat|storage\.toggle)\..*' \
  '{"federation-upstream-set":"all"}'
```

Symmetrically configure Azure side pointing at DC RMQ.

### B.6 VPN site-to-site

**B.6.1 Azure VPN Gateway**

```bash
# Azure CLI
az network vnet-gateway create \
  --resource-group tmvault-infra \
  --name tmvault-vpn-gw \
  --public-ip-addresses tmvault-vpn-pip \
  --vnet tmvault-vnet \
  --gateway-type Vpn --vpn-type RouteBased \
  --sku VpnGw2 \
  --asn 65515
```

**B.6.2 DC side**

Configure your edge router:
- Phase 1: IKEv2, AES-256, SHA-256, DH group 14
- Phase 2: AES-256, SHA-256, PFS group 14, lifetime 3600s
- BGP neighbor: Azure VPN GW public IP, ASN 65515

Set up two tunnels to two Azure VPN GW instances for redundancy.

**B.6.3 Verify**

```bash
# From DC worker node
ping -c 4 pg.azure.tmvault.example  # Azure PG primary
nc -zv s3.tmvault.internal 443       # DC SeaweedFS (sanity — should work locally)
```

### B.7 DNS setup

Internal zone `tmvault.internal`:

```
api.tmvault.internal       CNAME  current-active-api-ingress   (TTL 30s)
pg.tmvault.internal        CNAME  current-primary-pg           (TTL 30s)
amqp.tmvault.internal      CNAME  current-active-rmq           (TTL 30s)
s3.tmvault.internal        A      <DC SeaweedFS ingress>       (TTL 300s — never flips)
```

DNS flip script `ops/dns/flip.sh <azure|dc>`:
```bash
#!/bin/bash
set -e
TARGET=$1
if [ "$TARGET" = "dc" ]; then
  IP_API=<dc-ingress>
  IP_PG=<dc-pg>
  IP_RMQ=<dc-rmq>
elif [ "$TARGET" = "azure" ]; then
  IP_API=<azure-ingress>
  IP_PG=<azure-pg>
  IP_RMQ=<azure-rmq>
else
  echo "Usage: $0 azure|dc"; exit 1
fi

# (Use your DNS provider's CLI/API — example with AWS Route53)
aws route53 change-resource-record-sets --hosted-zone-id $ZONE_ID --change-batch "{...}"

echo "DNS flipped to $TARGET"
```

### B.8 TMvault application deployment to DC

**B.8.1 Build + push images**

```bash
cd tm_backend
./scripts/build-all.sh     # builds all worker + service images
./scripts/push-to-registry.sh <client-internal-registry>
```

**B.8.2 Deploy via Helm**

```bash
helm install tmvault ./charts/tmvault \
  --namespace tmvault \
  --create-namespace \
  -f charts/tmvault/values-onprem.yaml \
  --set image.registry=<client-internal-registry> \
  --set activeZone=onprem \
  --set onprem.s3.endpoint=https://s3.tmvault.internal \
  --set onprem.s3.bucketsCsv="tmvault-shard-0,tmvault-shard-1,...,tmvault-shard-7" \
  --set onprem.s3.secretName=seaweedfs-credentials \
  --set onprem.s3.verifyTls=true \
  --set onprem.s3.caBundleSecret=internal-ca
```

**B.8.3 Run Alembic migration**

```bash
kubectl -n tmvault exec deploy/api-gateway -- alembic upgrade head
```

**B.8.4 Seed `storage_backends` + `system_config`**

```sql
-- Azure backend row already seeded by migration.
-- Insert SeaweedFS backend row:
INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config)
VALUES (
  'seaweedfs',
  'onprem-dc1',
  'https://s3.tmvault.internal',
  'vault://tmvault/seaweedfs/secret',
  '{"buckets":["tmvault-shard-0","tmvault-shard-1","tmvault-shard-2","tmvault-shard-3","tmvault-shard-4","tmvault-shard-5","tmvault-shard-6","tmvault-shard-7"],"region":"us-east-1","verify_tls":true,"ca_bundle":"/etc/tmvault/ca.pem"}'
);
```

### B.9 Preflight verification

Before running the first toggle, verify all green:

```bash
# DC worker → DC SeaweedFS
curl -s https://s3.tmvault.internal/ -o /dev/null -w "%{http_code}\n"  # expect 403 (auth required but reachable)

# Test bucket write
aws s3api --endpoint-url https://s3.tmvault.internal put-object \
  --bucket tmvault-shard-0 --key preflight-test.txt --body <(echo ok)
aws s3api --endpoint-url https://s3.tmvault.internal get-object \
  --bucket tmvault-shard-0 --key preflight-test.txt /tmp/out
diff /tmp/out <(echo ok)

# DB replica lag
psql "<dc-pg-url>" -c "SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) AS lag_seconds;"
# Expect < 10

# DC workers healthy
kubectl -n tmvault get pods -l app=backup-worker
kubectl -n tmvault get pods -l app=restore-worker

# RMQ federation
kubectl -n tmvault exec tmvault-rmq-0 -- rabbitmqctl federation-status
```

### B.10 First toggle (prod)

1. Schedule maintenance window (1-2 hours, though actual downtime is 5-15 min)
2. Notify users of brief API unavailability during flip
3. Login to TMvault UI as org-admin → Settings → Storage
4. Verify all preflight green
5. Click toggle → enter reason "Initial on-prem cutover" → type org name → submit
6. Monitor SSE stream through all 8 phases
7. Smoke test automatically runs in phase 7
8. On green → users resume work on DC side
9. Watch observability for 24-48h: backup latency, restore latency, replica lag, WORM delete attempts
10. Optional: toggle back to Azure after 24h to validate reversibility, then flip forward again for go-live

### B.11 Ongoing ops

**Daily**
- Check Grafana "Storage Overview" + "Cross-zone VPN health" dashboards
- Monitor `tmvault_toggle_replica_lag_seconds` — alert if sustained > 30s
- Verify nightly retention cleanup completes without errors

**Weekly**
- Check SeaweedFS cluster health (`cluster.check`)
- Check `pg_stat_replication` lag trends
- Review `storage_toggle_events` for any failed or aborted attempts

**Monthly**
- Disk health check across volume servers (`smartctl` + SeaweedFS health endpoints)
- Rotate S3 credentials (update `storage_backends.secret_ref` target in vault)
- Review WORM audit events (`WORMDeleteAttempted` log)

**Quarterly**
- Full DR drill: toggle to Azure, observe for 1h, toggle back
- Disaster scenario test: kill a volume server, verify cluster continues serving + self-heals
- Backup of SeaweedFS filer Postgres DB (metadata catalog)

### B.12 Scaling up

When adding volume servers:

```bash
kubectl -n seaweedfs scale statefulset volume --replicas=12   # from 8 to 12
# New volumes auto-join the cluster; EC re-balance over next N hours
```

When adding shard buckets:

```bash
# Create bucket (steps B.3.3)
# Update storage_backends.config JSONB:
UPDATE storage_backends
  SET config = jsonb_set(config, '{buckets}',
    config->'buckets' || '["tmvault-shard-8","tmvault-shard-9"]'::jsonb)
  WHERE name = 'onprem-dc1';
# Workers pick up new shards on next router reload (within 30s via LISTEN)
```

### B.13 Rollback / DR

If DC goes fully dark:

```bash
# From laptop or jumpbox with Azure access
# 1. Flip DNS back to Azure
ops/dns/flip.sh azure

# 2. Promote Azure replica back to primary (if it was demoted)
psql "<azure-pg-url>" -c "SELECT pg_promote();"

# 3. Update system_config
psql "<azure-pg-url>" -c "
  UPDATE system_config
  SET active_backend_id = (SELECT id FROM storage_backends WHERE name='azure-primary'),
      transition_state = 'stable'
  WHERE id = 1;
"

# 4. Scale Azure workers back up
kubectl --context azure -n tmvault scale deploy/backup-worker --replicas=8

# 5. Users continue working on Azure. All post-toggle on-prem snapshots remain in DC (unreachable until DC restored), but the system is functional.
```

RPO: minutes (limited by replica lag). RTO: 15-30 min.

### B.14 Handoff checklist

Client ops team should have:
- [ ] Root credentials for SeaweedFS S3 identities (in their vault)
- [ ] Postgres superuser access
- [ ] Kubernetes cluster admin kubeconfig
- [ ] VPN pre-shared keys / certificates
- [ ] Runbook: `ops/runbooks/storage-toggle.md`
- [ ] Runbook: `ops/runbooks/dr-failover.md`
- [ ] Runbook: `ops/runbooks/add-shard-bucket.md`
- [ ] Monitoring dashboards exported (Grafana JSON)
- [ ] Alert contacts configured in PagerDuty/OpsGenie
- [ ] Training session on toggle UI + `mc` CLI for SeaweedFS

---

**End of design.**