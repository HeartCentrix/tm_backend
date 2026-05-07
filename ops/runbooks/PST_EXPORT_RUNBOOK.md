# PST Export — Operations Runbook

Production runbook for PST export operations on TMvault. Covers normal
operations, observability, troubleshooting, and disaster recovery.

---

## 0. PST writer (post-Aspose)

PST files are produced by the bundled `pst_convert` CLI built from
`vendor/pstwriter` (the C++17 source vendored in this repo). The
restore-worker Docker image ships the binary at
`/usr/local/bin/pst_convert`; the Python wrapper lives in
`shared/pstwriter_cli.py`.

- Refresh the vendored sources from upstream `PST Dev`: see
  `vendor/pstwriter/README.md`.
- Override the binary path for ad-hoc dev (e.g. point at a Windows
  build): set `PSTWRITER_CLI=/abs/path/to/pst_convert(.exe)`.
- No license required. The Aspose.Email dependency was removed; the
  `shared.aspose_license` module is now a deprecation no-op.

---

## 1. Architecture overview

```
job-service ──┐
              │ /api/v1/jobs/restore (EXPORT_PST)
              ↓
       create_restore_message
              │
              │ pick_export_queue(spec.totalBytes)
              ↓
   ┌──────────────────────────────┐
   │ RabbitMQ                      │
   │   restore.normal  (≤20 GB)    │
   │   restore.heavy   (>20 GB)    │
   └──────┬─────────────┬──────────┘
          ↓             ↓
   restore-worker   restore-worker-heavy
   (12×, 8GB)       (4×, 32GB)
          │             │
          └─────┬───────┘
                ↓
        PstExportOrchestrator
                ↓
   stream items → per-group PST → upload to blob → final ZIP stream
                ↓
          exports/ container
                ↓
   user downloads via /api/v1/jobs/export/{id}/download
```

---

## 2. Daily operations

### Health checks (every 5 min)

```bash
# Worker pool status
docker compose ps | grep -E "backup-worker|restore-worker"

# RabbitMQ queue depths
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged | \
    grep -E "restore|backup"

# Active PST jobs (Redis counter)
docker compose exec redis redis-cli --scan --pattern "pst:active:*" | head

# Worker memory (Prometheus)
curl -s http://prometheus:9090/api/v1/query?query=pst_export_worker_rss_mb
```

### Healthy KPIs

| Metric | Healthy | Alert at |
|---|---|---|
| `restore.normal` queue depth | <100 | >500 sustained |
| `restore.heavy` queue depth | <20 | >50 sustained |
| Worker RSS (8GB pods) | <6GB | >7GB |
| Worker RSS (32GB pods) | <24GB | >28GB |
| `pst_export_jobs_total{status="upload_failed"}` | 0/min | >1/min |
| Job p95 duration (small) | <300s | >900s |
| Job p95 duration (whale) | <3600s | >7200s |

---

## 3. Scaling operations

### Scale worker pool up

```bash
# Add 5 more normal restore workers
docker compose up -d --scale restore-worker=17

# Add a heavy worker
docker compose up -d --scale restore-worker-heavy=5
```

### Adjust per-user concurrency (no restart needed for new env)

```bash
# Update .env, then restart workers
echo "PST_RATE_LIMIT_PER_KEY=5" >> .env
docker compose restart restore-worker restore-worker-heavy
```

### Lower the heavy threshold (route more jobs to heavy pool)

```bash
# Whale = anything > 10 GB instead of 20 GB
echo "HEAVY_EXPORT_THRESHOLD_BYTES=10737418240" >> .env
docker compose restart job-service
```

---

## 4. Troubleshooting playbook

### Symptom: PST jobs all failing with `upload_failed`

**Diagnose:**
```bash
docker compose logs restore-worker --since=10m | grep -E "upload|stream_zip"
```

**Common causes:**
- **Azure Storage quota / network** — check `AZURE_STORAGE_*` env vars
- **Wrong blob container** — verify `dest_container` in logs matches what exists
- **`pst_convert` binary missing** — see logs for `pst_convert binary not found`
  or `[pstwriter_cli] FAILED rc=…`. Recover by rebuilding the worker image
  (`docker compose build restore-worker`) so the multi-stage build re-emits
  `/usr/local/bin/pst_convert` from `vendor/pstwriter`.

**Fix:**
```bash
# Test blob upload from worker
docker compose exec restore-worker python3 -c "
from shared.azure_storage import azure_storage_manager
shard = azure_storage_manager.get_default_shard()
import asyncio
async def t():
    r = await shard.upload_blob('exports', 'test.txt', b'hello')
    print(r)
asyncio.run(t())
"
```

### Symptom: Jobs queueing but not processing (queue depth growing)

**Diagnose:**
```bash
# Are workers connected to RabbitMQ?
docker compose exec rabbitmq rabbitmqctl list_consumers | grep restore

# Are workers in healthy state?
docker compose ps | grep restore
```

**Common causes:**
- **All workers stuck on whale jobs** (single-pod with `MAX_CONCURRENT_EXPORTS_PER_WORKER=1`)
- **RabbitMQ connection lost** — `MESSAGE_BUS Connection attempt N failed`
- **DB connection pool exhausted** — `OperationalError: too many connections`

**Fix (whale starvation):**
```bash
# Scale heavy pool
docker compose up -d --scale restore-worker-heavy=8
```

**Fix (DB pool):**
```bash
# Increase Postgres connection limit
docker compose exec postgres psql -U postgres -c "ALTER SYSTEM SET max_connections=400;"
docker compose restart postgres
```

### Symptom: Worker OOM-kills (k8s sees `OOMKilled`)

**Diagnose:**
```bash
# Check what tipped them over
docker compose logs restore-worker | grep -E "memory pressure|MemoryError|OOM"
```

**Common causes:**
- **Whale mailbox in normal pool** (didn't route to heavy) — check if `spec.totalBytes` populates
- **Single huge attachment** — `PST_MAX_ATTACHMENT_BYTES` too high
- **Many concurrent jobs on same pod** — `PST_WRITE_CONCURRENCY` > 1

**Fix:**
```bash
# Tighten attachment cap
echo "PST_MAX_ATTACHMENT_BYTES=52428800" >> .env  # 50MB
# Bump heavy pod RAM
echo "RESTORE_HEAVY_MEM_LIMIT=64G" >> .env
docker compose up -d --force-recreate restore-worker restore-worker-heavy
```

### Symptom: Job stuck at "Preparing…" in UI for hours

**Diagnose:**
```bash
# Find the job
docker compose exec postgres psql -U postgres -d railway -c \
    "SELECT id, status, progress_pct, created_at, result FROM tm.jobs \
     WHERE type='RESTORE' AND status='RUNNING' ORDER BY created_at DESC LIMIT 5;"
```

**Common causes:**
- **RabbitMQ redelivered + processing** — check log for two workers on same job_id
- **DB session deadlock on progress callback** — fixed in `5fcd01d`, ensure deployed
- **Hung `pst_convert` invocation** — `PSTWRITER_CLI_TIMEOUT_S` (default 1h
  per chunk) and `PST_GROUP_TIMEOUT_S` (default 4h per group) cap runaways

**Fix:**
```bash
# Manually cancel
curl -X POST http://localhost:8004/api/v1/jobs/{job_id}/cancel \
    -H "Authorization: Bearer $TOKEN"

# Or set status directly
docker compose exec postgres psql -U postgres -d railway -c \
    "UPDATE tm.jobs SET status='CANCELLED' WHERE id='{job_id}';"
```

---

## 5. Disaster recovery

### DR-1: One worker pod is unrecoverable

**Action:** No-op. Other replicas + checkpoint resume cover it.

```bash
docker compose stop tm_backend-restore-worker-3
# RabbitMQ redelivers any in-flight messages to other replicas.
# Each replica resumes from the job's checkpoint (Job.result.pst_checkpoint).
docker compose start tm_backend-restore-worker-3
```

### DR-2: Entire RabbitMQ broker is gone

**Impact:** all in-flight messages lost; all queued jobs lost.

**Recovery:**
```bash
# 1. Bring up a fresh RabbitMQ
docker compose up -d rabbitmq

# 2. Republish queued jobs from DB
docker compose exec postgres psql -U postgres -d railway -c "
SELECT id, type, spec FROM tm.jobs
WHERE status='QUEUED' AND created_at > NOW() - INTERVAL '24 hours';
"

# 3. For each, POST to /api/v1/jobs/restore again with same spec.
# Workers detect existing checkpoint and resume.
```

### DR-3: PostgreSQL primary lost

**If using HA profile** (`docker compose --profile ha up -d`):
```bash
# Promote replica
docker compose exec postgres-replica pg_ctl promote -D /var/lib/postgresql/data
# Update DATABASE_URL to point at replica
sed -i.bak 's|@postgres:|@postgres-replica:|' .env
docker compose restart restore-worker restore-worker-heavy job-service
```

**If not in HA:** restore from latest pg_dump backup. Jobs that were in-flight
get marked FAILED on restart; users re-trigger.

### DR-4: Blob storage region outage

**For Azure:** GRS replication should automatically fail over. Verify the
account has `Standard_GRS` redundancy. The `dr-replication-worker` service
also keeps a hot copy in a paired region.

**For SeaweedFS:** rely on SeaweedFS replication policy (`-replication=001`
minimum for production).

### DR-5: All exports in `/tmp` lost (pod restart)

**Impact:** The currently-flushing PST is lost; previously-uploaded PSTs in
blob storage are intact.

**Recovery:** automatic. The orchestrator's checkpoint includes uploaded
blob paths; on resume, completed groups are skipped, only the in-flight
group reprocesses.

---

## 6. Capacity planning for 250 TiB / 5K users

| Component | Size |
|---|---|
| restore-worker pool | 12 replicas × 8 GB = 96 GB RAM |
| restore-worker-heavy | 4 replicas × 32 GB = 128 GB RAM |
| backup-worker pool | 20 replicas × 8 GB = 160 GB RAM |
| backup-worker-heavy | 5 replicas × 32 GB = 160 GB RAM |
| Postgres | 16 GB RAM, 500 GB SSD, max_connections=300 |
| Redis | 4 GB RAM (rate-limit counters + job state) |
| RabbitMQ | 4 GB RAM, 50 GB disk (queue overflow) |
| Blob storage | 250 TiB + ~10% overhead for export staging |

**Network:** workers need ~100 Mbps each peak (Graph API + blob).
Total egress for worker pool: ~4 Gbps peak.

---

## 7. Observability

### Prometheus metrics (all exposed on `:9100/metrics` per worker)

```
pst_export_jobs_total{status, granularity}           # counter
pst_export_items_total{item_type}                    # counter
pst_export_items_failed_total{item_type, reason}     # counter
pst_export_attachments_skipped_total                 # counter
pst_export_job_duration_seconds{granularity, status} # histogram
pst_export_group_duration_seconds{item_type}         # histogram
pst_export_worker_rss_mb                             # gauge
pst_export_active_jobs{scope, scope_key}             # gauge
```

### Bring up Prometheus + Grafana

```bash
docker compose --profile observability up -d
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3001  (admin/admin or GRAFANA_PASSWORD)
```

### Suggested alerts

```yaml
- alert: PstExportFailureSpike
  expr: rate(pst_export_jobs_total{status="upload_failed"}[5m]) > 0.1
  for: 5m
  annotations:
    summary: PST export failures > 0.1/sec for 5 minutes

- alert: PstWorkerMemoryPressure
  expr: pst_export_worker_rss_mb > 7000
  for: 2m
  annotations:
    summary: Worker RSS approaching 8GB pod limit

- alert: PstQueueBacklog
  expr: rabbitmq_queue_messages{queue="restore.normal"} > 500
  for: 10m
  annotations:
    summary: PST queue backing up — scale workers
```

---

## 8. Load testing

```bash
# Run before production launch + after capacity changes
WHALE_SNAPSHOT_ID=$(uuid) SMALL_SNAPSHOT_ID=$(uuid) \
  python3 scripts/load_test_pst_export.py \
    --whales 5 --small 100 --concurrent 20

# Output: per-job duration p50/p95/max, status histogram
```

Acceptance criteria for production rollout:
- 100 concurrent small jobs: p95 < 5 minutes, 0 failures
- 5 whale (100 GB) + 50 small concurrent: p95 small < 10 min, whales complete < 4h, 0 failures
- Worker RSS p99 < 6 GB for normal pool, < 28 GB for heavy
