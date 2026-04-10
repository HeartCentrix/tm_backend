# TM Vault Mass Backup Architecture - Microservices Implementation

## Executive Summary

This document outlines the **microservices-based mass backup architecture** for TM Vault, designed to handle enterprise-scale Microsoft 365/Azure backups (10k-100k+ resources) with improved efficiency, reliability, and Microsoft Graph API compliance.

---

## 1. Architecture Overview

### 1.1 Service Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Gateway (8080)                       │
│                 Routes requests to microservices                │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
┌──────────────────┐          ┌──────────────────┐
│  Auth Service    │          │  Tenant Service  │
│  (OAuth2/JWT)    │          │  (CRUD/Discover) │
└──────────────────┘          └──────────────────┘
          
┌──────────────────┐          ┌──────────────────┐
│ Resource Service │          │   Job Service    │
│ (Resources/SLA)  │          │ (Backup/Restore) │
└──────────────────┘          └──────────────────┘
                                         │
                    ┌────────────────────┘
                    │ (Publishes to RabbitMQ)
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      RabbitMQ Message Bus                       │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐  │
│  │backup.urgent│ │backup.high │ │backup.norm │ │ backup.low │  │
│  │  (prio 1)  │ │  (prio 2)  │ │  (prio 5)  │ │  (prio 8)  │  │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
┌──────────────────────┐      ┌──────────────────────┐
│  Backup Scheduler    │      │   Backup Worker(s)   │
│  (Port 8008)         │      │   (Scalable pool)    │
│  - SLA-based cron    │      │   - Batch processing │
│  - Resource grouping │      │   - Graph $batch     │
│  - Staggered dispatch│      │   - Dedup + Encrypt  │
└──────────────────────┘      └──────────┬───────────┘
                                          │
                           ┌──────────────┴──────────────┐
                           │                             │
                  ┌──────────────────┐        ┌──────────────────┐
                  │  Graph Proxy     │        │  Delta Token     │
                  │  (Port 8009)     │        │  (Port 8010)     │
                  │  - $batch API    │        │  - Token cache   │
                  │  - Throttling    │        │  - Per-folder    │
                  │  - Token cache   │        │  - History       │
                  └──────────────────┘        └──────────────────┘
                           │
                  ┌────────┴────────┐
                  │                 │
          ┌──────────────┐   ┌──────────────┐
          │  MS Graph    │   │ Azure Blob   │
          │  API         │   │  Storage     │
          └──────────────┘   └──────────────┘
```

---

## 2. New Microservices

### 2.1 Backup Scheduler Service (Port 8008)

**Purpose**: SLA-based backup scheduling with mass dispatch

**Key Features**:
- **Cron-based scheduling**: Gold (3x/day), Silver (1x/day), Bronze (Mon-Fri)
- **Resource grouping**: Groups by type + tenant for batch processing
- **Staggered dispatch**: Prevents Graph API throttling with delays
- **SLA violation tracking**: Alerts on missed backup windows

**Endpoints**:
```
GET  /health
POST /scheduler/trigger/{sla_tier}     # Manual SLA trigger
POST /scheduler/resource/{resource_id} # Single resource trigger
```

**Scheduling Algorithm**:
```python
# Gold SLA: 02:00, 10:00, 18:00 UTC
scheduler.add_job(dispatch_gold_backups, 'cron', hour='2,10,18')

# Silver SLA: 02:00 UTC
scheduler.add_job(dispatch_silver_backups, 'cron', hour=2)

# Bronze SLA: 04:00 UTC (Mon-Fri)
scheduler.add_job(dispatch_bronze_backups, 'cron', hour=4, day_of_week='mon-fri')

# SLA violation check: Every 30 minutes
scheduler.add_job(check_sla_violations, 'interval', minutes=30)
```

**Mass Dispatch Flow**:
1. Fetch all active resources for SLA tier
2. Group by `resource_type:tenant_id`
3. Split into batches of 1000 resources
4. Create job record per batch
5. Publish to RabbitMQ with staggered delays (100ms between batches)

---

### 2.2 Backup Worker (Scalable Pool)

**Purpose**: Execute backup jobs from RabbitMQ queues

**Key Features**:
- **Batch processing**: Handles up to 1000 resources per job
- **Graph $batch support**: 20 requests per batch call
- **Adaptive concurrency**: Semaphore-limited (50 concurrent by default)
- **Content deduplication**: SHA-256 hashing
- **AES-256-GCM encryption**: Per-tenant encryption
- **Versioned storage**: `{tenant}/{resource}/{snapshot}/{timestamp}/{item}`

**Queue Topology**:
| Queue | Priority | Use Case | Prefetch |
|-------|----------|----------|----------|
| `backup.urgent` | 1 | Manual/preemptive | 10 |
| `backup.high` | 2 | Gold SLA | 20 |
| `backup.normal` | 5 | Silver SLA | 50 |
| `backup.low` | 8 | Bronze SLA | 100 |

**Processing Flow**:
```
1. Dequeue message
2. Claim job (status=RUNNING)
3. Fetch tenant + resources
4. Get Graph client (token from cache)
5. Create snapshot record
6. Execute backup (route by resource type)
   a. Single resource: Direct Graph API calls
   b. Mass backup: Batch via $batch endpoint
7. For each item:
   - Calculate SHA-256 hash
   - Check dedup store
   - Encrypt content
   - Upload to Azure Blob
   - Create snapshot_item record
8. Update delta token
9. Complete snapshot
10. Update job (status=COMPLETED)
11. ACK message
```

**Scaling**:
```yaml
deploy:
  replicas: ${BACKUP_WORKER_REPLICAS:-3}  # Scale horizontally
```

---

### 2.3 Graph Proxy Service (Port 8009)

**Purpose**: Microsoft Graph API gateway with batching and throttling

**Key Features**:
- **$batch endpoint support**: Up to 20 requests per batch
- **Adaptive throttling**: Prevents 429 errors
- **Token caching**: Redis-backed with TTL
- **Automatic retry**: With exponential backoff

**Throttling Rules**:
```python
MAX_REQUESTS_PER_5MIN = 10000  # Per tenant
MAX_CONCURRENT = 20            # Per tenant

# Backoff on 429 response
if response.status_code == 429:
    retry_after = int(headers.get("Retry-After", "60"))
    record_throttle(tenant_id, retry_after)
    wait_and_retry(retry_after)
```

**Endpoints**:
```
POST /graph/batch?tenant_id={id}        # Batch request
GET  /graph/{path}?tenant_id={id}       # Proxy GET
POST /graph/{path}?tenant_id={id}       # Proxy POST
GET  /throttle/stats/{tenant_id}        # Throttle metrics
GET  /throttle/stats                    # All tenants
```

**Batch Request Example**:
```json
{
  "requests": [
    {"id": "1", "method": "GET", "url": "/users/user1/messages/delta"},
    {"id": "2", "method": "GET", "url": "/users/user2/messages/delta"},
    {"id": "3", "method": "GET", "url": "/users/user3/messages/delta"}
  ]
}
```

---

### 2.4 Delta Token Service (Port 8010)

**Purpose**: Centralized delta token management for incremental backups

**Key Features**:
- **Per-folder tokens**: Exchange mailbox (folder-level delta)
- **Per-resource tokens**: OneDrive, SharePoint, Entra ID
- **Redis caching**: Fast token lookup (30-day TTL)
- **Token history**: Track delta token changes
- **Bulk lookup**: Get tokens for multiple resources

**Storage Locations**:
1. **Redis cache**: `delta_token:{resource_id}:{folder_id}` (fastest)
2. **Snapshot record**: `snapshots.delta_token` (persistent)
3. **Tenant JSONB**: `tenants.graph_delta_tokens` (fallback)

**Endpoints**:
```
GET    /delta-token/{resource_id}              # Get token
POST   /delta-token                            # Save token
DELETE /delta-token/{resource_id}              # Invalidate token
GET    /delta-token/history/{resource_id}      # Get history
GET    /delta-tokens/bulk?resource_ids=...     # Bulk lookup
```

**Per-Folder Token Example**:
```json
{
  "messages_user123": {
    "inbox": "https://graph.microsoft.com/v1.0/users/.../deltaLink=ABC123",
    "sent": "https://graph.microsoft.com/v1.0/users/.../deltaLink=DEF456",
    "archive": "https://graph.microsoft.com/v1.0/users/.../deltaLink=GHI789"
  }
}
```

---

## 3. Data Model Changes

### 3.1 SLA Policy Enhancements

```python
class SlaPolicy(Base):
    tier = Column(String)  # GOLD | SILVER | BRONZE | MANUAL | CUSTOM
    backup_window_end = Column(String)  # NEW: end of backup window
    resource_types = Column(ARRAY(String))  # NEW: explicit types
    batch_size = Column(Integer, default=20)  # NEW: Graph $batch size
    max_concurrent_backups = Column(Integer, default=50)  # NEW
    sla_violation_alert = Column(Boolean, default=True)  # NEW
```

### 3.2 Job Enhancements

```python
class Job(Base):
    batch_resource_ids = Column(ARRAY(UUID))  # NEW: mass backup
```

### 3.3 Snapshot Enhancements

```python
class Snapshot(Base):
    delta_tokens_json = Column(JSON)  # NEW: per-folder tokens
    content_checksum = Column(String)  # NEW: SHA-256 of blob
    blob_path = Column(String)  # NEW: Azure Blob path
    storage_version = Column(Integer, default=1)  # NEW
```

### 3.4 SnapshotItem Enhancements

```python
class SnapshotItem(Base):
    content_checksum = Column(String)  # NEW: integrity check
    blob_path = Column(String)  # NEW: Azure Blob path
    encryption_key_id = Column(String)  # NEW: DEK version
    backup_version = Column(Integer, default=1)  # NEW
```

---

## 4. Storage Architecture

### 4.1 Versioned Path Structure

```
{org_id}/
  {tenant_id}/
    {resource_type}/        # MAILBOX, ONEDRIVE, etc.
      {resource_id}/
        {snapshot_id}/
          {timestamp}/
            {item_id}.json          # Metadata
            {item_id}.content       # Encrypted content
            {item_id}.checksum.sha  # SHA-256 integrity
```

### 4.2 Example Path

```
org-abc123/
  tenant-def456/
    MAILBOX/
      user-789/
        snapshot-xyz/
          2026-04-10T02:00:00/
            msg-001.json
            msg-001.content
            msg-001.checksum.sha
```

---

## 5. Message Formats

### 5.1 Mass Backup Message

```json
{
  "jobId": "uuid",
  "tenantId": "external-tenant-id",
  "resourceType": "MAILBOX",
  "resourceIds": ["uuid1", "uuid2", "..."],
  "type": "INCREMENTAL",
  "priority": 5,
  "slaTier": "SILVER",
  "triggeredBy": "SCHEDULED",
  "snapshotLabel": "scheduled",
  "forceFullBackup": false,
  "createdAt": "2026-04-10T02:00:00",
  "batchSize": 1000
}
```

### 5.2 Single Backup Message

```json
{
  "jobId": "uuid",
  "resourceId": "uuid",
  "tenantId": "external-tenant-id",
  "type": "INCREMENTAL",
  "priority": 1,
  "triggeredBy": "MANUAL",
  "snapshotLabel": "manual",
  "forceFullBackup": true
}
```

---

## 6. SLA Violation Detection

### 6.1 Violation Check Logic

```python
frequency_hours = {
    "THREE_DAILY": 8,   # Every 8 hours
    "DAILY": 24,
    "WEEKLY": 168,
}

hours_since_backup = (now - resource.last_backup_at).total_seconds() / 3600

if hours_since_backup > frequency_hours:
    severity = "CRITICAL" if hours_since_backup > frequency_hours * 2 else "WARNING"
    emit_violation_alert(resource, severity, hours_since_backup)
```

### 6.2 Alert Payload

```json
{
  "resource_id": "uuid",
  "tenant_id": "uuid",
  "resource_type": "MAILBOX",
  "sla_tier": "SILVER",
  "last_backup_at": "2026-04-09T02:00:00",
  "hours_overdue": 26.5,
  "severity": "WARNING"
}
```

---

## 7. Deployment Configuration

### 7.1 Environment Variables

```bash
# Mass Backup
BACKUP_WORKER_REPLICAS=3

# RabbitMQ
RABBITMQ_ENABLED=true
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

# Redis
REDIS_ENABLED=true
REDIS_HOST=redis
REDIS_PORT=6379

# Azure Storage
AZURE_STORAGE_ACCOUNT_NAME=tmvaultbackup
AZURE_STORAGE_ACCOUNT_KEY=<key>

# Microsoft Graph
AZURE_AD_CLIENT_ID=<client-id>
AZURE_AD_CLIENT_SECRET=<client-secret>
AZURE_AD_TENANT_ID=<tenant-id>
```

### 7.2 Docker Compose Services

| Service | Port | Purpose |
|---------|------|---------|
| `backup-scheduler` | 8008 | SLA scheduling |
| `backup-worker` | - | Backup execution (scaled to 3 replicas) |
| `graph-proxy` | 8009 | Graph API gateway |
| `delta-token` | 8010 | Delta token management |

---

## 8. Performance Targets

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Resources/hour | ~5,000 | ~50,000 | **10x** |
| Graph API calls/resource | 5-10 | 1-3 | **3-5x fewer** |
| Throttle rate | 2-5% | <0.5% | **4-10x reduction** |
| Max concurrent backups | 120 | 500 | **4x** |
| SLA compliance | 85% | 99% | **14% improvement** |

---

## 9. Scaling Strategy

### 9.1 Horizontal Scaling

```bash
# Scale backup workers
docker compose up -d --scale backup-worker=10

# In production (Kubernetes):
kubectl scale deployment backup-worker --replicas=20
```

### 9.2 Queue Prefetch Tuning

```python
# Adjust based on worker capacity
backup.urgent:  prefetch=10   # Low - manual/preemptive
backup.high:    prefetch=20   # Medium - Gold SLA
backup.normal:  prefetch=50   # High - Silver SLA
backup.low:     prefetch=100  # Very high - Bronze (less urgent)
```

### 9.3 Graph API Optimization

- **Batch requests**: 20 calls → 1 HTTP request
- **Connection pooling**: Reuse HTTP connections
- **Parallel processing**: Async/await for I/O
- **Token caching**: Redis-backed (avoids repeated auth)

---

## 10. Monitoring & Observability

### 10.1 Key Metrics

```python
# Per-tenant throttle stats
GET /throttle/stats/{tenant_id}

# Job progress (SSE streaming)
GET /jobs/{job_id}/status

# SLA violations
GET /alerts?type=SLA_VIOLATION&severity=CRITICAL
```

### 10.2 Health Checks

```bash
curl http://localhost:8008/health  # Scheduler
curl http://localhost:8009/health  # Graph Proxy
curl http://localhost:8010/health  # Delta Token
```

---

## 11. Migration Guide

### 11.1 Database Migration

```sql
-- Add new columns to sla_policies
ALTER TABLE sla_policies ADD COLUMN tier VARCHAR DEFAULT 'CUSTOM';
ALTER TABLE sla_policies ADD COLUMN backup_window_end VARCHAR;
ALTER TABLE sla_policies ADD COLUMN resource_types TEXT[] DEFAULT '{}';
ALTER TABLE sla_policies ADD COLUMN batch_size INTEGER DEFAULT 20;
ALTER TABLE sla_policies ADD COLUMN max_concurrent_backups INTEGER DEFAULT 50;
ALTER TABLE sla_policies ADD COLUMN sla_violation_alert BOOLEAN DEFAULT TRUE;

-- Add new columns to jobs
ALTER TABLE jobs ADD COLUMN batch_resource_ids UUID[] DEFAULT '{}';

-- Add new columns to snapshots
ALTER TABLE snapshots ADD COLUMN delta_tokens_json JSONB DEFAULT '{}';
ALTER TABLE snapshots ADD COLUMN content_checksum VARCHAR;
ALTER TABLE snapshots ADD COLUMN blob_path VARCHAR;
ALTER TABLE snapshots ADD COLUMN storage_version INTEGER DEFAULT 1;

-- Add new columns to snapshot_items
ALTER TABLE snapshot_items ADD COLUMN content_checksum VARCHAR;
ALTER TABLE snapshot_items ADD COLUMN blob_path VARCHAR;
ALTER TABLE snapshot_items ADD COLUMN encryption_key_id VARCHAR;
ALTER TABLE snapshot_items ADD COLUMN backup_version INTEGER DEFAULT 1;
```

### 11.2 Backward Compatibility

- **Old single-resource backups**: Still work via `backup.urgent` queue
- **New mass backups**: Use `backup.high/normal/low` queues
- **Delta tokens**: Fallback to existing `snapshots.delta_token` if Redis unavailable
- **Storage paths**: Old paths still readable, new backups use versioned paths

---

## 12. Troubleshooting

### 12.1 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| 429 Throttling | Too many concurrent requests | Reduce worker replicas or increase prefetch |
| Delta token missing | Redis unavailable | Fallback to database lookup |
| Job stuck in RUNNING | Worker crashed | Check worker logs, retry job |
| Storage upload failed | Azure credentials invalid | Verify `AZURE_STORAGE_ACCOUNT_KEY` |

### 12.2 Debug Commands

```bash
# Check worker logs
docker logs tm_vault_backup_worker

# View queue depths
rabbitmqctl list_queues name messages

# Check throttle stats
curl http://localhost:8009/throttle/stats

# View delta token
curl http://localhost:8010/delta-token/{resource_id}
```

---

## 13. Future Enhancements

1. **Change Notifications (Webhooks)**: Near real-time backup triggers
2. **Cross-region replication**: Backup to multiple Azure regions
3. **Incremental restore**: Restore individual items from batch backups
4. **AI-powered anomaly detection**: Predict backup failures
5. **Compression**: Gzip large JSON payloads before encryption
6. **BYOK integration**: Azure Key Vault for customer-managed keys
7. **Multi-tenant isolation**: Dedicated worker pools per tenant

---

## 14. Conclusion

This microservices-based mass backup architecture transforms TM Vault into an **enterprise-grade backup solution** capable of handling 50k+ resources with:

- **10x throughput** via batch processing and horizontal scaling
- **3-5x fewer Graph API calls** via `$batch` endpoint usage
- **99% SLA compliance** via predictive throttling and violation tracking
- **Immutable, versioned storage** with integrity checksums
- **Production reliability** with comprehensive monitoring and alerting

The implementation is **backward compatible** and can be deployed incrementally alongside existing services.
