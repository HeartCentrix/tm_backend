# TM Vault - Environment Configuration Reference

Complete reference for all configurable environment variables. Set these in Railway's dashboard, `.env` file, or Docker Compose.

---

## 🔐 Authentication

| Variable | Description | Default | Example |
|---|---|---|---|
| `JWT_SECRET` | Secret key for signing JWT tokens. Generate with: `openssl rand -base64 64` | *(required)* | `K7mN9pQ2xR...` |
| `JWT_ALGORITHM` | Algorithm used for JWT signing | `HS256` | `HS256` |
| `JWT_EXPIRATION_HOURS` | Hours until access token expires | `8` | `12` |
| `JWT_REFRESH_EXPIRATION_DAYS` | Days until refresh token expires | `7` | `14` |

---

## 🗄️ Database

| Variable | Description | Default | Example |
|---|---|---|---|
| `DATABASE_URL` | Railway auto-provided. Overrides individual DB vars if present | *(auto)* | `postgresql+asyncpg://...` |
| `DB_HOST` | PostgreSQL host address | `localhost` | `postgres.railway.internal` |
| `DB_PORT` | PostgreSQL port | `5432` | `5432` |
| `DB_NAME` | Database name | *(required)* | `tm_vault_db` |
| `DB_USERNAME` | Database user | `postgres` | `postgres` |
| `DB_PASSWORD` | Database password | *(required)* | `super-secret` |
| `DB_SCHEMA` | PostgreSQL schema to use | `public` | `tm_vault` |

---

## 🐇 RabbitMQ

| Variable | Description | Default | Example |
|---|---|---|---|
| `RABBITMQ_URL` | Railway auto-provided. Overrides individual vars if present | *(auto)* | `amqp://user:pass@host/` |
| `RABBITMQ_HOST` | RabbitMQ host address | `localhost` | `rabbitmq.railway.internal` |
| `RABBITMQ_PORT` | RabbitMQ port | `5672` | `5672` |
| `RABBITMQ_USER` | RabbitMQ username | `guest` | `tmvault` |
| `RABBITMQ_PASSWORD` | RabbitMQ password | `guest` | `super-secret` |
| `RABBITMQ_ENABLED` | Enable RabbitMQ integration | `false` | `true` |

---

## 🔴 Redis

| Variable | Description | Default | Example |
|---|---|---|---|
| `REDIS_URL` | Railway auto-provided. Overrides individual vars if present | *(auto)* | `redis://host:6379/0` |
| `REDIS_HOST` | Redis host address | `localhost` | `redis.railway.internal` |
| `REDIS_PORT` | Redis port | `6379` | `6379` |
| `REDIS_DB` | Redis database index | `0` | `1` |
| `REDIS_ENABLED` | Enable Redis caching | `false` | `true` |

---

## ☁️ Azure Storage (Primary)

| Variable | Description | Default | Example |
|---|---|---|---|
| `AZURE_STORAGE_ACCOUNT_NAME` | Primary Azure Storage Account name | *(required)* | `tmvaultbackup` |
| `AZURE_STORAGE_ACCOUNT_KEY` | Primary Azure Storage Account access key | *(required)* | `abcdef123456...` |
| `AZURE_STORAGE_BLOB_ENDPOINT` | Azure Blob service endpoint | `https://blob.core.windows.net` | `https://tmvault.blob.core.windows.net` |

---

## 🚀 High-Performance Backup Configuration

| Variable | Description | Default | Range | When to Change |
|---|---|---|---|---|
| `BACKUP_CONCURRENCY` | Max concurrent resource backups (per worker) | `50` | `10-200` | Increase for large scale. Decrease if Graph API throttles you. |
| `COPY_CONCURRENCY` | Max concurrent Server-Side Copy operations | `100` | `50-500` | Can be high — Azure copies internally, zero server load. |
| `WORKLOAD_CONCURRENCY` | How many workload types run simultaneously (Exchange, OneDrive, SharePoint, etc.) | `5` | `1-10` | Leave at 5 for full parallelism. |
| `SERVER_SIDE_COPY_THRESHOLD` | File size (bytes) above which Server-Side Copy is used | `10485760` (10MB) | `1MB-50MB` | Lower = more files use Server-Side Copy. Higher = more files use Python SDK. |
| `MAX_RETRIES` | Max retry attempts for failed backup operations | `5` | `3-10` | Increase for unreliable networks. |
| `RETRY_DELAY_MS` | Initial delay between retries (milliseconds) | `2000` (2s) | `500-10000` | Lower = faster retries. Higher = more patience. |
| `RETRY_BACKOFF_MULTIPLIER` | Multiplier for exponential backoff | `2.0` | `1.5-3.0` | Higher = slower backoff curve. |
| `GRAPH_BATCH_SIZE` | Graph API $batch endpoint batch size | `20` | `10-20` | Graph API max is 20. Don't exceed. |
| `RESOURCE_CHUNK_SIZE` | Number of resources processed per chunk | `50` | `10-200` | Lower = less memory. Higher = faster processing. |

---

## 💾 Storage Sharding (Multi-Account Setup)

Use this to distribute backup load across multiple Azure Storage Accounts. Prevents IOPS bottlenecks at scale.

| Variable | Description | Default | Example |
|---|---|---|---|
| `STORAGE_SHARD_COUNT` | Number of storage accounts to use | `1` | `5` |
| `STORAGE_SHARD_ACCOUNTS` | Comma-separated list of storage account names | *(empty)* | `backup1,backup2,backup3,backup4,backup5` |
| `STORAGE_SHARD_KEYS` | Comma-separated list of matching storage account keys | *(empty)* | `key1,key2,key3,key4,key5` |

### How Sharding Works
- Each tenant + resource ID is **deterministically hashed** to pick a storage account.
- Same resource always lands on the same account (consistent placement).
- Resources are **evenly distributed** across all configured accounts.

### Single Account Example
```env
STORAGE_SHARD_COUNT=1
AZURE_STORAGE_ACCOUNT_NAME=mybackupaccount
AZURE_STORAGE_ACCOUNT_KEY=my-key-here
# Leave STORAGE_SHARD_ACCOUNTS and STORAGE_SHARD_KEYS empty
```

### Multi-Account Example (10 Accounts)
```env
STORAGE_SHARD_COUNT=10
STORAGE_SHARD_ACCOUNTS=backup1,backup2,backup3,backup4,backup5,backup6,backup7,backup8,backup9,backup10
STORAGE_SHARD_KEYS=key1,key2,key3,key4,key5,key6,key7,key8,key9,key10
# Leave AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY empty (or set one as fallback)
```

---

## 📧 Microsoft Graph API (App Registrations)

Supports up to **10 app registrations** for distributing Graph API load.

| Variable | Description | Default | Example |
|---|---|---|---|
| `APP_{N}_CLIENT_ID` | Client ID for App N (N = 1-10) | *(required for N=1)* | `3139d955-...` |
| `APP_{N}_CLIENT_SECRET` | Client Secret for App N | *(required for N=1)* | `FB28Q~...` |
| `APP_{N}_TENANT_ID` | Tenant ID for App N | `common` | `d8a8bb9e-...` |
| `AZURE_AD_CLIENT_ID` | Legacy fallback for single-app mode | *(empty)* | `3139d955-...` |
| `AZURE_AD_CLIENT_SECRET` | Legacy fallback secret | *(empty)* | `N3V8Q~...` |
| `AZURE_AD_TENANT_ID` | Legacy fallback tenant ID | `common` | `d8a8bb9e-...` |

### Multi-App Setup Example
```env
# App 1 (Primary)
APP_1_CLIENT_ID=your-client-id-1
APP_1_CLIENT_SECRET=your-secret-1
APP_1_TENANT_ID=your-tenant-id

# App 2 (Secondary)
APP_2_CLIENT_ID=your-client-id-2
APP_2_CLIENT_SECRET=your-secret-2
APP_2_TENANT_ID=your-tenant-id

# App 3 (Tertiary)
APP_3_CLIENT_ID=your-client-id-3
APP_3_CLIENT_SECRET=your-secret-3
APP_3_TENANT_ID=your-tenant-id
```

> **Note:** Microsoft enforces **collective throttling** for backup apps in the same tenant (as of March 2026). Multiple apps help distribute load but won't bypass tenant-level limits.

---

## 🔗 Microservice URLs (Railway Private Networking)

Used by the API Gateway to route requests to backend services.

| Variable | Description | Default (Local) | Default (Railway) |
|---|---|---|---|
| `AUTH_SERVICE_URL` | Auth Service URL | `http://auth-service:8001` | `http://authservice.railway.internal:8080` |
| `TENANT_SERVICE_URL` | Tenant Service URL | `http://tenant-service:8002` | `http://tenantservice.railway.internal:8080` |
| `RESOURCE_SERVICE_URL` | Resource Service URL | `http://resource-service:8003` | `http://resourceservice.railway.internal:8080` |
| `JOB_SERVICE_URL` | Job Service URL | `http://job-service:8004` | `http://jobservice.railway.internal:8080` |
| `SNAPSHOT_SERVICE_URL` | Snapshot Service URL | `http://snapshot-service:8005` | `http://snapshotservice.railway.internal:8080` |
| `DASHBOARD_SERVICE_URL` | Dashboard Service URL | `http://dashboard-service:8006` | `http://dashboardservice.railway.internal:8080` |
| `ALERT_SERVICE_URL` | Alert Service URL | `http://alert-service:8007` | `http://alertservice.railway.internal:8080` |
| `BACKUP_SCHEDULER_URL` | Backup Scheduler URL | `http://backup-scheduler:8008` | `http://backupscheduler.railway.internal:8080` |
| `GRAPH_PROXY_URL` | Graph Proxy URL | `http://graph-proxy:8009` | `http://graphproxy.railway.internal:8080` |
| `DELTA_TOKEN_URL` | Delta Token Service URL | `http://delta-token:8010` | `http://deltatoken.railway.internal:8080` |
| `PROGRESS_TRACKER_URL` | Progress Tracker URL | `http://progress-tracker:8011` | `http://progresstracker.railway.internal:8080` |
| `AUDIT_SERVICE_URL` | Audit Service URL | `http://audit-service:8012` | `http://auditservice.railway.internal:8080` |

---

## 🌐 CORS

| Variable | Description | Default | Example |
|---|---|---|---|
| `CORS_ALLOWED_ORIGINS` | Comma-separated list of allowed frontend origins | `http://localhost:4200,http://localhost:3000,http://localhost:5173` | `https://tm-vault.vercel.app,http://localhost:4200` |

---

## 🧪 Feature Flags

| Variable | Description | Default | Values |
|---|---|---|---|
| `FEATURE_ELASTICSEARCH_ENABLED` | Enable full-text search via Elasticsearch | `false` | `true` / `false` |
| `FEATURE_AI_PROTECTION_ENABLED` | Enable AI-powered anomaly detection | `false` | `true` / `false` |
| `FEATURE_SELF_SERVICE_ENABLED` | Enable self-service restore for end users | `false` | `true` / `false` |
| `FEATURE_RETENTION_ENFORCEMENT_ENABLED` | Enable automatic retention policy enforcement | `false` | `true` / `false` |

---

## 📝 Logging

| Variable | Description | Default | Values |
|---|---|---|---|
| `LOG_LEVEL` | Logging verbosity level | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` |

---

## 🏗️ Recommended Configurations by Scale

### Small (100-500 Users, < 5TB)
```env
BACKUP_CONCURRENCY=50
COPY_CONCURRENCY=100
WORKLOAD_CONCURRENCY=5
STORAGE_SHARD_COUNT=1
SERVER_SIDE_COPY_THRESHOLD=10485760
MAX_RETRIES=5
APP_1_CLIENT_ID=...
APP_1_CLIENT_SECRET=...
APP_1_TENANT_ID=...
```

### Medium (500-5,000 Users, 5-100TB)
```env
BACKUP_CONCURRENCY=75
COPY_CONCURRENCY=150
WORKLOAD_CONCURRENCY=5
STORAGE_SHARD_COUNT=3
STORAGE_SHARD_ACCOUNTS=backup1,backup2,backup3
STORAGE_SHARD_KEYS=key1,key2,key3
SERVER_SIDE_COPY_THRESHOLD=10485760
MAX_RETRIES=5
APP_1_CLIENT_ID=...
APP_1_CLIENT_SECRET=...
APP_2_CLIENT_ID=...
APP_2_CLIENT_SECRET=...
APP_3_CLIENT_ID=...
APP_3_CLIENT_SECRET=...
```

### Large (5,000-10,000+ Users, 100-400TB+)
```env
BACKUP_CONCURRENCY=100
COPY_CONCURRENCY=200
WORKLOAD_CONCURRENCY=5
STORAGE_SHARD_COUNT=10
STORAGE_SHARD_ACCOUNTS=backup1,backup2,...,backup10
STORAGE_SHARD_KEYS=key1,key2,...,key10
SERVER_SIDE_COPY_THRESHOLD=5242880
MAX_RETRIES=7
APP_1_CLIENT_ID=... (up to APP_10_CLIENT_ID=...)
```

---

## ⚠️ Important Notes

1. **Never commit secrets** — Use Railway's environment variables or `.env` (which is in `.gitignore`).
2. **Storage sharding is additive only** — Adding accounts distributes *new* backups. Existing data stays on its original account.
3. **Microsoft throttling is tenant-level** — Multiple app registrations distribute load but don't bypass tenant egress limits.
4. **Server-Side Copy requires download URLs** — Only works for OneDrive/SharePoint files. Emails and metadata always use Python SDK upload.
5. **Changes require redeploy** — Environment variable changes take effect after the service is redeployed on Railway.
