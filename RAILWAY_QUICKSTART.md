# Railway Quick Start - TM Vault Backend

## What Changed

### Files Modified:
- `shared/config.py` - Now reads Railway's `DATABASE_URL`, `REDIS_URL`, `RABBITMQ_URL` env vars
- All `Dockerfile`s (17) - Now use `$PORT` env var instead of hardcoded ports

### Files Created:
- `init_db.py` - Database initialization script (creates enum types + tables)
- `.env.railway` - Environment variable template
- `railway-*.toml` (17) - Railway config files for each service
- `RAILWAY_DEPLOYMENT.md` - Full deployment guide

## 3-Step Deploy on Railway

### Step 1: Add Infrastructure
In your Railway project, add:
1. **PostgreSQL** (Database → PostgreSQL)
2. **Redis** (Data → Upstash Redis)
3. **CloudAMQP** (Data → CloudAMQP)

### Step 2: Deploy Services
For each of the 17 services:
1. **New** → **GitHub Repo** → Select `tm_backend`
2. Set Dockerfile path in service settings (see table below)
3. Link to PostgreSQL, Redis, and CloudAMQP via **References** tab
4. Add required env vars (see Step 3)
5. Deploy

| Service | Dockerfile Path |
|---------|----------------|
| api-gateway | `api-gateway/Dockerfile` |
| auth-service | `services/auth-service/Dockerfile` |
| tenant-service | `services/tenant-service/Dockerfile` |
| resource-service | `services/resource-service/Dockerfile` |
| job-service | `services/job-service/Dockerfile` |
| snapshot-service | `services/snapshot-service/Dockerfile` |
| dashboard-service | `services/dashboard-service/Dockerfile` |
| alert-service | `services/alert-service/Dockerfile` |
| backup-scheduler | `services/backup-scheduler/Dockerfile` |
| graph-proxy | `services/graph-proxy/Dockerfile` |
| delta-token | `services/delta-token/Dockerfile` |
| progress-tracker | `services/progress-tracker/Dockerfile` |
| audit-service | `services/audit-service/Dockerfile` |
| search-service | `services/search-service/Dockerfile` |
| discovery-worker | `workers/discovery-worker/Dockerfile` |
| backup-worker | `workers/backup-worker/Dockerfile` |
| restore-worker | `workers/restore-worker/Dockerfile` |

### Step 3: Set Env Variables

**On ALL services**, Railway auto-injects:
- `DATABASE_URL` (from PostgreSQL)
- `REDIS_URL` (from Upstash Redis)
- `RABBITMQ_URL` (from CloudAMQP)
- `PORT` (Railway assigns dynamically)

**On services using RabbitMQ** (job-service, backup-scheduler, backup-worker, restore-worker, progress-tracker, discovery-worker):
```
RABBITMQ_ENABLED=true
```

**On services using Redis** (graph-proxy, delta-token, backup-scheduler, backup-worker):
```
REDIS_ENABLED=true
```

**On ALL services**:
```
JWT_SECRET=<generate-random-string>
CORS_ALLOWED_ORIGINS=https://your-frontend.com
```

**On backup-scheduler, backup-worker, restore-worker, graph-proxy**:
```
APP_1_CLIENT_ID=<azure-id>
APP_1_CLIENT_SECRET=<azure-secret>
APP_1_TENANT_ID=<tenant-id>
AZURE_STORAGE_ACCOUNT_NAME=<storage-account>
AZURE_STORAGE_ACCOUNT_KEY=<storage-key>
AZURE_STORAGE_BLOB_ENDPOINT=https://<account>.blob.core.windows.net
```

### Step 4: Initialize Database (One-Time)
After PostgreSQL is deployed and any service is running:

1. Open Railway shell on any service:
   ```bash
   python init_db.py
   ```

2. Verify tables exist:
   ```bash
   # From PostgreSQL service console
   \dt
   ```

### Step 5: Expose API Gateway
1. Open `api-gateway` service → Settings → Networking
2. Enable **Public Networking**
3. Copy the public URL - this is your backend entry point

## Scale Workers
- **backup-worker** → Replicas: `3`
- **restore-worker** → Replicas: `2`
- **discovery-worker** → Replicas: `1`

## Verify Deployment
```bash
# Check health of any service
curl https://<service>.up.railway.app/health

# Expected: {"status": "ok"} or HTTP 200
```

## Full Details
See `RAILWAY_DEPLOYMENT.md` for complete guide with troubleshooting.
