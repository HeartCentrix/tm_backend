# Railway Deployment Guide

## Architecture

TM Vault backend deploys as **17 separate Railway services** sharing a common PostgreSQL, Redis, and RabbitMQ instance.

```
Infrastructure:
├── PostgreSQL (Railway managed)
├── Redis (Upstash Redis on Railway)
└── RabbitMQ (CloudAMQP on Railway)

Services (17):
├── api-gateway          (port 8000) → Public entry point
├── auth-service         (port 8001)
├── tenant-service       (port 8002)
├── resource-service     (port 8003)
├── job-service          (port 8004)
├── snapshot-service     (port 8005)
├── dashboard-service    (port 8006)
├── alert-service        (port 8007)
├── backup-scheduler     (port 8008)
├── graph-proxy          (port 8009)
├── delta-token          (port 8010)
├── progress-tracker     (port 8011)
├── audit-service        (port 8012)
├── search-service       (port 8013)
├── discovery-worker     (no port, background)
├── backup-worker        (no port, background, scale to 3)
└── restore-worker       (no port, background, scale to 2)
```

## Step 1: Prepare Railway Project

1. Go to [Railway](https://railway.app) and create a new project
2. Connect your GitHub repo (`tm_backend`)
3. In Railway dashboard, add these **infrastructure services**:
   - **PostgreSQL** → Click "New" → "Database" → "Add PostgreSQL"
   - **Redis** → Click "New" → "Data" → "Upstash Redis" (or any Redis)
   - **RabbitMQ** → Click "New" → "Data" → "CloudAMQP" (or any RabbitMQ addon)

## Step 2: Deploy Services

For each service, follow these steps:

### 2a. Add Service from GitHub Repo

1. Click **"New"** → **"GitHub Repo"**
2. Select your `tm_backend` repository
3. Railway will auto-detect the Dockerfile

### 2b. Set the Dockerfile Path

In the service settings, set the **Dockerfile Context** to point to the right file:

| Service Name            | Dockerfile Path                        |
|-------------------------|----------------------------------------|
| `api-gateway`           | `api-gateway/Dockerfile`               |
| `auth-service`          | `services/auth-service/Dockerfile`     |
| `tenant-service`        | `services/tenant-service/Dockerfile`   |
| `resource-service`      | `services/resource-service/Dockerfile` |
| `job-service`           | `services/job-service/Dockerfile`      |
| `snapshot-service`      | `services/snapshot-service/Dockerfile` |
| `dashboard-service`     | `services/dashboard-service/Dockerfile`|
| `alert-service`         | `services/alert-service/Dockerfile`    |
| `backup-scheduler`      | `services/backup-scheduler/Dockerfile` |
| `graph-proxy`           | `services/graph-proxy/Dockerfile`      |
| `delta-token`           | `services/delta-token/Dockerfile`      |
| `progress-tracker`      | `services/progress-tracker/Dockerfile` |
| `audit-service`         | `services/audit-service/Dockerfile`    |
| `search-service`        | `services/search-service/Dockerfile`   |
| `discovery-worker`      | `workers/discovery-worker/Dockerfile`  |
| `backup-worker`         | `workers/backup-worker/Dockerfile`     |
| `restore-worker`        | `workers/restore-worker/Dockerfile`    |

> **Alternative:** Use the provided `railway-*.toml` files. Rename the one you need to `railway.toml` before deploying each service.

### 2c. Configure Environment Variables

In each service's **Variables** tab, add:

**Required for ALL services:**
```
JWT_SECRET=<your-secure-random-string>
CORS_ALLOWED_ORIGINS=https://your-frontend-domain.com
```

**Required for services that use RabbitMQ:**
```
RABBITMQ_ENABLED=true
```
(Railway auto-injects `RABBITMQ_URL` from your CloudAMQP service)

**Required for services that use Redis:**
```
REDIS_ENABLED=true
```
(Railway auto-injects `REDIS_URL` from your Upstash Redis service)

**Required for backup-scheduler, backup-worker, restore-worker, graph-proxy:**
```
APP_1_CLIENT_ID=<azure-app-1-client-id>
APP_1_CLIENT_SECRET=<azure-app-1-client-secret>
APP_1_TENANT_ID=<your-tenant-id>
APP_2_CLIENT_ID=<azure-app-2-client-id>
APP_2_CLIENT_SECRET=<azure-app-2-client-secret>
APP_3_CLIENT_ID=<azure-app-3-client-id>
APP_3_CLIENT_SECRET=<azure-app-3-client-secret>
AZURE_STORAGE_ACCOUNT_NAME=<your-azure-storage-account>
AZURE_STORAGE_ACCOUNT_KEY=<your-azure-storage-key>
AZURE_STORAGE_BLOB_ENDPOINT=https://<account>.blob.core.windows.net
```

> Railway auto-injects `DATABASE_URL` from the PostgreSQL service. The app parses it automatically.

## Step 3: Link Services to Infrastructure

In Railway, each service needs access to PostgreSQL, Redis, and RabbitMQ:

1. Open each service → **References** tab
2. Click **"Add Reference"**
3. Select your **PostgreSQL**, **Redis**, and **RabbitMQ** services
4. Railway will auto-inject the connection URLs as env vars:
   - `DATABASE_URL` (PostgreSQL)
   - `REDIS_URL` (Redis)
   - `RABBITMQ_URL` (RabbitMQ/CloudAMQP)

## Step 4: Initialize Database (First Deploy Only)

After deploying PostgreSQL and at least one service:

1. Open any deployed service's shell:
   - Go to service → **Settings** → **Shell**
   - Or use Railway CLI: `railway shell`

2. Run the init script:
   ```bash
   cd /app
   python init_db.py
   ```

   This creates:
   - All enum types (tenantstatus, jobstatus, etc.)
   - All tables (tenants, resources, jobs, etc.)
   - Converts any existing VARCHAR columns to proper enum types

3. Verify tables exist:
   ```bash
   # From Railway's PostgreSQL service shell or pgAdmin
   \dt
   ```

## Step 5: Scale Workers

For background workers:

1. **backup-worker** → Set replicas to **3**:
   - Service → Settings → **Replicas** → `3`

2. **restore-worker** → Set replicas to **2**:
   - Service → Settings → **Replicas** → `2`

3. **discovery-worker** → Keep at **1** replica

## Step 6: Expose API Gateway

1. Open `api-gateway` service
2. Go to **Settings** → **Networking**
3. Enable **Public Networking** (Railway provides a public URL)
4. All other services should be **private** (internal only)

## Step 7: Configure CORS

In `api-gateway` service env vars:
```
CORS_ALLOWED_ORIGINS=https://your-frontend-domain.com,http://localhost:4200
```

## Step 8: Health Checks

All services have health check endpoints. Railway auto-detects them from Dockerfiles.

To verify a service is healthy:
```
curl https://<service-name>.up.railway.app/health
```

Expected: `{"status": "ok"}` or HTTP 200

## Environment Variables Reference

### Railway Auto-Injected (DO NOT set manually):
| Variable | Source | Description |
|----------|--------|-------------|
| `DATABASE_URL` | PostgreSQL service | `postgresql://user:pass@host:port/dbname` |
| `REDIS_URL` | Redis service | `redis://host:port` |
| `RABBITMQ_URL` | CloudAMQP service | `amqp://user:pass@host:port/vhost` |
| `PORT` | Railway | Port the service should listen on |

### You Must Set:
| Variable | Services | Description |
|----------|----------|-------------|
| `JWT_SECRET` | ALL | Secret key for JWT token signing |
| `CORS_ALLOWED_ORIGINS` | api-gateway, auth-service | Comma-separated frontend URLs |
| `APP_*_CLIENT_ID` | backup-scheduler, backup-worker, restore-worker, graph-proxy | Azure AD app registrations |
| `APP_*_CLIENT_SECRET` | Same as above | Azure AD app secrets |
| `APP_*_TENANT_ID` | Same as above | Azure tenant ID |
| `AZURE_STORAGE_ACCOUNT_NAME` | backup-worker, restore-worker | Azure Blob storage account |
| `AZURE_STORAGE_ACCOUNT_KEY` | backup-worker, restore-worker | Azure Blob storage key |
| `RABBITMQ_ENABLED` | job-service, backup-scheduler, backup-worker, restore-worker, progress-tracker, discovery-worker | Set to `true` |
| `REDIS_ENABLED` | graph-proxy, delta-token, backup-scheduler, backup-worker | Set to `true` |

## Service Dependencies (Deploy Order)

1. **Infrastructure first**: PostgreSQL, Redis, RabbitMQ
2. **Initialize DB**: Run `init_db.py`
3. **Core services**: auth-service → tenant-service → resource-service → job-service → snapshot-service
4. **Supporting services**: dashboard-service, alert-service, search-service, audit-service
5. **Workers**: discovery-worker, backup-worker, restore-worker
6. **Advanced**: backup-scheduler, graph-proxy, delta-token, progress-tracker
7. **Gateway**: api-gateway (last, depends on auth-service)

## Troubleshooting

### Service won't start
- Check logs in Railway dashboard
- Verify env vars are set correctly
- Ensure `DATABASE_URL`, `REDIS_URL`, `RABBITMQ_URL` are linked

### Database errors
- Run `python init_db.py` to create tables
- Check enum types exist: `\dT+` in psql
- Verify connection: `echo $DATABASE_URL`

### RabbitMQ connection refused
- Ensure CloudAMQP/RabbitMQ service is running
- Verify `RABBITMQ_URL` is set (Railway auto-injects it)
- Check `RABBITMQ_ENABLED=true` in service env vars

### Redis connection refused
- Ensure Redis service (Upstash) is running
- Verify `REDIS_URL` is set
- Check `REDIS_ENABLED=true` in service env vars

### Worker not processing jobs
- Check RabbitMQ connection in logs
- Verify queue bindings in Railway's RabbitMQ dashboard
- Scale replicas: backup-worker → 3, restore-worker → 2

## Railway CLI (Optional)

Install Railway CLI for easier management:
```bash
npm i -g @railway/cli
railway login
railway link  # Link to your project
railway shell  # Open shell in any service
railway logs   # View logs
railway up     # Deploy
```

## Cost Estimate (Railway Hobby Plan)

- **PostgreSQL**: $5/month (512MB RAM)
- **Upstash Redis**: Free tier available
- **CloudAMQP**: Free tier available (Lemur plan)
- **17 services**: Each uses ~128MB RAM minimum = ~2GB total
- **Total estimate**: ~$10-20/month depending on usage

For production, upgrade to Railway Pro and scale services individually.
