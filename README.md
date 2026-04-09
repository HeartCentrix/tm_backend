# TM Vault Backend - Microservices Architecture

Python FastAPI backend built with a microservices architecture for the TM Vault M365/Azure backup platform.

## Architecture

```
┌─────────────────────────────────────────────┐
│              API Gateway (8000)             │
└──────────────┬──────────────────────────────┘
               │ Routes requests to:
    ┌──────────┼──────────┬─────────┬──────────┐
    │          │          │         │          │
┌───▼───┐ ┌──▼───┐ ┌───▼───┐ ┌──▼───┐ ┌──▼───┐
│ Auth  │ │Tenant│ │Resource│ │ Job  │ │Snapshot│
│ :8001 │ │:8002 │ │ :8003 │ │:8004 │ │ :8005 │
└───────┘ └──────┘ └───────┘ └──────┘ └───────┘
                                             │
    ┌────────────────────────────────────────┘
    │
┌───▼────────┐  ┌────────────┐
│ Dashboard  │  │  Alert     │
│  :8006     │  │  :8007     │
└────────────┘  └────────────┘
```

### Services

| Service | Port | Responsibility |
|---------|------|----------------|
| **API Gateway** | 8000 | Request routing, CORS, health checks |
| **Auth Service** | 8001 | OAuth2, JWT, user management |
| **Tenant Service** | 8002 | Tenants, organizations, discovery |
| **Resource Service** | 8003 | Resources, SLA policies |
| **Job Service** | 8004 | Jobs, backup triggers, restore, export |
| **Snapshot Service** | 8005 | Snapshots, snapshot items browsing |
| **Dashboard Service** | 8006 | Metrics, aggregated statistics |
| **Alert Service** | 8007 | Alerts, notifications, access groups |

### Infrastructure

| Component | Purpose |
|-----------|---------|
| **PostgreSQL 16** | Shared database for all services |
| **RabbitMQ** | Async job queue (backup, restore, discovery) |
| **Redis** | Caching (optional) |

## Quick Start

### Docker Compose (Recommended)

```bash
cd tm_backend

# Copy environment file
cp .env.example .env

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f api-gateway

# Stop
docker-compose down
```

### Manual Development

```bash
# Start PostgreSQL first
# Then run each service:
cd services/auth-service
pip install -r requirements.txt
PYTHONPATH=../.. uvicorn main:app --reload --port 8001
```

## API Endpoints

All endpoints are accessible through the API Gateway at `http://localhost:8000/api/v1/`

### Authentication
- `GET /auth/microsoft/url` - Get Microsoft OAuth2 URL
- `POST /auth/callback` - Exchange code for tokens
- `POST /auth/refresh` - Refresh access token
- `POST /auth/logout` - Logout
- `GET /auth/me` - Get current user

### Dashboard
- `GET /dashboard/overview` - Overview metrics
- `GET /dashboard/status/24hour` - 24h backup status
- `GET /dashboard/status/7day` - 7-day trends
- `GET /dashboard/protection/status` - Protection coverage
- `GET /dashboard/backup/size` - Storage consumption

### Tenants
- `GET /tenants` - List tenants
- `POST /tenants` - Create tenant
- `PUT /tenants/{id}` - Update tenant
- `DELETE /tenants/{id}` - Delete tenant
- `POST /tenants/{id}/discover-m365` - Trigger M365 discovery
- `POST /tenants/{id}/discover-azure` - Trigger Azure discovery

### Resources
- `GET /resources` - List resources (paginated)
- `GET /resources/users` - Get users with workloads
- `POST /resources/{id}/assign-policy` - Assign SLA
- `DELETE /resources/{id}` - Delete resource

### Jobs
- `GET /jobs` - List jobs
- `GET /jobs/{id}/progress` - SSE progress stream
- `POST /jobs/{id}/cancel` - Cancel job
- `POST /backups/trigger` - Trigger backup

### Snapshots
- `GET /resources/{id}/snapshots` - List snapshots
- `GET /resources/snapshots/{id}/items` - List items

### SLA Policies
- `GET /policies` - List policies
- `POST /policies` - Create policy
- `PUT /policies/{id}` - Update policy
- `DELETE /policies/{id}` - Delete policy

### Alerts
- `GET /alerts` - List alerts
- `POST /alerts/{id}/resolve` - Resolve alert

## Configuration

Copy `.env.example` to `.env` and configure:

```env
# Database
DB_HOST=postgres
DB_PORT=5432
DB_NAME=tm_vault_db
DB_USERNAME=tm_vault_admin
DB_PASSWORD=admin123

# JWT
JWT_SECRET=your-secret-key

# Microsoft OAuth
MICROSOFT_CLIENT_ID=your-client-id
MICROSOFT_CLIENT_SECRET=your-client-secret
MICROSOFT_TENANT_ID=common

# RabbitMQ (optional)
RABBITMQ_ENABLED=false

# Redis (optional)
REDIS_ENABLED=false
```

## Project Structure

```
tm_backend/
├── shared/                    # Shared library
│   ├── config.py             # Configuration
│   ├── database.py           # Database connection
│   ├── models.py             # SQLAlchemy models
│   ├── schemas.py            # Pydantic schemas
│   ├── security.py           # Auth utilities
│   └── message_bus.py        # RabbitMQ client
│
├── api-gateway/              # API Gateway
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
├── services/
│   ├── auth-service/         # Authentication
│   ├── tenant-service/       # Tenant management
│   ├── resource-service/     # Resources & SLA
│   ├── job-service/          # Jobs & triggers
│   ├── snapshot-service/     # Snapshots
│   ├── dashboard-service/    # Metrics
│   └── alert-service/        # Alerts & access
│
├── workers/
│   └── backup-worker/        # Async backup processor
│
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## Inter-Service Communication

- **Synchronous**: HTTP calls via API Gateway routing
- **Asynchronous**: RabbitMQ for job processing
  - `backup.urgent` / `backup.normal` - Backup jobs
  - `restore.urgent` - Restore jobs
  - `discovery.m365` / `discovery.azure` - Discovery jobs
  - `notification` - Email/webhook alerts
  - `delete.low` - Delayed deletion

## Scaling

Each service runs independently and can be scaled horizontally:

```bash
# Scale a specific service
docker-compose up -d --scale resource-service=3
```

## Development

### Adding a New Service

1. Create directory: `services/new-service/`
2. Add `main.py`, `Dockerfile`, `requirements.txt`
3. Import shared models: `from shared.models import ...`
4. Add to `docker-compose.yml`
5. Add routes to `api-gateway/main.py`

### Health Checks

Each service exposes `/health` endpoint:
```bash
curl http://localhost:8001/health  # Auth
curl http://localhost:8002/health  # Tenant
curl http://localhost:8000/health  # Gateway
```

## Frontend Integration

Frontend connects to API Gateway at `http://localhost:8000/api/v1/`

Update frontend environment:
```typescript
// environment.ts
export const environment = {
  apiUrl: 'http://localhost:8000/api/v1'
};
```
