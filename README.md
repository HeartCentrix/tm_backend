# TM Vault Backend - Python FastAPI

Simplified Python backend for the TM Vault application, built with FastAPI.

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Copy and configure environment
cp .env.example .env

# Start backend + PostgreSQL
docker-compose up -d

# View logs
docker-compose logs -f backend

# Stop
docker-compose down
```

### Option 2: Local Development

#### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your configuration values
```

### 3. Run the Server

```bash
uvicorn main:app --reload --port 8000
```

The API will be available at `http://localhost:8000/api/v1`

## API Documentation

Once running, view interactive API docs at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Project Structure

```
tm_backend/
├── main.py                 # FastAPI application entry point
├── requirements.txt        # Python dependencies
├── .env.example           # Environment configuration template
├── app/
│   ├── __init__.py
│   ├── config.py          # Application settings
│   ├── schemas.py         # Pydantic request/response models
│   ├── security.py        # Authentication & authorization
│   ├── db/
│   │   ├── __init__.py
│   │   ├── database.py    # Database connection
│   │   └── models.py      # SQLAlchemy ORM models
│   └── api/
│       ├── __init__.py
│       ├── routes.py      # Route aggregation
│       ├── auth.py        # Authentication endpoints
│       ├── dashboard.py   # Dashboard metrics
│       ├── tenants.py     # Tenant management
│       ├── resources.py   # Resource management
│       ├── jobs.py        # Job management
│       ├── snapshots.py   # Snapshot browsing
│       ├── restore.py     # Restore/Export
│       ├── alerts.py      # Alerts & notifications
│       ├── policies.py    # SLA policies
│       └── access_control.py  # Access groups
```

## Configuration

### Required Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | localhost |
| `DB_PORT` | PostgreSQL port | 5432 |
| `DB_NAME` | Database name | tm_vault_db |
| `DB_USERNAME` | Database user | tm_vault_admin |
| `DB_PASSWORD` | Database password | admin123 |
| `JWT_SECRET` | JWT signing secret | (change in production!) |
| `MICROSOFT_CLIENT_ID` | Azure AD app client ID | |
| `MICROSOFT_CLIENT_SECRET` | Azure AD app client secret | |
| `MICROSOFT_TENANT_ID` | Azure AD tenant ID | common |

### Optional Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SERVER_PORT` | HTTP port | 8000 |
| `CORS_ORIGINS` | Allowed CORS origins | ["http://localhost:4200"] |
| `REDIS_ENABLED` | Enable Redis caching | false |
| `RABBITMQ_ENABLED` | Enable RabbitMQ | false |
| `ELASTICSEARCH_ENABLED` | Enable Elasticsearch | false |

## API Endpoints

### Authentication
- `GET /api/v1/auth/microsoft/url` - Get Microsoft OAuth2 login URL
- `POST /api/v1/auth/callback` - Exchange OAuth code for tokens
- `POST /api/v1/auth/refresh` - Refresh access token
- `POST /api/v1/auth/logout` - Logout
- `GET /api/v1/auth/me` - Get current user profile

### Dashboard
- `GET /api/v1/dashboard/overview` - Dashboard overview metrics
- `GET /api/v1/dashboard/status/24hour` - 24h backup status
- `GET /api/v1/dashboard/status/7day` - 7-day backup trends
- `GET /api/v1/dashboard/protection/status` - Protection coverage
- `GET /api/v1/dashboard/backup/size` - Storage consumption

### Tenants
- `GET /api/v1/tenants` - List tenants
- `POST /api/v1/tenants` - Create tenant
- `PUT /api/v1/tenants/{id}` - Update tenant
- `DELETE /api/v1/tenants/{id}` - Delete tenant
- `POST /api/v1/tenants/{id}/discover-m365` - Trigger M365 discovery
- `POST /api/v1/tenants/{id}/discover-azure` - Trigger Azure discovery

### Resources
- `GET /api/v1/resources` - List resources (paginated)
- `GET /api/v1/resources/{id}` - Get resource details
- `GET /api/v1/resources/users` - Get users with workloads
- `POST /api/v1/resources/{id}/assign-policy` - Assign SLA policy
- `DELETE /api/v1/resources/{id}` - Delete resource

### Jobs
- `GET /api/v1/jobs` - List jobs (paginated)
- `GET /api/v1/jobs/{id}/progress` - SSE job progress stream
- `POST /api/v1/jobs/{id}/cancel` - Cancel job
- `POST /api/v1/backups/trigger` - Trigger backup

### Snapshots
- `GET /api/v1/resources/{id}/snapshots` - List snapshots
- `GET /api/v1/resources/snapshots/{id}/items` - List snapshot items

### SLA Policies
- `GET /api/v1/policies` - List policies
- `POST /api/v1/policies` - Create policy
- `PUT /api/v1/policies/{id}` - Update policy
- `DELETE /api/v1/policies/{id}` - Delete policy

## Database

The application uses PostgreSQL with SQLAlchemy ORM. Tables are auto-created on startup.

### Seed Data

To seed initial data (run in psql):

```sql
-- Create default organization
INSERT INTO organizations (id, name, slug, created_at, updated_at)
VALUES ('00000000-0000-0000-0000-000000000001', 'Taylor Morrison', 'taylor-morrison', NOW(), NOW());

-- Create sample tenant
INSERT INTO tenants (id, org_id, type, display_name, status, created_at, updated_at)
VALUES (gen_random_uuid(), '00000000-0000-0000-0000-000000000001', 'M365', 'Sample Tenant', 'ACTIVE', NOW(), NOW());
```

## Development

### Running with Hot Reload

```bash
uvicorn main:app --reload --port 8000
```

## Production

### Using Docker

```bash
# Build and run
docker build -t tm-vault-backend .
docker run -p 8000:8000 --env-file .env tm-vault-backend
```

### Using Docker Compose

```bash
# From project root
docker-compose up -d
```

### Manual Production Deploy

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

## Frontend Integration

Update the frontend API base URL in your Angular environment file:

```typescript
// src/environments/environment.ts
export const environment = {
  apiUrl: 'http://localhost:8000/api/v1'
};
```

## Features Implemented

✅ Authentication (Microsoft OAuth2 + JWT)
✅ Tenant Management
✅ Resource Management  
✅ Dashboard Metrics
✅ Job Management (list, cancel, retry, progress SSE)
✅ Snapshot Browsing
✅ SLA Policies
✅ Alerts
✅ Access Control
✅ Restore/Export Endpoints

## Features Simplified/Stubbed

- **Backup Jobs**: Returns stub responses (no actual Graph API/Azure integration)
- **RabbitMQ**: Not integrated (jobs stored in DB only)
- **Redis**: Not integrated (caching disabled by default)
- **Elasticsearch**: Not integrated (search returns empty results)
- **Azure Storage**: Not integrated (no actual backup storage)
- **Encryption**: Not implemented
- **Email Notifications**: Not implemented

To add these features, integrate the respective services and enable them via environment variables.
