"""API Gateway - Routes requests to microservices"""
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx

from shared.config import settings
from shared.database import init_db, close_db

# Service URLs
SERVICES = {
    "auth": "http://auth-service:8001",
    "tenant": "http://tenant-service:8002",
    "resource": "http://resource-service:8003",
    "job": "http://job-service:8004",
    "snapshot": "http://snapshot-service:8005",
    "dashboard": "http://dashboard-service:8006",
    "alert": "http://alert-service:8007",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(title="TM Vault API Gateway", version="1.0.0", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def proxy_request(service: str, path: str, request: Request):
    """Forward request to microservice"""
    service_url = SERVICES.get(service)
    if not service_url:
        raise HTTPException(status_code=502, detail="Service not found")
    
    url = f"{service_url}{path}"
    
    # Build headers (forward auth)
    headers = {}
    if "authorization" in request.headers:
        headers["Authorization"] = request.headers["authorization"]
    
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method,
            url=url,
            headers=headers,
            params=request.query_params,
            content=await request.body(),
            timeout=30.0,
        )
        
        return response.content, response.status_code, dict(response.headers)


# Auth routes
@app.get("/api/v1/auth/microsoft/url")
@app.get("/api/v1/auth/microsoft/datasource/url")
@app.get("/api/v1/auth/azure/datasource/url")
async def auth_get(request: Request):
    content, status, headers = await proxy_request("auth", request.url.path, request)
    return content


@app.post("/api/v1/auth/callback")
@app.post("/api/v1/auth/refresh")
@app.post("/api/v1/auth/logout")
@app.post("/api/v1/auth/microsoft/datasource/callback")
@app.post("/api/v1/auth/azure/datasource/callback")
async def auth_post(request: Request):
    content, status, headers = await proxy_request("auth", request.url.path, request)
    return content


@app.get("/api/v1/auth/me")
async def auth_me(request: Request):
    content, status, headers = await proxy_request("auth", request.url.path, request)
    return content


# Dashboard routes
@app.get("/api/v1/dashboard/{rest:path}")
async def dashboard(request: Request):
    content, status, headers = await proxy_request("dashboard", request.url.path, request)
    return content


# Tenant routes
@app.get("/api/v1/tenants")
@app.get("/api/v1/tenants/{tenant_id}")
@app.post("/api/v1/tenants")
@app.put("/api/v1/tenants/{tenant_id}")
@app.delete("/api/v1/tenants/{tenant_id}")
@app.post("/api/v1/tenants/{tenant_id}/discover")
@app.post("/api/v1/tenants/{tenant_id}/discover-m365")
@app.post("/api/v1/tenants/{tenant_id}/discover-azure")
@app.get("/api/v1/tenants/{tenant_id}/discovery-status")
@app.get("/api/v1/tenants/{tenant_id}/storage-summary")
@app.post("/api/v1/tenants/{tenant_id}/test-connection")
@app.get("/api/v1/organizations")
@app.get("/api/v1/organizations/{org_id}")
async def tenant(request: Request):
    content, status, headers = await proxy_request("tenant", request.url.path, request)
    return content


# Resource routes
@app.get("/api/v1/resources")
@app.get("/api/v1/resources/{resource_id}")
@app.get("/api/v1/resources/search")
@app.get("/api/v1/resources/by-type")
@app.get("/api/v1/resources/users")
@app.get("/api/v1/resources/{resource_id}/storage-history")
@app.post("/api/v1/resources/{resource_id}/assign-policy")
@app.post("/api/v1/resources/{resource_id}/unassign-policy")
@app.post("/api/v1/resources/{resource_id}/archive")
@app.post("/api/v1/resources/{resource_id}/unarchive")
@app.delete("/api/v1/resources/{resource_id}")
@app.post("/api/v1/resources/bulk-assign-policy")
@app.post("/api/v1/resources/bulk-archive")
async def resource(request: Request):
    content, status, headers = await proxy_request("resource", request.url.path, request)
    return content


# Job routes
@app.get("/api/v1/jobs")
@app.get("/api/v1/jobs/{job_id}")
@app.get("/api/v1/jobs/{job_id}/progress")
@app.post("/api/v1/jobs/{job_id}/cancel")
@app.post("/api/v1/jobs/{job_id}/retry")
@app.get("/api/v1/jobs/{job_id}/logs")
@app.post("/api/v1/backups/trigger")
@app.post("/api/v1/backups/trigger-user/{resource_id}")
@app.post("/api/v1/backups/trigger-bulk")
@app.get("/api/v1/dlq/stats")
@app.post("/api/v1/dlq/{dlq_name}/purge")
@app.post("/api/v1/dlq/{dlq_name}/requeue")
async def job(request: Request):
    content, status, headers = await proxy_request("job", request.url.path, request)
    return content


# Snapshot routes
@app.get("/api/v1/resources/{resource_id}/snapshots")
@app.get("/api/v1/resources/{resource_id}/snapshots/by-date")
@app.get("/api/v1/resources/{resource_id}/snapshots/calendar")
@app.get("/api/v1/resources/snapshots/{snapshot_id}")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/search")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}/preview")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/diff")
@app.get("/api/v1/resources/{resource_id}/snapshots/{snapshot_id}/items")
@app.get("/api/v1/resources/{resource_id}/snapshots/changes")
async def snapshot(request: Request):
    content, status, headers = await proxy_request("snapshot", request.url.path, request)
    return content


# Restore routes
@app.post("/api/v1/jobs/restore")
@app.post("/api/v1/jobs/restore/mailbox")
@app.post("/api/v1/jobs/restore/onedrive")
@app.post("/api/v1/jobs/restore/sharepoint")
@app.post("/api/v1/jobs/restore/entra-object")
@app.get("/api/v1/jobs/restore/{job_id}/status")
@app.get("/api/v1/jobs/restore/history")
@app.post("/api/v1/jobs/export")
@app.get("/api/v1/jobs/export/{job_id}/status")
@app.get("/api/v1/jobs/export/{job_id}/download")
async def restore(request: Request):
    content, status, headers = await proxy_request("job", request.url.path, request)
    return content


# Alert routes
@app.get("/api/v1/alerts")
@app.get("/api/v1/alerts/{alert_id}")
@app.post("/api/v1/alerts/{alert_id}/resolve")
@app.get("/api/v1/alerts/notifications/settings")
@app.put("/api/v1/alerts/notifications/settings")
@app.get("/api/v1/alerts/webhooks")
@app.post("/api/v1/alerts/webhooks")
@app.delete("/api/v1/alerts/webhooks/{webhook_id}")
@app.post("/api/v1/alerts/webhooks/{webhook_id}/test")
async def alert(request: Request):
    content, status, headers = await proxy_request("alert", request.url.path, request)
    return content


# SLA Policy routes
@app.get("/api/v1/policies")
@app.get("/api/v1/policies/{policy_id}")
@app.post("/api/v1/policies")
@app.put("/api/v1/policies/{policy_id}")
@app.delete("/api/v1/policies/{policy_id}")
@app.get("/api/v1/policies/{policy_id}/resources")
@app.post("/api/v1/policies/{policy_id}/auto-assign")
async def policy(request: Request):
    content, status, headers = await proxy_request("resource", request.url.path, request)
    return content


# Access Control routes
@app.get("/api/v1/access-groups")
@app.post("/api/v1/access-groups")
@app.put("/api/v1/access-groups/{group_id}")
@app.delete("/api/v1/access-groups/{group_id}")
@app.post("/api/v1/access-groups/{group_id}/members")
@app.delete("/api/v1/access-groups/{group_id}/members/{member_id}")
@app.get("/api/v1/access-groups/self-service/settings")
@app.put("/api/v1/access-groups/self-service/settings")
@app.get("/api/v1/access-groups/ip-restrictions")
@app.put("/api/v1/access-groups/ip-restrictions")
async def access_control(request: Request):
    content, status, headers = await proxy_request("alert", request.url.path, request)
    return content


@app.get("/health")
async def health():
    return {"status": "ok", "service": "api-gateway"}
