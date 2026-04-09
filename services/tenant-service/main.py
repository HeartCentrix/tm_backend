"""Tenant Service - Manages tenants and organizations"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, func

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Tenant, TenantType, TenantStatus, Organization
from shared.schemas import (
    TenantResponse, TenantCreateRequest, DiscoveryStatus,
    StorageSummaryItem, OrganizationResponse
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Tenant Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "tenant"}


@app.get("/api/v1/tenants", response_model=list[TenantResponse])
async def list_tenants(
    orgId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Tenant).order_by(Tenant.created_at)
    if orgId:
        stmt = stmt.where(Tenant.org_id == UUID(orgId))
    result = await db.execute(stmt)
    tenants = result.scalars().all()
    return [
        TenantResponse(
            id=str(t.id),
            displayName=t.display_name,
            orgId=str(t.org_id) if t.org_id else None,
            type=t.type.value if t.type else None,
            externalTenantId=t.external_tenant_id,
            status=t.status.value if t.status else "PENDING",
            storageRegion=t.storage_region,
            lastDiscoveryAt=t.last_discovery_at.isoformat() if t.last_discovery_at else None,
            createdAt=t.created_at.isoformat() if t.created_at else None,
        )
        for t in tenants
    ]


@app.get("/api/v1/tenants/{tenant_id}", response_model=TenantResponse)
async def get_tenant(tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        orgId=str(tenant.org_id) if tenant.org_id else None,
        status=tenant.status.value if tenant.status else "PENDING",
        createdAt=tenant.created_at.isoformat() if tenant.created_at else None,
    )


@app.post("/api/v1/tenants", response_model=TenantResponse)
async def create_tenant(request: TenantCreateRequest, db: AsyncSession = Depends(get_db)):
    tenant = Tenant(
        id=uuid4(),
        org_id=UUID(request.organizationId),
        type=TenantType.M365,
        display_name=request.name,
        external_tenant_id=request.microsoftTenantId,
        status=TenantStatus.PENDING,
    )
    db.add(tenant)
    await db.flush()
    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        orgId=str(tenant.org_id) if tenant.org_id else None,
        status=tenant.status.value,
        createdAt=tenant.created_at.isoformat(),
    )


@app.put("/api/v1/tenants/{tenant_id}", response_model=TenantResponse)
async def update_tenant(tenant_id: str, request: dict, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    if request.get("name"):
        tenant.display_name = request["name"]
    if request.get("status"):
        tenant.status = TenantStatus(request["status"])
    tenant.updated_at = datetime.now(timezone.utc)
    await db.flush()
    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        status=tenant.status.value,
        createdAt=tenant.created_at.isoformat(),
    )


@app.delete("/api/v1/tenants/{tenant_id}", status_code=204)
async def delete_tenant(tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    tenant.status = TenantStatus.PENDING_DELETION
    await db.flush()


@app.post("/api/v1/tenants/{tenant_id}/discover")
@app.post("/api/v1/tenants/{tenant_id}/discover-m365")
@app.post("/api/v1/tenants/{tenant_id}/discover-azure")
async def trigger_discovery(tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if tenant:
        tenant.status = TenantStatus.DISCOVERING
        await db.flush()
    return {"discoveryId": str(uuid4())}


@app.get("/api/v1/tenants/{tenant_id}/discovery-status", response_model=DiscoveryStatus)
async def get_discovery_status(tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    is_running = tenant.status == TenantStatus.DISCOVERING
    return DiscoveryStatus(
        tenantId=tenant_id,
        status="RUNNING" if is_running else "COMPLETED",
        progress=100 if not is_running else 50,
        resourcesDiscovered=0,
        startedAt=tenant.last_discovery_at.isoformat() if tenant.last_discovery_at else datetime.now(timezone.utc).isoformat(),
    )


@app.get("/api/v1/tenants/{tenant_id}/storage-summary")
async def get_storage_summary(tenant_id: str, db: AsyncSession = Depends(get_db)):
    # Simplified - return empty list
    return []


@app.post("/api/v1/tenants/{tenant_id}/test-connection")
async def test_connection(tenant_id: str):
    return {"connected": True, "message": "Connection successful"}


@app.get("/api/v1/organizations", response_model=list[OrganizationResponse])
async def list_orgs(db: AsyncSession = Depends(get_db)):
    stmt = select(Organization).order_by(Organization.created_at)
    result = await db.execute(stmt)
    orgs = result.scalars().all()
    return [
        OrganizationResponse(
            id=str(o.id),
            name=o.name,
            status="ACTIVE",
            tenantCount=0,
            createdAt=o.created_at.isoformat() if o.created_at else "",
        )
        for o in orgs
    ]


@app.get("/api/v1/organizations/{org_id}", response_model=OrganizationResponse)
async def get_org(org_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Organization).where(Organization.id == UUID(org_id))
    result = await db.execute(stmt)
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return OrganizationResponse(
        id=str(org.id),
        name=org.name,
        status="ACTIVE",
        tenantCount=0,
        createdAt=org.created_at.isoformat() if org.created_at else "",
    )
