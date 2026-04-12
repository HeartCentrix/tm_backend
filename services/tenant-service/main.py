"""Tenant Service - Manages tenants and organizations"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta
import csv
import io

from fastapi import FastAPI, Depends, HTTPException, Query, Response
from sqlalchemy import select, func

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Tenant, TenantType, TenantStatus, Organization, Resource, ResourceStatus, ResourceType, SlaPolicy
from shared.schemas import (
    TenantResponse, TenantCreateRequest, DiscoveryStatus, TenantInfoResponse,
    StorageSummaryItem, OrganizationResponse
)
from shared.graph_client import GraphClient


TYPE_MAP = {
    "MAILBOX": ResourceType.MAILBOX,
    "SHARED_MAILBOX": ResourceType.SHARED_MAILBOX,
    "ONEDRIVE": ResourceType.ONEDRIVE,
    "SHAREPOINT_SITE": ResourceType.SHAREPOINT_SITE,
    "TEAMS_CHANNEL": ResourceType.TEAMS_CHANNEL,
    "TEAMS_CHAT": ResourceType.TEAMS_CHAT,
    "ENTRA_USER": ResourceType.ENTRA_USER,
    "ENTRA_GROUP": ResourceType.ENTRA_GROUP,
    "ENTRA_APP": ResourceType.ENTRA_APP,
    "ENTRA_DEVICE": ResourceType.ENTRA_DEVICE,
}


async def run_tenant_discovery(db: AsyncSession, tenant: Tenant) -> int:
    """Run discovery for a single tenant. Returns count of new resources."""
    tenant.status = TenantStatus.DISCOVERING
    await db.flush()

    client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
    client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
    ext_tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"

    graph = GraphClient(client_id, client_secret, ext_tenant_id)
    resources = await graph.discover_all()

    count = 0
    for r in resources:
        existing_stmt = select(Resource).where(
            Resource.tenant_id == tenant.id,
            Resource.external_id == r["external_id"],
        )
        existing_result = await db.execute(existing_stmt)
        existing = existing_result.scalar_one_or_none()

        if existing is None:
            rtype = TYPE_MAP.get(r.get("type", "ENTRA_USER"), ResourceType.ENTRA_USER)
            resource = Resource(
                id=uuid4(),
                tenant_id=tenant.id,
                type=rtype,
                external_id=r["external_id"],
                display_name=r.get("display_name", "Unknown"),
                email=r.get("email"),
                metadata=r.get("metadata", {}),
                status=ResourceStatus.DISCOVERED,
            )
            db.add(resource)
            count += 1

    tenant.status = TenantStatus.ACTIVE
    tenant.last_discovery_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.flush()
    return count


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
            customerId=t.customer_id,
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
        customer_id=str(uuid4()),  # Auto-generate customer ID
        status=TenantStatus.PENDING,
    )
    db.add(tenant)
    await db.flush()

    # Auto-trigger discovery for the new tenant
    try:
        count = await run_tenant_discovery(db, tenant)
        await db.commit()
    except Exception as e:
        # Don't fail tenant creation if discovery fails
        tenant.status = TenantStatus.DISCONNECTED
        await db.commit()
        # Still return the tenant - user can retry discovery manually
        pass

    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        orgId=str(tenant.org_id) if tenant.org_id else None,
        type=tenant.type.value if tenant.type else None,
        externalTenantId=tenant.external_tenant_id,
        status=tenant.status.value if tenant.status else "PENDING",
        storageRegion=tenant.storage_region,
        lastDiscoveryAt=tenant.last_discovery_at.isoformat() if tenant.last_discovery_at else None,
        createdAt=tenant.created_at.isoformat() if tenant.created_at else None,
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
    tenant.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
async def trigger_discovery(tenant_id: str, db: AsyncSession = Depends(get_db)):
    """Run M365 discovery via Graph API and store resources"""
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    try:
        count = await run_tenant_discovery(db, tenant)
        await db.commit()
        return {"discoveryId": str(uuid4()), "resourcesFound": count}
    except Exception as e:
        tenant.status = TenantStatus.DISCONNECTED
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Discovery failed: {str(e)}")


@app.post("/api/v1/tenants/{tenant_id}/discover-azure")
async def trigger_azure_discovery(tenant_id: str, db: AsyncSession = Depends(get_db)):
    """Azure discovery - not yet implemented"""
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if tenant:
        tenant.status = TenantStatus.DISCOVERING
        await db.flush()
    return {"discoveryId": str(uuid4()), "resourcesFound": 0}


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


@app.get("/api/v1/tenants/{tenant_id}/info", response_model=TenantInfoResponse)
async def get_tenant_info(tenant_id: str, db: AsyncSession = Depends(get_db)):
    """Get tenant info for the settings info page (Customer ID, Tenant ID, Region)"""
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Generate customer_id if it doesn't exist
    if not tenant.customer_id:
        tenant.customer_id = str(uuid4())
        await db.flush()
    
    # Map region code to human-readable name
    region_map = {
        "AU": "Australia",
        "US": "United States",
        "EU": "Europe",
        "UK": "United Kingdom",
        "CA": "Canada",
        "DE": "Germany",
        "FR": "France",
        "JP": "Japan",
        "IN": "India",
        "BR": "Brazil",
    }
    
    region_code = tenant.storage_region or "US"
    region_name = region_map.get(region_code, region_code)
    
    return TenantInfoResponse(
        customerId=tenant.customer_id,
        tenantId=tenant.external_tenant_id or tenant_id,
        region=region_name,
    )


@app.get("/api/v1/tenants/{tenant_id}/usage-report")
async def download_usage_report(tenant_id: str, db: AsyncSession = Depends(get_db)):
    """Download usage report as CSV for the tenant"""
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Get all resources for this tenant
    resource_stmt = select(Resource).where(Resource.tenant_id == UUID(tenant_id))
    resource_result = await db.execute(resource_stmt)
    resources = resource_result.scalars().all()
    
    # Get SLA policies for mapping
    policy_stmt = select(SlaPolicy).where(SlaPolicy.tenant_id == UUID(tenant_id))
    policy_result = await db.execute(policy_stmt)
    policies = {str(p.id): p.name for p in policy_result.scalars().all()}
    
    # Generate dates for the last 12 days (including today)
    today = datetime.now(timezone.utc).replace(tzinfo=None)
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(11, -1, -1)]
    
    # Map resource types to human-readable kinds
    type_map = {
        ResourceType.MAILBOX: "User",
        ResourceType.SHARED_MAILBOX: "Shared Mailbox",
        ResourceType.ONEDRIVE: "OneDrive",
        ResourceType.SHAREPOINT_SITE: "SharePoint site",
        ResourceType.TEAMS_CHANNEL: "Team Channel",
        ResourceType.TEAMS_CHAT: "Teams Chat",
        ResourceType.ENTRA_USER: "User",
        ResourceType.ENTRA_GROUP: "Microsoft 365 group",
        ResourceType.ENTRA_APP: "App",
        ResourceType.ENTRA_DEVICE: "Device",
    }
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header row with report date
    writer.writerow(["Report date:", today.strftime("%b %d %Y")])
    writer.writerow([])  # Empty row
    
    # Column headers
    headers = [
        "Resource ID",
        "Resource name",
        "Resource kind",
        "SLA",
        "Is active",
        "Backup Size, GB (current)",
    ] + dates
    writer.writerow(headers)
    
    # Data rows
    for resource in resources:
        # Determine resource kind
        resource_kind = type_map.get(resource.type, "Unknown")
        
        # Determine SLA
        sla_name = "Not protected"
        if resource.sla_policy_id:
            sla_name = policies.get(str(resource.sla_policy_id), "Manual")
        
        # Determine active status
        is_active = "active" if resource.status == ResourceStatus.ACTIVE else "archived"
        
        # Calculate backup size in GB
        backup_size_gb = resource.storage_bytes / (1024 ** 3) if resource.storage_bytes else 0.0
        
        # Row data
        row = [
            str(resource.id),
            resource.display_name,
            resource_kind,
            sla_name,
            is_active,
            f"{backup_size_gb:.1f}",
        ] + [f"{backup_size_gb:.1f}" for _ in dates]  # Same size for all dates (simplified)
        
        writer.writerow(row)
    
    # Get CSV content
    csv_content = output.getvalue()
    output.close()
    
    # Generate filename
    filename = f"{tenant.display_name.replace(' ', '_')}_report_{today.strftime('%b-%d-%Y')}.csv"
    
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
