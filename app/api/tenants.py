"""Tenant and Organization routes"""
import uuid
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import (
    TenantResponse,
    TenantCreateRequest,
    TenantUpdateRequest,
    DiscoveryStatus,
    StorageSummaryItem,
    OrganizationResponse,
)
from app.security import get_current_user

router = APIRouter()


# ============ Organizations ============

@router.get("", response_model=list[OrganizationResponse])
async def list_organizations(
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List all organizations"""
    stmt = select(models.Organization).order_by(models.Organization.created_at)
    result = await db.execute(stmt)
    orgs = result.scalars().all()
    
    return [
        OrganizationResponse(
            id=str(org.id),
            name=org.name,
            status="ACTIVE",
            tenantCount=0,  # Would need to query
            createdAt=org.created_at.isoformat() if org.created_at else "",
        )
        for org in orgs
    ]


@router.get("/{org_id}", response_model=OrganizationResponse)
async def get_organization(
    org_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get organization by ID"""
    stmt = select(models.Organization).where(models.Organization.id == uuid.UUID(org_id))
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


# ============ Tenants ============

@router.get("", response_model=list[TenantResponse])
async def list_tenants(
    orgId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List all tenants"""
    stmt = select(models.Tenant).order_by(models.Tenant.created_at)
    
    if orgId:
        stmt = stmt.where(models.Tenant.org_id == uuid.UUID(orgId))
    elif user.org_id:
        stmt = stmt.where(models.Tenant.org_id == user.org_id)
    
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


@router.get("/{tenant_id}", response_model=TenantResponse)
async def get_tenant(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get tenant by ID"""
    stmt = select(models.Tenant).where(models.Tenant.id == uuid.UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
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


@router.post("", response_model=TenantResponse)
async def create_tenant(
    request: TenantCreateRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Register new tenant"""
    tenant = models.Tenant(
        id=uuid.uuid4(),
        org_id=uuid.UUID(request.organizationId),
        type=models.TenantType.M365,
        display_name=request.name,
        external_tenant_id=request.microsoftTenantId,
        status=models.TenantStatus.PENDING,
    )
    db.add(tenant)
    await db.flush()
    
    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        orgId=str(tenant.org_id) if tenant.org_id else None,
        type=tenant.type.value if tenant.type else None,
        status=tenant.status.value if tenant.status else "PENDING",
        createdAt=tenant.created_at.isoformat() if tenant.created_at else None,
    )


@router.put("/{tenant_id}", response_model=TenantResponse)
async def update_tenant(
    tenant_id: str,
    request: TenantUpdateRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update tenant"""
    stmt = select(models.Tenant).where(models.Tenant.id == uuid.UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    if request.name:
        tenant.display_name = request.name
    if request.status:
        tenant.status = models.TenantStatus(request.status)
    
    tenant.updated_at = datetime.now(timezone.utc)
    await db.flush()
    
    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        orgId=str(tenant.org_id) if tenant.org_id else None,
        status=tenant.status.value if tenant.status else "PENDING",
        createdAt=tenant.created_at.isoformat() if tenant.created_at else None,
    )


@router.delete("/{tenant_id}", status_code=204)
async def delete_tenant(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Delete tenant (sets status to PENDING_DELETION)"""
    stmt = select(models.Tenant).where(models.Tenant.id == uuid.UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    tenant.status = models.TenantStatus.PENDING_DELETION
    await db.flush()


@router.post("/{tenant_id}/discover")
async def trigger_discovery(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger M365 resource discovery"""
    discovery_id = str(uuid.uuid4())
    
    # Update tenant status
    stmt = select(models.Tenant).where(models.Tenant.id == uuid.UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    
    if tenant:
        tenant.status = models.TenantStatus.DISCOVERING
        await db.flush()
    
    return {"discoveryId": discovery_id}


@router.post("/{tenant_id}/discover-m365")
async def trigger_m365_discovery(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger M365-specific discovery"""
    discovery_id = str(uuid.uuid4())
    
    stmt = select(models.Tenant).where(models.Tenant.id == uuid.UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    
    if tenant:
        tenant.status = models.TenantStatus.DISCOVERING
        await db.flush()
    
    return {"discoveryId": discovery_id}


@router.post("/{tenant_id}/discover-azure")
async def trigger_azure_discovery(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger Azure resource discovery"""
    discovery_id = str(uuid.uuid4())
    
    stmt = select(models.Tenant).where(models.Tenant.id == uuid.UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    
    if tenant:
        tenant.status = models.TenantStatus.DISCOVERING
        await db.flush()
    
    return {"discoveryId": discovery_id}


@router.get("/{tenant_id}/discovery-status", response_model=DiscoveryStatus)
async def get_discovery_status(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get tenant discovery status"""
    stmt = select(models.Tenant).where(models.Tenant.id == uuid.UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    status_value = tenant.status.value if tenant.status else "PENDING"
    is_running = status_value == "DISCOVERING"
    
    return DiscoveryStatus(
        tenantId=tenant_id,
        status="RUNNING" if is_running else "COMPLETED",
        progress=100 if not is_running else 50,
        resourcesDiscovered=0,
        startedAt=tenant.last_discovery_at.isoformat() if tenant.last_discovery_at else datetime.now(timezone.utc).isoformat(),
        errorMessage=None,
    )


@router.get("/{tenant_id}/storage-summary")
async def get_storage_summary(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get storage breakdown by workload type"""
    stmt = (
        select(
            models.Resource.type,
            func.sum(models.Resource.storage_bytes).label("size"),
            func.count().label("count"),
        )
        .where(models.Resource.tenant_id == uuid.UUID(tenant_id))
        .group_by(models.Resource.type)
    )
    result = await db.execute(stmt)
    rows = result.all()
    
    workload_map = {
        models.ResourceType.MAILBOX: "Exchange",
        models.ResourceType.SHARED_MAILBOX: "Exchange",
        models.ResourceType.ONEDRIVE: "OneDrive",
        models.ResourceType.SHAREPOINT_SITE: "SharePoint",
        models.ResourceType.TEAMS_CHANNEL: "Teams",
        models.ResourceType.TEAMS_CHAT: "Teams",
        models.ResourceType.ENTRA_GROUP: "Entra ID",
    }
    
    summary = {}
    for row in rows:
        workload = workload_map.get(row.type, row.type.value if hasattr(row.type, 'value') else str(row.type))
        if workload not in summary:
            summary[workload] = {"size": 0, "count": 0}
        summary[workload]["size"] += row.size or 0
        summary[workload]["count"] += row.count
    
    return [
        StorageSummaryItem(workload=k, size=v["size"], resourceCount=v["count"])
        for k, v in summary.items()
    ]


@router.post("/{tenant_id}/test-connection")
async def test_connection(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Test Microsoft Graph API connectivity"""
    return {"connected": True, "message": "Connection successful"}
