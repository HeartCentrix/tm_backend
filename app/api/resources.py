"""Resource routes"""
import uuid
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, or_

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import (
    ResourceResponse,
    ResourceListResponse,
    UserResourceResponse,
    StorageHistoryItem,
    AssignPolicyRequest,
    BulkOperationRequest,
)
from app.security import get_current_user

router = APIRouter()


def format_bytes(bytes_val: int) -> str:
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024**2:
        return f"{bytes_val / 1024:.1f} KB"
    elif bytes_val < 1024**3:
        return f"{bytes_val / 1024**2:.1f} MB"
    else:
        return f"{bytes_val / 1024**3:.1f} GB"


@router.get("")
async def list_resources(
    tenantId: str = Query(...),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List resources with pagination"""
    filters = [models.Resource.tenant_id == uuid.UUID(tenantId)]
    
    # Count total
    count_stmt = select(func.count(models.Resource.id)).where(*filters)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0
    
    # Get page
    stmt = (
        select(models.Resource)
        .where(*filters)
        .order_by(models.Resource.created_at.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    resources = result.scalars().all()
    
    # Get SLA policy names
    policy_ids = [r.sla_policy_id for r in resources if r.sla_policy_id]
    policies = {}
    if policy_ids:
        policy_stmt = select(models.SlaPolicy).where(models.SlaPolicy.id.in_(policy_ids))
        policy_result = await db.execute(policy_stmt)
        policies = {p.id: p.name for p in policy_result.scalars().all()}
    
    return ResourceListResponse(
        content=[
            ResourceResponse(
                id=str(r.id),
                name=r.display_name,
                email=r.email,
                type=r.type.value if hasattr(r.type, 'value') else str(r.type),
                sla=policies.get(r.sla_policy_id) if r.sla_policy_id else None,
                totalSize=format_bytes(r.storage_bytes or 0),
                lastBackup=r.last_backup_at.isoformat() if r.last_backup_at else None,
                status=r.status.value if hasattr(r.status, 'value') else str(r.status),
                tenantId=str(r.tenant_id),
                archived=r.status == models.ResourceStatus.ARCHIVED if r.status else False,
                createdAt=r.created_at.isoformat() if r.created_at else None,
            )
            for r in resources
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size,
        number=page,
        first=page == 1,
        last=page >= (total + size - 1) // size,
    )


@router.get("/{resource_id}", response_model=ResourceResponse)
async def get_resource(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get resource by ID"""
    stmt = select(models.Resource).where(models.Resource.id == uuid.UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    
    return ResourceResponse(
        id=str(resource.id),
        name=resource.display_name,
        email=resource.email,
        type=resource.type.value if hasattr(resource.type, 'value') else str(resource.type),
        totalSize=format_bytes(resource.storage_bytes or 0),
        lastBackup=resource.last_backup_at.isoformat() if resource.last_backup_at else None,
        status=resource.status.value if hasattr(resource.status, 'value') else str(resource.status),
        tenantId=str(resource.tenant_id),
        createdAt=resource.created_at.isoformat() if resource.created_at else None,
    )


@router.get("/search")
async def search_resources(
    query: str = Query(...),
    type: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Search resources by name/email"""
    filters = [
        or_(
            models.Resource.display_name.ilike(f"%{query}%"),
            models.Resource.email.ilike(f"%{query}%"),
        )
    ]
    
    if type:
        filters.append(models.Resource.type == type)
    
    stmt = select(models.Resource).where(*filters).limit(50)
    result = await db.execute(stmt)
    resources = result.scalars().all()
    
    return [
        ResourceResponse(
            id=str(r.id),
            name=r.display_name,
            email=r.email,
            type=r.type.value if hasattr(r.type, 'value') else str(r.type),
            totalSize=format_bytes(r.storage_bytes or 0),
            status=r.status.value if hasattr(r.status, 'value') else str(r.status),
        )
        for r in resources
    ]


@router.get("/by-type")
async def get_resources_by_type(
    type: str = Query(...),
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List resources filtered by type"""
    filters = [models.Resource.type == type]
    if tenantId:
        filters.append(models.Resource.tenant_id == uuid.UUID(tenantId))
    
    stmt = select(models.Resource).where(*filters)
    result = await db.execute(stmt)
    resources = result.scalars().all()
    
    return [
        ResourceResponse(
            id=str(r.id),
            name=r.display_name,
            email=r.email,
            type=r.type.value if hasattr(r.type, 'value') else str(r.type),
            totalSize=format_bytes(r.storage_bytes or 0),
            status=r.status.value if hasattr(r.status, 'value') else str(r.status),
        )
        for r in resources
    ]


@router.get("/users")
async def get_users_with_workloads(
    tenantId: str = Query(...),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get users with workload statuses"""
    stmt = select(models.Resource).where(
        models.Resource.tenant_id == uuid.UUID(tenantId),
        models.Resource.type.in_([
            models.ResourceType.MAILBOX,
            models.ResourceType.SHARED_MAILBOX,
            models.ResourceType.ONEDRIVE,
            models.ResourceType.TEAMS_CHAT,
        ])
    )
    result = await db.execute(stmt)
    resources = result.scalars().all()
    
    # Group by email
    users_map = {}
    for r in resources:
        email = r.email or f"unknown-{r.id}"
        if email not in users_map:
            users_map[email] = {
                "id": str(r.id),
                "tenantId": str(r.tenant_id),
                "email": email,
                "displayName": r.display_name,
                "resources": [],
            }
        users_map[email]["resources"].append(r)
    
    result_list = []
    for email, user_data in users_map.items():
        user_resources = user_data["resources"]
        
        mailbox = next((r for r in user_resources if "MAILBOX" in (r.type.value if hasattr(r.type, 'value') else str(r.type))), None)
        onedrive = next((r for r in user_resources if "ONEDRIVE" in (r.type.value if hasattr(r.type, 'value') else str(r.type))), None)
        teams = next((r for r in user_resources if "TEAMS" in (r.type.value if hasattr(r.type, 'value') else str(r.type))), None)
        
        result_list.append(
            UserResourceResponse(
                id=user_data["id"],
                tenantId=user_data["tenantId"],
                email=email,
                displayName=user_data["displayName"],
                hasMailbox=mailbox is not None,
                mailboxStatus=mailbox.status.value if mailbox and mailbox.status else None,
                mailboxLastBackup=mailbox.last_backup_at.isoformat() if mailbox and mailbox.last_backup_at else None,
                mailboxStorageBytes=mailbox.storage_bytes if mailbox else None,
                hasOneDrive=onedrive is not None,
                oneDriveStatus=onedrive.status.value if onedrive and onedrive.status else None,
                oneDriveLastBackup=onedrive.last_backup_at.isoformat() if onedrive and onedrive.last_backup_at else None,
                oneDriveStorageBytes=onedrive.storage_bytes if onedrive else None,
                hasTeamsChat=teams is not None,
                teamsChatStatus=teams.status.value if teams and teams.status else None,
                teamsChatLastBackup=teams.last_backup_at.isoformat() if teams and teams.last_backup_at else None,
            )
        )
    
    return result_list


@router.get("/{resource_id}/storage-history")
async def get_storage_history(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get storage growth history"""
    stmt = select(models.Resource).where(models.Resource.id == uuid.UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    
    # Generate simulated history
    from datetime import timedelta
    current_size = resource.storage_bytes or 0
    history = []
    
    for i in range(30):
        date = (datetime.now(timezone.utc) - timedelta(days=29-i)).date()
        # Simulate growth curve
        size = int(current_size * (0.5 + 0.5 * (i / 30)))
        history.append(StorageHistoryItem(date=date.isoformat(), size=size))
    
    return history


@router.post("/{resource_id}/assign-policy", status_code=204)
async def assign_policy(
    resource_id: str,
    request: AssignPolicyRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Assign SLA policy to resource"""
    stmt = select(models.Resource).where(models.Resource.id == uuid.UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    
    resource.sla_policy_id = uuid.UUID(request.policyId)
    resource.status = models.ResourceStatus.ACTIVE
    await db.flush()


@router.post("/{resource_id}/unassign-policy", status_code=204)
async def unassign_policy(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Remove SLA policy from resource"""
    stmt = select(models.Resource).where(models.Resource.id == uuid.UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    
    resource.sla_policy_id = None
    await db.flush()


@router.post("/{resource_id}/archive", status_code=204)
async def archive_resource(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Archive resource"""
    stmt = select(models.Resource).where(models.Resource.id == uuid.UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    
    resource.status = models.ResourceStatus.ARCHIVED
    resource.archived_at = datetime.now(timezone.utc)
    await db.flush()


@router.post("/{resource_id}/unarchive", status_code=204)
async def unarchive_resource(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Unarchive resource"""
    stmt = select(models.Resource).where(models.Resource.id == uuid.UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    
    resource.status = models.ResourceStatus.ACTIVE
    resource.archived_at = None
    await db.flush()


@router.delete("/{resource_id}", status_code=204)
async def delete_resource(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Delete resource (soft delete)"""
    stmt = select(models.Resource).where(models.Resource.id == uuid.UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    
    resource.status = models.ResourceStatus.PENDING_DELETION
    resource.deletion_queued_at = datetime.now(timezone.utc)
    await db.flush()


@router.post("/bulk-assign-policy", status_code=204)
async def bulk_assign_policy(
    request: BulkOperationRequest,
    policyId: str = Query(...),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Bulk assign SLA policy to multiple resources"""
    stmt = (
        select(models.Resource)
        .where(models.Resource.id.in_([uuid.UUID(rid) for rid in request.resourceIds]))
    )
    result = await db.execute(stmt)
    resources = result.scalars().all()
    
    for resource in resources:
        resource.sla_policy_id = uuid.UUID(policyId)
        resource.status = models.ResourceStatus.ACTIVE
    
    await db.flush()


@router.post("/bulk-archive", status_code=204)
async def bulk_archive(
    request: BulkOperationRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Bulk archive resources"""
    stmt = (
        select(models.Resource)
        .where(models.Resource.id.in_([uuid.UUID(rid) for rid in request.resourceIds]))
    )
    result = await db.execute(stmt)
    resources = result.scalars().all()
    
    for resource in resources:
        resource.status = models.ResourceStatus.ARCHIVED
        resource.archived_at = datetime.now(timezone.utc)
    
    await db.flush()
