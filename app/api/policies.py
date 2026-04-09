"""SLA Policy routes"""
import uuid
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import (
    SlaPolicyResponse,
    SlaPolicyCreateRequest,
    PolicyResourcesResponse,
    PolicyResourceItem,
)
from app.security import get_current_user

router = APIRouter()


@router.get("")
async def list_policies(
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List SLA policies"""
    stmt = select(models.SlaPolicy).order_by(models.SlaPolicy.created_at.desc())
    
    if tenantId:
        stmt = stmt.where(models.SlaPolicy.tenant_id == uuid.UUID(tenantId))
    
    result = await db.execute(stmt)
    policies = result.scalars().all()
    
    return [
        SlaPolicyResponse(
            id=str(p.id),
            tenantId=str(p.tenant_id),
            name=p.name,
            tier=p.tier,
            frequency=p.frequency,
            backupWindowStart=p.backup_window_start,
            backupExchange=p.backup_exchange,
            backupExchangeArchive=p.backup_exchange_archive,
            backupExchangeRecoverable=p.backup_exchange_recoverable,
            backupOneDrive=p.backup_onedrive,
            backupSharepoint=p.backup_sharepoint,
            backupTeams=p.backup_teams,
            backupTeamsChats=p.backup_teams_chats,
            backupEntraId=p.backup_entra_id,
            backupPowerPlatform=p.backup_power_platform,
            backupCopilot=p.backup_copilot,
            retentionType=p.retention_type,
            retentionDays=p.retention_days,
            retentionVersions=p.retention_versions,
            gfsDaily=p.gfs_daily,
            gfsWeekly=p.gfs_weekly,
            gfsMonthly=p.gfs_monthly,
            gfsYearly=p.gfs_yearly,
            excludeExchangeLabels=p.exclude_exchange_labels,
            excludeFileExtensions=p.exclude_file_extensions,
            encryptionMode=p.encryption_mode,
            byokKeyId=p.byok_key_id,
            enabled=p.enabled,
            isDefault=p.is_default,
            createdAt=p.created_at.isoformat() if p.created_at else "",
        )
        for p in policies
    ]


@router.get("/{policy_id}", response_model=SlaPolicyResponse)
async def get_policy(
    policy_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get SLA policy by ID"""
    stmt = select(models.SlaPolicy).where(models.SlaPolicy.id == uuid.UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    
    return SlaPolicyResponse(
        id=str(policy.id),
        tenantId=str(policy.tenant_id),
        name=policy.name,
        tier=policy.tier,
        frequency=policy.frequency,
        retentionType=policy.retention_type,
        createdAt=policy.created_at.isoformat(),
    )


@router.post("", response_model=SlaPolicyResponse)
async def create_policy(
    request: SlaPolicyCreateRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Create new SLA policy"""
    policy = models.SlaPolicy(
        id=uuid.uuid4(),
        tenant_id=uuid.UUID(request.tenantId),
        name=request.name,
        tier=request.tier,
        frequency=request.frequency,
        backup_window_start=request.backupWindowStart,
        backup_exchange=request.backupExchange if request.backupExchange is not None else True,
        backup_onedrive=request.backupOneDrive if request.backupOneDrive is not None else True,
        backup_sharepoint=request.backupSharepoint if request.backupSharepoint is not None else True,
        backup_teams=request.backupTeams if request.backupTeams is not None else True,
        backup_entra_id=request.backupEntraId if request.backupEntraId is not None else True,
        retention_type=request.retentionType,
        retention_days=request.retentionDays,
        retention_versions=request.retentionVersions,
        enabled=request.enabled if request.enabled is not None else True,
        is_default=request.isDefault if request.isDefault is not None else False,
    )
    db.add(policy)
    await db.flush()
    
    return SlaPolicyResponse(
        id=str(policy.id),
        tenantId=str(policy.tenant_id),
        name=policy.name,
        tier=policy.tier,
        frequency=policy.frequency,
        retentionType=policy.retention_type,
        createdAt=policy.created_at.isoformat(),
    )


@router.put("/{policy_id}", response_model=SlaPolicyResponse)
async def update_policy(
    policy_id: str,
    request: dict,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update SLA policy"""
    stmt = select(models.SlaPolicy).where(models.SlaPolicy.id == uuid.UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    
    for key, value in request.items():
        if hasattr(policy, key):
            setattr(policy, key, value)
    
    policy.updated_at = datetime.now(timezone.utc)
    await db.flush()
    
    return SlaPolicyResponse(
        id=str(policy.id),
        tenantId=str(policy.tenant_id),
        name=policy.name,
        tier=policy.tier,
        frequency=policy.frequency,
        retentionType=policy.retention_type,
        createdAt=policy.created_at.isoformat(),
    )


@router.delete("/{policy_id}", status_code=204)
async def delete_policy(
    policy_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Delete SLA policy"""
    stmt = select(models.SlaPolicy).where(models.SlaPolicy.id == uuid.UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    
    await db.delete(policy)
    await db.flush()


@router.get("/{policy_id}/resources")
async def get_policy_resources(
    policy_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List resources assigned to policy"""
    stmt = select(models.Resource).where(models.Resource.sla_policy_id == uuid.UUID(policy_id))
    result = await db.execute(stmt)
    resources = result.scalars().all()
    
    return PolicyResourcesResponse(
        content=[
            PolicyResourceItem(
                id=str(r.id),
                name=r.display_name,
                type=r.type.value if hasattr(r.type, 'value') else str(r.type),
                assignedAt=r.created_at.isoformat() if r.created_at else "",
            )
            for r in resources
        ],
        totalPages=1,
        totalElements=len(resources),
    )


@router.post("/{policy_id}/auto-assign", status_code=204)
async def auto_assign_policy(
    policy_id: str,
    request: dict,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Auto-assign policy to resources matching Entra group"""
    pass
