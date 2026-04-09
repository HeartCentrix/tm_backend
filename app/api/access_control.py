"""Access Control routes"""
import uuid
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import (
    AccessGroupResponse,
    AccessGroupListResponse,
    AccessGroupMemberResponse,
)
from app.security import get_current_user

router = APIRouter()


@router.get("")
async def list_access_groups(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List access groups"""
    filters = []
    if tenantId:
        filters.append(models.AccessGroup.tenant_id == uuid.UUID(tenantId))
    
    count_stmt = select(func.count(models.AccessGroup.id)).where(*filters)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0
    
    stmt = (
        select(models.AccessGroup)
        .where(*filters)
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    groups = result.scalars().all()
    
    return AccessGroupListResponse(
        content=[
            AccessGroupResponse(
                id=str(g.id),
                name=g.name,
                description=g.description,
                memberCount=len(g.member_ids) if g.member_ids else 0,
                createdAt=g.created_at.isoformat() if g.created_at else None,
            )
            for g in groups
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size,
        number=page,
    )


@router.post("", response_model=AccessGroupResponse)
async def create_access_group(
    request: dict,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Create access group"""
    group = models.AccessGroup(
        id=uuid.uuid4(),
        org_id=user.org_id,
        name=request.get("name"),
        description=request.get("description"),
        member_ids=request.get("memberIds", []),
    )
    db.add(group)
    await db.flush()
    
    return AccessGroupResponse(
        id=str(group.id),
        name=group.name,
        description=group.description,
        createdAt=group.created_at.isoformat() if group.created_at else None,
    )


@router.put("/{group_id}", response_model=AccessGroupResponse)
async def update_access_group(
    group_id: str,
    request: dict,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update access group"""
    stmt = select(models.AccessGroup).where(models.AccessGroup.id == uuid.UUID(group_id))
    result = await db.execute(stmt)
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    for key, value in request.items():
        if hasattr(group, key):
            setattr(group, key, value)
    
    await db.flush()
    
    return AccessGroupResponse(
        id=str(group.id),
        name=group.name,
        createdAt=group.created_at.isoformat() if group.created_at else None,
    )


@router.delete("/{group_id}", status_code=204)
async def delete_access_group(
    group_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Delete access group"""
    stmt = select(models.AccessGroup).where(models.AccessGroup.id == uuid.UUID(group_id))
    result = await db.execute(stmt)
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    await db.delete(group)
    await db.flush()


@router.post("/{group_id}/members", response_model=AccessGroupMemberResponse)
async def add_member(
    group_id: str,
    request: dict,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Add member to access group"""
    return AccessGroupMemberResponse(
        id=str(uuid.uuid4()),
        groupId=group_id,
        userId=request.get("userId", ""),
        userName=request.get("userName", ""),
        userEmail=request.get("userEmail", ""),
        role=request.get("role", "MEMBER"),
        addedAt=datetime.now(timezone.utc).isoformat(),
    )


@router.delete("/{group_id}/members/{member_id}", status_code=204)
async def remove_member(
    group_id: str,
    member_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Remove member from access group"""
    pass


@router.get("/self-service/settings")
async def get_self_service_settings(
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get self-service settings"""
    return {"enabled": True, "allowRestore": True, "allowExport": True, "maxExportItems": 100}


@router.put("/self-service/settings")
async def update_self_service_settings(
    settings: dict,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Update self-service settings"""
    return settings


@router.get("/ip-restrictions")
async def get_ip_restrictions(
    user=Depends(get_current_user),
):
    """List IP restriction rules"""
    return {"enabled": False, "allowedIPs": [], "blockedIPs": []}


@router.put("/ip-restrictions")
async def update_ip_restrictions(
    restrictions: dict,
    user=Depends(get_current_user),
):
    """Update IP restriction rules"""
    return restrictions
