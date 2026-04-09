"""Snapshot routes"""
import uuid
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import (
    SnapshotListResponse,
    SnapshotResponse,
    SnapshotItemListResponse,
    SnapshotItemResponse,
    SnapshotDiff,
)
from app.security import get_current_user

router = APIRouter()


@router.get("/resources/{resource_id}/snapshots")
async def list_snapshots(
    resource_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List snapshots for a resource"""
    filters = [models.Snapshot.resource_id == uuid.UUID(resource_id)]
    
    count_stmt = select(func.count(models.Snapshot.id)).where(*filters)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0
    
    stmt = (
        select(models.Snapshot)
        .where(*filters)
        .order_by(models.Snapshot.created_at.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    snapshots = result.scalars().all()
    
    return SnapshotListResponse(
        content=[
            SnapshotResponse(
                id=str(s.id),
                resourceId=str(s.resource_id),
                createdAt=s.created_at.isoformat() if s.created_at else "",
                size=s.bytes_total or 0,
                status=s.status.value if hasattr(s.status, 'value') else str(s.status),
                type=s.type.value if hasattr(s.type, 'value') else str(s.type),
                itemCount=s.item_count or 0,
            )
            for s in snapshots
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size,
        number=page,
    )


@router.get("/resources/snapshots/{snapshot_id}", response_model=SnapshotResponse)
async def get_snapshot(
    snapshot_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get snapshot by ID"""
    stmt = select(models.Snapshot).where(models.Snapshot.id == uuid.UUID(snapshot_id))
    result = await db.execute(stmt)
    snapshot = result.scalar_one_or_none()
    
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    
    return SnapshotResponse(
        id=str(snapshot.id),
        resourceId=str(snapshot.resource_id),
        createdAt=snapshot.created_at.isoformat(),
        size=snapshot.bytes_total or 0,
        status=snapshot.status.value if hasattr(snapshot.status, 'value') else str(snapshot.status),
        type=snapshot.type.value if hasattr(snapshot.type, 'value') else str(snapshot.type),
        itemCount=snapshot.item_count or 0,
    )


@router.get("/resources/snapshots/{snapshot_id}/items")
async def list_snapshot_items(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List items in a snapshot"""
    filters = [models.SnapshotItem.snapshot_id == uuid.UUID(snapshot_id)]
    
    count_stmt = select(func.count(models.SnapshotItem.id)).where(*filters)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0
    
    stmt = (
        select(models.SnapshotItem)
        .where(*filters)
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    items = result.scalars().all()
    
    return SnapshotItemListResponse(
        content=[
            SnapshotItemResponse(
                id=str(i.id),
                snapshotId=str(i.snapshot_id),
                externalId=i.external_id,
                itemType=i.item_type,
                name=i.name,
                folderPath=i.folder_path,
                contentSize=i.content_size or 0,
                metadata=i.metadata or {},
                isDeleted=i.is_deleted or False,
                createdAt=i.created_at.isoformat() if i.created_at else "",
            )
            for i in items
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size,
        number=page,
    )


@router.get("/resources/snapshots/{snapshot_id}/diff", response_model=SnapshotDiff)
async def diff_snapshots(
    snapshot_id: str,
    snapshot2: str = Query(...),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Compare two snapshots"""
    # Simplified - in production, actually compare snapshot items
    return SnapshotDiff(added=[], removed=[], modified=[])


@router.get("/resources/{resource_id}/snapshots/{snapshot_id}/items")
async def browse_snapshot_items(
    resource_id: str,
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    itemType: Optional[str] = Query(None),
    folderPath: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Browse snapshot items with filters"""
    filters = [models.SnapshotItem.snapshot_id == uuid.UUID(snapshot_id)]
    
    if itemType:
        filters.append(models.SnapshotItem.item_type == itemType)
    if folderPath:
        filters.append(models.SnapshotItem.folder_path.like(f"{folderPath}%"))
    
    count_stmt = select(func.count(models.SnapshotItem.id)).where(*filters)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0
    
    stmt = (
        select(models.SnapshotItem)
        .where(*filters)
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    items = result.scalars().all()
    
    return SnapshotItemListResponse(
        content=[
            SnapshotItemResponse(
                id=str(i.id),
                snapshotId=str(i.snapshot_id),
                externalId=i.external_id,
                itemType=i.item_type,
                name=i.name,
                folderPath=i.folder_path,
                contentSize=i.content_size or 0,
                metadata=i.metadata or {},
                isDeleted=i.is_deleted or False,
                createdAt=i.created_at.isoformat() if i.created_at else "",
            )
            for i in items
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size,
        number=page,
    )
