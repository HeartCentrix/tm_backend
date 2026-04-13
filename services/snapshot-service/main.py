"""Snapshot Service - Manages snapshots and snapshot items"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, func, distinct, and_, or_

from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Snapshot, SnapshotItem, Resource, SnapshotType, SnapshotStatus
from shared.schemas import (
    SnapshotListResponse, SnapshotResponse, SnapshotItemListResponse, SnapshotItemResponse, SnapshotDiff
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Snapshot Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "snapshot"}


@app.get("/api/v1/resources/{resource_id}/snapshots")
async def list_snapshots(
    resource_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    db: AsyncSession = Depends(get_db),
):
    filters = [Snapshot.resource_id == UUID(resource_id)]
    total = (await db.execute(select(func.count(Snapshot.id)).where(*filters))).scalar() or 0
    stmt = select(Snapshot).where(*filters).order_by(Snapshot.created_at.desc()).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    snapshots = result.scalars().all()
    
    return SnapshotListResponse(
        content=[
            SnapshotResponse(
                id=str(s.id), resourceId=str(s.resource_id),
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
        size=size, number=page,
    )


@app.get("/api/v1/resources/snapshots/folders")
async def get_snapshot_folders(
    snapshot_id: str = Query(...),
    item_type: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get distinct folder paths for items in a snapshot, optionally filtered by item type."""
    filters = [SnapshotItem.snapshot_id == UUID(snapshot_id)]
    if item_type:
        filters.append(SnapshotItem.item_type == item_type)

    stmt = (
        select(SnapshotItem.folder_path, func.count(SnapshotItem.id).label("count"))
        .where(*filters)
        .group_by(SnapshotItem.folder_path)
        .order_by(SnapshotItem.folder_path)
    )
    result = await db.execute(stmt)
    rows = result.fetchall()

    return [
        {"path": row[0] or "", "count": row[1]}
        for row in rows
    ]


@app.get("/api/v1/resources/snapshots/{snapshot_id}", response_model=SnapshotResponse)
async def get_snapshot(snapshot_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Snapshot).where(Snapshot.id == UUID(snapshot_id))
    result = await db.execute(stmt)
    snapshot = result.scalar_one_or_none()
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return SnapshotResponse(
        id=str(snapshot.id), resourceId=str(snapshot.resource_id),
        createdAt=snapshot.created_at.isoformat(),
        size=snapshot.bytes_total or 0,
        status=snapshot.status.value if hasattr(snapshot.status, 'value') else str(snapshot.status),
        type=snapshot.type.value if hasattr(snapshot.type, 'value') else str(snapshot.type),
        itemCount=snapshot.item_count or 0,
    )


@app.get("/api/v1/resources/snapshots/{snapshot_id}/items")
@app.get("/api/v1/resources/{resource_id}/snapshots/{snapshot_id}/items")
async def list_snapshot_items(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    db: AsyncSession = Depends(get_db),
):
    filters = [SnapshotItem.snapshot_id == UUID(snapshot_id)]
    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    stmt = select(SnapshotItem).where(*filters).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    items = result.scalars().all()
    
    return SnapshotItemListResponse(
        content=[
            SnapshotItemResponse(
                id=str(i.id), snapshotId=str(i.snapshot_id), externalId=i.external_id,
                itemType=i.item_type, name=i.name, folderPath=i.folder_path,
                contentSize=i.content_size or 0, metadata=i.extra_data or {},
                isDeleted=i.is_deleted or False,
                createdAt=i.created_at.isoformat() if i.created_at else "",
            )
            for i in items
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
    )


@app.get("/api/v1/resources/snapshots/{snapshot_id}/diff", response_model=SnapshotDiff)
async def diff_snapshots(snapshot_id: str, snapshot2: str = Query(...)):
    return SnapshotDiff(added=[], removed=[], modified=[])


@app.get("/api/v1/resources/{resource_id}/snapshots/{snapshot_id}/items")
async def browse_snapshot_items(
    resource_id: str,
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    itemType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    filters = [SnapshotItem.snapshot_id == UUID(snapshot_id)]
    if itemType:
        filters.append(SnapshotItem.item_type == itemType)

    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    stmt = select(SnapshotItem).where(*filters).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    items = result.scalars().all()

    return SnapshotItemListResponse(
        content=[
            SnapshotItemResponse(
                id=str(i.id), snapshotId=str(i.snapshot_id), externalId=i.external_id,
                itemType=i.item_type, name=i.name, folderPath=i.folder_path,
                contentSize=i.content_size or 0, metadata=i.extra_data or {},
                isDeleted=i.is_deleted or False,
                createdAt=i.created_at.isoformat() if i.created_at else "",
            )
            for i in items
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
    )


# ============ Recovery: List resources with backups ============

@app.get("/api/v1/resources/with-backups")
async def list_resources_with_backups(
    tenantId: str = Query(...),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    db: AsyncSession = Depends(get_db),
):
    """List all resources for a tenant that have at least one completed snapshot."""
    # Get all resources for this tenant
    resources_stmt = (
        select(Resource)
        .where(Resource.tenant_id == UUID(tenantId))
        .order_by(Resource.display_name)
    )
    resources_result = await db.execute(resources_stmt)
    all_resources = resources_result.scalars().all()

    if not all_resources:
        return {
            "item_number": 0,
            "page_number": page,
            "next_page_token": None,
            "items": [],
        }

    resource_ids = [r.id for r in all_resources]

    # Get snapshot stats for these resources
    stats_stmt = (
        select(
            Snapshot.resource_id,
            func.count(Snapshot.id).label("snapshot_count"),
            func.sum(Snapshot.item_count).label("total_items"),
        )
        .where(Snapshot.status == SnapshotStatus.COMPLETE)
        .where(Snapshot.resource_id.in_(resource_ids))
        .group_by(Snapshot.resource_id)
    )
    stats_result = await db.execute(stats_stmt)
    stats = {}
    for s in stats_result.fetchall():
        stats[str(s[0])] = {"snapshot_count": s[1], "total_items": s[2] or 0}

    # Count and paginate
    total = len(stats)
    paginated_ids = list(stats.keys())[(page - 1) * size : page * size]

    def map_kind(t):
        m = {"MAILBOX": "office_user", "SHARED_MAILBOX": "shared_mailbox", "ROOM_MAILBOX": "room_mailbox",
             "ONEDRIVE": "onedrive", "SHAREPOINT_SITE": "sharepoint_site", "TEAMS_CHANNEL": "teams_channel",
             "TEAMS_CHAT": "teams_chat", "ENTRA_USER": "entra_user", "ENTRA_GROUP": "entra_group",
             "ENTRA_APP": "entra_app", "ENTRA_DEVICE": "entra_device", "AZURE_VM": "azure_vm",
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql"}
        return m.get(t, t.lower() if t else "unknown")

    resources_map = {str(r.id): r for r in all_resources}

    items = []
    for rid in paginated_ids:
        r = resources_map.get(rid)
        if not r:
            continue
        s = stats[rid]
        type_name = r.type.value if hasattr(r.type, 'value') else str(r.type)
        items.append({
            "id": str(r.id),
            "tenant_id": str(r.tenant_id),
            "kind": map_kind(type_name),
            "provider": "azure" if "AZURE" in (type_name or "") else "o365",
            "external_id": r.external_id,
            "name": r.display_name,
            "email": r.email,
            "data": r.extra_data or {},
            "storage_bytes": r.storage_bytes or 0,
            "last_backup_at": r.last_backup_at.isoformat() if r.last_backup_at else None,
            "last_backup_status": r.last_backup_status if r.last_backup_status else None,
            "snapshot_count": s["snapshot_count"],
            "total_items": s["total_items"],
        })

    has_next = (page * size) < total
    return {
        "item_number": total,
        "page_number": page,
        "next_page_token": str(page + 1) if has_next else None,
        "items": items,
    }


# ============ Recovery: Search items across snapshots ============

@app.get("/api/v1/resources/{resource_id}/snapshots/search")
async def search_snapshot_items(
    resource_id: str,
    query: Optional[str] = Query(None),
    item_type: Optional[str] = Query(None),
    folder_path: Optional[str] = Query(None),
    snapshot_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    db: AsyncSession = Depends(get_db),
):
    """Search snapshot items for a resource with optional filters."""
    snapshot_ids_stmt = select(Snapshot.id).where(
        Snapshot.resource_id == UUID(resource_id),
        Snapshot.status == SnapshotStatus.COMPLETE,
    )
    if snapshot_id:
        snapshot_ids_stmt = snapshot_ids_stmt.where(Snapshot.id == UUID(snapshot_id))
    snapshot_ids_result = await db.execute(snapshot_ids_stmt)
    snapshot_ids = [str(s.id) for s in snapshot_ids_result.scalars().all()]

    if not snapshot_ids:
        return SnapshotItemListResponse(
            content=[], totalPages=0, totalElements=0, size=size, number=page,
        )

    filters = [SnapshotItem.snapshot_id.in_([UUID(sid) for sid in snapshot_ids])]
    if item_type:
        filters.append(SnapshotItem.item_type == item_type)
    if folder_path:
        filters.append(SnapshotItem.folder_path == folder_path)
    if query:
        filters.append(
            or_(
                SnapshotItem.name.ilike(f"%{query}%"),
                SnapshotItem.external_id.ilike(f"%{query}%"),
            )
        )

    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    stmt = (
        select(SnapshotItem)
        .where(*filters)
        .order_by(SnapshotItem.created_at.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    items = result.scalars().all()

    return SnapshotItemListResponse(
        content=[
            SnapshotItemResponse(
                id=str(i.id), snapshotId=str(i.snapshot_id), externalId=i.external_id,
                itemType=i.item_type, name=i.name, folderPath=i.folder_path,
                contentSize=i.content_size or 0, metadata=i.extra_data or {},
                isDeleted=i.is_deleted or False,
                createdAt=i.created_at.isoformat() if i.created_at else "",
            )
            for i in items
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
    )
