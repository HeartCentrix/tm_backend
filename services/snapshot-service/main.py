"""Snapshot Service - Manages snapshots and snapshot items"""
from contextlib import asynccontextmanager
from typing import Optional, List
from uuid import UUID, uuid4
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, func, distinct, and_, or_

from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Snapshot, SnapshotItem, Resource, SnapshotType, SnapshotStatus
from shared.power_bi_snapshot import assemble_power_bi_items
from shared.schemas import (
    SnapshotListResponse, SnapshotResponse, SnapshotItemListResponse, SnapshotItemResponse, SnapshotDiff
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Snapshot Service", version="1.0.0", lifespan=lifespan)


def _item_to_response(item: SnapshotItem) -> SnapshotItemResponse:
    return SnapshotItemResponse(
        id=str(item.id),
        snapshotId=str(item.snapshot_id),
        externalId=item.external_id,
        itemType=item.item_type,
        name=item.name,
        folderPath=item.folder_path,
        contentSize=item.content_size or 0,
        metadata=item.extra_data or {},
        isDeleted=item.is_deleted or False,
        createdAt=item.created_at.isoformat() if item.created_at else "",
    )


async def _get_power_bi_assembled_items(db: AsyncSession, snapshot_id: str) -> Optional[List[SnapshotItem]]:
    snapshot = await db.get(Snapshot, UUID(snapshot_id))
    if not snapshot:
        return None

    resource = await db.get(Resource, snapshot.resource_id)
    if not resource:
        return None

    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
    if resource_type != "POWER_BI":
        return None

    snapshots_result = await db.execute(
        select(Snapshot).where(
            Snapshot.resource_id == snapshot.resource_id,
            Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
            Snapshot.created_at <= snapshot.created_at,
        ).order_by(Snapshot.created_at.asc())
    )
    snapshots = snapshots_result.scalars().all()
    if not snapshots:
        return []

    snapshot_ids = [s.id for s in snapshots]
    items_result = await db.execute(
        select(SnapshotItem).where(SnapshotItem.snapshot_id.in_(snapshot_ids)).order_by(SnapshotItem.created_at.asc())
    )
    return assemble_power_bi_items(snapshots, items_result.scalars().all(), up_to_snapshot_id=snapshot_id)


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
    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        filtered = [item for item in assembled_items if not item_type or item.item_type == item_type]
        counts: dict[str, int] = {}
        for item in filtered:
            path = item.folder_path or ""
            counts[path] = counts.get(path, 0) + 1
        return [{"path": path, "count": count} for path, count in sorted(counts.items())]

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


@app.get("/api/v1/resources/snapshots/{snapshot_id}/content-types")
async def get_snapshot_content_types(
    snapshot_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get distinct content types available in a snapshot."""
    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        return {"contentTypes": sorted({item.item_type for item in assembled_items if item.item_type})}

    stmt = (
        select(distinct(SnapshotItem.item_type))
        .where(SnapshotItem.snapshot_id == UUID(snapshot_id))
        .order_by(SnapshotItem.item_type)
    )
    result = await db.execute(stmt)
    types = [row[0] for row in result.fetchall() if row[0]]

    return {"contentTypes": types}


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
    itemType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        filtered = [item for item in assembled_items if not itemType or item.item_type == itemType]
        total = len(filtered)
        page_items = filtered[(page - 1) * size : page * size]
        return SnapshotItemListResponse(
            content=[_item_to_response(item) for item in page_items],
            totalPages=max(1, (total + size - 1) // size),
            totalElements=total,
            size=size,
            number=page,
        )

    filters = [SnapshotItem.snapshot_id == UUID(snapshot_id)]
    if itemType:
        filters.append(SnapshotItem.item_type == itemType)
    
    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    stmt = select(SnapshotItem).where(*filters).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    items = result.scalars().all()
    
    return SnapshotItemListResponse(
        content=[_item_to_response(item) for item in items],
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
    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        filtered = [item for item in assembled_items if not itemType or item.item_type == itemType]
        total = len(filtered)
        page_items = filtered[(page - 1) * size : page * size]
        return SnapshotItemListResponse(
            content=[_item_to_response(item) for item in page_items],
            totalPages=max(1, (total + size - 1) // size),
            totalElements=total,
            size=size,
            number=page,
        )

    filters = [SnapshotItem.snapshot_id == UUID(snapshot_id)]
    if itemType:
        filters.append(SnapshotItem.item_type == itemType)

    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    stmt = select(SnapshotItem).where(*filters).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    items = result.scalars().all()

    return SnapshotItemListResponse(
        content=[_item_to_response(item) for item in items],
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
        .where(Snapshot.status == SnapshotStatus.COMPLETED)
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
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql", "POWER_BI": "power_bi"}
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
    resource = await db.get(Resource, UUID(resource_id))
    resource_type = resource.type.value if resource and hasattr(resource.type, "value") else (str(resource.type) if resource else "")
    if resource and resource_type == "POWER_BI":
        target_snapshot_id = snapshot_id
        if not target_snapshot_id:
            latest_snapshot_result = await db.execute(
                select(Snapshot).where(
                    Snapshot.resource_id == UUID(resource_id),
                    Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
                ).order_by(Snapshot.created_at.desc()).limit(1)
            )
            latest_snapshot = latest_snapshot_result.scalar_one_or_none()
            target_snapshot_id = str(latest_snapshot.id) if latest_snapshot else None

        if not target_snapshot_id:
            return SnapshotItemListResponse(
                content=[], totalPages=0, totalElements=0, size=size, number=page,
            )

        assembled_items = await _get_power_bi_assembled_items(db, target_snapshot_id)
        filtered = assembled_items or []
        if item_type:
            filtered = [item for item in filtered if item.item_type == item_type]
        if folder_path:
            filtered = [item for item in filtered if item.folder_path == folder_path]
        if query:
            lowered = query.lower()
            filtered = [
                item for item in filtered
                if lowered in (item.name or "").lower() or lowered in (item.external_id or "").lower()
            ]
        total = len(filtered)
        page_items = filtered[(page - 1) * size : page * size]
        return SnapshotItemListResponse(
            content=[_item_to_response(item) for item in page_items],
            totalPages=max(1, (total + size - 1) // size),
            totalElements=total,
            size=size,
            number=page,
        )

    snapshot_ids_stmt = select(Snapshot.id).where(
        Snapshot.resource_id == UUID(resource_id),
        Snapshot.status == SnapshotStatus.COMPLETED,
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
        content=[_item_to_response(item) for item in items],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
    )


@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}", response_model=SnapshotItemResponse)
async def get_snapshot_item_detail(snapshot_id: str, item_id: str, db: AsyncSession = Depends(get_db)):
    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        item = next((row for row in assembled_items if str(row.id) == item_id), None)
        if not item:
            raise HTTPException(status_code=404, detail="Snapshot item not found")
        return _item_to_response(item)

    stmt = select(SnapshotItem).where(
        SnapshotItem.snapshot_id == UUID(snapshot_id),
        SnapshotItem.id == UUID(item_id),
    )
    result = await db.execute(stmt)
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Snapshot item not found")
    return _item_to_response(item)
