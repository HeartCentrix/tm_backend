import asyncio
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
from shared.azure_storage import azure_storage_manager, workload_candidates_for_resource_type
from shared.schemas import (
    SnapshotListResponse, SnapshotResponse, SnapshotItemListResponse, SnapshotItemResponse, SnapshotDiff
)
import logging

logger = logging.getLogger(__name__)


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


# ==================== Content-Type-Specific Endpoints ====================


async def _load_blob_context(db: AsyncSession, snapshot_id: str):
    """Load the snapshot + resource + prepared shard/workload hints needed to read blobs
    for every item in this snapshot. Returns (shard, tenant_id, candidates) or (None, None, ()).

    Shard is keyed by resource_id (matching the worker's write-side call).
    Candidates is an ordered tuple of workload suffixes for get_container_name."""
    snapshot = await db.get(Snapshot, UUID(snapshot_id))
    if not snapshot:
        return None, None, ()
    resource = await db.get(Resource, snapshot.resource_id)
    if not resource or not azure_storage_manager.shards:
        return None, str(resource.tenant_id) if resource else None, ()
    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
    shard = azure_storage_manager.get_shard_for_resource(
        str(resource.id), str(resource.tenant_id)
    )
    candidates = workload_candidates_for_resource_type(resource_type)
    return shard, str(resource.tenant_id), candidates


async def _download_item_blob(shard, tenant_id: str, candidates: tuple, blob_path: str) -> Optional[bytes]:
    """Try each candidate container in order, returning the first blob found.
    Returns None if the blob is absent from every candidate container or if no
    shard/tenant/candidates are available. Logs a warning if nothing was found."""
    if not shard or not tenant_id or not blob_path or not candidates:
        return None
    tried: List[str] = []
    for workload in candidates:
        container = azure_storage_manager.get_container_name(tenant_id, workload)
        tried.append(container)
        try:
            data = await shard.download_blob(container, blob_path)
        except Exception as exc:
            logger.warning("download_blob error on %s/%s: %s", container, blob_path, exc)
            continue
        if data is not None:
            return data
    logger.warning("blob not found in any candidate container %s for %s", tried, blob_path)
    return None


async def _read_blob_json(shard, tenant_id: str, candidates: tuple, blob_path: str) -> dict:
    """Thin JSON wrapper around _download_item_blob. Returns {} on miss or decode failure."""
    import json as _j
    data = await _download_item_blob(shard, tenant_id, candidates, blob_path)
    if not data:
        return {}
    try:
        return _j.loads(data.decode("utf-8"))
    except Exception as exc:
        logger.warning("blob at %s is not UTF-8 JSON: %s", blob_path, exc)
        return {}


def _raw(item) -> dict:
    meta = item.extra_data or {}
    r = meta.get("raw") or meta.get("structured") or meta
    return r if isinstance(r, dict) else {}


@app.get("/api/v1/resources/snapshots/{snapshot_id}/emails")
async def list_snapshot_emails(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1),
    db: AsyncSession = Depends(get_db),
):
    """Return emails with from/to/cc/subject/body/date extracted from metadata."""
    filters = [
        SnapshotItem.snapshot_id == UUID(snapshot_id),
        SnapshotItem.item_type == "EMAIL",
    ]
    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    items = (await db.execute(select(SnapshotItem).where(*filters).offset((page-1)*size).limit(size))).scalars().all()

    def fmt(i):
        raw = _raw(i)
        from_addr = raw.get("from", {}).get("emailAddress", {})
        to_list = [r.get("emailAddress", {}) for r in raw.get("toRecipients", [])]
        cc_list = [r.get("emailAddress", {}) for r in raw.get("ccRecipients", [])]
        return {
            "id": str(i.id),
            "snapshotId": str(i.snapshot_id),
            "externalId": i.external_id,
            "itemType": i.item_type,
            "subject": raw.get("subject") or i.name,
            "from": f"{from_addr.get('name', '')} <{from_addr.get('address', '')}>".strip(" <>") or None,
            "to": "; ".join(f"{r.get('name','')} <{r.get('address','')}>".strip(" <>") for r in to_list),
            "cc": "; ".join(f"{r.get('name','')} <{r.get('address','')}>".strip(" <>") for r in cc_list),
            "date": raw.get("sentDateTime") or raw.get("receivedDateTime") or (i.created_at.isoformat() if i.created_at else None),
            "bodyPreview": raw.get("bodyPreview") or "",
            "body": raw.get("body", {}).get("content") or "",
            "bodyContentType": raw.get("body", {}).get("contentType") or "text",
            "hasAttachments": raw.get("hasAttachments", False),
            "attachments": raw.get("attachments", []),
            "folderPath": i.folder_path,
            "contentSize": i.content_size or 0,
            "isDeleted": i.is_deleted or False,
            "createdAt": i.created_at.isoformat() if i.created_at else "",
            "name": raw.get("subject") or i.name or "",
            "metadata": {"raw": raw},
        }

    return {"content": [fmt(i) for i in items], "totalElements": total, "totalPages": max(1, (total+size-1)//size), "size": size, "number": page}


@app.get("/api/v1/resources/snapshots/{snapshot_id}/messages")
async def list_snapshot_messages(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1),
    db: AsyncSession = Depends(get_db),
):
    """Return Teams messages with sender/body/date extracted from metadata."""
    CHAT_TYPES = ["TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"]
    filters = [
        SnapshotItem.snapshot_id == UUID(snapshot_id),
        SnapshotItem.item_type.in_(CHAT_TYPES),
    ]
    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    items = (await db.execute(select(SnapshotItem).where(*filters).offset((page-1)*size).limit(size))).scalars().all()

    shard, tenant_id, candidates = await _load_blob_context(db, snapshot_id)
    sem = asyncio.Semaphore(20)

    async def fmt(i):
        raw = _raw(i)
        # Read blob concurrently if metadata is empty
        if (not raw or len(raw) <= 2) and i.blob_path:
            async with sem:
                raw = await _read_blob_json(shard, tenant_id, candidates, i.blob_path)
        if not isinstance(raw, dict): raw = {}
        _from = raw.get("from") or {}
        sender_obj = (_from.get("user") or _from.get("application") or {}) if isinstance(_from, dict) else {}
        body_obj = raw.get("body", {})
        # Fallback: name column stores the body text
        body_text = body_obj.get("content") or (i.name if i.name and i.name != "<systemEventMessage/>" else "")
        return {
            "id": str(i.id),
            "snapshotId": str(i.snapshot_id),
            "externalId": i.external_id,
            "itemType": i.item_type,
            "sender": sender_obj.get("displayName") or "",
            "senderEmail": sender_obj.get("userPrincipalName") or sender_obj.get("email") or "",
            "body": body_text,
            "bodyContentType": body_obj.get("contentType") or "html",
            "date": raw.get("createdDateTime") or (i.created_at.isoformat() if i.created_at else None),
            "chatTopic": (i.extra_data or {}).get("chatTopic") or "",
            "channelName": (i.extra_data or {}).get("channelName") or "",
            "folderPath": i.folder_path,
            "attachments": raw.get("attachments", []),
            "mentions": raw.get("mentions", []),
            "isDeleted": bool(raw.get("deletedDateTime")),
            "isReply": i.item_type == "TEAMS_MESSAGE_REPLY",
            "contentSize": i.content_size or 0,
            "createdAt": i.created_at.isoformat() if i.created_at else "",
            "name": sender_obj.get("displayName") or "",
            "metadata": {"raw": raw},
        }

    results = await asyncio.gather(*[fmt(i) for i in items])
    return {"content": list(results), "totalElements": total, "totalPages": max(1, (total+size-1)//size), "size": size, "number": page}


@app.get("/api/v1/resources/snapshots/{snapshot_id}/calendar")
async def list_snapshot_calendar(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1),
    db: AsyncSession = Depends(get_db),
):
    """Return calendar events with start/end/location/attendees extracted from metadata."""
    filters = [
        SnapshotItem.snapshot_id == UUID(snapshot_id),
        SnapshotItem.item_type == "CALENDAR_EVENT",
    ]
    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    items = (await db.execute(select(SnapshotItem).where(*filters).offset((page-1)*size).limit(size))).scalars().all()

    shard, tenant_id, candidates = await _load_blob_context(db, snapshot_id)
    sem = asyncio.Semaphore(20)

    async def fmt(i):
        raw = _raw(i)
        # Read from blob concurrently if metadata is empty
        if (not raw or len(raw) <= 2) and i.blob_path:
            async with sem:
                raw = await _read_blob_json(shard, tenant_id, candidates, i.blob_path)
        if not isinstance(raw, dict):
            raw = {}

        organizer_obj = (raw.get("organizer") or {}).get("emailAddress") or {}
        start_obj = raw.get("start") or {}
        end_obj = raw.get("end") or {}
        subject = raw.get("subject") or raw.get("name") or ""
        graph_type = raw.get("type") or ""  # singleInstance, occurrence, seriesMaster, exception

        # Build display name from available fields
        if not subject:
            start_dt = start_obj.get("dateTime") or start_obj.get("date") or ""
            if start_dt:
                try:
                    from datetime import datetime as _dt
                    parsed = _dt.fromisoformat(start_dt.replace("Z", "+00:00").split(".")[0])
                    time_str = parsed.strftime("%-I:%M %p") if not start_obj.get("date") else parsed.strftime("%b %-d")
                    subject = f"Event · {time_str}"
                except Exception:
                    subject = "Calendar Event"
            else:
                subject = "Calendar Event"

        # Derive a human-readable eventType label for filter sidebar
        is_cancelled = raw.get("isCancelled", False) or subject.lower().startswith("canceled") or subject.lower().startswith("cancelled")
        is_all_day = raw.get("isAllDay", False) or (not start_obj.get("dateTime") and bool(start_obj.get("date")))
        is_online = raw.get("isOnlineMeeting", False)
        has_recurrence = bool(raw.get("recurrence")) or graph_type in ("seriesMaster", "occurrence")
        has_attendees = bool(raw.get("attendees"))
        if is_cancelled:
            event_type = "Cancelled"
        elif graph_type == "seriesMaster":
            event_type = "Recurring Series"
        elif graph_type == "occurrence":
            event_type = "Recurring"
        elif graph_type == "exception":
            event_type = "Exception"
        elif is_all_day:
            event_type = "All Day"
        elif is_online:
            event_type = "Online Meeting"
        elif has_recurrence:
            event_type = "Recurring"
        elif has_attendees:
            event_type = "Meeting"
        else:
            event_type = "Appointment"

        return {
            "id": str(i.id),
            "snapshotId": str(i.snapshot_id),
            "externalId": i.external_id,
            "itemType": i.item_type,
            "subject": subject or i.name or "",
            "start": start_obj.get("dateTime") or start_obj.get("date"),
            "end": end_obj.get("dateTime") or end_obj.get("date"),
            "timeZone": start_obj.get("timeZone") or "UTC",
            "isAllDay": is_all_day,
            "isCancelled": is_cancelled,
            "location": (raw.get("location") or {}).get("displayName") or "",
            "organizer": organizer_obj.get("name") or organizer_obj.get("address") or None,
            "organizerEmail": organizer_obj.get("address") or "",
            "attendees": raw.get("attendees") or [],
            "body": (raw.get("body") or {}).get("content") or "",
            "bodyContentType": (raw.get("body") or {}).get("contentType") or "text",
            "isOnlineMeeting": is_online,
            "recurrence": raw.get("recurrence"),
            "recurrenceType": (raw.get("recurrence") or {}).get("pattern", {}).get("type") or None,
            "graphType": graph_type,
            "showAs": raw.get("showAs") or "",
            "importance": raw.get("importance") or "normal",
            "sensitivity": raw.get("sensitivity") or "normal",
            "categories": raw.get("categories") or [],
            "eventType": event_type,
            "folderPath": i.folder_path or "Calendar",
            "contentSize": i.content_size or 0,
            "isDeleted": i.is_deleted or False,
            "createdAt": i.created_at.isoformat() if i.created_at else "",
            "name": subject or i.name or "",
            "date": start_obj.get("dateTime") or start_obj.get("date") or "",
            "metadata": {"raw": raw},
        }

    results = await asyncio.gather(*[fmt(i) for i in items])
    return {"content": list(results), "totalElements": total, "totalPages": max(1, (total+size-1)//size), "size": size, "number": page}



@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}/content")
async def get_item_content(
    snapshot_id: str,
    item_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return the raw content of a snapshot item, reading from blob if metadata is empty.

    Blob location: the worker writes to container=backup-{workload}-{tenant[:8]}
    using blob_path={tenant}/{resource}/{snapshot}/{ts}/{item}. We derive the
    workload from the resource type (via workload_candidates_for_resource_type)
    rather than parsing blob_path, and we pick the shard by resource_id to match
    the worker's write call."""
    item = await db.get(SnapshotItem, UUID(item_id))
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    if item.blob_path and azure_storage_manager.shards:
        shard, tenant_id, candidates = await _load_blob_context(db, snapshot_id)
        try:
            data = await _download_item_blob(shard, tenant_id, candidates, item.blob_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Blob read failed: {e}")
        if data:
            import json as _json
            try:
                return {"source": "blob", "content": _json.loads(data.decode("utf-8"))}
            except Exception:
                from fastapi.responses import Response
                return Response(content=data, media_type="application/octet-stream")

    # Fall back to inline metadata (e.g. email bodies stored directly in extra_data)
    meta = item.extra_data or {}
    raw = meta.get("raw") or meta.get("structured")
    if raw:
        return {"source": "metadata", "content": raw}

    return {"source": "none", "content": {}}
