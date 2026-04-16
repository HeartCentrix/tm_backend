import asyncio
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


@app.get("/api/v1/resources/snapshots/{snapshot_id}/content-types")
async def get_snapshot_content_types(
    snapshot_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get distinct content types available in a snapshot."""
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


# ==================== Content-Type-Specific Endpoints ====================

def _get_blob_client():
    from shared.config import settings as _s
    from azure.storage.blob import BlobServiceClient as _BSC
    if _s.AZURE_STORAGE_ACCOUNT_NAME and _s.AZURE_STORAGE_ACCOUNT_KEY:
        conn = (f"DefaultEndpointsProtocol=https;AccountName={_s.AZURE_STORAGE_ACCOUNT_NAME};"
                f"AccountKey={_s.AZURE_STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net")
        return _BSC.from_connection_string(conn)
    return None


async def _read_blob(blob_path: str) -> dict:
    import json as _j, asyncio
    client = _get_blob_client()
    if not client or not blob_path:
        return {}
    try:
        # blob_path = {tenant_id}/{resource_id}/{snapshot_id}/{date}/{item_id}
        # Container = backup-{workload}-{tenant_id[:8]}, blob key = full blob_path
        parts = blob_path.split("/")
        tenant_short = parts[0][:8] if parts else ""
        for container in [f"backup-mailbox-{tenant_short}", f"backup-files-{tenant_short}",
                          f"backup-teams-{tenant_short}", f"backup-entra-{tenant_short}"]:
            try:
                blob = client.get_blob_client(container=container, blob=blob_path)
                data = await asyncio.get_event_loop().run_in_executor(None, lambda b=blob: b.download_blob().readall())
                return _j.loads(data.decode("utf-8"))
            except Exception:
                continue
        return {}
    except Exception:
        return {}


def _raw(item) -> dict:
    meta = item.extra_data or {}
    r = meta.get("raw") or meta.get("structured") or meta
    return r if isinstance(r, dict) else {}


@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}/content")
async def get_item_content(snapshot_id: str, item_id: str, db: AsyncSession = Depends(get_db)):
    item = await db.get(SnapshotItem, UUID(item_id))
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    raw = _raw(item)
    if raw and len(raw) > 2:
        return {"source": "metadata", "content": raw}
    if item.blob_path:
        blob_content = await _read_blob(item.blob_path)
        if blob_content:
            return {"source": "blob", "content": blob_content}
    return {"source": "none", "content": {}}

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

    sem = asyncio.Semaphore(20)

    async def fmt(i):
        raw = _raw(i)
        # Read blob concurrently if metadata is empty
        if (not raw or len(raw) <= 2) and i.blob_path:
            async with sem:
                raw = await _read_blob(i.blob_path) or {}
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

    sem = asyncio.Semaphore(20)

    async def fmt(i):
        raw = _raw(i)
        # Read from blob concurrently if metadata is empty
        if (not raw or len(raw) <= 2) and i.blob_path:
            async with sem:
                raw = await _read_blob(i.blob_path) or {}
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
    """Return the raw content of a snapshot item, reading from blob if metadata is empty."""
    from shared.azure_storage import azure_storage_manager
    item = await db.get(SnapshotItem, UUID(item_id))
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    # For items with a blob_path, the actual content lives in blob storage.
    # extra_data["structured"] only holds file properties (size, mime_type, etc.),
    # not the file content, so we must read from blob first.
    if item.blob_path and azure_storage_manager.shards:
        try:
            shard = azure_storage_manager.get_shard_for_resource(
                str(item.snapshot_id), str(item.tenant_id or "")
            )
            parts = item.blob_path.split("/", 1)
            container, path = (parts[0], parts[1]) if len(parts) == 2 else ("files", item.blob_path)
            data = await shard.download_blob(container, path)
            if data:
                import json as _json
                try:
                    content = _json.loads(data.decode("utf-8"))
                    return {"source": "blob", "content": content}
                except Exception:
                    from fastapi.responses import Response
                    return Response(content=data, media_type="application/octet-stream")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Blob read failed: {e}")

    # Fall back to inline metadata (e.g. email bodies stored directly in extra_data)
    meta = item.extra_data or {}
    raw = meta.get("raw") or meta.get("structured")
    if raw:
        return {"source": "metadata", "content": raw}

    return {"source": "none", "content": {}}
