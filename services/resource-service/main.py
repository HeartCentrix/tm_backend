"""Resource Service - Manages resources and SLA policies"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone
from datetime import timedelta

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, func, or_, text

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Resource, SlaPolicy, ResourceType, ResourceStatus
from shared.schemas import (
    ResourceResponse, ResourceListResponse, UserResourceResponse,
    AssignPolicyRequest, BulkOperationRequest,
    SlaPolicyResponse, SlaPolicyCreateRequest
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


def format_bytes(bytes_val: int) -> str:
    if bytes_val < 1024**3:
        return f"{bytes_val / 1024**2:.1f} MB"
    return f"{bytes_val / 1024**3:.1f} GB"


app = FastAPI(title="Resource Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "resource"}


# ============ Resources ============

@app.get("/api/v1/resources")
async def list_resources(
    tenantId: str = Query(...),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    status: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    status_clause = "AND status = :rstatus" if status else ""
    query = text(f"""
        SELECT id, tenant_id, type, external_id, display_name, email, metadata, sla_policy_id, 
               status, storage_bytes, last_backup_at, created_at
        FROM resources 
        WHERE tenant_id = :rtenant {status_clause}
        ORDER BY created_at DESC
        LIMIT :rlimit OFFSET :roffset
    """)
    params = {"rtenant": tenantId, "rlimit": size, "roffset": (page - 1) * size}
    if status:
        params["rstatus"] = status
    
    result = await db.execute(query, params)
    rows = result.fetchall()
    
    # Count
    count_query = text(f"""
        SELECT count(*) FROM resources WHERE tenant_id = :rtenant {status_clause}
    """)
    count_result = await db.execute(count_query, params)
    total = count_result.scalar() or 0

    # Get SLA policy names
    sla_ids = [r[7] for r in rows if r[7]]
    policies = {}
    if sla_ids:
        policy_stmt = select(SlaPolicy).where(SlaPolicy.id.in_(sla_ids))
        policy_result = await db.execute(policy_stmt)
        policies = {str(p.id): p.name for p in policy_result.scalars().all()}

    def map_kind(t):
        m = {"MAILBOX": "office_user", "SHARED_MAILBOX": "shared_mailbox", "ROOM_MAILBOX": "room_mailbox",
             "ONEDRIVE": "onedrive", "SHAREPOINT_SITE": "sharepoint_site", "TEAMS_CHANNEL": "teams_channel",
             "TEAMS_CHAT": "teams_chat", "ENTRA_USER": "entra_user", "ENTRA_GROUP": "entra_group",
             "ENTRA_APP": "entra_app", "ENTRA_DEVICE": "entra_device", "AZURE_VM": "azure_vm",
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql"}
        return m.get(t, t.lower() if t else "unknown")

    def map_status(s):
        return {"ACTIVE": "protected", "ARCHIVED": "archived", "SUSPENDED": "suspended"}.get(s, "discovered")

    items = []
    for r in rows:
        items.append({
            "id": str(r[0]), "tenant_id": str(r[1]), "owner": None,
            "kind": map_kind(r[2]),
            "provider": "azure" if r[2] and "AZURE" in r[2] else "o365",
            "external_id": r[3], "name": r[4], "email": r[5],
            "data": r[6] or {},
            "archived": r[8] == "ARCHIVED", "deleted": r[8] == "PENDING_DELETION",
            "protections": [{"policy_id": str(r[7])}] if r[7] else None,
            "usage": {"resource_id": str(r[0]), "tenant_id": str(r[1]), "backups": 0,
                      "size": r[9] or 0, "size_delta_year": 0, "size_delta_month": 0, "size_delta_week": 0},
            "status": map_status(r[8]),
            "sla": policies.get(str(r[7])) if r[7] else None,
            "last_backup": r[10].isoformat() if r[10] else None,
            "group_ids": [],
        })

    has_next = (page * size) < total
    return {"item_number": total, "page_number": page, "next_page_token": str(page + 1) if has_next else None, "items": items}


@app.get("/api/v1/resources/search")
async def search_resources(query: str = Query(...), type: Optional[str] = Query(None), db: AsyncSession = Depends(get_db)):
    filters = [or_(Resource.display_name.ilike(f"%{query}%"), Resource.email.ilike(f"%{query}%"))]
    if type:
        filters.append(Resource.type == type)
    stmt = select(Resource).where(*filters).limit(50)
    result = await db.execute(stmt)
    return [
        ResourceResponse(id=str(r.id), name=r.display_name, email=r.email,
                        type=r.type.value if hasattr(r.type, 'value') else str(r.type),
                        totalSize=format_bytes(r.storage_bytes or 0))
        for r in result.scalars().all()
    ]


@app.get("/api/v1/resources/by-type")
async def get_resources_by_type(type: str = Query(...), tenantId: Optional[str] = Query(None), page: int = Query(1, ge=1), size: int = Query(50, ge=1), db: AsyncSession = Depends(get_db)):
    query = text(f"""
        SELECT id, tenant_id, type, external_id, display_name, email, metadata, sla_policy_id,
               status, storage_bytes, last_backup_at, created_at
        FROM resources
        WHERE type = :rtype
        {'AND tenant_id = :rtenant' if tenantId else ''}
        ORDER BY created_at DESC
        LIMIT :rlimit OFFSET :roffset
    """)
    params = {"rtype": type, "rlimit": size, "roffset": (page - 1) * size}
    if tenantId:
        params["rtenant"] = tenantId

    result = await db.execute(query, params)
    rows = result.fetchall()

    count_query = text(f"""
        SELECT count(*) FROM resources WHERE type = :rtype
        {'AND tenant_id = :rtenant' if tenantId else ''}
    """)
    count_result = await db.execute(count_query, params)
    total = count_result.scalar() or 0

    def map_kind(t):
        m = {"MAILBOX": "office_user", "SHARED_MAILBOX": "shared_mailbox", "ROOM_MAILBOX": "room_mailbox",
             "ONEDRIVE": "onedrive", "SHAREPOINT_SITE": "sharepoint_site", "TEAMS_CHANNEL": "teams_channel",
             "TEAMS_CHAT": "teams_chat", "ENTRA_USER": "entra_user", "ENTRA_GROUP": "entra_group",
             "ENTRA_APP": "entra_app", "ENTRA_DEVICE": "entra_device", "AZURE_VM": "azure_vm",
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql"}
        return m.get(t, t.lower() if t else "unknown")

    items = []
    for row in rows:
        items.append({
            "id": str(row[0]), "tenant_id": str(row[1]), "owner": None,
            "kind": map_kind(row[2]),
            "provider": "azure" if "AZURE" in (row[2] or "") else "o365",
            "external_id": row[3], "name": row[4], "email": row[5],
            "data": row[6] or {},
            "archived": row[8] == "ARCHIVED", "deleted": row[8] == "PENDING_DELETION",
            "protections": None,
            "usage": {"resource_id": str(row[0]), "tenant_id": str(row[1]), "backups": 0,
                      "size": row[9] or 0, "size_delta_year": 0, "size_delta_month": 0, "size_delta_week": 0},
            "status": "protected" if row[8] == "ACTIVE" else "discovered",
            "sla": None, "last_backup": row[10].isoformat() if row[10] else None,
            "group_ids": [],
        })

    has_next = (page * size) < total
    return {"item_number": total, "page_number": page, "next_page_token": str(page + 1) if has_next else None, "items": items}


@app.get("/api/v1/resources/users")
async def get_users_with_workloads(tenantId: str = Query(...), db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(
        Resource.tenant_id == UUID(tenantId),
        Resource.type.in_([ResourceType.MAILBOX, ResourceType.SHARED_MAILBOX, ResourceType.ONEDRIVE, ResourceType.TEAMS_CHAT])
    )
    result = await db.execute(stmt)
    resources = result.scalars().all()

    users_map = {}
    for r in resources:
        email = r.email or f"unknown-{r.id}"
        if email not in users_map:
            users_map[email] = {"id": str(r.id), "tenantId": str(r.tenant_id), "email": email, "displayName": r.display_name, "resources": []}
        users_map[email]["resources"].append(r)

    return [
        UserResourceResponse(
            id=v["id"], tenantId=v["tenantId"], email=v["email"], displayName=v["displayName"],
            hasMailbox=any("MAILBOX" in (r.type.value if hasattr(r.type, 'value') else str(r.type)) for r in v["resources"]),
            hasOneDrive=any("ONEDRIVE" in (r.type.value if hasattr(r.type, 'value') else str(r.type)) for r in v["resources"]),
            hasTeamsChat=any("TEAMS" in (r.type.value if hasattr(r.type, 'value') else str(r.type)) for r in v["resources"]),
        )
        for v in users_map.values()
    ]


@app.get("/api/v1/resources/{resource_id}", response_model=ResourceResponse)
async def get_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    return ResourceResponse(
        id=str(resource.id), name=resource.display_name, email=resource.email,
        type=resource.type.value if hasattr(resource.type, 'value') else str(resource.type),
        totalSize=format_bytes(resource.storage_bytes or 0),
        status=resource.status.value if hasattr(resource.status, 'value') else str(resource.status),
        tenantId=str(resource.tenant_id),
    )


@app.get("/api/v1/resources/{resource_id}/storage-history")
async def get_storage_history(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    current_size = resource.storage_bytes or 0
    return [{"date": (datetime.now(timezone.utc) - timedelta(days=29-i)).date().isoformat(), "size": int(current_size * (0.5 + 0.5 * (i / 30)))} for i in range(30)]


@app.post("/api/v1/resources/{resource_id}/assign-policy", status_code=204)
async def assign_policy(resource_id: str, request: AssignPolicyRequest, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.sla_policy_id = UUID(request.policyId)
    resource.status = ResourceStatus.ACTIVE
    await db.flush()


@app.post("/api/v1/resources/{resource_id}/unassign-policy", status_code=204)
async def unassign_policy(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.sla_policy_id = None
    await db.flush()


@app.post("/api/v1/resources/{resource_id}/archive", status_code=204)
async def archive_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.ARCHIVED
    await db.flush()


@app.post("/api/v1/resources/{resource_id}/unarchive", status_code=204)
async def unarchive_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.ACTIVE
    await db.flush()


@app.delete("/api/v1/resources/{resource_id}", status_code=204)
async def delete_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.PENDING_DELETION
    await db.flush()


@app.post("/api/v1/resources/bulk-assign-policy", status_code=204)
@app.post("/api/v1/resources/bulk-archive", status_code=204)
async def bulk_action(request: BulkOperationRequest, db: AsyncSession = Depends(get_db)):
    pass


# ============ SLA Policies ============

@app.get("/api/v1/policies")
async def list_policies(tenantId: Optional[str] = Query(None), db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).order_by(SlaPolicy.created_at.desc())
    if tenantId:
        stmt = stmt.where(SlaPolicy.tenant_id == UUID(tenantId))
    result = await db.execute(stmt)
    policies = result.scalars().all()
    return [
        SlaPolicyResponse(
            id=str(p.id), tenantId=str(p.tenant_id), name=p.name, tier=p.tier,
            frequency=p.frequency, retentionType=p.retention_type,
            createdAt=p.created_at.isoformat() if p.created_at else "",
        )
        for p in policies
    ]


@app.get("/api/v1/policies/{policy_id}", response_model=SlaPolicyResponse)
async def get_policy(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).where(SlaPolicy.id == UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    return SlaPolicyResponse(
        id=str(policy.id), tenantId=str(policy.tenant_id), name=policy.name,
        tier=policy.tier, frequency=policy.frequency, retentionType=policy.retention_type,
        createdAt=policy.created_at.isoformat(),
    )


@app.post("/api/v1/policies", response_model=SlaPolicyResponse)
async def create_policy(request: SlaPolicyCreateRequest, db: AsyncSession = Depends(get_db)):
    policy = SlaPolicy(
        id=uuid4(), tenant_id=UUID(request.tenantId), name=request.name,
        tier=request.tier, frequency=request.frequency, retention_type=request.retentionType,
        enabled=request.enabled if request.enabled is not None else True,
    )
    db.add(policy)
    await db.flush()
    return SlaPolicyResponse(
        id=str(policy.id), tenantId=str(policy.tenant_id), name=policy.name,
        tier=policy.tier, frequency=policy.frequency, retentionType=policy.retention_type,
        createdAt=policy.created_at.isoformat(),
    )


@app.put("/api/v1/policies/{policy_id}", response_model=SlaPolicyResponse)
async def update_policy(policy_id: str, request: dict, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).where(SlaPolicy.id == UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    for key, value in request.items():
        if hasattr(policy, key):
            setattr(policy, key, value)
    policy.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.flush()
    return SlaPolicyResponse(
        id=str(policy.id), tenantId=str(policy.tenant_id), name=policy.name,
        tier=policy.tier, frequency=policy.frequency, retentionType=policy.retention_type,
        createdAt=policy.created_at.isoformat(),
    )


@app.delete("/api/v1/policies/{policy_id}", status_code=204)
async def delete_policy(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).where(SlaPolicy.id == UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    await db.delete(policy)
    await db.flush()


@app.get("/api/v1/policies/{policy_id}/resources")
async def get_policy_resources(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.sla_policy_id == UUID(policy_id))
    result = await db.execute(stmt)
    resources = result.scalars().all()
    return {"content": [{"id": str(r.id), "name": r.display_name, "type": r.type.value, "assignedAt": r.created_at.isoformat()} for r in resources], "totalPages": 1, "totalElements": len(resources)}


@app.post("/api/v1/policies/{policy_id}/auto-assign", status_code=204)
async def auto_assign_policy(policy_id: str, request: dict):
    pass
