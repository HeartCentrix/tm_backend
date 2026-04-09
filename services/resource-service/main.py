"""Resource Service - Manages resources and SLA policies"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone
from datetime import timedelta

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, func, or_

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
    db: AsyncSession = Depends(get_db),
):
    filters = [Resource.tenant_id == UUID(tenantId)]
    count_stmt = select(func.count(Resource.id)).where(*filters)
    total = (await db.execute(count_stmt)).scalar() or 0
    
    stmt = select(Resource).where(*filters).order_by(Resource.created_at.desc()).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    resources = result.scalars().all()
    
    return ResourceListResponse(
        content=[
            ResourceResponse(
                id=str(r.id), name=r.display_name, email=r.email,
                type=r.type.value if hasattr(r.type, 'value') else str(r.type),
                totalSize=format_bytes(r.storage_bytes or 0),
                lastBackup=r.last_backup_at.isoformat() if r.last_backup_at else None,
                status=r.status.value if hasattr(r.status, 'value') else str(r.status),
                tenantId=str(r.tenant_id),
                archived=r.status == ResourceStatus.ARCHIVED if r.status else False,
            )
            for r in resources
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
        first=page == 1,
        last=page >= (total + size - 1) // size,
    )


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
async def get_resources_by_type(type: str = Query(...), tenantId: Optional[str] = Query(None), db: AsyncSession = Depends(get_db)):
    filters = [Resource.type == type]
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    stmt = select(Resource).where(*filters)
    result = await db.execute(stmt)
    return [
        ResourceResponse(id=str(r.id), name=r.display_name, email=r.email,
                        type=r.type.value if hasattr(r.type, 'value') else str(r.type),
                        totalSize=format_bytes(r.storage_bytes or 0))
        for r in result.scalars().all()
    ]


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
@app.post("/api/v1/resources/{resource_id}/archive", status_code=204)
@app.post("/api/v1/resources/{resource_id}/unarchive", status_code=204)
async def resource_action(resource_id: str, db: AsyncSession = Depends(get_db)):
    pass


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
