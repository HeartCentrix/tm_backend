"""Resource Service - Manages resources and SLA policies"""
from contextlib import asynccontextmanager
from typing import Optional, Iterable
from uuid import UUID, uuid4
from datetime import datetime, timezone
from datetime import timedelta
import httpx

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, func, or_, text

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Resource, SlaPolicy, ResourceType, ResourceStatus, Tenant, TenantType
from shared.schemas import (
    ResourceResponse, ResourceListResponse, UserResourceResponse,
    AssignPolicyRequest, BulkOperationRequest,
    BulkAssignRequest, BulkUnassignRequest,
    SlaPolicyResponse, SlaPolicyCreateRequest
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


# Resource types hidden from UI listing endpoints by default.
# Rationale: POWER_BI workspaces share identity with the M365 Group that backs them,
# so they show up in the Power Platform tab looking like duplicates of Teams/SharePoint/
# Entra rows with the same display name and email. Discovery and backup still process
# these rows; they just don't appear in list/search/by-type responses unless the caller
# passes ?includeHidden=true.
UI_HIDDEN_TYPES = {"POWER_BI"}


def format_bytes(bytes_val: int) -> str:
    if bytes_val < 1024**3:
        return f"{bytes_val / 1024**2:.1f} MB"
    return f"{bytes_val / 1024**3:.1f} GB"


async def notify_scheduler_reschedule():
    """Notify the backup Scheduler to reschedule all SLA policy jobs"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post("http://backup-scheduler:8008/scheduler/reschedule-all")
    except Exception as e:
        print(f"[resource-service] Failed to notify scheduler: {e}")


app = FastAPI(title="Resource Service", version="1.0.0", lifespan=lifespan)


USER_LINKED_TYPES = {
    ResourceType.ENTRA_USER,
    ResourceType.MAILBOX,
    ResourceType.SHARED_MAILBOX,
    ResourceType.ROOM_MAILBOX,
    ResourceType.ONEDRIVE,
    ResourceType.TODO,
    ResourceType.ONENOTE,
}

GROUP_LINKED_TYPES = {
    ResourceType.ENTRA_GROUP,
    ResourceType.DYNAMIC_GROUP,
    ResourceType.PLANNER,
    ResourceType.TEAMS_CHANNEL,
}

VALID_POLICY_SERVICE_TYPES = {"m365", "azure"}
AZURE_POLICY_RESOURCE_TYPES = {
    ResourceType.AZURE_VM,
    ResourceType.AZURE_SQL_DB,
    ResourceType.AZURE_POSTGRESQL,
    ResourceType.AZURE_POSTGRESQL_SINGLE,
    ResourceType.RESOURCE_GROUP,
}


def normalize_policy_service_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized not in VALID_POLICY_SERVICE_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported serviceType '{value}'")
    return normalized


def tenant_policy_service_type(tenant: Tenant) -> str:
    tenant_type = tenant.type.value if hasattr(tenant.type, "value") else str(tenant.type or "")
    return "azure" if tenant_type.upper() == TenantType.AZURE.value else "m365"


def resource_policy_service_type(resource: Resource) -> str:
    return "azure" if resource.type in AZURE_POLICY_RESOURCE_TYPES else "m365"


async def validate_policy_scope(
    db: AsyncSession,
    *,
    policy_id: UUID,
    tenant_id: UUID,
    resources: Iterable[Resource],
) -> SlaPolicy:
    policy = await db.get(SlaPolicy, policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    if policy.tenant_id != tenant_id:
        raise HTTPException(status_code=400, detail="Policy belongs to a different tenant")

    policy_service_type = normalize_policy_service_type(getattr(policy, "service_type", None)) or "m365"
    tenant = await db.get(Tenant, tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    resource_service_types = set()
    for resource in resources:
        if resource.tenant_id != tenant_id:
            raise HTTPException(status_code=400, detail="All resources must belong to the same tenant as the policy")
        resource_service_types.add(resource_policy_service_type(resource))

    if len(resource_service_types) > 1:
        raise HTTPException(status_code=400, detail="Resources must belong to a single service type")

    target_service_type = next(iter(resource_service_types), tenant_policy_service_type(tenant))
    if policy_service_type != target_service_type:
        raise HTTPException(
            status_code=400,
            detail=f"{policy_service_type.upper()} SLA policies can't be assigned to {target_service_type.upper()} resources",
        )

    return policy


def _resource_user_key(resource: Resource) -> Optional[str]:
    if resource.type in {
        ResourceType.ENTRA_USER,
        ResourceType.MAILBOX,
        ResourceType.SHARED_MAILBOX,
        ResourceType.ROOM_MAILBOX,
        ResourceType.TODO,
        ResourceType.ONENOTE,
    }:
        return resource.external_id
    if resource.type == ResourceType.ONEDRIVE:
        return (resource.extra_data or {}).get("user_id")
    return None


def _resource_group_key(resource: Resource) -> Optional[str]:
    if resource.type in {
        ResourceType.ENTRA_GROUP,
        ResourceType.DYNAMIC_GROUP,
        ResourceType.PLANNER,
        ResourceType.TEAMS_CHANNEL,
    }:
        return resource.external_id
    return None


async def expand_linked_policy_scope(db: AsyncSession, seed_resources: Iterable[Resource]) -> list[Resource]:
    seed_resources = list(seed_resources)
    if not seed_resources:
        return []

    tenant_ids = {resource.tenant_id for resource in seed_resources}
    candidate_types = list(USER_LINKED_TYPES | GROUP_LINKED_TYPES)
    result = await db.execute(
        select(Resource).where(
            Resource.tenant_id.in_(tenant_ids),
            Resource.type.in_(candidate_types),
        )
    )
    candidates = result.scalars().all()

    user_keys = {
        (resource.tenant_id, key)
        for resource in seed_resources
        for key in [_resource_user_key(resource)]
        if key
    }
    group_keys = {
        (resource.tenant_id, key)
        for resource in seed_resources
        for key in [_resource_group_key(resource)]
        if key
    }

    expanded: dict[UUID, Resource] = {resource.id: resource for resource in seed_resources}
    for candidate in candidates:
        candidate_user_key = _resource_user_key(candidate)
        candidate_group_key = _resource_group_key(candidate)
        if candidate_user_key and (candidate.tenant_id, candidate_user_key) in user_keys:
            expanded[candidate.id] = candidate
            continue
        if candidate_group_key and (candidate.tenant_id, candidate_group_key) in group_keys:
            expanded[candidate.id] = candidate

    return list(expanded.values())


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
    types: Optional[str] = Query(None),  # comma-separated resource types
    includeHidden: bool = Query(False),  # opt-in to include UI_HIDDEN_TYPES like POWER_BI
    db: AsyncSession = Depends(get_db),
):
    status_clause = "AND status = :rstatus" if status else ""
    type_clause = ""
    hidden_clause = ""
    if types:
        # Explicit type filter. Respect it as-is but still drop hidden unless opted in —
        # e.g. if someone passes types=MAILBOX,POWER_BI the POWER_BI rows get stripped.
        type_list = [t.strip() for t in types.split(",")]
        if not includeHidden:
            type_list = [t for t in type_list if t not in UI_HIDDEN_TYPES]
        if not type_list:
            # All requested types were hidden — empty result without hitting the DB.
            return {"items": [], "item_number": 0, "page_number": page, "next_page_token": None}
        placeholders = ", ".join([f":rt{i}" for i in range(len(type_list))])
        type_clause = f"AND type IN ({placeholders})"
    elif not includeHidden and UI_HIDDEN_TYPES:
        # No explicit filter — exclude hidden by default.
        hidden_placeholders = ", ".join([f":hidden{i}" for i in range(len(UI_HIDDEN_TYPES))])
        hidden_clause = f"AND type NOT IN ({hidden_placeholders})"

    query = text(f"""
        SELECT id, tenant_id, type, external_id, display_name, email, metadata, sla_policy_id,
               status, storage_bytes, last_backup_at, last_backup_status, created_at
        FROM resources
        WHERE tenant_id = :rtenant {status_clause} {type_clause} {hidden_clause}
        ORDER BY created_at DESC
        LIMIT :rlimit OFFSET :roffset
    """)
    params = {"rtenant": tenantId, "rlimit": size, "roffset": (page - 1) * size}
    if status:
        params["rstatus"] = status
    if types:
        for i, t in enumerate(type_list):
            params[f"rt{i}"] = t
    if hidden_clause:
        for i, t in enumerate(sorted(UI_HIDDEN_TYPES)):
            params[f"hidden{i}"] = t

    result = await db.execute(query, params)
    rows = result.fetchall()

    # Count
    count_query = text(f"""
        SELECT count(*) FROM resources WHERE tenant_id = :rtenant {status_clause} {type_clause} {hidden_clause}
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

    # Get backup counts for all resources in one query
    resource_ids = [r[0] for r in rows]
    backup_counts = {}
    if resource_ids:
        counts_query = text("""
            SELECT resource_id, COUNT(*) as backup_count
            FROM snapshots
            WHERE resource_id = ANY(:resource_ids)
            AND status = 'COMPLETED'
            GROUP BY resource_id
        """)
        counts_result = await db.execute(counts_query, {"resource_ids": resource_ids})
        backup_counts = {str(row[0]): row[1] for row in counts_result.fetchall()}

    def map_kind(t):
        m = {"MAILBOX": "office_user", "SHARED_MAILBOX": "shared_mailbox", "ROOM_MAILBOX": "room_mailbox",
             "ONEDRIVE": "onedrive", "SHAREPOINT_SITE": "sharepoint_site", "TEAMS_CHANNEL": "teams_channel",
             "TEAMS_CHAT": "teams_chat", "ENTRA_USER": "entra_user", "ENTRA_GROUP": "entra_group",
             "ENTRA_APP": "entra_app", "ENTRA_DEVICE": "entra_device", "AZURE_VM": "azure_vm",
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql", "AZURE_POSTGRESQL_SINGLE": "azure_postgresql",
             "RESOURCE_GROUP": "resource_group", "DYNAMIC_GROUP": "dynamic_group",
             "POWER_BI": "power_bi", "POWER_APPS": "power_apps", "POWER_AUTOMATE": "power_automate",
             "POWER_DLP": "power_dlp", "COPILOT": "copilot", "PLANNER": "planner",
             "TODO": "todo", "ONENOTE": "onenote"}
        return m.get(t, t.lower() if t else "unknown")

    def map_status(s):
        return {"ACTIVE": "protected", "ARCHIVED": "archived", "SUSPENDED": "suspended"}.get(s, "discovered")

    def format_backup_size(bytes_val: int) -> str:
        """Format bytes to human-readable size string"""
        if not bytes_val or bytes_val == 0:
            return "0 B"
        if bytes_val >= 1099511627776:
            return f"{bytes_val / 1099511627776:.2f} TB"
        if bytes_val >= 1073741824:
            return f"{bytes_val / 1073741824:.2f} GB"
        if bytes_val >= 1048576:
            return f"{bytes_val / 1048576:.2f} MB"
        if bytes_val >= 1024:
            return f"{bytes_val / 1024:.2f} KB"
        return f"{bytes_val} B"

    items = []
    for r in rows:
        storage_bytes = r[9] or 0
        has_backup = r[10] is not None  # last_backup_at is not None
        backup_count = backup_counts.get(str(r[0]), 0)
        items.append({
            "id": str(r[0]), "tenant_id": str(r[1]), "owner": None,
            "kind": map_kind(r[2]),
            "provider": "azure" if r[2] and "AZURE" in r[2] else "o365",
            "external_id": r[3], "name": r[4], "email": r[5],
            "data": r[6] or {},
            "archived": r[8] == "ARCHIVED", "deleted": r[8] == "PENDING_DELETION",
            "protections": [{"policy_id": str(r[7])}] if r[7] else None,
            "usage": {"resource_id": str(r[0]), "tenant_id": str(r[1]), "backups": backup_count,
                      "size": storage_bytes, "size_delta_year": 0, "size_delta_month": 0, "size_delta_week": 0},
            "backupSize": format_backup_size(storage_bytes) if has_backup else None,
            "status": map_status(r[8]),
            "sla": policies.get(str(r[7])) if r[7] else None,
            "last_backup": r[10].isoformat() if r[10] else None,
            "last_backup_status": r[11] if r[11] else None,
            "group_ids": [],
        })

    has_next = (page * size) < total
    return {"item_number": total, "page_number": page, "next_page_token": str(page + 1) if has_next else None, "items": items}


@app.get("/api/v1/resources/search")
async def search_resources(
    query: str = Query(...),
    type: Optional[str] = Query(None),
    includeHidden: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    filters = [or_(Resource.display_name.ilike(f"%{query}%"), Resource.email.ilike(f"%{query}%"))]
    if type:
        if not includeHidden and type in UI_HIDDEN_TYPES:
            return []
        filters.append(Resource.type == type)
    elif not includeHidden and UI_HIDDEN_TYPES:
        filters.append(Resource.type.notin_(list(UI_HIDDEN_TYPES)))
    stmt = select(Resource).where(*filters).limit(50)
    result = await db.execute(stmt)
    return [
        ResourceResponse(id=str(r.id), name=r.display_name, email=r.email,
                        type=r.type.value if hasattr(r.type, 'value') else str(r.type),
                        totalSize=format_bytes(r.storage_bytes or 0))
        for r in result.scalars().all()
    ]


@app.get("/api/v1/resources/by-type")
async def get_resources_by_type(
    type: str = Query(...),
    tenantId: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    includeHidden: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    # Hidden types return an empty page unless the caller explicitly opts in.
    if not includeHidden and type in UI_HIDDEN_TYPES:
        return {"items": [], "item_number": 0, "page_number": page, "next_page_token": None}

    query = text(f"""
        SELECT id, tenant_id, type, external_id, display_name, email, metadata, sla_policy_id,
               status, storage_bytes, last_backup_at, last_backup_status, created_at
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
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql", "AZURE_POSTGRESQL_SINGLE": "azure_postgresql",
             "RESOURCE_GROUP": "resource_group", "DYNAMIC_GROUP": "dynamic_group",
             "POWER_BI": "power_bi", "POWER_APPS": "power_apps", "POWER_AUTOMATE": "power_automate",
             "POWER_DLP": "power_dlp", "COPILOT": "copilot", "PLANNER": "planner",
             "TODO": "todo", "ONENOTE": "onenote"}
        return m.get(t, t.lower() if t else "unknown")

    def map_status(s):
        return {"ACTIVE": "protected", "ARCHIVED": "archived", "SUSPENDED": "suspended"}.get(s, "discovered")

    def format_backup_size(bytes_val: int) -> str:
        """Format bytes to human-readable size string"""
        if not bytes_val or bytes_val == 0:
            return "0 B"
        if bytes_val >= 1099511627776:
            return f"{bytes_val / 1099511627776:.2f} TB"
        if bytes_val >= 1073741824:
            return f"{bytes_val / 1073741824:.2f} GB"
        if bytes_val >= 1048576:
            return f"{bytes_val / 1048576:.2f} MB"
        if bytes_val >= 1024:
            return f"{bytes_val / 1024:.2f} KB"
        return f"{bytes_val} B"

    # Get SLA policy names
    sla_ids = [row[7] for row in rows if row[7]]
    policies = {}
    if sla_ids:
        policy_stmt = select(SlaPolicy).where(SlaPolicy.id.in_(sla_ids))
        policy_result = await db.execute(policy_stmt)
        policies = {str(p.id): p.name for p in policy_result.scalars().all()}

    # Get backup counts for all resources in one query
    resource_ids = [row[0] for row in rows]
    backup_counts = {}
    if resource_ids:
        counts_query = text("""
            SELECT resource_id, COUNT(*) as backup_count
            FROM snapshots
            WHERE resource_id = ANY(:resource_ids)
            AND status = 'COMPLETED'
            GROUP BY resource_id
        """)
        counts_result = await db.execute(counts_query, {"resource_ids": resource_ids})
        backup_counts = {str(r[0]): r[1] for r in counts_result.fetchall()}

    items = []
    for row in rows:
        storage_bytes = row[9] or 0
        has_backup = row[10] is not None
        backup_count = backup_counts.get(str(row[0]), 0)
        items.append({
            "id": str(row[0]), "tenant_id": str(row[1]), "owner": None,
            "kind": map_kind(row[2]),
            "provider": "azure" if "AZURE" in (row[2] or "") else "o365",
            "external_id": row[3], "name": row[4], "email": row[5],
            "data": row[6] or {},
            "archived": row[8] == "ARCHIVED", "deleted": row[8] == "PENDING_DELETION",
            "protections": [{"policy_id": str(row[7])}] if row[7] else None,
            "usage": {"resource_id": str(row[0]), "tenant_id": str(row[1]), "backups": backup_count,
                      "size": storage_bytes, "size_delta_year": 0, "size_delta_month": 0, "size_delta_week": 0},
            "backupSize": format_backup_size(storage_bytes) if has_backup else None,
            "status": map_status(row[8]),
            "sla": policies.get(str(row[7])) if row[7] else None,
            "last_backup": row[10].isoformat() if row[10] else None,
            "last_backup_status": row[11] if row[11] else None,
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
    await validate_policy_scope(
        db,
        policy_id=UUID(request.policyId),
        tenant_id=resource.tenant_id,
        resources=[resource],
    )
    resources = await expand_linked_policy_scope(db, [resource])
    for target in resources:
        target.sla_policy_id = UUID(request.policyId)
        target.status = ResourceStatus.ACTIVE
    await db.commit()


@app.post("/api/v1/resources/{resource_id}/unassign-policy", status_code=204)
async def unassign_policy(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resources = await expand_linked_policy_scope(db, [resource])
    for target in resources:
        target.sla_policy_id = None
    await db.commit()


@app.post("/api/v1/resources/{resource_id}/archive", status_code=204)
async def archive_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.ARCHIVED
    await db.commit()


@app.post("/api/v1/resources/{resource_id}/unarchive", status_code=204)
async def unarchive_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.ACTIVE
    await db.commit()


@app.delete("/api/v1/resources/{resource_id}", status_code=204)
async def delete_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.PENDING_DELETION
    await db.commit()


@app.post("/api/v1/resources/bulk-assign-policy", status_code=200)
async def bulk_assign_policy(request: BulkAssignRequest, db: AsyncSession = Depends(get_db)):
    """
    Assign an SLA policy to multiple resources at once.

    Body:
    {
        "resourceIds": ["uuid1", "uuid2", ...],
        "policyId": "uuid-of-policy"
    }

    Returns:
    {
        "assigned": 10,
        "not_found": ["uuid-of-missing-resource", ...]
    }
    """
    print(f"[BULK_ASSIGN] Received policyId: '{request.policyId}', resourceIds: {request.resourceIds}")

    # Validate policyId
    if not request.policyId or request.policyId.strip() == "":
        # If policyId is empty, unassign policy from resources
        resource_ids = []
        for rid in request.resourceIds:
            try:
                resource_ids.append(UUID(rid))
            except ValueError:
                print(f"[BULK_ASSIGN] Skipping invalid resource ID: {rid}")
                continue

        stmt = select(Resource).where(Resource.id.in_(resource_ids))
        result = await db.execute(stmt)
        resources = result.scalars().all()

        expanded_resources = await expand_linked_policy_scope(db, resources)

        for resource in expanded_resources:
            resource.sla_policy_id = None
        await db.commit()

        return {
            "assigned": 0,
            "unassigned": len(expanded_resources),
            "not_found": [],
        }

    try:
        policy_id = UUID(request.policyId)
    except ValueError:
        print(f"[BULK_ASSIGN] ERROR: Invalid policy ID format: '{request.policyId}'")
        raise HTTPException(status_code=400, detail=f"Invalid policy ID format: '{request.policyId}'. Must be a valid UUID.")

    resource_ids = []
    for rid in request.resourceIds:
        try:
            resource_ids.append(UUID(rid))
        except ValueError:
            print(f"[BULK_ASSIGN] Skipping invalid resource ID: {rid}")
            continue
    
    # Fetch all matching resources in one query
    stmt = select(Resource).where(Resource.id.in_(resource_ids))
    result = await db.execute(stmt)
    resources = result.scalars().all()
    found_ids = {str(r.id) for r in resources}
    not_found = [rid for rid in request.resourceIds if rid not in found_ids]
    if resources:
        tenant_ids = {resource.tenant_id for resource in resources}
        if len(tenant_ids) != 1:
            raise HTTPException(status_code=400, detail="Resources must belong to a single tenant")
        await validate_policy_scope(
            db,
            policy_id=policy_id,
            tenant_id=next(iter(tenant_ids)),
            resources=resources,
        )
    
    # Bulk update
    expanded_resources = await expand_linked_policy_scope(db, resources)

    updated_count = 0
    for resource in expanded_resources:
        resource.sla_policy_id = policy_id
        resource.status = ResourceStatus.ACTIVE
        updated_count += 1
    
    await db.commit()
    
    return {
        "assigned": updated_count,
        "not_found": not_found,
    }


@app.post("/api/v1/resources/bulk-unassign-policy", status_code=200)
async def bulk_unassign_policy(request: BulkUnassignRequest, db: AsyncSession = Depends(get_db)):
    """
    Remove SLA policy from multiple resources at once.
    
    Body:
    {
        "resourceIds": ["uuid1", "uuid2", ...]
    }
    
    Returns:
    {
        "unassigned": 10,
        "not_found": ["uuid-of-missing-resource", ...]
    }
    """
    resource_ids = [UUID(rid) for rid in request.resourceIds]
    
    stmt = select(Resource).where(Resource.id.in_(resource_ids))
    result = await db.execute(stmt)
    resources = result.scalars().all()
    found_ids = {str(r.id) for r in resources}
    not_found = [rid for rid in request.resourceIds if rid not in found_ids]
    
    expanded_resources = await expand_linked_policy_scope(db, resources)

    for resource in expanded_resources:
        resource.sla_policy_id = None
    
    await db.commit()
    
    return {
        "unassigned": len(expanded_resources),
        "not_found": not_found,
    }


# ============ SLA Policies ============

def build_schedule(policy):
    """Build schedule object from policy fields"""
    hours = []
    if policy.frequency == "THREE_DAILY":
        hours = [4, 12, 20]
        sched_type = "hourly"
    else:
        # Parse backup_window_start like "21:00" -> [21]
        if policy.backup_window_start:
            try:
                hours = [int(policy.backup_window_start.split(":")[0])]
            except:
                hours = [21]
        else:
            hours = [21]
        sched_type = "daily"
    
    return {
        "type": sched_type,
        "hours": hours,
        "timezone": "Asia/Calcutta",
        "week_days": [0, 1, 2, 3, 4, 5, 6],
        "jitter_sec": 21600,
    }


def policy_to_dict(p):
    """Convert policy to API response format"""
    result = SlaPolicyResponse.model_validate(p).model_dump()
    result["serviceType"] = (result.get("serviceType") or "m365").lower()
    print(f"[POLICY] Converted policy: id={result.get('id')}, name={result.get('name')}")
    return result


@app.get("/api/v1/policies")
async def list_policies(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(SlaPolicy).order_by(SlaPolicy.created_at.desc())
    if tenantId:
        stmt = stmt.where(SlaPolicy.tenant_id == UUID(tenantId))
    normalized_service_type = normalize_policy_service_type(serviceType)
    if normalized_service_type:
        stmt = stmt.where(SlaPolicy.service_type == normalized_service_type)
    result = await db.execute(stmt)
    policies = result.scalars().all()
    return [policy_to_dict(p) for p in policies]


@app.get("/api/v1/policies/{policy_id}", response_model=SlaPolicyResponse)
async def get_policy(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).where(SlaPolicy.id == UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    return SlaPolicyResponse.model_validate(policy)


@app.post("/api/v1/policies")
async def create_policy(request: dict, db: AsyncSession = Depends(get_db)):
    # Helper to get value by camelCase or snake_case
    def get_val(camel: str, snake: str, default=None):
        return request.get(camel, request.get(snake, default))

    tenant_id = UUID(get_val("tenantId", "tenant_id"))
    tenant = await db.get(Tenant, tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    service_type = normalize_policy_service_type(get_val("serviceType", "service_type")) or tenant_policy_service_type(tenant)

    policy = SlaPolicy(
        id=uuid4(),
        tenant_id=tenant_id,
        service_type=service_type,
        name=get_val("name", "name", "New Policy"),
        frequency=get_val("frequency", "frequency", "DAILY"),
        backup_days=get_val("backupDays", "backup_days", ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]),
        backup_window_start=get_val("backupWindowStart", "backup_window_start", "21:00"),
        backup_exchange=get_val("backupExchange", "backup_exchange", service_type == "m365"),
        backup_exchange_archive=get_val("backupExchangeArchive", "backup_exchange_archive", False),
        backup_exchange_recoverable=get_val("backupExchangeRecoverable", "backup_exchange_recoverable", False),
        backup_onedrive=get_val("backupOneDrive", "backup_onedrive", service_type == "m365"),
        backup_sharepoint=get_val("backupSharepoint", "backup_sharepoint", service_type == "m365"),
        backup_teams=get_val("backupTeams", "backup_teams", service_type == "m365"),
        backup_teams_chats=get_val("backupTeamsChats", "backup_teams_chats", False),
        backup_entra_id=get_val("backupEntraId", "backup_entra_id", service_type == "m365"),
        backup_power_platform=get_val("backupPowerPlatform", "backup_power_platform", False),
        backup_copilot=get_val("backupCopilot", "backup_copilot", False),
        contacts=get_val("contacts", "contacts", service_type == "m365"),
        calendars=get_val("calendars", "calendars", service_type == "m365"),
        tasks=get_val("tasks", "tasks", False),
        group_mailbox=get_val("groupMailbox", "group_mailbox", service_type == "m365"),
        planner=get_val("planner", "planner", False),
        backup_azure_vm=get_val("backupAzureVm", "backup_azure_vm", service_type == "azure"),
        backup_azure_sql=get_val("backupAzureSql", "backup_azure_sql", service_type == "azure"),
        backup_azure_postgresql=get_val("backupAzurePostgresql", "backup_azure_postgresql", service_type == "azure"),
        retention_type=get_val("retentionType", "retention_type", "INDEFINITE"),
        retention_days=get_val("retentionDays", "retention_days"),
        enabled=get_val("enabled", "enabled", True),
        is_default=get_val("isDefault", "is_default", False),
    )
    db.add(policy)
    await db.commit()

    # Notify scheduler to reschedule jobs with updated policy
    await notify_scheduler_reschedule()

    return policy_to_dict(policy)


@app.put("/api/v1/policies/{policy_id}", response_model=SlaPolicyResponse)
async def update_policy(policy_id: str, request: dict, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).where(SlaPolicy.id == UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    
    # Helper to get value by camelCase or snake_case
    def get_val(camel: str, snake: str, default=None):
        return request.get(camel, request.get(snake, default))

    tenant = await db.get(Tenant, policy.tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    requested_service_type = normalize_policy_service_type(get_val("serviceType", "service_type"))
    if requested_service_type is not None:
        policy.service_type = requested_service_type
    elif not getattr(policy, "service_type", None):
        policy.service_type = tenant_policy_service_type(tenant)
    
    # Map camelCase field names to snake_case DB columns
    field_map = {
        'name': 'name', 'frequency': 'frequency', 'backupDays': 'backup_days',
        'backupWindowStart': 'backup_window_start',
        'backupExchange': 'backup_exchange', 'backupExchangeArchive': 'backup_exchange_archive',
        'backupExchangeRecoverable': 'backup_exchange_recoverable',
        'backupOneDrive': 'backup_onedrive', 'backupSharepoint': 'backup_sharepoint',
        'backupTeams': 'backup_teams', 'backupTeamsChats': 'backup_teams_chats',
        'backupEntraId': 'backup_entra_id', 'backupPowerPlatform': 'backup_power_platform',
        'backupCopilot': 'backup_copilot',
        'contacts': 'contacts', 'calendars': 'calendars', 'tasks': 'tasks',
        'groupMailbox': 'group_mailbox', 'planner': 'planner',
        'backupAzureVm': 'backup_azure_vm', 'backupAzureSql': 'backup_azure_sql',
        'backupAzurePostgresql': 'backup_azure_postgresql',
        'retentionType': 'retention_type', 'retentionDays': 'retention_days',
        'enabled': 'enabled', 'isDefault': 'is_default',
    }
    
    for camel_key, snake_key in field_map.items():
        val = get_val(camel_key, snake_key)
        if val is not None:
            setattr(policy, snake_key, val)
    
    policy.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.commit()

    # Notify scheduler to reschedule jobs with updated policy
    await notify_scheduler_reschedule()

    return SlaPolicyResponse.model_validate(policy)


@app.delete("/api/v1/policies/{policy_id}", status_code=204)
async def delete_policy(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).where(SlaPolicy.id == UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    await db.delete(policy)
    await db.commit()

    # Notify scheduler to reschedule jobs without this policy
    await notify_scheduler_reschedule()


@app.get("/api/v1/policies/{policy_id}/resources")
async def get_policy_resources(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.sla_policy_id == UUID(policy_id))
    result = await db.execute(stmt)
    resources = result.scalars().all()
    return {"content": [{"id": str(r.id), "name": r.display_name, "type": r.type.value, "assignedAt": r.created_at.isoformat()} for r in resources], "totalPages": 1, "totalElements": len(resources)}


@app.post("/api/v1/policies/{policy_id}/auto-assign", status_code=204)
async def auto_assign_policy(policy_id: str, request: dict):
    pass
