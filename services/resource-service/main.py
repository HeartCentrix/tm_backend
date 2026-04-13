"""Resource Service - Manages resources and SLA policies"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone
from datetime import timedelta
import httpx

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, func, or_, text

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Resource, SlaPolicy, ResourceType, ResourceStatus
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
               status, storage_bytes, last_backup_at, last_backup_status, created_at
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
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql"}
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
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql"}
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
    resource.sla_policy_id = UUID(request.policyId)
    resource.status = ResourceStatus.ACTIVE
    await db.commit()


@app.post("/api/v1/resources/{resource_id}/unassign-policy", status_code=204)
async def unassign_policy(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.sla_policy_id = None
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

        for resource in resources:
            resource.sla_policy_id = None
        await db.commit()

        return {
            "assigned": 0,
            "unassigned": len(resources),
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
    
    # Bulk update
    updated_count = 0
    for resource in resources:
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
    
    for resource in resources:
        resource.sla_policy_id = None
    
    await db.commit()
    
    return {
        "unassigned": len(resources),
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
    print(f"[POLICY] Converted policy: id={result.get('id')}, name={result.get('name')}")
    return result


@app.get("/api/v1/policies")
async def list_policies(tenantId: Optional[str] = Query(None), db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).order_by(SlaPolicy.created_at.desc())
    if tenantId:
        stmt = stmt.where(SlaPolicy.tenant_id == UUID(tenantId))
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

    policy = SlaPolicy(
        id=uuid4(),
        tenant_id=UUID(get_val("tenantId", "tenant_id")),
        name=get_val("name", "name", "New Policy"),
        frequency=get_val("frequency", "frequency", "DAILY"),
        backup_days=get_val("backupDays", "backup_days", ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]),
        backup_window_start=get_val("backupWindowStart", "backup_window_start", "21:00"),
        backup_exchange=get_val("backupExchange", "backup_exchange", True),
        backup_exchange_archive=get_val("backupExchangeArchive", "backup_exchange_archive", False),
        backup_exchange_recoverable=get_val("backupExchangeRecoverable", "backup_exchange_recoverable", False),
        backup_onedrive=get_val("backupOneDrive", "backup_onedrive", True),
        backup_sharepoint=get_val("backupSharepoint", "backup_sharepoint", True),
        backup_teams=get_val("backupTeams", "backup_teams", True),
        backup_teams_chats=get_val("backupTeamsChats", "backup_teams_chats", False),
        backup_entra_id=get_val("backupEntraId", "backup_entra_id", True),
        backup_power_platform=get_val("backupPowerPlatform", "backup_power_platform", False),
        backup_copilot=get_val("backupCopilot", "backup_copilot", False),
        contacts=get_val("contacts", "contacts", True),
        calendars=get_val("calendars", "calendars", True),
        tasks=get_val("tasks", "tasks", False),
        group_mailbox=get_val("groupMailbox", "group_mailbox", True),
        planner=get_val("planner", "planner", False),
        retention_type=get_val("retentionType", "retention_type", "INDEFINITE"),
        retention_days=get_val("retentionDays", "retention_days"),
        enabled=get_val("enabled", "enabled", True),
        is_default=get_val("isDefault", "is_default", False),
    )
    db.add(policy)
    await db.flush()

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
        'retentionType': 'retention_type', 'retentionDays': 'retention_days',
        'enabled': 'enabled', 'isDefault': 'is_default',
    }
    
    for camel_key, snake_key in field_map.items():
        val = get_val(camel_key, snake_key)
        if val is not None:
            setattr(policy, snake_key, val)
    
    policy.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.flush()

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
    await db.flush()

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
