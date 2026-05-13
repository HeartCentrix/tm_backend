"""Dashboard Service - Aggregated metrics and statistics"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import select, func, text, and_, or_

from shared.database import get_db, close_db, AsyncSession, engine
from shared.models import Resource, Job, JobType, JobStatus, Snapshot, SnapshotItem, SnapshotStatus, ResourceType, ResourceStatus, Tenant, TenantType


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Dashboard is read-only and should not run heavyweight schema migration logic
    # during startup. That path can block on application traffic from other services
    # and leave the container stuck in "Waiting for application startup".
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    yield
    await close_db()


app = FastAPI(title="Dashboard Service", version="1.0.0", lifespan=lifespan)


# Per-user content types — these all roll up under a single "Users" card in
# Protection Status. Both legacy (MAILBOX/ONEDRIVE) and post-refactor Tier-2
# (USER_MAIL/USER_ONEDRIVE/USER_CONTACTS/USER_CALENDAR/USER_CHATS) live here
# because they all belong to one ENTRA_USER parent and should not be counted
# separately. The Users bucket is special-cased below to DISTINCT-count by
# parent_resource_id so 9 users × 5 child types still shows as 9, not 45.
USER_CONTENT_TYPES = {
    ResourceType.MAILBOX,
    ResourceType.ONEDRIVE,
    ResourceType.USER_MAIL,
    ResourceType.USER_ONEDRIVE,
    ResourceType.USER_CONTACTS,
    ResourceType.USER_CALENDAR,
    ResourceType.USER_CHATS,
}

PROTECTION_BUCKETS = {
    "users": USER_CONTENT_TYPES,
    "sharedMailboxes": {ResourceType.SHARED_MAILBOX},
    "rooms": {ResourceType.ROOM_MAILBOX},
    "sharepointSites": {ResourceType.SHAREPOINT_SITE},
    "groupsAndTeams": {ResourceType.TEAMS_CHANNEL, ResourceType.TEAMS_CHAT, ResourceType.ENTRA_GROUP, ResourceType.DYNAMIC_GROUP},
    "entraId": {ResourceType.ENTRA_USER, ResourceType.ENTRA_APP, ResourceType.ENTRA_DEVICE, ResourceType.ENTRA_SERVICE_PRINCIPAL},
    "powerPlatform": {ResourceType.POWER_BI, ResourceType.POWER_APPS, ResourceType.POWER_AUTOMATE, ResourceType.POWER_DLP},
}

AZURE_PROTECTION_BUCKETS = {
    "virtualMachines": {ResourceType.AZURE_VM},
    "sqlDatabases": {ResourceType.AZURE_SQL_DB},
    "postgresqlDatabases": {ResourceType.AZURE_POSTGRESQL, ResourceType.AZURE_POSTGRESQL_SINGLE},
}

M365_RESOURCE_TYPES = {
    ResourceType.MAILBOX,
    ResourceType.SHARED_MAILBOX,
    ResourceType.ROOM_MAILBOX,
    ResourceType.ONEDRIVE,
    ResourceType.SHAREPOINT_SITE,
    ResourceType.TEAMS_CHANNEL,
    ResourceType.TEAMS_CHAT,
    ResourceType.ENTRA_USER,
    ResourceType.ENTRA_GROUP,
    ResourceType.ENTRA_APP,
    ResourceType.ENTRA_DEVICE,
    ResourceType.ENTRA_SERVICE_PRINCIPAL,
    ResourceType.POWER_BI,
    ResourceType.POWER_APPS,
    ResourceType.POWER_AUTOMATE,
    ResourceType.POWER_DLP,
    ResourceType.COPILOT,
    ResourceType.PLANNER,
    ResourceType.TODO,
    ResourceType.ONENOTE,
    ResourceType.DYNAMIC_GROUP,
    # Tier 2 per-user content types — these hold the actual backup bytes
    # post-refactor, so they MUST be in this set. Leaving them out
    # silently zeroed out the M365 filter on the dashboard (backup-size
    # dropped from 2.5 GB → 18 KB, 24h / 7d job counts dropped to 0).
    ResourceType.USER_MAIL,
    ResourceType.USER_ONEDRIVE,
    ResourceType.USER_CONTACTS,
    ResourceType.USER_CALENDAR,
    ResourceType.USER_CHATS,
    ResourceType.TEAMS_CHAT_EXPORT,
}

AZURE_RESOURCE_TYPES = {
    ResourceType.AZURE_VM,
    ResourceType.AZURE_SQL_DB,
    ResourceType.AZURE_POSTGRESQL,
    ResourceType.AZURE_POSTGRESQL_SINGLE,
    ResourceType.RESOURCE_GROUP,
}


def parse_service_type(service_type: Optional[str]) -> Optional[str]:
    if not service_type:
        return None
    normalized = service_type.lower()
    if normalized not in ("m365", "azure"):
        raise HTTPException(status_code=400, detail="Unsupported serviceType. Expected 'm365' or 'azure'.")
    return normalized


def resource_types_for_service(service_type: Optional[str]):
    if service_type == "m365":
        return M365_RESOURCE_TYPES
    if service_type == "azure":
        return AZURE_RESOURCE_TYPES
    return None


def datasource_batch_trigger_label(service_type: str) -> str:
    return f"MANUAL_DATASOURCE_{service_type.upper()}"


def format_bytes(bytes_val: int) -> str:
    if bytes_val < 1024**3:
        return f"{bytes_val / 1024**2:.1f} MB"
    return f"{bytes_val / 1024**3:.1f} GB"


@app.get("/health")
async def health():
    return {"status": "ok", "service": "dashboard"}


@app.get("/api/v1/dashboard/overview")
async def get_overview(
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    filters = []
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    
    total = (await db.execute(select(func.count(Resource.id)).where(*filters))).scalar() or 0
    protected = (await db.execute(select(func.count(Resource.id)).where(Resource.sla_policy_id.isnot(None), *filters))).scalar() or 0
    
    # Use naive datetime to match Job.created_at declared as
    # TIMESTAMP WITHOUT TIME ZONE. asyncpg raises DataError on a
    # tz-aware >= naive-column compare. Sibling queries in this file
    # (status/24hour, status/7day, …) already follow this pattern.
    yesterday = datetime.utcnow() - timedelta(hours=24)
    failed = (await db.execute(select(func.count(Job.id)).where(Job.status == JobStatus.FAILED, Job.type == JobType.BACKUP, Job.created_at >= yesterday, *filters))).scalar() or 0
    pending = (await db.execute(select(func.count(Job.id)).where(Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]), Job.type == JobType.BACKUP, *filters))).scalar() or 0
    
    storage = (await db.execute(select(func.sum(Resource.storage_bytes)).where(*filters))).scalar() or 0
    last_backup = (await db.execute(select(func.max(Resource.last_backup_at)).where(*filters))).scalar()
    
    return {
        "totalResources": total,
        "protectedResources": protected,
        "failedBackups": failed,
        "pendingBackups": pending,
        "storageUsed": format_bytes(storage),
        "lastBackupTime": last_backup.isoformat() if last_backup else None,
    }


@app.get("/api/v1/dashboard/status/24hour")
async def get_24hour_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    # Use naive datetime to match TIMESTAMP WITHOUT TIME ZONE
    yesterday = datetime.utcnow() - timedelta(hours=24)
    filters = [Job.type == JobType.BACKUP, Job.created_at >= yesterday]
    if tenantId:
        filters.append(Job.tenant_id == UUID(tenantId))

    service_clause = None
    if service_key and service_resource_types:
        # Two shapes of backup job exist:
        #   1. Per-resource: Job.resource_id points at a single Resource.
        #      Match by that resource's type.
        #   2. Batch (MANUAL_BATCH / USER_ORCHESTRATION): Job.resource_id is
        #      NULL and batch_resource_ids holds the fan-out. We can't join
        #      array → resources cheaply, so we match by Tenant.type
        #      instead — a tenant is either M365 or AZURE, not both, so the
        #      tenant's type tells us which service bucket the batch belongs
        #      to. Replaces the old MANUAL_DATASOURCE_{service} label check
        #      which didn't match the MANUAL_BATCH / USER_ORCHESTRATION
        #      labels our batch jobs actually carry.
        service_tenant_type = TenantType.M365 if service_key == "m365" else TenantType.AZURE
        service_clause = or_(
            and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
            and_(Job.resource_id.is_(None), Tenant.type == service_tenant_type),
        )

    success_stmt = (select(func.count()).select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id))
    warning_stmt = (select(func.count()).select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id))
    failure_stmt = (select(func.count()).select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id))

    if service_clause is not None:
        success_stmt = success_stmt.where(service_clause)
        warning_stmt = warning_stmt.where(service_clause)
        failure_stmt = failure_stmt.where(service_clause)

    # Outcome categories (do NOT count RUNNING/QUEUED — those are
    # in-flight, not outcomes):
    #   success  — COMPLETED with zero failed AND zero skipped items
    #   warnings — COMPLETED with failed_count>0 OR skipped_count>0
    #              (partial success, typical AFI/Veeam dashboard signal),
    #              plus RETRYING jobs (transient failures mid-execution)
    #   failures — FAILED
    # Runtime, queued, cancelled are intentionally excluded.
    _partial_json = text(
        "COALESCE((jobs.result->>'failed_count')::int, 0) > 0 "
        "OR COALESCE((jobs.result->>'skipped_count')::int, 0) > 0"
    )
    success = (await db.execute(
        success_stmt.where(
            Job.status == JobStatus.COMPLETED,
            text("NOT (" + _partial_json.text + ")"),
            *filters,
        )
    )).scalar() or 0
    warnings = (await db.execute(
        warning_stmt.where(
            or_(
                and_(Job.status == JobStatus.COMPLETED, _partial_json),
                Job.status == JobStatus.RETRYING,
            ),
            *filters,
        )
    )).scalar() or 0
    failures = (await db.execute(failure_stmt.where(Job.status == JobStatus.FAILED, *filters))).scalar() or 0

    return {"success": success, "warnings": warnings, "failures": failures}


@app.get("/api/v1/dashboard/status/7day")
async def get_7day_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    # Use naive datetime to match TIMESTAMP WITHOUT TIME ZONE
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    filters = [Job.type == JobType.BACKUP, Job.created_at >= seven_days_ago]
    if tenantId:
        filters.append(Job.tenant_id == UUID(tenantId))
    
    # 7-day outcome buckets match the 24h definitions above. RUNNING /
    # QUEUED jobs are in-flight and don't belong to any outcome bucket;
    # previously they were mis-counted as "warnings" so every active
    # backup inflated the Warnings number on the Overview chart.
    _partial_json_7d = text(
        "COALESCE((jobs.result->>'failed_count')::int, 0) > 0 "
        "OR COALESCE((jobs.result->>'skipped_count')::int, 0) > 0"
    )
    stmt = (
        select(
            func.date(Job.created_at).label("date"),
            func.count().filter(
                and_(
                    Job.status == JobStatus.COMPLETED,
                    text("NOT (" + _partial_json_7d.text + ")"),
                )
            ).label("success"),
            func.count().filter(
                or_(
                    and_(Job.status == JobStatus.COMPLETED, _partial_json_7d),
                    Job.status == JobStatus.RETRYING,
                )
            ).label("warnings"),
            func.count().filter(Job.status == JobStatus.FAILED).label("failures"),
        )
        .select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .outerjoin(Tenant, Job.tenant_id == Tenant.id)
        .where(*filters)
        .group_by(func.date(Job.created_at))
        .order_by(func.date(Job.created_at))
    )
    if service_key and service_resource_types:
        service_tenant_type = TenantType.M365 if service_key == "m365" else TenantType.AZURE
        stmt = stmt.where(
            or_(
                and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
                and_(Job.resource_id.is_(None), Tenant.type == service_tenant_type),
            )
        )
    result = await db.execute(stmt)
    rows = result.all()

    # Build a dict of existing data
    data_by_date = {str(r.date): {"success": r.success or 0, "warnings": r.warnings or 0, "failures": r.failures or 0} for r in rows}
    
    # Fill all 7 days, padding missing days with zeros
    daily_status = []
    for i in range(7):
        date = (datetime.utcnow() - timedelta(days=6-i)).date()
        date_str = date.isoformat()
        daily_status.append({
            "date": date_str,
            "success": data_by_date.get(date_str, {}).get("success", 0),
            "warnings": data_by_date.get(date_str, {}).get("warnings", 0),
            "failures": data_by_date.get(date_str, {}).get("failures", 0),
        })
    
    total_backups = sum(d["success"] + d["warnings"] + d["failures"] for d in daily_status)
    total_success = sum(d["success"] for d in daily_status)
    
    return {
        "dailyStatus": daily_status,
        "summary": {
            "totalBackups": total_backups,
            "successRate": round(total_success / total_backups * 100, 2) if total_backups > 0 else 0,
            "avgDuration": "N/A",
        },
    }


@app.get("/api/v1/dashboard/protection/status")
async def get_protection_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    filters = [Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE])]
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    if service_resource_types:
        filters.append(Resource.type.in_(service_resource_types))

    stmt = (
        select(
            Resource.type,
            func.count(Resource.id).label("total"),
            func.count(Resource.id).filter(Resource.sla_policy_id.isnot(None)).label("protected"),
        )
        .where(*filters)
        .group_by(Resource.type)
    )
    rows = (await db.execute(stmt)).all()

    totals_by_type = {
        row.type: {"total": row.total or 0, "protected": row.protected or 0}
        for row in rows
    }

    def bucket_item(bucket_name: str, buckets: dict):
        total = 0
        protected = 0
        for resource_type in buckets[bucket_name]:
            values = totals_by_type.get(resource_type, {"total": 0, "protected": 0})
            total += values["total"]
            protected += values["protected"]
        return {"protectedCount": protected, "total": total}
    if service_key == "azure":
        bucket_values = {name: bucket_item(name, AZURE_PROTECTION_BUCKETS) for name in AZURE_PROTECTION_BUCKETS}
        total = sum(item["total"] for item in bucket_values.values())
        protected = sum(item["protectedCount"] for item in bucket_values.values())
        percentage = round(protected / total * 100, 2) if total > 0 else 0
        return {
            "virtualMachines": bucket_values["virtualMachines"],
            "sqlDatabases": bucket_values["sqlDatabases"],
            "postgresqlDatabases": bucket_values["postgresqlDatabases"],
            "percentage": percentage,
        }

    bucket_values = {name: bucket_item(name, PROTECTION_BUCKETS) for name in PROTECTION_BUCKETS}

    # Users bucket: dedupe by email so a user with both MAILBOX+ONEDRIVE
    # (or with multiple Tier-2 USER_* rows) counts as ONE user. We use
    # lower(email) as the dedup key to match resource-service
    # /resources/users (which groups the Resources page table by email);
    # parent_resource_id is unreliable here because legacy MAILBOX/ONEDRIVE
    # rows may not have been linked to an ENTRA_USER parent. Falls back to
    # external_id when email is NULL (rare).
    user_key = func.coalesce(func.lower(Resource.email), Resource.external_id)
    user_filters = list(filters) + [Resource.type.in_(USER_CONTENT_TYPES)]
    users_total = (
        await db.execute(
            select(func.count(func.distinct(user_key))).where(*user_filters)
        )
    ).scalar() or 0
    users_protected = (
        await db.execute(
            select(func.count(func.distinct(user_key))).where(
                *user_filters, Resource.sla_policy_id.isnot(None)
            )
        )
    ).scalar() or 0
    bucket_values["users"] = {
        "total": int(users_total),
        "protectedCount": int(users_protected),
    }

    total = sum(item["total"] for item in bucket_values.values())
    protected = sum(item["protectedCount"] for item in bucket_values.values())
    percentage = round(protected / total * 100, 2) if total > 0 else 0

    return {
        "users": bucket_values["users"],
        "sharedMailboxes": bucket_values["sharedMailboxes"],
        "rooms": bucket_values["rooms"],
        "sharepointSites": bucket_values["sharepointSites"],
        "groupsAndTeams": bucket_values["groupsAndTeams"],
        "entraId": bucket_values["entraId"],
        "powerPlatform": bucket_values["powerPlatform"],
        "percentage": percentage,
    }


@app.get("/api/v1/dashboard/backup/size")
async def get_backup_size(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    service_key = parse_service_type(serviceType)
    service_resource_types = resource_types_for_service(service_key)

    filters = []
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    if service_resource_types:
        filters.append(Resource.type.in_(service_resource_types))

    # Single source of truth: Resource.storage_bytes. The snapshots table holds
    # many legacy FAILED rows and bytes_added is unreliable per snapshot, so using
    # it produced a dashboard where the headline said 7.9 MB but the chart/pills
    # read 0. All four fields below are derived from resources only.
    total = int((await db.execute(select(func.sum(Resource.storage_bytes)).where(*filters))).scalar() or 0)

    # Per-day growth based on when each resource was last backed up.
    # Resources last backed up BEFORE the 30-day window seed day 0 as a baseline.
    today = datetime.utcnow().date()
    window_start = today - timedelta(days=29)
    window_start_ts = datetime.combine(window_start, datetime.min.time())

    day_bucket = func.date_trunc("day", Resource.last_backup_at).label("day")
    per_day_rows = (await db.execute(
        select(day_bucket, func.sum(Resource.storage_bytes))
        .where(
            Resource.last_backup_at.isnot(None),
            Resource.last_backup_at >= window_start_ts,
            *filters,
        )
        .group_by(day_bucket)
    )).all()
    per_day_map: dict = {}
    for row in per_day_rows:
        day_val = row[0]
        if day_val is None:
            continue
        day_key = day_val.date() if hasattr(day_val, "date") else day_val
        per_day_map[day_key] = int(row[1] or 0)

    baseline = int((await db.execute(
        select(func.sum(Resource.storage_bytes)).where(
            Resource.last_backup_at.isnot(None),
            Resource.last_backup_at < window_start_ts,
            *filters,
        )
    )).scalar() or 0)

    daily_data = []
    running_total = baseline
    for i in range(30):
        date = window_start + timedelta(days=i)
        running_total += per_day_map.get(date, 0)
        daily_data.append({"date": date.isoformat(), "bytes": running_total})

    seven_day_change = daily_data[-1]["bytes"] - (daily_data[-8]["bytes"] if len(daily_data) > 7 else 0)
    one_day_change = daily_data[-1]["bytes"] - (daily_data[-2]["bytes"] if len(daily_data) > 1 else 0)

    return {
        "total": format_bytes(total),
        "oneDayChange": format_bytes(abs(one_day_change)) + (" ↑" if one_day_change > 0 else " ↓"),
        "oneMonthChange": format_bytes(abs(seven_day_change)) + (" ↑" if seven_day_change > 0 else " ↓"),
        "allTimeTotal": format_bytes(total),
        "dailyData": daily_data,
    }
