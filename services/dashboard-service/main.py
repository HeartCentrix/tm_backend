"""Dashboard Service - Aggregated metrics and statistics"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import select, func, text, and_, or_

from shared.database import get_db, close_db, AsyncSession, engine
from shared.models import Resource, Job, JobType, JobStatus, Snapshot, ResourceType, ResourceStatus


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


PROTECTION_BUCKETS = {
    "users": {ResourceType.MAILBOX, ResourceType.ONEDRIVE},
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
    
    yesterday = datetime.now(timezone.utc) - timedelta(hours=24)
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
        service_clause = or_(
            and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
            and_(Job.resource_id.is_(None), func.json_extract_path_text(Job.spec, "triggered_by") == datasource_batch_trigger_label(service_key)),
        )

    success_stmt = select(func.count()).select_from(Job).outerjoin(Resource, Job.resource_id == Resource.id)
    warning_stmt = select(func.count()).select_from(Job).outerjoin(Resource, Job.resource_id == Resource.id)
    failure_stmt = select(func.count()).select_from(Job).outerjoin(Resource, Job.resource_id == Resource.id)

    if service_clause is not None:
        success_stmt = success_stmt.where(service_clause)
        warning_stmt = warning_stmt.where(service_clause)
        failure_stmt = failure_stmt.where(service_clause)

    success = (await db.execute(success_stmt.where(Job.status == JobStatus.COMPLETED, *filters))).scalar() or 0
    warnings = (await db.execute(warning_stmt.where(Job.status.in_([JobStatus.RUNNING, JobStatus.QUEUED]), *filters))).scalar() or 0
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
    
    stmt = (
        select(
            func.date(Job.created_at).label("date"),
            func.count().filter(Job.status == JobStatus.COMPLETED).label("success"),
            func.count().filter(Job.status.in_([JobStatus.RUNNING, JobStatus.QUEUED])).label("warnings"),
            func.count().filter(Job.status == JobStatus.FAILED).label("failures"),
        )
        .select_from(Job)
        .outerjoin(Resource, Job.resource_id == Resource.id)
        .where(*filters)
        .group_by(func.date(Job.created_at))
        .order_by(func.date(Job.created_at))
    )
    if service_key and service_resource_types:
        stmt = stmt.where(
            or_(
                and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
                and_(Job.resource_id.is_(None), func.json_extract_path_text(Job.spec, "triggered_by") == datasource_batch_trigger_label(service_key)),
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
