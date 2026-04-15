"""Dashboard Service - Aggregated metrics and statistics"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Depends, Query
from sqlalchemy import select, func, text

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
    db: AsyncSession = Depends(get_db),
):
    # Use naive datetime to match TIMESTAMP WITHOUT TIME ZONE
    yesterday = datetime.utcnow() - timedelta(hours=24)
    filters = [Job.type == JobType.BACKUP, Job.created_at >= yesterday]
    if tenantId:
        filters.append(Job.tenant_id == UUID(tenantId))

    success = (await db.execute(select(func.count()).where(Job.status == JobStatus.COMPLETED, *filters))).scalar() or 0
    warnings = (await db.execute(select(func.count()).where(Job.status.in_([JobStatus.RUNNING, JobStatus.QUEUED]), *filters))).scalar() or 0
    failures = (await db.execute(select(func.count()).where(Job.status == JobStatus.FAILED, *filters))).scalar() or 0

    return {"success": success, "warnings": warnings, "failures": failures}


@app.get("/api/v1/dashboard/status/7day")
async def get_7day_status(
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
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
        .where(*filters)
        .group_by(func.date(Job.created_at))
        .order_by(func.date(Job.created_at))
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
    db: AsyncSession = Depends(get_db),
):
    filters = [Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE])]
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))

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

    def bucket_item(bucket_name: str):
        total = 0
        protected = 0
        for resource_type in PROTECTION_BUCKETS[bucket_name]:
            values = totals_by_type.get(resource_type, {"total": 0, "protected": 0})
            total += values["total"]
            protected += values["protected"]
        return {"protectedCount": protected, "total": total}

    bucket_values = {name: bucket_item(name) for name in PROTECTION_BUCKETS}
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
    db: AsyncSession = Depends(get_db),
):
    filters = []
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))

    # Get current total storage bytes
    total = (await db.execute(select(func.sum(Resource.storage_bytes)).where(*filters))).scalar() or 0
    total = int(total) if total else 0
    
    # Get ALL-TIME total bytes backed up (sum of all bytes_added from all completed snapshots)
    all_time_stmt = select(func.sum(Snapshot.bytes_added)).join(Resource, Snapshot.resource_id == Resource.id).where(Snapshot.status == "COMPLETED", *filters)
    all_time_total = (await db.execute(all_time_stmt)).scalar() or 0
    all_time_total = int(all_time_total) if all_time_total else 0

    # Calculate cumulative daily storage for last 30 days
    # For each day, sum all bytes_added from snapshots up to and including that day
    daily_data = []
    running_total = 0
    for i in range(30):
        date = (datetime.utcnow() - timedelta(days=29-i)).date()
        date_start = datetime.combine(date, datetime.min.time())
        date_end = datetime.combine(date, datetime.max.time())

        # Sum bytes_added from all completed snapshots for this day
        day_bytes = (await db.execute(
            select(func.sum(Snapshot.bytes_added))
            .join(Resource, Snapshot.resource_id == Resource.id)
            .where(
                Snapshot.status == "COMPLETED",
                Snapshot.created_at >= date_start,
                Snapshot.created_at <= date_end,
                *filters
            )
        )).scalar() or 0
        day_bytes = int(day_bytes) if day_bytes else 0
        
        running_total += day_bytes
        daily_data.append({
            "date": date.isoformat(),
            "bytes": running_total  # Cumulative total up to this day
        })

    # Calculate real changes (not mock data)
    last_7_days_bytes = sum(d["bytes"] for d in daily_data[-7:])
    seven_day_change = daily_data[-1]["bytes"] - (daily_data[-8]["bytes"] if len(daily_data) > 7 else 0)
    one_day_change = daily_data[-1]["bytes"] - (daily_data[-2]["bytes"] if len(daily_data) > 1 else 0)

    return {
        "total": format_bytes(total),
        "oneDayChange": format_bytes(abs(one_day_change)) + (" ↑" if one_day_change > 0 else " ↓"),
        "oneMonthChange": format_bytes(abs(seven_day_change)) + (" ↑" if seven_day_change > 0 else " ↓"),
        "allTimeTotal": format_bytes(all_time_total),
        "dailyData": daily_data,
    }
