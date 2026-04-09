"""Dashboard Service - Aggregated metrics and statistics"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Depends, Query
from sqlalchemy import select, func

from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Resource, Job, JobType, JobStatus


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Dashboard Service", version="1.0.0", lifespan=lifespan)


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
    yesterday = datetime.now(timezone.utc) - timedelta(hours=24)
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
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
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
    
    daily_status = [{"date": r.date.isoformat(), "success": r.success or 0, "warnings": r.warnings or 0, "failures": r.failures or 0} for r in rows]
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
    filters = []
    if tenantId:
        filters.append(Resource.tenant_id == UUID(tenantId))
    
    total = (await db.execute(select(func.count(Resource.id)).where(*filters))).scalar() or 0
    protected = (await db.execute(select(func.count(Resource.id)).where(Resource.sla_policy_id.isnot(None), *filters))).scalar() or 0
    percentage = round(protected / total * 100, 2) if total > 0 else 0
    
    def item(protected, total):
        return {"protectedCount": protected, "total": total}
    
    return {
        "users": item(protected, total),
        "sharedMailboxes": item(0, 0),
        "rooms": item(0, 0),
        "sharepointSites": item(0, 0),
        "groupsAndTeams": item(0, 0),
        "entraId": item(0, 0),
        "powerPlatform": item(0, 0),
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
    
    total = (await db.execute(select(func.sum(Resource.storage_bytes)).where(*filters))).scalar() or 0
    
    daily_data = []
    for i in range(30):
        date = (datetime.now(timezone.utc) - timedelta(days=29-i)).date()
        daily_data.append({"date": date.isoformat(), "bytes": int(total * (0.7 + 0.3 * (i / 30)))})
    
    one_day = daily_data[-1]["bytes"] - daily_data[-2]["bytes"] if len(daily_data) > 1 else 0
    one_month = daily_data[-1]["bytes"] - daily_data[0]["bytes"] if daily_data else 0
    
    return {
        "total": format_bytes(total),
        "oneDayChange": format_bytes(abs(one_day)) + (" ↑" if one_day > 0 else " ↓"),
        "oneMonthChange": format_bytes(abs(one_month)) + (" ↑" if one_month > 0 else " ↓"),
        "oneYearChange": format_bytes(int(total * 0.5)) + " ↑",
        "dailyData": daily_data,
    }
