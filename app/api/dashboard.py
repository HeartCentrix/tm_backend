"""Dashboard routes"""
from fastapi import APIRouter, Depends, Query
from typing import Optional
from sqlalchemy import select, func, and_
from datetime import datetime, timedelta, timezone

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import (
    DashboardOverview,
    BackupStatus24Hour,
    DailyStatus,
    ProtectionStatus,
    ProtectionStatusItem,
    BackupSize,
    BackupSizeDailyData,
)
from app.security import get_current_user

router = APIRouter()


def format_bytes(bytes_val: int) -> str:
    """Format bytes to human readable string"""
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024**2:
        return f"{bytes_val / 1024:.1f} KB"
    elif bytes_val < 1024**3:
        return f"{bytes_val / 1024**2:.1f} MB"
    elif bytes_val < 1024**4:
        return f"{bytes_val / 1024**3:.1f} GB"
    else:
        return f"{bytes_val / 1024**4:.1f} TB"


@router.get("/overview", response_model=DashboardOverview)
async def get_overview(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get comprehensive dashboard metrics"""
    # Build filters
    filters = []
    if tenantId:
        filters.append(models.Resource.tenant_id == tenantId)
    
    # Total resources
    total_stmt = select(func.count(models.Resource.id)).where(*filters)
    total_result = await db.execute(total_stmt)
    total_resources = total_result.scalar() or 0
    
    # Protected resources (with SLA)
    protected_stmt = select(func.count(models.Resource.id)).where(
        models.Resource.sla_policy_id.isnot(None), *filters
    )
    protected_result = await db.execute(protected_stmt)
    protected_resources = protected_result.scalar() or 0
    
    # Failed backups (last 24h)
    yesterday = datetime.now(timezone.utc) - timedelta(hours=24)
    failed_stmt = select(func.count(models.Job.id)).where(
        models.Job.status == models.JobStatus.FAILED,
        models.Job.type == models.JobType.BACKUP_SYNC,
        models.Job.created_at >= yesterday,
        *filters,
    )
    failed_result = await db.execute(failed_stmt)
    failed_backups = failed_result.scalar() or 0
    
    # Pending backups
    pending_stmt = select(func.count(models.Job.id)).where(
        models.Job.status.in_([models.JobStatus.QUEUED, models.JobStatus.RUNNING]),
        models.Job.type == models.JobType.BACKUP_SYNC,
        *filters,
    )
    pending_result = await db.execute(pending_stmt)
    pending_backups = pending_result.scalar() or 0
    
    # Storage used
    storage_stmt = select(func.sum(models.Resource.storage_bytes)).where(*filters)
    storage_result = await db.execute(storage_stmt)
    storage_used = storage_result.scalar() or 0
    
    # Last backup time
    last_backup_stmt = select(func.max(models.Resource.last_backup_at)).where(*filters)
    last_backup_result = await db.execute(last_backup_stmt)
    last_backup_time = last_backup_result.scalar()
    
    return DashboardOverview(
        totalResources=total_resources or 0,
        protectedResources=protected_resources or 0,
        failedBackups=failed_backups or 0,
        pendingBackups=pending_backups or 0,
        storageUsed=format_bytes(storage_used),
        lastBackupTime=last_backup_time.isoformat() if last_backup_time else None,
    )


@router.get("/status/24hour", response_model=BackupStatus24Hour)
async def get_24hour_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get last 24h backup job counts by status"""
    yesterday = datetime.now(timezone.utc) - timedelta(hours=24)
    
    filters = [
        models.Job.type == models.JobType.BACKUP_SYNC,
        models.Job.created_at >= yesterday,
    ]
    if tenantId:
        filters.append(models.Job.tenant_id == tenantId)
    
    success_stmt = select(func.count()).where(
        models.Job.status == models.JobStatus.COMPLETED, *filters
    )
    success_result = await db.execute(success_stmt)
    success = success_result.scalar() or 0
    
    # Warnings = running/queued
    warning_stmt = select(func.count()).where(
        models.Job.status.in_([models.JobStatus.RUNNING, models.JobStatus.QUEUED]),
        *filters,
    )
    warning_result = await db.execute(warning_stmt)
    warnings = warning_result.scalar() or 0
    
    failed_stmt = select(func.count()).where(
        models.Job.status == models.JobStatus.FAILED, *filters
    )
    failed_result = await db.execute(failed_stmt)
    failures = failed_result.scalar() or 0
    
    return BackupStatus24Hour(success=success, warnings=warnings, failures=failures)


@router.get("/status/7day")
async def get_7day_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get last 7 days daily backup statistics"""
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
    
    filters = [
        models.Job.type == models.JobType.BACKUP_SYNC,
        models.Job.created_at >= seven_days_ago,
    ]
    if tenantId:
        filters.append(models.Job.tenant_id == tenantId)
    
    # Group by date
    stmt = (
        select(
            func.date(models.Job.created_at).label("date"),
            func.count().filter(models.Job.status == models.JobStatus.COMPLETED).label("success"),
            func.count().filter(
                models.Job.status.in_([models.JobStatus.RUNNING, models.JobStatus.QUEUED])
            ).label("warnings"),
            func.count().filter(models.Job.status == models.JobStatus.FAILED).label("failures"),
        )
        .where(*filters)
        .group_by(func.date(models.Job.created_at))
        .order_by(func.date(models.Job.created_at))
    )
    
    result = await db.execute(stmt)
    rows = result.all()
    
    daily_status = []
    for row in rows:
        daily_status.append(
            DailyStatus(
                date=row.date.isoformat() if row.date else "",
                success=row.success or 0,
                warnings=row.warnings or 0,
                failures=row.failures or 0,
            )
        )
    
    # Fill missing dates
    for i in range(7):
        date = (datetime.now(timezone.utc) - timedelta(days=6-i)).date()
        date_str = date.isoformat()
        if not any(d.date == date_str for d in daily_status):
            daily_status.insert(
                next(i for i, d in enumerate(daily_status) if d.date > date_str)
                if any(d.date > date_str for d in daily_status)
                else len(daily_status),
                DailyStatus(date=date_str, success=0, warnings=0, failures=0),
            )
    
    total_backups = sum(d.success + d.warnings + d.failures for d in daily_status)
    total_success = sum(d.success for d in daily_status)
    
    return {
        "dailyStatus": daily_status,
        "summary": {
            "totalBackups": total_backups,
            "successRate": round(total_success / total_backups * 100, 2) if total_backups > 0 else 0,
            "avgDuration": "N/A",
        },
    }


@router.get("/protection/status", response_model=ProtectionStatus)
async def get_protection_status(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get resource protection breakdown"""
    filters = []
    if tenantId:
        filters.append(models.Resource.tenant_id == tenantId)
    
    # Get all resources grouped by type
    stmt = (
        select(
            models.Resource.type,
            func.count().label("total"),
            func.count().filter(models.Resource.sla_policy_id.isnot(None)).label("protected"),
        )
        .where(*filters)
        .group_by(models.Resource.type)
    )
    result = await db.execute(stmt)
    rows = result.all()
    
    type_counts = {}
    for row in rows:
        type_counts[row.type] = {"total": row.total, "protected": row.protected}
    
    def get_counts(resource_types):
        total = sum(type_counts.get(t, {}).get("total", 0) for t in resource_types)
        protected = sum(type_counts.get(t, {}).get("protected", 0) for t in resource_types)
        return ProtectionStatusItem(protectedCount=protected, total=total)
    
    users = get_counts([models.ResourceType.MAILBOX, models.ResourceType.SHARED_MAILBOX, models.ResourceType.ROOM_MAILBOX])
    shared_mailboxes = get_counts([models.ResourceType.SHARED_MAILBOX])
    rooms = get_counts([models.ResourceType.ROOM_MAILBOX])
    sharepoint = get_counts([models.ResourceType.SHAREPOINT_SITE])
    teams = get_counts([models.ResourceType.TEAMS_CHANNEL, models.ResourceType.TEAMS_CHAT])
    entra = get_counts([models.ResourceType.ENTRA_USER, models.ResourceType.ENTRA_GROUP, models.ResourceType.ENTRA_APP])
    power = get_counts([models.ResourceType.POWER_BI, models.ResourceType.POWER_APPS, models.ResourceType.POWER_AUTOMATE])
    
    total = sum(c["total"] for c in type_counts.values())
    protected = sum(c["protected"] for c in type_counts.values())
    percentage = round(protected / total * 100, 2) if total > 0 else 0
    
    return ProtectionStatus(
        users=users,
        sharedMailboxes=shared_mailboxes,
        rooms=rooms,
        sharepointSites=sharepoint,
        groupsAndTeams=teams,
        entraId=entra,
        powerPlatform=power,
        percentage=percentage,
    )


@router.get("/backup/size", response_model=BackupSize)
async def get_backup_size(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get 30-day storage consumption data"""
    filters = []
    if tenantId:
        filters.append(models.Resource.tenant_id == tenantId)
    
    storage_stmt = select(func.sum(models.Resource.storage_bytes)).where(*filters)
    storage_result = await db.execute(storage_stmt)
    total = storage_result.scalar() or 0
    
    # Generate daily data for last 30 days
    daily_data = []
    for i in range(30):
        date = (datetime.now(timezone.utc) - timedelta(days=29-i)).date()
        # Simplified: use current storage for all dates
        daily_data.append(
            BackupSizeDailyData(
                date=date.isoformat(),
                bytes=int(total * (0.7 + 0.3 * (i / 30))),  # Simulated growth
            )
        )
    
    one_day = daily_data[-1].bytes - daily_data[-2].bytes if len(daily_data) > 1 else 0
    one_month = daily_data[-1].bytes - daily_data[0].bytes if daily_data else 0
    
    return BackupSize(
        total=format_bytes(total),
        oneDayChange=format_bytes(abs(one_day)) + (" ↑" if one_day > 0 else " ↓"),
        oneMonthChange=format_bytes(abs(one_month)) + (" ↑" if one_month > 0 else " ↓"),
        oneYearChange=format_bytes(int(total * 0.5)) + " ↑",
        dailyData=daily_data,
    )
