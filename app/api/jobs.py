"""Job routes"""
import uuid
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func
import asyncio
import json

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import (
    JobResponse,
    JobListResponse,
    JobLog,
    TriggerBackupRequest,
    TriggerBulkBackupRequest,
    DLQStats,
)
from app.security import get_current_user

router = APIRouter()


@router.get("/jobs")
async def list_jobs(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """List jobs with pagination"""
    filters = []
    if tenantId:
        filters.append(models.Job.tenant_id == uuid.UUID(tenantId))
    if status:
        filters.append(models.Job.status == status)
    if type:
        filters.append(models.Job.type == type)
    
    # Count
    count_stmt = select(func.count(models.Job.id)).where(*filters)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0
    
    # Get page
    stmt = (
        select(models.Job)
        .where(*filters)
        .order_by(models.Job.created_at.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    jobs = result.scalars().all()
    
    return JobListResponse(
        content=[
            JobResponse(
                id=str(j.id),
                type=j.type.value if hasattr(j.type, 'value') else str(j.type),
                status=j.status.value if hasattr(j.status, 'value') else str(j.status),
                progress=j.progress_pct or 0,
                resourceId=str(j.resource_id) if j.resource_id else None,
                tenantId=str(j.tenant_id) if j.tenant_id else None,
                createdAt=j.created_at.isoformat() if j.created_at else "",
                updatedAt=j.updated_at.isoformat() if j.updated_at else "",
                completedAt=j.completed_at.isoformat() if j.completed_at else None,
                errorMessage=j.error_message,
            )
            for j in jobs
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size,
        number=page,
        first=page == 1,
        last=page >= (total + size - 1) // size,
    )


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get job by ID"""
    stmt = select(models.Job).where(models.Job.id == uuid.UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JobResponse(
        id=str(job.id),
        type=job.type.value if hasattr(job.type, 'value') else str(job.type),
        status=job.status.value if hasattr(job.status, 'value') else str(job.status),
        progress=job.progress_pct or 0,
        resourceId=str(job.resource_id) if job.resource_id else None,
        tenantId=str(job.tenant_id) if job.tenant_id else None,
        createdAt=job.created_at.isoformat() if job.created_at else "",
        updatedAt=job.updated_at.isoformat() if job.updated_at else "",
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
        errorMessage=job.error_message,
    )


@router.get("/jobs/{job_id}/progress")
async def get_job_progress(job_id: str, token: Optional[str] = Query(None)):
    """SSE stream for real-time job progress"""
    async def event_stream():
        for i in range(300):  # 5 min timeout (300 * 1s)
            # In production, poll database or message queue
            yield f"data: {json.dumps({'jobId': job_id, 'status': 'RUNNING', 'progress': min(i, 100), 'message': 'Processing'})}\n\n"
            await asyncio.sleep(1)
    
    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/jobs/{job_id}/cancel", status_code=204)
async def cancel_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Cancel a QUEUED or RUNNING job"""
    stmt = select(models.Job).where(models.Job.id == uuid.UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status in [models.JobStatus.QUEUED, models.JobStatus.RUNNING]:
        job.status = models.JobStatus.CANCELLED
        await db.flush()


@router.post("/jobs/{job_id}/retry", response_model=JobResponse)
async def retry_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Retry a FAILED/CANCELLED/COMPLETED job"""
    stmt = select(models.Job).where(models.Job.id == uuid.UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job.status = models.JobStatus.QUEUED
    job.attempts = 0
    job.progress_pct = 0
    await db.flush()
    
    return JobResponse(
        id=str(job.id),
        type=job.type.value if hasattr(job.type, 'value') else str(job.type),
        status=job.status.value,
        progress=0,
        resourceId=str(job.resource_id) if job.resource_id else None,
        createdAt=job.created_at.isoformat(),
        updatedAt=job.updated_at.isoformat(),
    )


@router.get("/jobs/{job_id}/logs")
async def get_job_logs(
    job_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get job execution logs"""
    stmt = (
        select(models.JobLog)
        .where(models.JobLog.job_id == uuid.UUID(job_id))
        .order_by(models.JobLog.timestamp.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    logs = result.scalars().all()
    
    return [
        JobLog(
            id=str(log.id),
            jobId=str(log.job_id),
            timestamp=log.timestamp.isoformat() if log.timestamp else "",
            level=log.level,
            message=log.message,
            details=log.details,
        )
        for log in logs
    ]


@router.post("/backups/trigger", response_model=JobResponse)
async def trigger_backup(
    request: TriggerBackupRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger on-demand backup for a resource"""
    job = models.Job(
        id=uuid.uuid4(),
        type=models.JobType.BACKUP_SYNC,
        resource_id=uuid.UUID(request.resourceId),
        status=models.JobStatus.QUEUED,
        priority=request.priority or 1,
        spec={"fullBackup": request.fullBackup, "note": request.note},
    )
    db.add(job)
    await db.flush()
    
    return JobResponse(
        id=str(job.id),
        type="BACKUP",
        status="QUEUED",
        progress=0,
        resourceId=request.resourceId,
        createdAt=job.created_at.isoformat(),
        updatedAt=job.created_at.isoformat(),
    )


@router.post("/backups/trigger-user/{resource_id}")
async def trigger_user_backup(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger backup for a user's resources"""
    job = models.Job(
        id=uuid.uuid4(),
        type=models.JobType.BACKUP_SYNC,
        resource_id=uuid.UUID(resource_id),
        status=models.JobStatus.QUEUED,
        priority=1,
    )
    db.add(job)
    await db.flush()
    
    return [{"jobId": str(job.id), "status": "QUEUED"}]


@router.post("/backups/trigger-bulk")
async def trigger_bulk_backup(
    request: TriggerBulkBackupRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger backup for multiple resources"""
    jobs = []
    for resource_id in request.resourceIds:
        job = models.Job(
            id=uuid.uuid4(),
            type=models.JobType.BACKUP_SYNC,
            resource_id=uuid.UUID(resource_id),
            status=models.JobStatus.QUEUED,
            priority=request.priority or 5,
        )
        db.add(job)
        jobs.append(job)
    
    await db.flush()
    
    return [
        {"jobId": str(j.id), "status": "QUEUED", "resourceId": str(j.resource_id)}
        for j in jobs
    ]


@router.get("/dlq/stats")
async def get_dlq_stats(
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get Dead Letter Queue statistics"""
    # Simplified - in production, query RabbitMQ DLQ
    return [
        DLQStats(dlqName="backup.urgent.dlq", messageCount=0),
        DLQStats(dlqName="backup.normal.dlq", messageCount=0),
        DLQStats(dlqName="restore.urgent.dlq", messageCount=0),
    ]


@router.post("/dlq/{dlq_name}/purge", status_code=204)
async def purge_dlq(
    dlq_name: str,
    user=Depends(get_current_user),
):
    """Purge all messages from a DLQ"""
    # In production, purge RabbitMQ DLQ
    pass


@router.post("/dlq/{dlq_name}/requeue", status_code=204)
async def requeue_dlq(
    dlq_name: str,
    user=Depends(get_current_user),
):
    """Re-queue one message from DLQ"""
    # In production, requeue from RabbitMQ DLQ
    pass
