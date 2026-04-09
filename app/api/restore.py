"""Restore and Export routes"""
import uuid
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List

from app.db.database import get_db, AsyncSession
from app.db import models
from app.schemas import RestoreJobStatus, RestoreHistoryResponse, ExportJobStatus, ExportDownloadResponse
from app.security import get_current_user

router = APIRouter()


class RestoreRequest(BaseModel):
    snapshotId: str
    snapshotItemIds: Optional[List[str]] = None
    targetResourceId: Optional[str] = None
    inPlaceRestore: Optional[bool] = True


@router.post("/restore")
async def trigger_restore(
    request: RestoreRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger restore job"""
    job = models.Job(
        id=uuid.uuid4(),
        type=models.JobType.RESTORE,
        status=models.JobStatus.QUEUED,
        priority=1,
        snapshot_id=uuid.UUID(request.snapshotId),
        spec={"itemIds": request.snapshotItemIds, "inPlace": request.inPlaceRestore},
    )
    db.add(job)
    await db.flush()
    
    return {"jobId": str(job.id)}


@router.post("/restore/mailbox")
@router.post("/restore/onedrive")
@router.post("/restore/sharepoint")
@router.post("/restore/entra-object")
async def trigger_specific_restore(
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger specific type restore (mailbox, onedrive, sharepoint, entra)"""
    job = models.Job(
        id=uuid.uuid4(),
        type=models.JobType.RESTORE,
        status=models.JobStatus.QUEUED,
        priority=1,
    )
    db.add(job)
    await db.flush()
    
    return {"jobId": str(job.id)}


@router.get("/restore/{job_id}/status", response_model=RestoreJobStatus)
async def get_restore_status(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get restore job status"""
    stmt = select(models.Job).where(models.Job.id == uuid.UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return RestoreJobStatus(
        jobId=str(job.id),
        status=job.status.value if hasattr(job.status, 'value') else str(job.status),
        progress=job.progress_pct or 0,
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
    )


@router.get("/restore/history")
async def get_restore_history(
    page: int = 1,
    size: int = 50,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get paginated restore history"""
    stmt = (
        select(models.Job)
        .where(models.Job.type == models.JobType.RESTORE)
        .order_by(models.Job.created_at.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    jobs = result.scalars().all()
    
    return RestoreHistoryResponse(
        content=[
            {"id": str(j.id), "status": j.status.value, "createdAt": j.created_at.isoformat()}
            for j in jobs
        ],
        totalPages=1,
        totalElements=len(jobs),
        size=size,
        number=page,
    )


@router.post("/export")
async def trigger_export(
    request: dict,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Trigger export job"""
    job = models.Job(
        id=uuid.uuid4(),
        type=models.JobType.EXPORT,
        status=models.JobStatus.QUEUED,
        priority=5,
        spec={"itemIds": request.get("snapshotItemIds", [])},
    )
    db.add(job)
    await db.flush()
    
    return {"jobId": str(job.id)}


@router.get("/export/{job_id}/status", response_model=ExportJobStatus)
async def get_export_status(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """Get export job status"""
    stmt = select(models.Job).where(models.Job.id == uuid.UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return ExportJobStatus(
        jobId=str(job.id),
        status=job.status.value if hasattr(job.status, 'value') else str(job.status),
        progress=job.progress_pct or 0,
    )


@router.get("/export/{job_id}/download", response_model=ExportDownloadResponse)
async def get_export_download_url(
    job_id: str,
    user=Depends(get_current_user),
):
    """Get time-limited download URL for completed export"""
    return {"url": f"/api/v1/exports/{job_id}/download"}
