"""Job Service - Manages jobs, backup triggers, restore, and exports"""
from contextlib import asynccontextmanager
from typing import Optional, Dict, List
from uuid import UUID, uuid4
from datetime import datetime, timezone
import json
import asyncio

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Job, JobLog, JobType, JobStatus, Resource
from shared.schemas import (
    JobResponse, JobListResponse, TriggerBackupRequest, TriggerBulkBackupRequest
)
from shared.message_bus import message_bus, create_backup_message


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await message_bus.connect()
    yield
    await message_bus.disconnect()
    await close_db()


app = FastAPI(title="Job Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "job"}


@app.get("/api/v1/jobs")
async def list_jobs(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    tenantId: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    filters = []
    if tenantId:
        filters.append(Job.tenant_id == UUID(tenantId))
    if status:
        filters.append(Job.status == status)
    if type:
        filters.append(Job.type == type)
    
    total = (await db.execute(select(func.count(Job.id)).where(*filters))).scalar() or 0
    stmt = select(Job).where(*filters).order_by(Job.created_at.desc()).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    jobs = result.scalars().all()
    
    return JobListResponse(
        content=[
            JobResponse(
                id=str(j.id), type=j.type.value if hasattr(j.type, 'value') else str(j.type),
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
        size=size, number=page,
        first=page == 1,
        last=page >= (total + size - 1) // size,
    )


@app.get("/api/v1/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobResponse(
        id=str(job.id), type=job.type.value if hasattr(job.type, 'value') else str(job.type),
        status=job.status.value if hasattr(job.status, 'value') else str(job.status),
        progress=job.progress_pct or 0,
        resourceId=str(job.resource_id) if job.resource_id else None,
        tenantId=str(job.tenant_id) if job.tenant_id else None,
        createdAt=job.created_at.isoformat() if job.created_at else "",
        updatedAt=job.updated_at.isoformat() if job.updated_at else "",
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
        errorMessage=job.error_message,
    )


@app.get("/api/v1/jobs/{job_id}/progress")
async def get_job_progress(job_id: str, token: Optional[str] = Query(None)):
    async def event_stream():
        for i in range(300):
            yield f"data: {json.dumps({'jobId': job_id, 'status': 'RUNNING', 'progress': min(i, 100), 'message': 'Processing'})}\n\n"
            await asyncio.sleep(1)
    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/api/v1/jobs/{job_id}/cancel", status_code=204)
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status in [JobStatus.QUEUED, JobStatus.RUNNING]:
        job.status = JobStatus.CANCELLED
        await db.flush()


@app.post("/api/v1/jobs/{job_id}/retry", response_model=JobResponse)
async def retry_job(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job.status = JobStatus.QUEUED
    job.attempts = 0
    job.progress_pct = 0
    await db.flush()
    return JobResponse(
        id=str(job.id), type=job.type.value if hasattr(job.type, 'value') else str(job.type),
        status=job.status.value, progress=0,
        resourceId=str(job.resource_id) if job.resource_id else None,
        createdAt=job.created_at.isoformat(),
        updatedAt=job.updated_at.isoformat(),
    )


@app.get("/api/v1/jobs/{job_id}/logs")
async def get_job_logs(job_id: str, page: int = Query(1), size: int = Query(50), db: AsyncSession = Depends(get_db)):
    stmt = select(JobLog).where(JobLog.job_id == UUID(job_id)).order_by(JobLog.timestamp.desc()).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    logs = result.scalars().all()
    return [
        {"id": str(log.id), "jobId": str(log.job_id), "timestamp": log.timestamp.isoformat() if log.timestamp else "",
         "level": log.level, "message": log.message, "details": log.details}
        for log in logs
    ]


@app.post("/api/v1/backups/trigger", response_model=JobResponse)
async def trigger_backup(request: TriggerBackupRequest, db: AsyncSession = Depends(get_db)):
    # Fetch resource to get tenant info
    resource_stmt = select(Resource).where(Resource.id == UUID(request.resourceId))
    resource_result = await db.execute(resource_stmt)
    resource = resource_result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")

    job = Job(
        id=uuid4(), type=JobType.BACKUP,
        tenant_id=resource.tenant_id,
        resource_id=UUID(request.resourceId),
        status=JobStatus.QUEUED, priority=request.priority or 1,
        progress_pct=0, items_processed=0, bytes_processed=0,
        spec={"fullBackup": request.fullBackup, "note": request.note, "triggered_by": "MANUAL"},
    )
    db.add(job)
    await db.flush()

    # Publish to RabbitMQ
    if settings.RABBITMQ_ENABLED:
        routing_key = "backup.urgent"
        await message_bus.publish(routing_key, create_backup_message(
            job_id=str(job.id), resource_id=request.resourceId,
            tenant_id=str(resource.tenant_id), full_backup=request.fullBackup or False
        ), priority=request.priority or 1)

    # Log audit event: BACKUP_TRIGGERED
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post("http://audit-service:8012/api/v1/audit/log", json={
                "action": "BACKUP_TRIGGERED",
                "tenant_id": str(resource.tenant_id),
                "org_id": None,  # Would need tenant org lookup
                "actor_type": "USER",
                "resource_id": request.resourceId,
                "resource_type": resource.type.value if hasattr(resource.type, 'value') else resource.type,
                "resource_name": resource.display_name,
                "outcome": "SUCCESS",
                "job_id": str(job.id),
                "details": {"fullBackup": request.fullBackup, "note": request.note},
            })
    except Exception:
        pass  # Don't fail the trigger if audit logging fails

    return JobResponse(
        id=str(job.id), type="BACKUP", status="QUEUED", progress=0,
        resourceId=request.resourceId,
        createdAt=job.created_at.isoformat(),
        updatedAt=job.created_at.isoformat(),
    )


@app.post("/api/v1/backups/trigger-user/{resource_id}")
@app.post("/api/v1/backups/trigger-bulk")
async def trigger_bulk_backup(resource_id: str = None, request: TriggerBulkBackupRequest = None, db: AsyncSession = Depends(get_db)):
    if request and request.resourceIds:
        # Fetch all resources with tenant info
        resources_map = {}
        for rid in request.resourceIds:
            res_stmt = select(Resource).where(Resource.id == UUID(rid))
            res_result = await db.execute(res_stmt)
            res = res_result.scalar_one_or_none()
            if res:
                resources_map[rid] = res

        if not resources_map:
            raise HTTPException(status_code=404, detail="No valid resources found")

        # Group resources by tenant for batch processing
        tenant_groups: Dict[uuid.UUID, List[str]] = {}
        for rid, res in resources_map.items():
            tenant_groups.setdefault(res.tenant_id, []).append(rid)

        jobs_created = []

        # For each tenant, create a single mass backup job
        for tenant_id, resource_ids in tenant_groups.items():
            # Create single mass backup job
            job = Job(
                id=uuid4(), type=JobType.BACKUP,
                tenant_id=tenant_id,
                resource_id=None,  # Mass backup
                batch_resource_ids=[UUID(rid) for rid in resource_ids],
                status=JobStatus.QUEUED, priority=1,
                progress_pct=0, items_processed=0, bytes_processed=0,
                spec={"triggered_by": "MANUAL_BATCH", "resource_count": len(resource_ids)},
            )
            db.add(job)

            # Publish mass backup message to RabbitMQ
            if settings.RABBITMQ_ENABLED:
                from shared.message_bus import create_mass_backup_message
                # Get first resource to determine resource type
                first_res = resources_map[resource_ids[0]]
                resource_type = first_res.type.value if hasattr(first_res.type, 'value') else str(first_res.type)

                await message_bus.publish("backup.urgent", create_mass_backup_message(
                    job_id=str(job.id),
                    tenant_id=str(tenant_id),
                    resource_type=resource_type,
                    resource_ids=resource_ids,
                    sla_tier="MANUAL",
                    full_backup=False
                ), priority=1)

            jobs_created.append({"jobId": str(job.id), "status": "QUEUED", "resourceId": "BATCH", "resourceCount": len(resource_ids)})

        await db.flush()

        # Log audit event
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post("http://audit-service:8012/api/v1/audit/log", json={
                    "action": "BACKUP_TRIGGERED",
                    "tenant_id": str(list(tenant_groups.keys())[0]),
                    "org_id": None,
                    "actor_type": "USER",
                    "resource_id": None,
                    "resource_type": "BATCH",
                    "resource_name": f"Batch backup: {len(resources_map)} resources",
                    "outcome": "SUCCESS",
                    "job_id": jobs_created[0]["jobId"] if jobs_created else None,
                    "details": {"resourceCount": len(resources_map), "batch": True},
                })
        except Exception:
            pass

        return jobs_created

    elif resource_id:
        res_stmt = select(Resource).where(Resource.id == UUID(resource_id))
        res_result = await db.execute(res_stmt)
        res = res_result.scalar_one_or_none()
        if not res:
            raise HTTPException(status_code=404, detail="Resource not found")

        job = Job(
            id=uuid4(), type=JobType.BACKUP,
            tenant_id=res.tenant_id,
            resource_id=UUID(resource_id),
            status=JobStatus.QUEUED, priority=1,
            progress_pct=0, items_processed=0, bytes_processed=0,
            spec={"triggered_by": "MANUAL"},
        )
        db.add(job)
        await db.flush()

        if settings.RABBITMQ_ENABLED:
            await message_bus.publish("backup.urgent", create_backup_message(
                job_id=str(job.id), resource_id=resource_id,
                tenant_id=str(res.tenant_id), full_backup=False
            ), priority=1)

        return {"jobId": str(job.id), "status": "QUEUED", "resourceId": resource_id}
    return {"error": "No resources provided"}


@app.post("/api/v1/jobs/restore")
@app.post("/api/v1/jobs/restore/mailbox")
@app.post("/api/v1/jobs/restore/onedrive")
@app.post("/api/v1/jobs/restore/sharepoint")
@app.post("/api/v1/jobs/restore/entra-object")
async def trigger_restore(request: dict = None, db: AsyncSession = Depends(get_db)):
    job = Job(id=uuid4(), type=JobType.RESTORE, status=JobStatus.QUEUED, priority=1, spec=request or {})
    db.add(job)
    await db.flush()
    return {"jobId": str(job.id)}


@app.get("/api/v1/jobs/restore/{job_id}/status")
async def get_restore_status(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"jobId": str(job.id), "status": job.status.value if hasattr(job.status, 'value') else str(job.status), "progress": job.progress_pct or 0}


@app.get("/api/v1/jobs/restore/history")
async def get_restore_history(page: int = 1, size: int = 50, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.type == JobType.RESTORE).order_by(Job.created_at.desc()).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    jobs = result.scalars().all()
    return {"content": [{"id": str(j.id), "status": j.status.value, "createdAt": j.created_at.isoformat()} for j in jobs], "totalPages": 1, "totalElements": len(jobs), "size": size, "number": page}


@app.post("/api/v1/jobs/export")
async def trigger_export(request: dict, db: AsyncSession = Depends(get_db)):
    job = Job(id=uuid4(), type=JobType.EXPORT, status=JobStatus.QUEUED, priority=5, spec=request)
    db.add(job)
    await db.flush()
    return {"jobId": str(job.id)}


@app.get("/api/v1/jobs/export/{job_id}/status")
async def get_export_status(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"jobId": str(job.id), "status": job.status.value if hasattr(job.status, 'value') else str(job.status), "progress": job.progress_pct or 0}


@app.get("/api/v1/jobs/export/{job_id}/download")
async def get_export_download_url(job_id: str):
    return {"url": f"/api/v1/exports/{job_id}/download"}


@app.get("/api/v1/dlq/stats")
async def get_dlq_stats():
    return [
        {"dlqName": "backup.urgent.dlq", "messageCount": 0},
        {"dlqName": "backup.normal.dlq", "messageCount": 0},
        {"dlqName": "restore.urgent.dlq", "messageCount": 0},
    ]


@app.post("/api/v1/dlq/{dlq_name}/purge", status_code=204)
@app.post("/api/v1/dlq/{dlq_name}/requeue", status_code=204)
async def dlq_action(dlq_name: str):
    pass
