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
from shared.models import Job, JobLog, JobType, JobStatus, Resource, Snapshot
from shared.schemas import (
    JobResponse, JobListResponse, TriggerBackupRequest, TriggerBulkBackupRequest
)
from shared.message_bus import message_bus, create_backup_message, create_restore_message


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

    # Require SLA policy assignment
    if not resource.sla_policy_id:
        raise HTTPException(
            status_code=400,
            detail="Resource must have an SLA policy assigned before triggering a backup"
        )

    # If fullBackup is True but resource already has a backup, set to False
    effective_full_backup = request.fullBackup or False
    if effective_full_backup and resource.last_backup_at is not None:
        print(f"[JOB_SERVICE] Resource {request.resourceId} has last_backup_at={resource.last_backup_at}, setting fullBackup=False")
        effective_full_backup = False
    else:
        print(f"[JOB_SERVICE] Resource {request.resourceId} first backup (last_backup_at={resource.last_backup_at}), fullBackup={effective_full_backup}")

    job = Job(
        id=uuid4(), type=JobType.BACKUP,
        tenant_id=resource.tenant_id,
        resource_id=UUID(request.resourceId),
        status=JobStatus.QUEUED, priority=request.priority or 1,
        progress_pct=0, items_processed=0, bytes_processed=0,
        spec={"fullBackup": effective_full_backup, "note": request.note, "triggered_by": "MANUAL"},
    )
    db.add(job)
    await db.commit()  # commit BEFORE publishing — worker must find the job in DB

    # Publish to RabbitMQ
    if settings.RABBITMQ_ENABLED:
        routing_key = "backup.urgent"
        msg = create_backup_message(
            job_id=str(job.id), resource_id=request.resourceId,
            tenant_id=str(resource.tenant_id), full_backup=effective_full_backup
        )
        print(f"[JOB_SERVICE] Publishing backup message to {routing_key}: {msg}")
        await message_bus.publish(routing_key, msg, priority=request.priority or 1)
        print(f"[JOB_SERVICE] Message published successfully")
    else:
        print(f"[JOB_SERVICE] RabbitMQ not enabled, skipping publish. RABBITMQ_ENABLED={settings.RABBITMQ_ENABLED}")

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
                "details": {"fullBackup": effective_full_backup, "note": request.note},
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

        # Filter out resources without SLA policy
        resources_without_sla = [rid for rid, res in resources_map.items() if not res.sla_policy_id]
        if resources_without_sla:
            raise HTTPException(
                status_code=400,
                detail=f"Resources must have SLA policies assigned. {len(resources_without_sla)} resource(s) missing policy: {', '.join(resources_without_sla[:5])}"
            )

        # Group resources by tenant for batch processing
        tenant_groups: Dict[uuid.UUID, List[str]] = {}
        for rid, res in resources_map.items():
            tenant_groups.setdefault(res.tenant_id, []).append(rid)

        jobs_created = []
        pending_publishes = []  # collect publish payloads; send AFTER commit

        # For each tenant, create a single mass backup job
        for tenant_id, resource_ids in tenant_groups.items():
            has_previous_backup = any(resources_map[rid].last_backup_at is not None for rid in resource_ids)
            effective_full_backup = (request.fullBackup or False) and not has_previous_backup
            print(f"[JOB_SERVICE] Batch backup for {len(resource_ids)} resources, fullBackup={effective_full_backup}")

            job = Job(
                id=uuid4(), type=JobType.BACKUP,
                tenant_id=tenant_id,
                resource_id=None,
                batch_resource_ids=[UUID(rid) for rid in resource_ids],
                status=JobStatus.QUEUED, priority=1,
                progress_pct=0, items_processed=0, bytes_processed=0,
                spec={"triggered_by": "MANUAL_BATCH", "resource_count": len(resource_ids), "fullBackup": effective_full_backup},
            )
            db.add(job)
            jobs_created.append({"jobId": str(job.id), "status": "QUEUED", "resourceId": "BATCH", "resourceCount": len(resource_ids)})

            if settings.RABBITMQ_ENABLED:
                from shared.message_bus import create_mass_backup_message
                first_res = resources_map[resource_ids[0]]
                resource_type = first_res.type.value if hasattr(first_res.type, 'value') else str(first_res.type)
                pending_publishes.append(create_mass_backup_message(
                    job_id=str(job.id),
                    tenant_id=str(tenant_id),
                    resource_type=resource_type,
                    resource_ids=resource_ids,
                    sla_policy_id=None,
                    full_backup=effective_full_backup
                ))

        # Commit ALL jobs first so workers always find them in DB
        await db.commit()

        # Now publish — jobs are visible to workers
        for msg in pending_publishes:
            await message_bus.publish("backup.urgent", msg, priority=1)

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
                    "details": {"resourceCount": len(resources_map), "batch": True, "fullBackup": effective_full_backup},
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

        # Require SLA policy assignment
        if not res.sla_policy_id:
            raise HTTPException(
                status_code=400,
                detail="Resource must have an SLA policy assigned before triggering a backup"
            )

        # Determine fullBackup based on whether resource has been backed up before
        effective_full_backup = not (res.last_backup_at is not None)
        print(f"[JOB_SERVICE] Single resource backup for {resource_id}, fullBackup={effective_full_backup}")

        job = Job(
            id=uuid4(), type=JobType.BACKUP,
            tenant_id=res.tenant_id,
            resource_id=UUID(resource_id),
            status=JobStatus.QUEUED, priority=1,
            progress_pct=0, items_processed=0, bytes_processed=0,
            spec={"triggered_by": "MANUAL", "fullBackup": effective_full_backup},
        )
        db.add(job)
        await db.commit()  # commit before publish so worker finds the job

        if settings.RABBITMQ_ENABLED:
            await message_bus.publish("backup.urgent", create_backup_message(
                job_id=str(job.id), resource_id=resource_id,
                tenant_id=str(res.tenant_id), full_backup=effective_full_backup
            ), priority=1)

        return {"jobId": str(job.id), "status": "QUEUED", "resourceId": resource_id}
    return {"error": "No resources provided"}


@app.post("/api/v1/jobs/restore")
@app.post("/api/v1/jobs/restore/mailbox")
@app.post("/api/v1/jobs/restore/onedrive")
@app.post("/api/v1/jobs/restore/sharepoint")
@app.post("/api/v1/jobs/restore/entra-object")
async def trigger_restore(request: dict = None, db: AsyncSession = Depends(get_db)):
    """Trigger a restore job and publish to RabbitMQ"""
    if not request:
        raise HTTPException(status_code=400, detail="Request body is required")

    restore_type = request.get("restoreType", "IN_PLACE")
    snapshot_ids = request.get("snapshotIds", [])
    item_ids = request.get("itemIds", [])
    target_user_id = request.get("targetUserId")
    spec = {
        "targetUserId": target_user_id,
        "targetResourceId": request.get("targetResourceId"),
        "exportFormat": request.get("exportFormat"),
    }

    # Fetch first snapshot to get tenant/resource info
    tenant_id = None
    resource_id = None
    if snapshot_ids:
        from sqlalchemy import select as sa_select
        stmt = sa_select(Snapshot).where(Snapshot.id == uuid.UUID(snapshot_ids[0]))
        result = await db.execute(stmt)
        snapshot = result.scalar_one_or_none()
        if snapshot:
            resource_id = str(snapshot.resource_id)
            resource_stmt = sa_select(Resource).where(Resource.id == snapshot.resource_id)
            resource_result = await db.execute(resource_stmt)
            resource = resource_result.scalar_one_or_none()
            if resource:
                tenant_id = str(resource.tenant_id)

    job = Job(
        id=uuid4(),
        type=JobType.RESTORE,
        tenant_id=uuid.UUID(tenant_id) if tenant_id else None,
        resource_id=uuid.UUID(resource_id) if resource_id else None,
        status=JobStatus.QUEUED,
        priority=1,
        spec={
            "restore_type": restore_type,
            "snapshot_ids": snapshot_ids,
            "item_ids": item_ids,
            **spec,
        }
    )
    db.add(job)
    await db.flush()

    # Publish to RabbitMQ
    if settings.RABBITMQ_ENABLED:
        restore_message = create_restore_message(
            job_id=str(job.id),
            restore_type=restore_type,
            snapshot_ids=snapshot_ids,
            item_ids=item_ids,
            resource_id=resource_id,
            tenant_id=tenant_id,
            spec=spec,
        )
        queue = restore_message.get("queue", "restore.normal")
        await message_bus.publish(queue, restore_message, priority=restore_message.get("priority", 5))

    return {
        "jobId": str(job.id),
        "status": "QUEUED",
        "restoreType": restore_type,
        "snapshotCount": len(snapshot_ids),
        "itemCount": len(item_ids),
    }


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
