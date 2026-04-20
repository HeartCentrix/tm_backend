"""Job Service - Manages jobs, backup triggers, restore, and exports"""
from contextlib import asynccontextmanager
from typing import Optional, Dict, List
import uuid
from uuid import UUID, uuid4
from datetime import datetime, timezone
import json
import asyncio

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, text

from shared.config import settings
from shared.database import get_db, close_db, AsyncSession, engine
from shared.models import Job, JobLog, JobType, JobStatus, Resource, Snapshot, SnapshotItem, SnapshotStatus, SlaPolicy, ResourceType, ResourceStatus
from shared.schemas import (
    JobResponse, JobListResponse, TriggerBackupRequest, TriggerBulkBackupRequest, TriggerDatasourceBackupRequest
)
from shared.message_bus import message_bus, create_backup_message, create_restore_message

# AZ-4: Azure workload resources go to dedicated queues (not backup.*)
AZURE_WORKLOAD_QUEUES = {
    "AZURE_VM": "azure.vm",
    "AZURE_SQL_DB": "azure.sql",
    "AZURE_SQL": "azure.sql",
    "AZURE_POSTGRESQL": "azure.postgres",
    "AZURE_POSTGRESQL_SINGLE": "azure.postgres",
    "AZURE_PG": "azure.postgres",
}

M365_RESOURCE_TYPES = [
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
]

AZURE_RESOURCE_TYPES = [
    ResourceType.AZURE_VM,
    ResourceType.AZURE_SQL_DB,
    ResourceType.AZURE_POSTGRESQL,
    ResourceType.AZURE_POSTGRESQL_SINGLE,
    ResourceType.RESOURCE_GROUP,
]


async def _redirect_teams_chat_to_export(db: AsyncSession, resource: Resource) -> Resource:
    """If `resource` is a per-chat TEAMS_CHAT row, return the matching
    per-user TEAMS_CHAT_EXPORT resource (one delta pull per user covers
    all their chats). If no matching export exists yet, return the
    original so it drains through the legacy handler.

    For group chats multiple users carry the same chatId in their export's
    chatIds. Prefer the user whose Graph id appears as a key in the source
    TEAMS_CHAT's metadata.chat_delta_tokens — that's the user the legacy
    drain path used, so the fast path stays routed to the same shard. If no
    such hint exists (chats discovered post-refactor), fall back to the
    first chatIds match.

    TEAMS_CHAT_EXPORT is UI-hidden, so its SLA is never set by the user.
    Inherit SLA from the source TEAMS_CHAT so the trigger check passes."""
    if resource.type != ResourceType.TEAMS_CHAT:
        return resource
    chat_external_id = resource.external_id
    stmt = select(Resource).where(
        Resource.tenant_id == resource.tenant_id,
        Resource.type == ResourceType.TEAMS_CHAT_EXPORT,
    )
    rows = (await db.execute(stmt)).scalars().all()

    candidates = [
        r for r in rows
        if chat_external_id in (r.extra_data or {}).get("chatIds", [])
    ]
    if not candidates:
        return resource

    preferred_user_ids = set(
        (resource.extra_data or {}).get("chat_delta_tokens", {}).keys()
    )
    matched = None
    if preferred_user_ids:
        matched = next(
            (r for r in candidates if r.external_id in preferred_user_ids),
            None,
        )
    if matched is None:
        matched = candidates[0]
    if matched.sla_policy_id is None and resource.sla_policy_id is not None:
        matched.sla_policy_id = resource.sla_policy_id
        await db.commit()
        await db.refresh(matched)
        print(
            f"[JOB_SERVICE] Inherited SLA {resource.sla_policy_id} "
            f"from TEAMS_CHAT {resource.id} to TEAMS_CHAT_EXPORT {matched.id}"
        )
    print(
        f"[JOB_SERVICE] TEAMS_CHAT {resource.id} ({chat_external_id}) "
        f"→ TEAMS_CHAT_EXPORT {matched.id} (user {matched.external_id})"
    )
    return matched


async def _create_batch_backup_jobs(
    resources_map: Dict[str, Resource],
    db: AsyncSession,
    full_backup: bool = True,
    priority: int = 1,
    note: Optional[str] = None,
    trigger_label: str = "MANUAL_BATCH",
):
    if not resources_map:
        raise HTTPException(status_code=404, detail="No valid resources found")

    resources_without_sla = [rid for rid, res in resources_map.items() if not res.sla_policy_id]
    if resources_without_sla:
        raise HTTPException(
            status_code=400,
            detail=f"Resources must have SLA policies assigned. {len(resources_without_sla)} resource(s) missing policy: {', '.join(resources_without_sla[:5])}"
        )

    # Partition by (tenant, queue). Oversized OneDrive children land in a
    # separate (tenant, backup.heavy) bucket so they don't get stuck
    # behind light resources on the shared urgent queue, and vice-versa
    # so small resources don't wait behind a 500 GB drive scan.
    from shared.export_routing import pick_backup_queue
    tenant_queue_groups: Dict[tuple, List[str]] = {}
    for rid, res in resources_map.items():
        res_type = res.type.value if hasattr(res.type, "value") else str(res.type)
        drive_bytes = int((res.extra_data or {}).get("drive_quota_used", 0))
        routing_key = pick_backup_queue(
            drive_bytes_estimate=drive_bytes,
            resource_type=res_type,
            default_queue="backup.urgent",
        )
        tenant_queue_groups.setdefault((res.tenant_id, routing_key), []).append(rid)

    jobs_created = []
    pending_publishes: List[tuple] = []   # (routing_key, msg)

    for (tenant_id, routing_key), resource_ids in tenant_queue_groups.items():
        has_previous_backup = any(resources_map[rid].last_backup_at is not None for rid in resource_ids)
        effective_full_backup = (full_backup or False) and not has_previous_backup

        job = Job(
            id=uuid4(), type=JobType.BACKUP,
            tenant_id=tenant_id,
            resource_id=None,
            batch_resource_ids=[UUID(rid) for rid in resource_ids],
            status=JobStatus.QUEUED, priority=priority,
            progress_pct=0, items_processed=0, bytes_processed=0,
            spec={
                "triggered_by": trigger_label,
                "resource_count": len(resource_ids),
                "fullBackup": effective_full_backup,
                "note": note,
                "queue": routing_key,
            },
        )
        db.add(job)
        jobs_created.append({
            "jobId": str(job.id),
            "status": "QUEUED",
            "resourceId": "BATCH",
            "resourceCount": len(resource_ids),
            "queue": routing_key,
        })

        if settings.RABBITMQ_ENABLED:
            from shared.message_bus import create_mass_backup_message
            first_res = resources_map[resource_ids[0]]
            resource_type = first_res.type.value if hasattr(first_res.type, 'value') else str(first_res.type)
            pending_publishes.append((routing_key, create_mass_backup_message(
                job_id=str(job.id),
                tenant_id=str(tenant_id),
                resource_type=resource_type,
                resource_ids=resource_ids,
                sla_policy_id=None,
                full_backup=effective_full_backup,
            )))

    await db.commit()

    for routing_key, msg in pending_publishes:
        print(f"[JOB_SERVICE] batch backup → {routing_key} ({len(msg.get('resource_ids', []))} resources)")
        await message_bus.publish(routing_key, msg, priority=priority)

    try:
        import httpx
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
                "action": "BACKUP_TRIGGERED",
                "tenant_id": str(list(tenant_groups.keys())[0]),
                "org_id": None,
                "actor_type": "USER",
                "resource_id": None,
                "resource_type": "BATCH",
                "resource_name": f"Batch backup: {len(resources_map)} resources",
                "outcome": "SUCCESS",
                "job_id": jobs_created[0]["jobId"] if jobs_created else None,
                "details": {"resourceCount": len(resources_map), "batch": True, "fullBackup": full_backup, "note": note},
            })
    except Exception:
        pass

    return jobs_created


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    await message_bus.connect()
    yield
    await message_bus.disconnect()
    await close_db()


app = FastAPI(title="Job Service", version="1.0.0", lifespan=lifespan)

# Chat-export router — /api/v1/exports/chat/* (trigger, estimate, SSE, cancel, delete).
# Imported after app creation so the module's router instance is registered.
from chat_export import router as chat_export_router  # noqa: E402
app.include_router(chat_export_router)


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
    """Cancel a job and cascade the cancellation to its dependents.

    Cascade:
      1. Job.status → CANCELLED.
      2. Every related Resource (single resource_id OR each id in
         batch_resource_ids) gets last_backup_status='CANCELLED' so the
         Protection page reflects the cancellation immediately, matching
         what the Activity page shows for the same job.
      3. Any IN_PROGRESS snapshot tied to this job is flipped to FAILED —
         SnapshotStatus has no CANCELLED value, and FAILED is closer to the
         truth than leaving it permanently 'in progress'.

    The worker's batch loop doesn't poll mid-flight, so an already-running
    backup may still complete its current resource — but downstream state is
    consistent the moment the cancel returns."""
    job = (await db.execute(select(Job).where(Job.id == UUID(job_id)))).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRYING):
        # Already terminal — nothing to cascade.
        return

    job.status = JobStatus.CANCELLED
    job.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Collect every resource this job touched.
    resource_ids: List[UUID] = []
    if job.resource_id:
        resource_ids.append(job.resource_id)
    for rid in (job.batch_resource_ids or []):
        try:
            resource_ids.append(UUID(rid) if isinstance(rid, str) else rid)
        except (ValueError, TypeError):
            continue

    if resource_ids:
        await db.execute(
            text("UPDATE resources SET last_backup_status = 'CANCELLED', last_backup_job_id = :jid WHERE id = ANY(:ids)"),
            {"jid": job.id, "ids": resource_ids},
        )
        # Mark any in-flight snapshots for this job as FAILED so the
        # Recovery page doesn't show a permanently-spinning row.
        await db.execute(
            text("UPDATE snapshots SET status = 'FAILED' WHERE job_id = :jid AND status = 'IN_PROGRESS'"),
            {"jid": job.id},
        )

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

    resource = await _redirect_teams_chat_to_export(db, resource)
    request.resourceId = str(resource.id)

    # Prevent backup on inaccessible/suspended/deleted resources
    status_val = resource.status.value if hasattr(resource.status, 'value') else str(resource.status)
    if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
        raise HTTPException(
            status_code=422,
            detail=f"Resource is {status_val} and cannot be backed up. "
                   f"Run discovery first to restore access or remove the resource."
        )

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
        # AZ-4: Route Azure workload resources to dedicated queues
        resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
        routing_key = AZURE_WORKLOAD_QUEUES.get(resource_type, "backup.urgent")

        # Heavy-pool routing: oversized OneDrive drives go to backup.heavy so
        # a single 500 GB drive can't block the shared backup.normal queue.
        # Non-OneDrive types keep their existing routing (urgent / Azure queues).
        if resource_type == "USER_ONEDRIVE":
            from shared.export_routing import pick_backup_queue
            drive_bytes_estimate = int((resource.extra_data or {}).get("drive_quota_used", 0))
            routing_key = pick_backup_queue(
                drive_bytes_estimate=drive_bytes_estimate,
                resource_type=resource_type,
                # Preserve user-initiated urgency for below-threshold
                # drives; previously this leaked into BACKUP_WORKER_QUEUE
                # (scheduled-backup queue), delaying interactive backups.
                default_queue=routing_key,
            )

        msg = create_backup_message(
            job_id=str(job.id), resource_id=request.resourceId,
            tenant_id=str(resource.tenant_id), full_backup=effective_full_backup
        )
        print(f"[JOB_SERVICE] Resource type={resource_type} → queue {routing_key}")
        print(f"[JOB_SERVICE] Publishing backup message to {routing_key}: {msg}")
        await message_bus.publish(routing_key, msg, priority=request.priority or 1)
        print(f"[JOB_SERVICE] Message published successfully")
    else:
        print(f"[JOB_SERVICE] RabbitMQ not enabled, skipping publish. RABBITMQ_ENABLED={settings.RABBITMQ_ENABLED}")

    # Log audit event: BACKUP_TRIGGERED
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
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
        inaccessible_resources = []
        for rid in request.resourceIds:
            res_stmt = select(Resource).where(Resource.id == UUID(rid))
            res_result = await db.execute(res_stmt)
            res = res_result.scalar_one_or_none()
            if res:
                res = await _redirect_teams_chat_to_export(db, res)
                status_val = res.status.value if hasattr(res.status, 'value') else str(res.status)
                if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
                    inaccessible_resources.append({"id": str(res.id), "status": status_val})
                else:
                    resources_map[str(res.id)] = res

        if not resources_map and inaccessible_resources:
            raise HTTPException(
                status_code=422,
                detail=f"All requested resources are inaccessible: "
                       f"{', '.join(r['id'] + '(' + r['status'] + ')' for r in inaccessible_resources)}"
            )

        return await _create_batch_backup_jobs(
            resources_map=resources_map,
            db=db,
            full_backup=request.fullBackup or False,
            priority=request.priority or 1,
            note=request.note,
            trigger_label="MANUAL_BATCH",
        )

    elif resource_id:
        res_stmt = select(Resource).where(Resource.id == UUID(resource_id))
        res_result = await db.execute(res_stmt)
        res = res_result.scalar_one_or_none()
        if not res:
            raise HTTPException(status_code=404, detail="Resource not found")

        res = await _redirect_teams_chat_to_export(db, res)
        resource_id = str(res.id)

        # Prevent backup on inaccessible/suspended/deleted resources
        status_val = res.status.value if hasattr(res.status, 'value') else str(res.status)
        if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
            raise HTTPException(
                status_code=422,
                detail=f"Resource is {status_val} and cannot be backed up. "
                       f"Run discovery first to restore access or remove the resource."
            )

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
            # Heavy-pool routing for oversized OneDrive drives (Tier 2
            # USER_ONEDRIVE children). Everything else keeps the
            # user-initiated urgent queue.
            from shared.export_routing import pick_backup_queue
            res_type = res.type.value if hasattr(res.type, "value") else str(res.type)
            drive_bytes = int((res.extra_data or {}).get("drive_quota_used", 0))
            routing_key = pick_backup_queue(
                drive_bytes_estimate=drive_bytes,
                resource_type=res_type,
                default_queue="backup.urgent",
            )
            print(f"[JOB_SERVICE] trigger-user {resource_id} type={res_type} bytes={drive_bytes} → {routing_key}")
            await message_bus.publish(routing_key, create_backup_message(
                job_id=str(job.id), resource_id=resource_id,
                tenant_id=str(res.tenant_id), full_backup=effective_full_backup
            ), priority=1)

        return {"jobId": str(job.id), "status": "QUEUED", "resourceId": resource_id}
    return {"error": "No resources provided"}


@app.post("/api/v1/backups/trigger-datasource")
async def trigger_datasource_backup(request: TriggerDatasourceBackupRequest, db: AsyncSession = Depends(get_db)):
    service_key = (request.serviceType or "").lower()
    resource_types = M365_RESOURCE_TYPES if service_key == "m365" else AZURE_RESOURCE_TYPES if service_key == "azure" else None
    if resource_types is None:
        raise HTTPException(status_code=400, detail="Unsupported serviceType. Expected 'm365' or 'azure'.")

    excluded_statuses = [
        ResourceStatus.INACCESSIBLE,
        ResourceStatus.SUSPENDED,
        ResourceStatus.PENDING_DELETION,
    ]
    scoped_filters = [
        Resource.tenant_id == UUID(request.tenantId),
        Resource.type.in_(resource_types),
    ]
    summary_stmt = select(
        func.count(Resource.id).label("total_discovered"),
        func.count(Resource.id).filter(
            Resource.sla_policy_id.is_not(None),
            Resource.status.notin_(excluded_statuses),
        ).label("eligible"),
        func.count(Resource.id).filter(Resource.sla_policy_id.is_(None)).label("skip_no_sla"),
        func.count(Resource.id).filter(
            Resource.sla_policy_id.is_not(None),
            Resource.status == ResourceStatus.INACCESSIBLE,
        ).label("skip_inaccessible"),
        func.count(Resource.id).filter(
            Resource.sla_policy_id.is_not(None),
            Resource.status == ResourceStatus.SUSPENDED,
        ).label("skip_suspended"),
        func.count(Resource.id).filter(
            Resource.sla_policy_id.is_not(None),
            Resource.status == ResourceStatus.PENDING_DELETION,
        ).label("skip_pending_deletion"),
    ).where(*scoped_filters)
    summary = (await db.execute(summary_stmt)).one()
    skipped_by_reason = {
        "no_sla": int(summary.skip_no_sla or 0),
        "inaccessible": int(summary.skip_inaccessible or 0),
        "suspended": int(summary.skip_suspended or 0),
        "pending_deletion": int(summary.skip_pending_deletion or 0),
    }
    print(
        f"[JOB_SERVICE] DATASOURCE_BACKUP_SUMMARY tenant={request.tenantId} service={service_key} "
        f"discovered={int(summary.total_discovered or 0)} eligible={int(summary.eligible or 0)} "
        f"skipped_total={sum(skipped_by_reason.values())} skipped_by_reason={skipped_by_reason}"
    )

    stmt = select(Resource).where(
        *scoped_filters,
        Resource.sla_policy_id.is_not(None),
        Resource.status.notin_(excluded_statuses),
    )
    result = await db.execute(stmt)
    resources = result.scalars().all()

    if not resources:
        raise HTTPException(
            status_code=404,
            detail=f"No backup-eligible {service_key.upper()} resources found for this datasource. Make sure discovery has run and SLA policies are assigned."
        )

    resources_map = {str(resource.id): resource for resource in resources}
    return await _create_batch_backup_jobs(
        resources_map=resources_map,
        db=db,
        full_backup=request.fullBackup or False,
        priority=request.priority or 1,
        note=request.note,
        trigger_label=f"MANUAL_DATASOURCE_{service_key.upper()}",
    )


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
        "targetEnvironmentId": request.get("targetEnvironmentId"),
        "exportFormat": request.get("exportFormat"),
        # Folder-select intent: true when user chose a folder checkbox
        # (not individual files). Restore-worker uses this to skip the
        # single-file raw-stream shortcut so even a 1-item expansion is
        # zipped with its folder path preserved.
        "preserveTree": bool(request.get("preserveTree", False)),
        "targetFolder": request.get("targetFolder"),
        "overwrite": request.get("overwrite", False),
        "entraSections": request.get("entraSections"),
        "format": request.get("format"),
        "includeNestedDetail": bool(request.get("includeNestedDetail", False)),
        "recoverMode": request.get("recoverMode"),
        "includeGroupMembership": bool(request.get("includeGroupMembership", True)),
        "includeAuMembership": bool(request.get("includeAuMembership", True)),
        # RestoreModal sends labels like ["Mail","OneDrive","Contacts","Calendar","Chats"].
        # None / missing = restore everything (back-compat). Restore-worker maps each
        # label to the matching item_type values and skips anything else in the snapshot.
        "workloads": request.get("workloads"),
        # Files folder-select v2. When either list is non-empty the
        # restore-worker delegates to shared.folder_resolver instead of
        # treating itemIds as the authoritative selection.
        "folderPaths": request.get("folderPaths") or [],
        "excludedItemIds": request.get("excludedItemIds") or [],
        # SharePoint/OneDrive/Teams/Group restore conflict handling.
        # OVERWRITE replaces in-place; SEPARATE_FOLDER lands under
        # `Restored by TM/{date}/…`. Defaults to SEPARATE_FOLDER in the
        # restore engine when not set.
        "conflictMode": (request.get("conflictMode") or None),
        # Pass-through params consumed by Azure restore handlers (target RG, VM/DB name,
        # subscription, PITR time, firewall flag, disk name, etc). Kept as a nested
        # dict so non-Azure restores ignore it cleanly.
        "azureRestoreParams": request.get("azureRestoreParams", {}),
        # Sub-mode selector for Azure restores: VM → FULL_VM|DISK, SQL → FULL|PITR|SCHEMA_ONLY
        "azureRestoreMode": request.get("azureRestoreMode"),
        # Optional folder filter for USER_CONTACT exports. Empty/missing = all.
        "contactFolders": request.get("contactFolders"),
        # SharePoint "Recover to new site" mode: restore-worker provisions a
        # fresh Communication Site before replaying files. Alias / owner are
        # optional — restore-worker falls back to sensible defaults.
        "newSiteName": request.get("newSiteName"),
        "newSiteAlias": request.get("newSiteAlias"),
        "newSiteOwnerEmail": request.get("newSiteOwnerEmail"),
    }

    # Fetch tenant/resource info — try snapshot first, then fall back to item lookup.
    # Without this, item-driven restores (no snapshot_ids passed) end up with
    # NULL resource_id on the Job row → no audit linkage, no UI rendering, and
    # no way to know which resource was meant for restore if the queue message
    # is lost.
    from sqlalchemy import select as sa_select
    tenant_id = None
    resource_id = None
    snapshot = None
    if snapshot_ids:
        stmt = sa_select(Snapshot).where(Snapshot.id == uuid.UUID(snapshot_ids[0]))
        snapshot = (await db.execute(stmt)).scalar_one_or_none()
    if not snapshot and item_ids:
        # Item-driven restore — derive snapshot from the first item, then
        # snapshot.resource_id gives us the source resource.
        item_stmt = sa_select(SnapshotItem).where(SnapshotItem.id == uuid.UUID(item_ids[0]))
        first_item = (await db.execute(item_stmt)).scalar_one_or_none()
        if first_item:
            snapshot = await db.get(Snapshot, first_item.snapshot_id)
            # Backfill snapshot_ids on the spec so the worker can pull blobs.
            if snapshot and not snapshot_ids:
                snapshot_ids = [str(snapshot.id)]

    resource_type = None
    if snapshot:
        resource_id = str(snapshot.resource_id)
        resource = await db.get(Resource, snapshot.resource_id)
        if resource:
            tenant_id = str(resource.tenant_id)
            resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)

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
            resource_type=resource_type,
        )
        from shared.export_routing import pick_restore_queue
        from sqlalchemy import func as sa_func
        total_bytes = 0
        try:
            # Folder-scope path: resolve the selection first so the
            # routing decision sees the same bytes the worker will
            # actually process. Legacy paths (itemIds only, or just a
            # snapshot) fall back to the simple aggregate.
            if (spec.get("folderPaths") or spec.get("excludedItemIds")) and snapshot_ids:
                from shared.folder_resolver import resolve_selection
                resolved = await resolve_selection(
                    db,
                    snapshot_id=snapshot_ids[0],
                    item_ids=item_ids,
                    folder_paths=spec.get("folderPaths") or [],
                    excluded_item_ids=spec.get("excludedItemIds") or [],
                )
                total_bytes = sum(int(r.content_size or 0) for r in resolved)
            elif item_ids:
                q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                    SnapshotItem.id.in_([uuid.UUID(x) for x in item_ids])
                )
                total_bytes = int((await db.execute(q)).scalar() or 0)
            elif snapshot_ids:
                q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                    SnapshotItem.snapshot_id.in_([uuid.UUID(x) for x in snapshot_ids])
                )
                total_bytes = int((await db.execute(q)).scalar() or 0)
        except Exception:
            total_bytes = 0
        queue = restore_message.get("queue") or pick_restore_queue(total_bytes=total_bytes)
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
    """Queue an export job: persist the Job row, publish to RabbitMQ so
    the restore-worker picks it up, and fire an EXPORT_TRIGGERED audit.

    Scope fields (tenant_id / resource_id / resource_type) are derived by
    walking snapshot → resource in the DB rather than trusting whatever
    the frontend happened to send on the request body — gives us reliable
    audit scoping even when the caller omits those fields."""
    restore_type = request.get("restoreType", "EXPORT_ZIP")
    snapshot_ids = request.get("snapshotIds", [])
    item_ids = request.get("itemIds", [])

    tenant_id = None
    resource_id = None
    resource_type = None
    snapshot = None
    if snapshot_ids:
        snapshot = (await db.execute(
            select(Snapshot).where(Snapshot.id == UUID(snapshot_ids[0]))
        )).scalar_one_or_none()
    if not snapshot and item_ids:
        first_item = (await db.execute(
            select(SnapshotItem).where(SnapshotItem.id == UUID(item_ids[0]))
        )).scalar_one_or_none()
        if first_item:
            snapshot = await db.get(Snapshot, first_item.snapshot_id)
            if snapshot and not snapshot_ids:
                snapshot_ids = [str(snapshot.id)]
    if snapshot:
        resource_id = str(snapshot.resource_id)
        resource = await db.get(Resource, snapshot.resource_id)
        if resource:
            tenant_id = str(resource.tenant_id)
            resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)

    job = Job(
        id=uuid4(),
        type=JobType.EXPORT,
        tenant_id=UUID(tenant_id) if tenant_id else None,
        resource_id=UUID(resource_id) if resource_id else None,
        status=JobStatus.QUEUED,
        priority=5,
        spec={
            "restore_type": restore_type,
            "snapshot_ids": snapshot_ids,
            "item_ids": item_ids,
            **request,
        },
    )
    db.add(job)
    await db.commit()  # commit before publish so worker finds job

    if settings.RABBITMQ_ENABLED:
        # Reuse the restore pipeline: restore-worker already has EXPORT_ZIP,
        # EXPORT_PST, and DOWNLOAD handlers routed off its restore queues.
        # Publishing here means the dedicated export.normal queue stays
        # unused, which matches what the code actually supports today.
        restore_message = create_restore_message(
            job_id=str(job.id),
            restore_type=restore_type,
            snapshot_ids=snapshot_ids,
            item_ids=item_ids,
            resource_id=resource_id,
            tenant_id=tenant_id,
            spec=request,
            resource_type=resource_type,
        )

        # Total bytes for M5 preflight + Task 23 heavy pool routing.
        from sqlalchemy import func as sa_func, select as sa_select
        total_bytes = 0
        try:
            if item_ids:
                q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                    SnapshotItem.id.in_([uuid.UUID(x) for x in item_ids])
                )
                total_bytes = int((await db.execute(q)).scalar() or 0)
            elif snapshot_ids:
                q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                    SnapshotItem.snapshot_id.in_([uuid.UUID(x) for x in snapshot_ids])
                )
                total_bytes = int((await db.execute(q)).scalar() or 0)
        except Exception:
            total_bytes = 0

        from shared.export_routing import pick_export_queue
        queue = pick_export_queue(
            total_bytes=total_bytes,
            include_attachments=bool(request.get("includeAttachments", True)),
        )
        await message_bus.publish(queue, restore_message, priority=restore_message.get("priority", 5))
    else:
        print(f"[JOB_SERVICE] RabbitMQ not enabled, export job {job.id} will stay QUEUED")

    # Audit: EXPORT_TRIGGERED — captures who exported what, when, and which
    # items/snapshots are in scope so the trail is reconstructable for
    # compliance review. Non-blocking: if audit-service is down we don't
    # fail the export itself.
    try:
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=5.0) as _c:
            await _c.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
                "action": "EXPORT_TRIGGERED",
                "tenant_id": tenant_id,
                "actor_type": "USER",
                "resource_id": resource_id,
                "resource_type": resource_type,
                "outcome": "SUCCESS",
                "job_id": str(job.id),
                "details": {
                    "restoreType": restore_type,
                    "snapshotIds": snapshot_ids,
                    "itemIds": item_ids,
                    "itemCount": len(item_ids),
                    "snapshotCount": len(snapshot_ids),
                },
            })
    except Exception:
        pass

    return {"jobId": str(job.id)}


# Workloads that share the folder_path column and can be mutually
# cross-restored. Teams = the team's backing SharePoint site drive;
# M365_GROUP carries the group's backing SharePoint drive under the
# same snapshot. USER_ONEDRIVE is the Tier-2 child under a discovered
# user. Adding a new family member means verifying its backup populates
# folder_path.
_FILES_FAMILY = {
    "ONEDRIVE",
    "USER_ONEDRIVE",
    "SHAREPOINT_SITE",
    "TEAMS_CHANNEL",
    "M365_GROUP",
}


def _resource_family(resource_type: str) -> str:
    """Collapse file-family resource types into one 'FILES' bucket for
    the cross-resource restore guard. Non-family types return their own
    name so the comparison is identity."""
    return "FILES" if (resource_type or "").upper() in _FILES_FAMILY else str(resource_type or "").upper()


@app.post("/api/v1/resources/{resource_id}/export-or-restore")
async def files_export_or_restore(
    resource_id: str,
    request: dict,
    db: AsyncSession = Depends(get_db),
):
    """Unified entry point for Files folder-select v2.

    Body shape (see docs/superpowers/specs/2026-04-20-files-folder-select-design.md):
      restoreType:      EXPORT_ZIP | IN_PLACE | CROSS_RESOURCE
      snapshotId:       UUID of the source snapshot (single)
      itemIds:          files individually ticked (optional)
      folderPaths:      folders ticked; server expands via shared resolver
      excludedItemIds:  files un-ticked inside a ticked folder
      conflictMode:     OVERWRITE | SEPARATE_FOLDER (IN_PLACE only)
      targetResourceId: required for CROSS_RESOURCE; same workload family
      preserveTree:     defaults true when folderPaths is non-empty

    Delegates to ``trigger_restore`` so queueing / auditing / size-cap
    logic stays in one place.
    """
    resource = await db.get(Resource, uuid.UUID(resource_id))
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
    if resource_type.upper() not in _FILES_FAMILY:
        raise HTTPException(
            status_code=400,
            detail=f"Resource type {resource_type} is not a file workload",
        )

    restore_type = (request.get("restoreType") or "").upper()
    if restore_type not in ("EXPORT_ZIP", "IN_PLACE", "CROSS_RESOURCE"):
        raise HTTPException(
            status_code=400,
            detail="restoreType must be EXPORT_ZIP, IN_PLACE, or CROSS_RESOURCE",
        )

    snapshot_id = request.get("snapshotId")
    if not snapshot_id:
        raise HTTPException(status_code=400, detail="snapshotId is required")

    item_ids = list(request.get("itemIds") or [])
    folder_paths = list(request.get("folderPaths") or [])
    excluded_item_ids = list(request.get("excludedItemIds") or [])
    if not item_ids and not folder_paths:
        raise HTTPException(status_code=400, detail="Select at least one item or folder")

    preserve_tree = bool(request.get("preserveTree", bool(folder_paths)))

    conflict_mode = (request.get("conflictMode") or "").upper() or None
    if restore_type == "IN_PLACE":
        if conflict_mode not in ("OVERWRITE", "SEPARATE_FOLDER"):
            raise HTTPException(
                status_code=400,
                detail="conflictMode=OVERWRITE or SEPARATE_FOLDER required for IN_PLACE",
            )

    target_resource_id = request.get("targetResourceId")
    if restore_type == "CROSS_RESOURCE":
        if not target_resource_id:
            raise HTTPException(status_code=400, detail="targetResourceId required for CROSS_RESOURCE")
        target = await db.get(Resource, uuid.UUID(target_resource_id))
        if not target:
            raise HTTPException(status_code=404, detail="Target resource not found")
        if target.tenant_id != resource.tenant_id:
            raise HTTPException(status_code=400, detail="Cross-tenant restore is not supported")
        target_type = target.type.value if hasattr(target.type, "value") else str(target.type)
        if _resource_family(target_type) != _resource_family(resource_type):
            raise HTTPException(
                status_code=400,
                detail=f"Target workload family mismatch ({target_type} vs {resource_type})",
            )

    # Forward into the shared trigger_restore path so auditing, queueing,
    # and size-cap logic stay in one place.
    body = {
        "restoreType": restore_type,
        "snapshotIds": [snapshot_id],
        "itemIds": item_ids,
        "folderPaths": folder_paths,
        "excludedItemIds": excluded_item_ids,
        "preserveTree": preserve_tree,
        "conflictMode": conflict_mode,
        "targetResourceId": target_resource_id,
        "exportFormat": request.get("exportFormat")
            or ("ZIP" if restore_type == "EXPORT_ZIP" else None),
    }
    return await trigger_restore(body, db)


@app.post("/api/v1/sharepoint/{resource_id}/download")
async def sharepoint_download(resource_id: str, request: dict, db: AsyncSession = Depends(get_db)):
    """DEPRECATED — use ``POST /api/v1/resources/{resource_id}/export-or-restore``.

    This endpoint predates the Files folder-select v2 payload. It
    continues to work identically for the OneDrive / SharePoint callers
    that still use it; removal is scheduled for the release AFTER
    ``FILES_FOLDER_SELECT_V2`` is fully on in prod.

    Request body (unchanged):
      * ``scope``: ``"site"`` | ``"folder"`` | ``"file"``
      * ``snapshotId``: required for ``site``.
      * ``folderPath``: required for ``folder``.
      * ``itemId``: required for ``file``.

    Returns ``{jobId}``; poll ``/api/v1/jobs/export/{jobId}/status`` and
    then GET ``/api/v1/jobs/export/{jobId}/download`` to pull the bytes.
    """
    scope = (request.get("scope") or "").lower()
    if scope not in ("site", "folder", "file"):
        raise HTTPException(status_code=400, detail="scope must be site, folder, or file")

    resource = await db.get(Resource, UUID(resource_id))
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
    if resource_type != "SHAREPOINT":
        raise HTTPException(status_code=400, detail=f"Resource is not SharePoint (got {resource_type})")

    snapshot_ids: List[str] = []
    item_ids: List[str] = []
    preserve_tree = True
    export_format = "ZIP"

    if scope == "site":
        snap_id = request.get("snapshotId")
        if not snap_id:
            # Pick the latest completed snapshot for the site.
            stmt = select(Snapshot).where(Snapshot.resource_id == resource.id).order_by(Snapshot.created_at.desc()).limit(1)
            latest = (await db.execute(stmt)).scalar_one_or_none()
            if not latest:
                raise HTTPException(status_code=404, detail="No snapshot found for SharePoint site")
            snap_id = str(latest.id)
        snapshot_ids = [snap_id]

    elif scope == "folder":
        folder_path = request.get("folderPath")
        snap_id = request.get("snapshotId")
        if not folder_path:
            raise HTTPException(status_code=400, detail="folderPath required for scope=folder")
        stmt = select(SnapshotItem).where(
            SnapshotItem.folder_path.startswith(folder_path),
        )
        if snap_id:
            stmt = stmt.where(SnapshotItem.snapshot_id == UUID(snap_id))
        else:
            # Restrict to snapshots of this resource so a bare folderPath
            # can't bleed across sites.
            sub = select(Snapshot.id).where(Snapshot.resource_id == resource.id)
            stmt = stmt.where(SnapshotItem.snapshot_id.in_(sub))
        items = (await db.execute(stmt)).scalars().all()
        if not items:
            raise HTTPException(status_code=404, detail=f"No items under folder {folder_path!r}")
        item_ids = [str(i.id) for i in items]
        # preserveTree stays True — a folder download must keep the tree even
        # if the folder happens to have a single file inside.

    else:  # scope == "file"
        item_id = request.get("itemId")
        if not item_id:
            raise HTTPException(status_code=400, detail="itemId required for scope=file")
        item_ids = [item_id]
        preserve_tree = False
        export_format = "ORIGINAL"

    # Reuse the existing export pipeline — same audit, same worker path,
    # same download endpoint — so this wrapper doesn't duplicate plumbing.
    body = {
        "restoreType": "EXPORT_ZIP",
        "snapshotIds": snapshot_ids,
        "itemIds": item_ids,
        "preserveTree": preserve_tree,
        "exportFormat": export_format,
        "scope": scope,
    }
    return await trigger_export(body, db)


@app.get("/api/v1/jobs/export/{job_id}/status")
async def get_export_status(job_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Job).where(Job.id == UUID(job_id))
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"jobId": str(job.id), "status": job.status.value if hasattr(job.status, 'value') else str(job.status), "progress": job.progress_pct or 0}


@app.get("/api/v1/jobs/export/{job_id}/download")
async def download_export_zip(job_id: str, db: AsyncSession = Depends(get_db)):
    """Stream the export ZIP back to the user.

    Restore-worker uploads the built ZIP to the `exports` Azure container at
    `exports/{job_id}/export_{timestamp}.zip` and stores the path in
    `Job.spec.result.blob_path`. We:
      1. Look up the job + verify it's COMPLETED + EXPORT_ZIP-typed
      2. Download the blob bytes from `exports`
      3. Stream them back as a ZIP attachment

    Without this, the frontend gets a 404 when it tries to download — what was
    happening on Recovery exports."""
    from fastapi.responses import StreamingResponse
    job = await db.get(Job, UUID(job_id))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    status_val = job.status.value if hasattr(job.status, "value") else str(job.status)
    if status_val != "COMPLETED":
        raise HTTPException(status_code=409, detail=f"Export not ready (status={status_val})")

    # Worker persists upload metadata in Job.result (not Job.spec).
    result = job.result or {}
    output_mode = result.get("output_mode", "zip")

    from shared.azure_storage import azure_storage_manager
    shard = azure_storage_manager.get_default_shard()

    # Raw single-file shortcut — OneDrive export v2 writes output_mode='raw_single'
    # when exactly one file is exported in ORIGINAL format. Skip ZIP wrapper and
    # stream the source blob bytes directly with the original Content-Type +
    # filename so the browser saves it as the user expects.
    if output_mode == "raw_single":
        src_container = result.get("source_container")
        src_blob_path = result.get("source_blob_path")
        if not src_container or not src_blob_path:
            raise HTTPException(status_code=500, detail="raw_single job missing source blob info")
        content_type = result.get("content_type") or "application/octet-stream"
        original_name = result.get("original_name") or f"export_{job_id}.bin"
        size_bytes = int(result.get("size_bytes") or 0)

        async def _iter_raw():
            async for chunk in shard.download_blob_stream(src_container, src_blob_path):
                yield chunk

        raw_headers = {"Content-Disposition": f'attachment; filename="{original_name}"'}
        if size_bytes:
            raw_headers["Content-Length"] = str(size_bytes)
        return StreamingResponse(_iter_raw(), media_type=content_type, headers=raw_headers)

    blob_path = result.get("blob_path") or result.get("blobPath")
    if not blob_path:
        raise HTTPException(status_code=500, detail="Job completed but no blob_path recorded")

    # Reuse the same shard the workers use so credentials line up.
    # Container naming mirrors backup-worker / restore-worker:
    # `backup-exports-{tenant_hash}`. Fallbacks: literal "exports" (legacy) and
    # Job.result.container (if the worker recorded it explicitly).
    candidate_containers: list = []
    if result.get("container"):
        candidate_containers.append(str(result["container"]))
    if job.tenant_id:
        candidate_containers.append(azure_storage_manager.get_container_name(str(job.tenant_id), "exports"))
    candidate_containers.append("exports")
    # dedupe preserving order
    seen = set(); _uniq = []
    for c in candidate_containers:
        if c and c not in seen:
            seen.add(c); _uniq.append(c)
    candidate_containers = _uniq

    content = None
    last_err: Exception | None = None
    for cand in candidate_containers:
        try:
            content = await shard.download_blob(cand, blob_path)
            if content is not None:
                print(f"[JOB_SERVICE] export download: found in container={cand}")
                break
        except Exception as exc:
            last_err = exc
            print(f"[JOB_SERVICE] container={cand} download failed: {exc}")
    if content is None:
        if last_err:
            raise HTTPException(status_code=500, detail=f"Failed to fetch export blob: {last_err}")
        raise HTTPException(status_code=404, detail=f"Export blob not found in any of: {candidate_containers}")

    fname = blob_path.rsplit("/", 1)[-1] or f"export_{job_id}.zip"

    # Audit: EXPORT_DOWNLOADED — records the user actually pulled the built
    # zip (distinct from EXPORT_TRIGGERED, which only records the request).
    # Spec carries the original restoreType + items so the audit trail
    # matches what was queued earlier.
    try:
        import httpx as _httpx
        spec = job.spec or {}
        async with _httpx.AsyncClient(timeout=5.0) as _c:
            await _c.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
                "action": "EXPORT_DOWNLOADED",
                "tenant_id": str(job.tenant_id) if job.tenant_id else spec.get("tenantId"),
                "actor_type": "USER",
                "resource_id": str(job.resource_id) if job.resource_id else spec.get("resourceId"),
                "resource_type": spec.get("resourceType"),
                "outcome": "SUCCESS",
                "job_id": str(job.id),
                "details": {
                    "blobPath": blob_path,
                    "filename": fname,
                    "byteSize": len(content),
                    "restoreType": spec.get("restoreType") or "EXPORT_ZIP",
                    "snapshotIds": spec.get("snapshotIds") or [],
                    "itemIds": spec.get("itemIds") or [],
                    "itemCount": len(spec.get("itemIds") or []),
                },
            })
    except Exception:
        pass

    return StreamingResponse(
        iter([content]),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


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
