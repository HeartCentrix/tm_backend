"""Job Service - Manages jobs, backup triggers, restore, and exports"""
from contextlib import asynccontextmanager
from typing import Optional, Dict, List
from uuid import UUID, uuid4
from datetime import datetime, timezone
import json
import asyncio

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, text

from shared.config import settings
from shared.database import get_db, close_db, AsyncSession, engine
from shared.models import Job, JobLog, JobType, JobStatus, Resource, Snapshot, SnapshotItem, SlaPolicy, ResourceType, ResourceStatus
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

    tenant_groups: Dict[UUID, List[str]] = {}
    for rid, res in resources_map.items():
        tenant_groups.setdefault(res.tenant_id, []).append(rid)

    jobs_created = []
    pending_publishes = []

    for tenant_id, resource_ids in tenant_groups.items():
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
            },
        )
        db.add(job)
        jobs_created.append({
            "jobId": str(job.id),
            "status": "QUEUED",
            "resourceId": "BATCH",
            "resourceCount": len(resource_ids),
        })

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
                full_backup=effective_full_backup,
            ))

    await db.commit()

    for msg in pending_publishes:
        await message_bus.publish("backup.urgent", msg, priority=priority)

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
                status_val = res.status.value if hasattr(res.status, 'value') else str(res.status)
                if status_val in ("INACCESSIBLE", "SUSPENDED", "PENDING_DELETION"):
                    inaccessible_resources.append({"id": rid, "status": status_val})
                else:
                    resources_map[rid] = res

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
            await message_bus.publish("backup.urgent", create_backup_message(
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
        "targetFolder": request.get("targetFolder"),
        "overwrite": request.get("overwrite", False),
        # RestoreModal sends labels like ["Mail","OneDrive","Contacts","Calendar","Chats"].
        # None / missing = restore everything (back-compat). Restore-worker maps each
        # label to the matching item_type values and skips anything else in the snapshot.
        "workloads": request.get("workloads"),
        # Pass-through params consumed by Azure restore handlers (target RG, VM/DB name,
        # subscription, PITR time, firewall flag, disk name, etc). Kept as a nested
        # dict so non-Azure restores ignore it cleanly.
        "azureRestoreParams": request.get("azureRestoreParams", {}),
        # Sub-mode selector for Azure restores: VM → FULL_VM|DISK, SQL → FULL|PITR|SCHEMA_ONLY
        "azureRestoreMode": request.get("azureRestoreMode"),
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
    blob_path = result.get("blob_path") or result.get("blobPath")
    if not blob_path:
        raise HTTPException(status_code=500, detail="Job completed but no blob_path recorded")

    # Reuse the same shard the workers use so credentials line up.
    try:
        from shared.azure_storage import azure_storage_manager
        shard = azure_storage_manager.get_default_shard()
        content = await shard.download_blob("exports", blob_path)
    except Exception as exc:
        print(f"[JOB_SERVICE] export download failed for job {job_id}: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch export blob: {exc}")
    if content is None:
        raise HTTPException(status_code=404, detail="Export blob not found in storage")

    fname = blob_path.rsplit("/", 1)[-1] or f"export_{job_id}.zip"
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
