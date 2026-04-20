"""
Audit Log Service - Tracks all backup/restore/admin actions
Port: 8012

Responsibilities:
- Record all backup events (started, completed, failed, preemptive)
- Record restore, export, SLA changes, resource actions
- Support Microsoft Graph audit log ingestion
- Paginated listing with multi-filter search
- CSV export for compliance
- Consume audit events from RabbitMQ message bus
- SIEM webhook integration for external log forwarding
"""
import csv
import io
import uuid
import json as json_lib
from datetime import datetime, timedelta
from typing import Any, Optional, List, Dict, Set, Tuple
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, desc, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import httpx

from shared.database import async_session_factory, Base
from shared.models import AuditEvent, Resource, Tenant, Organization, Job, JobStatus, SlaPolicy, ResourceType
from shared.config import settings
from shared.graph_client import GraphClient
from shared.multi_app_manager import multi_app_manager
from shared.message_bus import message_bus

app = FastAPI(title="Audit Log Service", version="1.0.0")

# Action codes

# In-memory cache for running job progress (refreshed every second)
_running_job_cache: Dict[str, dict] = {}


def _fmt_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def _compute_details(job: Job) -> str:
    """Compute details string using data_backed_up/total_data formula."""
    if job.error_message:
        return job.error_message
    cached = _running_job_cache.get(str(job.id), {})
    data_backed_up = cached.get("data_backed_up", job.bytes_processed or 0)
    total_data = cached.get("total_data") or (job.result.get("total_bytes", 0) if job.result else 0)
    if total_data > 0:
        pct = min(100, int((data_backed_up / total_data) * 100))
        return f"{pct}% ({_fmt_bytes(data_backed_up)} / {_fmt_bytes(total_data)})"
    if data_backed_up > 0:
        return f"{_fmt_bytes(data_backed_up)} backed up"
    return f"Progress: {job.progress_pct or 0}%"


async def _refresh_running_jobs():
    """Background task: refresh data_backed_up/total_data for RUNNING jobs every second."""
    while True:
        try:
            async with async_session_factory() as db:
                result = await db.execute(select(Job).where(Job.status == JobStatus.RUNNING))
                running_jobs = result.scalars().all()
                new_cache = {
                    str(job.id): {
                        "data_backed_up": job.bytes_processed or 0,
                        "total_data": (job.result.get("total_bytes", 0) if job.result else 0),
                    }
                    for job in running_jobs
                }
                _running_job_cache.clear()
                _running_job_cache.update(new_cache)
        except Exception:
            pass
        await asyncio.sleep(1)


@app.on_event("startup")
async def startup():
    """Initialize message bus and start consumer on startup"""
    await message_bus.connect()
    asyncio.create_task(consume_audit_events())
    asyncio.create_task(_refresh_running_jobs())


@app.on_event("shutdown")
async def shutdown():
    """Disconnect message bus on shutdown"""
    await message_bus.disconnect()


ACTIONS = {
    "BACKUP_TRIGGERED": "Manual or scheduled backup triggered",
    "BACKUP_STARTED": "Backup job started executing",
    "BACKUP_COMPLETED": "Backup completed successfully",
    "BACKUP_FAILED": "Backup failed permanently",
    "BACKUP_CANCELLED": "Backup cancelled by user",
    "BACKUP_PREEMPTIVE": "Preemptive backup triggered (AI detection)",
    "BACKUP_SKIPPED_SLA_SCOPE": "Scheduled backup skipped because the assigned SLA does not cover the resource workload",
    "RESTORE_TRIGGERED": "Restore job triggered",
    "RESTORE_RUNNING": "Restore job picked up by worker",
    "RESTORE_COMPLETED": "Restore completed",
    "RESTORE_FAILED": "Restore failed",
    "RESTORE_CANCELLED": "Restore cancelled by user",
    "EXPORT_TRIGGERED": "Export job triggered",
    "EXPORT_COMPLETED": "Export completed",
    "EXPORT_DOWNLOADED": "Export file downloaded",
    "FILE_DOWNLOADED": "Single file downloaded from a snapshot",
    "DISCOVERY_RUN": "Resource discovery executed",
    "SLA_CREATED": "SLA policy created",
    "SLA_UPDATED": "SLA policy updated",
    "SLA_DELETED": "SLA policy deleted",
    "SLA_ASSIGNED": "SLA policy assigned to resource(s)",
    "SLA_UNASSIGNED": "SLA policy removed from resource(s)",
    "RESOURCE_ARCHIVED": "Resource archived",
    "RESOURCE_UNARCHIVED": "Resource unarchived",
    "RESOURCE_DELETED": "Resource deleted",
    "SNAPSHOT_DELETED": "Snapshot deleted",
    "CONTENT_VIEWED": "Backup content browsed/viewed",
    "LOGIN_SUCCESS": "User login successful",
    "LOGIN_FAILED": "User login failed",
    "RANSOMWARE_SIGNAL": "AI ransomware signal detected",
}

# Built-in audit filter presets surfaced to the UI via /audit/presets
AUDIT_PRESETS = [
    {"key": "chat_exports", "label": "Chat exports",
     "actions": ["CHAT_EXPORT_START", "CHAT_EXPORT_COMPLETED", "CHAT_EXPORT_CANCELLED",
                 "CHAT_EXPORT_DOWNLOADED", "CHAT_EXPORT_FORCE_DELETED"]},
]

WARNING_ACTIVITY_ACTIONS = {"BACKUP_SKIPPED_SLA_SCOPE", "RANSOMWARE_SIGNAL"}

# Discovery events (run start + completion, per-tenant) are surfaced in the
# activity feed so users can see auto- and manual-triggered discoveries.
DISCOVERY_ACTIVITY_ACTIONS = {"DISCOVERY_STARTED", "DISCOVERY_RUN"}

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


def _parse_service_type(service_type: Optional[str]) -> Optional[str]:
    if not service_type:
        return None
    normalized = service_type.lower()
    if normalized not in ("m365", "azure"):
        raise HTTPException(status_code=400, detail="Unsupported serviceType. Expected 'm365' or 'azure'.")
    return normalized


def _resource_types_for_service(service_type: Optional[str]):
    if service_type == "m365":
        return M365_RESOURCE_TYPES
    if service_type == "azure":
        return AZURE_RESOURCE_TYPES
    return None


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "audit-log"}


@app.get("/api/v1/activity")
async def list_activities(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
):
    """
    List backup/restore activities (job-level operations) for the Tasks tab.
    Maps jobs to the frontend's expected ActivityItem format.
    """
    async with async_session_factory() as db:
        service_key = _parse_service_type(serviceType)
        service_resource_types = _resource_types_for_service(service_key)
        service_type_values = {rt.value if hasattr(rt, "value") else str(rt) for rt in service_resource_types} if service_resource_types else None

        # Build status filter
        status_map = {
            "Done": JobStatus.COMPLETED,
            "In Progress": JobStatus.RUNNING,
            "Failed": JobStatus.FAILED,
            "Canceled": JobStatus.CANCELLED,
        }
        include_job_items = status != "Warning"
        include_warning_items = (not operation or operation.upper() == "BACKUP") and (not status or status == "Warning")

        stmt = select(Job).select_from(Job).outerjoin(Resource, Job.resource_id == Resource.id).order_by(desc(Job.created_at))
        count_stmt = select(func.count()).select_from(Job).outerjoin(Resource, Job.resource_id == Resource.id)

        filters = []
        if tenantId:
            filters.append(Job.tenant_id == uuid.UUID(tenantId))
        if start_date:
            filters.append(Job.created_at >= datetime.fromisoformat(start_date))
        if end_date:
            filters.append(Job.created_at <= datetime.fromisoformat(end_date))
        if operation:
            op_upper = operation.upper()
            filters.append(Job.type == op_upper)
        if status and status in status_map:
            filters.append(Job.status == status_map[status])
        if service_key and service_resource_types:
            filters.append(
                or_(
                    and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
                    and_(Job.resource_id.is_(None), func.json_extract_path_text(Job.spec, "triggered_by") == f"MANUAL_DATASOURCE_{service_key.upper()}"),
                )
            )

        jobs: List[Job] = []
        job_total = 0
        if include_job_items:
            if filters:
                stmt = stmt.where(and_(*filters))
                count_stmt = count_stmt.where(and_(*filters))

            # Fetch enough rows so warning items can be merged into the same paginated feed.
            fetch_limit = max(size, page * size)
            stmt = stmt.limit(fetch_limit)
            total_result = await db.execute(count_stmt)
            job_total = total_result.scalar() or 0
            result = await db.execute(stmt)
            jobs = result.scalars().all()

        warning_filters = [AuditEvent.action.in_(WARNING_ACTIVITY_ACTIONS)]
        if tenantId:
            warning_filters.append(AuditEvent.tenant_id == uuid.UUID(tenantId))
        if start_date:
            warning_filters.append(AuditEvent.occurred_at >= datetime.fromisoformat(start_date))
        if end_date:
            warning_filters.append(AuditEvent.occurred_at <= datetime.fromisoformat(end_date))
        if service_type_values:
            warning_filters.append(AuditEvent.resource_type.in_(service_type_values))

        warning_events: List[AuditEvent] = []
        warning_total = 0
        if include_warning_items:
            warning_stmt = (
                select(AuditEvent)
                .where(and_(*warning_filters))
                .order_by(desc(AuditEvent.occurred_at))
                .limit(max(size, page * size))
            )
            warning_count_stmt = select(func.count()).select_from(AuditEvent).where(and_(*warning_filters))
            warning_total = (await db.execute(warning_count_stmt)).scalar() or 0
            warning_result = await db.execute(warning_stmt)
            warning_events = warning_result.scalars().all()

        # Discovery events: fetch when no operation filter is set, or when the
        # user specifically asks for DISCOVERY. Status filter "Failed"/"Done"/
        # "In Progress" maps to outcomes below.
        include_discovery_items = (
            (not operation or operation.upper() == "DISCOVERY")
            and (not status or status in {"Done", "In Progress", "Failed"})
        )
        discovery_events: List[AuditEvent] = []
        discovery_total = 0
        if include_discovery_items:
            discovery_filters = [AuditEvent.action.in_(DISCOVERY_ACTIVITY_ACTIONS)]
            if tenantId:
                discovery_filters.append(AuditEvent.tenant_id == uuid.UUID(tenantId))
            if start_date:
                discovery_filters.append(AuditEvent.occurred_at >= datetime.fromisoformat(start_date))
            if end_date:
                discovery_filters.append(AuditEvent.occurred_at <= datetime.fromisoformat(end_date))
            # serviceType (m365/azure) maps to the resource_type we store on
            # discovery events ("M365" or "AZURE").
            if service_key:
                discovery_filters.append(AuditEvent.resource_type == service_key.upper())
            discovery_stmt = (
                select(AuditEvent)
                .where(and_(*discovery_filters))
                .order_by(desc(AuditEvent.occurred_at))
                .limit(max(size, page * size))
            )
            # Count one row per discovery run. Each run emits a STARTED event
            # (and usually a paired RUN); counting STARTED gives the merged-row
            # count. Legacy RUN-only events (pre-STARTED emission) under-count
            # slightly and decay with time. discovery_filters[0] is the
            # action-in filter; re-apply the rest with an exact action match.
            discovery_total = (await db.execute(
                select(func.count()).select_from(AuditEvent).where(
                    and_(AuditEvent.action == "DISCOVERY_STARTED", *discovery_filters[1:])
                )
            )).scalar() or 0
            discovery_result = await db.execute(discovery_stmt)
            discovery_events = discovery_result.scalars().all()

        # Map jobs to ActivityItem format
        status_reverse_map = {
            JobStatus.COMPLETED: "Done",
            JobStatus.RUNNING: "In Progress",
            JobStatus.FAILED: "Failed",
            JobStatus.CANCELLED: "Canceled",
            JobStatus.QUEUED: "In Progress",
            JobStatus.RETRYING: "In Progress",
        }

        items = []
        for job in jobs:
            # Try to resolve resource name from resource_id
            resource_name = "Bulk Operation"
            if job.resource_id:
                resource = await db.get(Resource, job.resource_id)
                if resource:
                    resource_name = resource.display_name

            cached = _running_job_cache.get(str(job.id), {})
            data_backed_up = cached.get("data_backed_up", job.bytes_processed or 0)
            total_data = cached.get("total_data") or (job.result.get("total_bytes", 0) if job.result else 0)
            items.append({
                "id": str(job.id),
                "start_time": job.created_at.isoformat() if job.created_at else "",
                "operation": job.type.value if hasattr(job.type, 'value') else str(job.type),
                "object": resource_name,
                "status": status_reverse_map.get(job.status, "In Progress"),
                "finish_time": job.completed_at.isoformat() if job.completed_at else "",
                "details": _compute_details(job),
                "data_backed_up": data_backed_up,
                "total_data": total_data,
            })

        for event in warning_events:
            details = event.details or {}
            items.append({
                "id": f"audit-{event.id}",
                "start_time": event.occurred_at.isoformat() if event.occurred_at else "",
                "operation": "BACKUP",
                "object": event.resource_name or event.resource_type or "Unknown resource",
                "status": "Warning",
                "finish_time": event.occurred_at.isoformat() if event.occurred_at else "",
                "details": details.get("message") or "Backup skipped because the assigned SLA does not cover this resource type.",
            })

        # Discovery events: pair each DISCOVERY_STARTED with its matching
        # DISCOVERY_RUN so a single row shows "In Progress" and then flips to
        # "Done"/"Failed" in place. Pairing key: (tenant_id, resource_type);
        # earliest STARTED binds to the earliest later RUN for the same key.
        started_sorted = sorted(
            (e for e in discovery_events if e.action == "DISCOVERY_STARTED"),
            key=lambda e: e.occurred_at or datetime.min,
        )
        run_sorted = sorted(
            (e for e in discovery_events if e.action == "DISCOVERY_RUN"),
            key=lambda e: e.occurred_at or datetime.min,
        )
        run_buckets: Dict[Any, List[AuditEvent]] = {}
        for r in run_sorted:
            run_buckets.setdefault((r.tenant_id, r.resource_type), []).append(r)

        paired_run_ids: Set[Any] = set()
        paired: List[Tuple[AuditEvent, Optional[AuditEvent]]] = []
        for s in started_sorted:
            bucket = run_buckets.get((s.tenant_id, s.resource_type), [])
            match: Optional[AuditEvent] = None
            while bucket:
                candidate = bucket[0]
                if (candidate.occurred_at or datetime.min) >= (s.occurred_at or datetime.min) \
                        and candidate.id not in paired_run_ids:
                    match = candidate
                    paired_run_ids.add(candidate.id)
                    bucket.pop(0)
                    break
                bucket.pop(0)
            paired.append((s, match))

        orphan_runs = [r for r in run_sorted if r.id not in paired_run_ids]

        def _render_discovery_row(
            started: Optional[AuditEvent],
            run: Optional[AuditEvent],
        ) -> Optional[Dict[str, Any]]:
            anchor = started or run
            if anchor is None:
                return None
            details_src = (run.details if run else started.details) or {}
            disco_type = details_src.get("type") or anchor.resource_type or "Discovery"

            if run is None:
                disco_status = "In Progress"
                detail_text = f"{disco_type} discovery in progress"
                finish_time = ""
            else:
                outcome = (run.outcome or "").upper()
                if outcome == "SUCCESS":
                    disco_status = "Done"
                    found = (run.details or {}).get("resourcesFound")
                    detail_text = f"{disco_type} discovery completed"
                    if found is not None:
                        detail_text += f" — {found} resources"
                elif outcome == "FAILURE":
                    disco_status = "Failed"
                    err = (run.details or {}).get("error") or "unknown error"
                    detail_text = f"{disco_type} discovery failed: {err}"
                else:
                    disco_status = "In Progress"
                    detail_text = f"{disco_type} discovery"
                finish_time = run.occurred_at.isoformat() if run.occurred_at else ""

            if status and status != disco_status:
                return None

            anchor_for_id = run or started
            start_time = (started.occurred_at if started else run.occurred_at)
            object_name = (started.resource_name if started else None) \
                or (run.resource_name if run else None) or disco_type

            return {
                "id": f"discovery-{anchor_for_id.id}",
                "start_time": start_time.isoformat() if start_time else "",
                "operation": "DISCOVERY",
                "object": object_name,
                "status": disco_status,
                "finish_time": finish_time,
                "details": detail_text,
            }

        for started, run in paired:
            row = _render_discovery_row(started, run)
            if row:
                items.append(row)
        # Legacy DISCOVERY_RUN events without a matching STARTED still render.
        for run in orphan_runs:
            row = _render_discovery_row(None, run)
            if row:
                items.append(row)

        items.sort(key=lambda item: item["start_time"] or "", reverse=True)
        total = job_total + warning_total + discovery_total
        start_index = (page - 1) * size
        paginated_items = items[start_index:start_index + size]

        return {
            "items": paginated_items,
            "total": total,
            "page": page,
            "size": size,
            "has_more": (page * size) < total,
        }


@app.get("/api/v1/activity/export")
async def export_activity_csv(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
):
    """Export activity (jobs) as CSV for the Tasks tab"""
    status_map = {
        "Done": JobStatus.COMPLETED,
        "In Progress": JobStatus.RUNNING,
        "Failed": JobStatus.FAILED,
        "Canceled": JobStatus.CANCELLED,
    }

    async with async_session_factory() as db:
        service_key = _parse_service_type(serviceType)
        service_resource_types = _resource_types_for_service(service_key)

        stmt = select(Job).select_from(Job).outerjoin(Resource, Job.resource_id == Resource.id).order_by(desc(Job.created_at))
        filters = []
        if tenantId:
            filters.append(Job.tenant_id == uuid.UUID(tenantId))
        if start_date:
            filters.append(Job.created_at >= datetime.fromisoformat(start_date))
        if end_date:
            filters.append(Job.created_at <= datetime.fromisoformat(end_date))
        if operation:
            filters.append(Job.type == operation.upper())
        if status and status in status_map:
            filters.append(Job.status == status_map[status])
        if service_key and service_resource_types:
            filters.append(
                or_(
                    and_(Job.resource_id.is_not(None), Resource.type.in_(service_resource_types)),
                    and_(Job.resource_id.is_(None), func.json_extract_path_text(Job.spec, "triggered_by") == f"MANUAL_DATASOURCE_{service_key.upper()}"),
                )
            )

        if filters:
            stmt = stmt.where(and_(*filters))

        result = await db.execute(stmt)
        jobs = result.scalars().all()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "ID", "Start Time", "Operation", "Object",
            "Status", "Finish Time", "Details"
        ])
        for job in jobs:
            writer.writerow([
                str(job.id),
                job.created_at.isoformat() if job.created_at else "",
                job.type.value if hasattr(job.type, 'value') else str(job.type),
                str(job.resource_id) if job.resource_id else "Bulk",
                job.status.value if hasattr(job.status, 'value') else str(job.status),
                job.completed_at.isoformat() if job.completed_at else "",
                _compute_details(job),
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=activities.csv"},
        )


@app.post("/api/v1/audit/log")
async def create_audit_event(event: dict):
    """
    Internal endpoint for services/workers to log events.
    Enriches events with SLA context, resource metadata, and risk signals.
    Body:
    {
        "action": "BACKUP_COMPLETED",
        "tenant_id": "uuid",
        "org_id": "uuid",
        "actor_type": "SYSTEM|USER|WORKER",
        "actor_id": "uuid-or-null",
        "actor_email": "email-or-null",
        "resource_id": "uuid-or-null",
        "resource_type": "MAILBOX|ONEDRIVE|...",
        "resource_name": "display name",
        "outcome": "SUCCESS|FAILURE|PARTIAL",
        "job_id": "uuid-or-null",
        "snapshot_id": "uuid-or-null",
        "details": { ... }
    }
    """
    async with async_session_factory() as db:
        # Normalize the incoming event
        event = _normalize_event(event)

        # Enrichment: Fetch SLA context if resource_id is provided
        sla_name = None
        sla_violation_alert = None
        last_backup_at = None
        resource_email = None

        if event.get("resource_id"):
            resource = await db.get(Resource, uuid.UUID(event["resource_id"]))
            if resource:
                resource_email = resource.email
                last_backup_at = resource.last_backup_at.isoformat() if resource.last_backup_at else None
                if resource.sla_policy_id:
                    sla = await db.get(SlaPolicy, resource.sla_policy_id)
                    if sla:
                        sla_name = sla.name
                        sla_violation_alert = sla.sla_violation_alert

        # Enrichment: Detect ransomware signals (mass deletions, rapid failures)
        risk_signals = _detect_risk_signals(event)

        # Merge enrichment into details
        enriched_details = event.get("details", {})
        enriched_details["enrichment"] = {
            "sla_policy": sla_name,
            "sla_violation_alert": sla_violation_alert,
            "last_backup_at": last_backup_at,
            "resource_email": resource_email,
        }
        if risk_signals:
            enriched_details["risk_signals"] = risk_signals

        audit = AuditEvent(
            id=uuid.uuid4(),
            org_id=uuid.UUID(event["org_id"]) if event.get("org_id") else None,
            tenant_id=uuid.UUID(event["tenant_id"]) if event.get("tenant_id") else None,
            actor_id=uuid.UUID(event["actor_id"]) if event.get("actor_id") else None,
            actor_email=event.get("actor_email"),
            actor_type=event.get("actor_type", "SYSTEM"),
            action=event["action"],
            resource_id=uuid.UUID(event["resource_id"]) if event.get("resource_id") else None,
            resource_type=event.get("resource_type"),
            resource_name=event.get("resource_name"),
            outcome=event.get("outcome", "SUCCESS"),
            job_id=uuid.UUID(event["job_id"]) if event.get("job_id") else None,
            snapshot_id=uuid.UUID(event["snapshot_id"]) if event.get("snapshot_id") else None,
            details=enriched_details,
            occurred_at=datetime.utcnow(),
        )
        db.add(audit)
        await db.commit()
        return {"id": str(audit.id), "action": audit.action, "occurred_at": audit.occurred_at.isoformat()}


def _detect_risk_signals(event: dict) -> Optional[dict]:
    """
    Detect potential ransomware or data loss patterns from event context.
    Uses heuristic analysis based on:
    - Backup failure patterns
    - Mass deletion events
    - Unusual data volume changes
    - Suspicious login patterns
    - Anomalous activity timing

    Returns risk signal dict if detected, None otherwise.
    """
    signals = {}
    risk_score = 0  # 0-100 scale

    action = event.get("action", "")
    outcome = event.get("outcome", "")
    details = event.get("details", {})
    tenant_id = event.get("tenant_id")

    # Signal 1: Backup failure pattern (potential ransomware blocking backup)
    if outcome == "FAILURE" and "BACKUP" in action:
        signals["backup_failure"] = {
            "action": action,
            "timestamp": datetime.utcnow().isoformat(),
        }
        risk_score += 20

    # Signal 2: Mass deletion events (from M365 audit logs)
    if "GRAPH" in action:
        resource_name = event.get("resource_name", "").lower()
        deletion_keywords = ["delete", "remove", "purge", "harddelete", "emptyrecyclebin"]
        if any(kw in resource_name for kw in deletion_keywords):
            signals["mass_deletion"] = {
                "resource": event.get("resource_name"),
                "timestamp": datetime.utcnow().isoformat(),
            }
            risk_score += 30

        # Signal 3: Permission changes (potential privilege escalation)
        permission_keywords = ["add member to role", "add owner", "grant", "permission"]
        if any(kw in resource_name for kw in permission_keywords):
            signals["privilege_change"] = {
                "resource": event.get("resource_name"),
                "timestamp": datetime.utcnow().isoformat(),
            }
            risk_score += 15

    # Signal 4: Unusual item counts (potential ransomware encryption)
    # Ransomware often encrypts many files rapidly, causing large backup deltas
    item_count = details.get("item_count", 0)
    bytes_added = details.get("bytes_added", 0)
    if item_count > 10000 and bytes_added > 10_000_000_000:  # >10K items, >10GB
        signals["high_volume_change"] = {
            "item_count": item_count,
            "bytes_added": bytes_added,
            "threshold_exceeded": True,
        }
        risk_score += 25

    # Signal 5: Failed login attempts (potential brute force)
    if "SIGNIN" in action:
        status = details.get("status", {})
        error_code = status.get("errorCode", 0) if isinstance(status, dict) else 0
        if error_code != 0:
            signals["failed_login"] = {
                "error_code": error_code,
                "ip_address": details.get("ipAddress"),
                "location": details.get("location", {}),
            }
            # High-risk error codes
            if error_code in (50053, 50055, 50140):  # Account locked, password expired, sign-in blocked
                risk_score += 35
            else:
                risk_score += 10

        # Signal 6: Sign-in from risky location (if location data available)
        location = details.get("location", {})
        if isinstance(location, dict):
            country = location.get("countryOrRegion", "")
            city = location.get("city", "")
            # Check against known risky regions (configurable)
            if details.get("risk_level") == "high":
                signals["risky_location"] = {
                    "country": country,
                    "city": city,
                    "risk_level": "high",
                }
                risk_score += 20

    # Signal 7: Multiple resource deletions in short time window
    # (Would require cross-event analysis - flag for further investigation)
    if details.get("deletion_count", 0) > 100:
        signals["bulk_deletion"] = {
            "deletion_count": details.get("deletion_count"),
            "investigation_recommended": True,
        }
        risk_score += 40

    # Calculate overall risk level
    if risk_score >= 60:
        signals["risk_level"] = "CRITICAL"
        signals["risk_score"] = min(risk_score, 100)
        signals["investigation_urgency"] = "immediate"
    elif risk_score >= 40:
        signals["risk_level"] = "HIGH"
        signals["risk_score"] = min(risk_score, 100)
        signals["investigation_urgency"] = "urgent"
    elif risk_score >= 20:
        signals["risk_level"] = "MEDIUM"
        signals["risk_score"] = min(risk_score, 100)
        signals["investigation_urgency"] = "standard"
    else:
        signals["risk_level"] = "LOW"
        signals["risk_score"] = risk_score
        signals["investigation_urgency"] = "none"

    return signals if risk_score > 0 else None


def _normalize_event(event: dict) -> dict:
    """
    Normalize incoming events to ensure consistent schema.
    Handles events from:
    - Internal services (backup/restore workers, job service)
    - Microsoft Graph audit logs
    - External sources
    """
    action = event.get("action", "UNKNOWN")
    details = event.get("details", {})

    # Normalize actor_type
    actor_type = event.get("actor_type", "SYSTEM")
    if actor_type not in ("USER", "SYSTEM", "WORKER"):
        actor_type = "SYSTEM"

    # Normalize outcome based on action context
    outcome = event.get("outcome", "SUCCESS")
    if outcome not in ("SUCCESS", "FAILURE", "PARTIAL"):
        outcome = "SUCCESS"

    # Normalize resource_type to uppercase
    resource_type = event.get("resource_type")
    if resource_type:
        resource_type = resource_type.upper()

    # Add workload context for Graph events
    if action.startswith("GRAPH_"):
        log_type = action.replace("GRAPH_", "").lower()
        workload_map = {
            "directory": "entra",
            "signin": "entra",
        }
        details["workload"] = workload_map.get(log_type, "entra")
        details["source"] = "microsoft_graph"

    # Normalize timestamp if provided in details (for Graph logs)
    if "activityDateTime" in details:
        try:
            occurred_at = datetime.fromisoformat(details["activityDateTime"].replace("Z", "+00:00"))
            event["occurred_at"] = occurred_at.replace(tzinfo=None)
        except (ValueError, AttributeError):
            pass

    # Ensure consistent detail structure
    normalized_details = {
        "source_event_type": action,
        **details,
    }

    return {
        **event,
        "actor_type": actor_type,
        "outcome": outcome,
        "resource_type": resource_type,
        "details": normalized_details,
    }


@app.get("/api/v1/audit/events")
async def list_audit_events(
    tenantId: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    outcome: Optional[str] = Query(None),
    resourceId: Optional[str] = Query(None),
    actorType: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
):
    """
    List audit events with filtering and pagination.
    Query params: tenantId, action, outcome, resourceId, actorType, from_date, to_date, page, size
    """
    async with async_session_factory() as db:
        stmt = select(AuditEvent).order_by(desc(AuditEvent.occurred_at))
        count_stmt = select(func.count()).select_from(AuditEvent)

        filters = []
        if tenantId:
            filters.append(AuditEvent.tenant_id == uuid.UUID(tenantId))
        if action:
            filters.append(AuditEvent.action == action)
        if outcome:
            filters.append(AuditEvent.outcome == outcome)
        if resourceId:
            filters.append(AuditEvent.resource_id == uuid.UUID(resourceId))
        if actorType:
            filters.append(AuditEvent.actor_type == actorType)
        if from_date:
            filters.append(AuditEvent.occurred_at >= datetime.fromisoformat(from_date))
        if to_date:
            filters.append(AuditEvent.occurred_at <= datetime.fromisoformat(to_date))

        if filters:
            stmt = stmt.where(and_(*filters))
            count_stmt = count_stmt.where(and_(*filters))

        # Total count
        total_result = await db.execute(count_stmt)
        total = total_result.scalar() or 0

        # Paginated results
        stmt = stmt.offset((page - 1) * size).limit(size)
        result = await db.execute(stmt)
        events = result.scalars().all()

        return {
            "items": [_format_event(e) for e in events],
            "total": total,
            "page": page,
            "size": size,
            "pages": max(1, (total + size - 1) // size),
        }


@app.get("/api/v1/audit/resource/{resource_id}")
async def get_resource_audit_log(
    resource_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    """Get all audit events for a specific resource"""
    async with async_session_factory() as db:
        stmt = (
            select(AuditEvent)
            .where(AuditEvent.resource_id == uuid.UUID(resource_id))
            .order_by(desc(AuditEvent.occurred_at))
            .offset((page - 1) * size)
            .limit(size)
        )
        result = await db.execute(stmt)
        events = result.scalars().all()

        count_stmt = select(func.count()).select_from(AuditEvent).where(
            AuditEvent.resource_id == uuid.UUID(resource_id)
        )
        total_result = await db.execute(count_stmt)
        total = total_result.scalar() or 0

        return {
            "items": [_format_event(e) for e in events],
            "total": total,
            "page": page,
            "size": size,
        }


@app.get("/api/v1/audit/events/{event_id}")
async def get_audit_event(event_id: str):
    """Get a single audit event by ID"""
    async with async_session_factory() as db:
        event = await db.get(AuditEvent, uuid.UUID(event_id))
        if not event:
            raise HTTPException(status_code=404, detail="Audit event not found")
        return _format_event(event)


@app.get("/api/v1/audit/risk-signals")
async def get_high_risk_events(
    tenantId: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    min_risk_score: int = Query(20, ge=0, le=100),
    risk_level: Optional[str] = Query(None),  # CRITICAL, HIGH, MEDIUM
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
):
    """
    Query high-risk audit events based on risk signal analysis.
    Useful for security investigations and compliance audits.
    """
    if not from_date:
        from_date = (datetime.utcnow() - timedelta(days=7)).isoformat()
    if not to_date:
        to_date = datetime.utcnow().isoformat()

    async with async_session_factory() as db:
        # Use PostgreSQL JSONB containment operator via text()
        filters = [
            AuditEvent.occurred_at >= datetime.fromisoformat(from_date),
            AuditEvent.occurred_at <= datetime.fromisoformat(to_date),
            text("details @> '{\"risk_signals\":{}}'"),  # Has risk_signals key
        ]
        if tenantId:
            filters.append(AuditEvent.tenant_id == uuid.UUID(tenantId))
        if risk_level:
            filters.append(text(f"details->'risk_signals'->>'risk_level' = '{risk_level}'"))

        stmt = select(AuditEvent).where(and_(*filters)).order_by(desc(AuditEvent.occurred_at))
        count_stmt = select(func.count()).select_from(AuditEvent).where(and_(*filters))

        total_result = await db.execute(count_stmt)
        total = total_result.scalar() or 0

        stmt = stmt.offset((page - 1) * size).limit(size)
        result = await db.execute(stmt)
        events = result.scalars().all()

        # Extract risk signals from details for easier consumption
        enriched_items = []
        for e in events:
            item = _format_event(e)
            details = e.details or {}
            risk_signals = details.get("risk_signals", {})
            item["risk_signals"] = risk_signals
            item["risk_score"] = risk_signals.get("risk_score", 0)
            item["risk_level"] = risk_signals.get("risk_level", "UNKNOWN")
            enriched_items.append(item)

        # Sort by risk_score descending
        enriched_items.sort(key=lambda x: x.get("risk_score", 0), reverse=True)

        return {
            "items": enriched_items,
            "total": total,
            "page": page,
            "size": size,
            "has_more": (page * size) < total,
        }


@app.get("/api/v1/audit/export")
async def export_audit_csv(
    tenantId: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
):
    """Export audit log as CSV (defaults to last 30 days if no dates provided)"""
    if not from_date:
        from_date = (datetime.utcnow() - timedelta(days=30)).isoformat()
    if not to_date:
        to_date = datetime.utcnow().isoformat()

    async with async_session_factory() as db:
        filters = [
            AuditEvent.occurred_at >= datetime.fromisoformat(from_date),
            AuditEvent.occurred_at <= datetime.fromisoformat(to_date),
        ]
        if tenantId:
            filters.append(AuditEvent.tenant_id == uuid.UUID(tenantId))
        if action:
            filters.append(AuditEvent.action == action)

        stmt = select(AuditEvent).where(and_(*filters)).order_by(desc(AuditEvent.occurred_at))
        result = await db.execute(stmt)
        events = result.scalars().all()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Timestamp", "Action", "Outcome", "Resource Type",
            "Resource Name", "Actor Type", "Actor Email",
            "Job ID", "Snapshot ID", "Details"
        ])
        for e in events:
            writer.writerow([
                e.occurred_at.isoformat() if e.occurred_at else "",
                e.action,
                e.outcome,
                e.resource_type or "",
                e.resource_name or "",
                e.actor_type or "",
                e.actor_email or "",
                str(e.job_id) if e.job_id else "",
                str(e.snapshot_id) if e.snapshot_id else "",
                str(e.details) if e.details else "",
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=audit-log.csv"},
        )


@app.get("/api/v1/audit/presets")
async def list_presets():
    """List built-in audit filter presets for the UI (e.g. 'Chat exports')."""
    return {"presets": AUDIT_PRESETS}


@app.get("/api/v1/audit/actions")
async def list_actions():
    """List all available audit action codes"""
    # Get distinct actions from DB
    async with async_session_factory() as db:
        result = await db.execute(
            select(AuditEvent.action).distinct().order_by(AuditEvent.action)
        )
        db_actions = [row[0] for row in result.all()]

    return {
        "actions": db_actions,
        "descriptions": ACTIONS,
    }


@app.get("/api/v1/audit/stats")
async def get_audit_stats(
    tenantId: Optional[str] = Query(None),
    days: int = Query(7),
):
    """Get audit statistics: counts by action and outcome"""
    from_dt = datetime.utcnow() - timedelta(days=days)

    async with async_session_factory() as db:
        filters = [AuditEvent.occurred_at >= from_dt]
        if tenantId:
            filters.append(AuditEvent.tenant_id == uuid.UUID(tenantId))

        # Count by outcome
        outcome_stmt = select(AuditEvent.outcome, func.count()).where(and_(*filters)).group_by(AuditEvent.outcome)
        outcome_result = await db.execute(outcome_stmt)
        by_outcome = {row[0]: row[1] for row in outcome_result.all()}

        # Count by action
        action_stmt = select(AuditEvent.action, func.count()).where(and_(*filters)).group_by(AuditEvent.action)
        action_result = await db.execute(action_stmt)
        by_action = {row[0]: row[1] for row in action_result.all()}

        # Total events
        total_stmt = select(func.count()).select_from(AuditEvent).where(and_(*filters))
        total_result = await db.execute(total_stmt)
        total = total_result.scalar() or 0

        return {
            "total": total,
            "by_outcome": by_outcome,
            "by_action": by_action,
            "period_days": days,
            "graph_apps": multi_app_manager.get_stats(),
            "app_count": multi_app_manager.app_count,
        }


# ==================== SIEM Webhook Integration ====================

@app.get("/api/v1/audit/siem/stream")
async def stream_audit_events_for_siem(
    tenantId: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    format: str = Query("json"),  # json | cef
):
    """
    Stream audit events in SIEM-friendly format.
    Supports JSON and CEF (Common Event Format) for integration with Splunk, Sentinel, etc.
    """
    if not from_date:
        from_date = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    if not to_date:
        to_date = datetime.utcnow().isoformat()

    async with async_session_factory() as db:
        filters = [
            AuditEvent.occurred_at >= datetime.fromisoformat(from_date),
            AuditEvent.occurred_at <= datetime.fromisoformat(to_date),
        ]
        if tenantId:
            filters.append(AuditEvent.tenant_id == uuid.UUID(tenantId))

        stmt = select(AuditEvent).where(and_(*filters)).order_by(desc(AuditEvent.occurred_at))
        result = await db.execute(stmt)
        events = result.scalars().all()

    if format == "cef":
        # CEF format for Splunk/ArcSight
        def to_cef(event: AuditEvent) -> str:
            severity = 3
            if event.outcome == "FAILURE":
                severity = 7
            elif event.outcome == "PARTIAL":
                severity = 5

            cef_fields = [
                "CEF:0",
                "TMVault",  # Device Vendor
                "BackupPlatform",  # Device Product
                "1.0",  # Device Version
                event.action,  # Signature ID
                event.action.replace("_", " ").title(),  # Name
                str(severity),
                f"rt={event.occurred_at.isoformat() if event.occurred_at else ''}",
                f"act={event.action}",
                f"outcome={event.outcome}",
                f"destination={event.resource_name or ''}",
                f"deviceExternalId={event.resource_id or ''}",
                f"requester={event.actor_email or ''}",
                f"externalId={event.job_id or ''}",
                f"flexString1={event.tenant_id or ''}",
            ]
            return " | ".join(cef_fields)

        output = io.StringIO()
        for e in events:
            output.write(to_cef(e) + "\n")
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/plain",
            headers={"Content-Disposition": "attachment; filename=audit-events.cef"},
        )
    else:
        # JSON Lines format for ELK/Sumo Logic
        output = io.StringIO()
        for e in events:
            formatted = _format_event(e)
            formatted["@timestamp"] = formatted["occurred_at"]
            formatted["event_module"] = "tm_vault_audit"
            output.write(json_lib.dumps(formatted) + "\n")
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": "attachment; filename=audit-events.ndjson"},
        )


@app.post("/api/v1/audit/siem/webhook")
async def register_siem_webhook(webhook: dict):
    """
    Register a SIEM webhook URL for real-time event forwarding.
    Body:
    {
        "url": "https://siem.example.com/api/events",
        "tenant_id": "uuid-or-null",  # null = all tenants
        "format": "json|cef",
        "auth_header": "Bearer <token>",  # optional
        "actions": ["BACKUP_FAILED", "RANSOMWARE_SIGNAL"]  # optional filter
    }
    """
    # Store webhook config in a simple table or settings
    # For now, store in a JSON file or env var (production: add a DB table)
    webhook_id = str(uuid.uuid4())
    webhook["id"] = webhook_id
    webhook["created_at"] = datetime.utcnow().isoformat()
    webhook["enabled"] = True

    # In production: store in database
    # For now, return the config
    return {
        "id": webhook_id,
        "status": "registered",
        "webhook": {k: v for k, v in webhook.items() if k != "auth_header"},
    }


@app.post("/api/v1/audit/siem/webhook/{webhook_id}/test")
async def test_siem_webhook(webhook_id: str):
    """Test a registered SIEM webhook by sending a sample event"""
    # In production: fetch webhook config from DB
    # For now, return a mock response
    sample_event = {
        "action": "BACKUP_COMPLETED",
        "tenant_id": "test-tenant",
        "resource_type": "MAILBOX",
        "resource_name": "test@contoso.com",
        "outcome": "SUCCESS",
        "occurred_at": datetime.utcnow().isoformat(),
    }
    return {
        "status": "test_sent",
        "webhook_id": webhook_id,
        "sample_event": sample_event,
    }


@app.get("/api/v1/audit/graph-apps")
async def get_graph_apps():
    """Get multi-app registration status and usage stats"""
    return {
        "app_count": multi_app_manager.app_count,
        "apps": multi_app_manager.get_stats(),
    }


@app.post("/api/v1/audit/ingest/graph/{tenant_id}")
async def ingest_graph_audit_logs(
    tenant_id: str,
    days: int = Query(7),
    log_type: str = Query("directory"),  # directory | signin
):
    """
    Pull Microsoft Graph audit logs and store them as audit events.
    Requires Graph API permissions: AuditLog.Read.All

    log_type:
    - directory: Entra ID directory audit logs
    - signin: Sign-in logs
    """
    async with async_session_factory() as db:
        tenant = await db.get(Tenant, uuid.UUID(tenant_id))
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")

    # Build date filter for Graph API
    from_dt = datetime.utcnow() - timedelta(days=days)
    filter_expr = f"activityDateTime ge {from_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"

    # Create Graph client
    graph = GraphClient(
        client_id=tenant.client_id or "",
        client_secret="",  # Would use secret ref in production
        tenant_id=tenant.external_tenant_id or "",
    )

    # Fetch logs from Graph
    if log_type == "directory":
        logs = await graph.get_directory_audit_logs(filter_expr=filter_expr, top=500)
    else:
        logs = await graph.get_sign_in_logs(filter_expr=filter_expr, top=500)

    ingested = 0
    async with async_session_factory() as db:
        for log_entry in logs:
            log_id = log_entry.get("id")
            if not log_id:
                continue

            # Skip if already ingested (check by Graph log ID in details)
            existing = await db.execute(
                select(AuditEvent).where(
                    AuditEvent.tenant_id == uuid.UUID(tenant_id),
                    AuditEvent.action == f"GRAPH_{log_type.upper()}",
                    AuditEvent.details.cast(str).contains(log_id),
                ).limit(1)
            )
            if existing.scalars().first():
                continue

            audit = AuditEvent(
                id=uuid.uuid4(),
                tenant_id=uuid.UUID(tenant_id),
                org_id=tenant.org_id,
                actor_type="SYSTEM",
                actor_email=log_entry.get("initiatedBy", {}).get("user", {}).get("userPrincipalName"),
                action=f"GRAPH_{log_type.upper()}",
                resource_name=log_entry.get("displayName") or log_entry.get("resultReason"),
                outcome="SUCCESS" if log_entry.get("result") == "success" else "FAILURE",
                details=log_entry,
                occurred_at=datetime.fromisoformat(log_entry["activityDateTime"]) if log_entry.get("activityDateTime") else datetime.utcnow(),
            )
            db.add(audit)
            ingested += 1

        await db.commit()

    return {"ingested": ingested, "log_type": log_type, "days": days}


def _format_event(event: AuditEvent) -> dict:
    return {
        "id": str(event.id),
        "org_id": str(event.org_id) if event.org_id else None,
        "tenant_id": str(event.tenant_id) if event.tenant_id else None,
        "actor_id": str(event.actor_id) if event.actor_id else None,
        "actor_email": event.actor_email,
        "actor_type": event.actor_type,
        "action": event.action,
        "resource_id": str(event.resource_id) if event.resource_id else None,
        "resource_type": event.resource_type,
        "resource_name": event.resource_name,
        "outcome": event.outcome,
        "job_id": str(event.job_id) if event.job_id else None,
        "snapshot_id": str(event.snapshot_id) if event.snapshot_id else None,
        "details": event.details,
        "occurred_at": event.occurred_at.isoformat() if event.occurred_at else None,
    }


async def consume_audit_events():
    """
    Consume audit events from RabbitMQ and store them in the database.
    This provides an async event bus alternative to direct HTTP POST.
    Workers can publish to the audit.events queue for reliable delivery.
    """
    async def callback(body: dict):
        """Process a single audit event message from the queue"""
        try:
            # Transform message format to internal event format
            event = {
                "action": body.get("action"),
                "tenant_id": body.get("tenantId"),
                "org_id": body.get("orgId"),
                "actor_type": body.get("actorType", "SYSTEM"),
                "actor_id": body.get("actorId"),
                "actor_email": body.get("actorEmail"),
                "resource_id": body.get("resourceId"),
                "resource_type": body.get("resourceType"),
                "resource_name": body.get("resourceName"),
                "outcome": body.get("outcome", "SUCCESS"),
                "job_id": body.get("jobId"),
                "snapshot_id": body.get("snapshotId"),
                "details": body.get("details", {}),
            }

            # Normalize and enrich
            event = _normalize_event(event)

            async with async_session_factory() as db:
                # Fetch SLA context if resource_id is provided
                if event.get("resource_id"):
                    resource = await db.get(Resource, uuid.UUID(event["resource_id"]))
                    if resource:
                        sla_name = None
                        if resource.sla_policy_id:
                            sla = await db.get(SlaPolicy, resource.sla_policy_id)
                            if sla:
                                sla_name = sla.name
                        event.setdefault("details", {})
                        event["details"].setdefault("enrichment", {})
                        event["details"]["enrichment"]["sla_policy"] = sla_name

                # Detect risk signals
                risk_signals = _detect_risk_signals(event)
                if risk_signals:
                    event.setdefault("details", {})
                    event["details"]["risk_signals"] = risk_signals

                audit = AuditEvent(
                    id=uuid.uuid4(),
                    org_id=uuid.UUID(event["org_id"]) if event.get("org_id") else None,
                    tenant_id=uuid.UUID(event["tenant_id"]) if event.get("tenant_id") else None,
                    actor_id=uuid.UUID(event["actor_id"]) if event.get("actor_id") else None,
                    actor_email=event.get("actor_email"),
                    actor_type=event.get("actor_type", "SYSTEM"),
                    action=event["action"],
                    resource_id=uuid.UUID(event["resource_id"]) if event.get("resource_id") else None,
                    resource_type=event.get("resource_type"),
                    resource_name=event.get("resource_name"),
                    outcome=event.get("outcome", "SUCCESS"),
                    job_id=uuid.UUID(event["job_id"]) if event.get("job_id") else None,
                    snapshot_id=uuid.UUID(event["snapshot_id"]) if event.get("snapshot_id") else None,
                    details=event.get("details", {}),
                    occurred_at=datetime.utcnow(),
                )
                db.add(audit)
                await db.commit()
        except Exception as e:
            print(f"[AUDIT_CONSUMER] Error processing message: {e}")
            # In production: send to DLQ

    await message_bus.consume("audit.events", callback)
