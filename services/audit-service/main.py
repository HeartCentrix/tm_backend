"""
Audit Log Service - Tracks all backup/restore/admin actions
Port: 8012

Responsibilities:
- Record all backup events (started, completed, failed, preemptive)
- Record restore, export, SLA changes, resource actions
- Support Microsoft Graph audit log ingestion
- Paginated listing with multi-filter search
- CSV export for compliance
"""
import csv
import io
import uuid
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, desc, and_
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import async_session_factory
from shared.models import AuditEvent, Resource, Tenant, Organization
from shared.config import settings
from shared.graph_client import GraphClient
from shared.multi_app_manager import multi_app_manager

app = FastAPI(title="Audit Log Service", version="1.0.0")

# Action codes
ACTIONS = {
    "BACKUP_TRIGGERED": "Manual or scheduled backup triggered",
    "BACKUP_STARTED": "Backup job started executing",
    "BACKUP_COMPLETED": "Backup completed successfully",
    "BACKUP_FAILED": "Backup failed permanently",
    "BACKUP_PREEMPTIVE": "Preemptive backup triggered (AI detection)",
    "RESTORE_TRIGGERED": "Restore job triggered",
    "RESTORE_COMPLETED": "Restore completed",
    "RESTORE_FAILED": "Restore failed",
    "EXPORT_TRIGGERED": "Export job triggered",
    "EXPORT_COMPLETED": "Export completed",
    "EXPORT_DOWNLOADED": "Export file downloaded",
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


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "audit-log"}


@app.post("/api/v1/audit/log")
async def create_audit_event(event: dict):
    """
    Internal endpoint for services/workers to log events.
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
        return {"id": str(audit.id), "action": audit.action, "occurred_at": audit.occurred_at.isoformat()}


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
