"""/api/v1/exports/chat/* endpoints — trigger, estimate, SSE, cancel, delete.

Router is included from main.py. All paths route to the chat-export-worker
via `q.export.chat.thread`; progress streams come back through Redis Pub/Sub.

Auth model (matches shared.security.get_current_user_from_token): user is a
dict with keys {id, email, roles, org_id, tenant_ids}. We treat the FIRST
tenant_id in the list as the actor's active tenant (matches how existing
services interpret JWT scope). `roles` carries UserRole enum values as
strings — "TENANT_ADMIN" / "SUPER_ADMIN" / "ORG_ADMIN" elevates for admin-
only routes.

Status values:
- New jobs start as PENDING (distinct from worker-owned QUEUED so the
  idempotency check can see both states).
- cancel() sets CANCELLING — worker flips to CANCELLED on its next poll.
- force_delete() sets CANCELLED + result.force_deleted=true so cleanup
  cron reclaims the blob.
"""
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from sqlalchemy import func, select, update
from sqlalchemy.orm import aliased

from shared.config import settings
from shared.database import AsyncSession, get_db
from shared.message_bus import message_bus
from shared.models import (
    Job,
    JobStatus,
    JobType,
    Resource,
    Snapshot,
    SnapshotItem,
    Tenant,
)
from shared.security import get_current_user_from_token

router = APIRouter(prefix="/api/v1/exports/chat", tags=["Chat Export"])

Q = "q.export.chat.thread"
CHAT_MSG_TYPES = ("TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY")
KIND = "chat_export_thread"
ADMIN_ROLES = {"SUPER_ADMIN", "ORG_ADMIN", "TENANT_ADMIN", "tenant_admin"}


class ChatExportRequest(BaseModel):
    resourceId: str
    snapshotIds: list[str]
    threadPath: str | None = None
    itemIds: list[str] = Field(default_factory=list)
    exportFormat: str
    includeAttachments: bool = True
    force: bool = False


def _user_tenant_id(user: dict) -> uuid.UUID | None:
    """First tenant id from the JWT scope, as UUID, or None."""
    tids = user.get("tenant_ids") or []
    if not tids:
        return None
    try:
        return uuid.UUID(tids[0]) if isinstance(tids[0], str) else tids[0]
    except (ValueError, TypeError):
        return None


def _is_admin(user: dict) -> bool:
    return any(r in ADMIN_ROLES for r in (user.get("roles") or []))


async def _ensure_ownership(sess: AsyncSession, user: dict, resource_id: str) -> Resource:
    try:
        rid = uuid.UUID(resource_id)
    except (ValueError, TypeError):
        raise HTTPException(400, detail={"error": "INVALID_RESOURCE_ID"})
    res = (await sess.execute(select(Resource).where(Resource.id == rid))).scalar_one_or_none()
    if not res:
        raise HTTPException(404, detail={"error": "RESOURCE_NOT_FOUND"})
    user_tenant = _user_tenant_id(user)
    if user_tenant is None or res.tenant_id != user_tenant:
        raise HTTPException(403, detail={"error": "AUTH_DENIED"})
    return res


async def _resolve_counts(
    sess: AsyncSession,
    *,
    resource_id: str,
    snapshot_ids: list[str],
    thread_path: str | None,
    item_ids: list[str],
):
    if thread_path and item_ids:
        raise HTTPException(400, detail={"error": "INVALID_SELECTION"})
    if not thread_path and not item_ids:
        raise HTTPException(400, detail={"error": "SCOPE_EMPTY"})

    resource_uuid = uuid.UUID(resource_id)
    snapshot_uuids = [uuid.UUID(s) for s in snapshot_ids] if snapshot_ids else []
    item_uuids = [uuid.UUID(i) for i in item_ids] if item_ids else []

    Sn = aliased(Snapshot)
    base = (
        select(func.count(), func.coalesce(func.sum(SnapshotItem.content_size), 0))
        .select_from(SnapshotItem)
        .join(Sn, Sn.id == SnapshotItem.snapshot_id)
        .where(Sn.resource_id == resource_uuid)
    )
    if thread_path:
        if not snapshot_uuids:
            raise HTTPException(400, detail={"error": "SCOPE_EMPTY"})
        q = (
            base.where(SnapshotItem.snapshot_id.in_(snapshot_uuids))
            .where(SnapshotItem.item_type.in_(CHAT_MSG_TYPES))
            .where(SnapshotItem.folder_path == thread_path)
        )
        layout = "single_thread"
        effective = thread_path
    else:
        # Multi-item case. Reject when items straddle multiple thread folders —
        # cross-thread merge isn't implemented yet (single-thread for v1).
        paths_q = (
            select(SnapshotItem.folder_path)
            .join(Sn, Sn.id == SnapshotItem.snapshot_id)
            .where(Sn.resource_id == resource_uuid)
            .where(SnapshotItem.id.in_(item_uuids))
            .distinct()
        )
        paths = [r[0] for r in (await sess.execute(paths_q))]
        if len(paths) > 1:
            raise HTTPException(400, detail={"error": "MULTI_THREAD_NOT_SUPPORTED_YET"})
        if not paths:
            raise HTTPException(400, detail={"error": "SCOPE_EMPTY"})
        q = base.where(SnapshotItem.id.in_(item_uuids))
        effective = paths[0]
        layout = "per_message"

    count, bytes_ = (await sess.execute(q)).one()

    # Attachment bytes — linked by parent_external_id to the matched messages.
    ext_ids_q = (
        select(SnapshotItem.external_id)
        .join(Sn, Sn.id == SnapshotItem.snapshot_id)
        .where(Sn.resource_id == resource_uuid)
    )
    if thread_path:
        ext_ids_q = ext_ids_q.where(SnapshotItem.folder_path == thread_path).where(
            SnapshotItem.item_type.in_(CHAT_MSG_TYPES)
        )
    else:
        ext_ids_q = ext_ids_q.where(SnapshotItem.id.in_(item_uuids))
    ext_ids = [r[0] for r in (await sess.execute(ext_ids_q))]

    att_size = 0
    if ext_ids:
        att_q = (
            select(func.coalesce(func.sum(SnapshotItem.content_size), 0))
            .where(SnapshotItem.item_type.in_(["CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"]))
            .where(SnapshotItem.parent_external_id.in_(ext_ids))
        )
        att_size = (await sess.execute(att_q)).scalar() or 0

    return int(count or 0), int(bytes_ or 0) + int(att_size or 0), layout, effective


@router.post("/estimate")
async def estimate(
    req: ChatExportRequest,
    user: dict = Depends(get_current_user_from_token),
    sess: AsyncSession = Depends(get_db),
):
    await _ensure_ownership(sess, user, req.resourceId)
    count, est_bytes, layout, _ = await _resolve_counts(
        sess,
        resource_id=req.resourceId,
        snapshot_ids=req.snapshotIds,
        thread_path=req.threadPath,
        item_ids=req.itemIds,
    )
    return {
        "messages": count,
        "attachmentBytes": int(est_bytes),
        "estimatedZipBytes": int(est_bytes * 1.03),
        "layoutMode": layout,
        "softCapExceeded": est_bytes > settings.chat_export_size_soft_cap_bytes,
        "hardCapExceeded": est_bytes > settings.chat_export_size_hard_cap_bytes,
    }


@router.post("", status_code=202)
async def trigger(
    req: ChatExportRequest,
    request: Request,
    user: dict = Depends(get_current_user_from_token),
    sess: AsyncSession = Depends(get_db),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    await _ensure_ownership(sess, user, req.resourceId)
    user_tenant = _user_tenant_id(user)

    # Idempotency: if the same tenant sends the same Idempotency-Key, return
    # the prior job rather than queuing a duplicate export.
    if idempotency_key:
        dup_q = (
            select(Job)
            .where(Job.tenant_id == user_tenant)
            .where(Job.spec["idempotency_key"].astext == idempotency_key)
            .order_by(Job.created_at.desc())
            .limit(1)
        )
        dup = (await sess.execute(dup_q)).scalar_one_or_none()
        if dup:
            status_val = dup.status.value if hasattr(dup.status, "value") else str(dup.status)
            return JSONResponse(
                {
                    "jobId": str(dup.id),
                    "status": status_val,
                    "estimatedMessages": (dup.spec or {}).get("estimatedMessages", 0),
                    "estimatedBytes": (dup.spec or {}).get("estimatedBytes", 0),
                    "softCapWarning": False,
                    "hardCapExceeded": False,
                },
                status_code=202,
            )

    count, est_bytes, layout, _ = await _resolve_counts(
        sess,
        resource_id=req.resourceId,
        snapshot_ids=req.snapshotIds,
        thread_path=req.threadPath,
        item_ids=req.itemIds,
    )

    # Size caps — hard is hard 409; soft returns 409 unless force=true.
    hard_cap = settings.chat_export_size_hard_cap_bytes
    soft_cap = settings.chat_export_size_soft_cap_bytes
    if est_bytes > hard_cap:
        raise HTTPException(
            409,
            detail={"error": "SIZE_HARD_CAP_EXCEEDED", "bytes": int(est_bytes)},
        )
    if est_bytes > soft_cap and not req.force:
        raise HTTPException(
            409,
            detail={
                "error": "SIZE_SOFT_CAP_EXCEEDED",
                "bytes": int(est_bytes),
                "hint": "retry with force=true to override",
            },
        )

    # Per-tenant concurrency cap.
    running_q = (
        select(func.count())
        .select_from(Job)
        .where(Job.tenant_id == user_tenant)
        .where(Job.type == JobType.EXPORT)
        .where(Job.spec["kind"].astext == KIND)
        .where(Job.status.in_([JobStatus.QUEUED, JobStatus.PENDING, JobStatus.RUNNING]))
    )
    running = (await sess.execute(running_q)).scalar_one() or 0
    tenant = (await sess.execute(select(Tenant).where(Tenant.id == user_tenant))).scalar_one_or_none()
    limits = ((tenant.extra_data if tenant else None) or {}).get("limits", {})
    cap = limits.get("chat_export_concurrent") or settings.chat_export_tenant_concurrent_min
    if running >= cap:
        return JSONResponse(
            {"error": "RATE_LIMITED", "retry_after_seconds": 60},
            status_code=429,
            headers={"Retry-After": "60"},
        )

    spec = {
        **req.model_dump(),
        "kind": KIND,
        "idempotency_key": idempotency_key,
        "userEmail": user.get("email", ""),
        "userId": user.get("id", ""),
        "estimatedMessages": count,
        "estimatedBytes": int(est_bytes),
        "layoutMode": layout,
        "triggered_by": "MANUAL",
    }
    job = Job(
        id=uuid.uuid4(),
        tenant_id=user_tenant,
        type=JobType.EXPORT,
        resource_id=uuid.UUID(req.resourceId),
        status=JobStatus.PENDING,
        spec=spec,
    )
    sess.add(job)
    await sess.commit()

    # Fire-and-forget publish. If RabbitMQ is disabled or down, the job stays
    # PENDING; admin/cron can retrigger. Don't 500 the user for a broker blip.
    try:
        await message_bus.publish(Q, {"jobId": str(job.id), "tenantId": str(user_tenant)})
    except Exception as exc:
        print(f"[chat_export] publish failed for job {job.id}: {exc}")

    return {
        "jobId": str(job.id),
        "status": "PENDING",
        "estimatedMessages": count,
        "estimatedBytes": int(est_bytes),
        "softCapWarning": est_bytes > soft_cap,
        "hardCapExceeded": False,
    }


def _job_snapshot(job: Job) -> dict:
    status_val = job.status.value if hasattr(job.status, "value") else str(job.status)
    out = {
        "jobId": str(job.id),
        "status": status_val,
        "createdAt": job.created_at.isoformat() if job.created_at else None,
        "updatedAt": job.updated_at.isoformat() if job.updated_at else None,
    }
    if job.status == JobStatus.COMPLETED and job.result:
        op = job.result or {}
        out["download"] = {
            "url": op.get("signed_url"),
            "sizeBytes": op.get("total_bytes"),
            "sha256": op.get("sha256"),
            "filename": f"teams-chat-{str(job.id)[:8]}.zip",
            "supportsRange": True,
        }
        out["summary"] = {
            "messages": op.get("total_msgs"),
            "format": (job.spec or {}).get("exportFormat"),
            "layoutMode": (job.spec or {}).get("layoutMode"),
        }
    if job.status == JobStatus.FAILED and job.result:
        out["error"] = (job.result or {}).get("error", {"code": "UNKNOWN"})
    return out


@router.get("/{job_id}")
async def status_or_sse(
    job_id: str,
    request: Request,
    user: dict = Depends(get_current_user_from_token),
    sess: AsyncSession = Depends(get_db),
):
    try:
        jid = uuid.UUID(job_id)
    except (ValueError, TypeError):
        raise HTTPException(400, detail={"error": "INVALID_JOB_ID"})
    job = (await sess.execute(select(Job).where(Job.id == jid))).scalar_one_or_none()
    if not job:
        raise HTTPException(404, detail={"error": "JOB_NOT_FOUND"})
    if job.tenant_id != _user_tenant_id(user):
        raise HTTPException(403, detail={"error": "AUTH_DENIED"})

    # Accept header switches JSON snapshot vs SSE stream. Frontend polling
    # uses JSON; the live progress bar subscribes over SSE.
    if "text/event-stream" not in (request.headers.get("Accept") or ""):
        return _job_snapshot(job)

    async def event_stream():
        redis_url = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
        r = Redis.from_url(redis_url)
        ps = r.pubsub()
        channel = f"chat_export:progress:{job_id}"
        await ps.subscribe(channel)
        # Replay the most-recent progress snapshot so clients reconnecting
        # mid-export see current state instead of silence until the next tick.
        last = await r.get(f"chat_export:last:{job_id}")
        if last:
            last_str = last.decode() if isinstance(last, bytes) else last
            yield f"event: progress\ndata: {last_str}\n\n"
        try:
            async for msg in ps.listen():
                if msg["type"] != "message":
                    continue
                data = msg["data"].decode() if isinstance(msg["data"], bytes) else msg["data"]
                try:
                    evt = json.loads(data).get("event", "progress")
                except (ValueError, TypeError):
                    evt = "progress"
                yield f"event: {evt}\ndata: {data}\n\n"
                if evt in ("complete", "error", "cancelled"):
                    break
        finally:
            try:
                await ps.unsubscribe(channel)
            except Exception:
                pass
            try:
                await ps.close()
            except Exception:
                pass
            try:
                await r.close()
            except Exception:
                pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/{job_id}/cancel", status_code=202)
async def cancel(
    job_id: str,
    user: dict = Depends(get_current_user_from_token),
    sess: AsyncSession = Depends(get_db),
):
    try:
        jid = uuid.UUID(job_id)
    except (ValueError, TypeError):
        raise HTTPException(400, detail={"error": "INVALID_JOB_ID"})
    job = (await sess.execute(select(Job).where(Job.id == jid))).scalar_one_or_none()
    if not job:
        raise HTTPException(404, detail={"error": "JOB_NOT_FOUND"})
    if job.tenant_id != _user_tenant_id(user):
        raise HTTPException(403, detail={"error": "AUTH_DENIED"})
    # Only flip transient states. Terminal jobs (COMPLETED/FAILED/CANCELLED)
    # are left alone so we don't overwrite a legit final state.
    if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        return {"status": job.status.value if hasattr(job.status, "value") else str(job.status)}
    await sess.execute(
        update(Job)
        .where(Job.id == jid)
        .values(status=JobStatus.CANCELLING, updated_at=datetime.now(timezone.utc).replace(tzinfo=None))
    )
    await sess.commit()
    return {"status": "CANCELLING"}


@router.delete("/{job_id}", status_code=204)
async def force_delete(
    job_id: str,
    user: dict = Depends(get_current_user_from_token),
    sess: AsyncSession = Depends(get_db),
):
    # Admin-only. JobStatus.DELETED doesn't exist — we mark CANCELLED with
    # result.force_deleted=true so the exports_cleanup cron drops the blob
    # on its next sweep.
    if not _is_admin(user):
        raise HTTPException(403, detail={"error": "AUTH_DENIED"})
    try:
        jid = uuid.UUID(job_id)
    except (ValueError, TypeError):
        raise HTTPException(400, detail={"error": "INVALID_JOB_ID"})
    job = (await sess.execute(select(Job).where(Job.id == jid))).scalar_one_or_none()
    if not job:
        raise HTTPException(404, detail={"error": "JOB_NOT_FOUND"})
    new_result = {
        **(job.result or {}),
        "force_deleted": True,
        "admin_user_id": str(user.get("id", "")),
        "force_deleted_at": datetime.now(timezone.utc).isoformat(),
    }
    await sess.execute(
        update(Job).where(Job.id == jid).values(status=JobStatus.CANCELLED, result=new_result)
    )
    await sess.commit()
    return None
