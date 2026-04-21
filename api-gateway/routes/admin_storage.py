"""Admin storage API: status, toggle, events, SSE stream, abort.

Mounted into api-gateway/main.py via include_router. All endpoints require
org_admin role (checked by _require_org_admin dependency).
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import aio_pika
import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from shared.schemas import (
    StorageBackendOut,
    ToggleRequest,
    ToggleStatusOut,
)
from shared.security import get_current_user_from_token

router = APIRouter(prefix="/api/admin/storage", tags=["admin-storage"])


def _require_org_admin(user: dict = Depends(get_current_user_from_token)) -> dict:
    roles = {str(r).upper() for r in user.get("roles", [])}
    if "ORG_ADMIN" not in roles and "SUPER_ADMIN" not in roles:
        raise HTTPException(status_code=403, detail="org_admin role required")
    return user


def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    u = os.getenv("DB_USERNAME", "postgres")
    p = os.getenv("DB_PASSWORD", "")
    db = os.getenv("DB_NAME", "postgres")
    return f"postgresql://{u}:{p}@{host}:{port}/{db}"


def _schema() -> str:
    return os.getenv("DB_SCHEMA", "tm")


def _rmq_url() -> str:
    url = os.getenv("RABBITMQ_URL")
    if url:
        return url
    u = os.getenv("RABBITMQ_USERNAME", os.getenv("RABBITMQ_USER", "guest"))
    p = os.getenv("RABBITMQ_PASSWORD", "guest")
    h = os.getenv("RABBITMQ_HOST", "localhost")
    port = os.getenv("RABBITMQ_PORT", "5672")
    return f"amqp://{u}:{p}@{h}:{port}/"


async def _connect():
    conn = await asyncpg.connect(_dsn())
    await conn.execute(f"SET search_path TO {_schema()}, public")
    return conn


def _row_to_dict(row: Any) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v)
            for k, v in dict(row).items()}


@router.get("/status")
async def status(user: dict = Depends(_require_org_admin)):
    db = await _connect()
    try:
        sc = await db.fetchrow(
            "SELECT sc.active_backend_id, sc.transition_state, sc.last_toggle_at, "
            "  sc.cooldown_until, sb.id AS b_id, sb.kind, sb.name, sb.endpoint, "
            "  sb.is_enabled "
            "FROM system_config sc "
            "JOIN storage_backends sb ON sb.id = sc.active_backend_id "
            "WHERE sc.id=1"
        )
        inflight = await db.fetchval(
            "SELECT count(*)::int FROM jobs "
            "WHERE status IN ('RUNNING','QUEUED','PENDING')"
        )
    finally:
        await db.close()
    if not sc:
        raise HTTPException(500, detail="system_config singleton missing")
    return {
        "active_backend": {
            "id": str(sc["b_id"]), "kind": sc["kind"], "name": sc["name"],
            "endpoint": sc["endpoint"], "is_enabled": sc["is_enabled"],
        },
        "transition_state": sc["transition_state"],
        "last_toggle_at": sc["last_toggle_at"].isoformat() if sc["last_toggle_at"] else None,
        "cooldown_until": sc["cooldown_until"].isoformat() if sc["cooldown_until"] else None,
        "inflight_jobs_count": inflight,
    }


@router.get("/backends")
async def list_backends(user: dict = Depends(_require_org_admin)):
    db = await _connect()
    try:
        rows = await db.fetch(
            "SELECT id::text, kind, name, endpoint, is_enabled "
            "FROM storage_backends ORDER BY name"
        )
    finally:
        await db.close()
    return [dict(r) for r in rows]


@router.post("/toggle")
async def submit_toggle(
    req: ToggleRequest, request: Request,
    user: dict = Depends(_require_org_admin),
):
    org_id = user.get("org_id")
    # Org name fetched via a separate query — in production, resolve from
    # users.org_id -> organizations.name. For pilot we accept the raw org_id
    # string or "TMVAULT-ORG" (documented fallback for single-org installs).
    expected = _EXPECTED_ORG_CONFIRM or (org_id and str(org_id)) or "TMVAULT-ORG"
    if req.confirmation_text not in (expected, "TMVAULT-ORG"):
        raise HTTPException(400, detail="confirmation_text does not match org name")

    db = await _connect()
    try:
        sc = await db.fetchrow(
            "SELECT active_backend_id, transition_state, cooldown_until "
            "FROM system_config WHERE id=1"
        )
        if sc["transition_state"] != "stable":
            raise HTTPException(
                409, detail=f"transition already in progress: {sc['transition_state']}",
            )
        if sc["cooldown_until"] and sc["cooldown_until"] > datetime.now(timezone.utc):
            raise HTTPException(
                429, detail=f"cooldown active until {sc['cooldown_until'].isoformat()}",
            )

        from_id = sc["active_backend_id"]
        if str(from_id) == str(req.target_backend_id):
            raise HTTPException(400, detail="target backend is already active")

        event_id = uuid.uuid4()
        actor_id = user.get("id")
        await db.execute(
            "INSERT INTO storage_toggle_events (id, actor_id, actor_ip, "
            "  from_backend_id, to_backend_id, reason, status) "
            "VALUES ($1,$2,$3,$4,$5,$6,'started')",
            event_id,
            uuid.UUID(str(actor_id)) if actor_id else uuid.UUID(int=0),
            request.client.host if request.client else None,
            from_id, req.target_backend_id, req.reason,
        )
    finally:
        await db.close()

    # Enqueue toggle message
    conn = await aio_pika.connect_robust(_rmq_url())
    try:
        ch = await conn.channel()
        await ch.declare_queue("storage.toggle", durable=True)
        await ch.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps({
                    "event_id": str(event_id),
                    "from_id": str(from_id),
                    "to_id": str(req.target_backend_id),
                    "actor_id": str(actor_id),
                    "reason": req.reason,
                }).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="storage.toggle",
        )
    finally:
        await conn.close()

    return {"event_id": str(event_id), "status": "queued"}


@router.get("/events")
async def list_events(
    limit: int = 20,
    user: dict = Depends(_require_org_admin),
):
    db = await _connect()
    try:
        rows = await db.fetch(
            "SELECT * FROM storage_toggle_events ORDER BY started_at DESC LIMIT $1",
            limit,
        )
    finally:
        await db.close()
    return [_row_to_dict(r) for r in rows]


@router.get("/events/{event_id}")
async def get_event(
    event_id: uuid.UUID,
    user: dict = Depends(_require_org_admin),
):
    db = await _connect()
    try:
        row = await db.fetchrow(
            "SELECT * FROM storage_toggle_events WHERE id=$1", event_id,
        )
    finally:
        await db.close()
    if not row:
        raise HTTPException(404)
    return _row_to_dict(row)


@router.get("/events/{event_id}/stream")
async def stream_event(
    event_id: uuid.UUID, request: Request,
    user: dict = Depends(_require_org_admin),
):
    async def generator():
        conn = await _connect()
        try:
            last_status = None
            while True:
                if await request.is_disconnected():
                    break
                row = await conn.fetchrow(
                    "SELECT status, drained_job_count, retried_job_count, error_message "
                    "FROM storage_toggle_events WHERE id=$1", event_id,
                )
                if row and row["status"] != last_status:
                    last_status = row["status"]
                    yield {"event": "status", "data": json.dumps(dict(row))}
                if row and row["status"] in ("completed", "aborted", "failed"):
                    break
                await asyncio.sleep(1)
        finally:
            await conn.close()

    return EventSourceResponse(generator())


@router.post("/toggle/{event_id}/abort")
async def abort_toggle(
    event_id: uuid.UUID,
    user: dict = Depends(_require_org_admin),
):
    db = await _connect()
    try:
        row = await db.fetchrow(
            "SELECT status FROM storage_toggle_events WHERE id=$1", event_id,
        )
        if not row:
            raise HTTPException(404)
        if row["status"] not in ("started", "drain_started"):
            raise HTTPException(
                409, detail="cannot abort after drain has completed",
            )
        await db.execute(
            "UPDATE storage_toggle_events SET status='aborted', completed_at=NOW() "
            "WHERE id=$1",
            event_id,
        )
        await db.execute(
            "UPDATE system_config SET transition_state='stable', updated_at=NOW() "
            "WHERE id=1",
        )
    finally:
        await db.close()
    return {"status": "aborted"}


# Expected org-confirm text — if unset, falls back to the user's org_id or
# the well-known "TMVAULT-ORG" (accepted in both cases for single-org
# internal deploys).
_EXPECTED_ORG_CONFIRM = os.getenv("TOGGLE_CONFIRM_TEXT") or None
