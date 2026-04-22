"""8-phase toggle state machine."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import asyncpg

from services.storage_toggle_worker.drain import drain_inflight
from services.storage_toggle_worker.dns_flip import flip_dns_to
from services.storage_toggle_worker.pg_promote import promote_target
from services.storage_toggle_worker.preflight import run_preflight
from services.storage_toggle_worker.rollback import rollback
from services.storage_toggle_worker.smoke import run_smoke
from services.storage_toggle_worker.worker_restart import restart_workers
from shared.storage.router import router

log = logging.getLogger("toggle.orchestrator")


async def _open_db():
    dsn = os.getenv("DATABASE_URL") or _build_dsn()
    conn = await asyncpg.connect(dsn)
    schema = os.getenv("DB_SCHEMA", "tm")
    await conn.execute(f"SET search_path TO {schema}, public")
    return conn


def _build_dsn() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USERNAME", "postgres")
    pw = os.getenv("DB_PASSWORD", "")
    db = os.getenv("DB_NAME", "postgres")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


async def _update_event(db, event_id: str, **cols) -> None:
    if not cols:
        return
    keys = list(cols.keys())
    values = list(cols.values())
    set_clause = ", ".join(f"{k}=${i + 2}" for i, k in enumerate(keys))
    await db.execute(
        f"UPDATE storage_toggle_events SET {set_clause} WHERE id=$1",
        event_id, *values,
    )


async def _set_transition(db, state: str) -> None:
    await db.execute(
        "UPDATE system_config SET transition_state=$1, updated_at=NOW() WHERE id=1",
        state,
    )


async def _set_active(db, backend_id: str) -> None:
    await db.execute(
        "UPDATE system_config "
        "SET active_backend_id=$1, last_toggle_at=NOW(), "
        "    cooldown_until=NOW() + INTERVAL '30 minutes', updated_at=NOW() "
        "WHERE id=1",
        backend_id,
    )


async def run_toggle(payload: dict) -> None:
    event_id = payload["event_id"]
    from_id = payload["from_id"]
    to_id = payload["to_id"]

    db = await _open_db()
    try:
        # Phase 1 — preflight
        log.info("phase 1: preflight for event %s", event_id)
        sc = await db.fetchrow(
            "SELECT transition_state, cooldown_until FROM system_config WHERE id=1"
        )
        cooldown_elapsed = (
            sc["cooldown_until"] is None
            or sc["cooldown_until"] < datetime.now(timezone.utc)
        )
        preflight = await run_preflight(
            router=router, db=db, target_backend_id=to_id,
            cooldown_elapsed=cooldown_elapsed,
            transition_state=sc["transition_state"],
        )
        import json as _json
        await _update_event(
            db, event_id,
            pre_flight_checks=_json.dumps({
                c.name: {"ok": c.ok, "detail": c.detail}
                for c in preflight.checks
            }),
        )
        if not preflight.ok:
            await _update_event(
                db, event_id, status="aborted",
                error_message="preflight failed",
                completed_at=datetime.now(timezone.utc),
            )
            return

        # Phase 2 — drain
        log.info("phase 2: drain")
        await _set_transition(db, "draining")
        drain_result = await drain_inflight(db, event_id=event_id)
        await _update_event(
            db, event_id, status="drain_completed",
            drain_completed_at=datetime.now(timezone.utc),
            drained_job_count=drain_result.drained_count,
            retried_job_count=drain_result.retried_count,
        )

        # Phase 3 — DB promote
        log.info("phase 3: promote db")
        try:
            await promote_target(payload.get("target_kind", "onprem"))
            await _update_event(db, event_id, status="db_promoted")
        except Exception as e:
            log.error("promote failed: %s", e)
            await rollback(db, event_id=event_id, phase="db_promote", error=str(e))
            return

        # Phase 4 — DNS flip
        log.info("phase 4: dns flip")
        try:
            await flip_dns_to(payload.get("target_kind", "onprem"))
            await _update_event(db, event_id, status="dns_flipped")
        except Exception as e:
            log.error("dns flip failed: %s", e)
            await rollback(db, event_id=event_id, phase="dns", error=str(e))
            return

        # Phase 5 — workers
        log.info("phase 5: restart workers")
        try:
            await restart_workers(
                payload.get("from_kind", "azure"),
                payload.get("target_kind", "onprem"),
            )
            await _update_event(db, event_id, status="workers_restarted")
        except Exception as e:
            log.error("worker restart failed: %s", e)
            await rollback(db, event_id=event_id, phase="workers", error=str(e))
            return

        # Phase 6 — cutover
        log.info("phase 6: cutover")
        await _set_transition(db, "flipping")
        await _set_active(db, to_id)

        # Phase 7 — smoke
        log.info("phase 7: smoke")
        try:
            # Reload router so get_store_by_id picks up any config mutations.
            await router._reload_from_db()
            await run_smoke(db, new_backend_id=to_id, old_backend_id=from_id)
            await _update_event(db, event_id, status="smoke_passed")
        except AssertionError as e:
            log.error("smoke failed: %s", e)
            await _update_event(
                db, event_id, status="failed",
                error_message=f"smoke: {e}",
                completed_at=datetime.now(timezone.utc),
            )
            return

        # Phase 8 — open
        log.info("phase 8: open")
        await _set_transition(db, "stable")
        await _update_event(
            db, event_id, status="completed",
            completed_at=datetime.now(timezone.utc),
            flip_completed_at=datetime.now(timezone.utc),
        )
        log.info("toggle event %s complete", event_id)
    finally:
        await db.close()
