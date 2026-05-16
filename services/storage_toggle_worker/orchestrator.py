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
    """Cutover: point system_config at ``backend_id``.

    Does NOT set the cooldown — that's the success-path's job (see
    Phase 8 below). Previously this function stamped cooldown_until
    inside the same UPDATE, which meant a smoke-failure rollback that
    calls ``_set_active(db, from_id)`` to revert the cutover would
    re-stamp the cooldown — locking the operator out for 30 minutes
    even though the flip had failed (2026-05-16 incident).
    """
    await db.execute(
        "UPDATE system_config "
        "SET active_backend_id=$1, last_toggle_at=NOW(), updated_at=NOW() "
        "WHERE id=1",
        backend_id,
    )


async def _set_cooldown(db, *, minutes: int = 30) -> None:
    """Stamp the post-flip cooldown. Called only from the success path
    (Phase 8) so a failed flip leaves the operator free to retry
    immediately."""
    await db.execute(
        "UPDATE system_config "
        "SET cooldown_until=NOW() + ($1 * INTERVAL '1 minute'), updated_at=NOW() "
        "WHERE id=1",
        minutes,
    )


async def _clear_cooldown(db) -> None:
    """Wipe the cooldown — called from the smoke-failure rollback so a
    failed flip doesn't strand the operator behind a 30-min wall."""
    await db.execute(
        "UPDATE system_config SET cooldown_until=NULL, updated_at=NOW() WHERE id=1"
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
        # Catch every exception, not just AssertionError. The data-plane
        # store classes (azure/seaweedfs/s3) raise ``BackendUnreachable
        # Error``, ``ClientError``, network timeouts, etc. on failed
        # PutObject — none of which subclass AssertionError. Before this
        # widened catch, a non-AssertionError exception in run_smoke
        # propagated up, the orchestrator crashed, and the event row
        # was left wedged in status='workers_restarted' with no rollback
        # ever firing — exactly the 2026-05-16 "flipping forever"
        # incident triggered by SeaweedFS running out of volume capacity.
        #
        # In addition to marking the event ``failed``, we now revert
        # ``active_backend_id`` to ``from_id`` so the data plane keeps
        # serving from the previously-known-good backend instead of
        # pointing at the broken target. Phase 6 cutover commits
        # active_backend_id BEFORE smoke runs, so without this revert
        # a smoke failure would silently leave the system pointed at
        # the failed backend.
        log.info("phase 7: smoke")
        try:
            # Reload router so get_store_by_id picks up any config mutations.
            await router._reload_from_db()
            await run_smoke(db, new_backend_id=to_id, old_backend_id=from_id)
            await _update_event(db, event_id, status="smoke_passed")
        except Exception as e:
            log.error("smoke failed (%s): %s", type(e).__name__, e)
            # Revert the cutover from Phase 6 — active_backend_id was
            # already flipped to ``to_id`` and that's now pointing at a
            # broken backend. Restore from_id so writes keep working.
            try:
                await _set_active(db, from_id)
            except Exception as set_err:
                log.error("revert active_backend_id failed: %s", set_err)
            # Wipe the cooldown. A failed flip must not strand the
            # operator behind the 30-min cooldown wall — otherwise
            # they'll see "cooldown active until X" while staring at a
            # failed event and have no way to retry.
            try:
                await _clear_cooldown(db)
            except Exception as cc_err:
                log.error("clear_cooldown failed: %s", cc_err)
            # Standard rollback path: clear transition_state and mark
            # the event row failed. ``rollback`` is idempotent w.r.t.
            # the _update_event we'd otherwise call directly.
            try:
                await rollback(
                    db, event_id=event_id, phase="smoke",
                    error=f"{type(e).__name__}: {e}",
                )
            except Exception as rb_err:
                # Best-effort: even if rollback() itself fails, force a
                # terminal event status so the UI cooldown banner
                # unblocks. Without this, an outage in the rollback path
                # could re-wedge the row.
                log.error("rollback() failed during smoke handler: %s", rb_err)
                await _update_event(
                    db, event_id, status="failed",
                    error_message=f"smoke: {type(e).__name__}: {e} "
                                  f"(rollback also failed: {rb_err})",
                    completed_at=datetime.now(timezone.utc),
                )
            return

        # Phase 8 — open
        # Successful flip: stamp the post-flip cooldown so back-to-back
        # toggles can't thrash the cluster. Cooldown is success-only —
        # the smoke-failure branch above wipes cooldown_until so a
        # failed flip is immediately retryable.
        log.info("phase 8: open")
        await _set_transition(db, "stable")
        await _set_cooldown(db, minutes=30)
        await _update_event(
            db, event_id, status="completed",
            completed_at=datetime.now(timezone.utc),
            flip_completed_at=datetime.now(timezone.utc),
        )
        log.info("toggle event %s complete", event_id)
    finally:
        await db.close()
