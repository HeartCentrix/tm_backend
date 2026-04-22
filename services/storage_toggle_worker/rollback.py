"""Per-phase rollback."""
from __future__ import annotations

import logging

log = logging.getLogger("toggle.rollback")


async def rollback(db, event_id: str, phase: str, error: str) -> None:
    log.error("rolling back from phase=%s error=%s", phase, error)
    await db.execute(
        "UPDATE system_config SET transition_state='stable', updated_at=NOW() WHERE id=1"
    )
    await db.execute(
        "UPDATE storage_toggle_events "
        "SET status='failed', error_message=$2, completed_at=NOW() "
        "WHERE id=$1",
        event_id, f"{phase}: {error}",
    )
