"""Live-progress helper shared by every Azure workload handler.

Writes `jobs.progress_pct` on a short-lived DB session so the Protection /
Activity UI can animate a live progress bar. Best-effort: swallows errors
so a transient DB hiccup never interrupts the long-running backup. Use
from any handler:

    from handlers._progress import update_job_pct
    await update_job_pct(job_id, 42)
"""
from typing import Optional
import uuid as _uuid

from shared.database import async_session_factory
from shared.models import Job


async def update_job_pct(job_id, pct: int) -> None:
    """Update `jobs.progress_pct` for a running backup job. No-op when
    the job id is missing or the job has already moved past RUNNING."""
    if job_id is None:
        return
    try:
        if isinstance(job_id, str):
            job_id = _uuid.UUID(job_id)
    except Exception:
        return
    clamped = max(0, min(100, int(pct)))
    try:
        async with async_session_factory() as session:
            j: Optional[Job] = await session.get(Job, job_id)
            if not j:
                return
            status_val = j.status.name if hasattr(j.status, "name") else str(j.status)
            # Only update while the job is still in flight — avoid
            # overwriting a settled COMPLETED/FAILED/CANCELLED row.
            if status_val not in ("RUNNING", "QUEUED"):
                return
            j.progress_pct = clamped
            await session.commit()
    except Exception:
        pass
