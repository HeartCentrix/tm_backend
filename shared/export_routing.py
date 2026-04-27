"""Helpers for routing export jobs to the right RabbitMQ queue.

Lives in `shared/` so every service (job-service, restore-worker, tests) picks
it up via the standard `from shared.X import ...` package path without needing
docker-build-context gymnastics.
"""
import os
from typing import Optional

from shared.config import settings


def pick_export_queue(total_bytes: int = 0, include_attachments: bool = True) -> str:
    """Return the RabbitMQ queue for an export job.

    Heavy exports go to a dedicated worker pool with larger memory limits.
    Both keyword and positional arg forms are accepted for back-compat with
    existing callers.

    The ``include_attachments`` filter used to gate heavy routing — for PST
    exports we now route by total size regardless, since even no-attachment
    100GB exports are heavy by item count alone.
    """
    if not settings.HEAVY_EXPORT_ENABLED:
        return "restore.normal"
    if total_bytes >= settings.HEAVY_EXPORT_THRESHOLD_BYTES:
        return settings.HEAVY_EXPORT_QUEUE
    return "restore.normal"


def pick_backup_queue(
    *,
    drive_bytes_estimate: int,
    resource_type: str,
    default_queue: Optional[str] = None,
) -> str:
    """Return the RabbitMQ queue for a backup job. OneDrive drives above
    BACKUP_HEAVY_THRESHOLD_BYTES go to the dedicated heavy pool so regular
    backup-worker replicas aren't blocked by a single monster drive.

    ``default_queue`` lets the caller preserve the queue semantics of the
    trigger path (e.g. "backup.urgent" for user-initiated backups) while
    still opting in to heavy routing for oversized OneDrive drives. When
    omitted, falls back to the scheduler queue (``BACKUP_WORKER_QUEUE``)
    for back-compat with existing callers."""
    fallback = default_queue or settings.BACKUP_WORKER_QUEUE
    if not settings.BACKUP_HEAVY_ENABLED:
        return fallback
    if resource_type != "USER_ONEDRIVE":
        return fallback
    if drive_bytes_estimate >= settings.BACKUP_HEAVY_THRESHOLD_BYTES:
        return settings.BACKUP_HEAVY_QUEUE
    return fallback


_HEAVY_RESTORE_THRESHOLD = int(
    os.getenv("HEAVY_RESTORE_THRESHOLD_BYTES", str(20 * 1024**3))
)  # default 20 GiB — catches whales sooner on large workloads


def pick_restore_queue(total_bytes: int) -> str:
    """Route restore jobs by scope size.

    Large restores (>HEAVY_RESTORE_THRESHOLD_BYTES) run on restore.heavy
    so they don't block small quick restores on the shared restore.normal
    queue. Threshold is env-tunable for very large M365 deployments where
    50 GB used to be too high a bar.
    """
    if total_bytes and total_bytes > _HEAVY_RESTORE_THRESHOLD:
        return "restore.heavy"
    return "restore.normal"
