"""Helpers for routing export jobs to the right RabbitMQ queue.

Lives in `shared/` so every service (job-service, restore-worker, tests) picks
it up via the standard `from shared.X import ...` package path without needing
docker-build-context gymnastics.
"""
from typing import Optional

from shared.config import settings


def pick_export_queue(*, total_bytes: int, include_attachments: bool) -> str:
    """Return the RabbitMQ queue for an export job. Heavy exports go to a
    dedicated worker pool with larger memory limits — see Task 23 of the
    mail-export plan."""
    if not settings.HEAVY_EXPORT_ENABLED:
        return "restore.normal"
    if include_attachments and total_bytes >= settings.HEAVY_EXPORT_THRESHOLD_BYTES:
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


_HEAVY_RESTORE_THRESHOLD = 50 * 1024**3  # 50 GiB


def pick_restore_queue(total_bytes: int) -> str:
    """Route restore jobs by scope size.

    Large restores (>50 GiB) run on restore.heavy so they don't block
    small quick restores on the shared restore.normal queue. Mirrors
    pick_backup_queue / pick_export_queue in the same module.
    """
    if total_bytes and total_bytes > _HEAVY_RESTORE_THRESHOLD:
        return "restore.heavy"
    return "restore.normal"
