"""Helpers for routing export jobs to the right RabbitMQ queue.

Lives in `shared/` so every service (job-service, restore-worker, tests) picks
it up via the standard `from shared.X import ...` package path without needing
docker-build-context gymnastics.
"""
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


def pick_backup_queue(*, drive_bytes_estimate: int, resource_type: str) -> str:
    """Return the RabbitMQ queue for a backup job. OneDrive drives above
    BACKUP_HEAVY_THRESHOLD_BYTES go to the dedicated heavy pool so regular
    backup-worker replicas aren't blocked by a single monster drive."""
    if not settings.BACKUP_HEAVY_ENABLED:
        return settings.BACKUP_WORKER_QUEUE
    if resource_type != "USER_ONEDRIVE":
        return settings.BACKUP_WORKER_QUEUE
    if drive_bytes_estimate >= settings.BACKUP_HEAVY_THRESHOLD_BYTES:
        return settings.BACKUP_HEAVY_QUEUE
    return settings.BACKUP_WORKER_QUEUE
