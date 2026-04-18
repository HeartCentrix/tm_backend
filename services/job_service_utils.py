"""Helpers used by job-service. Kept outside services/job-service/ so tests
can import without starting the FastAPI app."""
from shared.config import settings


def pick_export_queue(*, total_bytes: int, include_attachments: bool) -> str:
    """Return the RabbitMQ queue for an export job. Heavy exports go to a
    dedicated worker pool with larger memory limits — see Task 23."""
    if not settings.HEAVY_EXPORT_ENABLED:
        return "restore.normal"
    if include_attachments and total_bytes >= settings.HEAVY_EXPORT_THRESHOLD_BYTES:
        return settings.HEAVY_EXPORT_QUEUE
    return "restore.normal"
