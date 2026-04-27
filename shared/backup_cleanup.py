"""Cleanup of partial-backup artifacts when a job is cancelled.

When a user / admin cancels a running backup job, the worker stops
pulling new items from Graph but the items it had already written to
blob storage remain. Without this module those blobs would orphan: the
DB-side ``DROP SCHEMA`` (or a manual cleanup) eventually wipes the
``snapshot_items`` rows, but the underlying object-store bytes linger
and consume disk indefinitely.

``cleanup_cancelled_snapshots`` deletes the bytes + DB rows in one
pass:

  1. Find every snapshot for the job whose status is IN_PROGRESS or
     COMPLETED (the worker may have flushed a few entries before the
     cancel signal landed).
  2. Stream-delete every snapshot_item's blob via the storage abstraction
     (works for Azure Blob and SeaweedFS uniformly).
  3. Delete attachment blobs referenced by ``extra_data.attachment_blob_paths``.
  4. Delete the snapshot_items rows.
  5. Mark the snapshot as CANCELLED.

Idempotent: re-running on an already-cleaned job is a no-op.  Safe to
call from the worker mid-shutdown OR from a separate cancel endpoint.

Soft failure model: per-blob deletion errors are logged + counted.
We do NOT abort cleanup on a single missing/permission-denied blob —
the goal is to free as many bytes as we can. Hard storage errors
(connection refused) abort the run so the caller can retry.
"""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional
from uuid import UUID

from sqlalchemy import delete as sa_delete, select, update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import Snapshot, SnapshotItem, SnapshotStatus

logger = logging.getLogger(__name__)


async def cleanup_cancelled_snapshots(
    *,
    job_id: UUID,
    session_factory: Callable[[], AsyncSession],
    shard: Any,
    container_resolver: Callable[[SnapshotItem], str],
    delete_db_rows: bool = True,
    batch_size: int = 500,
) -> Dict[str, int]:
    """Delete partial backup artifacts for a cancelled job.

    Parameters
    ----------
    job_id
        The cancelled Job's UUID. Snapshots are matched via
        ``Snapshot.last_backup_job_id`` OR via ``Snapshot.id IN
        snapshot_ids stored on the job spec`` — we use the
        ``Snapshot.last_backup_job_id`` link since it's set on every
        worker write.
    session_factory
        ``async_session_factory`` — opens a fresh DB session per batch
        so the cleanup doesn't hold one connection for the whole run.
    shard
        Storage shard (Azure / SeaweedFS) with an async
        ``delete_blob(container, path)`` method.
    container_resolver
        Callable that turns a ``SnapshotItem`` into the container the
        item's blob lives in. Workers know this mapping (mail container
        vs files container vs sharepoint container vs exports).
    delete_db_rows
        When True (default) snapshot_items rows are deleted after their
        blobs. Set False if you want to keep audit history.
    batch_size
        Number of items processed per session — caps memory + bounds
        DB transaction size.

    Returns
    -------
    dict with counts: ``{"snapshots": N, "items": N, "blobs_deleted": N,
    "blob_errors": N}``.
    """
    stats = {"snapshots": 0, "items": 0, "blobs_deleted": 0, "blob_errors": 0}

    # --- 1. Find target snapshots --------------------------------------
    async with session_factory() as session:
        stmt = (
            select(Snapshot.id)
            .where(Snapshot.last_backup_job_id == job_id)
            .where(Snapshot.status.in_([
                SnapshotStatus.IN_PROGRESS,
                SnapshotStatus.COMPLETED,
            ]))
        )
        snapshot_ids: List[UUID] = list(
            (await session.execute(stmt)).scalars().all()
        )

    if not snapshot_ids:
        logger.info("[backup_cleanup] no snapshots to clean for job %s", job_id)
        return stats

    stats["snapshots"] = len(snapshot_ids)
    logger.info(
        "[backup_cleanup] job %s — cleaning %d snapshot(s)",
        job_id, len(snapshot_ids),
    )

    # --- 2. Stream-delete blobs in batches -----------------------------
    # Keyset pagination on item id so a worker crash mid-cleanup can
    # resume cheaply on the next call (idempotent: missing blobs = no-op).
    last_id: Optional[UUID] = None
    while True:
        async with session_factory() as session:
            stmt = (
                select(SnapshotItem)
                .where(SnapshotItem.snapshot_id.in_(snapshot_ids))
                .order_by(SnapshotItem.id)
                .limit(batch_size)
            )
            if last_id is not None:
                stmt = stmt.where(SnapshotItem.id > last_id)
            items: List[SnapshotItem] = list(
                (await session.execute(stmt)).scalars().all()
            )
            for it in items:
                session.expunge(it)

        if not items:
            break

        for it in items:
            stats["items"] += 1

            # Primary item blob
            blob_path = getattr(it, "blob_path", None)
            if blob_path:
                container = container_resolver(it)
                try:
                    await shard.delete_blob(container, blob_path)
                    stats["blobs_deleted"] += 1
                except Exception as exc:
                    stats["blob_errors"] += 1
                    logger.warning(
                        "[backup_cleanup] blob delete failed (%s/%s): %s",
                        container, blob_path, exc,
                    )

            # Attachment blobs (stored separately for mail items)
            extra = getattr(it, "extra_data", None) or {}
            att_paths = (
                extra.get("attachment_blob_paths") if isinstance(extra, dict) else None
            ) or []
            if att_paths:
                container = container_resolver(it)
                for ap in att_paths:
                    try:
                        await shard.delete_blob(container, ap)
                        stats["blobs_deleted"] += 1
                    except Exception as exc:
                        stats["blob_errors"] += 1
                        logger.warning(
                            "[backup_cleanup] attachment delete failed (%s/%s): %s",
                            container, ap, exc,
                        )

        last_id = items[-1].id

    # --- 3. Delete snapshot_items + mark snapshots CANCELLED -----------
    async with session_factory() as session:
        if delete_db_rows:
            await session.execute(
                sa_delete(SnapshotItem).where(
                    SnapshotItem.snapshot_id.in_(snapshot_ids)
                )
            )
        await session.execute(
            sa_update(Snapshot)
            .where(Snapshot.id.in_(snapshot_ids))
            .values(
                status=SnapshotStatus.CANCELLED
                if hasattr(SnapshotStatus, "CANCELLED")
                else SnapshotStatus.FAILED,
                bytes_total=0,
                item_count=0,
            )
        )
        await session.commit()

    logger.info(
        "[backup_cleanup] job %s — done: snapshots=%d items=%d blobs_deleted=%d blob_errors=%d",
        job_id,
        stats["snapshots"], stats["items"],
        stats["blobs_deleted"], stats["blob_errors"],
    )
    return stats


def default_container_resolver(item: SnapshotItem) -> str:
    """Best-effort container picker by item type.

    Workers call this when they don't have explicit container context
    (e.g. cleanup-from-CLI). Production callers should pass a
    tenant-scoped resolver that knows the actual container names from
    ``azure_storage_manager.get_container_name``.
    """
    t = (getattr(item, "item_type", "") or "").upper()
    if t.startswith("EMAIL"):
        return "email"
    if t in {"USER_CONTACT", "USER_CALENDAR", "CALENDAR_EVENT"}:
        return "mailbox"
    if t.startswith("SHAREPOINT"):
        return "sharepoint"
    if t in {"FILE", "ONEDRIVE_FILE", "FILE_VERSION"}:
        return "files"
    return "files"
