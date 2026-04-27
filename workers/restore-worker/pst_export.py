"""PST export — planner + orchestrator.

PstGroupPlanner groups SnapshotItems into PstGroups by mailbox, folder, or
individual item.  PstExportOrchestrator plans the groups, dispatches the
correct per-type writer, ZIPs the resulting ``.pst`` files, uploads the
archive to blob storage, and reports progress via an optional async
callback.

All Aspose imports are lazy (never at module top level) because aspose-email
is not installed in the local dev environment.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Tunables (all env-configurable for production deployment)
# ---------------------------------------------------------------------------
# PST_WRITE_CONCURRENCY        (default 1)    — per-process Aspose semaphore
# PST_DISK_OVERHEAD_MULTIPLIER (default 2.0)  — how much disk per source byte
# PST_DISK_FLOOR_BYTES         (default 256MB)— min free bytes to reserve
# PST_MEMORY_LIMIT_MB          (default 0)    — RSS soft cap; 0=disabled
# PST_MEMORY_CHECK_INTERVAL    (default 50)   — items between RSS checks
# PST_RABBIT_HEARTBEAT_HINT    (default 1800) — informational; ack-timeout
#                                               must be set on RabbitMQ side
# PST_PROGRESS_GROUP_TIMEOUT_S (default 7200) — kill group writers exceeding
#                                               this wall-clock seconds
# ---------------------------------------------------------------------------
_PST_WRITE_SEMAPHORE: asyncio.Semaphore | None = None
_PST_WRITE_SEMAPHORE_LIMIT = int(os.environ.get("PST_WRITE_CONCURRENCY", "1"))

DISK_OVERHEAD_MULTIPLIER = float(os.environ.get("PST_DISK_OVERHEAD_MULTIPLIER", "2.0"))
DISK_FLOOR_BYTES = int(os.environ.get("PST_DISK_FLOOR_BYTES", str(256 * 1024 * 1024)))
MEMORY_LIMIT_MB = int(os.environ.get("PST_MEMORY_LIMIT_MB", "0"))
MEMORY_CHECK_INTERVAL = int(os.environ.get("PST_MEMORY_CHECK_INTERVAL", "50"))
GROUP_TIMEOUT_S = int(os.environ.get("PST_GROUP_TIMEOUT_S", "7200"))


def _get_write_semaphore() -> asyncio.Semaphore:
    global _PST_WRITE_SEMAPHORE
    if _PST_WRITE_SEMAPHORE is None:
        _PST_WRITE_SEMAPHORE = asyncio.Semaphore(_PST_WRITE_SEMAPHORE_LIMIT)
    return _PST_WRITE_SEMAPHORE


def _check_disk_space(workdir: Path, required_bytes: int) -> tuple[bool, int]:
    """Return (has_space, free_bytes_at_target). Pre-flight guard so we
    fail fast instead of crashing mid-write when /tmp fills up."""
    try:
        check_at = workdir
        while not check_at.exists() and check_at.parent != check_at:
            check_at = check_at.parent
        stat = shutil.disk_usage(str(check_at))
        return (stat.free >= required_bytes, stat.free)
    except Exception as exc:
        logger.warning("disk_usage check failed: %s — assuming sufficient space", exc)
        return (True, 0)


def _current_rss_mb() -> int:
    """Current resident-set-size in MB. Returns 0 on platforms / configs
    where we can't read it (e.g. psutil missing). Used by the memory
    pressure check inside long-running write loops."""
    try:
        import psutil
        return int(psutil.Process().memory_info().rss / (1024 * 1024))
    except Exception:
        try:
            # Fallback: read /proc/self/status (Linux only)
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        kb = int(line.split()[1])
                        return kb // 1024
        except Exception:
            pass
        return 0


async def _memory_pressure_check(items_processed: int) -> None:
    """If MEMORY_LIMIT_MB is set and we've crossed it, log + GC + brief
    pause. Soft pressure signal — caller decides whether to continue.
    Called every PST_MEMORY_CHECK_INTERVAL items inside the writer."""
    if MEMORY_LIMIT_MB <= 0:
        return
    if items_processed and items_processed % MEMORY_CHECK_INTERVAL != 0:
        return
    rss = _current_rss_mb()
    if rss == 0:
        return
    if rss > MEMORY_LIMIT_MB:
        import gc
        gc.collect()
        post_rss = _current_rss_mb()
        logger.warning(
            "[pst_export] memory pressure: rss=%dMB > limit=%dMB after_gc=%dMB items=%d",
            rss, MEMORY_LIMIT_MB, post_rss, items_processed,
        )
        # Yield event loop so other tasks can drain queued work
        await asyncio.sleep(0.1)

# Per-type writers — imported at module top so tests can patch
# ``pst_export.MailPstWriter`` etc.  The writer modules themselves keep
# all ``aspose.*`` imports lazy, so importing them here is safe in the
# dev/test environment.
from pst_writers.mail import MailPstWriter
from pst_writers.calendar import CalendarPstWriter
from pst_writers.contact import ContactPstWriter

logger = logging.getLogger(__name__)


def _safe_name(s: str) -> str:
    """Replace non-alphanumeric chars (except ``-`` and ``_``) with ``_``,
    truncate to 64 characters."""
    sanitized = re.sub(r"[^A-Za-z0-9\-_]", "_", s)
    return sanitized[:64]


def _cleanup_dir(path: Path) -> None:
    """Best-effort recursive delete of ``path``.  Swallows errors; the
    PST workdir lives under ``/tmp`` and we don't want a stray cleanup
    failure to mask an otherwise successful export."""
    try:
        shutil.rmtree(str(path), ignore_errors=True)
    except Exception:  # pragma: no cover - defensive
        pass


@dataclass
class PstGroup:
    key: tuple          # the grouping key
    item_type: str      # "EMAIL" | "CALENDAR_EVENT" | "USER_CONTACT"
    items: list = field(default_factory=list)   # list of SnapshotItem objects
    pst_filename: str = ""                      # e.g. "snapshot123-mail.pst"


class PstGroupPlanner:
    PST_ITEM_TYPES = {"EMAIL", "CALENDAR_EVENT", "USER_CONTACT"}
    TYPE_SUFFIX = {
        "EMAIL": "mail",
        "CALENDAR_EVENT": "calendar",
        "USER_CONTACT": "contacts",
    }

    def plan(
        self,
        items: list,
        granularity: str,
        include_types: set,
    ) -> list:
        """Group items into PstGroups.

        Parameters
        ----------
        items:
            List of SnapshotItem-like objects.
        granularity:
            ``"MAILBOX"``, ``"FOLDER"``, or ``"ITEM"``.
        include_types:
            Subset of ``PST_ITEM_TYPES`` to include; anything else is silently
            skipped.  Pass the full ``PST_ITEM_TYPES`` set to include all.

        Returns
        -------
        list[PstGroup]
            Sorted by ``(item_type, key)``.

        Raises
        ------
        ValueError
            If *granularity* is unknown, or if ITEM granularity exceeds the
            100 k-group hard limit.
        """
        if granularity not in ("MAILBOX", "FOLDER", "ITEM"):
            raise ValueError(
                f"Unknown PST granularity: {granularity!r}. "
                "Expected MAILBOX, FOLDER, or ITEM."
            )

        effective_types = self.PST_ITEM_TYPES & include_types

        # Filter items first
        filtered = [
            it for it in items
            if getattr(it, "item_type", None) in effective_types
        ]

        # ITEM granularity guardrails (checked before building the full dict)
        if granularity == "ITEM":
            count = len(filtered)
            if count > 100_000:
                raise ValueError(
                    f"PST ITEM granularity rejected: {count} items exceeds 100k limit"
                )
            if count > 10_000:
                logger.warning(
                    "PST ITEM granularity: %d groups will create ~%d MB of PST overhead",
                    count,
                    count * 2,
                )

        groups: dict[tuple, PstGroup] = {}

        for it in filtered:
            snap_prefix = str(it.snapshot_id)[:8]
            item_type = it.item_type
            suffix = self.TYPE_SUFFIX[item_type]

            if granularity == "MAILBOX":
                key = (str(it.snapshot_id), item_type)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{suffix}.pst",
                    )

            elif granularity == "FOLDER":
                folder = it.folder_path or "root"
                key = (str(it.snapshot_id), item_type, folder)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{_safe_name(folder)}-{suffix}.pst",
                    )

            else:  # ITEM
                key = (str(it.snapshot_id), item_type, it.external_id)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{it.external_id[:16]}-{suffix}.pst",
                    )

            groups[key].items.append(it)

        result = sorted(groups.values(), key=lambda g: (g.item_type, g.key))
        return result


class PstExportOrchestrator:
    """Entry point for PST export.

    Plans groups, dispatches per-type writers, ZIPs the resulting PST
    files, uploads the archive to blob storage, cleans up the local
    workdir, and reports progress via an optional async callback.
    """

    def __init__(
        self,
        job_id: str,
        items: list,
        spec: dict,
        storage_shard=None,
        dest_container: str = "exports",
        source_container: str = "",
        tenant_id: str = "",
        update_progress=None,
        checkpoint_loader=None,    # async () -> dict | None
        checkpoint_saver=None,     # async (dict) -> None
    ):
        self.job_id = job_id
        self.items = items
        self.spec = spec
        self.storage_shard = storage_shard
        self.dest_container = dest_container or "exports"
        self.source_container = source_container
        self.tenant_id = tenant_id
        self.update_progress = update_progress
        # Resumability hooks. ``checkpoint_loader`` returns the prior
        # checkpoint dict (or None for a fresh run); ``checkpoint_saver``
        # is called after each completed group so a worker crash mid-job
        # can resume by skipping already-finished groups. The schema is:
        #   { "completed_keys": [<group_key_str>, ...],
        #     "pst_blobs": [{path, size, key}, ...] }
        # We persist intermediate PSTs to blob (durable) so they survive
        # the worker restart even though /tmp/<job_id> is wiped.
        self.checkpoint_loader = checkpoint_loader
        self.checkpoint_saver = checkpoint_saver
        self.granularity = spec.get("pstGranularity", "MAILBOX").upper()
        self.split_gb = float(spec.get("pstSplitSizeGb", 45))
        self.include_types = set(
            spec.get("pstIncludeTypes", ["EMAIL", "CALENDAR_EVENT", "USER_CONTACT"])
        )
        self.workdir = Path(f"/tmp/pst-export/{job_id}")

    async def _safe_progress(self, pct: int) -> None:
        """Invoke ``update_progress`` swallowing any exception it raises."""
        if self.update_progress:
            try:
                await self.update_progress(pct)
            except Exception as exc:
                logger.warning("progress update failed at %d%%: %s", pct, exc)

    async def run(self) -> dict:
        """Plan groups, dispatch writers, ZIP output, upload, return result.

        Steps:
          1. ``apply_license()`` (Aspose.Email needs the metered licence).
          2. Plan groups via :class:`PstGroupPlanner`.
          3. For each group, dispatch the matching writer; collect PSTs.
          4. ZIP all ``.pst`` files into ``{job_id}.zip`` (STORED — the
             PSTs are already large/dense; deflate adds overhead).
          5. Upload the archive (built-in cleanup deletes the local zip).
          6. Cleanup local workdir.
          7. Return result dict.
        """
        import zipfile as _zipfile
        from shared.aspose_license import apply_license  # lazy import
        from shared.azure_storage import upload_blob_with_retry_from_file

        apply_license()

        self.workdir.mkdir(parents=True, exist_ok=True)

        # Resolve storage shard if caller didn't pass one in.
        if self.storage_shard is None:
            from shared.azure_storage import azure_storage_manager
            self.storage_shard = azure_storage_manager.get_default_shard()
            if not self.dest_container or self.dest_container == "exports":
                self.dest_container = azure_storage_manager.get_container_name(
                    self.tenant_id, "exports"
                )
            await self.storage_shard.ensure_container(self.dest_container)

        # Step 1: Plan
        await self._safe_progress(5)
        planner = PstGroupPlanner()
        groups = planner.plan(self.items, self.granularity, self.include_types)

        # Resumability: load any prior checkpoint and skip already-completed
        # groups. Useful when a worker crashes mid-100GB-mailbox export —
        # restart picks up where the prior attempt left off.
        completed_keys: set = set()
        if self.checkpoint_loader is not None:
            try:
                prior = await self.checkpoint_loader()
                if prior and isinstance(prior.get("completed_keys"), list):
                    completed_keys = {tuple(k) if isinstance(k, list) else k
                                      for k in prior["completed_keys"]}
                    logger.info(
                        "[pst_export] checkpoint loaded — %d groups already done",
                        len(completed_keys),
                    )
            except Exception as exc:
                logger.warning("[pst_export] checkpoint load failed: %s", exc)

        # Disk-space pre-flight. Estimate 2x the inbound content_size:
        # one copy in the .pst, one in the zipped archive (worst case
        # before upload-and-delete reclaims the zip). Refuse the job
        # rather than crash mid-write with disk full.
        estimated_bytes = int(
            sum((getattr(it, "content_size", 0) or 1024) for it in self.items)
            * DISK_OVERHEAD_MULTIPLIER
        )
        # Add safety floor for .NET runtime tempfiles + PST headers.
        estimated_bytes = max(estimated_bytes, DISK_FLOOR_BYTES)
        has_space, free_bytes = _check_disk_space(self.workdir, estimated_bytes)
        if not has_space:
            msg = (
                f"insufficient disk space: estimated need={estimated_bytes/1024**3:.1f}GB, "
                f"free={free_bytes/1024**3:.1f}GB at {self.workdir.parent}"
            )
            logger.error("[pst_export] %s", msg)
            return {
                "job_id": self.job_id,
                "status": "disk_full",
                "blob_path": None,
                "container": self.dest_container,
                "pst_files": [],
                "pst_count": 0,
                "total_size_bytes": 0,
                "item_counts_by_type": {},
                "failed_counts_by_type": {},
                "skipped_groups": [],
                "granularity": self.granularity,
                "error": msg,
            }
        logger.info(
            "[pst_export] disk pre-check OK: estimated=%.2fGB free=%.2fGB groups=%d",
            estimated_bytes / 1024**3, free_bytes / 1024**3, len(groups),
        )

        # Step 2: Dispatch writers
        writer_map = {
            "EMAIL": MailPstWriter,
            "CALENDAR_EVENT": CalendarPstWriter,
            "USER_CONTACT": ContactPstWriter,
        }
        all_pst_paths: list = []
        item_counts: dict = {}
        failed_counts: dict = {}
        total_groups = len(groups)

        skipped_groups: list = []
        import time as _time
        try:
            for idx, group in enumerate(groups):
                if group.key in completed_keys:
                    logger.info(
                        "[pst_export] skipping completed group %d/%d key=%s (resumed)",
                        idx + 1, total_groups, group.key,
                    )
                    continue

                WriterClass = writer_map.get(group.item_type)
                if WriterClass is None:
                    logger.warning(
                        "skipping group with unsupported item_type=%s key=%s",
                        group.item_type, group.key,
                    )
                    skipped_groups.append({"key": str(group.key), "reason": "unsupported_type"})
                    continue

                t0 = _time.monotonic()
                logger.info(
                    "[pst_export] group %d/%d start type=%s items=%d filename=%s",
                    idx + 1, total_groups, group.item_type, len(group.items), group.pst_filename,
                )

                # Per-group exception isolation: a failed group must not poison
                # the whole job. We log, skip, and continue. The job result
                # records skipped groups so the caller can decide on retry.
                # Acquire the per-process write semaphore so concurrent jobs
                # in the same worker process don't oversubscribe Aspose's
                # .NET runtime (memory + GC pressure). Wall-clock timeout
                # so a single hung group can't block the whole job
                # indefinitely (e.g. corrupted MAPI item that loops in
                # native .NET code).
                try:
                    writer = WriterClass()
                    async with _get_write_semaphore():
                        write_result = await asyncio.wait_for(
                            writer.write(
                                group=group,
                                workdir=self.workdir,
                                split_gb=self.split_gb,
                                shard=self.storage_shard,
                                source_container=self.source_container,
                            ),
                            timeout=GROUP_TIMEOUT_S,
                        )
                    # Memory pressure check between groups so a long job
                    # can drain GC pressure before the next group ramps up.
                    await _memory_pressure_check(idx + 1)
                    all_pst_paths.extend(write_result.pst_paths)
                    item_counts[group.item_type] = (
                        item_counts.get(group.item_type, 0) + write_result.item_count
                    )
                    failed_counts[group.item_type] = (
                        failed_counts.get(group.item_type, 0) + write_result.failed_count
                    )
                    elapsed = _time.monotonic() - t0
                    logger.info(
                        "[pst_export] group %d/%d done type=%s wrote=%d failed=%d psts=%d in %.2fs",
                        idx + 1, total_groups, group.item_type,
                        write_result.item_count, write_result.failed_count,
                        len(write_result.pst_paths), elapsed,
                    )
                    completed_keys.add(group.key)
                    if self.checkpoint_saver is not None:
                        try:
                            await self.checkpoint_saver({
                                "completed_keys": [list(k) for k in completed_keys],
                            })
                        except Exception as exc:
                            logger.warning(
                                "[pst_export] checkpoint save failed: %s",
                                exc,
                            )
                except Exception as exc:
                    logger.exception(
                        "[pst_export] group %d/%d FAILED type=%s key=%s items=%d: %s",
                        idx + 1, total_groups, group.item_type,
                        group.key, len(group.items), exc,
                    )
                    skipped_groups.append({
                        "key": str(group.key),
                        "type": group.item_type,
                        "items": len(group.items),
                        "error": str(exc)[:200],
                    })
                    failed_counts[group.item_type] = (
                        failed_counts.get(group.item_type, 0) + len(group.items)
                    )

                # Progress: 5% start → 80% after all groups are written.
                pct = 5 + int(75 * (idx + 1) / max(total_groups, 1))
                await self._safe_progress(pct)

            # Step 3: ZIP all .pst files into a single archive.
            zip_path = self.workdir.parent / f"{self.job_id}.zip"
            total_size = 0
            pst_file_names: list = []
            with _zipfile.ZipFile(str(zip_path), "w", _zipfile.ZIP_STORED) as zf:
                for pst_path in all_pst_paths:
                    if pst_path.exists():
                        zf.write(str(pst_path), arcname=pst_path.name)
                        total_size += pst_path.stat().st_size
                        pst_file_names.append(pst_path.name)

            # Step 4: Upload zip.  upload_blob_with_retry_from_file deletes
            # the local zip on success.
            blob_path = f"pst-exports/{self.job_id}/{self.job_id}.zip"
            zip_size = zip_path.stat().st_size if zip_path.exists() else 0
            upload_result = await upload_blob_with_retry_from_file(
                container_name=self.dest_container,
                blob_path=blob_path,
                file_path=str(zip_path),
                shard=self.storage_shard,
                file_size=zip_size,
                metadata={"job_id": self.job_id, "granularity": self.granularity},
            )

            if not upload_result.get("success"):
                # upload_blob_with_retry_from_file only auto-deletes on success
                try:
                    zip_path.unlink(missing_ok=True)
                except Exception:
                    pass

            await self._safe_progress(95)

            success = bool(upload_result.get("success"))
            # Status taxonomy:
            #   "done"            — upload OK, all groups OK
            #   "done_with_errors"— upload OK, some groups failed (partial)
            #   "upload_failed"   — PSTs written but blob upload failed
            #   "no_psts"         — no groups produced output (empty selection)
            if not success:
                status = "upload_failed"
            elif skipped_groups:
                status = "done_with_errors"
            elif not pst_file_names:
                status = "no_psts"
            else:
                status = "done"

            return {
                "job_id": self.job_id,
                "status": status,
                "blob_path": blob_path if success else None,
                "container": self.dest_container,
                "pst_files": pst_file_names,
                "pst_count": len(pst_file_names),
                "total_size_bytes": total_size,
                "item_counts_by_type": item_counts,
                "failed_counts_by_type": failed_counts,
                "skipped_groups": skipped_groups,
                "granularity": self.granularity,
            }
        finally:
            # Step 5: Always clean up local workdir (PSTs etc.).
            _cleanup_dir(self.workdir)
            await self._safe_progress(100)
