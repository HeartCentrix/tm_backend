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

import logging
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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
    ):
        self.job_id = job_id
        self.items = items
        self.spec = spec
        self.storage_shard = storage_shard
        self.dest_container = dest_container or "exports"
        self.source_container = source_container
        self.tenant_id = tenant_id
        self.update_progress = update_progress
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

        try:
            for idx, group in enumerate(groups):
                WriterClass = writer_map.get(group.item_type)
                if WriterClass is None:
                    continue
                writer = WriterClass()
                write_result = await writer.write(
                    group=group,
                    workdir=self.workdir,
                    split_gb=self.split_gb,
                    shard=self.storage_shard,
                    source_container=self.source_container,
                )
                all_pst_paths.extend(write_result.pst_paths)
                item_counts[group.item_type] = (
                    item_counts.get(group.item_type, 0) + write_result.item_count
                )
                failed_counts[group.item_type] = (
                    failed_counts.get(group.item_type, 0) + write_result.failed_count
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
            return {
                "job_id": self.job_id,
                "status": "done" if success else "upload_failed",
                "blob_path": blob_path if success else None,
                "container": self.dest_container,
                "pst_files": pst_file_names,
                "pst_count": len(pst_file_names),
                "total_size_bytes": total_size,
                "item_counts_by_type": item_counts,
                "failed_counts_by_type": failed_counts,
                "granularity": self.granularity,
            }
        finally:
            # Step 5: Always clean up local workdir (PSTs etc.).
            _cleanup_dir(self.workdir)
            await self._safe_progress(100)
