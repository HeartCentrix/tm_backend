"""Mail export (MBOX + EML) — streaming, blob-to-blob pipeline.

See docs/superpowers/specs/2026-04-19-mbox-mail-export-design.md for design.
Entry point is MailExportOrchestrator.run(); FolderExportTask implements the
per-folder streaming writer.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, List, Optional

# Import shared utilities. The restore-worker runtime mounts /app/shared, so
# `from shared.X import ...` works in prod. Tests import via shim that preloads
# the module; we also allow repo-root imports when PYTHONPATH=.
_HERE = os.path.dirname(__file__)
_SHARED = os.path.abspath(os.path.join(_HERE, "..", "..", "shared"))
if os.path.isdir(_SHARED) and _SHARED not in sys.path:
    sys.path.insert(0, os.path.dirname(_SHARED))  # parent so `import shared.X` works

from shared.mime_builder import AttachmentRef, build_eml, build_eml_streaming  # noqa: E402
from shared.mbox_writer import MboxWriter  # noqa: E402


@dataclass
class FolderExportResult:
    folder_name: str
    produced_blobs: List[str] = field(default_factory=list)
    exported_count: int = 0
    failed_items: List[dict] = field(default_factory=list)


class FolderExportTask:
    """Streams one folder's worth of messages into per-folder MBOX blobs (or
    loose EML blobs) on the given Azure shard. Parallel fetch of message JSON
    and attachment bytes; serial writer path so mbox chunks stay ordered."""

    def __init__(
        self,
        *,
        folder_name: str,
        items: List[Any],
        shard,
        source_container: str,
        dest_container: str,
        dest_blob_prefix: str,
        split_bytes: int,
        block_size: int,
        fetch_batch_size: int,
        queue_maxsize: int,
        format: str,
        include_attachments: bool,
        manifest,
    ):
        self.folder_name = folder_name
        self.items = items
        self.shard = shard
        self.source_container = source_container
        self.dest_container = dest_container
        self.dest_blob_prefix = dest_blob_prefix
        self.split_bytes = split_bytes
        self.block_size = block_size
        self.fetch_batch_size = fetch_batch_size
        self.queue_maxsize = queue_maxsize
        self.format = format.upper()
        self.include_attachments = include_attachments
        self.manifest = manifest

    async def run(self) -> FolderExportResult:
        result = FolderExportResult(folder_name=self.folder_name)
        if self.format == "MBOX":
            await self._run_mbox(result)
        elif self.format == "EML":
            await self._run_eml(result)
        else:
            raise ValueError(f"Unknown format: {self.format}")
        return result

    async def _fetch_message(self, item) -> Optional[dict]:
        raw = await self.shard.download_blob(self.source_container, item.blob_path)
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    async def _gather_attachments(self, att_paths) -> List[AttachmentRef]:
        if not self.include_attachments or not att_paths:
            return []
        out: List[AttachmentRef] = []
        for path in att_paths:
            async def _gen(p=path):
                async for chunk in self.shard.download_blob_stream(self.source_container, p):
                    yield chunk
            out.append(AttachmentRef(
                name=path.rsplit("/", 1)[-1],
                content_type="application/octet-stream",
                data_stream=_gen(),
            ))
        return out

    async def _build_eml_for_item(self, item):
        msg_json = await self._fetch_message(item)
        if msg_json is None:
            raise FileNotFoundError(f"blob missing: {item.blob_path}")
        attachments = await self._gather_attachments(
            getattr(item, "attachment_blob_paths", []) or []
        )
        if attachments:
            pieces = []
            async for chunk in build_eml_streaming(msg_json, attachments):
                pieces.append(chunk)
            eml_bytes = b"".join(pieces)
        else:
            eml_bytes = build_eml(msg_json, attachments=[])
        return msg_json, eml_bytes

    async def _run_mbox(self, result: FolderExportResult):
        part_index = 1
        current_path = f"{self.dest_blob_prefix}.01.mbox"
        block_ids: List[str] = []
        buf = bytearray()

        async def flush_block(final: bool = False):
            nonlocal buf
            while len(buf) >= self.block_size or (final and buf):
                take = min(self.block_size, len(buf))
                chunk = bytes(buf[:take])
                del buf[:take]
                bid = f"blk-{len(block_ids):05d}"
                await self.shard.stage_block(self.dest_container, current_path, bid, chunk)
                block_ids.append(bid)

        def emit(data: bytes):
            buf.extend(data)

        def _on_rollover(idx: int):
            nonlocal part_index
            part_index = idx + 1

        writer = MboxWriter(emit=emit, split_bytes=self.split_bytes, on_rollover=_on_rollover)

        async def finalize_current():
            nonlocal current_path, block_ids
            await flush_block(final=True)
            if block_ids:
                await self.shard.commit_block_list_manual(
                    self.dest_container, current_path, block_ids,
                    metadata={"folder": _sanitize_metadata(self.folder_name), "part": str(part_index)},
                )
                result.produced_blobs.append(current_path)
            block_ids = []

        idx = 0
        while idx < len(self.items):
            batch = self.items[idx : idx + self.fetch_batch_size]
            idx += len(batch)

            async def _prep(it):
                try:
                    msg_json, eml_bytes = await self._build_eml_for_item(it)
                    return it, (msg_json, eml_bytes), None
                except Exception as exc:
                    return it, None, f"{type(exc).__name__}: {exc}"

            prepared = await asyncio.gather(*(_prep(i) for i in batch))

            for it, data, err in prepared:
                if err:
                    result.failed_items.append({"id": it.external_id, "name": getattr(it, "name", ""), "error": err})
                    if self.manifest:
                        self.manifest.record_failure(
                            item_id=it.external_id, name=getattr(it, "name", ""),
                            folder=self.folder_name, error=err,
                        )
                    continue
                msg_json, eml_bytes = data
                sender = ((msg_json.get("from") or {}).get("emailAddress") or {}).get("address", "unknown@unknown")
                sent_iso = msg_json.get("sentDateTime") or msg_json.get("receivedDateTime")
                try:
                    from email.utils import parsedate_to_datetime
                    sent_epoch = int(parsedate_to_datetime(sent_iso).timestamp()) if sent_iso else int(time.time())
                except Exception:
                    sent_epoch = int(time.time())

                prior_part = part_index
                writer.append_message(eml_bytes, sender_addr=sender, sent_at_epoch=sent_epoch)
                result.exported_count += 1
                if self.manifest:
                    self.manifest.record_success(
                        item_id=it.external_id, name=getattr(it, "name", ""),
                        folder=self.folder_name, size_bytes=len(eml_bytes),
                    )

                if part_index != prior_part:
                    await finalize_current()
                    current_path = f"{self.dest_blob_prefix}.{part_index:02d}.mbox"
                    buf[:] = b""

                if len(buf) >= self.block_size:
                    await flush_block()

        writer.close()
        await finalize_current()

    async def _run_eml(self, result: FolderExportResult):
        for item in self.items:
            try:
                _, eml_bytes = await self._build_eml_for_item(item)
                safe_name = "".join(c if c.isalnum() or c in "-._" else "_" for c in (item.name or item.external_id))
                blob_path = f"{self.dest_blob_prefix}/{safe_name}-{item.external_id}.eml"
                await self.shard.upload_blob(self.dest_container, blob_path, eml_bytes)
                result.produced_blobs.append(blob_path)
                result.exported_count += 1
                if self.manifest:
                    self.manifest.record_success(
                        item_id=item.external_id, name=getattr(item, "name", ""),
                        folder=self.folder_name, size_bytes=len(eml_bytes),
                    )
            except Exception as exc:
                result.failed_items.append({
                    "id": item.external_id, "name": getattr(item, "name", ""),
                    "error": f"{type(exc).__name__}: {exc}",
                })
                if self.manifest:
                    self.manifest.record_failure(
                        item_id=item.external_id, name=getattr(item, "name", ""),
                        folder=self.folder_name, error=str(exc), error_class=type(exc).__name__,
                    )


def _sanitize_metadata(val: str) -> str:
    """Azure blob metadata values must be ASCII — folder names can be UTF-8."""
    return val.encode("ascii", errors="replace").decode("ascii")


try:
    from shared.export_manifest import ExportManifestBuilder  # type: ignore
except ImportError:  # pragma: no cover
    ExportManifestBuilder = None  # type: ignore


class MailExportOrchestrator:
    """Entry point for the mail-export pipeline. Partitions items by folder,
    spawns one FolderExportTask per folder under asyncio.Semaphore(parallelism),
    assembles the final ZIP + manifest.

    Final ZIP is built in memory for this task; Task 28 upgrades to streaming
    into stage_block calls via zipstream-ng."""

    def __init__(
        self,
        *,
        job_id: str,
        snapshot_ids: List[str],
        items: List[Any],
        shard,
        source_container: str,
        dest_container: str,
        parallelism: int,
        split_bytes: int,
        block_size: int,
        fetch_batch_size: int,
        queue_maxsize: int,
        format: str,
        include_attachments: bool,
        manifest: Optional["ExportManifestBuilder"] = None,
    ):
        self.job_id = job_id
        self.snapshot_ids = snapshot_ids
        self.items = items
        self.shard = shard
        self.source_container = source_container
        self.dest_container = dest_container
        self.parallelism = max(1, parallelism)
        self.split_bytes = split_bytes
        self.block_size = block_size
        self.fetch_batch_size = fetch_batch_size
        self.queue_maxsize = queue_maxsize
        self.format = format.upper()
        self.include_attachments = include_attachments
        self.manifest = manifest or (
            ExportManifestBuilder(job_id=job_id, snapshot_ids=snapshot_ids)
            if ExportManifestBuilder
            else None
        )

    def preflight(self) -> List[str]:
        """M5 — warn if total export size is pathologically large."""
        warnings: List[str] = []
        total = sum(getattr(it, "content_size", 0) or 0 for it in self.items)
        threshold = 100 * 1024 * 1024 * 1024  # 100 GB
        if self.include_attachments and total > threshold:
            warnings.append(
                f"large export: {total // (1024 ** 3)} GB with attachments on "
                f"({len(self.items)} items) — may take 15+ min and peak memory ~800 MB"
            )
        return warnings

    async def run(self) -> dict:
        # M5 preflight
        for w in self.preflight():
            print(f"[MailExportOrchestrator] {w}")  # routed to JobLog by caller

        # M6 memory monitor — soft-kill before Docker OOM mid-commit.
        from shared.memory_monitor import MemoryMonitor
        from shared.config import settings as _s

        breached = asyncio.Event()

        async def _on_breach():
            breached.set()

        # 4 GB Docker limit from compose M4. soft_limit_pct and grace from config.
        monitor = MemoryMonitor(
            limit_bytes=4 * 1024 * 1024 * 1024,
            soft_limit_pct=_s.EXPORT_MEMORY_SOFT_LIMIT_PCT,
            grace_seconds=_s.EXPORT_MEMORY_KILL_GRACE_SECONDS,
            poll_interval_seconds=5.0,
            on_breach=_on_breach,
        )
        await monitor.start()

        try:
            folders = self._partition_by_folder()
            sem = asyncio.Semaphore(self.parallelism)
            folder_results = await asyncio.gather(
                *(self._run_one_folder(sem, folder_name, folder_items)
                  for folder_name, folder_items in folders.items())
            )

            zip_blob_path = f"{self.job_id}/export_{int(time.time())}.zip"
            await self._assemble_zip(zip_blob_path, folder_results)

            status = "COMPLETED"
            if breached.is_set():
                status = "COMPLETED_WITH_ERRORS"
                print("[MailExportOrchestrator] M6 breach — export finalized under pressure")

            return {
                "blob_path": zip_blob_path,
                "exported_count": self.manifest.exported_count if self.manifest else sum(fr.exported_count for fr in folder_results),
                "failed_count": self.manifest.failed_count if self.manifest else sum(len(fr.failed_items) for fr in folder_results),
                "folder_count": len(folders),
                "status": status,
                "manifest": json.loads(self.manifest.to_json().decode("utf-8")) if self.manifest else None,
            }
        finally:
            await monitor.stop()

    def _partition_by_folder(self) -> dict:
        from collections import defaultdict

        out: dict = defaultdict(list)
        for it in self.items:
            folder = (getattr(it, "folder_path", None) or "Inbox").strip("/")
            out[folder].append(it)
        return dict(out)

    async def _run_one_folder(
        self,
        sem: asyncio.Semaphore,
        folder_name: str,
        folder_items: List[Any],
    ) -> FolderExportResult:
        async with sem:
            task = FolderExportTask(
                folder_name=folder_name,
                items=folder_items,
                shard=self.shard,
                source_container=self.source_container,
                dest_container=self.dest_container,
                dest_blob_prefix=f"{self.job_id}/{folder_name}",
                split_bytes=self.split_bytes,
                block_size=self.block_size,
                fetch_batch_size=self.fetch_batch_size,
                queue_maxsize=self.queue_maxsize,
                format=self.format,
                include_attachments=self.include_attachments,
                manifest=self.manifest,
            )
            return await task.run()

    async def _assemble_zip(
        self, zip_blob_path: str, folder_results: List[FolderExportResult]
    ):
        """Build the final ZIP. Task 28 replaces this with a streaming impl
        backed by zipstream-ng + stage_block."""
        import io as _io
        import zipfile as _zipfile

        zip_buf = _io.BytesIO()
        with _zipfile.ZipFile(zip_buf, "w", _zipfile.ZIP_STORED, allowZip64=True) as zf:
            for fr in folder_results:
                for blob_path in fr.produced_blobs:
                    content = await self.shard.download_blob(
                        self.dest_container, blob_path
                    )
                    arcname = blob_path.split(f"{self.job_id}/", 1)[-1]
                    zf.writestr(arcname, content or b"")
            if self.manifest:
                zf.writestr("_MANIFEST.json", self.manifest.to_json())

        await self.shard.upload_blob(
            self.dest_container, zip_blob_path, zip_buf.getvalue()
        )
