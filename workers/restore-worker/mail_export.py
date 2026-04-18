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


async def stream_zip_to_block_blob(
    *,
    dest_shard,
    dest_container: str,
    dest_blob_path: str,
    members,
    member_source_shard,
    manifest_bytes: bytes,
    block_size: int,
) -> None:
    """Build a ZIP64 archive chunk-by-chunk and stage blocks on the destination
    blob as bytes flow out. Memory stays at ~block_size regardless of archive
    size.

    `members` is a list of (arcname, container, blob_path) tuples. Each
    member's bytes are fetched via `member_source_shard.download_blob_stream`
    (chunked), so no single member materializes in RAM.

    Cross-shard members should be copied to the destination shard first
    (see MailExportOrchestrator._assemble_zip_multi_shard) — this function
    pulls ALL members from `member_source_shard`.
    """
    import zipstream  # package: zipstream-ng

    zs = zipstream.ZipStream(compress_type=zipstream.ZIP_STORED)

    block_ids: list = []
    staged = bytearray()
    block_index = 0

    async def _flush_to_block(final: bool = False):
        nonlocal block_index
        while len(staged) >= block_size or (final and staged):
            take = min(block_size, len(staged))
            chunk = bytes(staged[:take])
            del staged[:take]
            bid = f"blk-{block_index:05d}"
            await dest_shard.stage_block(dest_container, dest_blob_path, bid, chunk)
            block_ids.append(bid)
            block_index += 1

    for arcname, container, path in members:
        # Pre-buffer this member's chunks (bounded by download_blob_stream's
        # chunk_size = block_size each). We hold at most one member's worth
        # of chunks in RAM at a time — the list is deleted before the next
        # member is loaded.
        chunks: list = []
        async for chunk in member_source_shard.download_blob_stream(
            container, path, chunk_size=block_size
        ):
            chunks.append(chunk)
        zs.add(iter(chunks), arcname=arcname)
        del chunks  # release list reference; zs holds the iterator

        # Drain only this member's bytes from zs using file(). This keeps
        # memory bounded to one member's data at a time rather than
        # accumulating all members before draining.
        for zchunk in zs.file():
            staged.extend(zchunk)
            await _flush_to_block()

    zs.add(iter([manifest_bytes]), arcname="_MANIFEST.json")

    # Finalize: emits remaining member(s) + central directory + EOCD.
    for final_chunk in zs.finalize():
        staged.extend(final_chunk)
        await _flush_to_block()

    await _flush_to_block(final=True)
    await dest_shard.commit_block_list_manual(
        dest_container, dest_blob_path, block_ids,
        metadata={"streamed": "true"},
    )


class MailExportOrchestrator:
    """Entry point for the mail-export pipeline. Partitions items by folder,
    spawns one FolderExportTask per folder under asyncio.Semaphore(parallelism),
    assembles the final ZIP + manifest.

    When shard_manager is provided (Task 27+), items are grouped by
    (folder, shard_index) and each FolderExportTask runs on the shard that
    holds its source data. The final ZIP lands on the shard with the biggest
    byte contribution. The legacy shard= kwarg is preserved for single-shard
    callers.

    Final ZIP is built in memory for this task; Task 28 upgrades to streaming
    into stage_block calls via zipstream-ng."""

    def __init__(
        self,
        *,
        job_id: str,
        snapshot_ids: List[str],
        items: List[Any],
        shard=None,                     # legacy — single shard
        shard_manager=None,             # NEW — preferred, per-item resolution
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
        checkpoint: Optional[dict] = None,
        persist_checkpoint=None,  # async callable(dict) — e.g. update Job.result in DB
    ):
        if shard_manager is None and shard is None:
            raise ValueError("pass either shard_manager (preferred) or shard (legacy)")
        self.shard_manager = shard_manager
        self.shard = shard
        self.job_id = job_id
        self.snapshot_ids = snapshot_ids
        self.items = items
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
        self.checkpoint = dict(checkpoint or {"completed_folders": [], "produced_blobs": {}})
        self.persist_checkpoint = persist_checkpoint

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
            if self.shard_manager:
                groups = self._partition_by_folder_and_shard()
            else:
                groups = {(fn, 0): items for fn, items in self._partition_by_folder().items()}

            completed = set(self.checkpoint.get("completed_folders", []))
            sem = asyncio.Semaphore(self.parallelism)

            async def _run_and_persist(folder_name, shard_index, folder_items):
                shard = self.shard_manager.get_shard_by_index(shard_index) if self.shard_manager else self.shard
                async with sem:
                    task = FolderExportTask(
                        folder_name=folder_name,
                        items=folder_items,
                        shard=shard,
                        source_container=self.source_container,
                        dest_container=self.dest_container,
                        dest_blob_prefix=(
                            f"{self.job_id}/shard-{shard_index}/{folder_name}"
                            if self.shard_manager else f"{self.job_id}/{folder_name}"
                        ),
                        split_bytes=self.split_bytes,
                        block_size=self.block_size,
                        fetch_batch_size=self.fetch_batch_size,
                        queue_maxsize=self.queue_maxsize,
                        format=self.format,
                        include_attachments=self.include_attachments,
                        manifest=self.manifest,
                    )
                    result = await task.run()

                cp_folders = list(self.checkpoint.get("completed_folders", []))
                cp_blobs = dict(self.checkpoint.get("produced_blobs", {}))
                if folder_name not in cp_folders:
                    cp_folders.append(folder_name)
                cp_blobs[folder_name] = list(result.produced_blobs)
                self.checkpoint["completed_folders"] = cp_folders
                self.checkpoint["produced_blobs"] = cp_blobs
                if self.persist_checkpoint:
                    try:
                        await self.persist_checkpoint(dict(self.checkpoint))
                    except Exception as exc:
                        print(f"[MailExportOrchestrator] checkpoint persist failed (non-fatal): {exc}")
                return shard_index, shard, result

            remaining_groups = {
                k: v for k, v in groups.items() if k[0] not in completed
            }
            per_group = await asyncio.gather(
                *(_run_and_persist(fn, si, items)
                  for (fn, si), items in remaining_groups.items())
            )

            # For legacy single-shard path: also recover prior checkpoint blobs
            # into folder_results so ZIP assembly includes them.
            if not self.shard_manager:
                done_folder_names = {fr.folder_name for (_, _, fr) in per_group}
                extra = []
                for prior_folder, prior_blobs in self.checkpoint.get("produced_blobs", {}).items():
                    if prior_folder not in done_folder_names:
                        rec = FolderExportResult(folder_name=prior_folder)
                        rec.produced_blobs = list(prior_blobs)
                        extra.append((0, self.shard, rec))
                per_group = list(per_group) + extra

            # Pick destination shard: the one with the largest contribution.
            bytes_per_shard: dict = {}
            for shard_index, shard, fr in per_group:
                total = 0
                for p in fr.produced_blobs:
                    try:
                        props = await shard.get_blob_properties(self.dest_container, p)
                        total += (props or {}).get("size", 0) or 0
                    except Exception:
                        pass
                bytes_per_shard[shard_index] = bytes_per_shard.get(shard_index, 0) + total

            dest_shard_index = max(bytes_per_shard, key=bytes_per_shard.get) if bytes_per_shard else 0
            dest_shard = (
                self.shard_manager.get_shard_by_index(dest_shard_index)
                if self.shard_manager else self.shard
            )

            zip_blob_path = f"{self.job_id}/export_{int(time.time())}.zip"
            await self._assemble_zip_multi_shard(
                dest_shard=dest_shard,
                zip_blob_path=zip_blob_path,
                per_group=per_group,
                dest_shard_index=dest_shard_index,
            )

            status = "COMPLETED"
            if breached.is_set():
                status = "COMPLETED_WITH_ERRORS"
                print("[MailExportOrchestrator] M6 breach — export finalized under pressure")

            return {
                "blob_path": zip_blob_path,
                "dest_shard_index": dest_shard_index,
                "exported_count": self.manifest.exported_count if self.manifest else sum(fr.exported_count for (_, _, fr) in per_group),
                "failed_count": self.manifest.failed_count if self.manifest else sum(len(fr.failed_items) for (_, _, fr) in per_group),
                "folder_count": len(groups),
                "status": status,
                "manifest": json.loads(self.manifest.to_json().decode("utf-8")) if self.manifest else None,
                "checkpoint": dict(self.checkpoint),
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

    def _partition_by_folder_and_shard(self) -> dict:
        """Group items by (folder_name, shard_index). Used when shard_manager
        is set so each FolderExportTask runs on the shard that owns its data."""
        from collections import defaultdict

        out: dict = defaultdict(list)
        for it in self.items:
            folder = (getattr(it, "folder_path", None) or "Inbox").strip("/")
            shard_index = getattr(it, "shard_index", 0) if self.shard_manager else 0
            out[(folder, shard_index)].append(it)
        return dict(out)

    async def _assemble_zip_multi_shard(self, *, dest_shard, zip_blob_path, per_group, dest_shard_index):
        """Stream the final ZIP into block staging on dest_shard.
        Cross-shard members are copied to dest_shard first via put_block_from_url."""
        same_shard_members = []
        cross_shard_tmp_copies = []

        for shard_index, shard, fr in per_group:
            for blob_path in fr.produced_blobs:
                arc = blob_path.split(f"{self.job_id}/shard-{shard_index}/", 1)[-1]
                if "/shard-" not in blob_path:
                    arc = blob_path.split(f"{self.job_id}/", 1)[-1]
                if shard_index == dest_shard_index:
                    same_shard_members.append((arc, self.dest_container, blob_path))
                else:
                    # Server-side copy into dest shard's exports container.
                    try:
                        sas_src = await shard.get_blob_sas_url(self.dest_container, blob_path)
                    except Exception:
                        sas_src = await shard.get_blob_url(self.dest_container, blob_path)
                    tmp_path = f"{self.job_id}/_crosshard/shard-{shard_index}/{arc}"
                    try:
                        await dest_shard.put_block_from_url(
                            self.dest_container, tmp_path, "b01", sas_src
                        )
                        await dest_shard.commit_block_list_manual(
                            self.dest_container, tmp_path, ["b01"]
                        )
                        cross_shard_tmp_copies.append((arc, self.dest_container, tmp_path))
                    except Exception as exc:
                        print(f"[MailExportOrchestrator] cross-shard copy failed for {blob_path}: {exc}")

        all_members = same_shard_members + cross_shard_tmp_copies
        manifest_bytes = self.manifest.to_json() if self.manifest else b"{}"

        await stream_zip_to_block_blob(
            dest_shard=dest_shard,
            dest_container=self.dest_container,
            dest_blob_path=zip_blob_path,
            members=all_members,
            member_source_shard=dest_shard,
            manifest_bytes=manifest_bytes,
            block_size=self.block_size,
        )

        # Cleanup intermediate per-folder MBOX blobs + cross-shard temp copies.
        # Failures here are non-fatal — the final ZIP already committed.
        for _, container, path in same_shard_members + cross_shard_tmp_copies:
            try:
                await dest_shard.delete_blob(container, path)
            except Exception:
                pass

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
