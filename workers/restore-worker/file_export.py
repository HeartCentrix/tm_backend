"""OneDrive / SharePoint file export pipeline.

Mirrors the mail-v2 pattern — thin orchestrator + per-folder task — but with
file-specific behaviour: blob-path ZIP members (no EML build), folder-tree
arcname mirroring, single-file raw-stream shortcut, Windows-safe path
sanitization.

See docs/superpowers/specs/2026-04-19-onedrive-export-and-backup-uncap-design.md.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, List, Optional

_HERE = os.path.dirname(__file__)
_SHARED = os.path.abspath(os.path.join(_HERE, "..", "..", "shared"))
if os.path.isdir(_SHARED) and _SHARED not in sys.path:
    sys.path.insert(0, os.path.dirname(_SHARED))

from shared.file_path_sanitize import sanitize_arcname, resolve_arcname_collision  # noqa: E402


@dataclass
class FileFolderExportResult:
    folder_name: str
    produced_members: List[tuple] = field(default_factory=list)
    exported_count: int = 0
    failed_items: List[dict] = field(default_factory=list)

    @property
    def failed_count(self) -> int:
        return len(self.failed_items)


class FileFolderExportTask:
    """Per-folder task. Walks items, resolves blob_path, emits member tuples for
    the ZIP stream. Honors missing-blob policy and size/path caps."""

    def __init__(
        self,
        *,
        folder_name: str,
        items: List[Any],
        shard,
        source_container: str,
        dest_container: str,
        missing_policy: str,
        max_file_bytes: int,
        path_max_len: int,
        sanitize_chars: str,
        manifest,
    ):
        self.folder_name = folder_name
        self.items = items
        self.shard = shard
        self.source_container = source_container
        self.dest_container = dest_container
        self.missing_policy = missing_policy
        self.max_file_bytes = max_file_bytes
        self.path_max_len = path_max_len
        self.sanitize_chars = sanitize_chars
        self.manifest = manifest

    async def _resolve_blob_path(self, item) -> Optional[str]:
        bp = getattr(item, "blob_path", None)
        if bp:
            return bp
        try:
            snap_id = str(getattr(item, "snapshot_id", "") or "")
            ext_id = getattr(item, "external_id", "") or ""
            async for name in self.shard.list_blobs(self.source_container):
                if snap_id and snap_id in name and name.endswith(f"/{ext_id}"):
                    return name
        except Exception:
            pass
        return None

    async def run(self) -> FileFolderExportResult:
        print(
            f"[FileFolderExportTask/{self.folder_name}] START items={len(self.items)} policy={self.missing_policy}",
            flush=True,
        )
        result = FileFolderExportResult(folder_name=self.folder_name)
        used_arcnames: set = set()

        for item in self.items:
            ext_id = getattr(item, "external_id", "")
            name = getattr(item, "name", "") or ext_id
            content_size = int(getattr(item, "content_size", 0) or 0)

            if content_size > self.max_file_bytes:
                print(
                    f"[FileFolderExportTask/{self.folder_name}] skip too_large ext_id={ext_id} size={content_size}",
                    flush=True,
                )
                failure = {
                    "id": ext_id, "name": name, "folder": self.folder_name,
                    "status": "skipped", "reason": "too_large",
                    "size_bytes": content_size,
                }
                result.failed_items.append(failure)
                if self.manifest:
                    self.manifest.record_failure(
                        item_id=ext_id, name=name, folder=self.folder_name,
                        error="too_large", error_class="SizeCap",
                    )
                continue

            blob_path = await self._resolve_blob_path(item)
            if not blob_path:
                reason = "not_yet_backed_up"
                print(
                    f"[FileFolderExportTask/{self.folder_name}] blob_path=NULL ext_id={ext_id} policy={self.missing_policy}",
                    flush=True,
                )
                if self.missing_policy == "fail":
                    raise RuntimeError(f"not_yet_backed_up: {ext_id}")
                result.failed_items.append({
                    "id": ext_id, "name": name, "folder": self.folder_name,
                    "status": "skipped", "reason": reason,
                })
                if self.manifest:
                    self.manifest.record_failure(
                        item_id=ext_id, name=name, folder=self.folder_name,
                        error=reason, error_class="MissingBlob",
                    )
                continue

            # Use the item's own folder_path for the arcname — reflects the real
            # location in OneDrive. folder_name is the task's grouping label
            # (same as folder_path for items the orchestrator grouped by path,
            # but we honour item-level path for safety).
            item_folder = (getattr(item, "folder_path", None) or self.folder_name or "").strip("/")
            raw_arcname = f"{item_folder}/{name}" if item_folder else name
            safe = sanitize_arcname(
                raw_arcname, max_len=self.path_max_len, replace_chars=self.sanitize_chars,
            )
            unique = resolve_arcname_collision(safe, external_id=ext_id, used=used_arcnames)

            result.produced_members.append((unique, self.source_container, blob_path))
            result.exported_count += 1
            if self.manifest:
                self.manifest.record_success(
                    item_id=ext_id, name=name, folder=self.folder_name,
                    size_bytes=content_size,
                )

        print(
            f"[FileFolderExportTask/{self.folder_name}] DONE produced={len(result.produced_members)} failed={result.failed_count}",
            flush=True,
        )
        return result


import time as _time

try:
    from shared.export_manifest import ExportManifestBuilder  # type: ignore
except ImportError:  # pragma: no cover
    ExportManifestBuilder = None  # type: ignore

from mail_export import stream_zip_to_block_blob  # reused verbatim from mail-v2.


class FileExportOrchestrator:
    """Thin orchestrator for OneDrive / SharePoint file exports.

    - Single file + ORIGINAL → no ZIP; writes a raw-single Job.result so the
      job-service download endpoint streams the raw blob bytes directly.
    - Multi-file or ZIP format → partition by (folder, shard), per-folder
      tasks emit member tuples, stream_zip_to_block_blob assembles final ZIP.
    """

    def __init__(
        self,
        *,
        job_id: str,
        snapshot_ids: List[str],
        items: List[Any],
        shard_manager,
        source_container: str,
        dest_container: str,
        parallelism: int,
        block_size: int,
        fetch_batch_size: int,
        export_format: str,
        missing_policy: str,
        max_file_bytes: int,
        path_max_len: int,
        sanitize_chars: str,
        manifest: Optional["ExportManifestBuilder"] = None,
        checkpoint: Optional[dict] = None,
        persist_checkpoint=None,
    ):
        self.job_id = job_id
        self.snapshot_ids = snapshot_ids
        self.items = items
        self.shard_manager = shard_manager
        self.source_container = source_container
        self.dest_container = dest_container
        self.parallelism = max(1, parallelism)
        self.block_size = block_size
        self.fetch_batch_size = fetch_batch_size
        self.export_format = export_format.upper()
        self.missing_policy = missing_policy
        self.max_file_bytes = max_file_bytes
        self.path_max_len = path_max_len
        self.sanitize_chars = sanitize_chars
        self.manifest = manifest or (
            ExportManifestBuilder(job_id=job_id, snapshot_ids=snapshot_ids)
            if ExportManifestBuilder else None
        )
        self.checkpoint = dict(checkpoint or {"completed_folders": [], "produced_blobs": {}})
        self.persist_checkpoint = persist_checkpoint

    async def run(self) -> dict:
        print(
            f"[FileExportOrchestrator] ENTER job={self.job_id} items={len(self.items)} fmt={self.export_format}",
            flush=True,
        )

        # ── Single-file ORIGINAL raw-stream shortcut ──
        if (
            self.export_format == "ORIGINAL"
            and len(self.items) == 1
            and getattr(self.items[0], "blob_path", None)
        ):
            return await self._single_raw_stream_result(self.items[0])

        # ── Multi-file ZIP path ──
        return await self._zip_assembly_run()

    async def _single_raw_stream_result(self, item) -> dict:
        print(
            f"[FileExportOrchestrator] raw_single ext_id={getattr(item, 'external_id', '')} blob_path={item.blob_path}",
            flush=True,
        )
        size = int(getattr(item, "content_size", 0) or 0)
        name = getattr(item, "name", "") or getattr(item, "external_id", "")
        content_type = "application/octet-stream"
        extra = getattr(item, "extra_data", None) or {}
        raw = extra.get("raw") or {} if isinstance(extra, dict) else {}
        file_meta = raw.get("file") if isinstance(raw, dict) else None
        if isinstance(file_meta, dict):
            content_type = file_meta.get("mimeType") or content_type

        if self.manifest:
            self.manifest.record_success(
                item_id=getattr(item, "external_id", ""),
                name=name,
                folder=getattr(item, "folder_path", "") or "",
                size_bytes=size,
            )

        return {
            "output_mode": "raw_single",
            "source_container": self.source_container,
            "source_blob_path": item.blob_path,
            "original_name": name,
            "content_type": content_type,
            "size_bytes": size,
            "exported_count": 1,
            "failed_count": 0,
            "manifest": json.loads(self.manifest.to_json().decode("utf-8")) if self.manifest else None,
        }

    def _partition_by_folder_and_shard(self) -> dict:
        from collections import defaultdict as _dd
        groups = _dd(list)
        for it in self.items:
            folder = (getattr(it, "folder_path", None) or "").strip("/")
            shard_index = getattr(it, "shard_index", 0) if self.shard_manager else 0
            groups[(folder, shard_index)].append(it)
        return dict(groups)

    async def _zip_assembly_run(self) -> dict:
        groups = self._partition_by_folder_and_shard()
        completed = set(self.checkpoint.get("completed_folders", []))
        sem = asyncio.Semaphore(self.parallelism)

        async def _run_one(folder_name, shard_index, items):
            shard = self.shard_manager.get_shard_by_index(shard_index)
            async with sem:
                task = FileFolderExportTask(
                    folder_name=folder_name,
                    items=items,
                    shard=shard,
                    source_container=self.source_container,
                    dest_container=self.dest_container,
                    missing_policy=self.missing_policy,
                    max_file_bytes=self.max_file_bytes,
                    path_max_len=self.path_max_len,
                    sanitize_chars=self.sanitize_chars,
                    manifest=self.manifest,
                )
                tr = await task.run()
            cp_folders = list(self.checkpoint.get("completed_folders", []))
            if folder_name not in cp_folders:
                cp_folders.append(folder_name)
            self.checkpoint["completed_folders"] = cp_folders
            return shard_index, shard, tr

        remaining = {k: v for k, v in groups.items() if k[0] not in completed}
        per_group = await asyncio.gather(*(_run_one(fn, si, items) for (fn, si), items in remaining.items()))

        dest_shard_index = 0
        dest_shard = self.shard_manager.get_default_shard()
        if per_group:
            dest_shard_index = per_group[0][0]
            dest_shard = per_group[0][1]

        all_members = []
        for _si, _shard, folder_result in per_group:
            all_members.extend(folder_result.produced_members)

        zip_blob_path = f"{self.job_id}/export_{int(_time.time())}.zip"
        manifest_bytes = self.manifest.to_json() if self.manifest else b"{}"

        print(
            f"[FileExportOrchestrator] assembling ZIP members={len(all_members)} dest_shard={dest_shard_index} path={zip_blob_path}",
            flush=True,
        )
        await stream_zip_to_block_blob(
            dest_shard=dest_shard,
            dest_container=self.dest_container,
            dest_blob_path=zip_blob_path,
            members=all_members,
            member_source_shard=dest_shard,
            manifest_bytes=manifest_bytes,
            block_size=self.block_size,
        )

        exported = sum(fr.exported_count for _, _, fr in per_group)
        failed = sum(fr.failed_count for _, _, fr in per_group)

        return {
            "output_mode": "zip",
            "blob_path": zip_blob_path,
            "dest_shard_index": dest_shard_index,
            "container": self.dest_container,
            "exported_count": exported,
            "failed_count": failed,
            "folder_count": len(groups),
            "manifest": json.loads(self.manifest.to_json().decode("utf-8")) if self.manifest else None,
            "checkpoint": dict(self.checkpoint),
        }
