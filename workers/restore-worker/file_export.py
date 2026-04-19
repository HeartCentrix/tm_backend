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


# ── Orchestrator placeholder; fleshed out in Task 5. ──
class FileExportOrchestrator:
    pass
