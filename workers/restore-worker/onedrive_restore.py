"""OneDriveRestoreEngine — stream-restore OneDrive files at scale.

Handles four user-facing modes via two inputs:
  * Mode.OVERWRITE          → conflictBehavior=replace
  * Mode.SEPARATE_FOLDER    → conflictBehavior=rename, prefix path

Path derivation preserves the original folder tree under the chosen
prefix (or at root if no prefix). Upload dispatch picks a single PUT
for files < 4 MB and resumable uploadSession chunks for >= 4 MB so
multi-GB files resume on transient failure instead of restarting.
"""
from __future__ import annotations

import asyncio
import enum
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from shared.azure_storage import azure_storage_manager
from shared.config import settings
from shared.graph_client import GraphClient
from shared.models import Resource, SnapshotItem


SMALL_FILE_MAX_BYTES = 4 * 1024 * 1024  # Graph simple-upload cap


class Mode(str, enum.Enum):
    OVERWRITE = "OVERWRITE"
    SEPARATE_FOLDER = "SEPARATE_FOLDER"


@dataclass
class ItemOutcome:
    item_id: str
    external_id: str
    name: str
    outcome: str           # created | overwritten | renamed | skipped | failed
    size_bytes: int = 0
    reason: Optional[str] = None


class OneDriveRestoreEngine:
    """Per-(job, target drive) orchestrator."""

    def __init__(
        self,
        graph_client: GraphClient,
        source_resource: Resource,
        target_drive_user_id: str,
        tenant_id: str,
        mode: Mode,
        *,
        separate_folder_root: Optional[str] = None,
        worker_id: str = "",
        is_cross_user: bool = False,
    ):
        self.graph = graph_client
        self.source = source_resource
        self.target_user_id = target_drive_user_id
        self.tenant_id = tenant_id
        self.mode = mode
        self.separate_folder_root = (separate_folder_root or "").strip("/") or None
        self.worker_id = worker_id
        self.is_cross_user = is_cross_user

    # ---------- path derivation ----------

    @staticmethod
    def resolve_drive_path(
        item: Any,
        mode: Mode,
        separate_folder_root: Optional[str],
    ) -> Tuple[str, str]:
        """Return (drive_relative_path, conflict_behavior) for this item.

        folder_path captured by the backup can be either "/drive/root:/A/B"
        or "/drives/{driveId}/root:/A/B". Either form strips to the user-
        visible trail. None / "" = file at the drive root.
        """
        name = getattr(item, "name", "") or getattr(item, "external_id", "item")
        raw = (getattr(item, "folder_path", None) or "").strip()
        trail = raw
        if ":" in trail:
            trail = trail.split(":", 1)[1]
        trail = trail.strip("/")
        root = (separate_folder_root or "").strip("/")
        parts = [p for p in (root, trail, name) if p]
        path = "/".join(parts)
        conflict = "replace" if mode == Mode.OVERWRITE else "rename"
        return path, conflict

    # ---------- single-file upload ----------

    async def _read_blob_bytes(self, item: SnapshotItem) -> Optional[bytes]:
        """Fetch captured bytes from Azure Blob. Returns None on missing
        blob_path or any read error — callers treat that as 'skipped'.

        Uses ``AzureStorageShard.download_blob`` which returns bytes
        directly and yields None on ResourceNotFoundError; the previous
        ``shard.get_blob_client(...)`` call is not part of the shard's
        public surface.
        """
        blob_path = getattr(item, "blob_path", None)
        if not blob_path:
            return None
        try:
            shard = azure_storage_manager.get_shard_for_resource(
                str(getattr(self.source, "id", "")), str(self.tenant_id),
            )
            container = azure_storage_manager.get_container_name(
                str(self.tenant_id), "files",
            )
            return await shard.download_blob(container, blob_path)
        except Exception as exc:
            print(f"[{self.worker_id}] [ONEDRIVE-RESTORE] blob read "
                  f"{blob_path} failed: {type(exc).__name__}: {exc}", flush=True)
            return None

    async def _iter_blob_stream(
        self, item: SnapshotItem, chunk_size: int = 8 * 1024 * 1024,
    ):
        """Async-iterate blob bytes from the active storage backend
        (SeaweedFS or Azure Blob via the facade) in bounded chunks.
        Used by the streaming large-file restore path so a 100 GB
        file never materialises in worker RAM.
        """
        blob_path = getattr(item, "blob_path", None)
        if not blob_path:
            return
        shard = azure_storage_manager.get_shard_for_resource(
            str(getattr(self.source, "id", "")), str(self.tenant_id),
        )
        container = azure_storage_manager.get_container_name(
            str(self.tenant_id), "files",
        )
        async for chunk in shard.download_blob_stream(
            container, blob_path, chunk_size=chunk_size,
        ):
            if chunk:
                yield chunk

    async def upload_one(self, item: SnapshotItem) -> ItemOutcome:
        """Upload one SnapshotItem to the target drive.

        Three size regimes:
          * < 4 MiB — Graph simple PUT, fully buffered (Graph requires)
          * 4 MiB ≤ size ≤ STREAMING_THRESHOLD — uploadSession w/
            buffered bytes (backwards-compatible)
          * > STREAMING_THRESHOLD — uploadSession fed directly by
            the backend's download_stream, so a 100 GB restore
            never materialises 100 GB in worker RAM.
        """
        name = getattr(item, "name", None) or getattr(item, "external_id", "item")
        size = int(getattr(item, "content_size", 0) or 0)

        drive_path, conflict = self.resolve_drive_path(
            item, self.mode, self.separate_folder_root,
        )

        streaming_threshold = getattr(
            settings, "ONEDRIVE_RESTORE_STREAMING_THRESHOLD_BYTES",
            64 * 1024 * 1024,
        )
        streaming_eligible = size > streaming_threshold

        body: Optional[bytes] = None
        if not streaming_eligible:
            body = await self._read_blob_bytes(item)
            if body is None:
                return ItemOutcome(
                    item_id=str(getattr(item, "id", "")),
                    external_id=getattr(item, "external_id", ""),
                    name=name, outcome="skipped", reason="blob_missing",
                )

        try:
            if streaming_eligible:
                created = await self.graph.upload_large_file_stream_to_drive(
                    drive_id=self.target_user_id,
                    drive_path=drive_path,
                    byte_iter=self._iter_blob_stream(
                        item,
                        chunk_size=settings.ONEDRIVE_RESTORE_CHUNK_BYTES,
                    ),
                    total_size=size,
                    chunk_size=settings.ONEDRIVE_RESTORE_CHUNK_BYTES,
                    conflict_behavior=conflict,
                )
            elif size < SMALL_FILE_MAX_BYTES:
                created = await self.graph.upload_small_file_to_drive(
                    drive_id=self.target_user_id,
                    drive_path=drive_path,
                    body=body,
                    conflict_behavior=conflict,
                )
            else:
                created = await self.graph.upload_large_file_to_drive(
                    drive_id=self.target_user_id,
                    drive_path=drive_path,
                    body=body,
                    total_size=len(body),
                    chunk_size=settings.ONEDRIVE_RESTORE_CHUNK_BYTES,
                    conflict_behavior=conflict,
                )
        except Exception as exc:
            return ItemOutcome(
                item_id=str(getattr(item, "id", "")),
                external_id=getattr(item, "external_id", ""),
                name=name, outcome="failed",
                reason=f"{type(exc).__name__}: {exc}",
            )

        # Restore captured creation / modification dates. Graph stamps
        # server-now on upload; without this PATCH the file looks
        # "modified today" in Explorer.
        raw = (getattr(item, "extra_data", None) or {}).get("raw") or {}
        fsi = raw.get("fileSystemInfo") or {}
        new_item_id = (created or {}).get("id")
        if new_item_id and (fsi.get("createdDateTime") or fsi.get("lastModifiedDateTime")):
            try:
                await self.graph.patch_drive_item_file_system_info(
                    drive_id=self.target_user_id,
                    drive_item_id=new_item_id,
                    created_iso=fsi.get("createdDateTime"),
                    modified_iso=fsi.get("lastModifiedDateTime"),
                )
            except Exception as exc:
                print(f"[{self.worker_id}] [ONEDRIVE-RESTORE] fileSystemInfo "
                      f"PATCH failed {name}: {type(exc).__name__}: {exc}", flush=True)

        outcome_label = "overwritten" if conflict == "replace" else (
            "renamed" if (created or {}).get("name") != name else "created"
        )
        bytes_moved = len(body) if body is not None else size
        return ItemOutcome(
            item_id=str(getattr(item, "id", "")),
            external_id=getattr(item, "external_id", ""),
            name=name, outcome=outcome_label, size_bytes=bytes_moved,
        )

    # ---------- orchestrator ----------

    async def run(self, items: List[SnapshotItem]) -> Dict[str, Any]:
        """Bounded producer/consumer over the item list.

        Queue size caps memory pressure when feeding a 1M-item restore
        through; consumer pool caps Graph parallelism per worker; a
        separate per-target-user semaphore caps parallelism against any
        single drive so 5k simultaneous restores don't 429 one user's
        drive with hundreds of concurrent writes.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=2048)
        per_user_sem = asyncio.Semaphore(settings.ONEDRIVE_RESTORE_PER_TARGET_USER_CAP)
        outcomes: List[ItemOutcome] = []

        async def producer():
            for it in items:
                await queue.put(it)

        async def consumer():
            while True:
                try:
                    it = await queue.get()
                except Exception:
                    return
                try:
                    if it is None:
                        return
                    async with per_user_sem:
                        outcome = await self.upload_one(it)
                    outcomes.append(outcome)
                finally:
                    queue.task_done()

        num_consumers = max(1, min(settings.ONEDRIVE_RESTORE_CONCURRENCY, 64))
        prod = asyncio.create_task(producer())
        cons = [asyncio.create_task(consumer()) for _ in range(num_consumers)]

        await prod
        await queue.join()
        for _ in range(num_consumers):
            await queue.put(None)
        await asyncio.gather(*cons, return_exceptions=True)

        summary: Dict[str, Any] = {
            "created": 0, "overwritten": 0, "renamed": 0,
            "skipped": 0, "failed": 0, "bytes": 0,
        }
        for o in outcomes:
            summary[o.outcome] = summary.get(o.outcome, 0) + 1
            summary["bytes"] += int(o.size_bytes or 0)
        summary["items"] = [o.__dict__ for o in outcomes]
        print(
            f"[{self.worker_id}] [ONEDRIVE-RESTORE] summary: "
            f"created={summary['created']} overwritten={summary['overwritten']} "
            f"renamed={summary['renamed']} skipped={summary['skipped']} "
            f"failed={summary['failed']} bytes={summary['bytes']}",
            flush=True,
        )
        return summary
