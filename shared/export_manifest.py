"""Accumulates per-item success/failure rows during a mail export and emits
`_MANIFEST.json` at the end of the ZIP. Required for eDiscovery audit trail."""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import List, Optional


class ExportManifestBuilder:
    def __init__(self, job_id: str, snapshot_ids: List[str]):
        self.job_id = job_id
        self.snapshot_ids = snapshot_ids
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._items: list = []
        self._lock = threading.Lock()

    def record_success(self, *, item_id: str, name: str, folder: str, size_bytes: int) -> None:
        with self._lock:
            self._items.append({
                "id": item_id,
                "name": name,
                "folder": folder,
                "status": "success",
                "size_bytes": size_bytes,
            })

    def record_failure(
        self,
        *,
        item_id: str,
        name: str,
        folder: str,
        error: str,
        error_class: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._items.append({
                "id": item_id,
                "name": name,
                "folder": folder,
                "status": "failed",
                "error": error,
                "error_class": error_class or "Exception",
            })

    @property
    def exported_count(self) -> int:
        return sum(1 for i in self._items if i["status"] == "success")

    @property
    def failed_count(self) -> int:
        return sum(1 for i in self._items if i["status"] == "failed")

    def to_json(self) -> bytes:
        payload = {
            "job_id": self.job_id,
            "snapshot_ids": self.snapshot_ids,
            "started_at": self.started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "exported_count": self.exported_count,
            "failed_count": self.failed_count,
            "items": list(self._items),
        }
        return json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
