"""Per-job backup checkpoint shared by OneDrive + any future streaming-backup workload.

Lives on `Job.result.backup_checkpoint`. Commit cadence is "N files OR M bytes
since last commit, whichever first". Resume filters file metadata by the
`files_done_ext_ids` set so redeliveries don't re-download.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class BackupCheckpoint:
    resource_id: str
    drive_id: str
    delta_token_at_resume: Optional[str] = None
    files_done_ext_ids: set = field(default_factory=set)
    files_done_count: int = 0
    bytes_done: int = 0
    last_commit_at: Optional[str] = None

    _files_since_commit: int = field(default=0, repr=False)
    _bytes_since_commit: int = field(default=0, repr=False)

    @classmethod
    def empty(cls, *, resource_id: str, drive_id: str) -> "BackupCheckpoint":
        return cls(resource_id=resource_id, drive_id=drive_id)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "BackupCheckpoint":
        return cls(
            resource_id=payload.get("resource_id", ""),
            drive_id=payload.get("drive_id", ""),
            delta_token_at_resume=payload.get("delta_token_at_resume"),
            files_done_ext_ids=set(payload.get("files_done_ext_ids", []) or []),
            files_done_count=int(payload.get("files_done_count", 0)),
            bytes_done=int(payload.get("bytes_done", 0)),
            last_commit_at=payload.get("last_commit_at"),
        )

    def is_done(self, external_id: str) -> bool:
        return external_id in self.files_done_ext_ids

    def record_file_done(self, *, external_id: str, size: int) -> None:
        if external_id in self.files_done_ext_ids:
            return
        self.files_done_ext_ids.add(external_id)
        self.files_done_count += 1
        self.bytes_done += size
        self._files_since_commit += 1
        self._bytes_since_commit += size

    def should_commit(self, *, every_files: int, every_bytes: int) -> bool:
        return self._files_since_commit >= every_files or self._bytes_since_commit >= every_bytes

    def mark_committed(self) -> None:
        self.last_commit_at = datetime.now(timezone.utc).isoformat()
        self._files_since_commit = 0
        self._bytes_since_commit = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource_id": self.resource_id,
            "drive_id": self.drive_id,
            "delta_token_at_resume": self.delta_token_at_resume,
            "files_done_ext_ids": sorted(self.files_done_ext_ids),
            "files_done_count": self.files_done_count,
            "bytes_done": self.bytes_done,
            "last_commit_at": self.last_commit_at,
        }
