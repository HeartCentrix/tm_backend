"""BackendStore protocol + DTOs.

Every concrete storage implementation (Azure Blob, SeaweedFS) fulfills
this protocol. Callers program against the protocol, never the SDK directly.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class BlobInfo:
    """Returned on successful upload. `backend_id` must be persisted on the
    SnapshotItem row so restores find the right backend later."""
    backend_id: str
    container: str
    path: str
    size: int
    etag: str
    url: str
    content_md5: Optional[str]
    last_modified: datetime


@dataclass(frozen=True)
class BlobProps:
    """Returned by get_properties."""
    size: int
    content_type: Optional[str]
    last_modified: datetime
    metadata: dict
    copy_status: Optional[str]
    copy_progress: Optional[str]
    retention_until: Optional[datetime]
    legal_hold: bool


@runtime_checkable
class BackendStore(Protocol):
    """Protocol for a storage backend (Azure Blob, SeaweedFS, ...).

    Implementations are instantiated once per process, held in `StorageRouter`,
    and resolved per-item or per-active-backend.
    """

    backend_id: str      # storage_backends.id (UUID string)
    kind: str            # 'azure_blob' | 'seaweedfs'
    name: str            # human-readable ('azure-primary', 'onprem-dc1')

    async def upload(
        self, container: str, path: str, content: bytes,
        metadata: Optional[dict] = None, overwrite: bool = True,
    ) -> BlobInfo: ...

    async def upload_from_file(
        self, container: str, path: str, file_path: str, size: int,
        metadata: Optional[dict] = None, overwrite: bool = True,
    ) -> BlobInfo: ...

    async def download(self, container: str, path: str) -> Optional[bytes]: ...

    def download_stream(
        self, container: str, path: str, chunk_size: int = 4 * 1024 * 1024,
    ) -> AsyncIterator[bytes]: ...

    async def stage_block(
        self, container: str, path: str, block_id: str, data: bytes,
    ) -> None: ...

    async def commit_blocks(
        self, container: str, path: str, block_ids: list[str],
        metadata: Optional[dict] = None,
    ) -> None: ...

    async def put_block_from_url(
        self, container: str, path: str, block_id: str, source_url: str,
    ) -> None: ...

    async def server_side_copy(
        self, source_url: str, container: str, path: str, size: int,
        metadata: Optional[dict] = None,
    ) -> BlobInfo: ...

    def list_blobs(self, container: str) -> AsyncIterator[str]: ...

    def list_with_props(
        self, container: str,
    ) -> AsyncIterator[tuple[str, BlobProps]]: ...

    async def get_properties(
        self, container: str, path: str,
    ) -> Optional[BlobProps]: ...

    async def delete(self, container: str, path: str) -> None: ...

    async def presigned_url(
        self, container: str, path: str, valid_hours: int = 6,
    ) -> str: ...

    async def apply_immutability(
        self, container: str, path: str, until: datetime, mode: str = "Unlocked",
    ) -> None: ...

    async def apply_legal_hold(
        self, container: str, path: str, tag: str = "tmvault-legal-hold",
    ) -> None: ...

    async def remove_legal_hold(self, container: str, path: str) -> None: ...

    async def apply_lifecycle(
        self, container: str, hot_days: int, cool_days: int,
        archive_days: Optional[int] = None,
    ) -> None: ...

    async def ensure_container(self, container: str) -> None: ...

    async def close(self) -> None: ...

    def shard_for(
        self, tenant_id: str, resource_id: str,
    ) -> "BackendStore": ...
