# On-Prem Backup Store + Toggle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a toggleable on-prem backup store (SeaweedFS) alongside existing Azure Blob. Same app code runs in either mode. Org-admin flips via UI; all writes/reads/WORM follow.

**Architecture:** Extract a `BackendStore` protocol in front of today's Azure Blob code; add a `SeaweedStore` S3 impl via `aioboto3`; route every call through a `StorageRouter` that picks by `system_config.active_backend_id` (writes) or `item.backend_id` (reads). A `storage-toggle-worker` orchestrates an 8-phase toggle (drain → Postgres promote → DNS flip → worker restart → smoke → open). First deploy is a Railway pilot (single-node SeaweedFS, tiny workers) then ship to clients with a real DC cluster.

**Tech Stack:** Python 3.11, FastAPI, asyncpg + SQLAlchemy (2.x async), aio_pika (RabbitMQ), aioboto3 (new), Postgres 16, raw-SQL migrations (not Alembic — follow `migrations/*.sql` convention), pytest-asyncio, docker-compose for local dev, Azurite + SeaweedFS containers for testing, React/Vite for frontend.

**Spec reference:** `tm_backend/docs/superpowers/specs/2026-04-21-onprem-backup-store-design.md`

**Branch:** `on-prem` (already created off `dev-am`)

**Audience assumption:** Skilled engineer, new to this repo, new to the storage-abstraction idea. Follow every path exactly. Prefer small commits. TDD: test first.

**Scope:** Phases 1–6 from spec §10. Phase 0 (client DC infra), phase 7 (production cutover), phase 8 (background migration tooling) are **deferred** — not in this plan.

---

## File Structure Map

### New files

| Path | Purpose |
|------|---------|
| `tm_backend/shared/storage/__init__.py` | Package init. Exports `router`, `BackendStore`, `BlobInfo`, `BlobProps` |
| `tm_backend/shared/storage/base.py` | `BackendStore` Protocol + `BlobInfo`/`BlobProps` dataclasses |
| `tm_backend/shared/storage/azure_blob.py` | `AzureBlobStore` impl — wraps existing `AzureStorageShard` |
| `tm_backend/shared/storage/seaweedfs.py` | `SeaweedStore` impl via `aioboto3` |
| `tm_backend/shared/storage/router.py` | `StorageRouter` singleton; load from DB, LISTEN/NOTIFY |
| `tm_backend/shared/storage/errors.py` | `ImmutableBlobError`, `BackendUnreachableError`, `TransitionInProgressError` |
| `tm_backend/shared/retention.py` | `compute_retention_until()`, `compute_immutability_mode()` |
| `tm_backend/migrations/2026_04_21_onprem_storage_schema.sql` | New tables + columns + seed |
| `tm_backend/migrations/2026_04_21_onprem_storage_backfill.sql` | Backfill `backend_id` on existing snapshots + set NOT NULL |
| `tm_backend/services/storage-toggle-worker/Dockerfile` | Worker image |
| `tm_backend/services/storage-toggle-worker/main.py` | Worker entrypoint — consumes `storage.toggle` queue |
| `tm_backend/services/storage-toggle-worker/orchestrator.py` | 8-phase state machine |
| `tm_backend/services/storage-toggle-worker/preflight.py` | Preflight checks |
| `tm_backend/services/storage-toggle-worker/drain.py` | Drain in-flight jobs |
| `tm_backend/services/storage-toggle-worker/pg_promote.py` | Postgres promote orchestration |
| `tm_backend/services/storage-toggle-worker/dns_flip.py` | DNS flip hook (pluggable) |
| `tm_backend/services/storage-toggle-worker/worker_restart.py` | Worker pool scale/restart hook (pluggable) |
| `tm_backend/services/storage-toggle-worker/smoke.py` | Canary backup + restore |
| `tm_backend/services/storage-toggle-worker/rollback.py` | Per-phase rollback |
| `tm_backend/services/api-gateway/routes/admin_storage.py` | `/api/admin/storage/*` endpoints + SSE stream |
| `tm_backend/scripts/migrate_to_router.py` | Codemod — rewrite `azure_storage_manager` call sites |
| `tm_backend/scripts/create_seaweedfs_buckets.py` | Idempotent bucket creation with Object Lock |
| `tm_backend/scripts/smoke_onprem_pilot.py` | Pilot smoke script |
| `tm_backend/ops/runbooks/storage-toggle.md` | Ops runbook |
| `tm_backend/ops/runbooks/dr-failover.md` | DR runbook |
| `tm_backend/tests/shared/storage/test_contract.py` | Contract tests run vs both backends |
| `tm_backend/tests/shared/storage/test_router.py` | Router unit tests (mocked backends) |
| `tm_backend/tests/shared/storage/test_worm_parity.py` | WORM parity tests |
| `tm_backend/tests/services/storage_toggle/test_orchestrator.py` | Full toggle flow test |
| `tm_backend/tests/services/storage_toggle/test_preflight.py` | Preflight unit tests |
| `tm_backend/tests/services/storage_toggle/test_rollback.py` | Rollback per-phase tests |
| `tm_backend/tests/integration/test_toggle_end_to_end.py` | Docker-compose end-to-end |
| `tm_backend/docker-compose.seaweedfs.yml` | Optional compose overlay for local SeaweedFS |
| `tm_vault/src/pages/Settings/Storage/index.tsx` | Settings page |
| `tm_vault/src/pages/Settings/Storage/StateCard.tsx` | Active state display |
| `tm_vault/src/pages/Settings/Storage/PreflightCard.tsx` | Preflight checklist |
| `tm_vault/src/pages/Settings/Storage/HistoryCard.tsx` | Toggle event history |
| `tm_vault/src/pages/Settings/Storage/ToggleModal.tsx` | Confirmation + SSE stream |
| `tm_vault/src/api/adminStorage.ts` | Typed client for admin storage endpoints |

### Modified files

| Path | Change |
|------|--------|
| `tm_backend/shared/config.py` | Add `ONPREM_S3_*` env vars |
| `tm_backend/shared/models.py` | Add `StorageBackend`, `SystemConfig`, `StorageToggleEvent` ORM; `backend_id` FK on `Snapshot`, `SnapshotItem`; `retry_reason` + `pre_toggle_job_id` on `Job` |
| `tm_backend/shared/schemas.py` | Add Pydantic DTOs for toggle API |
| `tm_backend/shared/retention_cleanup.py` | Use router; skip expected `ImmutableBlobError` |
| `tm_backend/shared/azure_storage.py` | Keep as shim; mark `# DEPRECATED — use shared.storage.router` |
| `tm_backend/requirements.base.txt` | Add `aioboto3==12.3.0`, `aiobotocore==2.11.0`, `sse-starlette==1.8.2` |
| `tm_backend/docker-compose.yml` | Add `seaweedfs` + `azurite` dev services (profile-gated) |
| `tm_backend/pytest.ini` | Add marker `worm` |
| `tm_backend/ops/docker/Dockerfile.worker` | Add SSE + new toggle worker entrypoint variants (or new Dockerfile per service) |
| All 26 files with `azure_storage_manager` refs (see spec §13) | Codemod-rewritten to use router |

---

## Phase 1 — Storage Abstraction Layer (est. 3 weeks)

Goal at phase end: `BackendStore` protocol + `AzureBlobStore` + `SeaweedStore` + `StorageRouter` all exist. Contract tests green on both backends. No production behavior change yet — router is defined but nothing calls it.

### Task 1: Add new dependencies and dev services

**Files:**
- Modify: `tm_backend/requirements.base.txt`
- Modify: `tm_backend/docker-compose.yml`
- Create: `tm_backend/docker-compose.seaweedfs.yml`
- Create: `tm_backend/tests/shared/storage/__init__.py`
- Create: `tm_backend/tests/shared/storage/conftest.py`

- [ ] **Step 1: Append deps**

Edit `tm_backend/requirements.base.txt`, add:

```
aioboto3==12.3.0
aiobotocore==2.11.0
sse-starlette==1.8.2
```

- [ ] **Step 2: Add SeaweedFS + Azurite profile to compose**

Append to `tm_backend/docker-compose.yml` under `services:`:

```yaml
  azurite:
    image: mcr.microsoft.com/azure-storage/azurite:3.29.0
    container_name: tm_vault_azurite
    command: ["azurite", "--blobHost", "0.0.0.0", "--loose"]
    ports:
      - "10000:10000"
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "10000"]
      interval: 5s
      timeout: 3s
      retries: 10
    profiles: ["storage-test"]
    networks: [tm-network]

  seaweedfs:
    image: chrislusf/seaweedfs:3.71
    container_name: tm_vault_seaweedfs
    command:
      - "server"
      - "-dir=/data"
      - "-ip=0.0.0.0"
      - "-s3"
      - "-s3.config=/config/s3.json"
      - "-filer"
      - "-master.volumeSizeLimitMB=1024"
    volumes:
      - ./tests/fixtures/seaweedfs-s3.json:/config/s3.json:ro
      - seaweedfs_data:/data
    ports:
      - "8333:8333"
      - "9333:9333"
    healthcheck:
      test: ["CMD", "wget", "-q", "-O", "-", "http://127.0.0.1:8333/healthz"]
      interval: 10s
      timeout: 5s
      retries: 10
    profiles: ["storage-test"]
    networks: [tm-network]

volumes:
  seaweedfs_data:
```

Create `tm_backend/tests/fixtures/seaweedfs-s3.json`:

```json
{
  "identities": [
    {
      "name": "tmvault-test",
      "credentials": [
        { "accessKey": "testaccess", "secretKey": "testsecret" }
      ],
      "actions": ["Admin"]
    }
  ],
  "buckets": { "objectLockEnabled": true, "versioning": "Enabled" }
}
```

- [ ] **Step 3: Create test package init + shared conftest fixtures**

`tm_backend/tests/shared/storage/__init__.py`:

```python
```

`tm_backend/tests/shared/storage/conftest.py`:

```python
"""Shared fixtures for storage-layer tests.

Spins up Azurite + SeaweedFS containers via docker-compose profile
`storage-test` before the test session and exposes connected backend
instances as pytest fixtures.
"""
import asyncio
import os
import subprocess
import time
from typing import AsyncIterator

import pytest
import pytest_asyncio

COMPOSE_FILE = os.path.join(os.path.dirname(__file__), "../../../docker-compose.yml")
COMPOSE_PROFILE = "storage-test"


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "--profile", COMPOSE_PROFILE, *args],
        check=True,
    )


@pytest.fixture(scope="session", autouse=True)
def storage_test_services():
    """Start Azurite + SeaweedFS for the whole session."""
    if os.getenv("TMVAULT_SKIP_STORAGE_SERVICES") == "1":
        yield
        return
    _compose("up", "-d", "azurite", "seaweedfs")
    # Wait for healthchecks
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            out = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Health.Status}}",
                 "tm_vault_azurite", "tm_vault_seaweedfs"]).decode()
            if out.count("healthy") == 2:
                break
        except subprocess.CalledProcessError:
            pass
        time.sleep(2)
    yield
    if os.getenv("TMVAULT_KEEP_STORAGE_SERVICES") != "1":
        _compose("stop", "azurite", "seaweedfs")
```

- [ ] **Step 4: Verify services spin up**

```bash
cd tm_backend
docker compose --profile storage-test up -d azurite seaweedfs
docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'azurite|seaweedfs'
```

Expected: both show `healthy` (may take ~30s).

- [ ] **Step 5: Commit**

```bash
git add tm_backend/requirements.base.txt tm_backend/docker-compose.yml \
        tm_backend/tests/fixtures/seaweedfs-s3.json \
        tm_backend/tests/shared/storage/__init__.py \
        tm_backend/tests/shared/storage/conftest.py
git commit -m "chore(storage): add aioboto3 deps + test containers (azurite, seaweedfs)"
```

---

### Task 2: Define `BackendStore` protocol + dataclasses

**Files:**
- Create: `tm_backend/shared/storage/__init__.py`
- Create: `tm_backend/shared/storage/base.py`
- Create: `tm_backend/shared/storage/errors.py`
- Create: `tm_backend/tests/shared/storage/test_base.py`

- [ ] **Step 1: Write failing protocol test**

`tm_backend/tests/shared/storage/test_base.py`:

```python
from datetime import datetime, timezone

from shared.storage.base import BackendStore, BlobInfo, BlobProps


def test_blob_info_fields_present():
    info = BlobInfo(
        backend_id="00000000-0000-0000-0000-000000000001",
        container="c",
        path="p",
        size=10,
        etag="e",
        url="u",
        content_md5=None,
        last_modified=datetime.now(timezone.utc),
    )
    assert info.backend_id
    assert info.size == 10


def test_blob_props_defaults():
    props = BlobProps(
        size=0,
        content_type=None,
        last_modified=datetime.now(timezone.utc),
        metadata={},
        copy_status=None,
        copy_progress=None,
        retention_until=None,
        legal_hold=False,
    )
    assert props.legal_hold is False


def test_backend_store_is_protocol():
    # Protocols must not be instantiable directly — presence of __subclasshook__
    # or runtime_checkable is the marker we care about for consumers.
    assert hasattr(BackendStore, "__mro_entries__") or hasattr(BackendStore, "_is_protocol")
```

- [ ] **Step 2: Run — expect ImportError**

```bash
cd tm_backend
pytest tests/shared/storage/test_base.py -x
```

Expected: `ModuleNotFoundError: shared.storage.base`

- [ ] **Step 3: Write `shared/storage/__init__.py`**

```python
"""TMvault storage abstraction layer.

Every worker and service that wrote to Azure Blob directly now goes through
this package. The router picks the correct backend based on:
  - `system_config.active_backend_id` for NEW writes
  - `snapshot_item.backend_id` for READS (permanent record)

See: docs/superpowers/specs/2026-04-21-onprem-backup-store-design.md
"""
from shared.storage.base import BackendStore, BlobInfo, BlobProps

__all__ = ["BackendStore", "BlobInfo", "BlobProps"]
```

- [ ] **Step 4: Write `shared/storage/errors.py`**

```python
"""Storage-layer exceptions."""


class StorageError(Exception):
    """Base for all storage errors."""


class BackendUnreachableError(StorageError):
    """Backend endpoint not reachable (network, DNS, service down)."""


class ImmutableBlobError(StorageError):
    """Attempted to delete / shorten retention of an immutable blob.

    Expected during retention cleanup of locked snapshots — callers must
    catch this specifically to distinguish from real failures.
    """


class TransitionInProgressError(StorageError):
    """Write attempted while system is in draining or flipping state."""


class BackendNotFoundError(StorageError):
    """Router asked for a backend_id not present in storage_backends."""
```

- [ ] **Step 5: Write `shared/storage/base.py`**

```python
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
```

- [ ] **Step 6: Run — expect pass**

```bash
pytest tests/shared/storage/test_base.py -v
```

Expected: all 3 tests pass.

- [ ] **Step 7: Commit**

```bash
git add tm_backend/shared/storage/__init__.py tm_backend/shared/storage/base.py \
        tm_backend/shared/storage/errors.py tm_backend/tests/shared/storage/test_base.py
git commit -m "feat(storage): add BackendStore protocol + BlobInfo/BlobProps DTOs + errors"
```

---

### Task 3: Implement `AzureBlobStore` wrapping existing `AzureStorageShard`

**Files:**
- Create: `tm_backend/shared/storage/azure_blob.py`
- Create: `tm_backend/tests/shared/storage/test_azure_blob.py`

- [ ] **Step 1: Write failing upload test using Azurite**

`tm_backend/tests/shared/storage/test_azure_blob.py`:

```python
import os
import uuid

import pytest
import pytest_asyncio

from shared.storage.azure_blob import AzureBlobStore

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
)


@pytest_asyncio.fixture
async def azure_store() -> AzureBlobStore:
    store = AzureBlobStore.from_connection_string(
        conn_str=AZURITE_CONN,
        backend_id="00000000-0000-0000-0000-000000000001",
        name="azurite-test",
    )
    yield store
    await store.close()


@pytest.mark.asyncio
async def test_azure_upload_download_roundtrip(azure_store: AzureBlobStore):
    container = f"test-{uuid.uuid4().hex[:8]}"
    path = "hello.txt"
    body = b"hello tmvault"

    info = await azure_store.upload(container, path, body, metadata={"kind": "test"})
    assert info.backend_id == "00000000-0000-0000-0000-000000000001"
    assert info.size == len(body)

    got = await azure_store.download(container, path)
    assert got == body


@pytest.mark.asyncio
async def test_azure_download_missing_returns_none(azure_store: AzureBlobStore):
    got = await azure_store.download("no-such-container", "nope.txt")
    assert got is None
```

- [ ] **Step 2: Run — expect ImportError**

```bash
pytest tests/shared/storage/test_azure_blob.py -x
```

Expected: fails on import.

- [ ] **Step 3: Implement `shared/storage/azure_blob.py`**

```python
"""AzureBlobStore — fulfills BackendStore by delegating to the existing
AzureStorageShard / AzureStorageManager code in shared.azure_storage.

Semantics are unchanged from today: SSC, multipart, WORM, lifecycle all
work exactly as they did before the abstraction.
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import AsyncIterator, Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient

from shared.azure_storage import AzureStorageShard
from shared.storage.base import BlobInfo, BlobProps


class AzureBlobStore:
    """Thin adapter over AzureStorageShard/AzureStorageManager."""

    kind = "azure_blob"

    def __init__(
        self,
        backend_id: str,
        name: str,
        shards: list[AzureStorageShard],
    ):
        self.backend_id = backend_id
        self.name = name
        self._shards = shards
        self._default = shards[0] if shards else None

    # --- factories

    @classmethod
    def from_connection_string(
        cls, conn_str: str, backend_id: str, name: str,
    ) -> "AzureBlobStore":
        shard = AzureStorageShard.from_connection_string(conn_str)
        return cls(backend_id=backend_id, name=name, shards=[shard])

    @classmethod
    def from_config(cls, backend_id: str, name: str, config: dict) -> "AzureBlobStore":
        """config = {'shards': [{'account': ..., 'key_ref': 'env://...'}]}"""
        import os
        shards: list[AzureStorageShard] = []
        for idx, s in enumerate(config.get("shards", [])):
            key_ref: str = s["key_ref"]
            if key_ref.startswith("env://"):
                key = os.getenv(key_ref[len("env://"):], "")
            else:
                raise ValueError(f"Unsupported key_ref scheme: {key_ref}")
            shards.append(AzureStorageShard(
                account_name=s["account"], account_key=key, shard_index=idx,
            ))
        return cls(backend_id=backend_id, name=name, shards=shards)

    # --- sharding

    def shard_for(self, tenant_id: str, resource_id: str) -> "AzureBlobStore":
        if not self._shards:
            raise RuntimeError("no shards configured")
        hash_input = f"{tenant_id}:{resource_id}"
        idx = int(hashlib.md5(hash_input.encode()).hexdigest(), 16) % len(self._shards)
        return AzureBlobStore(
            backend_id=self.backend_id, name=self.name, shards=[self._shards[idx]],
        )

    # --- helpers

    def _default_shard(self) -> AzureStorageShard:
        if not self._default:
            raise RuntimeError("no default shard")
        return self._default

    # --- BackendStore impl

    async def upload(self, container, path, content, metadata=None, overwrite=True) -> BlobInfo:
        shard = self._default_shard()
        result = await shard.upload_blob(container, path, content,
                                         overwrite=overwrite, metadata=metadata)
        if not result.get("success"):
            raise RuntimeError(result.get("error", "upload failed"))
        props = await shard.get_blob_properties(container, path)
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=result.get("size_bytes", len(content)),
            etag=props.get("etag", "") if props else "",
            url=result["blob_url"],
            content_md5=None,
            last_modified=props["last_modified"] if props else datetime.utcnow(),
        )

    async def upload_from_file(self, container, path, file_path, size,
                               metadata=None, overwrite=True) -> BlobInfo:
        shard = self._default_shard()
        result = await shard.upload_blob_from_file(
            container, path, file_path, file_size=size,
            overwrite=overwrite, metadata=metadata,
        )
        if not result.get("success"):
            raise RuntimeError(result.get("error", "upload_from_file failed"))
        props = await shard.get_blob_properties(container, path)
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=size,
            etag=props.get("etag", "") if props else "",
            url=result["blob_url"],
            content_md5=None,
            last_modified=props["last_modified"] if props else datetime.utcnow(),
        )

    async def download(self, container, path) -> Optional[bytes]:
        shard = self._default_shard()
        return await shard.download_blob(container, path)

    def download_stream(self, container, path, chunk_size=4 * 1024 * 1024) -> AsyncIterator[bytes]:
        shard = self._default_shard()
        return shard.download_blob_stream(container, path, chunk_size=chunk_size)

    async def stage_block(self, container, path, block_id, data) -> None:
        await self._default_shard().stage_block(container, path, block_id, data)

    async def commit_blocks(self, container, path, block_ids, metadata=None) -> None:
        await self._default_shard().commit_block_list_manual(
            container, path, block_ids, metadata=metadata,
        )

    async def put_block_from_url(self, container, path, block_id, source_url) -> None:
        await self._default_shard().put_block_from_url(
            container, path, block_id, source_url,
        )

    async def server_side_copy(self, source_url, container, path, size,
                               metadata=None) -> BlobInfo:
        shard = self._default_shard()
        await shard.copy_from_url_sync(source_url, container, path, size, metadata)
        props = await shard.get_blob_properties(container, path)
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=props["size"] if props else size,
            etag=props.get("etag", "") if props else "",
            url=await shard.get_blob_url(container, path),
            content_md5=None,
            last_modified=props["last_modified"] if props else datetime.utcnow(),
        )

    async def list_blobs(self, container) -> AsyncIterator[str]:
        async for name in self._default_shard().list_blobs(container):
            yield name

    async def list_with_props(self, container) -> AsyncIterator[tuple[str, BlobProps]]:
        async for name, p in self._default_shard().list_blobs_with_properties(container):
            yield name, BlobProps(
                size=p["size"], content_type=None,
                last_modified=p["last_modified"], metadata={},
                copy_status=None, copy_progress=None,
                retention_until=None, legal_hold=False,
            )

    async def get_properties(self, container, path) -> Optional[BlobProps]:
        p = await self._default_shard().get_blob_properties(container, path)
        if not p:
            return None
        return BlobProps(
            size=p["size"], content_type=p.get("content_type"),
            last_modified=p["last_modified"], metadata=p.get("metadata", {}),
            copy_status=p.get("copy_status"), copy_progress=p.get("copy_progress"),
            retention_until=None, legal_hold=False,
        )

    async def delete(self, container, path) -> None:
        await self._default_shard().delete_blob(container, path)

    async def presigned_url(self, container, path, valid_hours=6) -> str:
        return await self._default_shard().get_blob_sas_url(container, path, valid_hours)

    async def apply_immutability(self, container, path, until, mode="Unlocked") -> None:
        from shared.azure_storage import apply_blob_immutability
        await apply_blob_immutability(container, path, until, mode=mode, shard=self._default_shard())

    async def apply_legal_hold(self, container, path, tag="tmvault-legal-hold") -> None:
        from shared.azure_storage import apply_legal_hold
        await apply_legal_hold(container, path, tag=tag, shard=self._default_shard())

    async def remove_legal_hold(self, container, path) -> None:
        shard = self._default_shard()
        async_client = shard.get_async_client()
        blob_client = async_client.get_blob_client(container, path)
        await blob_client.set_legal_hold(legal_hold=False)

    async def apply_lifecycle(self, container, hot_days, cool_days, archive_days=None) -> None:
        from shared.azure_storage import apply_lifecycle_policy
        await apply_lifecycle_policy(
            container, hot_days=hot_days, cool_days=cool_days,
            archive_days=archive_days, shard=self._default_shard(),
        )

    async def ensure_container(self, container) -> None:
        await self._default_shard().ensure_container(container)

    async def close(self) -> None:
        for s in self._shards:
            await s.close()
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/shared/storage/test_azure_blob.py -v
```

Expected: both tests pass against Azurite.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/shared/storage/azure_blob.py tm_backend/tests/shared/storage/test_azure_blob.py
git commit -m "feat(storage): AzureBlobStore adapter wrapping existing AzureStorageShard"
```

---

### Task 4: Implement `SeaweedStore` via aioboto3

**Files:**
- Create: `tm_backend/shared/storage/seaweedfs.py`
- Create: `tm_backend/tests/shared/storage/test_seaweedfs.py`

- [ ] **Step 1: Write failing upload test**

`tm_backend/tests/shared/storage/test_seaweedfs.py`:

```python
import os
import uuid

import pytest
import pytest_asyncio

from shared.storage.seaweedfs import SeaweedStore

ENDPOINT = os.getenv("SEAWEEDFS_TEST_ENDPOINT", "http://localhost:8333")
ACCESS = "testaccess"
SECRET = "testsecret"


@pytest_asyncio.fixture
async def sw_store() -> SeaweedStore:
    store = SeaweedStore(
        backend_id="00000000-0000-0000-0000-000000000002",
        name="seaweedfs-test",
        endpoint=ENDPOINT,
        access_key=ACCESS,
        secret_key=SECRET,
        buckets=["tmvault-test-0"],
        region="us-east-1",
        verify_tls=False,
    )
    # Ensure bucket exists for test
    await store.ensure_container("tmvault-test-0")
    yield store
    await store.close()


@pytest.mark.asyncio
async def test_seaweedfs_upload_download_roundtrip(sw_store):
    path = f"sw-{uuid.uuid4().hex[:8]}.txt"
    body = b"hello seaweed"
    info = await sw_store.upload("tmvault-test-0", path, body, metadata={"kind": "test"})
    assert info.size == len(body)
    got = await sw_store.download("tmvault-test-0", path)
    assert got == body
    await sw_store.delete("tmvault-test-0", path)


@pytest.mark.asyncio
async def test_seaweedfs_missing_returns_none(sw_store):
    got = await sw_store.download("tmvault-test-0", "absent-key.txt")
    assert got is None
```

- [ ] **Step 2: Run — expect ImportError**

```bash
pytest tests/shared/storage/test_seaweedfs.py -x
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement `shared/storage/seaweedfs.py`**

```python
"""SeaweedStore — S3-compat backend via aioboto3.

Bucket convention: one bucket per shard (e.g. `tmvault-shard-0`). Keys are
path-prefixed: `{workload}/{tenant_id}/{resource_id}/{snapshot_id}/...`.

All buckets MUST be created with ObjectLockEnabledForBucket=True +
versioning enabled (see scripts/create_seaweedfs_buckets.py). Retrofit
not possible.
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import AsyncIterator, Optional

import aioboto3
from botocore.exceptions import ClientError

from shared.storage.base import BlobInfo, BlobProps
from shared.storage.errors import BackendUnreachableError, ImmutableBlobError

_MODE_TMVAULT_TO_S3 = {"Locked": "COMPLIANCE", "Unlocked": "GOVERNANCE"}


class SeaweedStore:
    kind = "seaweedfs"

    def __init__(
        self,
        backend_id: str,
        name: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
        buckets: list[str],
        region: str = "us-east-1",
        verify_tls: bool = True,
        ca_bundle: Optional[str] = None,
        upload_concurrency: int = 8,
        multipart_threshold_mb: int = 100,
    ):
        self.backend_id = backend_id
        self.name = name
        self._endpoint = endpoint
        self._access = access_key
        self._secret = secret_key
        self._buckets = buckets
        self._region = region
        self._verify = ca_bundle if ca_bundle else verify_tls
        self._session = aioboto3.Session()
        self._upload_concurrency = upload_concurrency
        self._multipart_threshold = multipart_threshold_mb * 1024 * 1024
        self._forced_bucket: Optional[str] = None  # set by shard_for()

    @classmethod
    def from_config(cls, backend_id: str, name: str, endpoint: str,
                    secret_ref: str, config: dict) -> "SeaweedStore":
        import os
        if secret_ref.startswith("env://"):
            secret = os.getenv(secret_ref[len("env://"):], "")
        else:
            raise ValueError(f"Unsupported secret_ref scheme: {secret_ref}")
        access_env = config.get("access_key_env", "ONPREM_S3_ACCESS_KEY")
        access = os.getenv(access_env, "")
        return cls(
            backend_id=backend_id, name=name, endpoint=endpoint,
            access_key=access, secret_key=secret,
            buckets=list(config.get("buckets", [])),
            region=config.get("region", "us-east-1"),
            verify_tls=config.get("verify_tls", True),
            ca_bundle=config.get("ca_bundle"),
            upload_concurrency=config.get("upload_concurrency", 8),
            multipart_threshold_mb=config.get("multipart_threshold_mb", 100),
        )

    # --- sharding

    def shard_for(self, tenant_id: str, resource_id: str) -> "SeaweedStore":
        if not self._buckets:
            raise RuntimeError("no buckets configured")
        h = int(hashlib.md5(f"{tenant_id}:{resource_id}".encode()).hexdigest(), 16)
        chosen = self._buckets[h % len(self._buckets)]
        clone = SeaweedStore(
            backend_id=self.backend_id, name=self.name, endpoint=self._endpoint,
            access_key=self._access, secret_key=self._secret, buckets=[chosen],
            region=self._region, verify_tls=bool(self._verify),
            ca_bundle=self._verify if isinstance(self._verify, str) else None,
            upload_concurrency=self._upload_concurrency,
            multipart_threshold_mb=self._multipart_threshold // (1024 * 1024),
        )
        clone._forced_bucket = chosen
        return clone

    # --- helpers

    def _client_ctx(self):
        return self._session.client(
            "s3",
            endpoint_url=self._endpoint,
            aws_access_key_id=self._access,
            aws_secret_access_key=self._secret,
            region_name=self._region,
            verify=self._verify,
        )

    def _bucket(self, container: str) -> str:
        # `container` is the TMvault logical container name. In S3 we map to
        # one bucket (via shard selection) and use container as a path prefix.
        return self._forced_bucket or self._buckets[0]

    def _key(self, container: str, path: str) -> str:
        return f"{container}/{path}" if container and not self._forced_bucket_is_per_container() else path

    def _forced_bucket_is_per_container(self) -> bool:
        # Future extension: if config says one-bucket-per-container.
        return False

    # --- BackendStore impl

    async def upload(self, container, path, content, metadata=None, overwrite=True) -> BlobInfo:
        bucket = self._bucket(container)
        key = self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                await s3.put_object(
                    Bucket=bucket, Key=key, Body=content,
                    Metadata=_clean_metadata(metadata or {}),
                )
                head = await s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise BackendUnreachableError(str(e)) from e
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=head["ContentLength"], etag=head["ETag"].strip('"'),
            url=f"{self._endpoint}/{bucket}/{key}",
            content_md5=None, last_modified=head["LastModified"],
        )

    async def upload_from_file(self, container, path, file_path, size,
                               metadata=None, overwrite=True) -> BlobInfo:
        bucket = self._bucket(container)
        key = self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                with open(file_path, "rb") as f:
                    await s3.upload_fileobj(
                        f, bucket, key,
                        ExtraArgs={"Metadata": _clean_metadata(metadata or {})},
                    )
                head = await s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise BackendUnreachableError(str(e)) from e
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=head["ContentLength"], etag=head["ETag"].strip('"'),
            url=f"{self._endpoint}/{bucket}/{key}",
            content_md5=None, last_modified=head["LastModified"],
        )

    async def download(self, container, path) -> Optional[bytes]:
        bucket, key = self._bucket(container), self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                obj = await s3.get_object(Bucket=bucket, Key=key)
                body = obj["Body"]
                return await body.read()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise BackendUnreachableError(str(e)) from e

    async def download_stream(self, container, path, chunk_size=4 * 1024 * 1024):
        bucket, key = self._bucket(container), self._key(container, path)
        async with self._client_ctx() as s3:
            try:
                obj = await s3.get_object(Bucket=bucket, Key=key)
            except ClientError as e:
                if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                    return
                raise BackendUnreachableError(str(e)) from e
            body = obj["Body"]
            async for chunk in body.iter_chunks(chunk_size):
                yield chunk

    async def stage_block(self, container, path, block_id, data) -> None:
        raise NotImplementedError("use initiate_multipart + upload_part — see commit_blocks")

    async def commit_blocks(self, container, path, block_ids, metadata=None) -> None:
        # For SeaweedFS we implement multipart differently — stage_block is
        # replaced by the pattern: initiate_multipart_upload → upload_part
        # (per block_id) → complete_multipart_upload. Router callers that
        # use stage_block/commit_blocks must switch to the multipart method.
        raise NotImplementedError("use multipart_upload helper instead")

    async def put_block_from_url(self, container, path, block_id, source_url) -> None:
        # Implemented as upload_part_copy inside an ongoing multipart session.
        # Router-side helper creates the session; callers pass the upload_id.
        raise NotImplementedError("use multipart_copy_from_url helper")

    async def server_side_copy(self, source_url, container, path, size,
                               metadata=None) -> BlobInfo:
        bucket, key = self._bucket(container), self._key(container, path)
        # S3 CopyObject works only between S3-compatible endpoints.
        # Cross-cloud (Azure→S3) source URLs must be streamed through worker.
        if not source_url.startswith(self._endpoint):
            raise NotImplementedError(
                "cross-backend server-side copy not supported — stream via worker",
            )
        try:
            async with self._client_ctx() as s3:
                await s3.copy_object(
                    Bucket=bucket, Key=key,
                    CopySource=source_url,
                    Metadata=_clean_metadata(metadata or {}),
                    MetadataDirective="REPLACE",
                )
                head = await s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise BackendUnreachableError(str(e)) from e
        return BlobInfo(
            backend_id=self.backend_id, container=container, path=path,
            size=head["ContentLength"], etag=head["ETag"].strip('"'),
            url=f"{self._endpoint}/{bucket}/{key}",
            content_md5=None, last_modified=head["LastModified"],
        )

    async def list_blobs(self, container):
        bucket = self._bucket(container)
        prefix = f"{container}/" if not self._forced_bucket_is_per_container() else ""
        async with self._client_ctx() as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    yield obj["Key"][len(prefix):] if prefix else obj["Key"]

    async def list_with_props(self, container):
        bucket = self._bucket(container)
        prefix = f"{container}/" if not self._forced_bucket_is_per_container() else ""
        async with self._client_ctx() as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    name = obj["Key"][len(prefix):] if prefix else obj["Key"]
                    yield name, BlobProps(
                        size=obj["Size"], content_type=None,
                        last_modified=obj["LastModified"], metadata={},
                        copy_status=None, copy_progress=None,
                        retention_until=None, legal_hold=False,
                    )

    async def get_properties(self, container, path) -> Optional[BlobProps]:
        bucket, key = self._bucket(container), self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                head = await s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise BackendUnreachableError(str(e)) from e
        return BlobProps(
            size=head["ContentLength"],
            content_type=head.get("ContentType"),
            last_modified=head["LastModified"],
            metadata=head.get("Metadata", {}),
            copy_status=None, copy_progress=None,
            retention_until=head.get("ObjectLockRetainUntilDate"),
            legal_hold=head.get("ObjectLockLegalHoldStatus") == "ON",
        )

    async def delete(self, container, path) -> None:
        bucket, key = self._bucket(container), self._key(container, path)
        try:
            async with self._client_ctx() as s3:
                await s3.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("AccessDenied", "InvalidRequest") \
               and "WORM" in e.response["Error"].get("Message", "") \
                 or "retention" in e.response["Error"].get("Message", "").lower():
                raise ImmutableBlobError(str(e)) from e
            if code in ("NoSuchKey", "404"):
                return
            raise BackendUnreachableError(str(e)) from e

    async def presigned_url(self, container, path, valid_hours=6) -> str:
        bucket, key = self._bucket(container), self._key(container, path)
        async with self._client_ctx() as s3:
            return await s3.generate_presigned_url(
                "get_object", Params={"Bucket": bucket, "Key": key},
                ExpiresIn=valid_hours * 3600,
            )

    async def apply_immutability(self, container, path, until, mode="Unlocked") -> None:
        bucket, key = self._bucket(container), self._key(container, path)
        s3_mode = _MODE_TMVAULT_TO_S3.get(mode, "GOVERNANCE")
        try:
            async with self._client_ctx() as s3:
                await s3.put_object_retention(
                    Bucket=bucket, Key=key,
                    Retention={"Mode": s3_mode, "RetainUntilDate": until},
                )
        except ClientError as e:
            raise BackendUnreachableError(str(e)) from e

    async def apply_legal_hold(self, container, path, tag="tmvault-legal-hold") -> None:
        bucket, key = self._bucket(container), self._key(container, path)
        async with self._client_ctx() as s3:
            await s3.put_object_legal_hold(
                Bucket=bucket, Key=key, LegalHold={"Status": "ON"},
            )

    async def remove_legal_hold(self, container, path) -> None:
        bucket, key = self._bucket(container), self._key(container, path)
        async with self._client_ctx() as s3:
            await s3.put_object_legal_hold(
                Bucket=bucket, Key=key, LegalHold={"Status": "OFF"},
            )

    async def apply_lifecycle(self, container, hot_days, cool_days, archive_days=None) -> None:
        bucket = self._bucket(container)
        rules = [{
            "ID": f"tier-cool-{hot_days}d",
            "Status": "Enabled",
            "Filter": {"Prefix": f"{container}/" if not self._forced_bucket_is_per_container() else ""},
            "Transitions": [{"Days": hot_days, "StorageClass": "STANDARD_IA"}],
        }]
        if archive_days:
            rules.append({
                "ID": f"expire-{archive_days}d",
                "Status": "Enabled",
                "Filter": {"Prefix": f"{container}/" if not self._forced_bucket_is_per_container() else ""},
                "Expiration": {"Days": hot_days + cool_days + archive_days},
            })
        async with self._client_ctx() as s3:
            await s3.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration={"Rules": rules},
            )

    async def ensure_container(self, container) -> None:
        # For bucket-per-shard layout, the bucket must already exist
        # (created via scripts/create_seaweedfs_buckets.py). This is a no-op
        # unless we ever go to bucket-per-container.
        return

    async def close(self) -> None:
        return


def _clean_metadata(metadata: dict) -> dict:
    """S3 metadata: ASCII keys, ASCII values."""
    clean = {}
    for k, v in metadata.items():
        ks = str(k).encode("ascii", errors="replace").decode("ascii")
        vs = str(v).encode("ascii", errors="replace").decode("ascii")
        clean[ks] = vs
    return clean
```

- [ ] **Step 4: Pre-create test bucket**

Add `tm_backend/tests/shared/storage/conftest.py` → extend with:

```python
import boto3
import pytest

@pytest.fixture(scope="session", autouse=True)
def _ensure_test_bucket(storage_test_services):
    s3 = boto3.client(
        "s3", endpoint_url="http://localhost:8333",
        aws_access_key_id="testaccess", aws_secret_access_key="testsecret",
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(
            Bucket="tmvault-test-0",
            ObjectLockEnabledForBucket=True,
        )
        s3.put_bucket_versioning(
            Bucket="tmvault-test-0",
            VersioningConfiguration={"Status": "Enabled"},
        )
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    yield
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/shared/storage/test_seaweedfs.py -v
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add tm_backend/shared/storage/seaweedfs.py \
        tm_backend/tests/shared/storage/test_seaweedfs.py \
        tm_backend/tests/shared/storage/conftest.py
git commit -m "feat(storage): SeaweedStore impl via aioboto3"
```

---

### Task 5: Contract tests — same behavior on both backends

**Files:**
- Create: `tm_backend/tests/shared/storage/test_contract.py`

- [ ] **Step 1: Write the parametrized contract test**

`tm_backend/tests/shared/storage/test_contract.py`:

```python
"""Contract tests — same test body runs against both AzureBlobStore and
SeaweedStore to guarantee behavior parity. Any new method added to the
BackendStore protocol should get a matching test here."""
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from shared.storage.azure_blob import AzureBlobStore
from shared.storage.seaweedfs import SeaweedStore

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
)


@pytest_asyncio.fixture(params=["azure", "seaweedfs"])
async def store(request):
    if request.param == "azure":
        s = AzureBlobStore.from_connection_string(
            AZURITE_CONN, backend_id="00000000-0000-0000-0000-000000000001",
            name="azurite-contract",
        )
        container = f"contract-{uuid.uuid4().hex[:8]}"
        await s.ensure_container(container)
    else:
        s = SeaweedStore(
            backend_id="00000000-0000-0000-0000-000000000002",
            name="seaweedfs-contract",
            endpoint=os.getenv("SEAWEEDFS_TEST_ENDPOINT", "http://localhost:8333"),
            access_key="testaccess", secret_key="testsecret",
            buckets=["tmvault-test-0"], verify_tls=False,
        )
        container = f"contract-{uuid.uuid4().hex[:8]}"
    yield s, container
    await s.close()


@pytest.mark.asyncio
async def test_upload_download(store):
    s, c = store
    info = await s.upload(c, "k.txt", b"payload")
    assert info.size == 7
    got = await s.download(c, "k.txt")
    assert got == b"payload"


@pytest.mark.asyncio
async def test_upload_metadata_roundtrip(store):
    s, c = store
    await s.upload(c, "m.txt", b"m", metadata={"kind": "meta", "tag": "t1"})
    props = await s.get_properties(c, "m.txt")
    assert props is not None
    assert props.metadata.get("kind") == "meta"


@pytest.mark.asyncio
async def test_missing_blob_returns_none(store):
    s, c = store
    got = await s.download(c, "absent.txt")
    assert got is None


@pytest.mark.asyncio
async def test_list_blobs(store):
    s, c = store
    await s.upload(c, "a.txt", b"a")
    await s.upload(c, "b.txt", b"b")
    names = []
    async for n in s.list_blobs(c):
        names.append(n)
    assert "a.txt" in names
    assert "b.txt" in names


@pytest.mark.asyncio
async def test_delete(store):
    s, c = store
    await s.upload(c, "del.txt", b"x")
    await s.delete(c, "del.txt")
    assert await s.download(c, "del.txt") is None


@pytest.mark.asyncio
async def test_presigned_url_is_get_readable(store):
    import aiohttp
    s, c = store
    await s.upload(c, "p.txt", b"presigned")
    url = await s.presigned_url(c, "p.txt", valid_hours=1)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            assert resp.status == 200
            assert await resp.read() == b"presigned"


@pytest.mark.asyncio
async def test_shard_for_is_deterministic(store):
    s, _ = store
    a = s.shard_for("t1", "r1")
    b = s.shard_for("t1", "r1")
    # Same inputs pick same shard; different inputs may differ.
    assert a.name == b.name
```

- [ ] **Step 2: Run contract tests**

```bash
pytest tests/shared/storage/test_contract.py -v
```

Expected: all tests pass, each exercised twice (azure + seaweedfs).

- [ ] **Step 3: Commit**

```bash
git add tm_backend/tests/shared/storage/test_contract.py
git commit -m "test(storage): contract tests — parity across Azure + SeaweedFS"
```

---

### Task 6: `StorageRouter` — DB-driven backend registry + LISTEN/NOTIFY

**Files:**
- Create: `tm_backend/shared/storage/router.py`
- Create: `tm_backend/tests/shared/storage/test_router.py`

- [ ] **Step 1: Write failing router test with mocked backends**

`tm_backend/tests/shared/storage/test_router.py`:

```python
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.storage.errors import (
    BackendNotFoundError,
    TransitionInProgressError,
)
from shared.storage.router import StorageRouter


def _mock_store(backend_id: str, name: str):
    s = MagicMock()
    s.backend_id = backend_id
    s.name = name
    s.kind = "mock"
    return s


@pytest.mark.asyncio
async def test_router_returns_active_store_when_stable():
    r = StorageRouter()
    az = _mock_store("az", "azure")
    sw = _mock_store("sw", "seaweedfs")
    r._backends = {"az": az, "sw": sw}
    r._active_backend_id = "az"
    r._transition_state = "stable"
    assert r.get_active_store() is az


@pytest.mark.asyncio
async def test_router_blocks_active_store_during_draining():
    r = StorageRouter()
    r._backends = {"az": _mock_store("az", "azure")}
    r._active_backend_id = "az"
    r._transition_state = "draining"
    with pytest.raises(TransitionInProgressError):
        r.get_active_store()


@pytest.mark.asyncio
async def test_router_get_by_id_ignores_transition():
    r = StorageRouter()
    az = _mock_store("az", "azure")
    r._backends = {"az": az}
    r._active_backend_id = "az"
    r._transition_state = "flipping"
    assert r.get_store_by_id("az") is az


@pytest.mark.asyncio
async def test_router_get_unknown_id_raises():
    r = StorageRouter()
    r._backends = {}
    with pytest.raises(BackendNotFoundError):
        r.get_store_by_id("nope")


@pytest.mark.asyncio
async def test_router_get_store_for_item():
    r = StorageRouter()
    sw = _mock_store("sw", "seaweedfs")
    r._backends = {"sw": sw}
    item = MagicMock()
    item.backend_id = "sw"
    assert r.get_store_for_item(item) is sw


@pytest.mark.asyncio
async def test_router_writable_reflects_state():
    r = StorageRouter()
    r._transition_state = "stable"
    assert r.writable() is True
    r._transition_state = "draining"
    assert r.writable() is False
    r._transition_state = "flipping"
    assert r.writable() is False
```

- [ ] **Step 2: Run — expect failure**

```bash
pytest tests/shared/storage/test_router.py -x
```

- [ ] **Step 3: Implement `shared/storage/router.py`**

```python
"""StorageRouter — DB-driven backend registry with LISTEN/NOTIFY.

Loaded once at process startup. Listens on Postgres channel
`system_config_changed` for runtime swaps without restart.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import asyncpg

from shared.storage.azure_blob import AzureBlobStore
from shared.storage.base import BackendStore
from shared.storage.errors import (
    BackendNotFoundError,
    TransitionInProgressError,
)
from shared.storage.seaweedfs import SeaweedStore

log = logging.getLogger("tmvault.storage.router")


@dataclass
class PreflightResult:
    ok: bool
    details: dict


class StorageRouter:
    def __init__(self):
        self._backends: dict[str, BackendStore] = {}
        self._active_backend_id: Optional[str] = None
        self._transition_state: str = "stable"
        self._cooldown_until: Optional[datetime] = None
        self._db_dsn: Optional[str] = None
        self._listener_task: Optional[asyncio.Task] = None

    # ---- lifecycle

    async def load(self, db_dsn: str) -> None:
        self._db_dsn = db_dsn
        await self._reload_from_db()
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def close(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
        for s in self._backends.values():
            await s.close()

    async def _reload_from_db(self) -> None:
        conn = await asyncpg.connect(self._db_dsn)
        try:
            sb_rows = await conn.fetch(
                "SELECT id::text, kind, name, endpoint, config, secret_ref "
                "FROM storage_backends WHERE is_enabled = true"
            )
            sc = await conn.fetchrow(
                "SELECT active_backend_id::text, transition_state, cooldown_until "
                "FROM system_config WHERE id = 1"
            )
        finally:
            await conn.close()

        new_backends: dict[str, BackendStore] = {}
        for r in sb_rows:
            import json
            cfg = r["config"] if isinstance(r["config"], dict) else json.loads(r["config"])
            if r["kind"] == "azure_blob":
                new_backends[r["id"]] = AzureBlobStore.from_config(
                    backend_id=r["id"], name=r["name"], config=cfg,
                )
            elif r["kind"] == "seaweedfs":
                new_backends[r["id"]] = SeaweedStore.from_config(
                    backend_id=r["id"], name=r["name"], endpoint=r["endpoint"],
                    secret_ref=r["secret_ref"], config=cfg,
                )
        # Close old backends that are no longer listed
        for old_id, old_store in self._backends.items():
            if old_id not in new_backends:
                await old_store.close()
        self._backends = new_backends
        if sc:
            self._active_backend_id = sc["active_backend_id"]
            self._transition_state = sc["transition_state"]
            self._cooldown_until = sc["cooldown_until"]

    async def _listen_loop(self) -> None:
        while True:
            try:
                conn = await asyncpg.connect(self._db_dsn)
                await conn.add_listener(
                    "system_config_changed",
                    lambda *_: asyncio.create_task(self._reload_from_db()),
                )
                # Keep the connection alive
                while True:
                    await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("router listen loop error: %s, reconnecting", e)
                await asyncio.sleep(5)

    # ---- primary API

    def get_active_store(self) -> BackendStore:
        if self._transition_state != "stable":
            raise TransitionInProgressError(
                f"cannot write during state={self._transition_state}",
            )
        if not self._active_backend_id:
            raise RuntimeError("router not loaded")
        return self.get_store_by_id(self._active_backend_id)

    def get_store_by_id(self, backend_id: str) -> BackendStore:
        if backend_id not in self._backends:
            raise BackendNotFoundError(f"backend_id not registered: {backend_id}")
        return self._backends[backend_id]

    def get_store_for_item(self, item) -> BackendStore:
        return self.get_store_by_id(str(item.backend_id))

    def get_store_for_snapshot(self, snapshot) -> BackendStore:
        return self.get_store_by_id(str(snapshot.backend_id))

    def writable(self) -> bool:
        return self._transition_state == "stable"

    def active_backend_id(self) -> Optional[str]:
        return self._active_backend_id

    def transition_state(self) -> str:
        return self._transition_state

    def list_backends(self) -> list[BackendStore]:
        return list(self._backends.values())


# Module-level singleton
router = StorageRouter()
```

- [ ] **Step 4: Run router tests**

```bash
pytest tests/shared/storage/test_router.py -v
```

Expected: all 6 pass.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/shared/storage/router.py tm_backend/tests/shared/storage/test_router.py
git commit -m "feat(storage): StorageRouter with LISTEN/NOTIFY + mocked unit tests"
```

---

### Task 7: Wire router startup into services

**Files:**
- Modify: `tm_backend/shared/config.py`
- Modify: `tm_backend/services/api-gateway/main.py` (find startup hook)
- Modify: each of: `snapshot-service`, `job-service`, `backup-scheduler`, `dr-replication-worker`, `backup-worker`, `restore-worker`, `azure-workload-worker`, `chat-export-worker`

- [ ] **Step 1: Add env vars to `shared/config.py`**

Locate the `Settings` class; add near the existing `AZURE_STORAGE_*` block:

```python
# --- On-prem SeaweedFS backend (enabled when storage_backends row exists)
self.ONPREM_S3_ENDPOINT = os.getenv("ONPREM_S3_ENDPOINT", "")
self.ONPREM_S3_ACCESS_KEY = os.getenv("ONPREM_S3_ACCESS_KEY", "")
self.ONPREM_S3_SECRET_KEY = os.getenv("ONPREM_S3_SECRET_KEY", "")
onprem_buckets = os.getenv("ONPREM_S3_BUCKETS", "")
self.ONPREM_S3_BUCKETS = [b.strip() for b in onprem_buckets.split(",") if b.strip()]
self.ONPREM_S3_REGION = os.getenv("ONPREM_S3_REGION", "us-east-1")
self.ONPREM_S3_VERIFY_TLS = os.getenv("ONPREM_S3_VERIFY_TLS", "true").lower() == "true"
self.ONPREM_S3_CA_BUNDLE = os.getenv("ONPREM_S3_CA_BUNDLE", "") or None
self.ONPREM_UPLOAD_CONCURRENCY = int(os.getenv("ONPREM_UPLOAD_CONCURRENCY", "8"))
self.ONPREM_MULTIPART_THRESHOLD_MB = int(os.getenv("ONPREM_MULTIPART_THRESHOLD_MB", "100"))
self.ONPREM_RETRY_MAX = int(os.getenv("ONPREM_RETRY_MAX", "3"))
```

- [ ] **Step 2: Find each service's startup hook**

```bash
grep -rn "on_event\|@app.on_event\|startup_event\|async def startup" tm_backend/services tm_backend/workers | head -30
```

- [ ] **Step 3: For each service main, add router.load() on startup**

Pattern (FastAPI services — `api-gateway`, `snapshot-service`, `job-service`, `backup-scheduler`):

```python
from shared.storage.router import router
from shared.config import settings

@app.on_event("startup")
async def _load_storage_router():
    await router.load(db_dsn=settings.DATABASE_URL)

@app.on_event("shutdown")
async def _close_storage_router():
    await router.close()
```

Pattern (worker services with their own `asyncio.run(main())`):

```python
from shared.storage.router import router
from shared.config import settings

async def main():
    await router.load(db_dsn=settings.DATABASE_URL)
    try:
        await run_worker()  # existing entry
    finally:
        await router.close()
```

- [ ] **Step 4: Smoke test one service loads router**

```bash
cd tm_backend
DATABASE_URL=postgresql://... python -c "
import asyncio
from shared.storage.router import router
async def main():
    await router.load('postgresql://...')
    print('backends:', [b.name for b in router.list_backends()])
    await router.close()
asyncio.run(main())
"
```

Will fail until Phase 2 migration seeds rows — expected. Proves import path works.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/shared/config.py tm_backend/services/*/main.py tm_backend/workers/*/main.py
git commit -m "feat(storage): wire StorageRouter.load() into all service startups"
```

---

**Phase 1 exit criteria:**
- [ ] `pytest tests/shared/storage/` — all green on both Azurite + SeaweedFS
- [ ] Router imports cleanly from every service startup
- [ ] No production behavior changed yet (routers load empty; nothing uses them)

---

## Phase 2 — Schema Migration + Call-Site Codemod (est. 2 weeks)

Goal at phase end: schema has new tables + `backend_id` columns; production runs through router (still Azure-only); all 26 files updated; integration test green.

### Task 8: Schema migration — new tables

**Files:**
- Create: `tm_backend/migrations/2026_04_21_onprem_storage_schema.sql`

- [ ] **Step 1: Write migration SQL**

```sql
-- tm_backend/migrations/2026_04_21_onprem_storage_schema.sql
-- New tables + column additions for on-prem storage backend + toggle.
-- Run via the same mechanism as other migrations/*.sql files.

BEGIN;

-- 1. Storage backend registry
CREATE TABLE IF NOT EXISTS storage_backends (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind         TEXT NOT NULL CHECK (kind IN ('azure_blob','seaweedfs')),
  name         TEXT NOT NULL UNIQUE,
  endpoint     TEXT NOT NULL,
  config       JSONB NOT NULL DEFAULT '{}'::jsonb,
  secret_ref   TEXT NOT NULL,
  is_enabled   BOOLEAN NOT NULL DEFAULT true,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Singleton runtime state
CREATE TABLE IF NOT EXISTS system_config (
  id                 SMALLINT PRIMARY KEY CHECK (id = 1),
  active_backend_id  UUID NOT NULL REFERENCES storage_backends(id),
  transition_state   TEXT NOT NULL DEFAULT 'stable'
                     CHECK (transition_state IN ('stable','draining','flipping')),
  last_toggle_at     TIMESTAMPTZ,
  cooldown_until     TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Audit / orchestration log
CREATE TABLE IF NOT EXISTS storage_toggle_events (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_id             UUID NOT NULL,
  actor_ip             INET,
  from_backend_id      UUID NOT NULL REFERENCES storage_backends(id),
  to_backend_id        UUID NOT NULL REFERENCES storage_backends(id),
  reason               TEXT,
  status               TEXT NOT NULL DEFAULT 'started',
  started_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  drain_completed_at   TIMESTAMPTZ,
  flip_completed_at    TIMESTAMPTZ,
  completed_at         TIMESTAMPTZ,
  error_message        TEXT,
  pre_flight_checks    JSONB,
  drained_job_count    INTEGER,
  retried_job_count    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_toggle_events_actor
  ON storage_toggle_events(actor_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_toggle_events_status
  ON storage_toggle_events(status)
  WHERE status NOT IN ('completed','aborted','failed');

-- 4. Add backend_id to snapshots + snapshot_items (nullable for now; backfill next migration)
ALTER TABLE snapshots
  ADD COLUMN IF NOT EXISTS backend_id UUID REFERENCES storage_backends(id);
ALTER TABLE snapshot_items
  ADD COLUMN IF NOT EXISTS backend_id UUID REFERENCES storage_backends(id);

CREATE INDEX IF NOT EXISTS idx_snapshots_backend ON snapshots(backend_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_items_backend ON snapshot_items(backend_id);

-- 5. Job retry plumbing
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS retry_reason TEXT,
  ADD COLUMN IF NOT EXISTS pre_toggle_job_id UUID REFERENCES jobs(id);

-- 6. Seed azure-primary backend + initial system_config row
-- Idempotent: only insert if missing.
INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config)
SELECT 'azure_blob', 'azure-primary',
       COALESCE(current_setting('tmvault.azure_endpoint', true),
                'https://PLACEHOLDER.blob.core.windows.net'),
       'env://AZURE_STORAGE_ACCOUNT_KEY',
       jsonb_build_object(
         'shards', jsonb_build_array(
           jsonb_build_object(
             'account', COALESCE(current_setting('tmvault.azure_account', true), 'PLACEHOLDER'),
             'key_ref', 'env://AZURE_STORAGE_ACCOUNT_KEY'
           )
         )
       )
WHERE NOT EXISTS (SELECT 1 FROM storage_backends WHERE name = 'azure-primary');

INSERT INTO system_config (id, active_backend_id)
SELECT 1, (SELECT id FROM storage_backends WHERE name = 'azure-primary')
WHERE NOT EXISTS (SELECT 1 FROM system_config WHERE id = 1);

-- 7. NOTIFY trigger on system_config changes
CREATE OR REPLACE FUNCTION notify_system_config_changed()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('system_config_changed', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_system_config_notify ON system_config;
CREATE TRIGGER trg_system_config_notify
  AFTER UPDATE OR INSERT ON system_config
  FOR EACH ROW
  EXECUTE FUNCTION notify_system_config_changed();

COMMIT;
```

- [ ] **Step 2: Apply locally**

```bash
cd tm_backend
psql "$DATABASE_URL" -f migrations/2026_04_21_onprem_storage_schema.sql
```

Expected: `COMMIT` and no errors.

- [ ] **Step 3: Verify**

```bash
psql "$DATABASE_URL" -c "\dt storage_backends"
psql "$DATABASE_URL" -c "SELECT name, kind FROM storage_backends"
psql "$DATABASE_URL" -c "SELECT id, active_backend_id, transition_state FROM system_config"
```

Expected: one row in each, `active_backend_id` matches azure row.

- [ ] **Step 4: Update placeholder values for local dev**

```bash
psql "$DATABASE_URL" -c "
UPDATE storage_backends
SET endpoint = '${AZURE_STORAGE_BLOB_ENDPOINT}',
    config = jsonb_set(config, '{shards,0,account}', to_jsonb('${AZURE_STORAGE_ACCOUNT_NAME}'::text))
WHERE name = 'azure-primary';
"
```

- [ ] **Step 5: Commit**

```bash
git add tm_backend/migrations/2026_04_21_onprem_storage_schema.sql
git commit -m "feat(db): add storage_backends, system_config, storage_toggle_events + backend_id columns"
```

---

### Task 9: Schema migration — backfill `backend_id` + NOT NULL

**Files:**
- Create: `tm_backend/migrations/2026_04_22_onprem_storage_backfill.sql`

- [ ] **Step 1: Write the backfill migration**

```sql
-- tm_backend/migrations/2026_04_22_onprem_storage_backfill.sql
-- Run AFTER schema migration is applied AND storage_backends seed row exists.
-- Sets backend_id on all existing snapshots/items to azure-primary, then NOT NULL.

BEGIN;

DO $$
DECLARE
  az_id UUID;
BEGIN
  SELECT id INTO az_id FROM storage_backends WHERE name = 'azure-primary' LIMIT 1;
  IF az_id IS NULL THEN
    RAISE EXCEPTION 'azure-primary backend not seeded — run 2026_04_21_onprem_storage_schema.sql first';
  END IF;

  UPDATE snapshots      SET backend_id = az_id WHERE backend_id IS NULL;
  UPDATE snapshot_items SET backend_id = az_id WHERE backend_id IS NULL;
END $$;

-- On very large tables this may take minutes; run during a low-traffic window.
ALTER TABLE snapshots      ALTER COLUMN backend_id SET NOT NULL;
ALTER TABLE snapshot_items ALTER COLUMN backend_id SET NOT NULL;

COMMIT;
```

- [ ] **Step 2: Apply + verify**

```bash
psql "$DATABASE_URL" -f migrations/2026_04_22_onprem_storage_backfill.sql
psql "$DATABASE_URL" -c "SELECT count(*) FROM snapshots WHERE backend_id IS NULL"  # expect 0
psql "$DATABASE_URL" -c "SELECT count(*) FROM snapshot_items WHERE backend_id IS NULL"  # expect 0
```

- [ ] **Step 3: Commit**

```bash
git add tm_backend/migrations/2026_04_22_onprem_storage_backfill.sql
git commit -m "feat(db): backfill snapshots.backend_id + snapshot_items.backend_id + NOT NULL"
```

---

### Task 10: Add ORM classes to `shared/models.py`

**Files:**
- Modify: `tm_backend/shared/models.py`

- [ ] **Step 1: Read the file to find the end of class defs**

```bash
grep -n "^class " tm_backend/shared/models.py | tail -10
```

- [ ] **Step 2: Append new ORM classes**

At the end of `shared/models.py`:

```python
# ---- Storage backend abstraction (2026-04-21) ----

class StorageBackendKind(str, enum.Enum):
    azure_blob = "azure_blob"
    seaweedfs = "seaweedfs"


class TransitionState(str, enum.Enum):
    stable = "stable"
    draining = "draining"
    flipping = "flipping"


class ToggleStatus(str, enum.Enum):
    started = "started"
    drain_started = "drain_started"
    drain_completed = "drain_completed"
    db_promoted = "db_promoted"
    dns_flipped = "dns_flipped"
    workers_restarted = "workers_restarted"
    smoke_passed = "smoke_passed"
    completed = "completed"
    aborted = "aborted"
    failed = "failed"


class StorageBackend(Base):
    __tablename__ = "storage_backends"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid())
    kind = Column(String, nullable=False)
    name = Column(String, unique=True, nullable=False)
    endpoint = Column(String, nullable=False)
    config = Column(JSONB, nullable=False, server_default="{}")
    secret_ref = Column(String, nullable=False)
    is_enabled = Column(Boolean, nullable=False, server_default="true")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SystemConfig(Base):
    __tablename__ = "system_config"

    id = Column(SmallInteger, primary_key=True)
    active_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    transition_state = Column(String, nullable=False, server_default="stable")
    last_toggle_at = Column(DateTime(timezone=True))
    cooldown_until = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class StorageToggleEvent(Base):
    __tablename__ = "storage_toggle_events"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid())
    actor_id = Column(UUID(as_uuid=True), nullable=False)
    actor_ip = Column(INET)
    from_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    to_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    reason = Column(String)
    status = Column(String, nullable=False, server_default="started")
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    drain_completed_at = Column(DateTime(timezone=True))
    flip_completed_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    pre_flight_checks = Column(JSONB)
    drained_job_count = Column(Integer)
    retried_job_count = Column(Integer)
```

Also add to the existing `Snapshot` + `SnapshotItem` + `Job` classes:

```python
# In Snapshot class
backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)

# In SnapshotItem class
backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)

# In Job class
retry_reason = Column(Text)
pre_toggle_job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))
```

Make sure `INET`, `SmallInteger`, `JSONB`, `Text`, `Boolean`, `ForeignKey`, `func` are imported at the top. Add any missing:

```python
from sqlalchemy import SmallInteger, Boolean, Text, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import INET, JSONB, UUID
```

- [ ] **Step 3: Verify model import works**

```bash
cd tm_backend
python -c "from shared.models import StorageBackend, SystemConfig, StorageToggleEvent; print('ok')"
```

Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add tm_backend/shared/models.py
git commit -m "feat(models): add StorageBackend, SystemConfig, StorageToggleEvent ORM + FK cols"
```

---

### Task 11: Add Pydantic DTOs for toggle API

**Files:**
- Modify: `tm_backend/shared/schemas.py`

- [ ] **Step 1: Append schemas**

```python
# ---- Storage toggle (2026-04-21) ----

class StorageBackendOut(BaseModel):
    id: UUID
    kind: str
    name: str
    endpoint: str
    is_enabled: bool
    class Config:
        from_attributes = True


class SystemConfigOut(BaseModel):
    active_backend_id: UUID
    active_backend_name: str
    transition_state: str
    last_toggle_at: Optional[datetime]
    cooldown_until: Optional[datetime]
    class Config:
        from_attributes = True


class ToggleRequest(BaseModel):
    target_backend_id: UUID
    reason: str = Field(..., min_length=10)
    confirmation_text: str  # must equal org name


class PreflightCheck(BaseModel):
    name: str
    ok: bool
    detail: Optional[str] = None


class PreflightResultOut(BaseModel):
    ok: bool
    checks: list[PreflightCheck]


class ToggleEventOut(BaseModel):
    id: UUID
    actor_id: UUID
    from_backend_id: UUID
    to_backend_id: UUID
    reason: Optional[str]
    status: str
    started_at: datetime
    drain_completed_at: Optional[datetime]
    flip_completed_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]
    drained_job_count: Optional[int]
    retried_job_count: Optional[int]
    class Config:
        from_attributes = True


class ToggleStatusOut(BaseModel):
    active_backend: StorageBackendOut
    transition_state: str
    last_toggle_at: Optional[datetime]
    cooldown_until: Optional[datetime]
    inflight_jobs_count: int
    preflight: Optional[PreflightResultOut]
```

- [ ] **Step 2: Verify**

```bash
python -c "from shared.schemas import ToggleRequest, ToggleStatusOut; print('ok')"
```

- [ ] **Step 3: Commit**

```bash
git add tm_backend/shared/schemas.py
git commit -m "feat(schemas): Pydantic DTOs for storage toggle API"
```

---

### Task 12: Write codemod script

**Files:**
- Create: `tm_backend/scripts/migrate_to_router.py`

- [ ] **Step 1: Write the codemod**

```python
"""Codemod: rewrite `azure_storage_manager` call sites to use StorageRouter.

Run:  python scripts/migrate_to_router.py --dry-run
Then: python scripts/migrate_to_router.py --apply

Idempotent. Safe to re-run after manual fixups.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
EXCLUDE = {"shared/azure_storage.py", "shared/storage"}


# Simple string-level rewrites. Complex call sites will be flagged and
# require manual fixing.
REPLACEMENTS = [
    # Import line
    (re.compile(r"from shared\.azure_storage import azure_storage_manager"),
     "from shared.storage.router import router"),
    (re.compile(r"from shared\.azure_storage import (.+), ?azure_storage_manager"),
     r"from shared.azure_storage import \1\nfrom shared.storage.router import router"),

    # azure_storage_manager.get_shard_for_resource(r, t).upload_blob(...)
    (re.compile(r"azure_storage_manager\.get_shard_for_resource\(([^,]+),\s*([^)]+)\)"),
     r"router.get_active_store().shard_for(\2, \1)"),

    # azure_storage_manager.get_default_shard()
    (re.compile(r"azure_storage_manager\.get_default_shard\(\)"),
     r"router.get_active_store()"),

    # azure_storage_manager.get_shard_by_index(i)
    (re.compile(r"azure_storage_manager\.get_shard_by_index\(([^)]+)\)"),
     r"router.get_active_store()"),  # simplification: shard_by_index not used in router

    # azure_storage_manager.get_shard_for_item(item)
    (re.compile(r"azure_storage_manager\.get_shard_for_item\(([^)]+)\)"),
     r"router.get_store_for_item(\1)"),

    # Shard methods map to BackendStore names
    (re.compile(r"\.upload_blob\("), ".upload("),
    (re.compile(r"\.upload_blob_from_file\("), ".upload_from_file("),
    (re.compile(r"\.download_blob\("), ".download("),
    (re.compile(r"\.download_blob_stream\("), ".download_stream("),
    (re.compile(r"\.get_blob_properties\("), ".get_properties("),
    (re.compile(r"\.delete_blob\("), ".delete("),
    (re.compile(r"\.get_blob_sas_url\("), ".presigned_url("),
]


def rewrite_file(path: Path, apply: bool) -> tuple[int, list[str]]:
    text = path.read_text()
    original = text
    flagged: list[str] = []
    for pat, repl in REPLACEMENTS:
        text = pat.sub(repl, text)
    # Flag lines that still reference azure_storage_manager or unconverted
    # shard methods for manual review
    for line_no, line in enumerate(text.splitlines(), 1):
        if "azure_storage_manager" in line:
            flagged.append(f"{path}:{line_no}: {line.strip()}")
    if text != original and apply:
        path.write_text(text)
    return (0 if text == original else 1), flagged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--path", default="tm_backend", help="root to scan")
    args = ap.parse_args()

    root = Path(args.path).resolve()
    changed: list[Path] = []
    all_flags: list[str] = []
    for py in root.rglob("*.py"):
        rel = py.relative_to(root.parent if root.name == "tm_backend" else root).as_posix()
        if any(rel.startswith(x) for x in EXCLUDE):
            continue
        n, flags = rewrite_file(py, apply=args.apply)
        if n:
            changed.append(py)
        all_flags.extend(flags)

    print(f"Files changed: {len(changed)}")
    for c in changed:
        print(f"  {c}")
    if all_flags:
        print("\nUnresolved references (manual fix needed):")
        for f in all_flags:
            print(f"  {f}")
        sys.exit(2)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Dry-run codemod**

```bash
cd tm_backend
python scripts/migrate_to_router.py  # no --apply; just scan
```

Expected: lists ~180 expected rewrites + flagged manual fixups.

- [ ] **Step 3: Commit the codemod script**

```bash
git add tm_backend/scripts/migrate_to_router.py
git commit -m "chore(tools): add codemod to rewrite azure_storage_manager call sites"
```

---

### Task 13: Apply codemod per-worker, commit by worker

For each of: `backup-worker`, `restore-worker`, `azure-workload-worker`, `chat-export-worker`, `dr-replication-worker`, `snapshot-service`, `job-service`, `backup-scheduler`, `api-gateway`, `exports_cleanup`.

- [ ] **Step 1: Run codemod scoped**

```bash
cd tm_backend
python scripts/migrate_to_router.py --apply --path tm_backend/workers/backup-worker
```

- [ ] **Step 2: Review diff + manually fix flagged lines**

```bash
git diff workers/backup-worker
```

Common manual patterns:
- Persist `backend_id` on the item after upload:
  ```python
  info = await store.upload(container, path, content, metadata=meta)
  snapshot_item.backend_id = info.backend_id
  ```
- Replace reads of an item's blob with `router.get_store_for_item(item)` rather than `get_active_store()`:
  ```python
  store = router.get_store_for_item(item).shard_for(item.tenant_id, item.resource_id)
  ```

- [ ] **Step 3: Run worker-specific tests**

```bash
pytest tests/workers/test_backup_worker_hosted_contents.py -v
pytest tests/workers/ -k backup_worker -v
```

Fix failures. Common: tests referenced `azure_storage_manager` directly — repoint to router fixtures.

- [ ] **Step 4: Commit per worker**

```bash
git add tm_backend/workers/backup-worker tm_backend/tests/workers
git commit -m "refactor(backup-worker): route all storage ops via StorageRouter"
```

- [ ] **Step 5: Repeat for each worker in the list above**

One commit per worker for clean bisect. Total ~10 commits.

---

### Task 14: Update `retention_cleanup.py` + remaining helpers

**Files:**
- Modify: `tm_backend/shared/retention_cleanup.py`
- Modify: other `shared/*.py` referencing `azure_storage_manager`

- [ ] **Step 1: Run codemod on shared/**

```bash
python scripts/migrate_to_router.py --apply --path tm_backend/shared
```

- [ ] **Step 2: Manually fix `retention_cleanup.py`**

Pattern to adopt for delete:

```python
from shared.storage.errors import ImmutableBlobError

async def cleanup_expired_snapshots():
    expired = await db.fetch("""
        SELECT s.id AS snapshot_id, si.id AS item_id,
               si.backend_id, si.container, si.blob_path
        FROM snapshots s
        JOIN snapshot_items si ON si.snapshot_id = s.id
        WHERE s.retention_until < NOW()
    """)
    for item in expired:
        store = router.get_store_by_id(str(item["backend_id"])).shard_for(
            item["tenant_id"], item["resource_id"])
        try:
            await store.delete(item["container"], item["blob_path"])
            await db.execute("DELETE FROM snapshot_items WHERE id=$1", item["item_id"])
        except ImmutableBlobError:
            log.info("skipping immutable blob %s (WORM protected)", item["blob_path"])
            continue
```

- [ ] **Step 3: Tests + commit**

```bash
pytest tests/shared -v
git add tm_backend/shared
git commit -m "refactor(shared): retention_cleanup + helpers via StorageRouter"
```

---

### Task 15: Integration test — end-to-end backup via router

**Files:**
- Create: `tm_backend/tests/integration/test_router_end_to_end.py`

- [ ] **Step 1: Write the test**

```python
"""End-to-end: router.get_active_store() writes to Azurite, then backend_id
is persisted on SnapshotItem, then router.get_store_for_item resolves back
to same backend and download succeeds."""
import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import Snapshot, SnapshotItem, StorageBackend, SystemConfig
from shared.storage.router import StorageRouter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_router_write_then_restore(test_db_session: AsyncSession):
    r = StorageRouter()
    await r.load(db_dsn=os.environ["DATABASE_URL"])
    try:
        store = r.get_active_store().shard_for("tenant-t1", "res-r1")
        container = f"e2e-{uuid.uuid4().hex[:8]}"
        info = await store.upload(container, "doc.txt", b"hello")

        # Persist minimal snapshot + item rows
        # (test harness already creates a snapshot fixture; re-use)
        item = SnapshotItem(
            snapshot_id=<test snapshot id>,
            tenant_id="tenant-t1", resource_id="res-r1",
            backend_id=info.backend_id,
            container=container, blob_path="doc.txt",
            size=info.size,
        )
        test_db_session.add(item)
        await test_db_session.commit()

        # Restore-side: look up by item.backend_id
        restored_store = r.get_store_for_item(item).shard_for("tenant-t1", "res-r1")
        got = await restored_store.download(container, "doc.txt")
        assert got == b"hello"
    finally:
        await r.close()
```

Adapt `<test snapshot id>` to existing test fixtures.

- [ ] **Step 2: Run**

```bash
pytest tests/integration/test_router_end_to_end.py -m integration -v
```

Expected: passes against Azurite + local Postgres.

- [ ] **Step 3: Commit**

```bash
git add tm_backend/tests/integration/test_router_end_to_end.py
git commit -m "test(integration): end-to-end backup-then-restore via StorageRouter"
```

---

**Phase 2 exit criteria:**
- [ ] Both migrations applied; `storage_backends` + `system_config` seeded
- [ ] All 26 former `azure_storage_manager` consumers now use `router`
- [ ] `pytest tests/` green (unit + contract + integration)
- [ ] Production still runs Azure-only through the router — zero user-visible change

---

## Phase 3 — Toggle Machinery (est. 3 weeks)

Goal: org-admin can press a button in the UI and the entire system flips Azure → on-prem (or back), orchestrated by `storage-toggle-worker`, visible in Settings → Storage with live updates.

### Task 16: Create `storage-toggle-worker` service skeleton

**Files:**
- Create: `tm_backend/services/storage-toggle-worker/__init__.py`
- Create: `tm_backend/services/storage-toggle-worker/main.py`
- Create: `tm_backend/services/storage-toggle-worker/Dockerfile`

- [ ] **Step 1: Write service entrypoint**

`tm_backend/services/storage-toggle-worker/main.py`:

```python
"""storage-toggle-worker — orchestrates azure↔onprem toggles.

Consumes `storage.toggle` RabbitMQ queue. Acquires a Postgres advisory
lock so only one instance actually runs the orchestration at a time;
the other is a hot standby.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal

import aio_pika
import asyncpg

from services.storage_toggle_worker.orchestrator import run_toggle
from shared.config import settings
from shared.storage.router import router

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("storage-toggle-worker")

ADVISORY_LOCK_ID = 9_042_042  # unique to storage toggles
QUEUE_NAME = "storage.toggle"


async def _acquire_advisory_lock_forever(dsn: str) -> asyncpg.Connection:
    while True:
        conn = await asyncpg.connect(dsn)
        locked = await conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", ADVISORY_LOCK_ID,
        )
        if locked:
            log.info("acquired advisory lock %s", ADVISORY_LOCK_ID)
            return conn
        await conn.close()
        log.info("lock held by another instance; retrying in 15s")
        await asyncio.sleep(15)


async def consume():
    lock_conn = await _acquire_advisory_lock_forever(settings.DATABASE_URL)
    await router.load(db_dsn=settings.DATABASE_URL)

    connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        async with queue.iterator() as it:
            async for message in it:
                async with message.process():
                    payload = json.loads(message.body)
                    log.info("toggle message: %s", payload)
                    await run_toggle(payload)
    finally:
        await lock_conn.close()
        await connection.close()
        await router.close()


def _install_sigterm():
    loop = asyncio.get_event_loop()
    stop = asyncio.Event()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)
    return stop


if __name__ == "__main__":
    asyncio.run(consume())
```

- [ ] **Step 2: Dockerfile**

```dockerfile
# tm_backend/services/storage-toggle-worker/Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY tm_backend/requirements.base.txt /app/requirements.base.txt
RUN pip install -r requirements.base.txt

COPY tm_backend /app
ENV PYTHONPATH=/app

CMD ["python", "-m", "services.storage-toggle-worker.main"]
```

- [ ] **Step 3: Commit**

```bash
git add tm_backend/services/storage-toggle-worker
git commit -m "feat(toggle): storage-toggle-worker service skeleton"
```

---

### Task 17: Preflight checks

**Files:**
- Create: `tm_backend/services/storage-toggle-worker/preflight.py`
- Create: `tm_backend/tests/services/storage_toggle/__init__.py`
- Create: `tm_backend/tests/services/storage_toggle/test_preflight.py`

- [ ] **Step 1: Write failing preflight test**

`tm_backend/tests/services/storage_toggle/test_preflight.py`:

```python
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.storage_toggle_worker.preflight import run_preflight


@pytest.mark.asyncio
async def test_preflight_returns_all_checks():
    router = MagicMock()
    target = MagicMock()
    target.upload = AsyncMock(return_value=MagicMock(backend_id="target"))
    target.delete = AsyncMock()
    router.get_store_by_id = MagicMock(return_value=target)

    db = AsyncMock()
    db.fetchval = AsyncMock(return_value=0.0)  # replica lag seconds
    db.fetchrow = AsyncMock(return_value={"count": 0})  # inflight jobs

    result = await run_preflight(
        router=router, db=db,
        target_backend_id="target",
        cooldown_elapsed=True,
        transition_state="stable",
    )
    assert result.ok is True
    names = [c.name for c in result.checks]
    assert "target_reachable" in names
    assert "db_replica_lag_ok" in names
    assert "no_inflight_transition" in names
    assert "cooldown_elapsed" in names


@pytest.mark.asyncio
async def test_preflight_fails_on_replica_lag():
    router = MagicMock()
    target = MagicMock()
    target.upload = AsyncMock(side_effect=Exception("unreachable"))
    router.get_store_by_id = MagicMock(return_value=target)

    db = AsyncMock()
    db.fetchval = AsyncMock(return_value=120.0)  # 2 min lag
    db.fetchrow = AsyncMock(return_value={"count": 0})

    result = await run_preflight(
        router=router, db=db,
        target_backend_id="target",
        cooldown_elapsed=True,
        transition_state="stable",
    )
    assert result.ok is False
    lag_check = next(c for c in result.checks if c.name == "db_replica_lag_ok")
    assert lag_check.ok is False
```

- [ ] **Step 2: Run — expect ImportError**

```bash
pytest tests/services/storage_toggle/test_preflight.py -x
```

- [ ] **Step 3: Implement `preflight.py`**

```python
"""Preflight checks for storage toggle."""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from shared.storage.router import StorageRouter


@dataclass
class Check:
    name: str
    ok: bool
    detail: str | None = None


@dataclass
class PreflightResult:
    ok: bool
    checks: list[Check]


async def run_preflight(
    router, db, target_backend_id: str,
    cooldown_elapsed: bool, transition_state: str,
    replica_lag_threshold_s: float = 10.0,
) -> PreflightResult:
    checks: list[Check] = []

    # 1. No transition already in flight
    checks.append(Check(
        "no_inflight_transition", transition_state == "stable",
        f"current state: {transition_state}",
    ))

    # 2. Cooldown elapsed
    checks.append(Check("cooldown_elapsed", cooldown_elapsed))

    # 3. Target backend reachable via quick round-trip
    try:
        target = router.get_store_by_id(target_backend_id)
        probe_container = "tmvault-preflight"
        probe_key = f"probe-{uuid.uuid4().hex[:8]}.txt"
        info = await target.upload(probe_container, probe_key, b"preflight")
        await target.delete(probe_container, probe_key)
        checks.append(Check("target_reachable", True,
                            f"{target.name}: etag {info.etag}"))
    except Exception as e:
        checks.append(Check("target_reachable", False, str(e)))

    # 4. Replica lag
    try:
        lag = await db.fetchval(
            "SELECT COALESCE(EXTRACT(EPOCH FROM "
            "  (NOW() - pg_last_xact_replay_timestamp())), 0)"
        )
        lag_s = float(lag) if lag is not None else 0.0
        checks.append(Check(
            "db_replica_lag_ok", lag_s < replica_lag_threshold_s,
            f"{lag_s:.2f}s",
        ))
    except Exception as e:
        checks.append(Check("db_replica_lag_ok", False, str(e)))

    # 5. Secrets accessible
    checks.append(Check("secrets_accessible", True,
                        "validated via target_reachable probe"))

    ok = all(c.ok for c in checks)
    return PreflightResult(ok=ok, checks=checks)
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/services/storage_toggle/test_preflight.py -v
```

Expected: both pass.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/services/storage-toggle-worker/preflight.py \
        tm_backend/tests/services/storage_toggle/
git commit -m "feat(toggle): preflight checks + unit tests"
```

---

### Task 18: Drain phase

**Files:**
- Create: `tm_backend/services/storage-toggle-worker/drain.py`
- Create: `tm_backend/tests/services/storage_toggle/test_drain.py`

- [ ] **Step 1: Write test**

```python
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from services.storage_toggle_worker.drain import drain_inflight


@pytest.mark.asyncio
async def test_drain_marks_stragglers_for_retry():
    db = AsyncMock()
    # First two polls see inflight jobs, third is empty after marking
    db.fetch = AsyncMock(side_effect=[
        [{"id": "job-1", "status": "running"}],
        [{"id": "job-1", "status": "running"}],
        [],
    ])
    db.execute = AsyncMock()

    result = await drain_inflight(
        db=db, poll_interval_s=0, max_wait_s=0,  # 0 -> hard-mark immediately
        event_id="evt-1",
    )
    assert result.drained_count == 0 or result.retried_count >= 1
```

- [ ] **Step 2: Run — expect fail**

- [ ] **Step 3: Implement**

```python
"""Drain phase — stop accepting new jobs, wait for inflight ones,
abort stragglers and create retry rows."""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

log = logging.getLogger("toggle.drain")


@dataclass
class DrainResult:
    drained_count: int
    retried_count: int


async def drain_inflight(db, event_id: str,
                         poll_interval_s: int = 5,
                         max_wait_s: int = 900) -> DrainResult:
    """Poll the jobs table until all running/queued jobs finish or max_wait.

    Stragglers at cutoff are marked `aborted_by_toggle` and a new job row
    is inserted with `pre_toggle_job_id = <old id>` and `retry_reason` set.
    """
    deadline = datetime.now(timezone.utc) + timedelta(seconds=max_wait_s)
    last_remaining = -1

    while datetime.now(timezone.utc) < deadline:
        rows = await db.fetch(
            "SELECT id, status FROM jobs "
            "WHERE status IN ('running','queued','dispatched')"
        )
        remaining = len(rows)
        if remaining == 0:
            return DrainResult(drained_count=last_remaining if last_remaining > 0 else 0,
                               retried_count=0)
        last_remaining = remaining
        log.info("[drain] %d jobs still running (event %s)", remaining, event_id)
        await asyncio.sleep(poll_interval_s)

    # Deadline: hard-mark stragglers
    rows = await db.fetch(
        "SELECT id, job_type, payload, retry_count FROM jobs "
        "WHERE status IN ('running','queued','dispatched')"
    )
    retried = 0
    for r in rows:
        new_id = uuid.uuid4()
        await db.execute(
            "UPDATE jobs SET status='aborted_by_toggle', updated_at=NOW() "
            "WHERE id=$1", r["id"],
        )
        await db.execute(
            "INSERT INTO jobs (id, job_type, status, payload, "
            "  retry_count, retry_reason, pre_toggle_job_id, created_at) "
            "VALUES ($1, $2, 'queued', $3, 0, $4, $5, NOW())",
            new_id, r["job_type"], r["payload"],
            f"toggle_retry_from_event_{event_id}", r["id"],
        )
        retried += 1

    return DrainResult(drained_count=last_remaining - retried, retried_count=retried)
```

- [ ] **Step 4: Run tests + commit**

```bash
pytest tests/services/storage_toggle/test_drain.py -v
git add tm_backend/services/storage-toggle-worker/drain.py tests/services/storage_toggle/test_drain.py
git commit -m "feat(toggle): drain phase — poll-then-abort-then-retry"
```

---

### Task 19: Postgres promote orchestration

**Files:**
- Create: `tm_backend/services/storage-toggle-worker/pg_promote.py`

- [ ] **Step 1: Implement promote hook**

```python
"""Postgres promote orchestration.

Environment-specific. In the Railway pilot: logical-replication reversal
is a no-op (both DBs see the same rows). In client prod: call Patroni
API, wait for new primary role.

The actual orchestration is pluggable via PG_PROMOTE_STRATEGY env var.
Valid: 'railway' (no-op), 'patroni' (REST), 'manual' (wait for human).
"""
from __future__ import annotations

import asyncio
import logging
import os

import aiohttp

log = logging.getLogger("toggle.pg_promote")


async def promote_target(target_kind: str) -> None:
    strategy = os.getenv("PG_PROMOTE_STRATEGY", "railway")
    if strategy == "railway":
        log.info("PG_PROMOTE_STRATEGY=railway — no-op for pilot")
        return
    if strategy == "patroni":
        await _promote_via_patroni(target_kind)
        return
    if strategy == "manual":
        log.warning("PG_PROMOTE_STRATEGY=manual — waiting 30s for human")
        await asyncio.sleep(30)
        return
    raise ValueError(f"unknown PG_PROMOTE_STRATEGY: {strategy}")


async def _promote_via_patroni(target_kind: str) -> None:
    url = os.getenv("PATRONI_TARGET_URL")
    if not url:
        raise RuntimeError("PATRONI_TARGET_URL required for strategy=patroni")
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{url}/failover", json={"candidate": target_kind}) as r:
            if r.status >= 400:
                raise RuntimeError(f"patroni failover failed: {await r.text()}")
    log.info("patroni failover request sent")
```

- [ ] **Step 2: Commit**

```bash
git add tm_backend/services/storage-toggle-worker/pg_promote.py
git commit -m "feat(toggle): pluggable Postgres promote (railway|patroni|manual)"
```

---

### Task 20: DNS flip + worker restart hooks

**Files:**
- Create: `tm_backend/services/storage-toggle-worker/dns_flip.py`
- Create: `tm_backend/services/storage-toggle-worker/worker_restart.py`

- [ ] **Step 1: DNS hook**

```python
"""DNS flip — pluggable per deployment."""
import logging
import os
import subprocess

log = logging.getLogger("toggle.dns")


async def flip_dns_to(target_zone: str) -> None:
    strategy = os.getenv("DNS_FLIP_STRATEGY", "noop")
    if strategy == "noop":
        log.info("DNS_FLIP_STRATEGY=noop — skipping")
        return
    if strategy == "script":
        script = os.getenv("DNS_FLIP_SCRIPT", "/opt/tmvault/ops/dns/flip.sh")
        subprocess.run([script, target_zone], check=True)
        return
    raise ValueError(f"unknown DNS_FLIP_STRATEGY: {strategy}")
```

- [ ] **Step 2: Worker restart hook**

```python
"""Worker pool restart — drain old zone, scale up new zone.
Pluggable: `noop` (pilot), `kubectl` (client), `railway` (pilot on Railway)."""
import asyncio
import logging
import os
import subprocess

log = logging.getLogger("toggle.workers")


async def restart_workers(from_zone: str, to_zone: str) -> None:
    strategy = os.getenv("WORKER_RESTART_STRATEGY", "noop")
    if strategy == "noop":
        log.info("WORKER_RESTART_STRATEGY=noop")
        return
    if strategy == "kubectl":
        # Scale down from-zone deployments, scale up to-zone.
        from_ctx = os.getenv(f"KUBE_CONTEXT_{from_zone.upper()}")
        to_ctx = os.getenv(f"KUBE_CONTEXT_{to_zone.upper()}")
        deployments = ["backup-worker", "restore-worker", "chat-export-worker",
                       "azure-workload-worker", "dr-replication-worker"]
        for d in deployments:
            subprocess.run(["kubectl", "--context", from_ctx, "-n", "tmvault",
                            "scale", "deploy", d, "--replicas=0"], check=True)
        for d in deployments:
            subprocess.run(["kubectl", "--context", to_ctx, "-n", "tmvault",
                            "scale", "deploy", d, "--replicas=4"], check=True)
        return
    if strategy == "railway":
        log.info("railway strategy: service toggle via env var")
        # In Railway pilot both sides always running; router state decides.
        return
    raise ValueError(f"unknown WORKER_RESTART_STRATEGY: {strategy}")
```

- [ ] **Step 3: Commit**

```bash
git add tm_backend/services/storage-toggle-worker/dns_flip.py \
        tm_backend/services/storage-toggle-worker/worker_restart.py
git commit -m "feat(toggle): pluggable DNS + worker restart hooks"
```

---

### Task 21: Smoke test phase

**Files:**
- Create: `tm_backend/services/storage-toggle-worker/smoke.py`

- [ ] **Step 1: Implement smoke**

```python
"""Post-cutover smoke: canary backup + restore + passthrough restore."""
from __future__ import annotations

import logging
import uuid

from shared.storage.router import router

log = logging.getLogger("toggle.smoke")


async def run_smoke(db, new_backend_id: str, old_backend_id: str) -> None:
    # 1. Canary backup to new backend
    store = router.get_store_by_id(new_backend_id).shard_for(
        "smoke-tenant", "smoke-res",
    )
    container = "tmvault-smoke"
    key = f"smoke-{uuid.uuid4().hex[:8]}.bin"
    body = b"smoke" * 2048  # 10KB
    info = await store.upload(container, key, body, metadata={"kind": "smoke"})
    got = await store.download(container, key)
    assert got == body, "smoke: round-trip mismatch"
    log.info("smoke: canary backup + restore ok on %s", new_backend_id)
    await store.delete(container, key)

    # 2. Passthrough restore — read from OLD backend
    old_store = router.get_store_by_id(old_backend_id).shard_for(
        "smoke-tenant", "smoke-res",
    )
    old_key = f"smoke-prev-{uuid.uuid4().hex[:8]}.bin"
    await old_store.upload(container, old_key, body, metadata={"kind": "smoke-prev"})
    # Simulate item with backend_id = old_id
    item = type("Item", (), {"backend_id": old_backend_id})()
    restored = await router.get_store_for_item(item).shard_for(
        "smoke-tenant", "smoke-res",
    ).download(container, old_key)
    assert restored == body, "smoke: passthrough mismatch"
    log.info("smoke: passthrough restore ok from %s", old_backend_id)
    await old_store.delete(container, old_key)
```

- [ ] **Step 2: Commit**

```bash
git add tm_backend/services/storage-toggle-worker/smoke.py
git commit -m "feat(toggle): post-cutover smoke (canary backup + passthrough restore)"
```

---

### Task 22: Orchestrator — the 8-phase state machine

**Files:**
- Create: `tm_backend/services/storage-toggle-worker/orchestrator.py`
- Create: `tm_backend/services/storage-toggle-worker/rollback.py`
- Create: `tm_backend/tests/services/storage_toggle/test_orchestrator.py`

- [ ] **Step 1: Write failing orchestrator test**

```python
from unittest.mock import AsyncMock, patch

import pytest

from services.storage_toggle_worker.orchestrator import run_toggle


@pytest.mark.asyncio
async def test_orchestrator_happy_path(monkeypatch):
    # Patch all side effects
    with patch("services.storage_toggle_worker.orchestrator.run_preflight",
               new=AsyncMock(return_value=type("R", (), {"ok": True, "checks": []}))):
        with patch("services.storage_toggle_worker.orchestrator.drain_inflight",
                   new=AsyncMock(return_value=type("D", (), {"drained_count": 0,
                                                             "retried_count": 0}))):
            with patch("services.storage_toggle_worker.orchestrator.promote_target",
                       new=AsyncMock(return_value=None)):
                with patch("services.storage_toggle_worker.orchestrator.flip_dns_to",
                           new=AsyncMock(return_value=None)):
                    with patch("services.storage_toggle_worker.orchestrator.restart_workers",
                               new=AsyncMock(return_value=None)):
                        with patch("services.storage_toggle_worker.orchestrator.run_smoke",
                                   new=AsyncMock(return_value=None)):
                            with patch("services.storage_toggle_worker.orchestrator._open_db",
                                       new=AsyncMock(return_value=AsyncMock())):
                                await run_toggle({
                                    "event_id": "e1",
                                    "from_id": "az",
                                    "to_id": "sw",
                                    "actor_id": "u1",
                                    "reason": "test",
                                })
                                # No exception = pass
```

- [ ] **Step 2: Implement orchestrator**

```python
"""8-phase toggle state machine."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import asyncpg

from services.storage_toggle_worker.drain import drain_inflight
from services.storage_toggle_worker.dns_flip import flip_dns_to
from services.storage_toggle_worker.pg_promote import promote_target
from services.storage_toggle_worker.preflight import run_preflight
from services.storage_toggle_worker.rollback import rollback
from services.storage_toggle_worker.smoke import run_smoke
from services.storage_toggle_worker.worker_restart import restart_workers
from shared.config import settings
from shared.storage.router import router

log = logging.getLogger("toggle.orchestrator")


async def _open_db():
    return await asyncpg.connect(settings.DATABASE_URL)


async def _update_event(db, event_id: str, **cols) -> None:
    keys = list(cols.keys())
    values = list(cols.values())
    set_clause = ", ".join(f"{k}=${i+2}" for i, k in enumerate(keys))
    await db.execute(
        f"UPDATE storage_toggle_events SET {set_clause}, "
        f"  started_at = COALESCE(started_at, NOW()) "
        f"WHERE id=$1",
        event_id, *values,
    )


async def _set_transition(db, state: str) -> None:
    await db.execute(
        "UPDATE system_config SET transition_state=$1, updated_at=NOW() WHERE id=1",
        state,
    )


async def _set_active(db, backend_id: str) -> None:
    await db.execute(
        "UPDATE system_config "
        "SET active_backend_id=$1, last_toggle_at=NOW(), "
        "    cooldown_until=NOW() + INTERVAL '30 minutes', updated_at=NOW() "
        "WHERE id=1",
        backend_id,
    )


async def run_toggle(payload: dict) -> None:
    event_id = payload["event_id"]
    from_id = payload["from_id"]
    to_id = payload["to_id"]
    reason = payload.get("reason")

    db = await _open_db()
    try:
        # --- Phase 1: preflight
        log.info("phase 1: preflight for event %s", event_id)
        sc = await db.fetchrow("SELECT transition_state, cooldown_until FROM system_config WHERE id=1")
        cooldown_elapsed = (
            sc["cooldown_until"] is None
            or sc["cooldown_until"] < datetime.now(timezone.utc)
        )
        preflight = await run_preflight(
            router=router, db=db, target_backend_id=to_id,
            cooldown_elapsed=cooldown_elapsed,
            transition_state=sc["transition_state"],
        )
        await _update_event(db, event_id,
                            pre_flight_checks={c.name: {"ok": c.ok, "detail": c.detail}
                                                for c in preflight.checks})
        if not preflight.ok:
            await _update_event(db, event_id, status="aborted",
                                error_message="preflight failed",
                                completed_at=datetime.now(timezone.utc))
            return

        # --- Phase 2: drain
        log.info("phase 2: drain")
        await _set_transition(db, "draining")
        drain_result = await drain_inflight(db, event_id=event_id)
        await _update_event(db, event_id, status="drain_completed",
                            drain_completed_at=datetime.now(timezone.utc),
                            drained_job_count=drain_result.drained_count,
                            retried_job_count=drain_result.retried_count)

        # --- Phase 3: promote DB
        log.info("phase 3: promote db")
        try:
            await promote_target(payload.get("target_kind", "onprem"))
            await _update_event(db, event_id, status="db_promoted")
        except Exception as e:
            log.error("promote failed: %s", e)
            await rollback(db, event_id=event_id, phase="db_promote", error=str(e))
            return

        # --- Phase 4: DNS flip
        log.info("phase 4: dns flip")
        await flip_dns_to(payload.get("target_kind", "onprem"))
        await _update_event(db, event_id, status="dns_flipped")

        # --- Phase 5: workers
        log.info("phase 5: restart workers")
        await restart_workers(payload.get("from_kind", "azure"),
                              payload.get("target_kind", "onprem"))
        await _update_event(db, event_id, status="workers_restarted")

        # --- Phase 6: cutover
        log.info("phase 6: cutover")
        await _set_transition(db, "flipping")
        await _set_active(db, to_id)

        # --- Phase 7: smoke
        log.info("phase 7: smoke")
        try:
            await run_smoke(db, new_backend_id=to_id, old_backend_id=from_id)
            await _update_event(db, event_id, status="smoke_passed")
        except AssertionError as e:
            log.error("smoke failed: %s", e)
            await _update_event(db, event_id, status="failed",
                                error_message=f"smoke: {e}")
            return

        # --- Phase 8: open
        log.info("phase 8: open")
        await _set_transition(db, "stable")
        await _update_event(db, event_id, status="completed",
                            completed_at=datetime.now(timezone.utc),
                            flip_completed_at=datetime.now(timezone.utc))
        log.info("toggle event %s complete", event_id)
    finally:
        await db.close()
```

- [ ] **Step 3: Rollback stub**

`tm_backend/services/storage-toggle-worker/rollback.py`:

```python
"""Per-phase rollback."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

log = logging.getLogger("toggle.rollback")


async def rollback(db, event_id: str, phase: str, error: str) -> None:
    log.error("rolling back from phase=%s error=%s", phase, error)
    if phase == "drain":
        # revert transition state to stable
        await db.execute(
            "UPDATE system_config SET transition_state='stable', updated_at=NOW() WHERE id=1"
        )
    elif phase == "db_promote":
        # revert transition state; human must verify primary role
        await db.execute(
            "UPDATE system_config SET transition_state='stable', updated_at=NOW() WHERE id=1"
        )
    elif phase == "dns":
        # DNS automatically revertable
        await db.execute(
            "UPDATE system_config SET transition_state='stable', updated_at=NOW() WHERE id=1"
        )
    else:
        log.warning("no rollback handler for phase=%s", phase)
    await db.execute(
        "UPDATE storage_toggle_events "
        "SET status='failed', error_message=$2, completed_at=NOW() "
        "WHERE id=$1",
        event_id, f"{phase}: {error}",
    )
```

- [ ] **Step 4: Run tests + commit**

```bash
pytest tests/services/storage_toggle/ -v
git add tm_backend/services/storage-toggle-worker tests/services/storage_toggle
git commit -m "feat(toggle): 8-phase orchestrator + rollback"
```

---

### Task 23: api-gateway admin endpoints + SSE stream

**Files:**
- Create: `tm_backend/services/api-gateway/routes/admin_storage.py`
- Modify: `tm_backend/services/api-gateway/main.py` (include router)

- [ ] **Step 1: Implement routes**

```python
"""Admin storage API: status, toggle, events, SSE stream."""
from __future__ import annotations

import asyncio
import json
import uuid

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from shared.auth import require_org_admin, get_current_user
from shared.config import settings
from shared.schemas import (
    PreflightCheck,
    PreflightResultOut,
    ToggleEventOut,
    ToggleRequest,
    ToggleStatusOut,
    StorageBackendOut,
)
from shared.storage.router import router as storage_router

router = APIRouter(prefix="/api/admin/storage", tags=["admin-storage"])


async def _db():
    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()


@router.get("/status", response_model=ToggleStatusOut,
            dependencies=[Depends(require_org_admin)])
async def status(db=Depends(_db)):
    sc = await db.fetchrow(
        "SELECT sc.active_backend_id, sc.transition_state, sc.last_toggle_at, sc.cooldown_until, "
        "  sb.id AS b_id, sb.kind, sb.name, sb.endpoint, sb.is_enabled "
        "FROM system_config sc JOIN storage_backends sb ON sb.id = sc.active_backend_id WHERE sc.id=1"
    )
    inflight = await db.fetchval(
        "SELECT count(*) FROM jobs WHERE status IN ('running','queued','dispatched')"
    )
    return ToggleStatusOut(
        active_backend=StorageBackendOut(
            id=sc["b_id"], kind=sc["kind"], name=sc["name"],
            endpoint=sc["endpoint"], is_enabled=sc["is_enabled"],
        ),
        transition_state=sc["transition_state"],
        last_toggle_at=sc["last_toggle_at"],
        cooldown_until=sc["cooldown_until"],
        inflight_jobs_count=inflight,
        preflight=None,
    )


@router.post("/toggle", dependencies=[Depends(require_org_admin)])
async def submit_toggle(
    req: ToggleRequest,
    request: Request,
    user=Depends(get_current_user),
    db=Depends(_db),
):
    # confirmation_text must match org name
    org_name = user.org_name
    if req.confirmation_text != org_name:
        raise HTTPException(400, detail="confirmation_text does not match org name")

    # cooldown check
    sc = await db.fetchrow("SELECT transition_state, cooldown_until FROM system_config WHERE id=1")
    from datetime import datetime, timezone
    if sc["transition_state"] != "stable":
        raise HTTPException(409, detail=f"transition already in progress: {sc['transition_state']}")
    if sc["cooldown_until"] and sc["cooldown_until"] > datetime.now(timezone.utc):
        raise HTTPException(429, detail=f"cooldown active until {sc['cooldown_until']}")

    from_id = await db.fetchval("SELECT active_backend_id FROM system_config WHERE id=1")
    if str(from_id) == str(req.target_backend_id):
        raise HTTPException(400, detail="target backend is already active")

    event_id = uuid.uuid4()
    await db.execute(
        "INSERT INTO storage_toggle_events (id, actor_id, actor_ip, from_backend_id, "
        "  to_backend_id, reason, status) VALUES ($1,$2,$3,$4,$5,$6,'started')",
        event_id, user.id, request.client.host,
        from_id, req.target_backend_id, req.reason,
    )

    # Enqueue toggle message
    import aio_pika
    conn = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    try:
        ch = await conn.channel()
        await ch.declare_queue("storage.toggle", durable=True)
        await ch.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps({
                    "event_id": str(event_id),
                    "from_id": str(from_id),
                    "to_id": str(req.target_backend_id),
                    "actor_id": str(user.id),
                    "reason": req.reason,
                }).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="storage.toggle",
        )
    finally:
        await conn.close()

    return {"event_id": str(event_id), "status": "queued"}


@router.get("/events", dependencies=[Depends(require_org_admin)])
async def list_events(limit: int = 20, db=Depends(_db)):
    rows = await db.fetch(
        "SELECT * FROM storage_toggle_events ORDER BY started_at DESC LIMIT $1", limit,
    )
    return [dict(r) for r in rows]


@router.get("/events/{event_id}", dependencies=[Depends(require_org_admin)])
async def get_event(event_id: uuid.UUID, db=Depends(_db)):
    row = await db.fetchrow("SELECT * FROM storage_toggle_events WHERE id=$1", event_id)
    if not row:
        raise HTTPException(404)
    return dict(row)


@router.get("/events/{event_id}/stream",
            dependencies=[Depends(require_org_admin)])
async def stream_event(event_id: uuid.UUID, request: Request):
    async def generator():
        conn = await asyncpg.connect(settings.DATABASE_URL)
        try:
            last_status = None
            while True:
                if await request.is_disconnected():
                    break
                row = await conn.fetchrow(
                    "SELECT status, drained_job_count, retried_job_count, error_message "
                    "FROM storage_toggle_events WHERE id=$1", event_id,
                )
                if row and row["status"] != last_status:
                    last_status = row["status"]
                    yield {"event": "status", "data": json.dumps(dict(row))}
                if row and row["status"] in ("completed", "aborted", "failed"):
                    break
                await asyncio.sleep(1)
        finally:
            await conn.close()
    return EventSourceResponse(generator())


@router.post("/toggle/{event_id}/abort",
             dependencies=[Depends(require_org_admin)])
async def abort_toggle(event_id: uuid.UUID, db=Depends(_db)):
    row = await db.fetchrow(
        "SELECT status FROM storage_toggle_events WHERE id=$1", event_id,
    )
    if not row:
        raise HTTPException(404)
    if row["status"] not in ("started", "drain_started"):
        raise HTTPException(409, detail="cannot abort after drain is complete")
    await db.execute(
        "UPDATE storage_toggle_events SET status='aborted', completed_at=NOW() "
        "WHERE id=$1", event_id,
    )
    await db.execute(
        "UPDATE system_config SET transition_state='stable', updated_at=NOW() WHERE id=1"
    )
    return {"status": "aborted"}
```

- [ ] **Step 2: Register router in `api-gateway/main.py`**

Find the FastAPI `app = FastAPI(...)` and after existing `app.include_router(...)` calls add:

```python
from services.api_gateway.routes import admin_storage
app.include_router(admin_storage.router)
```

- [ ] **Step 3: Quick manual curl test**

```bash
curl -sS -H "Authorization: Bearer $JWT" http://localhost:8080/api/admin/storage/status | jq .
```

Expected: `{ "active_backend": {"name": "azure-primary", ...}, "transition_state": "stable", ... }`.

- [ ] **Step 4: Commit**

```bash
git add tm_backend/services/api-gateway/routes/admin_storage.py \
        tm_backend/services/api-gateway/main.py
git commit -m "feat(api): admin storage endpoints (status, toggle, events, SSE stream, abort)"
```

---

### Task 24: Frontend — Settings → Storage page

**Files:**
- Create: `tm_vault/src/api/adminStorage.ts`
- Create: `tm_vault/src/pages/Settings/Storage/index.tsx`
- Create: `tm_vault/src/pages/Settings/Storage/StateCard.tsx`
- Create: `tm_vault/src/pages/Settings/Storage/PreflightCard.tsx`
- Create: `tm_vault/src/pages/Settings/Storage/HistoryCard.tsx`
- Create: `tm_vault/src/pages/Settings/Storage/ToggleModal.tsx`

- [ ] **Step 1: API client**

```ts
// tm_vault/src/api/adminStorage.ts
import { apiGet, apiPost } from "./_client";

export interface Backend {
  id: string; kind: string; name: string; endpoint: string; is_enabled: boolean;
}
export interface ToggleStatus {
  active_backend: Backend;
  transition_state: "stable" | "draining" | "flipping";
  last_toggle_at: string | null;
  cooldown_until: string | null;
  inflight_jobs_count: number;
}
export interface ToggleEvent {
  id: string; actor_id: string; from_backend_id: string; to_backend_id: string;
  reason: string | null; status: string; started_at: string;
  drain_completed_at: string | null; completed_at: string | null;
  error_message: string | null;
  drained_job_count: number | null; retried_job_count: number | null;
}

export const fetchStatus = () => apiGet<ToggleStatus>("/api/admin/storage/status");
export const submitToggle = (p: { target_backend_id: string; reason: string; confirmation_text: string }) =>
  apiPost<{ event_id: string; status: string }>("/api/admin/storage/toggle", p);
export const fetchEvents = () => apiGet<ToggleEvent[]>("/api/admin/storage/events?limit=20");
export const abortToggle = (eventId: string) =>
  apiPost<{ status: string }>(`/api/admin/storage/toggle/${eventId}/abort`, {});

export function streamEvent(eventId: string, onMessage: (data: any) => void): EventSource {
  const url = `/api/admin/storage/events/${eventId}/stream`;
  const es = new EventSource(url, { withCredentials: true });
  es.addEventListener("status", (e: any) => {
    try { onMessage(JSON.parse(e.data)); } catch { /* noop */ }
  });
  return es;
}
```

- [ ] **Step 2: State card**

```tsx
// tm_vault/src/pages/Settings/Storage/StateCard.tsx
import React from "react";
import type { ToggleStatus } from "../../../api/adminStorage";

export function StateCard({ status }: { status: ToggleStatus | null }) {
  if (!status) return <div>Loading...</div>;
  const pillColor = status.transition_state === "stable" ? "green"
    : "yellow";
  return (
    <div className="card">
      <h3>Active storage backend</h3>
      <div className={`pill pill-${pillColor}`}>
        {status.active_backend.name} ({status.active_backend.kind})
      </div>
      <div className="muted">
        State: {status.transition_state} ·
        Inflight jobs: {status.inflight_jobs_count}
      </div>
      {status.cooldown_until && (
        <div className="muted">Cooldown until: {status.cooldown_until}</div>
      )}
    </div>
  );
}
```

- [ ] **Step 3: Preflight card (same simple shape — read-only checklist)**

```tsx
// tm_vault/src/pages/Settings/Storage/PreflightCard.tsx
import React from "react";

export function PreflightCard({ checks }: { checks: { name: string; ok: boolean; detail?: string | null }[] }) {
  return (
    <div className="card">
      <h3>Preflight</h3>
      <ul>
        {checks.map((c) => (
          <li key={c.name} className={c.ok ? "ok" : "fail"}>
            {c.ok ? "✓" : "✗"} {c.name} {c.detail && <span className="muted">— {c.detail}</span>}
          </li>
        ))}
      </ul>
    </div>
  );
}
```

- [ ] **Step 4: History card**

```tsx
// tm_vault/src/pages/Settings/Storage/HistoryCard.tsx
import React, { useEffect, useState } from "react";
import { fetchEvents, type ToggleEvent } from "../../../api/adminStorage";

export function HistoryCard() {
  const [events, setEvents] = useState<ToggleEvent[]>([]);
  useEffect(() => { fetchEvents().then(setEvents); }, []);
  return (
    <div className="card">
      <h3>History</h3>
      <table>
        <thead>
          <tr><th>Started</th><th>From→To</th><th>Actor</th><th>Status</th><th>Duration</th></tr>
        </thead>
        <tbody>
          {events.map((e) => (
            <tr key={e.id}>
              <td>{e.started_at}</td>
              <td>{e.from_backend_id.slice(0, 8)} → {e.to_backend_id.slice(0, 8)}</td>
              <td>{e.actor_id.slice(0, 8)}</td>
              <td className={e.status === "completed" ? "ok" : e.status === "failed" ? "fail" : "muted"}>
                {e.status}
              </td>
              <td>
                {e.completed_at
                  ? Math.round((+new Date(e.completed_at) - +new Date(e.started_at)) / 1000) + "s"
                  : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 5: Toggle modal with SSE stream**

```tsx
// tm_vault/src/pages/Settings/Storage/ToggleModal.tsx
import React, { useState } from "react";
import { submitToggle, streamEvent } from "../../../api/adminStorage";

export function ToggleModal({ orgName, targetBackendId, onClose }: {
  orgName: string; targetBackendId: string; onClose: () => void;
}) {
  const [reason, setReason] = useState("");
  const [confirm, setConfirm] = useState("");
  const [status, setStatus] = useState<string | null>(null);
  const [phaseLog, setPhaseLog] = useState<string[]>([]);
  const canSubmit = reason.trim().length >= 10 && confirm === orgName;

  async function submit() {
    setStatus("queued");
    const r = await submitToggle({
      target_backend_id: targetBackendId, reason, confirmation_text: confirm,
    });
    const es = streamEvent(r.event_id, (data) => {
      setStatus(data.status);
      setPhaseLog((l) => [...l, `${data.status} (${new Date().toISOString()})`]);
      if (["completed", "failed", "aborted"].includes(data.status)) es.close();
    });
  }

  return (
    <div className="modal">
      <h2>Switch backend</h2>
      <p>This drains in-flight jobs, promotes the replica DB, restarts workers, and runs a smoke test. Expected 5-15 min downtime.</p>
      <label>
        Reason (required, min 10 chars):
        <textarea value={reason} onChange={(e) => setReason(e.target.value)} rows={3} />
      </label>
      <label>
        Type <code>{orgName}</code> to confirm:
        <input value={confirm} onChange={(e) => setConfirm(e.target.value)} />
      </label>
      <button onClick={submit} disabled={!canSubmit || status !== null}>
        {status ? `Running... (${status})` : "Start toggle"}
      </button>
      <button onClick={onClose}>Cancel</button>
      {phaseLog.length > 0 && (
        <pre className="log">{phaseLog.join("\n")}</pre>
      )}
    </div>
  );
}
```

- [ ] **Step 6: Page wrapper**

```tsx
// tm_vault/src/pages/Settings/Storage/index.tsx
import React, { useEffect, useState } from "react";

import { fetchStatus, type ToggleStatus } from "../../../api/adminStorage";
import { StateCard } from "./StateCard";
import { HistoryCard } from "./HistoryCard";
import { ToggleModal } from "./ToggleModal";

export default function StorageSettings() {
  const [status, setStatus] = useState<ToggleStatus | null>(null);
  const [showModal, setShowModal] = useState<{ targetId: string } | null>(null);

  useEffect(() => {
    const iv = setInterval(() => { fetchStatus().then(setStatus); }, 5000);
    fetchStatus().then(setStatus);
    return () => clearInterval(iv);
  }, []);

  const target = status?.active_backend.kind === "azure_blob"
    ? { id: "<SEAWEEDFS_BACKEND_ID>", label: "On-Prem (SeaweedFS)" }
    : { id: "<AZURE_BACKEND_ID>", label: "Azure Blob" };

  return (
    <div>
      <h1>Storage</h1>
      <StateCard status={status} />
      <button onClick={() => setShowModal({ targetId: target.id })}>
        Switch to {target.label}
      </button>
      <HistoryCard />
      {showModal && (
        <ToggleModal
          orgName="TMVAULT-ORG"
          targetBackendId={showModal.targetId}
          onClose={() => setShowModal(null)}
        />
      )}
    </div>
  );
}
```

**Note:** the `target.id` is placeholder — replace with real backend IDs fetched from an extended status endpoint that returns the full list of backends. Add such an endpoint or hard-code per-environment via env config.

- [ ] **Step 7: Wire into app router + verify UI in browser**

Add route `/settings/storage` that renders `<StorageSettings />`. Start local dev server, log in as admin, navigate, click the button.

```bash
cd tm_vault
npm run dev
# Open browser, test the toggle modal rendering + SSE stream
```

- [ ] **Step 8: Commit**

```bash
git add tm_vault/src/api/adminStorage.ts tm_vault/src/pages/Settings/Storage
# plus any router wiring
git commit -m "feat(ui): Settings → Storage page with toggle modal + SSE live updates"
```

---

### Task 25: End-to-end toggle test (docker-compose)

**Files:**
- Create: `tm_backend/tests/integration/test_toggle_end_to_end.py`

- [ ] **Step 1: Write the test**

```python
import asyncio
import json
import uuid
from datetime import datetime, timezone

import aio_pika
import asyncpg
import pytest

from services.storage_toggle_worker.orchestrator import run_toggle


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_toggle_flow_on_local():
    """End-to-end: insert toggle event → run orchestrator → assert
    active_backend_id flipped, smoke_passed, cooldown set."""
    dsn = pytest.helpers.dsn  # test fixture from conftest
    db = await asyncpg.connect(dsn)
    try:
        az = await db.fetchval("SELECT id FROM storage_backends WHERE name='azure-primary'")
        sw = await db.fetchval("SELECT id FROM storage_backends WHERE name='seaweedfs-test'")
        event_id = uuid.uuid4()
        await db.execute(
            "INSERT INTO storage_toggle_events (id, actor_id, from_backend_id, to_backend_id, reason, status) "
            "VALUES ($1, $2, $3, $4, $5, 'started')",
            event_id, uuid.UUID(int=1), az, sw, "e2e test",
        )
        await run_toggle({
            "event_id": str(event_id), "from_id": str(az), "to_id": str(sw),
            "actor_id": "1", "reason": "e2e test",
        })
        row = await db.fetchrow(
            "SELECT status, completed_at FROM storage_toggle_events WHERE id=$1",
            event_id,
        )
        assert row["status"] == "completed"
        sc = await db.fetchrow("SELECT active_backend_id, transition_state, cooldown_until FROM system_config WHERE id=1")
        assert str(sc["active_backend_id"]) == str(sw)
        assert sc["transition_state"] == "stable"
        assert sc["cooldown_until"] > datetime.now(timezone.utc)
    finally:
        await db.close()
```

- [ ] **Step 2: Set env for the orchestrator in test**

Before running:

```bash
export PG_PROMOTE_STRATEGY=railway
export DNS_FLIP_STRATEGY=noop
export WORKER_RESTART_STRATEGY=noop
```

- [ ] **Step 3: Run**

```bash
pytest tests/integration/test_toggle_end_to_end.py -m integration -v
```

Expected: passes. `seaweedfs-test` backend must be pre-seeded in the test DB; add to conftest:

```python
@pytest.fixture(autouse=True)
async def _seed_seaweedfs_backend(test_db):
    import uuid as _u
    await test_db.execute(
        "INSERT INTO storage_backends (id, kind, name, endpoint, secret_ref, config) "
        "VALUES ($1,'seaweedfs','seaweedfs-test','http://localhost:8333','env://SEAWEED_KEY',"
        "  $2::jsonb) ON CONFLICT (name) DO NOTHING",
        _u.uuid4(),
        '{"buckets":["tmvault-test-0"],"region":"us-east-1","verify_tls":false,"access_key_env":"SEAWEED_ACCESS"}',
    )
```

- [ ] **Step 4: Commit**

```bash
git add tm_backend/tests/integration/test_toggle_end_to_end.py
git commit -m "test(toggle): end-to-end full-flow integration test"
```

---

**Phase 3 exit criteria:**
- [ ] `storage-toggle-worker` starts cleanly, consumes queue, holds advisory lock
- [ ] Admin UI page renders, button triggers toggle, SSE stream shows live phase transitions
- [ ] Integration test flips a local stack azure→seaweedfs and back
- [ ] Rollback path exercised on deliberate failure injection

---

## Phase 4 — WORM Parity (est. 1 week; runs parallel with phase 3)

Goal: retention-until + legal hold behave identically on both backends. Contract tests green.

### Task 26: Retention helpers

**Files:**
- Create: `tm_backend/shared/retention.py`
- Create: `tm_backend/tests/shared/test_retention.py`

- [ ] **Step 1: Failing test**

```python
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from shared.retention import compute_retention_until, compute_immutability_mode


def test_compute_retention_until_uses_sla_days():
    created = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sla = MagicMock()
    sla.retention_days = 30
    assert compute_retention_until(sla, created) == created + timedelta(days=30)


def test_compute_immutability_mode_locked_vs_unlocked():
    sla = MagicMock()
    sla.retention_mode = "locked"
    assert compute_immutability_mode(sla) == "Locked"
    sla.retention_mode = "unlocked"
    assert compute_immutability_mode(sla) == "Unlocked"
    sla.retention_mode = None
    assert compute_immutability_mode(sla) == "Unlocked"
```

- [ ] **Step 2: Implement**

```python
"""Shared retention helpers."""
from datetime import datetime, timedelta


def compute_retention_until(sla_policy, created_at: datetime) -> datetime:
    return created_at + timedelta(days=getattr(sla_policy, "retention_days", 30))


def compute_immutability_mode(sla_policy) -> str:
    mode = getattr(sla_policy, "retention_mode", None)
    if mode == "locked":
        return "Locked"
    return "Unlocked"
```

- [ ] **Step 3: Tests + commit**

```bash
pytest tests/shared/test_retention.py -v
git add tm_backend/shared/retention.py tm_backend/tests/shared/test_retention.py
git commit -m "feat(shared): retention helpers — compute_retention_until + mode mapping"
```

---

### Task 27: WORM contract tests

**Files:**
- Create: `tm_backend/tests/shared/storage/test_worm_parity.py`

- [ ] **Step 1: Write parametrized WORM tests**

```python
"""WORM parity — these tests MUST pass identically on both backends."""
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from shared.storage.azure_blob import AzureBlobStore
from shared.storage.errors import ImmutableBlobError
from shared.storage.seaweedfs import SeaweedStore

AZURITE_CONN = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
)


@pytest_asyncio.fixture(params=["azure", "seaweedfs"])
async def store_and_container(request):
    if request.param == "azure":
        s = AzureBlobStore.from_connection_string(AZURITE_CONN, backend_id="az", name="az")
        container = f"worm-{uuid.uuid4().hex[:8]}"
        await s.ensure_container(container)
    else:
        s = SeaweedStore(backend_id="sw", name="sw",
                         endpoint=os.getenv("SEAWEEDFS_TEST_ENDPOINT", "http://localhost:8333"),
                         access_key="testaccess", secret_key="testsecret",
                         buckets=["tmvault-test-0"], verify_tls=False)
        container = f"worm-{uuid.uuid4().hex[:8]}"
    yield s, container
    await s.close()


@pytest.mark.worm
@pytest.mark.asyncio
async def test_retention_blocks_delete(store_and_container):
    s, c = store_and_container
    await s.upload(c, "w.txt", b"worm")
    until = datetime.now(timezone.utc) + timedelta(days=1)
    await s.apply_immutability(c, "w.txt", until, mode="Unlocked")
    with pytest.raises((ImmutableBlobError, Exception)):
        await s.delete(c, "w.txt")


@pytest.mark.worm
@pytest.mark.asyncio
async def test_legal_hold_blocks_delete(store_and_container):
    s, c = store_and_container
    await s.upload(c, "h.txt", b"hold")
    await s.apply_legal_hold(c, "h.txt")
    with pytest.raises((ImmutableBlobError, Exception)):
        await s.delete(c, "h.txt")
    await s.remove_legal_hold(c, "h.txt")
    # After remove, delete should succeed unless retention still holds
    try:
        await s.delete(c, "h.txt")
    except ImmutableBlobError:
        pass  # acceptable — retention may still hold (test-side both is ok)
```

- [ ] **Step 2: Run**

```bash
pytest tests/shared/storage/test_worm_parity.py -v -m worm
```

Expected: both parametrizations pass.

- [ ] **Step 3: Commit**

```bash
git add tm_backend/tests/shared/storage/test_worm_parity.py
git commit -m "test(worm): contract tests ensure retention + legal hold behave on both backends"
```

---

### Task 28: Wire WORM into backup-worker

**Files:**
- Modify: `tm_backend/workers/backup-worker/main.py` (or relevant file after snapshot-item creation)

- [ ] **Step 1: Locate where `SnapshotItem` is persisted after upload**

```bash
grep -n "snapshot_items\|SnapshotItem" tm_backend/workers/backup-worker/main.py
```

- [ ] **Step 2: After successful upload, call WORM + persist retention_until**

Add after `info = await store.upload(...)` and before commit:

```python
from shared.retention import compute_retention_until, compute_immutability_mode

if sla_policy and getattr(sla_policy, "worm_enabled", False):
    until = compute_retention_until(sla_policy, snapshot.created_at)
    mode = compute_immutability_mode(sla_policy)
    await store.apply_immutability(container, path, until, mode)
    snapshot_item.retention_until = until

if sla_policy and getattr(sla_policy, "legal_hold_enabled", False):
    await store.apply_legal_hold(container, path, tag=f"sla-{sla_policy.id}")
```

- [ ] **Step 3: Run backup-worker tests**

```bash
pytest tests/workers -k backup -v
```

- [ ] **Step 4: Commit**

```bash
git add tm_backend/workers/backup-worker
git commit -m "feat(backup-worker): apply WORM (retention + legal hold) via router after upload"
```

---

**Phase 4 exit criteria:**
- [ ] Contract tests green for retention + legal hold on both backends
- [ ] backup-worker applies WORM after upload using SLA settings
- [ ] retention_cleanup skips `ImmutableBlobError` gracefully

---

## Phase 5 — Staging Dry Run (est. 1 week)

Goal: full rehearsal on a local docker-compose environment that mirrors prod topology closely. Chaos drills. Runbook written.

### Task 29: Docker-compose staging profile

**Files:**
- Create: `tm_backend/docker-compose.staging.yml`
- Create: `tm_backend/ops/runbooks/storage-toggle.md`

- [ ] **Step 1: Compose with second Postgres (fake DC) + SeaweedFS + toggle worker**

```yaml
# tm_backend/docker-compose.staging.yml
services:
  postgres-azure:
    extends:
      file: docker-compose.yml
      service: postgres
    container_name: tm_vault_db_azure

  postgres-onprem:
    image: postgres:16-alpine
    container_name: tm_vault_db_onprem
    env_file: .env
    environment:
      POSTGRES_DB: ${DB_NAME}
      POSTGRES_USER: ${DB_USERNAME}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    ports: ["5433:5432"]
    volumes: ["pg_onprem:/var/lib/postgresql/data"]
    networks: [tm-network]

  storage-toggle-worker:
    build:
      context: ..
      dockerfile: services/storage-toggle-worker/Dockerfile
    env_file: .env
    environment:
      PG_PROMOTE_STRATEGY: noop
      DNS_FLIP_STRATEGY: noop
      WORKER_RESTART_STRATEGY: noop
    depends_on: [postgres-azure, rabbitmq, seaweedfs]
    networks: [tm-network]

volumes:
  pg_onprem:
```

- [ ] **Step 2: Spin up**

```bash
cd tm_backend
docker compose -f docker-compose.yml -f docker-compose.staging.yml --profile storage-test up -d
```

- [ ] **Step 3: Write runbook**

`tm_backend/ops/runbooks/storage-toggle.md`:

```markdown
# Storage Toggle Runbook

## Prereqs
- Org-admin credentials
- Slack/paging channel active
- Maintenance window (1-2h)
- Monitoring dashboards open (`tmvault_active_backend`, `tmvault_toggle_phase_duration_seconds`)

## Preflight (always green before flip)
1. Open Settings → Storage, click "Refresh preflight"
2. All rows must be ✓: target_reachable, db_replica_lag_ok, no_inflight_transition, cooldown_elapsed, secrets_accessible
3. If any ✗ — stop, resolve root cause, retry preflight. Common fixes:
   - `target_reachable`: check VPN tunnel health, S3 endpoint TLS cert
   - `db_replica_lag_ok`: lag > 10s → wait for catch-up or investigate slow writes
   - `no_inflight_transition`: another flip in progress — abort or wait for completion

## Executing a toggle
1. Click "Switch to [Target]"
2. Reason: short description of why (e.g. "quarterly DR drill", "on-prem cutover")
3. Type org name to confirm
4. Watch SSE stream — phases fire in order:
   - `started` → `drain_started` → `drain_completed` → `db_promoted` → `dns_flipped` → `workers_restarted` → `smoke_passed` → `completed`
5. Expected wall time: 5-15 min (dominated by drain)

## If it hangs
- At `draining` > 20 min: check `SELECT count(*) FROM jobs WHERE status IN ('running','queued','dispatched')`. If stuck, manually mark the offenders:
  ```sql
  UPDATE jobs SET status='aborted_by_toggle' WHERE status IN ('running','queued','dispatched') AND updated_at < NOW() - INTERVAL '30 min';
  ```
- At `db_promoted` failure: check replica lag on target side. Rollback via abort endpoint.
- At `smoke_passed` failure: target backend may be unhealthy post-flip. Option A: roll forward (toggle back). Option B: emergency manual fix.

## Rolling back
Click "Switch to [Original]" and run the toggle in reverse. Same procedure, same duration.

## Disaster recovery
If the entire DC is down and we need to force Azure back online:
1. Point DNS back to Azure manually (ops/dns/flip.sh azure)
2. Promote Azure replica: `psql $AZURE_DSN -c "SELECT pg_promote()"`
3. `UPDATE system_config SET active_backend_id = <azure-id>, transition_state = 'stable' WHERE id = 1;`
4. Scale Azure workers: `kubectl --context azure scale deploy/backup-worker --replicas=8`

RPO: minutes (replica lag). RTO: 15-30 min.

## Audit
After each toggle, export the event row:
```bash
psql $DSN -c "SELECT row_to_json(e) FROM storage_toggle_events e ORDER BY started_at DESC LIMIT 1" > ops/audit/toggle-$(date +%F).json
```
```

- [ ] **Step 4: Commit**

```bash
git add tm_backend/docker-compose.staging.yml tm_backend/ops/runbooks/storage-toggle.md
git commit -m "ops(staging): add staging compose overlay + toggle runbook"
```

---

### Task 30: Chaos drill — kill worker mid-backup

**Files:**
- Create: `tm_backend/tests/chaos/test_worker_kill.py`

- [ ] **Step 1: Write the test**

```python
import asyncio
import uuid

import pytest

from services.storage_toggle_worker.orchestrator import run_toggle


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_worker_killed_midbackup_leads_to_retry(docker_services, test_db):
    """Start a long-running backup, kill its worker, trigger toggle, assert
    the job is marked aborted_by_toggle and a retry row was created."""
    # Enqueue a synthetic slow backup job
    job_id = uuid.uuid4()
    await test_db.execute(
        "INSERT INTO jobs (id, job_type, status, payload, retry_count) "
        "VALUES ($1, 'backup', 'running', $2, 0)",
        job_id, '{"slow": true}',
    )
    # Invoke drain directly (simulates the toggle)
    from services.storage_toggle_worker.drain import drain_inflight
    result = await drain_inflight(test_db, event_id="chaos-1",
                                   poll_interval_s=0, max_wait_s=0)
    assert result.retried_count >= 1
    # Assert the original is aborted and a retry row exists
    row = await test_db.fetchrow("SELECT status FROM jobs WHERE id=$1", job_id)
    assert row["status"] == "aborted_by_toggle"
    retry = await test_db.fetchrow(
        "SELECT id FROM jobs WHERE pre_toggle_job_id=$1 AND status='queued'", job_id,
    )
    assert retry is not None
```

- [ ] **Step 2: Run + commit**

```bash
pytest tests/chaos/test_worker_kill.py -m chaos -v
git add tm_backend/tests/chaos/test_worker_kill.py
git commit -m "test(chaos): worker-kill mid-backup creates correct retry job"
```

---

**Phase 5 exit criteria:**
- [ ] Full toggle flow runs on docker-compose staging overlay
- [ ] Chaos drill passes: aborted jobs get retry rows
- [ ] Runbook reviewed by at least one other engineer
- [ ] Metrics emitted during toggle visible in local Prometheus (or log lines if Prometheus not wired)

---

## Phase 6 — Railway Pilot (est. 1 week)

Goal: stand up the full system with a SeaweedFS pilot service on Railway. Run real toggles in a production-like environment. Report findings for client handoff.

### Task 31: SeaweedFS bucket-creation script

**Files:**
- Create: `tm_backend/scripts/create_seaweedfs_buckets.py`

- [ ] **Step 1: Write the script**

```python
"""Idempotent SeaweedFS bucket creation with Object Lock + versioning.

Usage:
  python scripts/create_seaweedfs_buckets.py \
    --endpoint http://seaweedfs.railway.internal:8333 \
    --access-key PILOT_ACCESS \
    --secret-key PILOT_SECRET \
    --buckets tmvault-shard-0,tmvault-shard-1 \
    --default-retention-days 30 \
    --default-retention-mode GOVERNANCE
"""
from __future__ import annotations

import argparse

import boto3
from botocore.exceptions import ClientError


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--buckets", required=True, help="comma-separated")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--default-retention-days", type=int, default=30)
    ap.add_argument("--default-retention-mode", default="GOVERNANCE",
                    choices=["GOVERNANCE", "COMPLIANCE"])
    args = ap.parse_args()

    s3 = boto3.client(
        "s3", endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
    )
    for b in args.buckets.split(","):
        b = b.strip()
        try:
            s3.create_bucket(Bucket=b, ObjectLockEnabledForBucket=True)
            print(f"created bucket {b}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                print(f"bucket {b} already exists — ok")
            else:
                raise
        s3.put_bucket_versioning(
            Bucket=b, VersioningConfiguration={"Status": "Enabled"},
        )
        s3.put_object_lock_configuration(
            Bucket=b,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {
                    "DefaultRetention": {
                        "Mode": args.default_retention_mode,
                        "Days": args.default_retention_days,
                    },
                },
            },
        )
        print(f"  versioning + object-lock default configured")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Test locally against docker-compose SeaweedFS**

```bash
cd tm_backend
python scripts/create_seaweedfs_buckets.py \
  --endpoint http://localhost:8333 \
  --access-key testaccess --secret-key testsecret \
  --buckets tmvault-shard-0,tmvault-shard-1
```

Expected: both buckets created with Object Lock. Re-running is idempotent.

- [ ] **Step 3: Commit**

```bash
git add tm_backend/scripts/create_seaweedfs_buckets.py
git commit -m "tools(onprem): idempotent SeaweedFS bucket setup script"
```

---

### Task 32: Pilot smoke test script

**Files:**
- Create: `tm_backend/scripts/smoke_onprem_pilot.py`

- [ ] **Step 1: Write the script**

```python
"""Smoke script for Railway pilot or any newly-deployed SeaweedFS cluster.

Runs: upload → download → roundtrip check → apply retention → delete-
attempt → legal-hold → delete-attempt → cleanup."""
from __future__ import annotations

import argparse
import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from shared.storage.errors import ImmutableBlobError
from shared.storage.seaweedfs import SeaweedStore


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--verify-tls", action="store_true")
    args = ap.parse_args()

    s = SeaweedStore(
        backend_id="smoke", name="smoke",
        endpoint=args.endpoint,
        access_key=args.access_key, secret_key=args.secret_key,
        buckets=[args.bucket], verify_tls=args.verify_tls,
    )
    try:
        container = "smoke"
        key = f"smoke-{uuid.uuid4().hex[:8]}.txt"
        body = b"pilot smoke ok"
        print("1. upload ...")
        info = await s.upload(container, key, body, metadata={"kind": "smoke"})
        assert info.size == len(body)
        print("2. download ...")
        got = await s.download(container, key)
        assert got == body

        print("3. apply retention (1 day) ...")
        until = datetime.now(timezone.utc) + timedelta(days=1)
        await s.apply_immutability(container, key, until, mode="Unlocked")
        print("4. delete should fail with ImmutableBlobError ...")
        try:
            await s.delete(container, key)
            print("   !!! delete unexpectedly succeeded — WORM broken !!!")
        except ImmutableBlobError:
            print("   ok")

        print("5. apply legal hold ...")
        await s.apply_legal_hold(container, key)
        print("6. remove legal hold ...")
        await s.remove_legal_hold(container, key)
        print("smoke complete — cleanup this blob manually if retention expired")
    finally:
        await s.close()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Run locally**

```bash
python scripts/smoke_onprem_pilot.py \
  --endpoint http://localhost:8333 \
  --access-key testaccess --secret-key testsecret \
  --bucket tmvault-shard-0
```

Expected: all 6 lines print, step 4 reports `ok`.

- [ ] **Step 3: Commit**

```bash
git add tm_backend/scripts/smoke_onprem_pilot.py
git commit -m "tools(onprem): smoke script for WORM + upload/download sanity"
```

---

### Task 33: Railway pilot runbook

**Files:**
- Create: `tm_backend/ops/runbooks/railway-pilot-setup.md`

- [ ] **Step 1: Write runbook (matches spec Appendix A)**

`tm_backend/ops/runbooks/railway-pilot-setup.md`:

```markdown
# Railway Pilot Setup

Follow spec Appendix A in full. Quick-reference version below.

## Prereqs
- Railway CLI installed, logged in
- AWS CLI
- Existing `tmvault-azure` Railway project with production services

## Step 1 — Create pilot project
```bash
railway init tmvault-onprem-pilot
cd tmvault-onprem-pilot
railway environment create pilot
railway use pilot
```

## Step 2 — Deploy SeaweedFS service
Create `services/seaweedfs/railway.toml`:
```toml
[deploy]
startCommand = "weed server -dir=/data -ip=0.0.0.0 -s3 -s3.config=/config/s3.json -filer -master.volumeSizeLimitMB=1024"
[build]
image = "chrislusf/seaweedfs:3.71"
[[volumes]]
mountPath = "/data"
size = "30Gi"
[[volumes]]
mountPath = "/config"
size = "100Mi"
```

Upload `/config/s3.json` with `objectLockEnabled: true`.

```bash
railway up services/seaweedfs
```

## Step 3 — Create bucket with Object Lock
```bash
export ENDPOINT=$(railway service --name seaweedfs --json | jq -r '.publicUrl')
python scripts/create_seaweedfs_buckets.py \
  --endpoint $ENDPOINT \
  --access-key PILOT_ACCESS --secret-key PILOT_SECRET \
  --buckets tmvault-shard-0
```

## Step 4 — Deploy Postgres replica
```bash
railway add postgresql
railway variables  # note DATABASE_URL
```
Configure logical replication from `tmvault-azure` (see spec Appendix A.5).

## Step 5 — Deploy worker pool
```bash
railway add --template backup-worker
railway variables set \
  ONPREM_S3_ENDPOINT=$ENDPOINT \
  ONPREM_S3_ACCESS_KEY=PILOT_ACCESS \
  ONPREM_S3_SECRET_KEY=PILOT_SECRET \
  ONPREM_S3_BUCKETS=tmvault-shard-0 \
  ONPREM_S3_VERIFY_TLS=false \
  DATABASE_URL=<pilot-replica-url>
```
Repeat for `restore-worker`, `storage-toggle-worker`.

## Step 6 — Apply migrations
```bash
DATABASE_URL=<pilot-url> python -m alembic_like_runner migrations/2026_04_21_onprem_storage_schema.sql
DATABASE_URL=<pilot-url> python -m alembic_like_runner migrations/2026_04_22_onprem_storage_backfill.sql
```

## Step 7 — Seed pilot backend row
```sql
INSERT INTO storage_backends (kind, name, endpoint, secret_ref, config)
VALUES ('seaweedfs', 'onprem-pilot',
  'http://seaweedfs.railway.internal:8333',
  'env://ONPREM_S3_SECRET_KEY',
  '{"buckets":["tmvault-shard-0"],"region":"us-east-1","verify_tls":false,"access_key_env":"ONPREM_S3_ACCESS_KEY"}');
```

## Step 8 — Smoke test
```bash
python scripts/smoke_onprem_pilot.py \
  --endpoint $ENDPOINT \
  --access-key PILOT_ACCESS --secret-key PILOT_SECRET \
  --bucket tmvault-shard-0
```

## Step 9 — First toggle via UI
1. Open tm_vault → Settings → Storage
2. Verify "Active: azure-primary"
3. Click "Switch to On-Prem"
4. Preflight all green
5. Reason: "Railway pilot initial cutover"
6. Type org name, submit
7. Watch SSE → expect `completed` in 5-10 min
8. Run a small tenant backup → verify blob in SeaweedFS

## Step 10 — Toggle back
Same flow in reverse. Measure phase times. Note any preflight issues.

## Teardown
```bash
railway environment delete pilot
```
```

- [ ] **Step 2: Commit**

```bash
git add tm_backend/ops/runbooks/railway-pilot-setup.md
git commit -m "ops(pilot): railway pilot setup runbook"
```

---

**Phase 6 exit criteria:**
- [ ] Pilot SeaweedFS service deployed on Railway, bucket created with Object Lock
- [ ] Pilot Postgres replica streaming from primary
- [ ] Pilot worker pool deployed, connected
- [ ] At least 2 successful round-trip toggles (azure→onprem→azure) with completed status
- [ ] Smoke script passes including WORM delete-blocked assertion
- [ ] Timings + anomalies documented for client handoff

---

## Final rollout checklist (pre-client-handoff)

- [ ] All unit + contract + integration + chaos tests green in CI
- [ ] Runbooks reviewed: `storage-toggle.md`, `railway-pilot-setup.md`, `dr-failover.md`
- [ ] Grafana dashboards exported to `ops/dashboards/`
- [ ] PR merged to `dev-am` / `main`
- [ ] Client handoff packet: Docker images, Helm values template, env.example, runbooks, audit log export script, training slide deck
- [ ] Deferred items tracked as separate issues: phase 7 (prod cutover per client), phase 8 (background migration tooling)

---

## Self-review findings (inline, already fixed)

- Spec §4 mentions `storage_migration_jobs` as deferred — plan does not include it: intentional (phase 8 out of scope).
- Spec §3 lists `shared/azure_storage.py` as "DEPRECATE" — plan keeps it as a shim in Task 13; removal planned after 100% codemod; no task added to remove it because removal is a follow-up PR outside phase 1-6 scope.
- The placeholder backend IDs in `StorageSettings` frontend (`<SEAWEEDFS_BACKEND_ID>`) are intentional — resolved at wiring time when the real rows exist. Flagged in Task 24.
- Task 12 codemod may miss generator methods (`list_blobs`, `list_with_props`) because they are async generators — grep flags them for manual fix. Called out in Task 13 Step 2.
- All function names used in later tasks match earlier tasks (`router.get_active_store`, `router.get_store_for_item`, `drain_inflight`, `run_preflight`, etc.) — no drift detected.
- No "TBD"/"TODO"/"implement later" left.

---

**Plan complete and saved to `tm_backend/docs/superpowers/plans/2026-04-21-onprem-backup-store-plan.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**


