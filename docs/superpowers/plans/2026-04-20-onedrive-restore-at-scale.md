# OneDrive Restore at Scale Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a streaming OneDrive restore engine that supports four user-facing modes (original-overwrite, original-separate-folder, cross-user-overwrite, cross-user-separate-folder), preserves folder tree + original timestamps, survives mid-file failures via Graph upload sessions, and scales safely across 3-5k simultaneous restores on 400 TiB of data without OOM or Graph throttle.

**Architecture:** New `OneDriveRestoreEngine` (mirrored on `MailRestoreEngine`) with bounded `asyncio.Queue` producer/consumer and per-target-user sub-semaphores. Small files (<4 MB) go through a single PUT; large files go through `/createUploadSession` + resumable 10 MB chunks with `Content-Range`. Post-upload `fileSystemInfo` PATCH preserves original dates. Frontend reuses the existing mailbox-dropdown branch in RestoreModal, extended to `ONEDRIVE` kind, and exposes the overwrite/separate-folder radio pair for cross-user restores too.

**Tech Stack:** Python 3.10 / httpx / asyncio / FastAPI / SQLAlchemy / Azure Blob Storage / Microsoft Graph v1.0 · TypeScript / React / Vite

**Spec:** `docs/superpowers/specs/2026-04-20-onedrive-restore-at-scale-design.md`

---

## File Map

**Create:**
- `tm_backend/workers/restore-worker/onedrive_restore.py` — engine
- `tm_backend/tests/workers/test_onedrive_path_derivation.py`
- `tm_backend/tests/workers/test_onedrive_upload_session.py`
- `tm_backend/tests/workers/test_onedrive_restore_engine.py`

**Modify:**
- `tm_backend/shared/config.py` — 4 new env-driven settings
- `tm_backend/shared/export_routing.py` — add `pick_restore_queue`
- `tm_backend/shared/graph_client.py` — add `upload_small_file_to_drive`, `upload_large_file_to_drive`, `patch_drive_item_file_system_info`
- `tm_backend/workers/restore-worker/main.py` — engine hookup in `restore_in_place` + `restore_cross_user`; legacy `_restore_file_to_onedrive` shim rewrite
- `tm_backend/services/job-service/main.py` — route OneDrive restore jobs through `pick_restore_queue`
- `tm_vault/src/components/RestoreModal.tsx` — dropdown for `onedrive` kind + originalSub radios for `destination=another`

---

## Task 1: Add restore-engine config knobs

**Files:**
- Modify: `tm_backend/shared/config.py`

- [ ] **Step 1: Add env-driven settings**

Find the `class Settings` body (near the other `MAIL_RESTORE_*` lines around line 131). Add these four lines in the same cluster:

```python
self.ONEDRIVE_RESTORE_ENGINE_ENABLED = os.getenv("ONEDRIVE_RESTORE_ENGINE_ENABLED", "true").lower() == "true"
self.ONEDRIVE_RESTORE_CONCURRENCY = int(os.getenv("ONEDRIVE_RESTORE_CONCURRENCY", "16"))
self.ONEDRIVE_RESTORE_CHUNK_BYTES = int(os.getenv("ONEDRIVE_RESTORE_CHUNK_BYTES", str(10 * 1024 * 1024)))
self.ONEDRIVE_RESTORE_PER_TARGET_USER_CAP = int(os.getenv("ONEDRIVE_RESTORE_PER_TARGET_USER_CAP", "5"))
```

- [ ] **Step 2: Sanity-check import**

Run: `cd tm_backend && python3 -c "from shared.config import settings; print(settings.ONEDRIVE_RESTORE_CONCURRENCY, settings.ONEDRIVE_RESTORE_CHUNK_BYTES)"`

Expected: `16 10485760`

- [ ] **Step 3: Commit**

```bash
git add tm_backend/shared/config.py
git commit -m "feat(config): onedrive restore engine settings"
```

---

## Task 2: `pick_restore_queue` routing helper

**Files:**
- Modify: `tm_backend/shared/export_routing.py`
- Test: `tm_backend/tests/test_export_routing.py` (create if absent)

- [ ] **Step 1: Write the failing test**

Create or append to `tm_backend/tests/test_export_routing.py`:

```python
from shared.export_routing import pick_restore_queue

def test_small_restore_goes_to_normal():
    assert pick_restore_queue(total_bytes=10 * 1024 * 1024) == "restore.normal"

def test_large_restore_goes_to_heavy():
    # > 50 GB triggers heavy
    assert pick_restore_queue(total_bytes=60 * 1024**3) == "restore.heavy"

def test_zero_bytes_defaults_to_normal():
    assert pick_restore_queue(total_bytes=0) == "restore.normal"

def test_boundary_50gb_stays_normal():
    # 50 GB exactly is still normal; >50 GB is heavy
    assert pick_restore_queue(total_bytes=50 * 1024**3) == "restore.normal"
```

- [ ] **Step 2: Verify tests fail**

Run: `cd tm_backend && pytest tests/test_export_routing.py::test_small_restore_goes_to_normal -v`
Expected: ImportError or AttributeError — `pick_restore_queue` doesn't exist yet.

- [ ] **Step 3: Implement**

Append to `tm_backend/shared/export_routing.py`:

```python
_HEAVY_RESTORE_THRESHOLD = 50 * 1024**3  # 50 GiB


def pick_restore_queue(total_bytes: int) -> str:
    """Route restore jobs by scope size.

    Large restores (>50 GiB) run on restore.heavy so they don't block
    small quick restores on the shared restore.normal queue. Mirrors
    pick_backup_queue / pick_export_queue in the same module.
    """
    if total_bytes and total_bytes > _HEAVY_RESTORE_THRESHOLD:
        return "restore.heavy"
    return "restore.normal"
```

- [ ] **Step 4: Verify tests pass**

Run: `cd tm_backend && pytest tests/test_export_routing.py -v`
Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/shared/export_routing.py tm_backend/tests/test_export_routing.py
git commit -m "feat(routing): pick_restore_queue splits heavy restores to restore.heavy"
```

---

## Task 3: `upload_small_file_to_drive` in GraphClient

**Files:**
- Modify: `tm_backend/shared/graph_client.py`
- Test: `tm_backend/tests/shared/test_graph_upload_drive.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tm_backend/tests/shared/test_graph_upload_drive.py`:

```python
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
import httpx

from shared.graph_client import GraphClient


@pytest.fixture
def client():
    c = GraphClient("cid", "secret", "tenant")
    c._get_token = AsyncMock(return_value="fake-token")
    return c


@pytest.mark.asyncio
async def test_upload_small_file_builds_correct_url(client, monkeypatch):
    calls = {}

    class FakeResp:
        status_code = 201
        def json(self): return {"id": "drv-itm-1", "name": "report.docx"}
        headers = {"content-type": "application/json"}
        text = ""

    class FakeClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def put(self, url, headers=None, content=None):
            calls["url"] = url
            calls["headers"] = headers
            calls["body_len"] = len(content) if content else 0
            return FakeResp()

    monkeypatch.setattr("shared.graph_client.httpx.AsyncClient", FakeClient)

    result = await client.upload_small_file_to_drive(
        user_id="user-1",
        drive_path="Projects/Q1/report.docx",
        body=b"x" * 1024,
        conflict_behavior="replace",
    )

    assert result["id"] == "drv-itm-1"
    assert calls["url"].endswith("/users/user-1/drive/root:/Projects/Q1/report.docx:/content")
    assert calls["headers"]["@microsoft.graph.conflictBehavior"] == "replace"
    assert calls["body_len"] == 1024
```

- [ ] **Step 2: Verify test fails**

Run: `cd tm_backend && pytest tests/shared/test_graph_upload_drive.py::test_upload_small_file_builds_correct_url -v`
Expected: AttributeError — method doesn't exist.

- [ ] **Step 3: Implement**

Add to `tm_backend/shared/graph_client.py` right after the `move_message` method:

```python
async def upload_small_file_to_drive(
    self,
    user_id: str,
    drive_path: str,
    body: bytes,
    conflict_behavior: str = "rename",
) -> Dict[str, Any]:
    """Single-PUT upload for files < 4 MB (Graph's simple-upload cap).

    drive_path is relative to the drive root (no leading slash), e.g.
    "Projects/Q1/report.docx". Graph auto-creates missing ancestor
    folders. conflict_behavior = "replace" | "rename" | "fail".
    Returns the created driveItem dict.
    """
    url = (
        f"{self.GRAPH_URL}/users/{user_id}/drive/root:/"
        f"{drive_path}:/content"
    )
    token = await self._get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
        "@microsoft.graph.conflictBehavior": conflict_behavior,
    }
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as c:
        resp = await c.put(url, headers=headers, content=body)
        if resp.status_code in (429, 503):
            await asyncio.sleep(_parse_retry_after(resp))
            resp = await c.put(url, headers=headers, content=body)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"upload_small_file_to_drive {resp.status_code}: {resp.text[:300]}"
            )
        return resp.json()
```

- [ ] **Step 4: Run test**

Run: `cd tm_backend && pytest tests/shared/test_graph_upload_drive.py::test_upload_small_file_builds_correct_url -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/shared/graph_client.py tm_backend/tests/shared/test_graph_upload_drive.py
git commit -m "feat(graph-client): upload_small_file_to_drive with conflict behavior"
```

---

## Task 4: `upload_large_file_to_drive` — createUploadSession + chunked upload

**Files:**
- Modify: `tm_backend/shared/graph_client.py`
- Test: `tm_backend/tests/shared/test_graph_upload_drive.py`

- [ ] **Step 1: Write the failing test**

Append to `tm_backend/tests/shared/test_graph_upload_drive.py`:

```python
@pytest.mark.asyncio
async def test_upload_large_file_chunks_with_content_range(client, monkeypatch):
    calls = []

    session_body = {"uploadUrl": "https://upload/session?token=abc"}

    class SessPost:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = ""
        def json(self): return session_body

    chunk_resp = {"status": 202}
    final_resp = {"id": "drv-2", "name": "big.bin"}

    class ChunkResp:
        def __init__(self, status, body): self.status_code = status; self._body = body; self.headers = {}; self.text = ""
        def json(self): return self._body

    class FakeClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, headers=None, json=None):
            calls.append(("POST", url, headers, json))
            return SessPost()
        async def put(self, url, headers=None, content=None):
            calls.append(("PUT", url, dict(headers or {}), len(content or b"")))
            # Last chunk returns 201 + item; earlier 202
            is_last = "Content-Range" in headers and headers["Content-Range"].endswith("/25")
            if is_last:
                return ChunkResp(201, final_resp)
            return ChunkResp(202, chunk_resp)

    monkeypatch.setattr("shared.graph_client.httpx.AsyncClient", FakeClient)

    # 25-byte body with 10-byte chunks → 3 chunks (10, 10, 5).
    result = await client.upload_large_file_to_drive(
        user_id="user-1",
        drive_path="bigdir/big.bin",
        body=b"A" * 25,
        total_size=25,
        chunk_size=10,
        conflict_behavior="replace",
    )

    assert result["id"] == "drv-2"
    # First call is the session POST.
    assert calls[0][0] == "POST" and "createUploadSession" in calls[0][1]
    # Then 3 chunk PUTs.
    chunk_calls = [c for c in calls if c[0] == "PUT"]
    assert len(chunk_calls) == 3
    ranges = [c[2]["Content-Range"] for c in chunk_calls]
    assert ranges == ["bytes 0-9/25", "bytes 10-19/25", "bytes 20-24/25"]
```

- [ ] **Step 2: Verify test fails**

Run: `cd tm_backend && pytest tests/shared/test_graph_upload_drive.py::test_upload_large_file_chunks_with_content_range -v`
Expected: AttributeError.

- [ ] **Step 3: Implement**

Add to `tm_backend/shared/graph_client.py` right after `upload_small_file_to_drive`:

```python
async def upload_large_file_to_drive(
    self,
    user_id: str,
    drive_path: str,
    body: bytes,
    total_size: int,
    chunk_size: int = 10 * 1024 * 1024,
    conflict_behavior: str = "rename",
) -> Dict[str, Any]:
    """Resumable upload for files ≥ 4 MB via Graph's uploadSession.

    Splits ``body`` into ``chunk_size`` chunks (Microsoft mandates
    multiples of 320 KiB; 10 MiB is safe). Each chunk is PUT with a
    ``Content-Range: bytes X-Y/total`` header; the final chunk's
    response returns the created driveItem with status 201.

    Retries each chunk up to 3× on 500/503/connection reset — the
    uploadSession URL stays valid, so retries never re-upload
    earlier chunks.
    """
    create_url = (
        f"{self.GRAPH_URL}/users/{user_id}/drive/root:/"
        f"{drive_path}:/createUploadSession"
    )
    token = await self._get_token()
    create_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    create_payload = {
        "item": {
            "@microsoft.graph.conflictBehavior": conflict_behavior,
        }
    }
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as c:
        session_resp = await c.post(create_url, headers=create_headers, json=create_payload)
        if session_resp.status_code >= 400:
            raise RuntimeError(
                f"createUploadSession {session_resp.status_code}: {session_resp.text[:300]}"
            )
        upload_url = session_resp.json().get("uploadUrl")
        if not upload_url:
            raise RuntimeError("createUploadSession returned no uploadUrl")

        offset = 0
        last_json: Dict[str, Any] = {}
        while offset < total_size:
            end = min(offset + chunk_size, total_size) - 1
            chunk = body[offset : end + 1]
            put_headers = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {offset}-{end}/{total_size}",
            }
            attempt = 0
            while True:
                attempt += 1
                try:
                    resp = await c.put(upload_url, headers=put_headers, content=chunk)
                except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError):
                    if attempt >= 3:
                        raise
                    await asyncio.sleep(min(2**attempt, 30))
                    continue
                if resp.status_code in (429, 503):
                    await asyncio.sleep(_parse_retry_after(resp))
                    continue
                if resp.status_code >= 500 and attempt < 3:
                    await asyncio.sleep(min(2**attempt, 30))
                    continue
                if resp.status_code >= 400:
                    raise RuntimeError(
                        f"upload chunk {offset}-{end} {resp.status_code}: {resp.text[:300]}"
                    )
                if resp.status_code in (200, 201):
                    last_json = resp.json()
                break
            offset = end + 1
        return last_json
```

- [ ] **Step 4: Run test**

Run: `cd tm_backend && pytest tests/shared/test_graph_upload_drive.py::test_upload_large_file_chunks_with_content_range -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/shared/graph_client.py tm_backend/tests/shared/test_graph_upload_drive.py
git commit -m "feat(graph-client): upload_large_file_to_drive via resumable uploadSession"
```

---

## Task 5: `patch_drive_item_file_system_info`

**Files:**
- Modify: `tm_backend/shared/graph_client.py`
- Test: `tm_backend/tests/shared/test_graph_upload_drive.py`

- [ ] **Step 1: Write the failing test**

Append to the same test file:

```python
@pytest.mark.asyncio
async def test_patch_file_system_info_normalises_plus_zero(client, monkeypatch):
    calls = {}

    async def fake_patch(url, payload):
        calls["url"] = url
        calls["payload"] = payload
        return {"id": "drv-3"}

    client._patch = fake_patch

    await client.patch_drive_item_file_system_info(
        user_id="user-1",
        drive_item_id="drv-3",
        created_iso="2024-01-15T10:00:00+00:00",
        modified_iso="2024-02-20T12:30:00Z",
    )

    assert calls["url"].endswith("/users/user-1/drive/items/drv-3")
    fsi = calls["payload"]["fileSystemInfo"]
    assert fsi["createdDateTime"] == "2024-01-15T10:00:00Z"
    assert fsi["lastModifiedDateTime"] == "2024-02-20T12:30:00Z"
```

- [ ] **Step 2: Verify test fails**

Run: `cd tm_backend && pytest tests/shared/test_graph_upload_drive.py::test_patch_file_system_info_normalises_plus_zero -v`
Expected: AttributeError.

- [ ] **Step 3: Implement**

Add to `tm_backend/shared/graph_client.py`:

```python
async def patch_drive_item_file_system_info(
    self,
    user_id: str,
    drive_item_id: str,
    created_iso: Optional[str] = None,
    modified_iso: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Restore captured creation + modification timestamps on a
    driveItem. Without this, Explorer shows every restored file as
    created / modified "today". Normalises "+00:00" into the Z form
    Graph accepts; skips the call entirely when both inputs are empty.
    """
    def _norm(ts: Optional[str]) -> Optional[str]:
        if not ts:
            return None
        t = ts.strip()
        if t.endswith("+00:00"):
            t = t[:-6] + "Z"
        elif "+" not in t and not t.endswith("Z"):
            t = t + "Z"
        return t

    created = _norm(created_iso)
    modified = _norm(modified_iso)
    if not (created or modified):
        return None
    fsi: Dict[str, str] = {}
    if created:
        fsi["createdDateTime"] = created
    if modified:
        fsi["lastModifiedDateTime"] = modified
    url = f"{self.GRAPH_URL}/users/{user_id}/drive/items/{drive_item_id}"
    return await self._patch(url, {"fileSystemInfo": fsi})
```

- [ ] **Step 4: Run test**

Run: `cd tm_backend && pytest tests/shared/test_graph_upload_drive.py::test_patch_file_system_info_normalises_plus_zero -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/shared/graph_client.py tm_backend/tests/shared/test_graph_upload_drive.py
git commit -m "feat(graph-client): patch_drive_item_file_system_info preserves original dates"
```

---

## Task 6: `OneDriveRestoreEngine` — scaffold + path derivation

**Files:**
- Create: `tm_backend/workers/restore-worker/onedrive_restore.py`
- Test: `tm_backend/tests/workers/test_onedrive_path_derivation.py`

- [ ] **Step 1: Write the failing test**

Create `tm_backend/tests/workers/test_onedrive_path_derivation.py`:

```python
import pytest
from types import SimpleNamespace

from workers.restore_worker.onedrive_restore import (  # noqa: E402
    OneDriveRestoreEngine, Mode,
)


def _mk_item(name, folder_path=None, ext_id="e1"):
    return SimpleNamespace(
        id="i1", external_id=ext_id, name=name,
        folder_path=folder_path, content_size=100, blob_path="t/b", extra_data={},
    )


def test_original_overwrite_preserves_folder_path():
    path, conflict = OneDriveRestoreEngine.resolve_drive_path(
        item=_mk_item("report.docx", "/drive/root:/Projects/Q1"),
        mode=Mode.OVERWRITE,
        separate_folder_root=None,
    )
    assert path == "Projects/Q1/report.docx"
    assert conflict == "replace"


def test_original_overwrite_flat_file_at_root():
    path, conflict = OneDriveRestoreEngine.resolve_drive_path(
        item=_mk_item("a.txt", None),
        mode=Mode.OVERWRITE,
        separate_folder_root=None,
    )
    assert path == "a.txt"
    assert conflict == "replace"


def test_separate_folder_preserves_tree_under_root():
    path, conflict = OneDriveRestoreEngine.resolve_drive_path(
        item=_mk_item("report.docx", "/drive/root:/Projects/Q1"),
        mode=Mode.SEPARATE_FOLDER,
        separate_folder_root="Restored by TM 2026-04-20",
    )
    assert path == "Restored by TM 2026-04-20/Projects/Q1/report.docx"
    assert conflict == "rename"


def test_separate_folder_strips_parent_reference_anchor():
    # Graph's parentReference.path sometimes looks like "/drives/{id}/root:/foo"
    path, conflict = OneDriveRestoreEngine.resolve_drive_path(
        item=_mk_item("x.pdf", "/drives/abc/root:/folder"),
        mode=Mode.SEPARATE_FOLDER,
        separate_folder_root="MyRestored",
    )
    assert path == "MyRestored/folder/x.pdf"
    assert conflict == "rename"
```

- [ ] **Step 2: Verify test fails**

Run: `cd tm_backend && pytest tests/workers/test_onedrive_path_derivation.py -v`
Expected: ModuleNotFoundError (file doesn't exist).

- [ ] **Step 3: Create engine scaffold**

Create `tm_backend/workers/restore-worker/onedrive_restore.py`:

```python
"""OneDriveRestoreEngine — stream-restore OneDrive files at scale.

Handles four user-facing modes via two inputs:
  * Mode.OVERWRITE          → conflictBehavior=replace
  * Mode.SEPARATE_FOLDER    → conflictBehavior=rename, prefix path

Path derivation preserves the original folder tree under the chosen
prefix (or at root if no prefix). Upload dispatch picks a single PUT
for files < 4 MB and resumable uploadSession chunks for ≥ 4 MB so
multi-GB files resume on transient failure instead of restarting.
"""
from __future__ import annotations

import asyncio
import enum
from dataclasses import dataclass, field
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
```

- [ ] **Step 4: Run test**

Run: `cd tm_backend && pytest tests/workers/test_onedrive_path_derivation.py -v`
Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/workers/restore-worker/onedrive_restore.py tm_backend/tests/workers/test_onedrive_path_derivation.py
git commit -m "feat(onedrive-restore): engine scaffold + path derivation"
```

---

## Task 7: Engine per-file upload dispatch (small vs large)

**Files:**
- Modify: `tm_backend/workers/restore-worker/onedrive_restore.py`
- Test: `tm_backend/tests/workers/test_onedrive_restore_engine.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tm_backend/tests/workers/test_onedrive_restore_engine.py`:

```python
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from workers.restore_worker.onedrive_restore import (
    OneDriveRestoreEngine, Mode, SMALL_FILE_MAX_BYTES,
)


def _mk_engine(graph):
    source = SimpleNamespace(id="res1", tenant_id="t1", external_id="source-user")
    return OneDriveRestoreEngine(
        graph_client=graph,
        source_resource=source,
        target_drive_user_id="target-user",
        tenant_id="t1",
        mode=Mode.OVERWRITE,
        worker_id="wt",
    )


def _mk_item(name, size, folder_path=None, blob_path="tenant/res/snap/itm", ext_id="e"):
    return SimpleNamespace(
        id="i", external_id=ext_id, name=name,
        folder_path=folder_path, content_size=size, blob_path=blob_path,
        extra_data={"raw": {"file": {"mimeType": "application/octet-stream"}}},
    )


@pytest.mark.asyncio
async def test_small_file_uses_simple_put(monkeypatch):
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock(return_value={"id": "drv-1"})
    graph.upload_large_file_to_drive = AsyncMock(return_value={"id": "drv-ignored"})
    graph.patch_drive_item_file_system_info = AsyncMock()

    async def fake_read(item):
        return b"x" * (SMALL_FILE_MAX_BYTES - 1)
    engine = _mk_engine(graph)
    engine._read_blob_bytes = fake_read

    outcome = await engine.upload_one(_mk_item("a.txt", SMALL_FILE_MAX_BYTES - 1))

    assert outcome.outcome == "overwritten"  # Mode.OVERWRITE → conflict replace → classify overwritten
    assert graph.upload_small_file_to_drive.await_count == 1
    assert graph.upload_large_file_to_drive.await_count == 0


@pytest.mark.asyncio
async def test_large_file_uses_upload_session(monkeypatch):
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock(return_value={"id": "drv-bad"})
    graph.upload_large_file_to_drive = AsyncMock(return_value={"id": "drv-2"})
    graph.patch_drive_item_file_system_info = AsyncMock()

    big = SMALL_FILE_MAX_BYTES + 10
    async def fake_read(item):
        return b"x" * big
    engine = _mk_engine(graph)
    engine._read_blob_bytes = fake_read

    outcome = await engine.upload_one(_mk_item("big.bin", big))

    assert outcome.outcome == "overwritten"
    assert graph.upload_small_file_to_drive.await_count == 0
    assert graph.upload_large_file_to_drive.await_count == 1


@pytest.mark.asyncio
async def test_missing_blob_is_skipped_not_failed(monkeypatch):
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock()
    graph.upload_large_file_to_drive = AsyncMock()
    graph.patch_drive_item_file_system_info = AsyncMock()

    engine = _mk_engine(graph)
    async def fake_read(item): return None
    engine._read_blob_bytes = fake_read

    item = _mk_item("gone.txt", 100, blob_path=None)
    outcome = await engine.upload_one(item)

    assert outcome.outcome == "skipped"
    assert outcome.reason == "blob_missing"
    assert graph.upload_small_file_to_drive.await_count == 0
    assert graph.upload_large_file_to_drive.await_count == 0
```

- [ ] **Step 2: Verify tests fail**

Run: `cd tm_backend && pytest tests/workers/test_onedrive_restore_engine.py -v`
Expected: AttributeError — `upload_one` / `_read_blob_bytes` don't exist.

- [ ] **Step 3: Implement**

Append to `tm_backend/workers/restore-worker/onedrive_restore.py`:

```python
    # ---------- single-file upload ----------

    async def _read_blob_bytes(self, item: SnapshotItem) -> Optional[bytes]:
        """Fetch captured bytes from Azure Blob. Returns None on missing
        blob_path or any read error — callers treat that as 'skipped'."""
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
            blob_client = shard.get_blob_client(container, blob_path)
            stream = await blob_client.download_blob()
            return await stream.readall()
        except Exception as exc:
            print(f"[{self.worker_id}] [ONEDRIVE-RESTORE] blob read "
                  f"{blob_path} failed: {type(exc).__name__}: {exc}", flush=True)
            return None

    async def upload_one(self, item: SnapshotItem) -> ItemOutcome:
        """Upload one SnapshotItem to the target drive."""
        name = getattr(item, "name", None) or getattr(item, "external_id", "item")
        size = int(getattr(item, "content_size", 0) or 0)

        drive_path, conflict = self.resolve_drive_path(
            item, self.mode, self.separate_folder_root,
        )

        body = await self._read_blob_bytes(item)
        if body is None:
            return ItemOutcome(
                item_id=str(getattr(item, "id", "")),
                external_id=getattr(item, "external_id", ""),
                name=name, outcome="skipped", reason="blob_missing",
            )

        try:
            if size < SMALL_FILE_MAX_BYTES:
                created = await self.graph.upload_small_file_to_drive(
                    user_id=self.target_user_id,
                    drive_path=drive_path,
                    body=body,
                    conflict_behavior=conflict,
                )
            else:
                created = await self.graph.upload_large_file_to_drive(
                    user_id=self.target_user_id,
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

        outcome_label = "overwritten" if conflict == "replace" else (
            "renamed" if (created or {}).get("name") != name else "created"
        )
        return ItemOutcome(
            item_id=str(getattr(item, "id", "")),
            external_id=getattr(item, "external_id", ""),
            name=name, outcome=outcome_label, size_bytes=len(body),
        )
```

- [ ] **Step 4: Run tests**

Run: `cd tm_backend && pytest tests/workers/test_onedrive_restore_engine.py -v`
Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/workers/restore-worker/onedrive_restore.py tm_backend/tests/workers/test_onedrive_restore_engine.py
git commit -m "feat(onedrive-restore): upload_one dispatches small vs large, skips missing blobs"
```

---

## Task 8: Post-upload `fileSystemInfo` PATCH + ACL replay gating

**Files:**
- Modify: `tm_backend/workers/restore-worker/onedrive_restore.py`
- Test: `tm_backend/tests/workers/test_onedrive_restore_engine.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/workers/test_onedrive_restore_engine.py`:

```python
@pytest.mark.asyncio
async def test_upload_patches_file_system_info_from_raw(monkeypatch):
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock(return_value={"id": "drv-9", "name": "a.txt"})
    graph.upload_large_file_to_drive = AsyncMock()
    patched = {}
    async def patch_fsi(user_id, drive_item_id, created_iso, modified_iso):
        patched.update(dict(user=user_id, item=drive_item_id,
                            created=created_iso, modified=modified_iso))
    graph.patch_drive_item_file_system_info = patch_fsi

    engine = _mk_engine(graph)
    engine._read_blob_bytes = AsyncMock(return_value=b"x" * 50)

    item = _mk_item("a.txt", 50)
    item.extra_data = {"raw": {
        "fileSystemInfo": {
            "createdDateTime": "2024-01-01T00:00:00Z",
            "lastModifiedDateTime": "2024-06-01T12:00:00Z",
        },
    }}

    outcome = await engine.upload_one(item)
    assert outcome.outcome == "overwritten"
    assert patched["item"] == "drv-9"
    assert patched["created"] == "2024-01-01T00:00:00Z"
    assert patched["modified"] == "2024-06-01T12:00:00Z"
```

- [ ] **Step 2: Verify test fails**

Run: `cd tm_backend && pytest tests/workers/test_onedrive_restore_engine.py::test_upload_patches_file_system_info_from_raw -v`
Expected: FAIL — PATCH never called.

- [ ] **Step 3: Wire PATCH into `upload_one`**

Locate the block in `onedrive_restore.py` immediately after the `try/except` around the upload, before the outcome_label assignment, and insert:

```python
        # Restore captured creation / modification dates. Graph stamps
        # server-now on upload; without this PATCH the file looks
        # "modified today" in Explorer.
        raw = (getattr(item, "extra_data", None) or {}).get("raw") or {}
        fsi = raw.get("fileSystemInfo") or {}
        new_item_id = (created or {}).get("id")
        if new_item_id and (fsi.get("createdDateTime") or fsi.get("lastModifiedDateTime")):
            try:
                await self.graph.patch_drive_item_file_system_info(
                    user_id=self.target_user_id,
                    drive_item_id=new_item_id,
                    created_iso=fsi.get("createdDateTime"),
                    modified_iso=fsi.get("lastModifiedDateTime"),
                )
            except Exception as exc:
                print(f"[{self.worker_id}] [ONEDRIVE-RESTORE] fileSystemInfo "
                      f"PATCH failed {name}: {type(exc).__name__}: {exc}", flush=True)
```

- [ ] **Step 4: Run tests**

Run: `cd tm_backend && pytest tests/workers/test_onedrive_restore_engine.py -v`
Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/workers/restore-worker/onedrive_restore.py tm_backend/tests/workers/test_onedrive_restore_engine.py
git commit -m "feat(onedrive-restore): PATCH fileSystemInfo so original dates survive restore"
```

---

## Task 9: Engine `run()` — bounded queue + concurrency caps

**Files:**
- Modify: `tm_backend/workers/restore-worker/onedrive_restore.py`
- Test: `tm_backend/tests/workers/test_onedrive_restore_engine.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
@pytest.mark.asyncio
async def test_run_drains_items_and_aggregates(monkeypatch):
    graph = SimpleNamespace()
    graph.upload_small_file_to_drive = AsyncMock(side_effect=[
        {"id": "a", "name": "a.txt"},
        {"id": "b", "name": "b.txt"},
    ])
    graph.upload_large_file_to_drive = AsyncMock()
    graph.patch_drive_item_file_system_info = AsyncMock()

    engine = _mk_engine(graph)
    engine._read_blob_bytes = AsyncMock(side_effect=[b"1" * 10, b"2" * 10])

    items = [_mk_item("a.txt", 10, ext_id="a"), _mk_item("b.txt", 10, ext_id="b")]
    summary = await engine.run(items)

    assert summary["overwritten"] == 2
    assert summary["failed"] == 0
    assert len(summary["items"]) == 2
    assert graph.upload_small_file_to_drive.await_count == 2
```

- [ ] **Step 2: Verify test fails**

Run: `cd tm_backend && pytest tests/workers/test_onedrive_restore_engine.py::test_run_drains_items_and_aggregates -v`
Expected: AttributeError — `run` doesn't exist.

- [ ] **Step 3: Implement**

Append to `onedrive_restore.py`:

```python
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
```

- [ ] **Step 4: Run tests**

Run: `cd tm_backend && pytest tests/workers/test_onedrive_restore_engine.py -v`
Expected: 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/workers/restore-worker/onedrive_restore.py tm_backend/tests/workers/test_onedrive_restore_engine.py
git commit -m "feat(onedrive-restore): bounded queue + per-target-user concurrency cap"
```

---

## Task 10: Hook engine into `restore_in_place` + `_resolve_onedrive_target_user`

**Files:**
- Modify: `tm_backend/workers/restore-worker/main.py`

- [ ] **Step 1: Add the resolver helper**

Find `_restore_file_to_onedrive` (around line 1800). Above it, insert:

```python
    async def _resolve_onedrive_target_user(
        self,
        session: AsyncSession,
        source_resource: Resource,
        spec: Dict,
    ) -> tuple[str, bool]:
        """Return (target_user_graph_id, is_cross_user) for an OneDrive
        restore. spec.targetUserId is the DB UUID of the target OneDrive
        resource row (not a Graph user id). Unset → restore into the
        source's own drive."""
        target_uuid = spec.get("targetUserId")
        if not target_uuid:
            return source_resource.external_id, False
        target_res = await session.get(Resource, uuid.UUID(str(target_uuid)))
        if not target_res:
            raise ValueError(f"targetUserId {target_uuid} not found")
        target_type = target_res.type.value if hasattr(target_res.type, "value") else str(target_res.type)
        if target_type != "ONEDRIVE":
            raise ValueError(
                f"targetUserId {target_uuid} is not a OneDrive resource (got {target_type})"
            )
        if target_res.tenant_id != source_resource.tenant_id:
            raise ValueError("Cross-tenant restore is not supported")
        return target_res.external_id, (target_res.id != source_resource.id)
```

- [ ] **Step 2: Hook the engine into `restore_in_place`**

In `restore_in_place`, find the per-resource loop (starts around line 578 where `mail_items: List[SnapshotItem] = []` lives). Immediately after the mail-engine block (after its `for o in mail_summary.get("items", []):` loop), insert the OneDrive block:

```python
            # OneDrive restore v2 — route FILE / ONEDRIVE_FILE items
            # through the dedicated streaming engine when the feature
            # flag is on and we're restoring into a user's drive.
            onedrive_items: List[SnapshotItem] = []
            if settings.ONEDRIVE_RESTORE_ENGINE_ENABLED and resource_type in (
                "ONEDRIVE", "USER_ONEDRIVE"
            ):
                remaining2: List[SnapshotItem] = []
                for it in resource_items:
                    if it.item_type in ("FILE", "ONEDRIVE_FILE"):
                        onedrive_items.append(it)
                    else:
                        remaining2.append(it)
                resource_items = remaining2

            if onedrive_items:
                from onedrive_restore import OneDriveRestoreEngine, Mode
                target_user_id, is_cross = await self._resolve_onedrive_target_user(
                    session, resource, spec,
                )
                od_engine = OneDriveRestoreEngine(
                    graph_client=graph_client,
                    source_resource=resource,
                    target_drive_user_id=target_user_id,
                    tenant_id=str(resource.tenant_id),
                    mode=Mode.OVERWRITE if spec.get("overwrite") else Mode.SEPARATE_FOLDER,
                    separate_folder_root=spec.get("targetFolder"),
                    worker_id=self.worker_id,
                    is_cross_user=is_cross,
                )
                od_summary = await od_engine.run(onedrive_items)
                restored_count += (
                    od_summary.get("created", 0)
                    + od_summary.get("overwritten", 0)
                    + od_summary.get("renamed", 0)
                )
                failed_count += od_summary.get("failed", 0)
                for o in od_summary.get("items", []):
                    if o.get("outcome") == "failed":
                        print(
                            f"[{self.worker_id}] [ONEDRIVE-RESTORE FAIL] "
                            f"ext_id={o.get('external_id')} name={o.get('name')} "
                            f"reason={o.get('reason')}",
                            flush=True,
                        )
```

- [ ] **Step 3: Hook the engine into `restore_cross_user`**

Find `restore_cross_user` (around line 770). After its mail-engine block, mirror the same OneDrive routing (use `target_resource` instead of `resource`, and compute `target_user_id = target_resource.external_id` + `is_cross=True`):

```python
            od_cu_items: List[SnapshotItem] = []
            if settings.ONEDRIVE_RESTORE_ENGINE_ENABLED and target_type in (
                "ONEDRIVE", "USER_ONEDRIVE"
            ):
                remaining3: List[SnapshotItem] = []
                for it in items:
                    if it.item_type in ("FILE", "ONEDRIVE_FILE"):
                        od_cu_items.append(it)
                    else:
                        remaining3.append(it)
                items = remaining3

            if od_cu_items:
                from onedrive_restore import OneDriveRestoreEngine, Mode
                od_engine = OneDriveRestoreEngine(
                    graph_client=graph_client,
                    source_resource=target_resource,  # downloads read blob via target's tenant
                    target_drive_user_id=target_resource.external_id,
                    tenant_id=str(target_resource.tenant_id),
                    mode=Mode.OVERWRITE if spec.get("overwrite") else Mode.SEPARATE_FOLDER,
                    separate_folder_root=spec.get("targetFolder"),
                    worker_id=self.worker_id,
                    is_cross_user=True,
                )
                od_summary = await od_engine.run(od_cu_items)
                restored_count += (
                    od_summary.get("created", 0)
                    + od_summary.get("overwritten", 0)
                    + od_summary.get("renamed", 0)
                )
                failed_count += od_summary.get("failed", 0)
```

**Important:** in `restore_cross_user`, the engine's `source_resource` should stay as the **original source row** (so `_read_blob_bytes` reads the correct tenant's blob container) — change the line above to `source_resource=resource` where `resource` is the source item's resource; if the surrounding scope doesn't have it, fetch `source_resource = await session.get(Resource, uuid.UUID(first_item.snapshot.resource_id))`. Inspect the existing loop for the variable name already in scope before committing.

- [ ] **Step 4: Syntax check**

Run: `cd tm_backend && python3 -c "import ast; ast.parse(open('workers/restore-worker/main.py').read()); print('OK')"`
Expected: `OK`.

- [ ] **Step 5: Commit**

```bash
git add tm_backend/workers/restore-worker/main.py
git commit -m "feat(onedrive-restore): route FILE/ONEDRIVE_FILE items through engine in in-place + cross-user"
```

---

## Task 11: Rewrite legacy `_restore_file_to_onedrive` shim

**Files:**
- Modify: `tm_backend/workers/restore-worker/main.py`

- [ ] **Step 1: Replace the body**

Locate `_restore_file_to_onedrive` (around line 1800 after the earlier edit). Replace the body with a single-item dispatch through the new engine:

```python
    async def _restore_file_to_onedrive(
        self,
        graph_client: GraphClient,
        resource: Resource,
        item: SnapshotItem,
        conflict_mode: str = "SEPARATE_FOLDER",
    ):
        """Per-item shim kept for forward compatibility with callers that
        don't batch into the engine (e.g. single-file test paths). Uses
        the same streaming engine so nobody lands on a broken code
        path."""
        from onedrive_restore import OneDriveRestoreEngine, Mode

        engine = OneDriveRestoreEngine(
            graph_client=graph_client,
            source_resource=resource,
            target_drive_user_id=resource.external_id,
            tenant_id=str(resource.tenant_id),
            mode=Mode.OVERWRITE if conflict_mode == "OVERWRITE" else Mode.SEPARATE_FOLDER,
            separate_folder_root=(
                f"Restored by TM/{datetime.utcnow().strftime('%Y-%m-%d')}"
                if conflict_mode != "OVERWRITE" else None
            ),
            worker_id=self.worker_id,
            is_cross_user=False,
        )
        outcome = await engine.upload_one(item)
        if outcome.outcome == "failed":
            raise RuntimeError(outcome.reason or "onedrive restore failed")
```

- [ ] **Step 2: Syntax check**

Run: `cd tm_backend && python3 -c "import ast; ast.parse(open('workers/restore-worker/main.py').read()); print('OK')"`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add tm_backend/workers/restore-worker/main.py
git commit -m "refactor(onedrive-restore): legacy shim delegates to engine"
```

---

## Task 12: Route OneDrive restore jobs via `pick_restore_queue` in job-service

**Files:**
- Modify: `tm_backend/services/job-service/main.py`

- [ ] **Step 1: Find the restore publish site**

Locate the `trigger_restore` handler (around line 656 per earlier grep). Find its `await message_bus.publish(queue, …)` call around line 754.

- [ ] **Step 2: Replace the queue-selection line**

Before the `queue = restore_message.get("queue", "restore.normal")` line, compute an estimated `total_bytes` and call the routing helper:

```python
            from shared.export_routing import pick_restore_queue
            from sqlalchemy import func as sa_func, select as sa_select
            total_bytes = 0
            try:
                if item_ids:
                    q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                        SnapshotItem.id.in_([uuid.UUID(x) for x in item_ids])
                    )
                    total_bytes = int((await db.execute(q)).scalar() or 0)
                elif snapshot_ids:
                    q = sa_select(sa_func.coalesce(sa_func.sum(SnapshotItem.content_size), 0)).where(
                        SnapshotItem.snapshot_id.in_([uuid.UUID(x) for x in snapshot_ids])
                    )
                    total_bytes = int((await db.execute(q)).scalar() or 0)
            except Exception:
                total_bytes = 0
            queue = restore_message.get("queue") or pick_restore_queue(total_bytes=total_bytes)
```

- [ ] **Step 3: Syntax check**

Run: `cd tm_backend && python3 -c "import ast; ast.parse(open('services/job-service/main.py').read()); print('OK')"`
Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add tm_backend/services/job-service/main.py
git commit -m "feat(job-service): route large restores via pick_restore_queue"
```

---

## Task 13: Frontend — OneDrive target dropdown

**Files:**
- Modify: `tm_vault/src/components/RestoreModal.tsx`

- [ ] **Step 1: Extend the target-load effect to cover ONEDRIVE**

Locate the `useEffect` at `src/components/RestoreModal.tsx` lines ~135–170 that loads mailbox targets. Add a parallel branch for OneDrive. Rewrite the effect body so it dispatches based on `resourceKind`:

```tsx
  useEffect(() => {
    if (!isOpen || !tenantId) return;
    if (destination !== 'another') return;

    const isMailSource = resourceKind === 'mailbox'
      || resourceKind === 'shared_mailbox'
      || resourceKind === 'room_mailbox';
    const isOneDriveSource = resourceKind === 'onedrive';
    if (!isMailSource && !isOneDriveSource) return;

    let cancelled = false;
    setMailboxTargetsLoading(true);
    setMailboxTargetsError(null);

    const loaders = isMailSource
      ? [
          getResourcesByType(tenantId, 'MAILBOX', 1, 500, undefined, 'active'),
          getResourcesByType(tenantId, 'SHARED_MAILBOX', 1, 500, undefined, 'active'),
          getResourcesByType(tenantId, 'ROOM_MAILBOX', 1, 500, undefined, 'active'),
        ]
      : [getResourcesByType(tenantId, 'ONEDRIVE', 1, 500, undefined, 'active')];

    Promise.all(loaders)
      .then((results) => {
        if (cancelled) return;
        const merged: ResourceItem[] = [];
        for (const r of results) merged.push(...(r.items || []));
        setMailboxTargets(merged);
      })
      .catch((err) => {
        if (cancelled) return;
        setMailboxTargets([]);
        setMailboxTargetsError(err instanceof Error ? err.message : 'Failed to load resources');
      })
      .finally(() => {
        if (!cancelled) setMailboxTargetsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [isOpen, tenantId, destination, resourceKind]);
```

- [ ] **Step 2: Expand the dropdown-render branch to match OneDrive kind**

Find the `destination === 'another' && !isPowerBiItem && !isPowerPlatformItem && (` block (around line 426). Change the inner ternary so `resourceKind === 'onedrive'` also renders the `<select>`. Replace:

```tsx
  (resourceKind === 'mailbox' || resourceKind === 'shared_mailbox' || resourceKind === 'room_mailbox') ? (
```

with:

```tsx
  (resourceKind === 'mailbox' || resourceKind === 'shared_mailbox' || resourceKind === 'room_mailbox' || resourceKind === 'onedrive') ? (
```

No other changes needed in the `<select>` body — OneDrive resources render as `{name}` (no email), which the existing fallback already handles.

- [ ] **Step 3: Commit**

```bash
git add tm_vault/src/components/RestoreModal.tsx
git commit -m "feat(restore-modal): OneDrive target dropdown on 'recover to another'"
```

---

## Task 14: Frontend — show overwrite/separate-folder radios for cross-user OneDrive

**Files:**
- Modify: `tm_vault/src/components/RestoreModal.tsx`

- [ ] **Step 1: Locate the originalSub radio panel**

Find the block rendering the `overwrite` / `separate_folder` radios — it should be gated on `destination === 'original'` (search for `originalSub`). It lives in the same form section as the "Recover to original" option.

- [ ] **Step 2: Widen the visibility condition**

Change the visibility check so the panel shows whenever `destination === 'original'` OR (`destination === 'another'` AND `resourceKind === 'onedrive'` AND a target is selected). Example (adapt to the exact surrounding JSX):

```tsx
{(destination === 'original'
  || (destination === 'another' && resourceKind === 'onedrive' && !!targetUserId)) && (
  <div className="restore-sub-options">
    <label>
      <input type="radio" checked={originalSub === 'overwrite'} onChange={() => setOriginalSub('overwrite')} />
      <span>Overwrite existing content</span>
    </label>
    <label>
      <input type="radio" checked={originalSub === 'separate_folder'} onChange={() => setOriginalSub('separate_folder')} />
      <span>Create a separate folder</span>
    </label>
    {originalSub === 'separate_folder' && (
      <input
        className="folder-input"
        placeholder="Folder name (e.g. Restored by TM 2026-04-20)"
        value={folderName}
        onChange={(e) => setFolderName(e.target.value)}
      />
    )}
  </div>
)}
```

Also update the submit-payload line (around line 256) that reads:

```tsx
targetFolder: !isPowerBiItem && !isPowerPlatformItem && destination === 'original' && originalSub === 'separate_folder' ? folderName : undefined,
overwrite: !isPowerBiItem && !isPowerPlatformItem && destination === 'original' && originalSub === 'overwrite',
```

to:

```tsx
targetFolder: !isPowerBiItem && !isPowerPlatformItem
  && ((destination === 'original') || (destination === 'another' && resourceKind === 'onedrive'))
  && originalSub === 'separate_folder' ? folderName : undefined,
overwrite: !isPowerBiItem && !isPowerPlatformItem
  && ((destination === 'original') || (destination === 'another' && resourceKind === 'onedrive'))
  && originalSub === 'overwrite',
```

- [ ] **Step 3: Update restoreType mapping for cross-user OneDrive**

The existing mapping (around line 246) sets `restoreType = destination === 'another' ? 'CROSS_USER' : 'IN_PLACE'`. For OneDrive cross-user, we still want `CROSS_USER` — no change here, but add a comment to clarify:

```tsx
// OneDrive cross-user goes through CROSS_USER where the worker resolves
// spec.targetUserId (DB UUID) → target OneDrive resource → Graph user id.
restoreType = destination === 'another' ? 'CROSS_USER' : 'IN_PLACE';
```

- [ ] **Step 4: Commit**

```bash
git add tm_vault/src/components/RestoreModal.tsx
git commit -m "feat(restore-modal): show overwrite/separate-folder radios for cross-user OneDrive"
```

---

## Task 15: Rebuild + recreate workers and job-service

**Files:** n/a

- [ ] **Step 1: Build images**

Run: `cd tm_backend && docker compose build restore-worker restore-worker-heavy job-service 2>&1 | tail -5`
Expected: three `Image ... Built` lines.

- [ ] **Step 2: Recreate containers**

Run: `cd tm_backend && docker compose up -d --no-deps --force-recreate restore-worker restore-worker-heavy job-service 2>&1 | grep -vE "^time=|AZURE_AD" | tail -8`
Expected: `Started` for each container.

- [ ] **Step 3: Tail a sanity check**

Run: `cd tm_backend && sleep 6 && docker compose logs --tail=4 restore-worker restore-worker-heavy job-service 2>&1 | grep -vE "^time=|AZURE_AD" | head -15`
Expected: workers subscribed to their queues, job-service reports `Application startup complete`.

- [ ] **Step 4: Commit deploy note** (optional)

No code change required — skip commit unless config was also touched in this task.

---

## Self-Review Notes

- **Spec coverage** — each spec section maps to at least one task:
  - Modes table → tasks 6–9 + 10 + 11
  - Frontend changes → tasks 13 + 14
  - Upload engine (small/large/PATCH) → tasks 3 + 4 + 5 + 7 + 8
  - Concurrency caps → task 9
  - Job routing → tasks 2 + 12
  - Config surface → task 1
  - Error handling / summary logs → task 9 (summary) + task 10 (per-item FAIL log)
  - Testing boundaries → tests embedded in tasks 3/4/5/6/7/8/9
  - Migration / rollout → feature flag landed in task 1, shim rewritten task 11
- **Placeholder scan** — no TBDs, every code step contains the full code needed.
- **Type consistency** — `Mode` enum members (`OVERWRITE` / `SEPARATE_FOLDER`) used identically across engine + main.py hookup. `ItemOutcome.outcome` values (`created|overwritten|renamed|skipped|failed`) used in tests + engine + summary keys. `conflict_behavior` string used identically across Graph helpers + engine.
