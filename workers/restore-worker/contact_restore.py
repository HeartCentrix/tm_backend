"""Contact Restore engine — folder-aware, batched, backpressured.

Handles USER_CONTACT snapshot items at scale:

* Resolves the target Graph user id (strips the :contacts tier-2 suffix
  so the URL path is valid).
* Groups items by snapshot_item.folder_path. Items without a folder (or
  folder_path in {"", "Contacts"}) go to the default Contacts folder;
  everything else is routed to the matching custom contactFolder,
  creating it on demand.
* Uses Microsoft Graph's /$batch endpoint (20 sub-requests per HTTP
  call) via ``shared.graph_batch.BatchClient`` — which already knows
  how to retry 429/503 sub-responses with Retry-After honoured.
* Bounds Graph pressure with two semaphores:
    - GLOBAL: per-worker cap across every in-flight restore job.
    - PER-USER: Outlook serializes contactFolder sub-requests 4-at-a-time
      per mailbox; anything over 4 triggers 429 storms.
* Classifies per-subrequest results: 2xx → created; 404 / invalid folder
  → retryable at the logical level (refresh folder cache); other 4xx
  → permanent failure; 429 / 5xx → already handled by BatchClient.

Mode semantics mirror MailRestoreEngine:
    MODE_SEPARATE  — restore into a timestamped subfolder under the
                     target user's root contactFolders ("Restored by TMvault/…").
                     Leaves existing contacts untouched; default.
    MODE_OVERWRITE — restore directly into the original folder_path.
                     Duplicates across retries are possible; upstream
                     RabbitMQ redelivery is the normal retry path.

Scale sizing (5k users × ~500 contacts = 2.5M rows):
    ~125k Graph sub-requests with 20-per-batch amortization.
    At 32 concurrent batches × ~4 req/s per Outlook mailbox,
    wall-clock target ~20-40 min assuming light throttling.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from shared.graph_batch import BatchClient, BatchRequest

MODE_SEPARATE = "SEPARATE_FOLDER"
MODE_OVERWRITE = "OVERWRITE"

# Graph /$batch allows 20 sub-requests per call. Outlook sub-requests are
# then serialized 4-at-a-time per mailbox inside the batch — throughput
# comes from running many batches (different users) in parallel, not
# from packing more per batch.
_BATCH_SIZE = 20

# Default root for separate-folder mode. Job-level `targetFolder` overrides.
_SEPARATE_ROOT_DEFAULT = "Restored by TMvault"


@dataclass
class _ContactTask:
    """One contact we need to POST. ``dest_folder_path`` is the
    user-visible path we resolved from ``folder_path`` (after applying
    separate-folder mode); ``dest_folder_id`` is populated after the
    folder-ensure pass and is ``None`` for the default folder."""
    item_id: str
    external_id: str
    display_name: str
    payload: Dict[str, Any]
    source_folder_path: str
    dest_folder_path: str
    dest_folder_id: Optional[str] = None
    attempts: int = 0


@dataclass
class _Outcome:
    item_id: str
    external_id: str
    display_name: str
    outcome: str  # "created" | "failed" | "skipped"
    reason: Optional[str] = None


@dataclass
class _RunStats:
    created: int = 0
    failed: int = 0
    skipped: int = 0
    folders_ensured: int = 0
    batches_sent: int = 0
    items: List[_Outcome] = field(default_factory=list)


def _normalize_folder(path: Optional[str]) -> str:
    """Canonicalize the source folder_path. The backup writer stores
    either ``""`` (default folder) or the folder's displayName (e.g.
    ``"Contacts"`` or ``"Vendors"``). We treat ``""`` and ``"Contacts"``
    as the default folder since Graph's default folder is named
    ``Contacts`` in every locale we ship to."""
    s = (path or "").strip()
    if not s or s.lower() == "contacts":
        return ""
    return s


class ContactRestoreEngine:
    """Per-job engine; instantiated once per `restore_in_place` /
    `restore_cross_user` call for a given resource."""

    def __init__(
        self,
        graph_client,
        resource,
        *,
        mode: str,
        graph_user_id: str,
        worker_id: str,
        separate_folder_root: Optional[str] = None,
        global_sem: Optional[asyncio.Semaphore] = None,
        per_user_sem: Optional[asyncio.Semaphore] = None,
        max_retries: int = 5,
    ):
        self.graph = graph_client
        self.resource = resource
        self.mode = mode if mode in (MODE_SEPARATE, MODE_OVERWRITE) else MODE_SEPARATE
        self.user_id = graph_user_id
        self.worker_id = worker_id
        # Separate-folder root. If the caller supplied a targetFolder we
        # use it verbatim (the UI already timestamps these). Only synthesize
        # our own "YYYY-MM-DD HH:MM" suffix when no name was provided, so
        # back-to-back restores to the same root still get unique folders
        # instead of collapsing into one.
        if self.mode == MODE_SEPARATE:
            if separate_folder_root and separate_folder_root.strip("/"):
                self._separate_root = separate_folder_root.strip("/")
            else:
                stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
                self._separate_root = f"{_SEPARATE_ROOT_DEFAULT}/{stamp}"
        else:
            self._separate_root = ""
        self._global_sem = global_sem or asyncio.Semaphore(32)
        self._per_user_sem = per_user_sem or asyncio.Semaphore(4)
        self._max_retries = max(1, int(max_retries))
        # Folder name -> folder_id cache. Seeded by a single GET on first
        # access so we don't create duplicates across retries or across
        # parallel jobs for the same user.
        self._folder_cache: Dict[str, Optional[str]] = {}
        self._folder_cache_loaded = False
        self._folder_lock = asyncio.Lock()

    # ─── folder resolution ───────────────────────────────────────

    async def _load_folder_cache(self) -> None:
        if self._folder_cache_loaded:
            return
        async with self._folder_lock:
            if self._folder_cache_loaded:
                return
            try:
                existing = await self.graph.list_contact_folders(self.user_id)
            except Exception as e:
                # Non-fatal: restore still works for the default folder.
                print(
                    f"[{self.worker_id}] [CONTACT-RESTORE] list_contact_folders "
                    f"failed for user={self.user_id}: {e}; assuming empty"
                )
                existing = []
            for f in existing:
                name = (f.get("displayName") or "").strip()
                if name:
                    self._folder_cache[name] = f.get("id")
            self._folder_cache_loaded = True

    async def _ensure_folder(self, name: str) -> Optional[str]:
        """Return the folder_id for ``name``, creating it if missing.
        Returns ``None`` when the caller should fall back to the default
        Contacts folder (name empty, no permission, or a benign failure).

        Error handling here is important — the earlier revision called
        ``_load_folder_cache`` from inside the held ``_folder_lock`` on
        create failure, which deadlocked because asyncio ``Lock`` is
        non-reentrant. Classify and return instead of re-listing under
        the lock."""
        if not name:
            return None
        await self._load_folder_cache()
        fid = self._folder_cache.get(name)
        if fid is not None:
            return fid
        async with self._folder_lock:
            # Double-check under the lock in case another task populated.
            fid = self._folder_cache.get(name)
            if fid is not None:
                return fid
            try:
                created = await self.graph.create_contact_folder(self.user_id, name)
                fid = created.get("id")
                self._folder_cache[name] = fid
                return fid
            except httpx.HTTPStatusError as e:
                status = e.response.status_code if e.response is not None else 0
                if status in (401, 403):
                    # Tenant hasn't consented contactFolder write scope —
                    # permanent for this user. Cache a negative result so
                    # the rest of the batch falls back to the default
                    # folder (or also fails there) without another
                    # round-trip per task.
                    self._folder_cache[name] = None
                    print(
                        f"[{self.worker_id}] [CONTACT-RESTORE] create_contact_folder "
                        f"{name!r} denied (status={status}); falling back to default folder"
                    )
                    return None
                if status == 409:
                    # Race: another worker/session already created the
                    # folder. Let the NEXT caller repopulate the cache;
                    # this task falls back to default-folder for safety.
                    self._folder_cache_loaded = False
                    print(
                        f"[{self.worker_id}] [CONTACT-RESTORE] create_contact_folder "
                        f"{name!r} conflict (409); will refresh on next access"
                    )
                    return None
                # Other HTTP errors (429/5xx): BatchClient retries at the
                # subrequest layer; this path is only hit on a non-batch
                # exception from create_contact_folder. Log and fall back.
                print(
                    f"[{self.worker_id}] [CONTACT-RESTORE] create_contact_folder "
                    f"{name!r} http error {status}: {e}; falling back to default folder"
                )
                self._folder_cache[name] = None
                return None
            except Exception as e:
                print(
                    f"[{self.worker_id}] [CONTACT-RESTORE] create_contact_folder "
                    f"{name!r} unexpected: {type(e).__name__}: {e}; "
                    f"falling back to default folder"
                )
                self._folder_cache[name] = None
                return None

    async def _resolve_dest_folder(self, task: _ContactTask) -> None:
        """Populate ``task.dest_folder_id`` based on mode + source folder."""
        src = _normalize_folder(task.source_folder_path)
        if self.mode == MODE_SEPARATE:
            # Collapse everything into the timestamped root so restores
            # don't collide with existing contacts in the user's default
            # folder. The (admittedly lossy) source folder is encoded
            # into the displayName prefix for traceability.
            target = self._separate_root
            task.dest_folder_path = target
            task.dest_folder_id = await self._ensure_folder(target)
            if src:
                # Preserve provenance in displayName so a user can tell
                # which original folder a restored row came from.
                dn = task.payload.get("displayName") or task.display_name
                task.payload["displayName"] = f"[{src}] {dn}"
            return
        # OVERWRITE — route back into the original folder if named,
        # else the default Contacts folder.
        task.dest_folder_path = src
        task.dest_folder_id = await self._ensure_folder(src) if src else None

    # ─── batching ────────────────────────────────────────────────

    def _build_sub_url(self, task: _ContactTask) -> str:
        if task.dest_folder_id:
            return f"/users/{self.user_id}/contactFolders/{task.dest_folder_id}/contacts"
        return f"/users/{self.user_id}/contacts"

    async def _send_batch(self, tasks: List[_ContactTask]) -> None:
        """POST a single /$batch to Graph with up to _BATCH_SIZE contacts
        and merge the outcomes back into the run stats."""
        if not tasks:
            return
        # BatchRequest.id maps response → task
        reqs = [
            BatchRequest(
                id=str(i),
                method="POST",
                url=self._build_sub_url(t),
                headers={"Content-Type": "application/json"},
                body=self.graph.clean_contact_payload(t.payload),
            )
            for i, t in enumerate(tasks)
        ]
        by_id = {str(i): t for i, t in enumerate(tasks)}

        client = BatchClient(self.graph)
        async with self._global_sem:
            async with self._per_user_sem:
                self._stats.batches_sent += 1
                try:
                    responses = await client.batch(reqs)
                except Exception as e:
                    # Whole-batch failure (network, auth, token refresh) —
                    # fail each task once, let the caller decide to retry
                    # the logical items via RabbitMQ redelivery.
                    for t in tasks:
                        self._record_failure(t, f"batch submit: {e}")
                    return

        # Per-subrequest bookkeeping. 429/503 have already been retried
        # inside BatchClient; if they're still here the retry budget
        # ran out.
        retry_bucket: List[_ContactTask] = []
        for sub_id, resp in responses.items():
            t = by_id.get(sub_id)
            if t is None:
                continue
            if 200 <= resp.status < 300:
                self._record_created(t)
                continue
            if resp.status == 404 and t.dest_folder_id is not None:
                # Folder id may be stale (deleted by the user between
                # list and POST). Force a refresh + one more try at the
                # individual level.
                if t.attempts < self._max_retries:
                    t.attempts += 1
                    t.dest_folder_id = None
                    self._folder_cache_loaded = False
                    retry_bucket.append(t)
                    continue
            self._record_failure(
                t,
                f"status={resp.status} body={_resp_snippet(resp.body)}",
            )

        # Re-enqueue 404-bucket items via single-item batches so a stale
        # folder reference doesn't sink a full batch of 20.
        for t in retry_bucket:
            await self._resolve_dest_folder(t)
            await self._send_batch([t])

    def _record_created(self, t: _ContactTask) -> None:
        self._stats.created += 1
        self._stats.items.append(
            _Outcome(
                item_id=t.item_id, external_id=t.external_id,
                display_name=t.display_name, outcome="created",
            )
        )

    def _record_failure(self, t: _ContactTask, reason: str) -> None:
        self._stats.failed += 1
        self._stats.items.append(
            _Outcome(
                item_id=t.item_id, external_id=t.external_id,
                display_name=t.display_name, outcome="failed",
                reason=reason[:500],
            )
        )

    # ─── orchestration ───────────────────────────────────────────

    # Hard ceiling on a single run. Sized for the 5k-user design point:
    # ~500 contacts per user × 20-per-batch × ~2s per batch = ~50s/user.
    # With PER_USER=4 concurrency = ~12s p50 per user. 30 min is a large
    # safety margin; tune via env if larger restores become routine.
    _RUN_TIMEOUT_SEC = 30 * 60

    async def run(self, items: List[Any]) -> Dict[str, Any]:
        """Main entry point. ``items`` is a list of SnapshotItem rows
        with item_type == USER_CONTACT."""
        self._stats = _RunStats()
        try:
            return await asyncio.wait_for(
                self._run_inner(items), timeout=self._RUN_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            print(
                f"[{self.worker_id}] [CONTACT-RESTORE] TIMEOUT after "
                f"{self._RUN_TIMEOUT_SEC}s user={self.user_id} "
                f"items={len(items)}; marking remaining as failed",
                flush=True,
            )
            # Best-effort: whatever made it into _stats.items is real;
            # the rest count as failed so the caller isn't lying to the
            # user about success.
            pending = len(items) - (self._stats.created + self._stats.failed + self._stats.skipped)
            for _ in range(max(0, pending)):
                self._stats.failed += 1
            return self._serialize()

    async def _run_inner(self, items: List[Any]) -> Dict[str, Any]:
        if not items:
            return self._serialize()

        # Build task list. Items whose metadata lacks a raw Graph
        # payload get a minimal displayName-only payload — matches the
        # legacy _restore_contact_to_mailbox fallback.
        tasks: List[_ContactTask] = []
        for it in items:
            meta = getattr(it, "extra_data", None) or getattr(it, "metadata", None) or {}
            raw = meta.get("raw") or {}
            if not raw:
                raw = {"displayName": getattr(it, "name", None) or "Restored contact"}
            tasks.append(_ContactTask(
                item_id=str(getattr(it, "id", "")),
                external_id=str(getattr(it, "external_id", "") or ""),
                display_name=(raw.get("displayName") or getattr(it, "name", None) or ""),
                payload=raw,
                source_folder_path=str(getattr(it, "folder_path", None) or ""),
                dest_folder_path="",
            ))

        # Folder-ensure phase — dedupe by destination before calling Graph
        # so we make at most one create per distinct destination.
        resolves = []
        for t in tasks:
            resolves.append(self._resolve_dest_folder(t))
        await asyncio.gather(*resolves, return_exceptions=True)
        self._stats.folders_ensured = sum(
            1 for v in self._folder_cache.values() if v is not None
        )

        # Partition by dest_folder_id so every batch hits a single
        # URL. Outlook's 4-at-a-time serialization is per mailbox, not
        # per folder, so mixing folders across batches is fine; we only
        # group for URL-uniformity inside BatchRequest.
        by_dest: Dict[Optional[str], List[_ContactTask]] = {}
        for t in tasks:
            by_dest.setdefault(t.dest_folder_id, []).append(t)

        # Fire all batches in parallel; global + per-user semaphores
        # gate real concurrency.
        batch_coros = []
        for _, bucket in by_dest.items():
            for i in range(0, len(bucket), _BATCH_SIZE):
                batch_coros.append(self._send_batch(bucket[i:i + _BATCH_SIZE]))
        await asyncio.gather(*batch_coros, return_exceptions=False)

        print(
            f"[{self.worker_id}] [CONTACT-RESTORE] summary user={self.user_id} "
            f"created={self._stats.created} failed={self._stats.failed} "
            f"skipped={self._stats.skipped} folders_ensured={self._stats.folders_ensured} "
            f"batches={self._stats.batches_sent} mode={self.mode}",
            flush=True,
        )
        return self._serialize()

    def _serialize(self) -> Dict[str, Any]:
        return {
            "created": self._stats.created,
            "failed": self._stats.failed,
            "skipped": self._stats.skipped,
            "folders_ensured": self._stats.folders_ensured,
            "batches_sent": self._stats.batches_sent,
            "items": [
                {
                    "item_id": o.item_id,
                    "external_id": o.external_id,
                    "display_name": o.display_name,
                    "outcome": o.outcome,
                    "reason": o.reason,
                }
                for o in self._stats.items
            ],
        }


def _resp_snippet(body: Any) -> str:
    """Pull a useful error string out of a Graph subresponse body."""
    if isinstance(body, dict):
        err = body.get("error") or {}
        if isinstance(err, dict):
            return f"{err.get('code', '')}: {err.get('message', '')}"
        return str(body)[:200]
    return str(body)[:200]
