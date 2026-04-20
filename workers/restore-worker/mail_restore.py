"""MailRestoreEngine — AFI-parity mail restore pipeline.

Five phases:
    plan           partition items by (tier, folder_path); group
                   attachments under their parent email.
    ensure_folders one pass per (tier, path) to look up or create the
                   target folder; caches paths for the job.
    build_sieve    overwrite mode only — per target folder, pull
                   `{internetMessageId → graphMessageId}` to decide
                   create-vs-patch.
    create_or_patch per-item worker-pool drains the plan, creating new
                   messages + attachments or patching mutable metadata
                   on matched messages.
    report         progress + per-item outcome summary.

See docs/superpowers/specs/2026-04-20-mail-restore-afi-parity-design.md
"""
from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from shared.azure_storage import azure_storage_manager
from shared.config import settings
from shared.graph_client import GraphClient
from shared.models import Resource, SnapshotItem
from shared._graph_retry import _is_retryable, _retry_after_seconds  # noqa: F401  (re-exported so mail tests work unchanged)


# ----- Mode enum-ish ---------------------------------------------------

MODE_SEPARATE = "SEPARATE_FOLDER"
MODE_OVERWRITE = "OVERWRITE"


# ----- Plan types ------------------------------------------------------

BucketKey = Tuple[str, str]   # (tier, folder_path)


@dataclass
class MailRestorePlan:
    """Output of phase 1 — describes what will be touched."""
    # (tier, path) → list of EMAIL SnapshotItems destined for that bucket.
    messages: Dict[BucketKey, List[SnapshotItem]] = field(default_factory=dict)
    # Parent message external_id → EMAIL_ATTACHMENT SnapshotItems for it.
    attachments: Dict[str, List[SnapshotItem]] = field(default_factory=dict)


@dataclass
class ItemOutcome:
    item_id: str
    external_id: str
    outcome: str             # "created" | "updated" | "skipped" | "failed"
    graph_message_id: Optional[str] = None
    reason: Optional[str] = None


# ----- Engine ----------------------------------------------------------

class MailRestoreEngine:
    """Stateless-ish orchestrator. One instance per (job, target mailbox);
    the per-mailbox concurrency semaphore is scoped to this instance, so
    callers MUST NOT reuse a single engine across multiple target
    mailboxes — that would let one mailbox's traffic consume another's
    concurrency budget. The folder-ensure cache lives on the underlying
    GraphClient and is safe to share."""

    def __init__(
        self,
        graph_client: GraphClient,
        target_resource: Resource,
        mode: str,
        *,
        separate_folder_root: Optional[str] = None,
        worker_id: str = "",
        graph_user_id: Optional[str] = None,
    ):
        self.graph = graph_client
        self.target = target_resource
        # Real Graph user id for Microsoft Graph URL construction. Tier 1
        # mailbox rows (MAILBOX/SHARED_MAILBOX/ROOM_MAILBOX) store the
        # Graph user id directly in `external_id`. Tier 2 USER_MAIL rows
        # append a `:mail` suffix for uniqueness, so the caller MUST pass
        # the suffix-free user id via `graph_user_id`.
        self.graph_user_id = graph_user_id or target_resource.external_id
        self.mode = mode if mode in (MODE_SEPARATE, MODE_OVERWRITE) else MODE_SEPARATE
        # In SEPARATE mode every original path is prefixed with the user-
        # supplied root (e.g. "/Restored by TM/2026-04-20"). An empty root
        # falls back to a timestamped default so the original tree never
        # collides with live data.
        if not separate_folder_root:
            separate_folder_root = f"/Restored by TM/{datetime.utcnow().strftime('%Y-%m-%d')}"
        if not separate_folder_root.startswith("/"):
            separate_folder_root = "/" + separate_folder_root
        self.separate_folder_root = separate_folder_root.rstrip("/")
        self.worker_id = worker_id

    # ---- phase 1 ----

    @staticmethod
    def build_plan(items: List[SnapshotItem]) -> MailRestorePlan:
        """Partition a flat list of SnapshotItems into a MailRestorePlan.
        EMAIL → bucketed by (tier, folder_path). EMAIL_ATTACHMENT → grouped
        under its parent message external_id so the create phase can emit
        attachments in the same breath as the new message."""
        plan = MailRestorePlan()
        for it in items:
            kind = getattr(it, "item_type", None)
            extra = getattr(it, "extra_data", None) or {}
            if kind == "EMAIL":
                tier = extra.get("_source_mailbox") or "primary"
                path = (getattr(it, "folder_path", None) or "").strip() or "/"
                plan.messages.setdefault((tier, path), []).append(it)
            elif kind == "EMAIL_ATTACHMENT":
                parent = extra.get("parent_item_id")
                if not parent:
                    # Orphaned attachment — nothing to hang it off. The
                    # caller may report these as skipped; we just don't
                    # lose them on the floor.
                    parent = f"__orphan__/{it.external_id}"
                plan.attachments.setdefault(parent, []).append(it)
        # Ensure every message has an attachment entry (possibly empty) —
        # simplifies the create phase which always does a dict lookup.
        for bucket in plan.messages.values():
            for msg in bucket:
                plan.attachments.setdefault(msg.external_id, [])
        return plan

    def resolve_target_path(self, source_path: str) -> str:
        """Compute the destination path for a message whose source path
        was `source_path`. OVERWRITE → original path unchanged. SEPARATE
        → prefix with the configured root."""
        src = source_path or "/"
        if self.mode == MODE_OVERWRITE:
            return src
        # SEPARATE: "/Inbox/Project X" → "/Restored by TM/2026-04-20/Inbox/Project X"
        if src == "/" or not src:
            return self.separate_folder_root
        return f"{self.separate_folder_root}{src if src.startswith('/') else '/' + src}"

    # ---- phase 2 ----

    async def ensure_folders(
        self,
        plan: MailRestorePlan,
    ) -> Dict[BucketKey, Optional[str]]:
        """Resolve each unique (tier, target_path) bucket to a folder id
        on the target mailbox. Creates missing folders as needed.

        Returns a map keyed by the ORIGINAL (tier, source_path) bucket so
        the create phase can look up `folder_map[key]` directly. None =
        tier root not provisioned; caller surfaces those items as
        failed:`target_archive_not_provisioned`.
        """
        unique_buckets = list(plan.messages.keys())
        out: Dict[BucketKey, Optional[str]] = {}
        # Deduplicate concurrent requests to the same (tier, target_path)
        # so two different source paths mapping to the same target path
        # in SEPARATE mode don't double-look-up.
        resolved: Dict[Tuple[str, str], Optional[str]] = {}
        for tier, source_path in unique_buckets:
            target_path = self.resolve_target_path(source_path)
            if (tier, target_path) in resolved:
                out[(tier, source_path)] = resolved[(tier, target_path)]
                continue
            fid = await self.graph.ensure_mail_folder_path(
                self.graph_user_id, tier, target_path,
            )
            resolved[(tier, target_path)] = fid
            out[(tier, source_path)] = fid
        return out

    # ---- phase 3 ----

    async def build_sieve(
        self,
        folder_map: Dict[BucketKey, Optional[str]],
    ) -> Dict[str, Dict[str, str]]:
        """Overwrite mode only. For each target folder that actually
        resolved, pull its current internetMessageId → graphMessageId map.

        Returns `{folder_id: {imid: msg_id}}`. Separate mode returns an
        empty dict (no dedup needed)."""
        if self.mode != MODE_OVERWRITE:
            return {}
        out: Dict[str, Dict[str, str]] = {}
        seen: Set[str] = set()
        for fid in folder_map.values():
            if not fid or fid in seen:
                continue
            seen.add(fid)
            try:
                out[fid] = await self.graph.list_folder_internet_message_ids(
                    self.graph_user_id, fid,
                )
            except Exception as e:
                print(f"[{self.worker_id}] [MAIL-RESTORE] sieve fetch failed for folder {fid}: {type(e).__name__}: {e}")
                out[fid] = {}
        return out

    # Allowlisted fields for the create payload. Graph rejects / overwrites
    # anything outside this set for POST /messages. `conversationId` and
    # `id` are server-assigned. AFI matches this policy.
    _CREATE_FIELDS = (
        "subject", "body", "toRecipients", "ccRecipients", "bccRecipients",
        "sentDateTime", "receivedDateTime", "internetMessageId",
        "importance", "isRead", "flag", "categories",
    )

    @classmethod
    def shape_message_payload(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Project a snapshot's raw Graph payload onto the allowlist for
        POST /messages. Drops Graph-assigned identifiers and anything
        outside the restore contract."""
        out: Dict[str, Any] = {}
        for k in cls._CREATE_FIELDS:
            if k in raw:
                out[k] = raw[k]
        return out

    # ---- phase 4 (per-item) ----

    async def restore_one_message(
        self,
        msg_item: SnapshotItem,
        *,
        folder_id: Optional[str],
        sieve: Dict[str, Dict[str, str]],
        attachments: List[SnapshotItem],
    ) -> ItemOutcome:
        """Restore a single EMAIL item. Delegates to PATCH (overwrite
        match) or CREATE + attachment replay.

        Raises retryable HTTP errors (429 / 5xx) so the outer `run()`
        loop can apply `Retry-After` / exponential backoff. Terminal
        errors (400, 404, non-HTTP exceptions) are captured in the
        returned ItemOutcome and never propagate."""
        raw = (msg_item.extra_data or {}).get("raw") or {}
        imid = raw.get("internetMessageId")

        # Tier root missing — reported per the design.
        if folder_id is None:
            return ItemOutcome(
                item_id=str(msg_item.id),
                external_id=msg_item.external_id,
                outcome="failed",
                reason="target_archive_not_provisioned",
            )

        # Overwrite-mode match → patch only.
        if self.mode == MODE_OVERWRITE and imid:
            existing_id = sieve.get(folder_id, {}).get(imid)
            if existing_id:
                try:
                    await self.graph.patch_message_metadata(
                        self.graph_user_id, existing_id, raw,
                    )
                    return ItemOutcome(
                        item_id=str(msg_item.id),
                        external_id=msg_item.external_id,
                        outcome="updated",
                        graph_message_id=existing_id,
                    )
                except Exception as e:
                    if _is_retryable(e):
                        raise
                    return ItemOutcome(
                        item_id=str(msg_item.id),
                        external_id=msg_item.external_id,
                        outcome="failed",
                        reason=f"patch_error: {type(e).__name__}: {e}",
                    )

        # Otherwise create.
        payload = self.shape_message_payload(raw)
        try:
            new_id = await self.graph.create_message_in_folder(
                self.graph_user_id, folder_id, payload,
            )
        except Exception as e:
            if _is_retryable(e):
                raise
            return ItemOutcome(
                item_id=str(msg_item.id),
                external_id=msg_item.external_id,
                outcome="failed",
                reason=f"create_error: {type(e).__name__}: {e}",
            )
        if not new_id:
            return ItemOutcome(
                item_id=str(msg_item.id),
                external_id=msg_item.external_id,
                outcome="failed",
                reason="create_returned_no_id",
            )

        # Attachment replay. Failures here don't void the parent create
        # — we report the parent as "created" and attach a reason string
        # noting how many attachments failed.
        if attachments:
            att_failed = await self._replay_attachments(new_id, attachments)
        else:
            att_failed = 0

        reason = f"attachments_failed={att_failed}" if att_failed else None
        return ItemOutcome(
            item_id=str(msg_item.id),
            external_id=msg_item.external_id,
            outcome="created",
            graph_message_id=new_id,
            reason=reason,
        )

    async def _replay_attachments(
        self,
        new_message_id: str,
        attachments: List[SnapshotItem],
    ) -> int:
        """Replay each attachment onto the freshly-created message.
        Returns the count of attachments that failed to replay."""
        failed = 0
        large_threshold = settings.MAIL_RESTORE_ATTACH_LARGE_MB * 1024 * 1024
        for att in attachments:
            ed = att.extra_data or {}
            kind = (ed.get("attachment_kind") or "").lower()
            name = att.name or "attachment"
            try:
                # fileAttachment is the common case. When the backup
                # stored no explicit @odata.type (empty `kind`) but did
                # capture blob bytes, treat it as a fileAttachment — the
                # blob is the only thing we can reconstruct. Do NOT fall
                # into this branch for itemAttachment / referenceAttachment
                # kinds even if `resolved` is true, because those have
                # their own handlers below.
                is_file = "fileattachment" in kind or (not kind and att.blob_path and ed.get("resolved"))
                if is_file:
                    content_bytes = await self._read_attachment_blob(att)
                    if content_bytes is None:
                        failed += 1
                        continue
                    if len(content_bytes) >= large_threshold:
                        await self.graph.upload_large_attachment(
                            self.graph_user_id,
                            new_message_id,
                            name=name,
                            size=len(content_bytes),
                            content_bytes=content_bytes,
                            content_type=ed.get("content_type"),
                            is_inline=bool(ed.get("is_inline")),
                        )
                    else:
                        await self.graph.post_small_attachment(
                            self.graph_user_id,
                            new_message_id,
                            {
                                "@odata.type": "#microsoft.graph.fileAttachment",
                                "name": name,
                                "contentType": ed.get("content_type") or "application/octet-stream",
                                "isInline": bool(ed.get("is_inline")),
                                "contentBytes": base64.b64encode(content_bytes).decode("ascii"),
                            },
                        )
                elif "itemattachment" in kind:
                    raw_bytes = await self._read_attachment_blob(att)
                    inner = {}
                    if raw_bytes:
                        try:
                            inner = json.loads(raw_bytes.decode("utf-8"))
                        except Exception:
                            inner = {}
                    await self.graph.post_small_attachment(
                        self.graph_user_id,
                        new_message_id,
                        {
                            "@odata.type": "#microsoft.graph.itemAttachment",
                            "name": name,
                            "item": inner,
                        },
                    )
                elif "referenceattachment" in kind:
                    source_url = ed.get("source_url")
                    if not source_url:
                        failed += 1
                        continue
                    await self.graph.post_small_attachment(
                        self.graph_user_id,
                        new_message_id,
                        {
                            "@odata.type": "#microsoft.graph.referenceAttachment",
                            "name": name,
                            "sourceUrl": source_url,
                            "providerType": "other",
                            "permission": "view",
                            "isFolder": False,
                        },
                    )
                else:
                    failed += 1
            except Exception as e:
                print(f"[{self.worker_id}] [MAIL-RESTORE] attachment {name} failed: {type(e).__name__}: {e}")
                failed += 1
        return failed

    async def _read_attachment_blob(self, att: SnapshotItem) -> Optional[bytes]:
        """Read an EMAIL_ATTACHMENT's blob bytes. Returns None on failure.

        Uses the shard routing from azure_storage_manager (same pattern as
        _download_blob_content in the file restore worker). Mail attachment
        blobs are written to the "email" container; "mailbox" is the legacy
        fallback."""
        if not getattr(att, "blob_path", None):
            return None
        try:
            tenant_id = str(self.target.tenant_id)
            shard = azure_storage_manager.get_shard_for_resource(tenant_id, tenant_id)
            container = azure_storage_manager.get_container_name(tenant_id, "email")
            blob_client = shard.get_blob_client(container, att.blob_path)
            stream = await blob_client.download_blob()
            return await stream.readall()
        except Exception as e:
            print(f"[{self.worker_id}] [MAIL-RESTORE] blob read failed {att.blob_path}: {type(e).__name__}: {e}")
            return None

    async def run(self, items: List[SnapshotItem]) -> Dict[str, Any]:
        """Top-level driver — plan → ensure → sieve → concurrent
        create/patch → aggregate."""
        plan = self.build_plan(items)
        if not plan.messages:
            return {"created": 0, "updated": 0, "skipped": 0, "failed": 0, "items": []}

        folder_map = await self.ensure_folders(plan)
        sieve = await self.build_sieve(folder_map)

        global_sem = asyncio.Semaphore(settings.MAIL_RESTORE_GLOBAL_POOL)
        per_mailbox_sem = asyncio.Semaphore(settings.MAIL_RESTORE_PER_MAILBOX)

        async def one(msg, folder_id):
            async with global_sem, per_mailbox_sem:
                attempt = 0
                while True:
                    try:
                        return await self.restore_one_message(
                            msg,
                            folder_id=folder_id,
                            sieve=sieve,
                            attachments=plan.attachments.get(msg.external_id, []),
                        )
                    except Exception as e:
                        if _is_retryable(e) and attempt < settings.MAIL_RESTORE_MAX_RETRIES:
                            delay = _retry_after_seconds(e)
                            if delay is None:
                                delay = min(1.0 * (2 ** attempt), 16.0)
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        return ItemOutcome(
                            item_id=str(msg.id),
                            external_id=msg.external_id,
                            outcome="failed",
                            reason=f"exhausted: {type(e).__name__}: {e}",
                        )

        tasks: List[asyncio.Task] = []
        for bucket_key, msgs in plan.messages.items():
            folder_id = folder_map.get(bucket_key)
            for msg in msgs:
                tasks.append(asyncio.create_task(one(msg, folder_id)))
        outcomes: List[ItemOutcome] = await asyncio.gather(*tasks)

        summary = {"created": 0, "updated": 0, "skipped": 0, "failed": 0}
        for o in outcomes:
            summary[o.outcome] = summary.get(o.outcome, 0) + 1
        summary["items"] = [o.__dict__ for o in outcomes]
        return summary
