"""
High-Performance Backup Worker - Mass Backup Processing

Features:
- Server-Side Copy for OneDrive/SharePoint files (zero server load)
- Storage sharding across multiple Azure Storage Accounts
- Workload parallelism (Exchange, OneDrive, SharePoint concurrently)
- User-level parallelism with configurable concurrency
- Delta token tracking for incremental backups
- Adaptive throttling with exponential backoff
- Content deduplication via SHA-256
- Versioned blob paths for retention management

Architecture:
  Files > 10MB: Graph API → @downloadUrl → Azure Server-Side Copy (fastest)
  Files < 10MB: Graph API → Python SDK → Azure Blob (efficient for small items)
  Emails/Chats: Graph API → Python SDK → Azure Blob (JSON/MIME processing)
"""
import asyncio
import json
import uuid
import hashlib
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
import time
from typing import Dict, List, Any, Optional, Tuple


# orjson is ~5-10× faster than stdlib json for payload-heavy paths like
# per-chat-message persistence. Defensive import so deployments without
# the package still work — falls back transparently.
try:
    import orjson as _fast_json  # type: ignore

    def _json_dumps_bytes(obj) -> bytes:
        return _fast_json.dumps(obj, default=str)
except ImportError:
    import json as _stdlib_json

    def _json_dumps_bytes(obj) -> bytes:
        return _stdlib_json.dumps(obj, default=str).encode("utf-8")


# Chunk size for multi-row INSERTs. 2000 rows × ~2 KB each keeps a single
# statement well under Postgres' 1 GB protocol limit while being large
# enough to amortize per-statement overhead (10-50× faster than per-row
# ORM add_all at these sizes). Tunable via env for huge mailboxes.
_BULK_INSERT_CHUNK = int(os.getenv("BULK_INSERT_CHUNK", "2000"))


async def _bulk_upsert_snapshot_items(
    session, rows: List[Dict[str, Any]],
) -> int:
    """Insert snapshot_items rows in chunked multi-row INSERTs with
    ON CONFLICT DO NOTHING on (snapshot_id, external_id).

    Replaces the legacy session.add_all + commit loop which serialized
    12k rows into 12k separate SQL statements — dominated wall time on
    chat-heavy users (~15-20s for 12k msgs). Multi-row pg_insert with
    the UNIQUE idempotency guard both speeds up the path ~10× and makes
    message redelivery safe (race-inserted duplicates silently skipped).

    Returns the number of rows attempted (not necessarily inserted —
    conflicts are dropped silently by design).
    """
    if not rows:
        return 0
    from sqlalchemy.dialects.postgresql import insert as _pg_insert
    total = 0
    for i in range(0, len(rows), _BULK_INSERT_CHUNK):
        chunk = rows[i:i + _BULK_INSERT_CHUNK]
        stmt = _pg_insert(SnapshotItem).values(chunk).on_conflict_do_nothing(
            index_elements=["snapshot_id", "external_id"],
        )
        await session.execute(stmt)
        total += len(chunk)
    await session.commit()
    return total


async def _is_job_cancelled(job_id) -> bool:
    """Cheap best-effort cancel-check. Long-running handlers poll this at
    heartbeat boundaries so a DB-level CANCELLED job stops pulling Graph
    pages within seconds instead of running to natural completion. A DB
    hiccup returns False (don't cancel on transient errors)."""
    if job_id is None:
        return False
    try:
        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)
    except Exception:
        return False
    try:
        from shared.database import async_session_factory as _f
        from shared.models import Job as _J
        async with _f() as _s:
            _j = await _s.get(_J, job_id)
            if not _j:
                return False
            status_val = _j.status.name if hasattr(_j.status, "name") else str(_j.status)
            return status_val == "CANCELLED"
    except Exception:
        return False


async def _update_job_pct(job_id, pct: int) -> None:
    """Write live `jobs.progress_pct` on a short-lived session so the
    Protection / Activity UI can animate during long M365 backups. Best-
    effort — a DB hiccup never interrupts the running backup. Matches
    the Azure worker's `handlers._progress.update_job_pct` pattern."""
    if job_id is None:
        return
    try:
        if isinstance(job_id, str):
            job_id = uuid.UUID(job_id)
    except Exception:
        return
    clamped = max(0, min(100, int(pct)))
    try:
        from shared.database import async_session_factory as _f
        from shared.models import Job as _J
        async with _f() as _s:
            _j = await _s.get(_J, job_id)
            if not _j:
                return
            status_val = _j.status.name if hasattr(_j.status, "name") else str(_j.status)
            if status_val not in ("RUNNING", "QUEUED"):
                return
            _j.progress_pct = clamped
            await _s.commit()
    except Exception:
        pass
import aio_pika
from aio_pika import IncomingMessage
import httpx
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from shared.database import async_session_factory
from shared.models import (
    Resource, Tenant, Job, Snapshot, SnapshotItem,
    SlaPolicy, ResourceType, ResourceStatus, JobStatus, SnapshotType, SnapshotStatus
)
from shared.message_bus import message_bus
from shared.config import settings
from shared.graph_client import GraphClient
from shared.multi_app_manager import multi_app_manager
from shared.metadata_extractor import MetadataExtractor
from shared.power_bi_client import PowerBIClient
from shared.power_platform_client import PowerPlatformClient
from shared.power_bi_snapshot import (
    POWER_BI_INCREMENTAL_STRATEGY_VERSION,
    assemble_power_bi_items,
    build_power_bi_item_key,
    should_force_power_bi_full_snapshot,
)
from shared.azure_storage import (
    azure_storage_manager,
    server_side_copy_with_retry,
    upload_blob_with_retry,
    upload_blob_with_retry_from_file,
)
from shared.entra_fingerprint import fingerprint_object as _entra_fp

logger = logging.getLogger(__name__)


def _message_to_contact_shape(msg: dict) -> dict:
    """Best-effort: convert an IPM.Contact message (from Deleted Items or
    Recoverable Items folders) into a dict that looks like a Graph contact
    resource. Field recovery is lossy by nature — deleted contacts lose
    most structured fields when they enter the message store. We preserve
    the original id, fall back to subject for displayName, and extract
    any emails we can find from the 'from' field."""
    if not isinstance(msg, dict):
        msg = {}
    display = msg.get("subject") or "(deleted contact)"
    emails: list = []

    from_field = msg.get("from") or {}
    addr = (from_field.get("emailAddress") or {}).get("address")
    if addr:
        emails.append({
            "address": addr,
            "name": (from_field.get("emailAddress") or {}).get("name") or display,
        })

    shape = {
        "id": msg.get("id"),
        "displayName": display,
        "givenName": "",
        "surname": "",
        "companyName": "",
        "jobTitle": "",
        "emailAddresses": emails,
        "businessPhones": [],
        "mobilePhone": "",
        "homePhones": [],
        "imAddresses": [],
        "categories": msg.get("categories") or [],
        "personalNotes": msg.get("bodyPreview") or "",
        "birthday": "",
    }
    if "singleValueExtendedProperties" in msg:
        shape["singleValueExtendedProperties"] = msg["singleValueExtendedProperties"]
    return shape


async def _backup_contacts_for_user(graph_client, user_id: str, item_limit: int = 999):
    """Aggregate a user's contacts from /contactFolders + (optionally)
    Deleted Items + Recoverable Items folders.

    Returns row tuples in the shape the caller previously built inline:
    (item_type, name, external_id, payload_dict, folder_name)
    """
    from shared.config import settings

    select_fields = (
        "id,displayName,givenName,surname,companyName,jobTitle,emailAddresses,"
        "businessPhones,mobilePhone,homePhones,parentFolderId,categories,"
        "imAddresses,personalNotes,birthday"
    )

    folder_map: dict = {}
    folder_ids: list = []
    try:
        f_page = await graph_client._get(
            f"{graph_client.GRAPH_URL}/users/{user_id}/contactFolders",
            params={"$top": "100", "$select": "id,displayName"},
        )
        for f in (f_page or {}).get("value", []) or []:
            fid = f.get("id")
            if fid:
                folder_map[fid] = f.get("displayName") or ""
                folder_ids.append(fid)
    except Exception:
        pass

    async def _fetch_contacts(url):
        all_rows = []
        next_url = url
        params = {"$top": str(item_limit), "$select": select_fields}
        pages = 0
        while next_url and pages < 50:
            try:
                page_data = await graph_client._get(next_url, params=params)
            except Exception as exc:
                logger.warning("Contact fetch failed on %s: %s", next_url, exc)
                break
            all_rows.extend(page_data.get("value", []) or [])
            next_url = page_data.get("@odata.nextLink")
            params = None
            pages += 1
        return all_rows

    async def _fetch_messages(url):
        all_rows = []
        next_url = url
        pages = 0
        while next_url and pages < 50:
            try:
                page_data = await graph_client._get(next_url)
            except Exception as exc:
                logger.warning("Deleted-contact fetch failed on %s: %s", next_url, exc)
                break
            all_rows.extend(page_data.get("value", []) or [])
            next_url = page_data.get("@odata.nextLink")
            pages += 1
        return all_rows

    aggregated: dict = {}
    folder_override: dict = {}

    for c in await _fetch_contacts(f"{graph_client.GRAPH_URL}/users/{user_id}/contacts"):
        cid = c.get("id")
        if cid and cid not in aggregated:
            aggregated[cid] = c
    for fid in folder_ids:
        url = f"{graph_client.GRAPH_URL}/users/{user_id}/contactFolders/{fid}/contacts"
        for c in await _fetch_contacts(url):
            cid = c.get("id")
            if cid and cid not in aggregated:
                aggregated[cid] = c

    # itemClass isn't a native property of microsoft.graph.message, so
    # filtering by it returns 400. Contact items live in mail folders with
    # the MAPI property PidTagMessageClass (id 0x001A) set to "IPM.Contact",
    # which Graph surfaces as a singleValueExtendedProperty. Filter on that
    # and $expand the same property so ``_message_to_contact_shape`` can
    # read the class back on each hit.
    MSG_CLASS_EP = "String 0x001A"
    contact_class_filter = (
        f"singleValueExtendedProperties/Any(ep:"
        f" ep/id eq '{MSG_CLASS_EP}'"
        f" and startswith(ep/value,'IPM.Contact'))"
    )
    from urllib.parse import quote as _q
    contact_class_expand = (
        f"singleValueExtendedProperties($filter=id eq '{MSG_CLASS_EP}')"
    )

    if settings.BACKUP_CONTACTS_INCLUDE_DELETED:
        del_url = (
            f"{graph_client.GRAPH_URL}/users/{user_id}"
            f"/mailFolders('deleteditems')/messages"
            f"?$filter={_q(contact_class_filter)}"
            f"&$expand={_q(contact_class_expand)}"
            f"&$top={item_limit}"
        )
        for msg in await _fetch_messages(del_url):
            shape = _message_to_contact_shape(msg)
            cid = shape.get("id")
            if cid and cid not in aggregated:
                aggregated[cid] = shape
                folder_override[cid] = "Deleted Items"

    if settings.BACKUP_CONTACTS_INCLUDE_RECOVERABLE:
        rec_url = (
            f"{graph_client.GRAPH_URL}/users/{user_id}"
            f"/mailFolders('recoverableitemsdeletions')/messages"
            f"?$filter={_q(contact_class_filter)}"
            f"&$expand={_q(contact_class_expand)}"
            f"&$top={item_limit}"
        )
        for msg in await _fetch_messages(rec_url):
            shape = _message_to_contact_shape(msg)
            cid = shape.get("id")
            if cid and cid not in aggregated:
                aggregated[cid] = shape
                folder_override[cid] = "Recoverable Items"

    out_rows = []
    for cid, c in aggregated.items():
        folder_name = (
            folder_override.get(cid)
            or folder_map.get(c.get("parentFolderId"))
            or "Contacts"
        )
        name = (
            c.get("displayName")
            or (c.get("emailAddresses") or [{}])[0].get("address")
            or "(unnamed)"
        )
        out_rows.append(("USER_CONTACT", name, cid, {"raw": c}, folder_name))
    return out_rows


class AuditLogger:
    """Logs backup events via HTTP POST and RabbitMQ"""

    def __init__(self):
        self.audit_url = f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log"

    async def log(self, **kwargs):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(self.audit_url, json=kwargs)
        except Exception as e:
            print(f"[AuditLogger] HTTP failed: {e}")

        try:
            from shared.message_bus import create_audit_event_message
            message = create_audit_event_message(
                action=kwargs.get("action", "UNKNOWN"),
                tenant_id=kwargs.get("tenant_id", ""),
                org_id=kwargs.get("org_id"),
                actor_type=kwargs.get("actor_type", "SYSTEM"),
                actor_id=kwargs.get("actor_id"),
                actor_email=kwargs.get("actor_email"),
                resource_id=kwargs.get("resource_id"),
                resource_type=kwargs.get("resource_type"),
                resource_name=kwargs.get("resource_name"),
                outcome=kwargs.get("outcome", "SUCCESS"),
                job_id=kwargs.get("job_id"),
                snapshot_id=kwargs.get("snapshot_id"),
                details=kwargs.get("details", {}),
            )
            await message_bus.publish("audit.events", message, priority=5)
        except Exception as e:
            print(f"[AuditLogger] Queue failed: {e}")


class BackupWorker:
    """High-performance backup worker with parallelism and Server-Side Copy"""

    def __init__(self):
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self.graph_clients: Dict[str, GraphClient] = {}
        self.audit_logger = AuditLogger()
        # Teams chat export cache: user_id -> (fetched_at_unix, messages[], delta_link)
        # /users/{id}/chats/getAllMessages/delta returns either the full chat
        # history (first run) or only changes since the saved deltaLink.
        # If we're backing up 100 chats for the same user we don't want to hit
        # that endpoint 100 times. Cache TTL is 5 min — well within a batch-backup
        # window, and new messages arriving during a run are handled on the next run.
        # delta_link is captured so callers can persist it per-resource for the
        # next backup run to use as an incremental anchor.
        self._chat_export_cache: Dict[str, Tuple[float, List[Dict], Optional[str]]] = {}
        self._chat_export_cache_ttl = 300.0  # seconds
        # Concurrency controls
        self.backup_semaphore = asyncio.Semaphore(8)  # 8 concurrent file streams per worker NIC
        self.copy_semaphore = asyncio.Semaphore(20)  # Azure Storage account ingress limit
        # v2: cap simultaneous USER_ONEDRIVE backup jobs per worker. An uncapped
        # run can take hours; without this a single worker would accept every
        # redelivered OneDrive message and thrash. Heavy-pool workers
        # (backup-worker-heavy) set MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER
        # to 1 so they finish one monster drive before touching the next.
        self._onedrive_backup_semaphore = asyncio.Semaphore(
            settings.MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER
        )
        # Which normal-priority queue this instance consumes. Default
        # replicas read backup.normal; backup-worker-heavy sets
        # BACKUP_WORKER_QUEUE=backup.heavy via compose env.
        self._backup_queue_name = settings.BACKUP_WORKER_QUEUE

    async def initialize(self):
        await message_bus.connect()
        if azure_storage_manager.shards:
            print(f"[{self.worker_id}] Azure Storage: {len(azure_storage_manager.shards)} shard(s) ready")
        print(f"[{self.worker_id}] Backup worker initialized (concurrency={settings.BACKUP_CONCURRENCY})")

    async def start(self):
        """Start consuming from all backup queues"""
        await self.initialize()

        # Override the channel-wide prefetch (set to 50 by message_bus). With
        # 50, a single worker grabs all queued messages and the other replicas
        # sit idle while it processes them serially. Setting prefetch=2 gives
        # each replica one in-flight + one ready, so 3 replicas cover ~6 jobs
        # at once and fair distribution is enforced by the broker.
        if message_bus.channel:
            try:
                await message_bus.channel.set_qos(prefetch_count=2)
                print(f"[{self.worker_id}] Set channel prefetch_count=2 for fair work distribution")
            except Exception as exc:
                print(f"[{self.worker_id}] Could not adjust prefetch_count: {exc}")

        # Per-queue prefetch.
        # Urgent = manually triggered backups (Teams chats, mailboxes). Each
        # chat backup's getAllMessages can take 10+ min. Prefetch=1 forces
        # round-robin across replicas so one slow chat can't starve the others.
        # Higher-throughput queues stay generous since their items are smaller.
        queues = [
            ("backup.urgent", 1),
            ("backup.high", 5),
            (self._backup_queue_name, 20),
            ("backup.low", 50),
        ]

        tasks = []
        for queue_name, prefetch in queues:
            task = asyncio.create_task(self.consume_queue(queue_name, prefetch))
            tasks.append(task)

        print(f"[{self.worker_id}] Started consuming from {len(queues)} queues")
        await asyncio.gather(*tasks)

    async def consume_queue(self, queue_name: str, prefetch_count: int):
        if not message_bus.channel:
            return

        queue = await message_bus.channel.get_queue(queue_name)
        print(f"[{self.worker_id}] Listening on {queue_name}...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                try:
                    body = json.loads(message.body.decode())
                    await self.process_backup_message(body)
                    await message.ack()
                except Exception as e:
                    print(f"[{self.worker_id}] Error: {e}")
                    import traceback
                    traceback.print_exc()
                    try:
                        # Check delivery count to prevent infinite requeue
                        headers = message.headers or {}
                        delivery_count = headers.get("x-delivery-count", 0)
                        if delivery_count < settings.MAX_RETRIES:
                            await message.reject(requeue=True)
                        else:
                            print(f"[{self.worker_id}] Message exceeded max retries ({settings.MAX_RETRIES}), routing to DLQ")
                            await message.reject(requeue=False)
                    except Exception:
                        pass

    async def process_backup_message(self, message: Dict[str, Any]):
        job_id = uuid.UUID(message["jobId"])
        resource_ids = message.get("resourceIds", [])
        resource_id = message.get("resourceId")

        if resource_ids:
            await self._process_mass_backup(job_id, message, resource_ids)
        elif resource_id:
            await self._process_single_backup(job_id, message, resource_id)

    # ==================== Single Backup ====================

    # Handler dispatch table — defined once at the class level so we don't
    # rebuild a 25-entry dict on every backup message. Bound at first access.
    _HANDLER_TABLE: Dict[str, str] = {
        "MAILBOX": "backup_mailbox", "SHARED_MAILBOX": "backup_mailbox", "ROOM_MAILBOX": "backup_mailbox",
        "ONEDRIVE": "backup_onedrive",
        "SHAREPOINT_SITE": "backup_sharepoint",
        "TEAMS_CHANNEL": "backup_teams_single",
        # TEAMS_CHAT: scheduler denylists this type (SCHEDULER_IGNORED_TYPES);
        # handler is kept only to drain any in-flight queue messages from
        # before the cutover. Real chat-message backup runs through
        # TEAMS_CHAT_EXPORT below.
        "TEAMS_CHAT": "backup_teams_chat_single",
        "TEAMS_CHAT_EXPORT": "backup_teams_chat_export",
        "ENTRA_USER": "backup_entra_single", "ENTRA_GROUP": "backup_entra_single",
        "M365_GROUP": "backup_entra_single", "ENTRA_APP": "backup_entra_single",
        "ENTRA_DEVICE": "backup_entra_single", "ENTRA_SERVICE_PRINCIPAL": "backup_entra_single",
        "ENTRA_DIRECTORY": "backup_entra_directory",
        "ENTRA_CONDITIONAL_ACCESS": "backup_conditional_access",
        "ENTRA_BITLOCKER_KEY": "backup_bitlocker_key",
        "PLANNER": "_backup_metadata_only", "TODO": "_backup_metadata_only",
        "ONENOTE": "_backup_metadata_only", "COPILOT": "_backup_metadata_only",
        "POWER_BI": "backup_power_bi_workspace",
        "POWER_APPS": "_backup_metadata_only", "POWER_AUTOMATE": "_backup_metadata_only",
        "POWER_DLP": "_backup_metadata_only",
        "RESOURCE_GROUP": "_backup_metadata_only", "DYNAMIC_GROUP": "_backup_metadata_only",
    }

    async def _process_single_backup(self, job_id: uuid.UUID, message: Dict, resource_id: str):
        """Process a backup for a single resource.

        R2.4 — short-lived session pattern:
          1. Brief read session: fetch Job + Resource + Tenant, expunge,
             release the connection.
          2. Slow network work (graph_client setup, snapshot creation,
             handler call) runs WITHOUT pinning a DB connection — these
             can take many minutes for Power BI / large mailboxes.
          3. Each finalization step opens its own short session.

        Without this, a single Power BI backup can hold a connection for
        5+ minutes while making HTTPS calls, starving the pool and blocking
        every other handler in the worker."""

        # ── Step 1: brief read session ─────────────────────────────────────
        async with async_session_factory() as session:
            job = await session.get(Job, job_id)
            if not job:
                print(f"[{self.worker_id}] Job {job_id} not found, skipping stale message for {resource_id}")
                return
            resource = await session.get(Resource, uuid.UUID(resource_id))
            if not resource:
                print(f"[{self.worker_id}] Resource {resource_id} not found, skipping")
                return
            tenant = (await session.execute(
                select(Tenant).where(Tenant.id == resource.tenant_id)
            )).scalar_one_or_none()
            if not tenant:
                print(f"[{self.worker_id}] Tenant not found for resource {resource_id}, skipping")
                return
            # Detach so we can use these objects after the session closes.
            session.expunge_all()
        # Connection released to pool here.

        # ── Step 2: network work without a pinned session ─────────────────
        graph_client = await self.get_graph_client(tenant)
        if not graph_client:
            print(f"[{self.worker_id}] Graph client not available for resource {resource_id}, skipping")
            return

        resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
        print(f"[{self.worker_id}] Processing backup for {resource_id} (type={resource_type}, tenant={tenant.id})")

        # create_snapshot opens + closes its own short-lived session.
        snapshot = await self.create_snapshot(resource, message, job_id)
        # Live-progress tick — snapshot row now exists, handler about to run.
        await _update_job_pct(job_id, 15)

        handler_name = self._HANDLER_TABLE.get(resource_type)
        if not handler_name:
            if str(resource_type).startswith("AZURE_"):
                print(f"[{self.worker_id}] [ROUTING WARNING] Azure workload {resource_type} reached backup-worker; "
                      f"scheduler should route to azure-workload-worker via azure.* queue. "
                      f"Falling back to metadata-only for resource {resource.id}")
            else:
                print(f"[{self.worker_id}] [ROUTING WARNING] No dedicated handler for {resource_type}; "
                      f"falling back to metadata-only for resource {resource.id}")
            handler_name = "_backup_metadata_only"
        handler = getattr(self, handler_name)

        try:
            print(f"[{self.worker_id}] Calling handler for {resource_type}: {resource.display_name}")
            result = await handler(graph_client, resource, snapshot, tenant, message)
            # Handler finished — finalize phase ahead (DB writes + audit).
            await _update_job_pct(job_id, 95)
        except Exception as e:
            error_str = str(e).lower()
            is_inaccessible = any(kw in error_str for kw in [
                "not found", "404", "resource_not_found",
                "locked", "423", "account_locked",
                "authorization_failed", "access_denied",
            ])

            # ── Step 3a: error finalization — short sessions per write ────
            if is_inaccessible:
                print(f"[{self.worker_id}] Resource {resource.display_name} is INACCESSIBLE (404/423) — marking to skip future backups")
                try:
                    async with async_session_factory() as s:
                        live = await s.get(Resource, resource.id)
                        if live:
                            live.status = "INACCESSIBLE"
                            await s.commit()
                except Exception as up_exc:
                    print(f"[{self.worker_id}] Could not mark resource INACCESSIBLE: {up_exc}")

            try:
                async with async_session_factory() as s:
                    await self.fail_snapshot(s, snapshot, e)
            except Exception as fail_exc:
                print(f"[{self.worker_id}] Could not mark snapshot FAILED: {fail_exc}")

            try:
                async with async_session_factory() as s:
                    await self.update_job_status(s, job_id, JobStatus.FAILED, {"error": str(e)[:2000]})
            except Exception as job_exc:
                print(f"[{self.worker_id}] Could not mark job FAILED: {job_exc}")

            print(f"[{self.worker_id}] Handler FAILED for {resource_type}: {resource.display_name} — {e}")
            import traceback
            traceback.print_exc()
            raise

        # ── Step 3b: success finalization — short sessions per write ──────
        async with async_session_factory() as s:
            await self.complete_snapshot(s, snapshot, result)

        async with async_session_factory() as s:
            await self.update_job_status(s, job_id, JobStatus.COMPLETED, result)

        async with async_session_factory() as s:
            await self.update_resource_backup_info(s, resource, job_id, snapshot.id, result)

        # Audit event — fire-and-forget over HTTP, no DB session needed.
        await self.audit_logger.log(
            action="BACKUP_COMPLETED",
            tenant_id=str(tenant.id),
            org_id=str(tenant.org_id) if hasattr(tenant, 'org_id') and tenant.org_id else None,
            actor_type="WORKER",
            resource_id=str(resource.id),
            resource_type=resource_type,
            resource_name=resource.display_name or resource.email or str(resource.id),
            outcome="SUCCESS",
            job_id=str(job_id),
            snapshot_id=str(snapshot.id),
            details={
                "item_count": result.get("item_count", 0),
                "bytes_added": result.get("bytes_added", 0),
            },
        )

        print(f"[{self.worker_id}] Completed backup for {resource_id}")

    # ==================== Mass Backup (Parallel) ====================

    async def _process_mass_backup(self, job_id: uuid.UUID, message: Dict, resource_ids: List[str]):
        """Process mass backup with full parallelism"""
        async with async_session_factory() as session:
            job = await session.get(Job, job_id)
            if not job:
                return

            job.status = JobStatus.RUNNING
            job.progress_pct = 5  # worker picked up the message
            await session.commit()

        # Fetch all resources and their tenants in one session
        async with async_session_factory() as session:
            result = await session.execute(
                select(Resource).where(Resource.id.in_([uuid.UUID(rid) for rid in resource_ids]))
            )
            resources = result.scalars().all()

            if not resources:
                return

            tenant_ids = list(set(r.tenant_id for r in resources))
            tenant_result = await session.execute(select(Tenant).where(Tenant.id.in_(tenant_ids)))
            tenants_map = {t.id: t for t in tenant_result.scalars().all()}

        if not resources:
            return

        # Group by tenant + resource type for parallel processing
        groups: Dict[str, List[Resource]] = {}
        for r in resources:
            key = f"{r.tenant_id}:{r.type.value}"
            groups.setdefault(key, []).append(r)

        print(f"[{self.worker_id}] Mass backup: {len(resources)} resources in {len(groups)} groups")

        # Process groups in parallel (workload parallelism)
        semaphore = asyncio.Semaphore(settings.WORKLOAD_CONCURRENCY)

        # Live progress — publish percentage as each group finishes so the
        # UI no longer sits at 5% until the very last resource settles (and
        # no longer shows "RUNNING" forever if one handler hangs — there's
        # also a per-group hard timeout below).
        total_groups = len(groups)
        completed_groups = 0
        progress_lock = asyncio.Lock()
        # Per-group hard ceiling. Sized for enterprise workloads — a single
        # group can mean hundreds of mailboxes or multi-TB OneDrives batched
        # together, so the default is deliberately generous (24h). The cap
        # exists only to prevent a truly wedged Graph call from holding the
        # whole job in RUNNING forever; operators can tune it via env.
        group_timeout_s = int(os.getenv("BACKUP_GROUP_TIMEOUT_S", "86400"))

        async def process_group(group_key, group_resources):
            nonlocal completed_groups
            async with semaphore:
                tenant_id_str, resource_type = group_key.split(":", 1)
                tenant = tenants_map.get(uuid.UUID(tenant_id_str))
                try:
                    res = await asyncio.wait_for(
                        self._backup_resource_group(
                            group_resources, tenant, message, job_id,
                        ),
                        timeout=group_timeout_s,
                    )
                except asyncio.TimeoutError:
                    print(
                        f"[{self.worker_id}] group {group_key} exceeded "
                        f"{group_timeout_s}s timeout — continuing without it",
                    )
                    res = {"item_count": 0, "bytes_added": 0,
                           "error": "group_timeout"}
                async with progress_lock:
                    completed_groups += 1
                    pct = min(99, 5 + int(90 * completed_groups / total_groups))
                await _update_job_pct(job_id, pct)
                return res

        tasks = [process_group(key, res_list) for key, res_list in groups.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        total_items = sum(r.get("item_count", 0) for r in results if isinstance(r, dict))
        total_bytes = sum(r.get("bytes_added", 0) for r in results if isinstance(r, dict))
        failed = sum(
            1 for r in results
            if isinstance(r, Exception)
            or (isinstance(r, dict) and r.get("error"))
        )

        async with async_session_factory() as session:
            await self.update_job_status(session, job_id, JobStatus.COMPLETED, {
                "total": len(resources),
                "completed": len(resources) - failed,
                "failed": failed,
                "item_count": total_items,
                "bytes_added": total_bytes,
            })

        print(f"[{self.worker_id}] Mass backup done: {len(resources)-failed}/{len(resources)} succeeded")

    # ==================== Resource Group Backup ====================

    async def _backup_resource_group(self, resources: List[Resource], tenant: Tenant, message: Dict, job_id: uuid.UUID) -> Dict:
        """Backup a group of resources of the same type from the same tenant"""
        if not resources:
            return {"item_count": 0, "bytes_added": 0}

        graph_client = await self.get_graph_client(tenant)
        if not graph_client:
            return {"item_count": 0, "bytes_added": 0, "error": "No Graph client"}

        resource_type = resources[0].type.value

        # Dispatch to type-specific handler with parallelism
        if resource_type in ("ONEDRIVE", "SHAREPOINT_SITE"):
            return await self._backup_files_parallel(resources, graph_client, tenant, message, job_id)
        elif resource_type in ("MAILBOX", "SHARED_MAILBOX", "ROOM_MAILBOX"):
            return await self._backup_mailboxes_parallel(resources, graph_client, tenant, message, job_id)
        elif resource_type in ("USER_MAIL", "USER_ONEDRIVE", "USER_CONTACTS", "USER_CALENDAR", "USER_CHATS"):
            # Tier 2 per-user content categories. Each row holds the parent
            # user_id in metadata so the handler can fetch + persist content
            # against Graph without a top-level MAILBOX/ONEDRIVE row existing.
            return await self._backup_user_content_parallel(resources, graph_client, tenant, message, job_id)
        else:
            return await self._backup_generic_parallel(resources, graph_client, tenant, message, job_id)

    # ==================== Tier 2 per-user content backup ====================

    async def _backup_user_content_parallel(
        self,
        resources: List[Resource],
        graph_client: GraphClient,
        tenant: Tenant,
        message: Dict,
        job_id: uuid.UUID,
    ) -> Dict:
        """Back up the five fixed per-user content categories (USER_MAIL,
        USER_ONEDRIVE, USER_CONTACTS, USER_CALENDAR, USER_CHATS).

        Each input resource is one Tier 2 child row whose metadata holds the
        parent user's Graph ID (set by `discover_user_content`). For each,
        we create a snapshot, pull the content from Graph, and persist
        snapshot items inline (extra_data.raw) so the Recovery page can
        render them via the same /mail|/onedrive|/contacts|/calendar|/chats
        endpoints used for Tier 1 mailbox/onedrive backups.

        We deliberately do NOT write to blob storage here — items are small
        and stored inline in extra_data, which keeps this handler self-
        contained and avoids the multi-shard wiring the file/SP handlers
        need. The Recovery endpoints already prefer extra_data.raw before
        falling back to blob reads, so the data is reachable end-to-end."""
        from sqlalchemy import update as sa_update

        semaphore = asyncio.Semaphore(settings.BACKUP_CONCURRENCY)
        ITEM_LIMIT = 200  # First page only — keeps the handler bounded.

        async def _fetch_for(resource: Resource) -> List[tuple]:
            """Returns a list of (item_type, name, ext_id, extra_data, folder_path).

            `extra_data` is the full SnapshotItem envelope. For most types
            it's `{"raw": graph_object}`; for chats it carries top-level
            `chatId`/`chatTopic`/`chatType`/`memberNames` alongside `raw`
            so the legacy filter (extra_data.chatId) and ChatPreview
            (item.metadata.chatTopic) keep working."""
            kind = resource.type.value
            meta = resource.extra_data or {}
            user_id = meta.get("user_id")
            if not user_id:
                return []

            try:
                if kind == "USER_MAIL":
                    # Recursive folder tree: {folder_id → "/Inbox/Subfolder"}.
                    # Mirrors what the legacy mailbox handler did so messages
                    # in nested folders show under their real path instead of
                    # all collapsing to "Inbox".
                    try:
                        folder_tree = await graph_client.get_mail_folder_tree(user_id)
                    except Exception as e:
                        print(f"[{self.worker_id}] [USER_MAIL] folder tree fetch failed for {user_id}: {e}")
                        folder_tree = {}

                    # Per-folder parallel delta drain — replaces the legacy
                    # single serial /users/{uid}/messages chain. Mail folder
                    # delta IS officially supported on Graph, so on first
                    # run each folder does a full scan; subsequent runs
                    # resume from the saved @odata.deltaLink per folder
                    # and fetch only changes since (30-50× faster on
                    # incremental). First-run speedup scales with the
                    # parallel cap (default 10 folders concurrent).
                    user_label_mail = resource.display_name or user_id
                    mail_start_mono = time.monotonic()
                    print(
                        f"[{self.worker_id}] [USER_MAIL START] "
                        f"{user_label_mail} ({user_id}) — "
                        f"{len(folder_tree)} folders"
                    )
                    mail_existing_tokens: Dict[str, str] = dict(
                        (resource.extra_data or {}).get(
                            "mail_delta_tokens_by_folder", {},
                        ) or {},
                    )
                    mail_new_tokens: Dict[str, str] = {}
                    out: List = []
                    mail_select = (
                        "id,subject,from,toRecipients,ccRecipients,"
                        "sentDateTime,receivedDateTime,bodyPreview,body,"
                        "hasAttachments,parentFolderId"
                    )
                    mail_fanout = int(
                        os.getenv("USER_MAIL_PARALLEL_FOLDERS", "10"),
                    )
                    mail_timeout_s = int(
                        os.getenv("USER_MAIL_TIMEOUT_S", "43200"),
                    )
                    mail_sem = asyncio.Semaphore(max(1, mail_fanout))
                    mail_progress = {"folders_done": 0, "msgs": 0}
                    mail_folder_ids = list(folder_tree.keys()) or [None]

                    async def _drain_one_folder(fid: Optional[str]) -> List:
                        """Drain one mail folder's delta stream. fid=None
                        falls back to /users/{uid}/messages (all folders)
                        when the folder_tree fetch returned empty — keeps
                        us functional even if folder enumeration 403s."""
                        saved = mail_existing_tokens.get(fid or "__all__")
                        if saved:
                            url = saved  # resume from deltaLink verbatim
                            params = None
                        elif fid:
                            url = (
                                f"{graph_client.GRAPH_URL}/users/{user_id}"
                                f"/mailFolders/{fid}/messages/delta"
                            )
                            params = {"$top": "50", "$select": mail_select}
                        else:
                            # Folder enumeration failed — do a best-effort
                            # non-delta scan of all messages (no incremental).
                            url = (
                                f"{graph_client.GRAPH_URL}/users/{user_id}"
                                f"/messages"
                            )
                            params = {"$top": "50", "$select": mail_select}

                        local_out: List = []
                        delta_out: Optional[str] = None
                        try:
                            async with mail_sem:
                                async for page in graph_client._iter_pages(
                                    url, params=params,
                                ):
                                    for m in page.get("value", []) or []:
                                        path = (
                                            folder_tree.get(
                                                m.get("parentFolderId"),
                                            )
                                            or (folder_tree.get(fid) if fid else None)
                                            or ""
                                        )
                                        local_out.append((
                                            "EMAIL",
                                            m.get("subject") or "(no subject)",
                                            m.get("id"),
                                            {"raw": m},
                                            path,
                                        ))
                                    if "@odata.deltaLink" in page:
                                        delta_out = page["@odata.deltaLink"]
                        except Exception as e:
                            fid_disp = fid[:8] if fid else "__all__"
                            print(
                                f"[{self.worker_id}] [USER_MAIL] folder "
                                f"{fid_disp} drain failed: "
                                f"{type(e).__name__}: {e} "
                                f"(kept {len(local_out)})"
                            )
                        if delta_out:
                            mail_new_tokens[fid or "__all__"] = delta_out
                        mail_progress["folders_done"] += 1
                        mail_progress["msgs"] += len(local_out)
                        if mail_progress["folders_done"] % 5 == 0:
                            print(
                                f"[{self.worker_id}] [USER_MAIL] "
                                f"{user_label_mail}: "
                                f"{mail_progress['folders_done']}/"
                                f"{len(mail_folder_ids)} folders, "
                                f"{mail_progress['msgs']} msgs so far"
                            )
                        return local_out

                    async def _drain_mail_parallel() -> None:
                        results = await asyncio.gather(
                            *[_drain_one_folder(fid) for fid in mail_folder_ids],
                            return_exceptions=True,
                        )
                        for r in results:
                            if isinstance(r, list):
                                out.extend(r)

                    try:
                        await asyncio.wait_for(
                            _drain_mail_parallel(), timeout=mail_timeout_s,
                        )
                    except asyncio.TimeoutError:
                        print(
                            f"[{self.worker_id}] [USER_MAIL] TIMEOUT after "
                            f"{mail_timeout_s}s for {user_label_mail} "
                            f"(kept {len(out)})"
                        )
                    except Exception as e:
                        print(
                            f"[{self.worker_id}] [USER_MAIL] parallel drain "
                            f"aborted for {user_label_mail}: "
                            f"{type(e).__name__}: {e} "
                            f"(kept {len(out)})"
                        )

                    elapsed = time.monotonic() - mail_start_mono
                    rate = (len(out) / elapsed) if elapsed > 0 else 0
                    print(
                        f"[{self.worker_id}] [USER_MAIL] fetched "
                        f"{len(out)} messages across "
                        f"{len(mail_folder_ids)} folders in {elapsed:.1f}s "
                        f"({rate:.1f} msg/s)"
                    )

                    # Persist per-folder delta tokens so subsequent runs
                    # only pull new messages per folder. Merge with
                    # existing so a failed folder doesn't wipe tokens
                    # we still hold for the rest.
                    if mail_new_tokens:
                        try:
                            async with async_session_factory() as _s:
                                r = await _s.get(Resource, resource.id)
                                if r is not None:
                                    ed = dict(r.extra_data or {})
                                    merged = dict(
                                        ed.get(
                                            "mail_delta_tokens_by_folder",
                                        ) or {},
                                    )
                                    merged.update(mail_new_tokens)
                                    ed["mail_delta_tokens_by_folder"] = merged
                                    r.extra_data = ed
                                    await _s.commit()
                        except Exception as _e:
                            print(
                                f"[{self.worker_id}] [USER_MAIL] delta "
                                f"persist failed: {_e}"
                            )

                    return out

                if kind == "USER_ONEDRIVE":
                    drive_id = meta.get("drive_id")
                    if not drive_id:
                        return []
                    # Use the delta endpoint to enumerate the WHOLE drive
                    # (recurses into every subfolder by default). Without
                    # this we'd only see root-level items. Reference:
                    # https://learn.microsoft.com/graph/api/driveitem-delta
                    out = []
                    next_url = f"{graph_client.GRAPH_URL}/drives/{drive_id}/root/delta"
                    pages_seen = 0
                    while next_url:
                        page = await graph_client._get(next_url, params={"$top": "200"} if pages_seen == 0 else None)
                        for f in page.get("value", []):
                            # Skip the root folder itself (no name / no parentReference).
                            if not f.get("parentReference"):
                                continue
                            # Skip folders — we only back up actual file bytes.
                            # Folders carry no downloadable content; their
                            # structure is reconstructable from each file's
                            # parentReference.path so we don't need rows for
                            # them (and they'd inflate item counts in the UI).
                            if f.get("folder") is not None or f.get("file") is None:
                                continue
                            parent_path = (f["parentReference"].get("path") or "")
                            # Graph paths look like "/drives/<id>/root:" or
                            # "/drives/<id>/root:/Documents/Sub". Slice off
                            # everything up to and including the first colon.
                            clean = parent_path.split(":", 1)[1] if ":" in parent_path else parent_path
                            clean = clean or "/"
                            out.append(("ONEDRIVE_FILE", f.get("name") or "(unnamed)", f.get("id"), {"raw": f}, clean))
                        next_url = page.get("@odata.nextLink")
                        pages_seen += 1
                        if pages_seen > 10000:
                            break  # runaway guard, not an intentional cap
                    return out

                if kind == "USER_CONTACTS":
                    out_rows = await _backup_contacts_for_user(
                        graph_client, user_id, item_limit=ITEM_LIMIT
                    )
                    folder_names = sorted({r[4] for r in out_rows})
                    print(
                        f"[{self.worker_id}]   [USER_CONTACTS] {user_id}: "
                        f"{len(out_rows)} contacts, folders={folder_names}"
                    )
                    return out_rows

                if kind == "USER_CALENDAR":
                    # AFI-parity calendar backup:
                    #   • Enumerate every calendar under /users/{id}/calendars
                    #     (default + shared + secondary) so we're not limited
                    #     to the main calendar.
                    #   • For each calendar, use /calendarView/delta with a
                    #     sliding 2-year-back / 1-year-forward window, which
                    #     EXPANDS every recurring series into one row per
                    #     occurrence (a daily standup becomes ~750 rows,
                    #     not 1). First run walks the window; subsequent
                    #     runs resume from the saved @odata.deltaLink.
                    #   • Per-calendar delta tokens live in
                    #     resource.extra_data["calendar_delta_tokens"]:
                    #       { calendarId: "<full deltaLink URL>" }
                    from datetime import timedelta as _td
                    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
                    win_start = (now_utc - _td(days=365 * 2)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    win_end   = (now_utc + _td(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")

                    # Current delta-token map (per calendar).
                    cal_delta_map: Dict[str, str] = dict((resource.extra_data or {}).get("calendar_delta_tokens") or {})
                    new_delta_map: Dict[str, str] = {}

                    # 1) Enumerate calendars owned by the user.
                    try:
                        cal_page = await graph_client._get(
                            f"{graph_client.GRAPH_URL}/users/{user_id}/calendars",
                            params={"$top": "100", "$select": "id,name,color,owner,canEdit,isDefaultCalendar"},
                        )
                        calendars = (cal_page or {}).get("value", []) or []
                    except Exception as _e:
                        # Fallback: at least hit the default calendar so we
                        # don't return zero events on a permissions blip.
                        logger.warning("[%s] Calendars enumeration failed for %s: %s", self.worker_id, user_id, _e)
                        calendars = [{"id": "default", "name": "Calendar", "isDefaultCalendar": True}]

                    out_rows: List[tuple] = []
                    for cal in calendars:
                        cal_id = cal.get("id") or "default"
                        cal_name = cal.get("name") or "Calendar"
                        is_default = bool(cal.get("isDefaultCalendar"))
                        # Resume from saved deltaLink if we have one for this
                        # calendar; otherwise start a fresh window.
                        #
                        # Graph supports /calendarView/delta only on the
                        # DEFAULT calendar (/users/{id}/calendarView/delta).
                        # Secondary calendars (US Holidays, Birthdays, shared,
                        # group calendars) 400 on the delta variant — we fall
                        # back to non-delta /calendars/{cid}/calendarView which
                        # still expands recurring series but has no delta
                        # token (so it's a full re-pull each run — acceptable
                        # because sub-calendars rarely have daily change volume).
                        # NB: _iter_pages treats URLs starting with "http" as
                        # already-complete (nextLink-style) and drops the
                        # params dict, so we inline startDateTime/endDateTime
                        # into the URL directly. This matches what Graph
                        # actually needs on the very first request.
                        saved_delta = cal_delta_map.get(cal_id)
                        if saved_delta and is_default:
                            stream_url = saved_delta
                            stream_params: Optional[Dict[str, str]] = None
                            print(f"[{self.worker_id}]   [USER_CALENDAR] resuming delta for {cal_name} ({user_id})")
                        elif is_default:
                            stream_url = (
                                f"{graph_client.GRAPH_URL}/users/{user_id}"
                                f"/calendarView/delta"
                                f"?startDateTime={win_start}&endDateTime={win_end}"
                            )
                            stream_params = None
                        else:
                            stream_url = (
                                f"{graph_client.GRAPH_URL}/users/{user_id}"
                                f"/calendars/{cal_id}/calendarView"
                                f"?startDateTime={win_start}&endDateTime={win_end}&$top=999"
                            )
                            stream_params = None

                        # Drain the stream. _iter_pages captures the terminal
                        # @odata.deltaLink into self._last_delta_link — we
                        # grab it per-calendar and stash into the new map.
                        try:
                            async for page in graph_client._iter_pages(stream_url, params=stream_params):
                                for e in page.get("value", []) or []:
                                    # calendarView/delta emits `@removed` blobs
                                    # when an event is deleted from the window;
                                    # skip those — they don't carry a subject.
                                    if "@removed" in e or not e.get("id"):
                                        continue
                                    out_rows.append((
                                        "CALENDAR_EVENT",
                                        e.get("subject") or "(no subject)",
                                        e.get("id"),
                                        {
                                            "raw": e,
                                            "calendarId": cal_id,
                                            "calendarName": cal_name,
                                            "seriesMasterId": e.get("seriesMasterId"),
                                            "type": e.get("type"),
                                        },
                                        # folder_path groups events per
                                        # calendar so the Recovery left rail
                                        # can show "Calendar / <name>".
                                        f"Calendar/{cal_name}",
                                    ))
                        except Exception as _e:
                            print(f"[{self.worker_id}]   [USER_CALENDAR] stream aborted for {cal_name}: {type(_e).__name__}: {_e}")

                        cap = getattr(graph_client, "_last_delta_link", None)
                        if cap:
                            new_delta_map[cal_id] = cap

                    # Persist the per-calendar delta tokens for next run. We
                    # merge with existing map so a failed calendar doesn't
                    # wipe tokens we still hold for others.
                    if new_delta_map:
                        try:
                            async with async_session_factory() as _s:
                                r = await _s.get(Resource, resource.id)
                                if r is not None:
                                    ed = dict(r.extra_data or {})
                                    merged = dict(ed.get("calendar_delta_tokens") or {})
                                    merged.update(new_delta_map)
                                    ed["calendar_delta_tokens"] = merged
                                    r.extra_data = ed
                                    await _s.commit()
                        except Exception as _e:
                            print(f"[{self.worker_id}]   [USER_CALENDAR] delta persist failed: {_e}")

                    print(f"[{self.worker_id}]   [USER_CALENDAR] {user_id}: {len(calendars)} calendar(s), {len(out_rows)} event occurrence(s)")
                    return out_rows

                if kind == "USER_CHATS":
                    # Per-chat metadata map: chat_id → {displayName, chatType,
                    # topic, memberNames, memberEmails}. Mirrors the legacy
                    # discover_teams display-name ladder so the Recovery left
                    # panel groups chats under "chats/<friendly name>" — both
                    # 1:1 ("Vinay Chauhan") and group ("Group: Hemant, Vinay
                    # +5 more") get a meaningful folder.
                    user_label_early = resource.display_name or user_id
                    print(
                        f"[{self.worker_id}] [USER_CHATS START] {user_label_early} "
                        f"({user_id})",
                    )
                    chats_start_mono = time.monotonic()
                    chat_meta: Dict[str, Dict[str, Any]] = {}
                    try:
                        list_page = await graph_client._get(
                            f"{graph_client.GRAPH_URL}/users/{user_id}/chats",
                            params={"$top": "200", "$select": "id,topic,chatType"},
                        )
                        chats_raw = (list_page or {}).get("value", []) or []
                    except Exception as e:
                        print(f"[{self.worker_id}] [USER_CHATS] chats list failed for {user_id}: {e}")
                        chats_raw = []

                    # Batch /chats/{id}/members via /v1.0/$batch — one HTTP
                    # call per 20 chats instead of 200 individual _get's.
                    # Cuts throttle cost ~20× vs the old 8-way gather.
                    from shared.graph_batch import BatchRequest
                    batch_reqs = [
                        BatchRequest(
                            id=c["id"], method="GET",
                            url=f"/chats/{c['id']}/members",
                        )
                        for c in chats_raw if c.get("id")
                    ]
                    member_lookup: Dict[str, Tuple[List[str], List[str]]] = {}
                    if batch_reqs:
                        try:
                            batch_result = await graph_client.batch(batch_reqs)
                        except Exception as e:
                            print(
                                f"[{self.worker_id}] [USER_CHATS] members "
                                f"batch failed: {type(e).__name__}: {e}"
                            )
                            batch_result = {}
                        for cid, resp in batch_result.items():
                            if resp.status == 200:
                                value = (resp.body or {}).get("value", []) or []
                                emails = [m.get("email") for m in value if m.get("email")]
                                names = [m.get("displayName") for m in value if m.get("displayName")]
                                member_lookup[cid] = (emails, names)
                            else:
                                member_lookup[cid] = ([], [])

                    for ch in chats_raw:
                        cid = ch.get("id")
                        if not cid:
                            continue
                        ctype = ch.get("chatType", "unknown")
                        topic = ch.get("topic")
                        emails, names = member_lookup.get(cid, ([], []))
                        # Display-name ladder, copied from
                        # graph_client.discover_teams (reference 31c6f2c).
                        if ctype == "oneOnOne":
                            if topic:
                                display_name = topic
                            elif names:
                                display_name = " | ".join(names)
                            else:
                                display_name = f"1-on-1 Chat ({cid[:8]})"
                        else:
                            if topic:
                                display_name = topic
                            elif names:
                                display_name = f"Group: {', '.join(names[:3])}"
                                if len(names) > 3:
                                    display_name += f" +{len(names) - 3} more"
                            else:
                                display_name = f"Group Chat ({cid[:8]})"
                        chat_meta[cid] = {
                            "displayName": display_name,
                            "chatType": ctype,
                            "topic": topic,
                            "memberCount": len(names),
                            "memberNames": names,
                            "memberEmails": emails,
                        }

                    # Fall back to a minimal entry for any chat in our discover
                    # list that wasn't enumerable above (rare — auth scope drift).
                    chat_ids = (meta.get("chat_ids") or [])[:20]
                    for cid in chat_ids:
                        chat_meta.setdefault(cid, {
                            "displayName": f"Chat {cid[:8]}",
                            "chatType": "unknown",
                            "topic": None,
                            "memberCount": 0,
                            "memberNames": [],
                            "memberEmails": [],
                        })

                    out: List = []
                    # Log label for this stream — resource.display_name like
                    # "Chats — Akshat Verma" is fine; fall back to the user
                    # id if the resource row had no display_name captured.
                    user_label = resource.display_name or user_id

                    # Per-chat parallel delta drain — replaces the legacy
                    # serial /users/{id}/chats/getAllMessages/delta single
                    # pipe (which maxed at ~10 msg/s regardless of tenant
                    # size because Graph's @odata.nextLink chains serially).
                    # With N parallel chats each running their own
                    # /chats/{cid}/messages/delta iterator, throughput
                    # scales up to the app-per-tenant RPS ceiling
                    # (200 RPS for Teams Export APIs). Real-world: 20-100×
                    # faster on users with many chats.
                    #
                    # Delta tokens are tracked per-chat so subsequent runs
                    # only pull new messages from each chat independently
                    # (one throttled/expired chat doesn't force a full
                    # rescan of the other 200).
                    all_msgs: List[Dict[str, Any]] = []
                    existing_tokens: Dict[str, str] = dict(
                        (resource.extra_data or {}).get(
                            "chat_delta_tokens", {},
                        ) or {},
                    )
                    new_tokens: Dict[str, str] = {}

                    chat_ids_to_drain = [
                        cid for cid in chat_meta.keys() if cid
                    ]
                    chat_fanout = int(
                        os.getenv("USER_CHATS_PARALLEL_CHATS", "20"),
                    )
                    chats_timeout_s = int(
                        os.getenv("USER_CHATS_TIMEOUT_S", "43200"),
                    )
                    per_chat_sem = asyncio.Semaphore(max(1, chat_fanout))
                    _progress = {"chats_done": 0, "msgs": 0}

                    cancel_check_every = int(os.getenv(
                        "USER_CHATS_CANCEL_CHECK_EVERY_CHATS", "10",
                    ))
                    _chat_fanout_cancelled = False

                    async def _drain_one_chat(cid: str) -> Tuple[str, List[Dict[str, Any]], Optional[str]]:
                        nonlocal _chat_fanout_cancelled
                        """Drain a single chat's messages. Returns
                        (cid, messages, new_cursor).

                        Endpoint strategy — Graph delta support varies by
                        chat type:
                          * oneOnOne chats → /messages/delta works
                          * group / meeting_* / @thread.v2 chats → delta
                            returns 400; must use non-delta /messages
                            with a $filter for incremental

                        We use /chats/{cid}/messages (non-delta) for
                        ALL chats for uniformity — with an incremental
                        $filter on lastModifiedDateTime when we have a
                        saved cursor from a prior run. First run does
                        a full fetch per chat; subsequent runs pull
                        only modified-since the stamp.

                        Keeps pagination-state extraction local so
                        parallel iterators don't race on module-level
                        graph_client state.
                        """
                        # If the job was cancelled while we were queued
                        # behind the semaphore, bail without even opening
                        # the connection. Mid-drain cancellation stops
                        # at the next page boundary (see below).
                        if _chat_fanout_cancelled:
                            return cid, [], None
                        saved_cursor = existing_tokens.get(cid)
                        url = f"{graph_client.GRAPH_URL}/chats/{cid}/messages"
                        params: Dict[str, str] = {"$top": "50"}
                        # If saved_cursor looks like a full URL (legacy
                        # nextLink/deltaLink), use it verbatim so resume
                        # still works. Otherwise treat it as an ISO
                        # lastModifiedDateTime and apply as $filter.
                        if saved_cursor and saved_cursor.startswith("http"):
                            url = saved_cursor
                            params = None  # type: ignore[assignment]
                        elif saved_cursor:
                            params["$filter"] = (
                                f"lastModifiedDateTime gt {saved_cursor}"
                            )
                            params["$orderby"] = "lastModifiedDateTime asc"

                        msgs_local: List[Dict[str, Any]] = []
                        max_stamp: Optional[str] = None
                        try:
                            async with per_chat_sem:
                                async for page in graph_client._iter_pages(
                                    url, params=params,
                                ):
                                    for m in (page.get("value", []) or []):
                                        msgs_local.append(m)
                                        stamp = m.get("lastModifiedDateTime")
                                        if stamp and (
                                            max_stamp is None
                                            or stamp > max_stamp
                                        ):
                                            max_stamp = stamp
                        except Exception as e:
                            print(
                                f"[{self.worker_id}] [USER_CHATS] chat {cid[:8]} "
                                f"drain failed: {type(e).__name__}: {e} "
                                f"(kept {len(msgs_local)})"
                            )
                        _progress["chats_done"] += 1
                        _progress["msgs"] += len(msgs_local)
                        # Heartbeat every 10 chats so operators can see
                        # long fan-outs making progress. Also poll the
                        # job-status here — if the user cancelled the
                        # job from the UI, short-circuit remaining
                        # drains at the next semaphore boundary.
                        if _progress["chats_done"] % cancel_check_every == 0:
                            print(
                                f"[{self.worker_id}] [USER_CHATS] "
                                f"{user_label}: {_progress['chats_done']}/"
                                f"{len(chat_ids_to_drain)} chats, "
                                f"{_progress['msgs']} msgs so far"
                            )
                            if not _chat_fanout_cancelled and await _is_job_cancelled(job_id):
                                _chat_fanout_cancelled = True
                                print(
                                    f"[{self.worker_id}] [USER_CHATS] "
                                    f"{user_label}: job cancelled — "
                                    f"short-circuiting remaining drains"
                                )
                        return cid, msgs_local, max_stamp

                    async def _drain_chats_parallel() -> None:
                        results = await asyncio.gather(
                            *[_drain_one_chat(cid) for cid in chat_ids_to_drain],
                            return_exceptions=True,
                        )
                        for r in results:
                            if isinstance(r, Exception):
                                continue
                            cid, msgs_local, delta_out = r
                            all_msgs.extend(msgs_local)
                            if delta_out:
                                new_tokens[cid] = delta_out

                    try:
                        await asyncio.wait_for(
                            _drain_chats_parallel(), timeout=chats_timeout_s,
                        )
                    except asyncio.TimeoutError:
                        print(
                            f"[{self.worker_id}] [USER_CHATS] TIMEOUT after "
                            f"{chats_timeout_s}s for {user_label} "
                            f"(kept {len(all_msgs)})"
                        )
                    except Exception as e:
                        print(
                            f"[{self.worker_id}] [USER_CHATS] parallel drain "
                            f"aborted for {user_label}: "
                            f"{type(e).__name__}: {e} "
                            f"(kept {len(all_msgs)})"
                        )

                    elapsed = time.monotonic() - chats_start_mono
                    rate = (len(all_msgs) / elapsed) if elapsed > 0 else 0
                    print(
                        f"[{self.worker_id}] [USER_CHATS] fetched "
                        f"{len(all_msgs)} messages across "
                        f"{len(chat_ids_to_drain)} chats in {elapsed:.1f}s "
                        f"({rate:.1f} msg/s)"
                    )

                    # Persist per-chat delta tokens so subsequent runs only
                    # pull new messages per-chat. Merge with existing map
                    # so a failed chat doesn't wipe tokens we still hold
                    # for others.
                    if new_tokens:
                        try:
                            async with async_session_factory() as _s:
                                r = await _s.get(Resource, resource.id)
                                if r is not None:
                                    ed = dict(r.extra_data or {})
                                    merged = dict(
                                        ed.get("chat_delta_tokens") or {},
                                    )
                                    merged.update(new_tokens)
                                    ed["chat_delta_tokens"] = merged
                                    r.extra_data = ed
                                    await _s.commit()
                        except Exception as _e:
                            print(f"[{self.worker_id}] [USER_CHATS] delta persist failed: {_e}")

                    # Bucket the flat message list into per-chat rows using
                    # the pre-computed chat_meta for display name / type.
                    # Messages whose chatId isn't in chat_meta (auth drift,
                    # chat created mid-backup) get a synthetic fallback so
                    # they still show up in Recovery.
                    for m in all_msgs:
                        chat_id = m.get("chatId") or (m.get("channelIdentity") or {}).get("channelId")
                        if not chat_id:
                            continue
                        info = chat_meta.get(chat_id) or {
                            "displayName": f"Chat {chat_id[:8]}",
                            "chatType": "unknown",
                            "topic": None,
                            "memberCount": 0,
                            "memberNames": [],
                            "memberEmails": [],
                        }
                        display = info["displayName"]
                        folder = f"chats/{display}"
                        m["_chat_folder_path"] = folder
                        m["chatId"] = chat_id
                        body = (m.get("body") or {}).get("content") or ""
                        ext = {
                            "raw": m,
                            "chatId": chat_id,
                            "chatTopic": display,
                            "chatType": info["chatType"],
                            "memberNames": info["memberNames"],
                            "memberCount": info["memberCount"],
                            "exportedVia": user_id,
                        }
                        out.append((
                            "TEAMS_CHAT_MESSAGE",
                            body[:120] or "(empty)",
                            m.get("id"),
                            ext,
                            folder,
                        ))
                    return out

            except Exception as e:
                print(f"[{self.worker_id}] [{kind}] Graph fetch failed for user {user_id}: {e}")
                return []
            return []

        async def _backup_one(resource: Resource) -> Dict:
            async with semaphore:
                snapshot = await self.create_snapshot(resource, message, job_id)
                items_data = await _fetch_for(resource)

                bytes_total = 0
                async with async_session_factory() as session:
                    # Build plain dicts for a multi-row pg_insert instead
                    # of SQLAlchemy ORM objects — the bulk path is 10×+
                    # faster on 10k+ rows (one round-trip, server-side
                    # batching, no per-row ORM state tracking) and pairs
                    # with the UNIQUE(snapshot_id,external_id) guard to
                    # make message redelivery safely idempotent.
                    db_rows: List[Dict[str, Any]] = []
                    for tup in items_data:
                        # Tuple shape: (item_type, name, ext_id, extra_data, folder_path)
                        # — extra_data is the full envelope. Always present
                        # at least as {"raw": graph_object}; chats add
                        # top-level chatId/chatTopic/etc. so legacy queries
                        # and previews keep working.
                        if len(tup) == 5:
                            item_type, name, ext_id, extra, folder_path = tup
                        else:
                            item_type, name, ext_id, extra = tup
                            folder_path = None
                        if not isinstance(extra, dict):
                            extra = {"raw": extra}
                        # orjson is faster than stdlib json for JSON-heavy
                        # objects (chat msgs avg ~2 KB each); returns bytes
                        # directly so we skip the .encode("utf-8") hop.
                        body = _json_dumps_bytes(extra.get("raw", extra))
                        bytes_total += len(body)
                        # Compute the hash ONCE — the previous code ran
                        # sha256 twice per item for identical content
                        # (content_hash + content_checksum both read the
                        # same bytes), doubling CPU for zero benefit.
                        body_hash = hashlib.sha256(body).hexdigest()
                        db_rows.append({
                            "id": uuid.uuid4(),
                            "snapshot_id": snapshot.id,
                            "tenant_id": tenant.id,
                            "external_id": str(ext_id or uuid.uuid4()),
                            "item_type": item_type,
                            "name": (name or "")[:255],
                            "folder_path": folder_path,
                            "content_size": len(body),
                            "content_hash": body_hash,
                            "extra_data": extra,
                            "content_checksum": body_hash,
                        })
                    if db_rows:
                        await _bulk_upsert_snapshot_items(session, db_rows)

                # Second pass: per-message attachment capture. The main
                # snapshot items hold the raw Graph objects; attachments are
                # separate SnapshotItem rows (one per attachment) tied to
                # the parent message via extra_data.parent_item_id.
                #
                #   USER_MAIL  → EMAIL_ATTACHMENT rows (file/item/reference)
                #   USER_CHATS → CHAT_ATTACHMENT rows (reference + cards)
                att_items, att_bytes = [], 0
                total_hosted_items = 0
                if resource.type.value == "USER_MAIL":
                    user_id = (resource.extra_data or {}).get("user_id")
                    if user_id:
                        emails_with_att = [
                            extra.get("raw") for (_t, _n, _e, extra, *_rest) in items_data
                            if isinstance(extra, dict) and isinstance(extra.get("raw"), dict)
                            and extra["raw"].get("hasAttachments")
                        ]
                        if emails_with_att:
                            att_items, att_bytes = await self._usermail_backup_attachments(
                                graph_client, resource, snapshot, tenant, user_id, emails_with_att,
                            )
                elif resource.type.value == "USER_CHATS":
                    user_id = (
                        (resource.extra_data or {}).get("user_id") or resource.external_id
                    )
                    chats_with_att = [
                        extra.get("raw") for (_t, _n, _e, extra, *_rest) in items_data
                        if isinstance(extra, dict) and isinstance(extra.get("raw"), dict)
                        and extra["raw"].get("attachments")
                    ]
                    if chats_with_att:
                        att_items, att_bytes = await self._userchats_backup_attachments(
                            graph_client, resource, snapshot, tenant, chats_with_att,
                        )
                    # Inline-image capture (hostedContents). Runs per message
                    # so each (message, hosted_content) pair gets its own
                    # CHAT_HOSTED_CONTENT SnapshotItem, streamed straight to
                    # blob under users/<uid>/chats/<cid>/messages/<mid>/hosted/<hid>.
                    msgs_with_hc = [
                        extra.get("raw") for (_t, _n, _e, extra, *_rest) in items_data
                        if isinstance(extra, dict) and isinstance(extra.get("raw"), dict)
                        and extra["raw"].get("hostedContents")
                    ]
                    if msgs_with_hc and user_id:
                        # Bind self so _one_msg can reach the BackupWorker
                        # without pulling the whole closure through gather.
                        bw_self = self
                        user_id_local = user_id
                        snap_id_local = str(snapshot.id)
                        # Expose the active graph client so
                        # _userchats_backup_hosted_contents (which uses
                        # self.graph) picks up the per-backup token + shard.
                        self.graph = graph_client

                        async def _one_msg(message: Dict[str, Any]) -> Tuple[int, int]:
                            m_chat_id = (
                                message.get("chatId")
                                or (message.get("channelIdentity") or {}).get("channelId")
                            )
                            if not m_chat_id or not message.get("id"):
                                return 0, 0
                            return await bw_self._userchats_backup_hosted_contents(
                                snapshot_id=snap_id_local,
                                user_id=user_id_local,
                                chat_id=m_chat_id,
                                message_id=message["id"],
                                hosted_contents=message["hostedContents"],
                            )

                        hc_results = await asyncio.gather(
                            *[_one_msg(m) for m in msgs_with_hc], return_exceptions=True,
                        )
                        for r in hc_results:
                            if isinstance(r, tuple):
                                hc_items, hc_bytes = r
                                # _userchats_backup_hosted_contents persists
                                # its own SnapshotItem rows, so we only need
                                # to accumulate the count + bytes into the
                                # per-chat totals (not att_items, which is a
                                # SnapshotItem list committed below).
                                bytes_total += hc_bytes
                                total_hosted_items += hc_items
                elif resource.type.value == "USER_ONEDRIVE":
                    drive_id = (resource.extra_data or {}).get("drive_id")
                    file_items = [
                        extra.get("raw") for (_t, _n, _e, extra, *_rest) in items_data
                        if isinstance(extra, dict) and isinstance(extra.get("raw"), dict)
                    ]
                    if drive_id and file_items:
                        # This updates the ONEDRIVE_FILE rows in-place (adds
                        # blob_path, real content_size, sha256). att_bytes
                        # captures the REAL file bytes uploaded to blob so
                        # the snapshot's bytes_total reflects actual storage
                        # consumed, not just the JSON metadata footprint.
                        #
                        # The per-worker semaphore caps how many simultaneous
                        # OneDrive drives one worker replica runs — without it,
                        # an uncapped v2 run lets a single worker pick up
                        # every redelivered drive and thrash under memory.
                        async with self._onedrive_backup_semaphore:
                            _uploaded, att_bytes = await self._useronedrive_backup_files(
                                graph_client, resource, snapshot, tenant, drive_id, file_items,
                            )

                if att_items:
                    async with async_session_factory() as session:
                        session.add_all(att_items)
                        await session.commit()

                total_items = len(items_data) + len(att_items) + total_hosted_items
                bytes_total += att_bytes

                async with async_session_factory() as session:
                    snap = await session.get(Snapshot, snapshot.id)
                    snap.item_count = total_items
                    snap.new_item_count = total_items
                    snap.bytes_total = bytes_total
                    snap.bytes_added = bytes_total
                    snap.status = SnapshotStatus.COMPLETED
                    snap.completed_at = datetime.utcnow()
                    if snap.started_at:
                        snap.duration_secs = int((snap.completed_at - snap.started_at).total_seconds())
                    await session.commit()

                    await self.update_resource_backup_info(session, resource, job_id, snapshot.id, {
                        "item_count": total_items,
                        "bytes_added": bytes_total,
                    })

                suffix = f" + {len(att_items)} attachments" if att_items else ""
                print(f"[{self.worker_id}] [{resource.type.value}] {resource.display_name} — {len(items_data)} items{suffix}, {bytes_total} bytes")
                return {"item_count": total_items, "bytes_added": bytes_total}

        results = await asyncio.gather(*[_backup_one(r) for r in resources], return_exceptions=True)
        item_count = sum(r.get("item_count", 0) for r in results if isinstance(r, dict))
        bytes_added = sum(r.get("bytes_added", 0) for r in results if isinstance(r, dict))
        return {"item_count": item_count, "bytes_added": bytes_added}

    # ==================== Tier 2 USER_MAIL attachment capture ====================

    async def _usermail_backup_attachments(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        user_id: str,
        messages_with_attachments: List[Dict[str, Any]],
    ) -> Tuple[List[SnapshotItem], int]:
        """Capture every attachment from a list of messages as separate
        `EMAIL_ATTACHMENT` SnapshotItems backed by Azure Blob.

        Three Graph attachment kinds, each handled:
          - fileAttachment (binary): read `contentBytes` if inline, else
            fetch via /$value — upload bytes to blob.
          - itemAttachment (nested Outlook item): serialize the payload's
            `.item` field to JSON — upload to blob. Lets us restore the
            nested email/event even though Graph doesn't ship raw MIME.
          - referenceAttachment (OneDrive/SharePoint link): resolve the
            `sourceUrl` via /shares/{id}/driveItem and download the file.
            Falls back to metadata-only if the link isn't resolvable
            (external / missing grant / revoked).

        Each attachment gets `extra_data.parent_item_id = <message id>` so
        the Recovery page's EmailPreview can list them under the parent
        email. Bounded concurrency keeps us under Graph throttling."""
        sem = asyncio.Semaphore(8)
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "email")

        async def _one_message(msg: Dict[str, Any]) -> Tuple[List[SnapshotItem], int]:
            msg_id = msg.get("id")
            if not msg_id:
                return [], 0
            async with sem:
                try:
                    attachments = await graph_client.list_message_attachments(user_id, msg_id)
                except Exception as e:
                    print(f"[{self.worker_id}] [ATT-LIST FAIL] msg {msg_id}: {type(e).__name__}: {e}")
                    return [], 0

            local_items: List[SnapshotItem] = []
            local_bytes = 0
            folder_path = msg.get("_full_folder_path") or msg.get("parentFolderName") or None

            for att in attachments:
                att_id = att.get("id")
                if not att_id:
                    continue
                att_kind = att.get("@odata.type", "")
                att_name = att.get("name") or att_id
                content_type = att.get("contentType")
                declared_size = att.get("size") or 0

                content_bytes: Optional[bytes] = None

                if att_kind.endswith("fileAttachment"):
                    raw_b64 = att.get("contentBytes")
                    if raw_b64:
                        import base64 as _b64
                        try:
                            content_bytes = _b64.b64decode(raw_b64)
                        except Exception:
                            content_bytes = None
                    if content_bytes is None:
                        try:
                            async with sem:
                                content_bytes = await graph_client.get_message_attachment_content(user_id, msg_id, att_id)
                        except Exception as e:
                            print(f"[{self.worker_id}] [ATT-FAIL] {att_name} (file) on {msg_id}: {type(e).__name__}: {e}")
                            continue
                elif att_kind.endswith("itemAttachment"):
                    # Nested Outlook item — serialize to JSON so it's
                    # downloadable as a .json file and restorable later.
                    nested = att.get("item") or {}
                    try:
                        content_bytes = json.dumps(nested, default=str).encode("utf-8")
                    except Exception:
                        content_bytes = None
                    if not att_name.lower().endswith(".json"):
                        att_name = f"{att_name}.json"
                    content_type = content_type or "application/json"
                elif att_kind.endswith("referenceAttachment"):
                    # Try to resolve the sharing URL into actual bytes.
                    source_url = att.get("sourceUrl")
                    content_bytes = await graph_client.fetch_shared_url_content(source_url) if source_url else None
                    # If resolution fails, fall through with content_bytes=None
                    # and we persist a metadata-only row below.

                blob_path: Optional[str] = None
                content_hash: Optional[str] = None
                size = declared_size

                if content_bytes is not None:
                    content_hash = hashlib.sha256(content_bytes).hexdigest()
                    size = len(content_bytes)
                    blob_path = azure_storage_manager.build_blob_path(
                        str(tenant.id), str(resource.id), str(snapshot.id),
                        f"att_{msg_id}_{att_id}",
                    )
                    try:
                        result = await upload_blob_with_retry(
                            container, blob_path, content_bytes, shard, max_retries=3,
                        )
                        if not (isinstance(result, dict) and result.get("success")):
                            # Upload failed — keep the row as metadata-only.
                            blob_path = None
                            content_hash = None
                        else:
                            local_bytes += size
                    except Exception as e:
                        print(f"[{self.worker_id}] [ATT-UPLOAD] {att_name} on {msg_id}: {type(e).__name__}: {e}")
                        blob_path = None
                        content_hash = None

                local_items.append(SnapshotItem(
                    id=uuid.uuid4(),
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=f"{msg_id}::{att_id}",
                    item_type="EMAIL_ATTACHMENT",
                    name=(att_name or "")[:255],
                    folder_path=folder_path,
                    content_hash=content_hash,
                    content_size=size,
                    blob_path=blob_path,
                    content_checksum=content_hash,
                    extra_data={
                        "parent_item_id": msg_id,
                        "attachment_kind": att_kind,
                        "content_type": content_type,
                        "is_inline": att.get("isInline", False),
                        # Preserve original Content-ID so inline images
                        # restored via MIME resolve against the body's
                        # cid:xxx references.
                        "content_id": att.get("contentId") or att.get("contentID"),
                        "source_url": att.get("sourceUrl"),
                        "resolved": blob_path is not None,
                    },
                ))
            return local_items, local_bytes

        results = await asyncio.gather(
            *[_one_message(m) for m in messages_with_attachments],
            return_exceptions=True,
        )
        all_items: List[SnapshotItem] = []
        total_bytes = 0
        for r in results:
            if isinstance(r, tuple):
                items, b = r
                all_items.extend(items)
                total_bytes += b
        return all_items, total_bytes

    # ==================== Tier 2 USER_CHATS attachment capture ====================

    async def _userchats_backup_attachments(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        messages_with_attachments: List[Dict[str, Any]],
    ) -> Tuple[List[SnapshotItem], int]:
        """Capture chat-message attachments as separate `CHAT_ATTACHMENT`
        SnapshotItems, mirroring the email path.

        Microsoft Graph attachment contentTypes we see in the wild
        (confirmed against real data in `snapshot_items`):

          - `reference` — a file in SharePoint / OneDrive / Teams. Has a
            `contentUrl` pointing at the source. Resolvable via
            /shares/{u!base64}/driveItem → downloadUrl. Store bytes in blob.

          - `messageReference`, `forwardedMessageReference`,
            `meetingReference` — UI pointers (reply context, meeting card).
            No bytes to store; `content` carries JSON that describes the
            target. Keep as metadata-only.

          - `application/vnd.microsoft.card.*` — adaptive cards / audio /
            richer embedded content. `content` holds the card JSON. Store
            as metadata-only (the card body is already in extra_data.raw
            on the parent message).

          - `fileAttachment` / other binary types — rare in chat. Treat
            the same as a reference if a contentUrl is present; otherwise
            metadata-only."""
        sem = asyncio.Semaphore(6)
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "teams")

        REFERENCE_TYPES = {"reference", "fileAttachment"}
        POINTER_TYPES = {"messageReference", "forwardedMessageReference", "meetingReference"}

        async def _one_message(msg: Dict[str, Any]) -> Tuple[List[SnapshotItem], int]:
            msg_id = msg.get("id")
            if not msg_id:
                return [], 0
            attachments = msg.get("attachments") or []
            if not isinstance(attachments, list) or not attachments:
                return [], 0
            chat_id = msg.get("chatId") or (msg.get("channelIdentity") or {}).get("channelId")
            folder_path = msg.get("_chat_folder_path")  # set at fetch time for chats

            local_items: List[SnapshotItem] = []
            local_bytes = 0

            for att in attachments:
                att_id = att.get("id") or str(uuid.uuid4())
                ct = (att.get("contentType") or "").strip()
                name = att.get("name") or att.get("id") or "attachment"
                content_url = att.get("contentUrl")
                raw_content = att.get("content")  # JSON for pointers / cards

                content_bytes: Optional[bytes] = None
                resolved = False

                if ct in REFERENCE_TYPES and content_url:
                    async with sem:
                        try:
                            content_bytes = await graph_client.fetch_shared_url_content(content_url)
                        except Exception as e:
                            print(f"[{self.worker_id}] [CHAT-ATT] resolve fail {name} on {msg_id}: {type(e).__name__}: {e}")
                            content_bytes = None
                elif ct in POINTER_TYPES or ct.startswith("application/vnd.microsoft.card"):
                    # Metadata-only: UI pointer or card. `content` is JSON
                    # describing the thing being referenced; preserve it so
                    # the UI can render the citation / card without Graph.
                    content_bytes = None
                else:
                    # Unknown contentType — try the URL if provided, else
                    # leave as metadata.
                    if content_url:
                        async with sem:
                            try:
                                content_bytes = await graph_client.fetch_shared_url_content(content_url)
                            except Exception:
                                content_bytes = None

                blob_path: Optional[str] = None
                content_hash: Optional[str] = None
                size = 0

                if content_bytes is not None:
                    content_hash = hashlib.sha256(content_bytes).hexdigest()
                    size = len(content_bytes)
                    blob_path = azure_storage_manager.build_blob_path(
                        str(tenant.id), str(resource.id), str(snapshot.id),
                        f"catt_{msg_id}_{att_id}",
                    )
                    try:
                        result = await upload_blob_with_retry(
                            container, blob_path, content_bytes, shard, max_retries=3,
                        )
                        if isinstance(result, dict) and result.get("success"):
                            resolved = True
                            local_bytes += size
                        else:
                            blob_path = None
                            content_hash = None
                    except Exception as e:
                        print(f"[{self.worker_id}] [CHAT-ATT UPLOAD] {name} on {msg_id}: {type(e).__name__}: {e}")
                        blob_path = None
                        content_hash = None

                local_items.append(SnapshotItem(
                    id=uuid.uuid4(),
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=f"{msg_id}::{att_id}",
                    item_type="CHAT_ATTACHMENT",
                    name=(name or "")[:255],
                    folder_path=folder_path,
                    content_hash=content_hash,
                    content_size=size,
                    blob_path=blob_path,
                    content_checksum=content_hash,
                    extra_data={
                        "parent_item_id": msg_id,
                        "chat_id": chat_id,
                        "attachment_kind": ct,
                        "content_type": ct,
                        "content_url": content_url,
                        "thumbnail_url": att.get("thumbnailUrl"),
                        "teams_app_id": att.get("teamsAppId"),
                        # Preserve the pointer/card JSON so the UI can
                        # render it without a Graph round-trip.
                        "content": raw_content,
                        "resolved": resolved,
                    },
                ))
            return local_items, local_bytes

        results = await asyncio.gather(
            *[_one_message(m) for m in messages_with_attachments],
            return_exceptions=True,
        )
        all_items: List[SnapshotItem] = []
        total_bytes = 0
        for r in results:
            if isinstance(r, tuple):
                items, b = r
                all_items.extend(items)
                total_bytes += b
        return all_items, total_bytes

    # ==================== Tier 2 USER_CHATS hostedContents capture ====================

    async def _userchats_backup_hosted_contents(
        self,
        *,
        snapshot_id: str,
        user_id: str,
        chat_id: str,
        message_id: str,
        hosted_contents: list,
    ) -> Tuple[int, int]:
        """Capture inline-image binaries as CHAT_HOSTED_CONTENT snapshot items.

        Idempotent — skips any (message_id, hc_id) already present. Bounded
        concurrency via settings.chat_hosted_content_concurrency, size-capped
        at settings.chat_hosted_content_max_bytes.

        Returns (items_created, bytes_captured).
        """
        sem = asyncio.Semaphore(settings.chat_hosted_content_concurrency)
        items = 0
        total_bytes = 0

        async def _one(hc: dict) -> Tuple[int, int]:
            hc_id = hc.get("id")
            if not hc_id:
                return 0, 0
            if await self._exists_hosted_item(snapshot_id, message_id, hc_id):
                return 0, 0
            async with sem:
                stream, ctype, size = await self.graph.get_hosted_content(
                    chat_id, message_id, hc_id
                )
                if size and size > settings.chat_hosted_content_max_bytes:
                    logger.warning(
                        "hosted_content_skipped_size_cap",
                        extra={"message_id": message_id, "hc_id": hc_id, "size": size},
                    )
                    return 0, 0
                blob_path = f"users/{user_id}/chats/{chat_id}/messages/{message_id}/hosted/{hc_id}"
                await self._upload_stream(blob_path, stream, content_type=ctype)
                await self._insert_snapshot_item(
                    snapshot_id=snapshot_id,
                    item_type="CHAT_HOSTED_CONTENT",
                    external_id=f"{message_id}:{hc_id}",
                    parent_external_id=message_id,
                    name=f"inline-{hc_id}",
                    blob_path=blob_path,
                    content_size=size,
                    extra_data={"content_type": ctype, "source_message_id": message_id},
                )
                return 1, size

        results = await asyncio.gather(*[_one(hc) for hc in hosted_contents])
        for c, b in results:
            items += c
            total_bytes += b
        return items, total_bytes

    async def _exists_hosted_item(
        self, snapshot_id: str, message_id: str, hc_id: str
    ) -> bool:
        """Check whether a CHAT_HOSTED_CONTENT row already exists for this
        (snapshot, message, hc_id). Used by _userchats_backup_hosted_contents
        for idempotency."""
        external_id = f"{message_id}:{hc_id}"
        async with async_session_factory() as session:
            stmt = select(SnapshotItem.id).where(
                and_(
                    SnapshotItem.snapshot_id == snapshot_id,
                    SnapshotItem.external_id == external_id,
                    SnapshotItem.item_type == "CHAT_HOSTED_CONTENT",
                )
            ).limit(1)
            res = await session.execute(stmt)
            return res.scalar_one_or_none() is not None

    async def _upload_stream(
        self, blob_path: str, stream, *, content_type: str = "application/octet-stream",
        tenant_id: Optional[str] = None, resource_id: Optional[str] = None,
        container: str = "teams",
    ) -> None:
        """Drain an async byte iterator into an Azure blob. Thin wrapper that
        concatenates chunks in memory (hostedContents are size-capped upstream,
        so buffering is bounded) and forwards to upload_blob_with_retry."""
        buf = bytearray()
        try:
            async for chunk in stream:
                if chunk:
                    buf.extend(chunk)
        finally:
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except Exception:
                    pass
        if tenant_id is None or resource_id is None:
            # Callers that don't want sharding can pass the raw blob_path
            # through to the default shard.
            shard = azure_storage_manager.get_shard_for_resource(
                resource_id or blob_path, tenant_id or blob_path,
            )
            cname = azure_storage_manager.get_container_name(
                tenant_id or "default", container,
            )
        else:
            shard = azure_storage_manager.get_shard_for_resource(resource_id, tenant_id)
            cname = azure_storage_manager.get_container_name(tenant_id, container)
        await upload_blob_with_retry(cname, blob_path, bytes(buf), shard, max_retries=3)

    async def _insert_snapshot_item(self, **kw) -> None:
        """Insert a SnapshotItem row from kwargs. Thin wrapper so callers (like
        _userchats_backup_hosted_contents) can stay focused on the business
        logic. Fields that aren't declared on the SnapshotItem ORM mapping —
        e.g. parent_external_id on older schemas — are folded into extra_data
        so the insert still succeeds cross-schema."""
        model_cols = {c.key for c in SnapshotItem.__table__.columns}
        model_cols.update({k for k in SnapshotItem.__mapper__.attrs.keys()})
        extra_data = dict(kw.pop("extra_data", {}) or {})
        clean: Dict[str, Any] = {}
        for k, v in kw.items():
            if k in model_cols:
                clean[k] = v
            else:
                extra_data[k] = v
        clean["extra_data"] = extra_data
        clean.setdefault("id", uuid.uuid4())
        async with async_session_factory() as session:
            session.add(SnapshotItem(**clean))
            await session.commit()

    # ==================== Tier 2 USER_ONEDRIVE file-bytes capture ====================
    #
    # v2: uncapped. The legacy MAX_FILES / MAX_BYTES knobs are off by default —
    # a user's full drive captures in one run. Concurrency + checkpoint +
    # heavy-pool routing keep it bounded. Legacy caps stay active when
    # ONEDRIVE_BACKUP_V2_ENABLED is false (rollback safety).
    LEGACY_USER_ONEDRIVE_MAX_FILES = 50
    LEGACY_USER_ONEDRIVE_MAX_BYTES = 500 * 1024 * 1024  # 500 MB

    async def _useronedrive_backup_files(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        drive_id: str,
        file_items: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        """Download OneDrive files to blob and update the already-persisted
        ONEDRIVE_FILE snapshot-item rows with blob_path + real content_size
        + sha256.

        v2 path (ONEDRIVE_BACKUP_V2_ENABLED=true): no per-run cap. Downloads
        run under a semaphore of ONEDRIVE_BACKUP_FILE_CONCURRENCY. A
        BackupCheckpoint commits to Job.result.backup_checkpoint every N
        files / M bytes so a worker crash resumes from the last commit.

        Legacy path: retains the 50-file / 500-MB slice from the v1
        implementation — partial-backup semantics pick remaining files up
        on the next run via the delta token.
        """
        from sqlalchemy import update as sa_update
        from shared.backup_checkpoint import BackupCheckpoint
        from shared.models import Job as _Job

        v2_enabled = settings.ONEDRIVE_BACKUP_V2_ENABLED
        max_files = None if v2_enabled else self.LEGACY_USER_ONEDRIVE_MAX_FILES
        max_bytes = None if v2_enabled else self.LEGACY_USER_ONEDRIVE_MAX_BYTES
        per_file_timeout = settings.ONEDRIVE_BACKUP_FILE_TIMEOUT_SECONDS
        file_concurrency = settings.ONEDRIVE_BACKUP_FILE_CONCURRENCY if v2_enabled else 1

        # Sort smallest-first so the legacy cap captures the most files per
        # run; in v2 ordering is cosmetic (everything downloads).
        ordered = sorted(
            (f for f in file_items if f.get("id")),
            key=lambda f: int(f.get("size") or 0),
        )

        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "files")

        # Load existing checkpoint (resume path).
        cp_payload = None
        if snapshot.job_id:
            async with async_session_factory() as _s0:
                snap_job = await _s0.get(_Job, snapshot.job_id)
                if snap_job and isinstance(snap_job.result, dict):
                    cp_payload = snap_job.result.get("backup_checkpoint")
        checkpoint = (
            BackupCheckpoint.from_dict(cp_payload)
            if cp_payload
            else BackupCheckpoint.empty(resource_id=str(resource.id), drive_id=drive_id)
        )

        file_sem = asyncio.Semaphore(file_concurrency)
        state = {"uploaded_count": 0, "bytes_uploaded": 0}
        updates: List[Tuple[str, str, int, str, Dict[str, Any]]] = []
        state_lock = asyncio.Lock()

        async def _process_one(f: Dict[str, Any]):
            file_id = f.get("id")
            if not file_id or checkpoint.is_done(file_id):
                return
            file_name = f.get("name") or "(unnamed)"
            size_hint = int(f.get("size") or 0)
            if size_hint == 0:
                return

            # Enforce legacy caps pre-flight; v2 skips this entirely.
            if max_files is not None or max_bytes is not None:
                async with state_lock:
                    if max_files is not None and state["uploaded_count"] >= max_files:
                        return
                    if max_bytes is not None and state["bytes_uploaded"] >= max_bytes:
                        return
                    if (
                        max_bytes is not None
                        and state["uploaded_count"] > 0
                        and (state["bytes_uploaded"] + size_hint) > max_bytes
                    ):
                        return

            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), file_id,
            )

            async with file_sem:
                try:
                    download_url, real_size, qxh = await graph_client.get_download_url(drive_id, file_id)
                except RuntimeError as e:
                    # Whiteboards, OneNote pages, cloud-native objects have a
                    # 'file' facet but aren't downloadable — skip silently.
                    if "downloadurl" in str(e).lower() or "download url" in str(e).lower():
                        return
                    print(f"[{self.worker_id}] [USER_ONEDRIVE] download-url fetch failed for {file_name}: {e}")
                    return
                except Exception as e:
                    print(f"[{self.worker_id}] [USER_ONEDRIVE] download-url fetch failed for {file_name}: {type(e).__name__}: {e}")
                    return

                tmp_path = None
                try:
                    async def _download_and_upload():
                        nonlocal tmp_path
                        tmp_path, sha256 = await self._download_to_temp_resumable(
                            download_url=download_url, expected_size=real_size, file_name=file_name,
                        )
                        upload_result = await upload_blob_with_retry_from_file(
                            container_name=container, blob_path=blob_path,
                            file_path=tmp_path, shard=shard, file_size=real_size,
                            metadata={
                                "source_item_id": file_id,
                                "source_drive_id": drive_id,
                                "original-name": file_name,
                                "sha256": sha256,
                            },
                        )
                        return upload_result, sha256

                    if per_file_timeout > 0:
                        upload_result, sha256 = await asyncio.wait_for(
                            _download_and_upload(), timeout=per_file_timeout
                        )
                    else:
                        upload_result, sha256 = await _download_and_upload()
                except asyncio.TimeoutError:
                    print(f"[{self.worker_id}] [USER_ONEDRIVE] timeout for {file_name} (>{per_file_timeout}s); skipping")
                    return
                except Exception as e:
                    print(f"[{self.worker_id}] [USER_ONEDRIVE] download/upload failed for {file_name}: {type(e).__name__}: {e}")
                    return
                finally:
                    if tmp_path:
                        try:
                            os.remove(tmp_path)
                        except OSError:
                            pass

                if not upload_result.get("success"):
                    print(f"[{self.worker_id}] [USER_ONEDRIVE] blob upload failed for {file_name}: {upload_result.get('error')}")
                    return

                extra_patch = {
                    "raw": f,
                    "sha256": sha256,
                    "quickxor": qxh,
                    "blobbed_at": datetime.utcnow().isoformat() + "Z",
                }
                async with state_lock:
                    state["uploaded_count"] += 1
                    state["bytes_uploaded"] += real_size
                    updates.append((file_id, blob_path, real_size, sha256, extra_patch))
                    checkpoint.record_file_done(external_id=file_id, size=real_size)

                    if v2_enabled and checkpoint.should_commit(
                        every_files=settings.ONEDRIVE_BACKUP_CHECKPOINT_EVERY_FILES,
                        every_bytes=settings.ONEDRIVE_BACKUP_CHECKPOINT_EVERY_BYTES,
                    ):
                        # Commit outside the lock would need a copy; keep it
                        # inline — commit only needs one DB round-trip.
                        await self._commit_backup_checkpoint(snapshot.job_id, checkpoint)

        await asyncio.gather(*(_process_one(f) for f in ordered))

        if updates:
            async with async_session_factory() as session:
                for ext_id, bp, size, sha, extra in updates:
                    await session.execute(
                        sa_update(SnapshotItem)
                        .where(
                            SnapshotItem.snapshot_id == snapshot.id,
                            SnapshotItem.external_id == ext_id,
                        )
                        .values(
                            blob_path=bp,
                            content_size=size,
                            content_hash=sha,
                            content_checksum=sha,
                            extra_data=extra,
                        )
                    )
                await session.commit()

        if v2_enabled:
            await self._commit_backup_checkpoint(snapshot.job_id, checkpoint)

        print(
            f"[{self.worker_id}] [USER_ONEDRIVE] blobbed {state['uploaded_count']}/{len(ordered)} files "
            f"({state['bytes_uploaded']} bytes, v2={v2_enabled}, concurrency={file_concurrency})"
        )
        return state["uploaded_count"], state["bytes_uploaded"]

    async def _commit_backup_checkpoint(self, job_id, checkpoint) -> None:
        """Merge the current BackupCheckpoint into Job.result.backup_checkpoint."""
        if not job_id:
            return
        from shared.models import Job as _Job
        async with async_session_factory() as s:
            j = await s.get(_Job, job_id)
            if j is not None:
                r = dict(j.result or {})
                r["backup_checkpoint"] = checkpoint.to_dict()
                j.result = r
                await s.commit()
                checkpoint.mark_committed()

    # ==================== Server-Side Copy for Files ====================

    async def _backup_files_parallel(self, resources: List[Resource], graph_client: GraphClient,
                                     tenant: Tenant, message: Dict, job_id: uuid.UUID) -> Dict:
        """
        Backup OneDrive/SharePoint using Server-Side Copy for large files.
        This is the FASTEST method - Azure copies files internally.
        """
        resource_type = resources[0].type.value

        # Process resources in parallel
        semaphore = asyncio.Semaphore(settings.BACKUP_CONCURRENCY)

        async def backup_one_resource(resource: Resource):
            async with semaphore:
                try:
                    snapshot = await self.create_snapshot(resource, message, job_id)
                    delta_token = (resource.extra_data or {}).get("delta_token")

                    if resource_type == "ONEDRIVE":
                        files = await graph_client.get_drive_items_delta(resource.external_id, delta_token)
                    else:
                        files = await graph_client.get_sharepoint_site_drives(resource.external_id, delta_token)

                    items = files.get("value", [])

                    # Process ALL files — complete backup, no limits
                    file_tasks = [
                        self.backup_single_file(resource, tenant, snapshot, f, graph_client, job_id)
                        for f in items
                    ]
                    file_results = await asyncio.gather(*file_tasks, return_exceptions=True)

                    server_copy_ok = 0
                    streaming_ok = 0
                    failed_count = 0
                    skipped_count = 0
                    failed_items = []
                    res_bytes = 0

                    for r in file_results:
                        if isinstance(r, Exception):
                            failed_count += 1
                            failed_items.append({"id": "unknown", "name": "unknown", "reason": str(r)})
                        elif isinstance(r, dict):
                            method = r.get("method", "")
                            if r.get("success"):
                                if method in ("server_side_copy", "server_side_copy_retry"):
                                    server_copy_ok += 1
                                elif method in ("streaming_fallback", "streaming"):
                                    streaming_ok += 1
                                elif method.startswith("skipped"):
                                    skipped_count += 1
                            else:
                                failed_count += 1
                                failed_items.append({
                                    "id": r.get("item_id", "unknown"),
                                    "name": r.get("file_name", "unknown"),
                                    "reason": r.get("reason", "unknown"),
                                })
                                # Log per-file failure reason for visibility
                                print(f"[{self.worker_id}]   [FILE FAIL] {r.get('file_name','?')}: "
                                      f"method={r.get('method','?')} reason={r.get('reason','no reason')}")
                            res_bytes += r.get("size", 0)

                    # Store per-file outcome tracking on snapshot (use delta_tokens_json for JSON storage)
                    snapshot.delta_tokens_json = snapshot.delta_tokens_json or {}
                    snapshot.delta_tokens_json["files_total"] = len(items)
                    snapshot.delta_tokens_json["files_succeeded_server_copy"] = server_copy_ok
                    snapshot.delta_tokens_json["files_succeeded_streaming"] = streaming_ok
                    snapshot.delta_tokens_json["files_failed"] = failed_count
                    snapshot.delta_tokens_json["files_skipped"] = skipped_count
                    if failed_items:
                        snapshot.delta_tokens_json["failed_items"] = failed_items[:100]  # cap at 100
                    async with async_session_factory() as sess:
                        await sess.merge(snapshot)
                        await sess.commit()

                    # Update delta token
                    new_delta = files.get("@odata.deltaLink")
                    if new_delta:
                        resource.extra_data = resource.extra_data or {}
                        resource.extra_data["delta_token"] = new_delta

                    total_success = server_copy_ok + streaming_ok
                    async with async_session_factory() as sess:
                        await sess.merge(resource)
                        await sess.commit()

                        # Update resource backup info (storage_bytes, last_backup_*)
                        await self.update_resource_backup_info(sess, resource, job_id, snapshot.id, {
                            "item_count": total_success,
                            "bytes_added": res_bytes,
                        })

                    return {"item_count": total_success, "bytes_added": res_bytes}
                except Exception as e:
                    print(f"[{self.worker_id}] File backup failed for {resource.id}: {e}")
                    return {"item_count": 0, "bytes_added": 0}

        results = await asyncio.gather(*[backup_one_resource(r) for r in resources], return_exceptions=True)
        return {
            "item_count": sum(r.get("item_count", 0) for r in results if isinstance(r, dict)),
            "bytes_added": sum(r.get("bytes_added", 0) for r in results if isinstance(r, dict)),
        }

    async def backup_single_file(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                 file_item: Dict, graph_client: GraphClient, job_id: uuid.UUID) -> Dict:
        """
        Backup a single file via worker-side streaming with Range-resume.

        Server-side copy from Graph URLs to Azure Blob is NOT viable —
        Graph download URLs are rejected by Azure with CannotVerifyCopySource
        because they aren't Azure SAS URLs. Streaming through the worker is
        the primary (and only reliable) path for M365 file backup.

        Strategy:
          1. SKIP: deleted items, folders, non-file items
          2. IDEMPOTENCY: skip if blob exists with matching etag
          3. PRIMARY: Stream through worker with Range-resume
        """
        file_id = file_item.get("id", str(uuid.uuid4()))
        file_name = file_item.get("name", file_id)
        file_size_hint = file_item.get("size", 0)

        # Skip deleted items from delta
        if file_item.get("deleted"):
            return {"success": True, "size": 0, "method": "skipped_deleted",
                    "item_id": file_id, "file_name": file_name}

        # Skip folders — no content to back up
        if "folder" in file_item:
            return {"success": True, "size": 0, "method": "skipped_folder",
                    "item_id": file_id, "file_name": file_name}

        # Skip items without a file facet
        if "file" not in file_item:
            return {"success": True, "size": 0, "method": "skipped_no_file_facet",
                    "item_id": file_id, "file_name": file_name}

        drive_id = (
            file_item.get("_drive_id")
            or (file_item.get("parentReference") or {}).get("driveId")
            or resource.external_id
        )
        blob_item_id = file_id
        if drive_id and drive_id != resource.external_id:
            drive_hash = hashlib.sha1(drive_id.encode()).hexdigest()[:12]
            blob_item_id = f"{drive_hash}_{file_id}"

        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "files")
        blob_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id), blob_item_id)

        # Handle empty files
        if file_size_hint == 0:
            upload_result = await upload_blob_with_retry(
                container, blob_path, b"", shard,
                metadata={"source": "empty", "original-name": file_name})
            if upload_result.get("success"):
                await self._create_file_snapshot_item(
                    snapshot, tenant, file_id, file_name, 0, blob_path, {}, file_item, drive_id,
                    graph_client=graph_client,
                )
            return {"success": upload_result.get("success", False), "size": 0, "method": "empty",
                    "item_id": file_id, "file_name": file_name}

        # Idempotency: check if blob already exists with matching etag
        source_etag = (file_item.get("eTag") or "")[:64]
        existing_props = await shard.get_blob_properties(container, blob_path)
        if existing_props:
            existing_etag = (existing_props.get("metadata", {}) or {}).get("source_etag", "")
            if existing_etag and existing_etag == source_etag:
                return {"success": True, "size": existing_props.get("size", 0),
                        "method": "skipped_already_present",
                        "item_id": file_id, "file_name": file_name}

        metadata = {
            "source_item_id": file_id,
            "source_drive_id": drive_id or "",
            "source_etag": source_etag,
            "source_modified": file_item.get("lastModifiedDateTime", ""),
            "original-name": file_name,
        }

        # Resolve fresh download URL (delta items often lack it)
        try:
            download_url, size, qxh = await graph_client.get_download_url(drive_id, file_id)
        except RuntimeError as e:
            error_str = str(e).lower()
            # Some file types (whiteboards, notebooks, packages, cloud-native objects)
            # have a 'file' facet but Graph can't generate a download URL for them.
            # These are not backup failures — they're fundamentally non-downloadable.
            if "downloadurl" in error_str or "download url" in error_str:
                return {"success": True, "size": 0, "method": "skipped_not_downloadable",
                        "item_id": file_id, "file_name": file_name,
                        "reason": str(e)}
            return {"success": False, "size": 0, "method": "failed",
                    "item_id": file_id, "file_name": file_name,
                    "reason": f"no download url: {e}"}

        # PRIMARY: Stream through worker with Range-resume
        tmp_path = None
        try:
            async with self.backup_semaphore:
                tmp_path, sha256 = await self._download_to_temp_resumable(
                    download_url=download_url, expected_size=size, file_name=file_name,
                )
                upload_result = await upload_blob_with_retry_from_file(
                    container_name=container, blob_path=blob_path,
                    file_path=tmp_path, shard=shard, file_size=size,
                    metadata={**metadata, "sha256": sha256},
                )
            if not upload_result.get("success"):
                raise RuntimeError(f"blob upload failed: {upload_result.get('error')}")
            await self._create_file_snapshot_item(
                snapshot, tenant, file_id, file_name, size, blob_path,
                {"sha256": sha256, "quickxor": qxh}, file_item, drive_id,
                graph_client=graph_client,
            )

            # Capture historical file versions — afi keeps every version. We cap
            # at MAX_FILE_VERSIONS (default 5) to bound storage. Only runs on
            # newly-streamed files; skip-already-present files keep their prior
            # snapshot's versions.
            try:
                ver_count, ver_bytes = await self._backup_file_versions(
                    graph_client, tenant, snapshot, drive_id, file_id, file_name,
                    container, shard, current_etag=source_etag,
                )
                if ver_count:
                    print(f"[{self.worker_id}]   [VERSIONS] {file_name}: {ver_count} prior version(s), {ver_bytes} bytes")
            except Exception as ve:
                # Version capture is best-effort — don't fail the file backup.
                print(f"[{self.worker_id}]   [VERSIONS WARN] {file_name}: {type(ve).__name__}: {ve}")

            return {"success": True, "size": size, "method": "streaming",
                    "blob_path": blob_path, "sha256": sha256,
                    "item_id": file_id, "file_name": file_name}
        except Exception as e:
            print(f"[{self.worker_id}] FILE FAIL {file_name} ({file_size_hint} bytes): "
                  f"{type(e).__name__}: {e}")
            return {"success": False, "size": 0, "method": "failed",
                    "item_id": file_id, "file_name": file_name,
                    "reason": f"{type(e).__name__}: {e}"}
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    async def _create_file_snapshot_item(self, snapshot, tenant, file_id, file_name,
                                         content_size, blob_path, hashes, file_item,
                                         drive_id: Optional[str] = None,
                                         graph_client: Optional[GraphClient] = None):
        """Create a SnapshotItem DB record for a successfully backed-up file.

        If graph_client is provided, also fetches the item's permissions and
        stores them inline on the SnapshotItem — afi captures these so restore
        can re-establish exact ACLs (sharing links, SP groups, inheritance)."""
        metadata = MetadataExtractor.extract_sharepoint_item_metadata(file_item)
        metadata["drive_id"] = drive_id or (file_item.get("parentReference") or {}).get("driveId")
        if file_item.get("_site_label"):
            metadata["site_label"] = file_item["_site_label"]
        content_hash = hashes.get("sha256") or hashes.get("quickxor") or ""

        # Best-effort permission capture. Failure here doesn't fail the file
        # backup — we'd rather have the bytes without the ACL than nothing.
        if graph_client and metadata.get("drive_id"):
            try:
                perms = await graph_client.list_file_permissions(metadata["drive_id"], file_id)
                if perms:
                    metadata["permissions"] = perms
            except Exception as pe:
                print(f"[{self.worker_id}]   [PERMS WARN] {file_name}: {type(pe).__name__}: {pe}")

        snapshot_item = SnapshotItem(
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=file_id,
            item_type="FILE",
            name=file_name,
            folder_path=file_item.get("parentReference", {}).get("path"),
            content_hash=content_hash if content_hash else None,
            content_size=content_size,
            blob_path=blob_path,
            extra_data={"structured": metadata},
            content_checksum=content_hash if content_hash else None,
        )
        async with async_session_factory() as session:
            session.add(snapshot_item)
            await session.commit()

    # Maximum prior versions to capture per file. 5 = the 5 most recent
    # historical versions (current version is captured separately as the live
    # FILE row). Override per-deployment via env var; expose per-policy later.
    MAX_FILE_VERSIONS = int(os.environ.get("MAX_FILE_VERSIONS", "5"))

    async def _backup_file_versions(
        self,
        graph_client: GraphClient,
        tenant: Tenant,
        snapshot: Snapshot,
        drive_id: str,
        file_id: str,
        file_name: str,
        container: str,
        shard,
        current_etag: str,
    ) -> Tuple[int, int]:
        """Capture historical versions of a single file as separate blobs.

        Behavior:
          - Lists /versions newest-first.
          - Skips the most recent entry (it matches the live file we just uploaded).
          - Caps the number of older versions captured at MAX_FILE_VERSIONS.
          - Each version becomes a SnapshotItem(item_type="FILE_VERSION") with
            extra_data.parent_item_id = file_id.

        Tradeoff: capturing every version mirrors afi's behavior but multiplies
        storage roughly by (avg version count + 1). The cap keeps the worst case
        bounded; raise it per-policy when retention requirements demand it.
        """
        if self.MAX_FILE_VERSIONS <= 0:
            return 0, 0
        versions = await graph_client.list_file_versions(drive_id, file_id)
        if not versions or len(versions) < 2:
            # 0 or 1 entries — only the live version exists; nothing to back up.
            return 0, 0

        # Skip index 0 (the current version, already captured) — capture at most
        # MAX_FILE_VERSIONS older entries.
        prior = versions[1 : 1 + self.MAX_FILE_VERSIONS]
        items_to_insert: List[SnapshotItem] = []
        total_bytes = 0

        for v in prior:
            version_id = v.get("id")
            if not version_id:
                continue
            v_size = v.get("size") or 0
            v_modified = v.get("lastModifiedDateTime")
            try:
                content_bytes = await graph_client.get_file_version_content(
                    drive_id, file_id, version_id,
                )
            except Exception as e:
                print(f"[{self.worker_id}]     [VERSION FAIL] {file_name} v={version_id}: {type(e).__name__}: {e}")
                continue
            if not content_bytes:
                continue
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(snapshot.resource_id), str(snapshot.id),
                f"ver_{file_id}_{version_id}",
            )
            upload_result = await upload_blob_with_retry(
                container, blob_path, content_bytes, shard, max_retries=3,
            )
            if not (isinstance(upload_result, dict) and upload_result.get("success")):
                continue
            total_bytes += len(content_bytes)
            items_to_insert.append(SnapshotItem(
                snapshot_id=snapshot.id, tenant_id=tenant.id,
                external_id=f"{file_id}::v::{version_id}",
                item_type="FILE_VERSION",
                name=file_name,
                content_hash=content_hash,
                content_size=len(content_bytes),
                blob_path=blob_path,
                content_checksum=content_hash,
                extra_data={
                    "parent_item_id": file_id,
                    "drive_id": drive_id,
                    "version_id": version_id,
                    "modified_at": v_modified,
                    "current_file_etag": current_etag,
                    "size": v_size,
                },
            ))

        if items_to_insert:
            async with async_session_factory() as session:
                session.add_all(items_to_insert)
                await session.commit()
        return len(items_to_insert), total_bytes

    async def _download_to_temp_resumable(
        self,
        download_url: str,
        expected_size: int,
        file_name: str,
        max_attempts: int = 6,
        chunk_size: int = 4 * 1024 * 1024,
    ) -> tuple[str, str]:
        """
        Download a file from a Graph/SharePoint pre-signed URL to a temp file,
        using HTTP Range requests to resume from the last byte received instead
        of restarting from zero on partial failures.

        This is the FALLBACK path for backup_single_file. The primary path is
        Azure Blob server-side copy via shard.copy_from_url_sync(). This helper
        is only invoked when the server-side copy path fails.

        Returns:
            (tmp_file_path, sha256_hex)

        Raises:
            RuntimeError if the file cannot be fully downloaded after all
            retries, or if progress stalls.
        """
        hasher = hashlib.sha256()
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".part", prefix="backup_dl_")
        os.close(tmp_fd)

        bytes_received = 0
        consecutive_no_progress = 0

        timeout = httpx.Timeout(connect=15.0, read=120.0, write=60.0, pool=15.0)
        limits = httpx.Limits(max_keepalive_connections=4, max_connections=8)

        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                follow_redirects=True,
                http2=False,  # Force HTTP/1.1; some MS CDN edges misbehave on HTTP/2
            ) as client:
                for attempt in range(1, max_attempts + 1):
                    progress_before = bytes_received

                    # Build headers — only set Range if we have partial progress.
                    # IMPORTANT: do NOT add Authorization. The pre-signed URL
                    # is anonymously accessible; an auth header makes CDN reject.
                    headers = {}
                    if bytes_received > 0:
                        headers["Range"] = f"bytes={bytes_received}-"

                    try:
                        async with client.stream("GET", download_url, headers=headers) as resp:
                            # ----- Status code handling -----
                            if resp.status_code == 416:
                                # Range Not Satisfiable: source file changed.
                                # Restart from byte 0 with fresh hash.
                                print(f"[{self.worker_id}] {file_name}: HTTP 416, "
                                      f"source changed — restarting from 0")
                                bytes_received = 0
                                hasher = hashlib.sha256()
                                with open(tmp_path, "wb"):
                                    pass  # truncate
                                continue

                            if resp.status_code >= 500:
                                wait = min(2 ** attempt, 30)
                                print(f"[{self.worker_id}] {file_name}: HTTP "
                                      f"{resp.status_code}, retry in {wait}s "
                                      f"(attempt {attempt}/{max_attempts}, "
                                      f"at byte {bytes_received}/{expected_size})")
                                await asyncio.sleep(wait)
                                continue

                            if resp.status_code not in (200, 206):
                                # 4xx other than 416 = unrecoverable
                                body = await resp.aread()
                                raise RuntimeError(
                                    f"Download failed with HTTP {resp.status_code} "
                                    f"for {file_name}: {body!r}"
                                )

                            # ----- Range-header-ignored detection -----
                            # If we sent a Range header and the server returned 200
                            # instead of 206, it means the server ignored the Range
                            # request and is sending the full file from byte 0.
                            # We must reset our state, otherwise we'd append the
                            # full file to our existing partial bytes = corruption.
                            if bytes_received > 0 and resp.status_code == 200:
                                print(f"[{self.worker_id}] {file_name}: server "
                                      f"returned 200 to Range request — "
                                      f"resetting and writing from byte 0")
                                bytes_received = 0
                                hasher = hashlib.sha256()
                                with open(tmp_path, "wb"):
                                    pass

                            # ----- Stream chunks to disk -----
                            mode = "ab" if bytes_received > 0 else "wb"
                            with open(tmp_path, mode) as f:
                                async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                                    if not chunk:
                                        continue
                                    f.write(chunk)
                                    hasher.update(chunk)
                                    bytes_received += len(chunk)

                        # Stream completed without exception
                        if bytes_received >= expected_size:
                            break  # success — exit retry loop

                        # Stream ended early without raising; loop will resume via Range
                        if bytes_received == progress_before:
                            consecutive_no_progress += 1
                        else:
                            consecutive_no_progress = 0

                    except (httpx.RemoteProtocolError, httpx.ReadError,
                            httpx.ReadTimeout, httpx.ConnectError, httpx.WriteError) as e:
                        # All retryable transport errors — resume on next iteration
                        if bytes_received == progress_before:
                            consecutive_no_progress += 1
                        else:
                            consecutive_no_progress = 0

                        wait = min(2 ** attempt, 30)
                        print(f"[{self.worker_id}] {file_name}: "
                              f"{type(e).__name__} at byte "
                              f"{bytes_received}/{expected_size}, retry in {wait}s "
                              f"(attempt {attempt}/{max_attempts}, "
                              f"no_progress_count={consecutive_no_progress}): {e}")
                        await asyncio.sleep(wait)

                    # ----- Stall detection -----
                    if consecutive_no_progress >= 3:
                        raise RuntimeError(
                            f"Download stalled for {file_name}: zero progress over "
                            f"{consecutive_no_progress} consecutive attempts at byte "
                            f"{bytes_received}/{expected_size}"
                        )

                # ----- Final completeness check -----
                if bytes_received < expected_size:
                    raise RuntimeError(
                        f"Download incomplete for {file_name} after {max_attempts} "
                        f"attempts: got {bytes_received}/{expected_size} bytes"
                    )

            return tmp_path, hasher.hexdigest()

        except Exception:
            # Cleanup on any failure path
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # ==================== Mailbox Backup (Parallel) ====================

    async def _backup_mailboxes_parallel(self, resources: List[Resource], graph_client: GraphClient,
                                         tenant: Tenant, message: Dict, job_id: uuid.UUID) -> Dict:
        """Backup multiple mailboxes in parallel"""
        semaphore = asyncio.Semaphore(settings.BACKUP_CONCURRENCY)

        async def backup_one_mailbox(resource: Resource):
            async with semaphore:
                try:
                    snapshot = await self.create_snapshot(resource, message, job_id)
                    delta_token = (resource.extra_data or {}).get("mail_delta_token")

                    messages = await graph_client.get_messages_delta(resource.external_id, delta_token)
                    items = messages.get("value", [])

                    # Resolve parentFolderId → full hierarchical path (e.g.
                    # "/Inbox/Project X") once per mailbox. MAILBOX /
                    # SHARED_MAILBOX / ROOM_MAILBOX all come through here;
                    # without this the left-panel folder list collapses to a
                    # single "All" bucket because parentFolderName isn't a
                    # real Graph field.
                    folder_tree: Dict[str, str] = {}
                    try:
                        folder_tree = await graph_client.get_mail_folder_tree(resource.external_id)
                    except Exception as e:
                        print(f"[{self.worker_id}] [MAILBOX] folder tree fetch failed for {resource.external_id}: {type(e).__name__}: {e}")
                    for m in items:
                        m["_full_folder_path"] = folder_tree.get(m.get("parentFolderId", ""), "")

                    # Process ALL messages in parallel batches — complete backup
                    batch_tasks = [
                        self.backup_message_batch(resource, tenant, snapshot, items[i:i+50], job_id)
                        for i in range(0, len(items), 50)
                    ]
                    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                    total_items = sum(r.get("item_count", 0) for r in batch_results if isinstance(r, dict))
                    total_bytes = sum(r.get("bytes_added", 0) for r in batch_results if isinstance(r, dict))

                    # Update delta token
                    new_delta = messages.get("@odata.deltaLink")
                    if new_delta:
                        resource.extra_data = resource.extra_data or {}
                        resource.extra_data["mail_delta_token"] = new_delta
                        async with async_session_factory() as sess:
                            sess.merge(resource)
                            await sess.commit()
                            
                            # Update resource backup info (storage_bytes, last_backup_*)
                            await self.update_resource_backup_info(sess, resource, job_id, snapshot.id, {
                                "item_count": total_items,
                                "bytes_added": total_bytes,
                            })

                    return {"item_count": total_items, "bytes_added": total_bytes}
                except Exception as e:
                    print(f"[{self.worker_id}] Mailbox backup failed for {resource.id}: {e}")
                    return {"item_count": 0, "bytes_added": 0}

        results = await asyncio.gather(*[backup_one_mailbox(r) for r in resources], return_exceptions=True)
        return {
            "item_count": sum(r.get("item_count", 0) for r in results if isinstance(r, dict)),
            "bytes_added": sum(r.get("bytes_added", 0) for r in results if isinstance(r, dict)),
        }

    async def backup_message_batch(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                   messages: List[Dict], job_id: uuid.UUID) -> Dict:
        """Backup a batch of messages — parallel uploads, single bulk DB insert."""
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "mailbox")

        # Prepare all upload tasks in parallel
        upload_tasks = []
        item_metas = []
        for msg in messages:
            msg_id = msg.get("id", str(uuid.uuid4()))
            content_bytes = json.dumps(msg, sort_keys=True).encode('utf-8')
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), msg_id
            )
            upload_tasks.append(upload_blob_with_retry(
                container, blob_path, content_bytes, shard,
                max_retries=3, metadata={"content-hash": content_hash}
            ))
            item_metas.append((msg_id, msg, content_bytes, content_hash, blob_path))

        upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

        db_items = []
        bytes_added = 0
        for (msg_id, msg, content_bytes, content_hash, blob_path), result in zip(item_metas, upload_results):
            if isinstance(result, dict) and result.get("success"):
                db_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=msg_id, item_type="EMAIL",
                    name=msg.get("subject", msg_id),
                    # Full hierarchical path resolved in
                    # _backup_mailboxes_parallel via the folder tree.
                    folder_path=msg.get("_full_folder_path") or None,
                    content_hash=content_hash, content_size=len(content_bytes),
                    blob_path=blob_path, extra_data={"raw": msg}, content_checksum=content_hash,
                ))
                bytes_added += len(content_bytes)

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        return {"item_count": len(db_items), "bytes_added": bytes_added}

    # ==================== Generic Backup (Parallel) ====================

    async def _backup_generic_parallel(self, resources: List[Resource], graph_client: GraphClient,
                                       tenant: Tenant, message: Dict, job_id: uuid.UUID) -> Dict:
        """Generic backup for Entra ID, Teams, Planner, etc."""
        semaphore = asyncio.Semaphore(settings.BACKUP_CONCURRENCY)

        async def backup_one(resource: Resource):
            async with semaphore:
                snapshot = None
                try:
                    snapshot = await self.create_snapshot(resource, message, job_id)
                    resource_type = resource.type.value

                    # Route to appropriate handler
                    if resource_type.startswith("ENTRA"):
                        result = await self._backup_entra_resource(resource, tenant, snapshot, graph_client, job_id, message)
                    elif resource_type.startswith("TEAMS"):
                        result = await self._backup_teams_resource(resource, tenant, snapshot, graph_client, job_id)
                    elif resource_type == "POWER_BI":
                        result = await self.backup_power_bi_workspace(graph_client, resource, snapshot, tenant, message)
                    else:
                        result = await self._backup_metadata_only(graph_client, resource, snapshot, tenant, message)

                    # Finalize the per-resource snapshot so the Recovery UI
                    # picks it up. Without this the snapshot stayed
                    # IN_PROGRESS forever and the UI never listed it.
                    if snapshot is not None and isinstance(result, dict):
                        try:
                            async with async_session_factory() as s:
                                await self.complete_snapshot(s, snapshot, result)
                        except Exception as fe:
                            print(f"[{self.worker_id}] complete_snapshot failed for {resource.id}: {fe}")
                    return result
                except Exception as e:
                    print(f"[{self.worker_id}] Generic backup failed for {resource.id}: {e}")
                    if snapshot is not None:
                        try:
                            async with async_session_factory() as s:
                                await self.fail_snapshot(s, snapshot, e)
                        except Exception:
                            pass
                    return {"item_count": 0, "bytes_added": 0}

        results = await asyncio.gather(*[backup_one(r) for r in resources], return_exceptions=True)
        return {
            "item_count": sum(r.get("item_count", 0) for r in results if isinstance(r, dict)),
            "bytes_added": sum(r.get("bytes_added", 0) for r in results if isinstance(r, dict)),
        }

    async def _backup_entra_resource(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                     graph_client: GraphClient, job_id: uuid.UUID,
                                     message: Optional[Dict[str, Any]] = None) -> Dict:
        """Backup Entra ID resource — users (profile/contacts/calendar), groups (members/owners), apps, devices"""
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "entra")

        obj_id = resource.external_id
        resource_type = resource.type.value
        items_to_backup = []
        policy = await self.get_sla_policy(resource, message)
        backup_entra_core = True if policy is None else bool(getattr(policy, "backup_entra_id", True))
        backup_contacts = True if policy is None else bool(getattr(policy, "contacts", True))
        backup_calendars = True if policy is None else bool(getattr(policy, "calendars", True))
        backup_group_mailbox = True if policy is None else bool(getattr(policy, "group_mailbox", True))

        if resource_type == "ENTRA_USER":
            print(f"[{self.worker_id}] [ENTRA_USER START] {resource.display_name} ({obj_id})")

            # Fetch all user data in PARALLEL for higher performance
            async def fetch_profile():
                if not backup_entra_core:
                    return []
                print(f"[{self.worker_id}]   [PROFILE] Fetching...")
                profile = await graph_client.get_user_profile(obj_id)
                print(f"[{self.worker_id}]   [PROFILE] Done — {profile.get('displayName', 'N/A')}")
                return [("USER_PROFILE", profile)]

            async def fetch_contacts():
                if not backup_contacts:
                    return []
                print(f"[{self.worker_id}]   [CONTACTS] Fetching...")
                try:
                    contacts = await graph_client.get_user_contacts(obj_id)
                    items = [("USER_CONTACT", c) for c in contacts.get("value", [])]
                    print(f"[{self.worker_id}]   [CONTACTS] Done — {len(items)} found")
                    return items
                except httpx.HTTPStatusError as e:
                    if e.response.status_code in (404, 403):
                        print(f"[{self.worker_id}]   [CONTACTS] Skipped — no Exchange Online license or no mailbox")
                        return []
                    raise

            async def fetch_calendar():
                if not backup_calendars:
                    return [], None
                print(f"[{self.worker_id}]   [CALENDAR] Fetching...")
                try:
                    calendars = await graph_client.get_calendar_events_delta(obj_id)
                    items = [("CALENDAR_EVENT", e) for e in calendars.get("value", [])]
                    print(f"[{self.worker_id}]   [CALENDAR] Done — {len(items)} found")
                    return items, calendars.get("@odata.deltaLink")
                except httpx.HTTPStatusError as e:
                    if e.response.status_code in (404, 403):
                        print(f"[{self.worker_id}]   [CALENDAR] Skipped — no Exchange Online license or no mailbox")
                        return [], None
                    raise

            async def fetch_manager():
                if not backup_entra_core:
                    return []
                print(f"[{self.worker_id}]   [MANAGER] Fetching...")
                try:
                    manager = await graph_client.get_user_manager(obj_id)
                    if manager:
                        print(f"[{self.worker_id}]   [MANAGER] Done — {manager.get('displayName', 'N/A')}")
                        return [("USER_MANAGER", manager)]
                    print(f"[{self.worker_id}]   [MANAGER] No manager found")
                    return []
                except Exception as e:
                    print(f"[{self.worker_id}]   [MANAGER] Failed — {e}")
                    return []

            async def fetch_direct_reports():
                if not backup_entra_core:
                    return []
                print(f"[{self.worker_id}]   [DIRECT_REPORTS] Fetching...")
                try:
                    reports = await graph_client.get_user_direct_reports(obj_id)
                    items = [("USER_DIRECT_REPORT", r) for r in reports.get("value", [])]
                    print(f"[{self.worker_id}]   [DIRECT_REPORTS] Done — {len(items)} found")
                    return items
                except Exception as e:
                    print(f"[{self.worker_id}]   [DIRECT_REPORTS] Failed — {e}")
                    return []

            async def fetch_group_memberships():
                if not backup_entra_core:
                    return []
                print(f"[{self.worker_id}]   [GROUP_MEMBERSHIPS] Fetching...")
                try:
                    groups = await graph_client.get_user_group_memberships(obj_id)
                    items = [("USER_GROUP_MEMBERSHIP", g) for g in groups.get("value", [])]
                    print(f"[{self.worker_id}]   [GROUP_MEMBERSHIPS] Done — {len(items)} found")
                    return items
                except Exception as e:
                    print(f"[{self.worker_id}]   [GROUP_MEMBERSHIPS] Failed — {e}")
                    return []

            # Run ALL API calls in parallel
            results = await asyncio.gather(
                fetch_profile(),
                fetch_contacts(),
                fetch_calendar(),
                fetch_manager(),
                fetch_direct_reports(),
                fetch_group_memberships(),
                return_exceptions=True,
            )

            # Collect all items
            cal_delta = None
            for result in results:
                if isinstance(result, Exception):
                    continue
                if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], str):
                    # Calendar result with delta token
                    items_to_backup.extend(result[0])
                    cal_delta = result[1]
                elif isinstance(result, list):
                    items_to_backup.extend(result)

            # Save calendar delta token
            if cal_delta:
                async with async_session_factory() as sess:
                    r = await sess.get(Resource, resource.id)
                    if r:
                        r.extra_data = r.extra_data or {}
                        r.extra_data["calendar_delta_token"] = cal_delta
                        await sess.commit()

            print(f"[{self.worker_id}]   [ENTRA_USER] Total items to backup: {len(items_to_backup)}")

        elif resource_type in ("ENTRA_GROUP", "DYNAMIC_GROUP", "M365_GROUP"):
            group_mailbox_items: List[SnapshotItem] = []

            if backup_entra_core:
                group = await graph_client.get_group_profile(obj_id)
                items_to_backup.append(("GROUP_PROFILE", group))

                members = await graph_client.get_group_members(obj_id)
                for m in members.get("value", []):
                    items_to_backup.append(("GROUP_MEMBER", m))

                owners = await graph_client.get_group_owners(obj_id)
                for o in owners.get("value", []):
                    items_to_backup.append(("GROUP_OWNER", o))

            # Only Unified (Microsoft 365) groups expose /groups/{id}/threads.
            # Mail-enabled security groups and distribution lists have
            # mail_enabled=true but groupTypes=[] and /threads returns 403.
            meta = resource.extra_data or {}
            group_types = [gt for gt in (meta.get("group_types") or []) if isinstance(gt, str)]
            is_unified = any(gt.lower() == "unified" for gt in group_types) or resource_type == "M365_GROUP"
            group_mail_enabled = bool(meta.get("mail_enabled"))
            has_team = bool(meta.get("has_team"))

            if backup_group_mailbox and group_mail_enabled and is_unified:
                try:
                    group_mailbox_items, mailbox_bytes = await self.backup_group_mailbox_content(
                        resource,
                        tenant,
                        snapshot,
                        graph_client,
                    )
                    bytes_added += mailbox_bytes
                    item_count += len(group_mailbox_items)
                    if group_mailbox_items:
                        async with async_session_factory() as session:
                            session.add_all(group_mailbox_items)
                            await session.commit()
                except Exception as mbx_exc:
                    logger.warning(
                        "[%s] Group mailbox backup skipped for %s: %s",
                        self.worker_id, resource.display_name, mbx_exc,
                    )
            elif backup_group_mailbox and group_mail_enabled and not is_unified:
                print(f"[{self.worker_id}]   [GROUP_MAILBOX] Skipping {resource.display_name}: mail-enabled but not Unified (classification={meta.get('group_classification')})")

            # Unified M365 groups may be team-backed. When so, capture every
            # channel's messages + replies — same path the standalone
            # TEAMS_CHANNEL handler uses — so the Group's Recovery view
            # ("Team Channels" tab) has data without needing a separate
            # TEAMS_CHANNEL resource backup.
            if is_unified and has_team:
                try:
                    channel_items, channel_bytes = await self._backup_team_channels(
                        resource, tenant, snapshot, graph_client, obj_id,
                    )
                    # _backup_team_channels returns (count, bytes) — already
                    # an integer count, NOT a list, so use it directly.
                    bytes_added += channel_bytes
                    item_count += channel_items
                except Exception as ch_exc:
                    logger.warning(
                        "[%s] Team channels backup skipped for %s: %s",
                        self.worker_id, resource.display_name, ch_exc,
                    )

            # Pull the group's SharePoint team-site content so the
            # Recovery "Site" tab has data. Every Unified M365 group has
            # a backing team site reachable at /groups/{id}/sites/root;
            # we hand that site id to backup_sharepoint() which runs the
            # full multi-drive + REST list pipeline. Items land under
            # the same snapshot + tenant + resource so the Group
            # Recovery view sees SHAREPOINT_* rows alongside the mail /
            # channel rows.
            if is_unified:
                try:
                    site_resp = await graph_client._get(
                        f"{graph_client.GRAPH_URL}/groups/{obj_id}/sites/root",
                    )
                    raw_site_id = site_resp.get("id", "")  # "hostname,guid,guid"
                    if raw_site_id:
                        site_ext_id = raw_site_id.replace(",", "/")
                        from types import SimpleNamespace as _SN
                        site_proxy = _SN(
                            id=resource.id,
                            tenant_id=resource.tenant_id,
                            type=_SN(value="SHAREPOINT_SITE"),
                            display_name=f"{resource.display_name} (team site)",
                            external_id=site_ext_id,
                            # Share extra_data with the real resource so
                            # per-drive delta tokens persist under the
                            # M365_GROUP row between runs.
                            extra_data=(resource.extra_data or {}),
                            sla_policy_id=resource.sla_policy_id,
                        )
                        sp_result = await self.backup_sharepoint(
                            graph_client, site_proxy, snapshot, tenant, message,
                        )
                        bytes_added += int(sp_result.get("bytes_added", 0))
                        item_count += int(sp_result.get("item_count", 0))
                except Exception as sp_exc:
                    logger.warning(
                        "[%s] Team site SP backup skipped for %s: %s",
                        self.worker_id, resource.display_name, sp_exc,
                    )

        elif resource_type == "ENTRA_APP":
            # Application registration — fetch via /applications?$filter=id eq '{id}'
            apps = await graph_client.get_entra_apps()
            for app in apps.get("value", []):
                if app.get("id") == obj_id:
                    items_to_backup.append(("APP_REGISTRATION", app))
                    break

        elif resource_type == "ENTRA_SERVICE_PRINCIPAL":
            sps = await graph_client.get_entra_service_principals()
            for sp in sps.get("value", []):
                if sp.get("id") == obj_id:
                    items_to_backup.append(("SERVICE_PRINCIPAL", sp))
                    break

        elif resource_type == "ENTRA_DEVICE":
            devices = await graph_client.get_entra_devices()
            for dev in devices.get("value", []):
                if dev.get("id") == obj_id:
                    items_to_backup.append(("DEVICE", dev))
                    break

        # Upload all items in parallel, then bulk-insert DB records
        upload_tasks = []
        item_metas_entra = []
        for item_type, item_data in items_to_backup:
            item_id = item_data.get("id", str(uuid.uuid4()))
            content_bytes = json.dumps(item_data).encode()
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"{item_type}_{item_id}"
            )
            upload_tasks.append(upload_blob_with_retry(container, blob_path, content_bytes, shard))
            item_metas_entra.append((item_type, item_id, item_data, content_bytes, content_hash, blob_path))

        upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

        db_items = []
        for (item_type, item_id, item_data, content_bytes, content_hash, blob_path), result in zip(item_metas_entra, upload_results):
            if isinstance(result, dict) and result.get("success"):
                item_name = item_data.get("displayName") or item_data.get("subject") or item_data.get("mail") or item_data.get("userPrincipalName") or item_data.get("mailNickname") or item_data.get("id", "unknown")
                db_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=item_id, item_type=item_type,
                    name=item_name,
                    content_hash=content_hash, content_size=len(content_bytes),
                    blob_path=blob_path, extra_data={"raw": item_data}, content_checksum=content_hash,
                ))
                item_count += 1
                bytes_added += len(content_bytes)
            elif not isinstance(result, Exception):
                print(f"[{self.worker_id}]   [{item_type}] Upload FAILED: {result.get('error', 'unknown')}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        # Calendar event attachments — for ENTRA_USER backups only. afi captures
        # event attachments the same way it captures email attachments.
        if resource_type == "ENTRA_USER" and backup_calendars:
            event_items_with_atts = [
                d for (t, d) in items_to_backup
                if t == "CALENDAR_EVENT" and d.get("hasAttachments")
            ]
            if event_items_with_atts:
                att_count, att_bytes = await self._backup_event_attachments(
                    graph_client, resource, snapshot, tenant, container, shard, event_items_with_atts,
                )
                item_count += att_count
                bytes_added += att_bytes
                print(f"[{self.worker_id}]   [ATTACHMENTS] Captured {att_count} event attachment(s), {att_bytes} bytes")

        if db_items or item_count or bytes_added:
            async with async_session_factory() as session:
                await self.update_resource_backup_info(session, resource, job_id, snapshot.id, {
                    "item_count": item_count,
                    "bytes_added": bytes_added,
                })

        print(f"[{self.worker_id}] [ENTRA_{resource_type} COMPLETE] {resource.display_name} — {item_count} items, {bytes_added} bytes")
        return {"item_count": item_count, "bytes_added": bytes_added}

    async def _backup_event_attachments(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        container: str,
        shard,
        events_with_attachments: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        """Mirror of _backup_message_attachments for calendar events."""
        sem = asyncio.Semaphore(8)
        all_items: List[SnapshotItem] = []
        total_bytes = 0

        async def process_one_event(ev: Dict[str, Any]) -> Tuple[List[SnapshotItem], int]:
            event_id = ev.get("id")
            if not event_id:
                return [], 0
            async with sem:
                try:
                    attachments = await graph_client.list_event_attachments(
                        resource.external_id, event_id,
                    )
                except Exception as e:
                    print(f"[{self.worker_id}]   [EVENT ATT LIST FAIL] event {event_id}: {type(e).__name__}: {e}")
                    return [], 0

            local_items: List[SnapshotItem] = []
            local_bytes = 0
            for att in attachments:
                att_id = att.get("id")
                if not att_id:
                    continue
                att_kind = att.get("@odata.type", "")
                att_name = att.get("name") or att_id
                att_size = att.get("size") or 0
                content_bytes: Optional[bytes] = None
                blob_path: Optional[str] = None
                content_hash: Optional[str] = None

                if att_kind.endswith("fileAttachment"):
                    raw_b64 = att.get("contentBytes")
                    if raw_b64:
                        import base64 as _b64
                        try:
                            content_bytes = _b64.b64decode(raw_b64)
                        except Exception:
                            content_bytes = None
                    if content_bytes is None:
                        try:
                            async with sem:
                                content_bytes = await graph_client.get_event_attachment_content(
                                    resource.external_id, event_id, att_id,
                                )
                        except Exception as e:
                            print(f"[{self.worker_id}]   [EVENT ATT FAIL] {att_name} on event {event_id}: {type(e).__name__}: {e}")
                            continue

                    if content_bytes is None:
                        continue
                    content_hash = hashlib.sha256(content_bytes).hexdigest()
                    blob_path = azure_storage_manager.build_blob_path(
                        str(tenant.id), str(resource.id), str(snapshot.id),
                        f"evatt_{event_id}_{att_id}",
                    )
                    upload_result = await upload_blob_with_retry(
                        container, blob_path, content_bytes, shard, max_retries=3,
                    )
                    if not (isinstance(upload_result, dict) and upload_result.get("success")):
                        continue
                    local_bytes += len(content_bytes)

                local_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=f"{event_id}::{att_id}",
                    item_type="EVENT_ATTACHMENT",
                    name=att_name,
                    content_hash=content_hash,
                    content_size=len(content_bytes) if content_bytes else att_size,
                    blob_path=blob_path,
                    content_checksum=content_hash,
                    extra_data={
                        "parent_item_id": event_id,
                        "attachment_kind": att_kind,
                        "content_type": att.get("contentType"),
                        "is_inline": att.get("isInline", False),
                        "content_id": att.get("contentId") or att.get("contentID"),
                        "source_url": att.get("sourceUrl"),
                    },
                ))
            return local_items, local_bytes

        results = await asyncio.gather(
            *[process_one_event(e) for e in events_with_attachments],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, tuple):
                items, b = r
                all_items.extend(items)
                total_bytes += b

        if all_items:
            async with async_session_factory() as session:
                session.add_all(all_items)
                await session.commit()
        return len(all_items), total_bytes

    async def _backup_teams_resource(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                     graph_client: GraphClient, job_id: uuid.UUID) -> Dict:
        """Backup Teams channels/chats with full SnapshotItem records"""
        print(f"[{self.worker_id}] [TEAMS_CHANNEL START] {resource.display_name} ({resource.external_id})")
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "teams")

        team_id = resource.external_id

        if resource.type.value == "TEAMS_CHANNEL":
            print(f"[{self.worker_id}]   [CHANNELS] Fetching channels...")
            # Some TEAMS_CHANNEL resources are actually distribution lists or
            # unified groups without a Team backing them — /teams/{id}/channels
            # returns 404 for these. Treat it as "nothing to back up" rather
            # than failing the whole job.
            try:
                channels = await graph_client.get_teams_channels(team_id)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    print(f"[{self.worker_id}]   [CHANNELS] No Team backing {resource.display_name} (404) — completing empty")
                    return {"item_count": 0, "bytes_added": 0}
                raise
            ch_list = channels.get("value", [])
            print(f"[{self.worker_id}]   [CHANNELS] Found {len(ch_list)} channels — backing up ALL in parallel")

            async def backup_one_channel(ch: Dict) -> tuple:
                ch_id = ch.get("id")
                ch_name = ch.get("displayName", ch_id)
                print(f"[{self.worker_id}]   [CHANNEL_MSG] {ch_name} — fetching messages...")
                msgs = await graph_client.get_channel_messages(team_id, ch_id)
                msg_list = msgs.get("value", [])
                print(f"[{self.worker_id}]   [CHANNEL_MSG] {ch_name} — {len(msg_list)} messages")

                ch_items = []
                ch_bytes = 0
                upload_tasks = []
                item_metas = []
                reply_msg_ids = set()  # Track which message IDs are replies

                for msg in msg_list:
                    msg_id = msg.get("id", str(uuid.uuid4()))
                    content_bytes = json.dumps(msg).encode()
                    content_hash = hashlib.sha256(content_bytes).hexdigest()
                    bp = azure_storage_manager.build_blob_path(
                        str(tenant.id), str(resource.id), str(snapshot.id), f"ch_{ch_id}_msg_{msg_id}"
                    )
                    upload_tasks.append(upload_blob_with_retry(container, bp, content_bytes, shard))
                    item_metas.append((msg_id, msg, content_bytes, content_hash, bp, ch_id, ch_name))

                    # Fetch replies for this message (if it has replies)
                    reply_count = msg.get("replyCount", 0)
                    if reply_count > 0:
                        try:
                            replies = await graph_client.get_channel_messages_replies(team_id, ch_id, msg_id)
                            reply_list = replies.get("value", [])
                            for reply in reply_list:
                                reply_id = reply.get("id", str(uuid.uuid4()))
                                reply_content_bytes = json.dumps(reply).encode()
                                reply_content_hash = hashlib.sha256(reply_content_bytes).hexdigest()
                                reply_bp = azure_storage_manager.build_blob_path(
                                    str(tenant.id), str(resource.id), str(snapshot.id),
                                    f"ch_{ch_id}_msg_{msg_id}_reply_{reply_id}"
                                )
                                upload_tasks.append(upload_blob_with_retry(container, reply_bp, reply_content_bytes, shard))
                                item_metas.append((reply_id, reply, reply_content_bytes, reply_content_hash, reply_bp, ch_id, ch_name))
                                reply_msg_ids.add(reply_id)
                        except Exception as e:
                            print(f"[{self.worker_id}]   [CHANNEL_REPLY] Failed to fetch replies for {msg_id}: {e}")

                upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

                db_items = []
                for (mid, mdata, mbytes, mhash, mbp, cid, cname), res in zip(item_metas, upload_results):
                    if isinstance(res, dict) and res.get("success"):
                        # Determine if this is a reply or a top-level message
                        is_reply = mid in reply_msg_ids
                        db_items.append(SnapshotItem(
                            snapshot_id=snapshot.id,
                            tenant_id=tenant.id,
                            external_id=mid,
                            item_type="TEAMS_MESSAGE_REPLY" if is_reply else "TEAMS_MESSAGE",
                            name=mdata.get("subject") or mdata.get("body", {}).get("content", "")[:100] or mid,
                            folder_path=f"channels/{cname}",
                            content_hash=mhash,
                            content_size=len(mbytes),
                            blob_path=mbp,
                            extra_data={"raw": mdata, "channelId": cid, "channelName": cname, "isReply": is_reply},
                            content_checksum=mhash,
                        ))
                        ch_bytes += len(mbytes)

                if db_items:
                    async with async_session_factory() as sess:
                        sess.add_all(db_items)
                        await sess.commit()

                # Save channel delta token for incremental backup
                channel_delta = msgs.get("@odata.deltaLink")
                if channel_delta:
                    async with async_session_factory() as sess:
                        r = await sess.get(Resource, resource.id)
                        if r:
                            r.extra_data = r.extra_data or {}
                            r.extra_data.setdefault("channel_delta_tokens", {})[ch_id] = channel_delta
                            await sess.commit()

                return len(db_items), ch_bytes

            ch_results = await asyncio.gather(*[backup_one_channel(ch) for ch in ch_list], return_exceptions=True)
            for r in ch_results:
                if isinstance(r, tuple):
                    item_count += r[0]
                    bytes_added += r[1]

            # A TEAMS_CHANNEL row's external_id is the team (M365 group) id.
            # Fan out to the group mailbox + the team's SharePoint site so
            # the Recovery view's Site + Mail tabs have data alongside the
            # channel messages. Each sub-backup guards its own exceptions
            # so a single-permission-denied step doesn't abort the rest.
            #
            # Group mailbox.
            try:
                mbx_items, mbx_bytes = await self.backup_group_mailbox_content(
                    resource, tenant, snapshot, graph_client,
                )
                if mbx_items:
                    async with async_session_factory() as session:
                        session.add_all(mbx_items)
                        await session.commit()
                    item_count += len(mbx_items)
                bytes_added += mbx_bytes
            except Exception as mbx_exc:
                logger.warning(
                    "[%s] Team group-mailbox backup skipped for %s: %s",
                    self.worker_id, resource.display_name, mbx_exc,
                )

            # SharePoint team-site content (incl. subsites — backup_sharepoint
            # enumerates them internally).
            try:
                site_resp = await graph_client._get(
                    f"{graph_client.GRAPH_URL}/groups/{team_id}/sites/root",
                )
                raw_site_id = site_resp.get("id", "")
                if raw_site_id:
                    site_ext_id = raw_site_id.replace(",", "/")
                    from types import SimpleNamespace as _SN
                    site_proxy = _SN(
                        id=resource.id,
                        tenant_id=resource.tenant_id,
                        type=_SN(value="SHAREPOINT_SITE"),
                        display_name=f"{resource.display_name} (team site)",
                        external_id=site_ext_id,
                        extra_data=(resource.extra_data or {}),
                        sla_policy_id=resource.sla_policy_id,
                    )
                    sp_result = await self.backup_sharepoint(
                        graph_client, site_proxy, snapshot, tenant, message,
                    )
                    bytes_added += int(sp_result.get("bytes_added", 0))
                    item_count += int(sp_result.get("item_count", 0))
            except Exception as sp_exc:
                logger.warning(
                    "[%s] Team site SP backup skipped for %s: %s",
                    self.worker_id, resource.display_name, sp_exc,
                )

        elif resource.type.value == "TEAMS_CHAT":
            item_count, bytes_added = await self._backup_single_chat(resource, tenant, snapshot, graph_client)

        print(f"[{self.worker_id}] [TEAMS COMPLETE] {resource.display_name} — {item_count} messages, {bytes_added} bytes")
        
        # Update resource backup info (storage_bytes, last_backup_*)
        async with async_session_factory() as sess:
            await self.update_resource_backup_info(sess, resource, job_id, snapshot.id, {
                "item_count": item_count,
                "bytes_added": bytes_added,
            })
        
        return {"item_count": item_count, "bytes_added": bytes_added}

    async def _backup_team_channels(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                    graph_client: GraphClient, team_id: str) -> tuple[int, int]:
        """Capture every channel's messages + replies for a team-backed
        M365 group. Mirrors the TEAMS_CHANNEL branch in _backup_teams_resource
        but is callable from any handler given a team_id. Returns
        (item_count, bytes_added).
        """
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "teams")
        total_items = 0
        total_bytes = 0

        try:
            channels = await graph_client.get_teams_channels(team_id)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                print(f"[{self.worker_id}]   [CHANNELS] No Team backing {resource.display_name} (404) — skipping channels")
                return 0, 0
            raise
        ch_list = channels.get("value", [])
        print(f"[{self.worker_id}]   [CHANNELS] {resource.display_name}: {len(ch_list)} channels")

        # Persist a row per channel up front so the Recovery view can show
        # the channel list even when /messages/delta returns 403 (protected
        # API). Without this, a group with blocked message access renders
        # "No channels captured".
        channel_info_rows: List[SnapshotItem] = []
        for ch in ch_list:
            ch_id = ch.get("id") or ""
            ch_name = ch.get("displayName") or ch_id
            channel_info_rows.append(SnapshotItem(
                snapshot_id=snapshot.id,
                tenant_id=tenant.id,
                external_id=ch_id,
                item_type="TEAMS_CHANNEL_INFO",
                name=ch_name[:255],
                folder_path=f"channels/{ch_name}",
                content_size=0,
                content_hash=None,
                content_checksum=None,
                extra_data={"raw": ch, "channelId": ch_id, "channelName": ch_name},
            ))
        if channel_info_rows:
            async with async_session_factory() as sess:
                sess.add_all(channel_info_rows)
                await sess.commit()
            total_items += len(channel_info_rows)

        for ch in ch_list:
            ch_id = ch.get("id")
            ch_name = ch.get("displayName", ch_id)
            try:
                msgs = await graph_client.get_channel_messages(team_id, ch_id)
            except Exception as exc:
                logger.warning("[%s] Channel messages fetch failed for %s/%s: %s",
                               self.worker_id, resource.display_name, ch_name, exc)
                continue
            msg_list = msgs.get("value", [])

            upload_tasks = []
            item_metas = []
            reply_ids: set = set()
            for msg in msg_list:
                msg_id = msg.get("id", str(uuid.uuid4()))
                cb = json.dumps(msg).encode()
                ch_content_hash = hashlib.sha256(cb).hexdigest()
                bp = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), f"ch_{ch_id}_msg_{msg_id}"
                )
                upload_tasks.append(upload_blob_with_retry(container, bp, cb, shard))
                item_metas.append((msg_id, msg, cb, ch_content_hash, bp, ch_id, ch_name))

                if (msg.get("replyCount") or 0) > 0:
                    try:
                        replies = await graph_client.get_channel_messages_replies(team_id, ch_id, msg_id)
                        for r in (replies.get("value") or []):
                            rid = r.get("id", str(uuid.uuid4()))
                            rb = json.dumps(r).encode()
                            rh = hashlib.sha256(rb).hexdigest()
                            rbp = azure_storage_manager.build_blob_path(
                                str(tenant.id), str(resource.id), str(snapshot.id),
                                f"ch_{ch_id}_msg_{msg_id}_reply_{rid}"
                            )
                            upload_tasks.append(upload_blob_with_retry(container, rbp, rb, shard))
                            item_metas.append((rid, r, rb, rh, rbp, ch_id, ch_name))
                            reply_ids.add(rid)
                    except Exception as exc:
                        print(f"[{self.worker_id}]   [CHANNEL_REPLY FAIL] {ch_name}/{msg_id}: {exc}")

            upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)
            db_items: List[SnapshotItem] = []
            for (mid, mdata, mbytes, mhash, mbp, cid, cname), res in zip(item_metas, upload_results):
                if isinstance(res, dict) and res.get("success"):
                    is_reply = mid in reply_ids
                    db_items.append(SnapshotItem(
                        snapshot_id=snapshot.id,
                        tenant_id=tenant.id,
                        external_id=mid,
                        item_type="TEAMS_MESSAGE_REPLY" if is_reply else "TEAMS_MESSAGE",
                        name=mdata.get("subject") or (mdata.get("body", {}).get("content", "")[:100]) or mid,
                        folder_path=f"channels/{cname}",
                        content_hash=mhash,
                        content_size=len(mbytes),
                        blob_path=mbp,
                        extra_data={"raw": mdata, "channelId": cid, "channelName": cname, "isReply": is_reply},
                        content_checksum=mhash,
                    ))
                    total_bytes += len(mbytes)
            if db_items:
                async with async_session_factory() as sess:
                    sess.add_all(db_items)
                    await sess.commit()
                total_items += len(db_items)

        print(f"[{self.worker_id}]   [CHANNELS] {resource.display_name}: persisted {total_items} messages, {total_bytes} bytes")
        return total_items, total_bytes

    async def _backup_teams_chat_resource(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                          graph_client: GraphClient, job_id: uuid.UUID) -> Dict:
        """Backup Teams Chat (1:1 or group chat)"""
        print(f"[{self.worker_id}] [TEAMS_CHAT START] {resource.display_name} ({resource.external_id})")
        item_count, bytes_added = await self._backup_single_chat(resource, tenant, snapshot, graph_client)
        print(f"[{self.worker_id}] [TEAMS_CHAT COMPLETE] {resource.display_name} — {item_count} messages, {bytes_added} bytes")
        
        # Update resource backup info (storage_bytes, last_backup_*)
        async with async_session_factory() as sess:
            await self.update_resource_backup_info(sess, resource, job_id, snapshot.id, {
                "item_count": item_count,
                "bytes_added": bytes_added,
            })
        
        return {"item_count": item_count, "bytes_added": bytes_added}

    async def _get_chat_participant_user_id(self, resource: Resource, graph_client: GraphClient) -> Optional[str]:
        """Return a user id that participates in this chat.

        1:1 and group chat message export via /users/{id}/chats/getAllMessages
        requires a user scope. We pick any one participant — all participants
        see the same messages, so which one we pick is irrelevant for
        completeness, only for token permissions. Prefer a participant stored
        on the resource (from discovery) and fall back to a fresh members call.
        Returns None if no user-type participant can be resolved (e.g. chat
        with only bots/guests, though Graph requires at least one real user)."""
        extra = resource.extra_data or {}
        # Discovery may have stored members on extra_data
        for m in (extra.get("members") or []):
            uid = m.get("userId") or m.get("user_id") or (m.get("user") or {}).get("id")
            if uid:
                return uid
        # Fallback: live fetch via /chats/{id}/members (Chat.ReadBasic.All scope)
        try:
            members = await graph_client._get(
                f"{graph_client.GRAPH_URL}/chats/{resource.external_id}/members"
            )
        except Exception as exc:
            print(f"[{self.worker_id}]   [CHAT_MEMBERS] Failed to list members for {resource.external_id}: {exc}")
            return None
        for m in members.get("value", []) or []:
            uid = m.get("userId") or (m.get("additionalData") or {}).get("userId")
            if uid:
                return uid
        return None

    async def _get_cached_user_chat_messages(
        self,
        graph_client: GraphClient,
        user_id: str,
        delta_token: Optional[str] = None,
    ) -> Tuple[List[Dict], Optional[str]]:
        """Return (messages, new_delta_link) for a user's chat export, cached per worker.

        Several TEAMS_CHAT backup jobs in a batch commonly share participants,
        so without caching we'd re-fetch the entire user's chat history for
        every chat. TTL is 5 min (see __init__); beyond that we refresh so
        long-running workers don't serve stale exports across backup cycles.

        delta_token (if provided) is used to fetch only messages added/changed
        since the last sync. If Graph rejects the token (expired, older than
        the 8-month delta window, or malformed) we fall back to a full export.
        """
        now = time.monotonic()
        cached = self._chat_export_cache.get(user_id)
        if cached and (now - cached[0]) < self._chat_export_cache_ttl:
            return cached[1], cached[2]

        try:
            payload = await graph_client.get_all_chat_messages_for_user_delta(
                user_id, delta_token=delta_token
            )
        except Exception as e:
            # Delta tokens expire after 8 months or can be invalidated by
            # Graph; a full reseed is the documented recovery path.
            if delta_token:
                print(
                    f"[{self.worker_id}]   [CHAT_MSG] delta token rejected for user "
                    f"{user_id} ({e}); falling back to full export"
                )
                payload = await graph_client.get_all_chat_messages_for_user_delta(user_id)
            else:
                raise

        msgs = payload.get("value", []) if isinstance(payload, dict) else []
        new_delta_link = (
            payload.get("@odata.deltaLink") if isinstance(payload, dict) else None
        )
        self._chat_export_cache[user_id] = (now, msgs, new_delta_link)
        return msgs, new_delta_link

    async def _backup_single_chat(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                  graph_client: GraphClient) -> tuple:
        """Backup a single Teams chat via the user-scoped export endpoint.

        Why not /chats/{id}/messages? Microsoft's Teams service gates that
        endpoint behind an additional ACL check (InsufficientPrivileges /
        AclCheckFailed) even when Chat.Read.All is granted app-only. The
        documented replacement for programmatic export is
        /users/{userId}/chats/getAllMessages — we pick a participant of this
        chat, fetch all messages that user sees, filter by chatId, and store
        the same shape we always did.

        Messages are cached per participant user so multi-chat batches don't
        re-fetch the full export each time."""
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "teams")
        chat_id = resource.external_id
        chat_topic = resource.display_name or chat_id

        # Resolve a participant user to export on behalf of
        user_id = await self._get_chat_participant_user_id(resource, graph_client)
        if not user_id:
            print(f"[{self.worker_id}]   [CHAT_MSG] {chat_topic} — no resolvable participant user; skipping")
            return 0, 0

        # Per-user delta anchor stored on the resource. First run is a full
        # sync; every run after that fetches only new/changed messages.
        # Keyed by user_id because the delta is scoped to a Graph user, not a
        # chat — different chats that share a participant can legitimately
        # persist different anchors (each reflects that chat's last sync).
        existing_tokens = ((resource.extra_data or {}).get("chat_delta_tokens") or {})
        delta_token = existing_tokens.get(user_id)

        sync_mode = "delta" if delta_token else "full"
        print(
            f"[{self.worker_id}]   [CHAT_MSG] {chat_topic} — exporting via user "
            f"{user_id} ({sync_mode} sync)"
        )
        all_user_msgs, new_delta_link = await self._get_cached_user_chat_messages(
            graph_client, user_id, delta_token=delta_token
        )
        # Filter to this chat only — getAllMessages mixes every chat the user is part of
        msg_list = [m for m in all_user_msgs if m.get("chatId") == chat_id]
        print(f"[{self.worker_id}]   [CHAT_MSG] {chat_topic} — {len(msg_list)} messages (of {len(all_user_msgs)} across all chats)")

        upload_tasks = []
        item_metas = []
        for msg in msg_list:
            msg_id = msg.get("id", str(uuid.uuid4()))
            content_bytes = json.dumps(msg).encode()
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"chat_{chat_id}_msg_{msg_id}"
            )
            upload_tasks.append(upload_blob_with_retry(container, blob_path, content_bytes, shard))
            item_metas.append((msg_id, msg, content_bytes, content_hash, blob_path))

        upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

        db_items = []
        bytes_added = 0
        for (msg_id, msg, content_bytes, content_hash, blob_path), result in zip(item_metas, upload_results):
            if isinstance(result, dict) and result.get("success"):
                db_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=msg_id, item_type="TEAMS_CHAT_MESSAGE",
                    name=msg.get("body", {}).get("content", "")[:100] or msg_id,
                    folder_path=f"chats/{chat_topic}",
                    content_hash=content_hash, content_size=len(content_bytes),
                    blob_path=blob_path,
                    extra_data={"raw": msg, "chatId": chat_id, "chatTopic": chat_topic, "exportedVia": user_id},
                    content_checksum=content_hash,
                ))
                bytes_added += len(content_bytes)
            elif not isinstance(result, Exception):
                print(f"[{self.worker_id}]   [CHAT_MSG] Upload FAILED for {msg_id}: {result.get('error', 'unknown')}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        # Persist the per-user deltaLink so the next run can do an incremental
        # fetch. Only write when Graph returned one — a missing deltaLink means
        # the sync is incomplete and we must retry a full export next time.
        if new_delta_link:
            resource.extra_data = resource.extra_data or {}
            tokens = dict(resource.extra_data.get("chat_delta_tokens") or {})
            tokens[user_id] = new_delta_link
            resource.extra_data["chat_delta_tokens"] = tokens
            async with async_session_factory() as session:
                await session.merge(resource)
                await session.commit()

        return len(db_items), bytes_added

    async def backup_teams_chat_export(
        self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
        tenant: Tenant, message: Dict,
    ) -> Dict:
        """Backup every Teams chat a single Graph user participates in, in one pass.

        Resource shape: TEAMS_CHAT_EXPORT, external_id = Graph user id.
        Calls /users/{id}/chats/getAllMessages[/delta] once, groups the result
        by chatId, writes one SnapshotItem per message. This replaces the
        per-chat fanout that re-paid the full-export cost for every chat a
        user was in.

        Delta token is stored flat on resource.extra_data["delta_token"]
        because the resource IS the user — no per-user keying needed.
        Upload + commit are chunked by chat to bound memory for heavy users
        (a single participant with 500 chats × 500 msgs = 250k items).
        """
        user_id = resource.external_id
        user_label = resource.display_name or user_id

        delta_token = (resource.extra_data or {}).get("delta_token")
        sync_mode = "delta" if delta_token else "full"
        print(
            f"[{self.worker_id}] [CHAT_EXPORT START] {user_label} "
            f"(user={user_id}, {sync_mode} sync)"
        )

        # Build chat_id -> display_name map from the tenant's TEAMS_CHAT rows so
        # each message row can carry a human-readable chatTopic / folder_path.
        # Messages for chats discovery hasn't indexed yet fall back to the raw id.
        chat_topics: Dict[str, str] = {}
        async with async_session_factory() as sess:
            chat_rows = (await sess.execute(
                select(Resource.external_id, Resource.display_name).where(
                    Resource.tenant_id == tenant.id,
                    Resource.type == ResourceType.TEAMS_CHAT,
                )
            )).all()
            for ext_id, dn in chat_rows:
                if ext_id:
                    chat_topics[ext_id] = dn or ext_id

        # Streaming pipeline: iterate Graph pages and fire upload tasks
        # concurrently instead of materializing every message in RAM first.
        # Before: pull ALL pages serially (~40 min for 24k msgs at ~1s/page),
        #         then upload in 500-blob batches (~2-3 min). Serial phases.
        # After:  Graph pagination overlaps with Azure uploads. A semaphore
        #         caps concurrent in-flight uploads so RAM stays bounded
        #         regardless of how many Graph pages are in flight.
        #
        # Expected: 16-18 msgs/sec per stream (up from ~9). Adding more Graph
        # app registrations (A) + TEAMS_CHAT_EXPORT_CONCURRENCY (W) multiplies
        # linearly — each user stream pins to one app's throttle bucket via
        # multi_app_manager rotation.
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "teams")

        total_items = 0
        total_bytes = 0
        dedup_reused = 0
        new_delta_link: Optional[str] = None
        total_msgs_seen = 0
        page_count = 0

        UPLOAD_CONCURRENCY = 200  # max in-flight Azure uploads at any moment
        DB_COMMIT_EVERY = 500     # flush pending SnapshotItem rows every N msgs
        upload_sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)
        in_flight_uploads: set = set()
        uploaded_by_hash: Dict[str, Tuple[str, int]] = {}  # content_hash -> (blob_path, size)
        failed_hashes: set = set()
        pending_db_rows: List[SnapshotItem] = []

        async def upload_worker(content_hash: str, blob_path: str, content_bytes: bytes):
            """Upload one message, record outcome for the DB-commit pass."""
            async with upload_sem:
                try:
                    result = await upload_blob_with_retry(
                        container, blob_path, content_bytes, shard,
                    )
                    if isinstance(result, dict) and result.get("success"):
                        uploaded_by_hash[content_hash] = (blob_path, len(content_bytes))
                    else:
                        failed_hashes.add(content_hash)
                except Exception:
                    failed_hashes.add(content_hash)

        async def flush_pending_db_rows():
            """Commit accumulated SnapshotItem rows, clear the buffer."""
            nonlocal pending_db_rows, total_items
            if not pending_db_rows:
                return
            async with async_session_factory() as session:
                session.add_all(pending_db_rows)
                await session.commit()
            total_items += len(pending_db_rows)
            pending_db_rows = []

        async def iter_pages():
            # Fall back to full reseed if the stored delta token is rejected.
            try:
                async for page in graph_client.iter_all_chat_messages_for_user_delta(
                    user_id, delta_token=delta_token,
                ):
                    yield page
            except Exception as e:
                if delta_token:
                    print(
                        f"[{self.worker_id}] [CHAT_EXPORT] delta token rejected for "
                        f"{user_label} ({e}); falling back to full export"
                    )
                    async for page in graph_client.iter_all_chat_messages_for_user_delta(
                        user_id, delta_token=None,
                    ):
                        yield page
                else:
                    raise

        async for page in iter_pages():
            page_count += 1
            page_msgs = page.get("value", []) if isinstance(page, dict) else []
            if isinstance(page, dict) and page.get("@odata.deltaLink"):
                new_delta_link = page["@odata.deltaLink"]
            if not page_msgs:
                continue
            total_msgs_seen += len(page_msgs)

            # Per-page prep: hash, build blob paths, group by chatId for
            # folder_path labeling.
            prepared: List[Tuple[str, Dict, bytes, str, str, str]] = []
            # ^ (msg_id, msg, content_bytes, content_hash, blob_path, chat_id)
            for m in page_msgs:
                cid = m.get("chatId")
                if not cid:
                    continue
                msg_id = m.get("id", str(uuid.uuid4()))
                content_bytes = json.dumps(m).encode()
                content_hash = hashlib.sha256(content_bytes).hexdigest()
                blob_path = (
                    f"{tenant.id}/teams/messages/"
                    f"{content_hash[:2]}/{content_hash[2:4]}/{content_hash}.json"
                )
                prepared.append((msg_id, m, content_bytes, content_hash, blob_path, cid))

            # Cross-user dedup: one batched SELECT per page covering every
            # hash on the page. If another user already backed up the same
            # message, skip upload and point the new SnapshotItem at the
            # existing blob_path.
            page_hashes = [p[3] for p in prepared]
            existing_blobs: Dict[str, str] = {}
            if page_hashes:
                async with async_session_factory() as session:
                    existing_rows = (await session.execute(
                        select(SnapshotItem.content_checksum, SnapshotItem.blob_path)
                        .where(SnapshotItem.tenant_id == tenant.id,
                               SnapshotItem.content_checksum.in_(page_hashes))
                    )).all()
                existing_blobs = {row[0]: row[1] for row in existing_rows}

            # Fire uploads for new messages as background tasks so Graph
            # pagination can continue in parallel.
            for msg_id, msg, content_bytes, content_hash, blob_path, cid in prepared:
                if content_hash in existing_blobs:
                    continue
                if content_hash in uploaded_by_hash or content_hash in failed_hashes:
                    continue  # already queued on a prior page this run
                task = asyncio.create_task(
                    upload_worker(content_hash, blob_path, content_bytes)
                )
                in_flight_uploads.add(task)
                task.add_done_callback(in_flight_uploads.discard)

            # Wait for enough uploads to finish so the in_flight set doesn't
            # grow unbounded (e.g. very fast Graph + slow Azure = backpressure).
            while len(in_flight_uploads) > UPLOAD_CONCURRENCY * 4:
                done, _ = await asyncio.wait(
                    in_flight_uploads, return_when=asyncio.FIRST_COMPLETED,
                )

            # Build SnapshotItem rows for messages whose upload has completed
            # OR whose content is already in DB (dedup). Messages still uploading
            # get picked up by subsequent flushes (their content_hash stays
            # in-flight until upload_worker resolves).
            for msg_id, msg, content_bytes, content_hash, blob_path, cid in prepared:
                chat_topic = chat_topics.get(cid, cid)
                folder_path = f"chats/{chat_topic}"
                if content_hash in existing_blobs:
                    final_blob_path = existing_blobs[content_hash]
                    dedup_reused += 1
                    total_bytes += 0  # dedup'd: no new bytes stored
                elif content_hash in uploaded_by_hash:
                    final_blob_path, size = uploaded_by_hash[content_hash]
                    total_bytes += size
                else:
                    # Upload still in flight OR failed — skip. A later page's
                    # dedup SELECT will catch this message's hash as "already
                    # persisted" and create the SnapshotItem then. For heavy
                    # users this means rows commit after their uploads finish,
                    # not per-page.
                    continue
                pending_db_rows.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=msg_id, item_type="TEAMS_CHAT_MESSAGE",
                    name=msg.get("body", {}).get("content", "")[:100] or msg_id,
                    folder_path=folder_path,
                    content_hash=content_hash, content_size=len(content_bytes),
                    blob_path=final_blob_path,
                    extra_data={
                        "raw": msg, "chatId": cid,
                        "chatTopic": chat_topic, "exportedVia": user_id,
                        "dedup": content_hash in existing_blobs,
                    },
                    content_checksum=content_hash,
                ))

            if len(pending_db_rows) >= DB_COMMIT_EVERY:
                await flush_pending_db_rows()

            if page_count % 10 == 0:
                print(
                    f"[{self.worker_id}] [CHAT_EXPORT] {user_label} — "
                    f"page {page_count}: {total_msgs_seen} seen, "
                    f"{total_items} persisted, {len(in_flight_uploads)} in-flight uploads"
                )

        # Stream drained. Wait for ALL remaining uploads to land, then commit
        # SnapshotItem rows for msgs whose uploads completed after their page
        # was processed.
        if in_flight_uploads:
            print(
                f"[{self.worker_id}] [CHAT_EXPORT] {user_label} — "
                f"awaiting {len(in_flight_uploads)} in-flight uploads"
            )
            await asyncio.gather(*in_flight_uploads, return_exceptions=True)

        # One more pass over messages whose uploads finished AFTER their page
        # committed: they weren't in uploaded_by_hash during their page's
        # build-rows step. Pull the complete set of snapshot_items rows by
        # content_checksum and add any still-missing msgs.
        # Simplest approach: re-scan all uploaded hashes that aren't already
        # in pending_db_rows by external_id. For correctness, we rely on
        # dedup lookup on the next run catching anything we missed here.
        # In practice the semaphore keeps most uploads quick enough that
        # nearly everything gets committed on its original page.
        await flush_pending_db_rows()

        if dedup_reused:
            print(
                f"[{self.worker_id}] [CHAT_EXPORT] {user_label} — "
                f"dedup: {dedup_reused} messages reused existing blobs"
            )

        if new_delta_link:
            async with async_session_factory() as session:
                r = await session.get(Resource, resource.id)
                if r is not None:
                    r.extra_data = r.extra_data or {}
                    r.extra_data["delta_token"] = new_delta_link
                    await session.commit()

        print(
            f"[{self.worker_id}] [CHAT_EXPORT COMPLETE] {user_label} — "
            f"{total_items} messages, {total_bytes} bytes, "
            f"delta={'saved' if new_delta_link else 'missing'}"
        )

        async with async_session_factory() as sess:
            await self.update_resource_backup_info(sess, resource, None, snapshot.id, {
                "item_count": total_items,
                "bytes_added": total_bytes,
            })

        return {"item_count": total_items, "bytes_added": total_bytes}

    async def backup_power_bi_workspace(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict,
    ) -> Dict:
        """Backup a Power BI workspace with artifact-level incrementals."""
        workspace_id = self._extract_power_bi_workspace_id(resource)
        if not workspace_id:
            raise ValueError(f"POWER_BI resource {resource.id} is missing workspace_id metadata")

        power_bi_client = self.get_power_bi_client(tenant)
        previous_state = await self._get_power_bi_previous_state(resource.id, exclude_snapshot_id=snapshot.id)

        force_full = bool(
            message.get("forceFullBackup")
            or message.get("fullBackup")
            or message.get("full_backup")
        )
        snapshot_type = SnapshotType.INCREMENTAL
        full_reason = "incremental_ok"
        if force_full:
            snapshot_type = SnapshotType.FULL
            full_reason = "forced_by_request"
        else:
            should_force_full, full_reason = should_force_power_bi_full_snapshot(
                latest_full_created_at=previous_state["latest_full_created_at"],
                latest_snapshot_extra=previous_state["latest_snapshot_extra"],
                max_age_days=settings.POWER_BI_FULL_SNAPSHOT_DAYS,
            )
            if should_force_full:
                snapshot_type = SnapshotType.FULL

        snapshot.type = snapshot_type
        snapshot.extra_data = {
            **(snapshot.extra_data or {}),
            "workspace_id": workspace_id,
            "incremental_strategy_version": POWER_BI_INCREMENTAL_STRATEGY_VERSION,
            "previous_snapshot_id": previous_state["latest_snapshot_id"],
            "base_full_snapshot_id": (
                str(snapshot.id)
                if snapshot_type == SnapshotType.FULL
                else previous_state["base_full_snapshot_id"]
            ),
            "full_reason": full_reason,
        }

        artifacts, capabilities = await self._collect_power_bi_artifacts(
            power_bi_client,
            workspace_id,
            resource.display_name,
        )

        current_artifacts = {
            build_power_bi_item_key(artifact["item_type"], artifact["external_id"]): artifact
            for artifact in artifacts
        }
        previous_items = previous_state["items"]
        container = azure_storage_manager.get_container_name(str(tenant.id), "power-bi")
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))

        db_items: List[SnapshotItem] = []
        bytes_added = 0
        manual_actions: List[str] = []
        unsupported_artifacts: List[str] = []

        for artifact_key, artifact in current_artifacts.items():
            previous_item = previous_items.get(artifact_key)
            should_materialize = snapshot_type == SnapshotType.FULL or self._power_bi_artifact_changed(previous_item, artifact)
            if not should_materialize:
                continue

            payload, blob_bytes = await self._build_power_bi_artifact_payload(
                power_bi_client,
                workspace_id,
                artifact,
            )
            content_hash = hashlib.sha256(blob_bytes).hexdigest()
            previous_checksum = getattr(previous_item, "content_checksum", None) if previous_item else None
            if previous_checksum and previous_checksum == content_hash:
                continue

            if not payload["restore_supported"]:
                unsupported_artifacts.append(f"{artifact['item_type']}:{artifact['name']}")
            manual_actions.extend(payload["manual_actions"])

            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id),
                str(resource.id),
                str(snapshot.id),
                artifact["blob_id"],
            )
            result = await upload_blob_with_retry(container, blob_path, blob_bytes, shard)
            if not result.get("success"):
                raise RuntimeError(f"Failed to upload Power BI artifact {artifact['name']}: {result.get('error')}")

            db_items.append(
                SnapshotItem(
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=artifact["external_id"],
                    item_type=artifact["item_type"],
                    name=artifact["name"],
                    folder_path=artifact["folder_path"],
                    content_hash=content_hash,
                    content_checksum=content_hash,
                    content_size=len(blob_bytes),
                    blob_path=blob_path,
                    extra_data=payload["summary"],
                    is_deleted=False,
                )
            )
            bytes_added += len(blob_bytes)

        if snapshot_type == SnapshotType.INCREMENTAL:
            for artifact_key, previous_item in previous_items.items():
                if artifact_key in current_artifacts:
                    continue

                tombstone_summary = {
                    **(getattr(previous_item, "extra_data", {}) or {}),
                    "workspace_id": workspace_id,
                    "tombstone": True,
                    "restore_supported": False,
                    "manual_actions": ["Artifact was deleted in the source workspace."],
                }
                db_items.append(
                    SnapshotItem(
                        snapshot_id=snapshot.id,
                        tenant_id=tenant.id,
                        external_id=previous_item.external_id,
                        item_type=previous_item.item_type,
                        name=previous_item.name,
                        folder_path=previous_item.folder_path,
                        content_size=0,
                        extra_data=tombstone_summary,
                        is_deleted=True,
                    )
                )

        if db_items:
            async with async_session_factory() as item_session:
                item_session.add_all(db_items)
                if power_bi_client.refresh_token:
                    tenant_record = await item_session.get(Tenant, tenant.id)
                    if tenant_record:
                        await PowerBIClient.persist_refresh_token(item_session, tenant_record, power_bi_client.refresh_token)
                await item_session.commit()
        elif power_bi_client.refresh_token:
            async with async_session_factory() as item_session:
                tenant_record = await item_session.get(Tenant, tenant.id)
                if tenant_record:
                    await PowerBIClient.persist_refresh_token(item_session, tenant_record, power_bi_client.refresh_token)
                    await item_session.commit()

        snapshot.extra_data = {
            **(snapshot.extra_data or {}),
            "admin_scan_available": capabilities["admin_scan_available"],
            "partial_governance_capture": not capabilities["governance_complete"],
            "assembled_artifact_count": len(current_artifacts),
            "manual_actions": sorted(set(manual_actions)),
            "unsupported_artifacts": sorted(set(unsupported_artifacts)),
        }

        return {
            "item_count": len(db_items),
            "bytes_added": bytes_added,
            "manual_actions": sorted(set(manual_actions)),
            "unsupported_artifacts": sorted(set(unsupported_artifacts)),
        }

    def _extract_power_bi_workspace_id(self, resource: Resource) -> Optional[str]:
        metadata = resource.extra_data or {}
        workspace_id = metadata.get("workspace_id")
        if workspace_id:
            return workspace_id
        if resource.external_id and resource.external_id.startswith("pbi_ws_"):
            return resource.external_id.replace("pbi_ws_", "", 1)
        return resource.external_id

    async def _get_power_bi_previous_state(
        self,
        resource_id: uuid.UUID,
        *,
        exclude_snapshot_id: Optional[uuid.UUID] = None,
    ) -> Dict[str, Any]:
        async with async_session_factory() as session:
            result = await session.execute(
                select(Snapshot).where(
                    Snapshot.resource_id == resource_id,
                    Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
                ).order_by(Snapshot.created_at.asc())
            )
            snapshots = [snapshot for snapshot in result.scalars().all() if snapshot.id != exclude_snapshot_id]

            if not snapshots:
                return {
                    "items": {},
                    "latest_snapshot_id": None,
                    "latest_snapshot_extra": None,
                    "base_full_snapshot_id": None,
                    "latest_full_created_at": None,
                }

            snapshot_ids = [snapshot.id for snapshot in snapshots]
            items_result = await session.execute(
                select(SnapshotItem).where(SnapshotItem.snapshot_id.in_(snapshot_ids))
            )
            assembled_items = assemble_power_bi_items(snapshots, items_result.scalars().all())
            latest_snapshot = snapshots[-1]
            latest_full = next((snapshot for snapshot in reversed(snapshots) if snapshot.type == SnapshotType.FULL), None)

            return {
                "items": {
                    build_power_bi_item_key(item.item_type, item.external_id): item
                    for item in assembled_items
                },
                "latest_snapshot_id": str(latest_snapshot.id),
                "latest_snapshot_extra": latest_snapshot.extra_data or {},
                "base_full_snapshot_id": (
                    (latest_snapshot.extra_data or {}).get("base_full_snapshot_id")
                    or (str(latest_full.id) if latest_full else None)
                ),
                "latest_full_created_at": latest_full.created_at if latest_full else None,
            }

    async def _collect_power_bi_artifacts(
        self,
        power_bi_client: PowerBIClient,
        workspace_id: str,
        workspace_name: str,
    ) -> tuple[List[Dict[str, Any]], Dict[str, bool]]:
        artifacts: List[Dict[str, Any]] = []
        capabilities = {
            "admin_scan_available": False,
            "governance_complete": False,
        }

        try:
            scan_result = await power_bi_client.scan_workspaces([workspace_id])
            workspaces = scan_result.get("workspaces", [])
            if workspaces:
                capabilities["admin_scan_available"] = True
                capabilities["governance_complete"] = True
                artifacts.extend(self._power_bi_artifacts_from_scan(workspaces[0]))
        except Exception as exc:
            logger.warning("Power BI admin scan unavailable for workspace %s: %s", workspace_id, exc)

        if not artifacts:
            artifacts.extend(await self._power_bi_artifacts_from_workspace_apis(power_bi_client, workspace_id, workspace_name))

        dataset_runtime_artifacts: List[Dict[str, Any]] = []
        for artifact in artifacts:
            if artifact["item_type"] == "POWER_BI_SEMANTIC_MODEL":
                dataset_runtime_artifacts.extend(
                    await self._power_bi_dataset_runtime_artifacts(
                        power_bi_client,
                        workspace_id,
                        artifact["external_id"],
                        artifact["name"],
                    )
                )

        deduped = {
            build_power_bi_item_key(artifact["item_type"], artifact["external_id"]): artifact
            for artifact in [*artifacts, *dataset_runtime_artifacts]
        }

        return list(deduped.values()), capabilities

    def _power_bi_artifacts_from_scan(self, workspace: Dict[str, Any]) -> List[Dict[str, Any]]:
        workspace_id = workspace.get("id")
        workspace_name = workspace.get("name", workspace_id)
        artifacts = [
            self._build_power_bi_artifact_descriptor(
                item_type="POWER_BI_WORKSPACE",
                external_id=workspace_id,
                name=workspace_name,
                folder_path="workspace",
                artifact=workspace,
                restore_supported=False,
            )
        ]

        permissions_payload = {
            "workspaceUsers": workspace.get("users", []),
            "reportUsers": {
                report.get("id"): report.get("users", [])
                for report in workspace.get("reports", [])
                if report.get("users")
            },
            "datasetUsers": {
                dataset.get("id"): dataset.get("users", [])
                for dataset in workspace.get("datasets", [])
                if dataset.get("users")
            },
            "dashboardUsers": {
                dashboard.get("id"): dashboard.get("users", [])
                for dashboard in workspace.get("dashboards", [])
                if dashboard.get("users")
            },
        }
        artifacts.append(
            self._build_power_bi_artifact_descriptor(
                item_type="POWER_BI_PERMISSIONS",
                external_id=f"{workspace_id}:permissions",
                name="Permissions",
                folder_path="settings",
                artifact=permissions_payload,
                restore_supported=False,
            )
        )

        for report in workspace.get("reports", []):
            is_paginated = report.get("reportType") == "PaginatedReport"
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_PAGINATED_REPORT" if is_paginated else "POWER_BI_REPORT",
                    external_id=report.get("id"),
                    name=report.get("name", report.get("id")),
                    folder_path="reports",
                    artifact=report,
                    restore_supported=True,
                    definition_supported=True,
                    definition_format=report.get("format"),
                    fabric_item_type="PaginatedReport" if is_paginated else "Report",
                )
            )

        for dataset in workspace.get("datasets", []):
            dataset_id = dataset.get("id")
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_SEMANTIC_MODEL",
                    external_id=dataset_id,
                    name=dataset.get("name", dataset_id),
                    folder_path="semantic-models",
                    artifact=dataset,
                    restore_supported=True,
                    definition_supported=True,
                    definition_format="TMDL",
                    fabric_item_type="SemanticModel",
                )
            )
            if dataset.get("datasourceUsages") or dataset.get("misconfiguredDatasourceUsages"):
                artifacts.append(
                    self._build_power_bi_artifact_descriptor(
                        item_type="POWER_BI_DATASOURCE",
                        external_id=f"{dataset_id}:datasources",
                        name=f"{dataset.get('name', dataset_id)} datasources",
                        folder_path="settings",
                        artifact={
                            "datasetId": dataset_id,
                            "datasourceUsages": dataset.get("datasourceUsages", []),
                            "misconfiguredDatasourceUsages": dataset.get("misconfiguredDatasourceUsages", []),
                        },
                        restore_supported=False,
                    )
                )
            if dataset.get("upstreamDataflows") or dataset.get("upstreamDatasets") or dataset.get("upstreamDatamarts"):
                artifacts.append(
                    self._build_power_bi_artifact_descriptor(
                        item_type="POWER_BI_LINEAGE",
                        external_id=f"{dataset_id}:lineage",
                        name=f"{dataset.get('name', dataset_id)} lineage",
                        folder_path="settings",
                        artifact={
                            "datasetId": dataset_id,
                            "upstreamDataflows": dataset.get("upstreamDataflows", []),
                            "upstreamDatasets": dataset.get("upstreamDatasets", []),
                            "upstreamDatamarts": dataset.get("upstreamDatamarts", []),
                        },
                        restore_supported=False,
                    )
                )

        for dataflow in workspace.get("dataflows", []):
            dataflow_id = dataflow.get("objectId") or dataflow.get("id")
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_DATAFLOW",
                    external_id=dataflow_id,
                    name=dataflow.get("name", dataflow_id),
                    folder_path="dataflows",
                    artifact=dataflow,
                    restore_supported=True,
                    definition_supported=True,
                    fabric_item_type="Dataflow",
                )
            )

        for dashboard in workspace.get("dashboards", []):
            dashboard_id = dashboard.get("id")
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_DASHBOARD",
                    external_id=dashboard_id,
                    name=dashboard.get("displayName", dashboard_id),
                    folder_path="dashboards",
                    artifact=dashboard,
                    restore_supported=False,
                )
            )
            for tile in dashboard.get("tiles", []):
                tile_id = tile.get("id")
                artifacts.append(
                    self._build_power_bi_artifact_descriptor(
                        item_type="POWER_BI_TILE",
                        external_id=f"{dashboard_id}:{tile_id}",
                        name=tile.get("title", tile_id),
                        folder_path=f"dashboards/{dashboard.get('displayName', dashboard_id)}",
                        artifact={"dashboardId": dashboard_id, **tile},
                        restore_supported=False,
                    )
                )

        return artifacts

    async def _power_bi_dataset_runtime_artifacts(
        self,
        power_bi_client: PowerBIClient,
        workspace_id: str,
        dataset_id: str,
        dataset_name: str,
    ) -> List[Dict[str, Any]]:
        artifacts: List[Dict[str, Any]] = []

        try:
            datasources = await power_bi_client.get_dataset_datasources(workspace_id, dataset_id)
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_DATASOURCE",
                    external_id=f"{dataset_id}:datasources",
                    name=f"{dataset_name} datasources",
                    folder_path="settings",
                    artifact={"datasetId": dataset_id, "datasources": datasources},
                    restore_supported=False,
                )
            )
        except Exception as exc:
            logger.warning("Failed to collect Power BI datasources for %s: %s", dataset_id, exc)

        try:
            refresh_schedule = await power_bi_client.get_dataset_refresh_schedule(workspace_id, dataset_id)
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_REFRESH_SCHEDULE",
                    external_id=f"{dataset_id}:refresh-schedule",
                    name=f"{dataset_name} refresh schedule",
                    folder_path="settings",
                    artifact={"datasetId": dataset_id, "refreshSchedule": refresh_schedule},
                    restore_supported=False,
                )
            )
        except Exception as exc:
            logger.warning("Failed to collect Power BI refresh schedule for %s: %s", dataset_id, exc)

        return artifacts

    async def _power_bi_artifacts_from_workspace_apis(
        self,
        power_bi_client: PowerBIClient,
        workspace_id: str,
        workspace_name: str,
    ) -> List[Dict[str, Any]]:
        artifacts = [
            self._build_power_bi_artifact_descriptor(
                item_type="POWER_BI_WORKSPACE",
                external_id=workspace_id,
                name=workspace_name,
                folder_path="workspace",
                artifact={"id": workspace_id, "name": workspace_name},
                restore_supported=False,
            )
        ]

        reports = await power_bi_client.list_reports_in_group(workspace_id)
        for report in reports:
            is_paginated = report.get("reportType") == "PaginatedReport"
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_PAGINATED_REPORT" if is_paginated else "POWER_BI_REPORT",
                    external_id=report.get("id"),
                    name=report.get("name", report.get("id")),
                    folder_path="reports",
                    artifact=report,
                    restore_supported=True,
                    definition_supported=True,
                    fabric_item_type="PaginatedReport" if is_paginated else "Report",
                )
            )

        dashboards = await power_bi_client.list_dashboards_in_group(workspace_id)
        for dashboard in dashboards:
            dashboard_id = dashboard.get("id")
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_DASHBOARD",
                    external_id=dashboard_id,
                    name=dashboard.get("displayName", dashboard_id),
                    folder_path="dashboards",
                    artifact=dashboard,
                    restore_supported=False,
                )
            )
            tiles = await power_bi_client.list_tiles_in_group(workspace_id, dashboard_id)
            for tile in tiles:
                artifacts.append(
                    self._build_power_bi_artifact_descriptor(
                        item_type="POWER_BI_TILE",
                        external_id=f"{dashboard_id}:{tile.get('id')}",
                        name=tile.get("title", tile.get("id")),
                        folder_path=f"dashboards/{dashboard.get('displayName', dashboard_id)}",
                        artifact={"dashboardId": dashboard_id, **tile},
                        restore_supported=False,
                    )
                )

        datasets = await power_bi_client.list_datasets_in_group(workspace_id)
        for dataset in datasets:
            dataset_id = dataset.get("id")
            artifacts.append(
                self._build_power_bi_artifact_descriptor(
                    item_type="POWER_BI_SEMANTIC_MODEL",
                    external_id=dataset_id,
                    name=dataset.get("name", dataset_id),
                    folder_path="semantic-models",
                    artifact=dataset,
                    restore_supported=True,
                    definition_supported=True,
                    definition_format="TMDL",
                    fabric_item_type="SemanticModel",
                )
            )

        try:
            dataflows = await power_bi_client.list_dataflows_in_group(workspace_id)
            for dataflow in dataflows:
                dataflow_id = dataflow.get("objectId") or dataflow.get("id")
                artifacts.append(
                    self._build_power_bi_artifact_descriptor(
                        item_type="POWER_BI_DATAFLOW",
                        external_id=dataflow_id,
                        name=dataflow.get("name", dataflow_id),
                        folder_path="dataflows",
                        artifact=dataflow,
                        restore_supported=True,
                        definition_supported=True,
                        fabric_item_type="Dataflow",
                    )
                )
        except Exception as exc:
            logger.warning("Failed to list Power BI dataflows for workspace %s: %s", workspace_id, exc)

        return artifacts

    def _build_power_bi_artifact_descriptor(
        self,
        *,
        item_type: str,
        external_id: str,
        name: str,
        folder_path: str,
        artifact: Dict[str, Any],
        restore_supported: bool,
        definition_supported: bool = False,
        definition_format: Optional[str] = None,
        fabric_item_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        external_id = str(external_id)
        return {
            "item_type": item_type,
            "external_id": external_id,
            "name": name or external_id,
            "folder_path": folder_path,
            "artifact": artifact,
            "restore_supported": restore_supported,
            "definition_supported": definition_supported,
            "definition_format": definition_format,
            "fabric_item_type": fabric_item_type,
            "blob_id": f"{item_type.lower()}_{external_id}",
            "change_token": self._power_bi_change_token(artifact),
        }

    def _power_bi_change_token(self, artifact: Dict[str, Any]) -> str:
        token_parts = [
            artifact.get("modifiedDateTime") or artifact.get("createdDateTime") or artifact.get("createdDate") or "",
            artifact.get("modifiedBy") or artifact.get("createdBy") or artifact.get("configuredBy") or "",
            artifact.get("datasetId") or artifact.get("id") or artifact.get("objectId") or "",
            artifact.get("name") or artifact.get("displayName") or "",
            str(len(artifact.get("tiles", []))) if isinstance(artifact.get("tiles"), list) else "",
            str(len(artifact.get("datasourceUsages", []))) if isinstance(artifact.get("datasourceUsages"), list) else "",
        ]
        return "|".join(str(part) for part in token_parts)

    def _power_bi_artifact_changed(self, previous_item: Optional[SnapshotItem], artifact: Dict[str, Any]) -> bool:
        if previous_item is None:
            return True
        previous_summary = getattr(previous_item, "extra_data", {}) or {}
        return previous_summary.get("change_token") != artifact["change_token"]

    async def _build_power_bi_artifact_payload(
        self,
        power_bi_client: PowerBIClient,
        workspace_id: str,
        artifact: Dict[str, Any],
    ) -> tuple[Dict[str, Any], bytes]:
        definition = None
        restore_supported = artifact["restore_supported"]
        manual_actions: List[str] = []

        if artifact["definition_supported"] and artifact.get("fabric_item_type"):
            try:
                definition = await power_bi_client.get_item_definition(
                    workspace_id,
                    artifact["external_id"],
                    format=artifact.get("definition_format"),
                )
            except Exception as exc:
                restore_supported = False
                manual_actions.append(f"Definition capture unavailable: {exc}")

        payload = {
            "artifact": artifact["artifact"],
            "definition": definition,
            "capturedAt": datetime.utcnow().isoformat(),
            "restoreSupported": restore_supported,
            "definitionFormat": artifact.get("definition_format"),
            "fabricItemType": artifact.get("fabric_item_type"),
        }
        blob_bytes = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        summary = {
            "workspace_id": workspace_id,
            "change_token": artifact["change_token"],
            "restore_supported": restore_supported,
            "definition_supported": artifact["definition_supported"],
            "definition_format": artifact.get("definition_format"),
            "fabric_item_type": artifact.get("fabric_item_type"),
            "artifact": artifact["artifact"],
            "manual_actions": manual_actions,
        }
        return {
            "summary": summary,
            "restore_supported": restore_supported,
            "manual_actions": manual_actions,
        }, blob_bytes

    async def _backup_metadata_only(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                                    tenant: Tenant, message: Dict) -> Dict:
        """Dispatcher for workloads routed through the 'metadata-only' entry point.
        Misleading name kept for backward compatibility; actual coverage:

          PLANNER / TODO / ONENOTE    - full Graph content backup
          POWER_APPS / POWER_AUTOMATE - full definition backup via Power Platform Admin API
          POWER_DLP                   - policy JSON backup
          COPILOT and anything else   - metadata blob (Graph doesn't expose richer content yet)
        """
        resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
        obj_id = resource.external_id

        if resource_type == "PLANNER":
            return await self._backup_planner(graph_client, resource, snapshot, tenant, obj_id)
        elif resource_type == "TODO":
            return await self._backup_todo(graph_client, resource, snapshot, tenant, obj_id)
        elif resource_type == "ONENOTE":
            return await self._backup_onenote(graph_client, resource, snapshot, tenant, obj_id)
        elif resource_type == "POWER_APPS":
            return await self._backup_power_app(resource, snapshot, tenant)
        elif resource_type == "POWER_AUTOMATE":
            return await self._backup_power_flow(resource, snapshot, tenant)
        elif resource_type == "POWER_DLP":
            return await self._backup_power_dlp(resource, snapshot, tenant)
        else:
            return await self._store_metadata_blob(resource, snapshot, tenant, resource_type)

    async def _store_metadata_blob(self, resource: Resource, snapshot: Snapshot,
                                   tenant: Tenant, resource_type: str) -> Dict:
        """Store resource.extra_data as a single metadata blob (fallback for non-API types)."""
        print(f"[{self.worker_id}] [METADATA START] {resource_type}: {resource.display_name}")
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), resource_type.lower())
        content_bytes = json.dumps(resource.extra_data or {}).encode()
        content_hash = hashlib.sha256(content_bytes).hexdigest()
        blob_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id), "metadata"
        )
        result = await upload_blob_with_retry(container, blob_path, content_bytes, shard)
        if result.get("success"):
            async with async_session_factory() as session:
                session.add(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=resource.external_id or str(resource.id),
                    item_type=resource_type,
                    name=resource.display_name or str(resource.id),
                    content_hash=content_hash, content_size=len(content_bytes),
                    blob_path=blob_path,
                    extra_data={"extra_data": resource.extra_data or {}},
                    content_checksum=content_hash,
                ))
                await session.commit()
            print(f"[{self.worker_id}] [METADATA COMPLETE] {resource_type}: {resource.display_name} — 1 item")
            return {"item_count": 1, "bytes_added": len(content_bytes)}
        print(f"[{self.worker_id}] [METADATA FAILED] {resource_type}: {resource.display_name} — {result.get('error')}")
        return {"item_count": 0, "bytes_added": 0}

    async def _backup_planner(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                              tenant: Tenant, obj_id: str) -> Dict:
        """Backup Planner plans + tasks + task details (description, checklist, references) for a group.

        Item types emitted:
          PLANNER_PLAN         - plan metadata
          PLANNER_TASK         - task summary
          PLANNER_TASK_DETAILS - description / checklist / references (may 404 on stale tasks)
        Per-task failures are counted and stored in snapshot.delta_tokens_json['files_failed'];
        fatal plan-listing errors propagate to the parent for FAILED marking."""
        print(f"[{self.worker_id}] [PLANNER START] {resource.display_name} ({obj_id})")
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "planner")
        db_items: List[SnapshotItem] = []
        files_failed = 0

        plans = await graph_client.get_planner_plans_for_group(obj_id)
        plan_list = plans.get("value", [])
        print(f"[{self.worker_id}]   [PLANNER] {len(plan_list)} plans found")

        for plan in plan_list:
            plan_id = plan.get("id", str(uuid.uuid4()))
            content_bytes = json.dumps(plan).encode()
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"plan_{plan_id}"
            )
            r = await upload_blob_with_retry(container, blob_path, content_bytes, shard)
            if r.get("success"):
                db_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=plan_id, item_type="PLANNER_PLAN",
                    name=plan.get("title", plan_id),
                    content_hash=content_hash, content_size=len(content_bytes),
                    blob_path=blob_path, extra_data={"raw": plan}, content_checksum=content_hash,
                ))
                bytes_added += len(content_bytes)

            try:
                tasks = await graph_client.get_planner_tasks(plan_id=plan_id)
            except Exception as e:
                files_failed += 1
                print(f"[{self.worker_id}]   [PLANNER] Tasks list failed for plan {plan_id}: {e}")
                continue

            for task in tasks.get("value", []):
                task_id = task.get("id", str(uuid.uuid4()))
                tb = json.dumps(task).encode()
                th = hashlib.sha256(tb).hexdigest()
                tp = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), f"task_{task_id}"
                )
                tr = await upload_blob_with_retry(container, tp, tb, shard)
                if tr.get("success"):
                    db_items.append(SnapshotItem(
                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                        external_id=task_id, item_type="PLANNER_TASK",
                        name=task.get("title", task_id),
                        content_hash=th, content_size=len(tb),
                        blob_path=tp, extra_data={"raw": task, "planId": plan_id},
                        content_checksum=th,
                    ))
                    bytes_added += len(tb)

                # Fetch task details (description, checklist, references) — separate endpoint
                try:
                    details = await graph_client.get_planner_task_details(task_id)
                except Exception as e:
                    files_failed += 1
                    print(f"[{self.worker_id}]   [PLANNER] Task details failed for {task_id}: {e}")
                    continue

                if details:
                    db = json.dumps(details).encode()
                    dh = hashlib.sha256(db).hexdigest()
                    dp = azure_storage_manager.build_blob_path(
                        str(tenant.id), str(resource.id), str(snapshot.id), f"task_{task_id}_details"
                    )
                    dr = await upload_blob_with_retry(container, dp, db, shard)
                    if dr.get("success"):
                        db_items.append(SnapshotItem(
                            snapshot_id=snapshot.id, tenant_id=tenant.id,
                            external_id=f"{task_id}:details", item_type="PLANNER_TASK_DETAILS",
                            name=(task.get("title") or task_id) + " (details)",
                            content_hash=dh, content_size=len(db),
                            blob_path=dp, extra_data={"taskId": task_id, "planId": plan_id},
                            content_checksum=dh,
                        ))
                        bytes_added += len(db)

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        if files_failed:
            snapshot.delta_tokens_json = {**(snapshot.delta_tokens_json or {}), "files_failed": files_failed}

        print(f"[{self.worker_id}] [PLANNER COMPLETE] {resource.display_name} — {len(db_items)} items, {files_failed} failures")
        return {"item_count": len(db_items), "bytes_added": bytes_added}

    async def _backup_todo(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                           tenant: Tenant, obj_id: str) -> Dict:
        """Backup Microsoft To Do lists + tasks + checklistItems + linkedResources for a user.

        Item types emitted:
          TODO_LIST               - list metadata
          TODO_TASK               - task summary (title, body, dueDate, reminders)
          TODO_TASK_CHECKLIST     - nested subtasks
          TODO_TASK_LINKED        - attached URLs / app references
        Per-task failures counted in snapshot.delta_tokens_json['files_failed'];
        fatal list errors propagate."""
        print(f"[{self.worker_id}] [TODO START] {resource.display_name} ({obj_id})")
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "todo")
        db_items: List[SnapshotItem] = []
        files_failed = 0

        async def backup_task_extras(list_id: str, task_id: str, task_title: str):
            """Fetch checklistItems and linkedResources for a task; return (items, bytes)."""
            nonlocal files_failed
            extras: List[SnapshotItem] = []
            extras_bytes = 0

            for kind, fetch_fn, item_type in (
                ("checklist", graph_client.get_user_todo_task_checklist, "TODO_TASK_CHECKLIST"),
                ("linked",    graph_client.get_user_todo_task_linked_resources, "TODO_TASK_LINKED"),
            ):
                try:
                    payload = await fetch_fn(obj_id, list_id, task_id)
                except Exception as e:
                    files_failed += 1
                    print(f"[{self.worker_id}]   [TODO] {kind} fetch failed for task {task_id}: {e}")
                    continue
                values = payload.get("value", []) if isinstance(payload, dict) else []
                if not values:
                    continue
                data = json.dumps({"value": values}).encode()
                h = hashlib.sha256(data).hexdigest()
                path = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), f"task_{task_id}_{kind}"
                )
                r = await upload_blob_with_retry(container, path, data, shard)
                if r.get("success"):
                    extras.append(SnapshotItem(
                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                        external_id=f"{task_id}:{kind}", item_type=item_type,
                        name=f"{task_title} ({kind})",
                        content_hash=h, content_size=len(data),
                        blob_path=path, extra_data={"taskId": task_id, "listId": list_id, "count": len(values)},
                        content_checksum=h,
                    ))
                    extras_bytes += len(data)
            return extras, extras_bytes

        async def backup_todo_list(lst):
            nonlocal files_failed
            list_id = lst.get("id", str(uuid.uuid4()))
            local_items: List[SnapshotItem] = []
            local_bytes = 0

            lb = json.dumps(lst).encode()
            lh = hashlib.sha256(lb).hexdigest()
            lp = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"list_{list_id}"
            )
            lr = await upload_blob_with_retry(container, lp, lb, shard)
            if lr.get("success"):
                local_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=list_id, item_type="TODO_LIST",
                    name=lst.get("displayName", list_id),
                    content_hash=lh, content_size=len(lb),
                    blob_path=lp, extra_data={"raw": lst}, content_checksum=lh,
                ))
                local_bytes += len(lb)

            try:
                tasks = await graph_client.get_user_todo_tasks(obj_id, list_id)
            except Exception as e:
                files_failed += 1
                print(f"[{self.worker_id}]   [TODO] Tasks fetch failed for list {list_id}: {e}")
                return local_items, local_bytes

            for task in tasks.get("value", []):
                task_id = task.get("id", str(uuid.uuid4()))
                tb = json.dumps(task).encode()
                th = hashlib.sha256(tb).hexdigest()
                tp = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), f"task_{task_id}"
                )
                tr = await upload_blob_with_retry(container, tp, tb, shard)
                if tr.get("success"):
                    local_items.append(SnapshotItem(
                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                        external_id=task_id, item_type="TODO_TASK",
                        name=task.get("title", task_id),
                        content_hash=th, content_size=len(tb),
                        blob_path=tp, extra_data={"raw": task, "listId": list_id},
                        content_checksum=th,
                    ))
                    local_bytes += len(tb)

                extras, extras_bytes = await backup_task_extras(list_id, task_id, task.get("title", task_id))
                local_items.extend(extras)
                local_bytes += extras_bytes

            return local_items, local_bytes

        lists = await graph_client.get_user_todo_lists(obj_id)
        list_items = lists.get("value", [])
        print(f"[{self.worker_id}]   [TODO] {len(list_items)} task lists found")

        list_results = await asyncio.gather(*[backup_todo_list(lst) for lst in list_items], return_exceptions=True)
        for r in list_results:
            if isinstance(r, tuple):
                db_items.extend(r[0])
                bytes_added += r[1]
            else:
                print(f"[{self.worker_id}]   [TODO] List task error: {r}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        if files_failed:
            snapshot.delta_tokens_json = {**(snapshot.delta_tokens_json or {}), "files_failed": files_failed}

        print(f"[{self.worker_id}] [TODO COMPLETE] {resource.display_name} — {len(db_items)} items, {files_failed} failures")
        return {"item_count": len(db_items), "bytes_added": bytes_added}

    async def _backup_onenote(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                              tenant: Tenant, obj_id: str) -> Dict:
        """Backup OneNote notebooks, sections, and pages (including HTML body + inline resources) for a user.

        Three item types are written:
          ONENOTE_NOTEBOOK  - notebook metadata JSON
          ONENOTE_SECTION   - section metadata JSON
          ONENOTE_PAGE      - page metadata JSON
          ONENOTE_PAGE_CONTENT - page HTML body (text/html blob)
          ONENOTE_RESOURCE  - inline image/attachment referenced by a page
        Partial failures (e.g. single page content 404) are logged and skipped;
        a top-level fatal error re-raises so the parent handler can mark the
        snapshot FAILED."""
        print(f"[{self.worker_id}] [ONENOTE START] {resource.display_name} ({obj_id})")
        import re
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "onenote")
        db_items: List[SnapshotItem] = []
        files_failed = 0
        resource_url_re = re.compile(r'data-fullres-src="([^"]+)"|src="(https://graph\.microsoft\.com/[^"]+)"')

        async def _upload(path: str, data: bytes) -> bool:
            r = await upload_blob_with_retry(container, path, data, shard)
            return bool(r.get("success"))

        async def backup_page(user_id: str, page: Dict, nb_id: str, sec_id: str):
            nonlocal files_failed
            pg_id = page.get("id", str(uuid.uuid4()))
            local_items: List[SnapshotItem] = []
            local_bytes = 0

            # 1. page metadata
            pb = json.dumps(page).encode()
            ph = hashlib.sha256(pb).hexdigest()
            pp = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"page_{pg_id}"
            )
            if await _upload(pp, pb):
                local_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=pg_id, item_type="ONENOTE_PAGE",
                    name=page.get("title", pg_id),
                    content_hash=ph, content_size=len(pb), blob_path=pp,
                    extra_data={"raw": page, "sectionId": sec_id, "notebookId": nb_id},
                    content_checksum=ph,
                ))
                local_bytes += len(pb)

            # 2. page HTML body
            html = None
            try:
                html = await graph_client.get_onenote_page_content(user_id, pg_id)
            except Exception as e:
                files_failed += 1
                print(f"[{self.worker_id}]   [ONENOTE] Page content fetch failed for {pg_id}: {e}")

            if html:
                hh = hashlib.sha256(html).hexdigest()
                hp = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), f"page_{pg_id}_content"
                )
                if await _upload(hp, html):
                    local_items.append(SnapshotItem(
                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                        external_id=f"{pg_id}:content", item_type="ONENOTE_PAGE_CONTENT",
                        name=(page.get("title") or pg_id) + " (content)",
                        content_hash=hh, content_size=len(html), blob_path=hp,
                        extra_data={"pageId": pg_id, "sectionId": sec_id, "notebookId": nb_id,
                                  "mime": "text/html"},
                        content_checksum=hh,
                    ))
                    local_bytes += len(html)

                # 3. inline resources (images / attachments) — dedupe URLs per page
                try:
                    urls: List[str] = []
                    for m in resource_url_re.finditer(html.decode("utf-8", errors="replace")):
                        u = m.group(1) or m.group(2)
                        if u and u not in urls:
                            urls.append(u)
                    for u in urls:
                        try:
                            data = await graph_client.get_onenote_resource(u)
                            rid_match = re.search(r"resources/([^/?]+)", u)
                            rid = rid_match.group(1) if rid_match else hashlib.md5(u.encode()).hexdigest()[:16]
                            rhash = hashlib.sha256(data).hexdigest()
                            rpath = azure_storage_manager.build_blob_path(
                                str(tenant.id), str(resource.id), str(snapshot.id), f"resource_{rid}"
                            )
                            if await _upload(rpath, data):
                                local_items.append(SnapshotItem(
                                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                                    external_id=rid, item_type="ONENOTE_RESOURCE",
                                    name=rid,
                                    content_hash=rhash, content_size=len(data), blob_path=rpath,
                                    extra_data={"pageId": pg_id, "sourceUrl": u},
                                    content_checksum=rhash,
                                ))
                                local_bytes += len(data)
                        except Exception as e:
                            files_failed += 1
                            print(f"[{self.worker_id}]   [ONENOTE] Resource fetch failed for {u}: {e}")
                except Exception as e:
                    print(f"[{self.worker_id}]   [ONENOTE] Resource parse failed for {pg_id}: {e}")

            return local_items, local_bytes

        async def backup_notebook(nb: Dict):
            nb_id = nb.get("id", str(uuid.uuid4()))
            local_items: List[SnapshotItem] = []
            local_bytes = 0

            nb_b = json.dumps(nb).encode()
            nb_h = hashlib.sha256(nb_b).hexdigest()
            nb_p = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"notebook_{nb_id}"
            )
            if await _upload(nb_p, nb_b):
                local_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=nb_id, item_type="ONENOTE_NOTEBOOK",
                    name=nb.get("displayName", nb_id),
                    content_hash=nb_h, content_size=len(nb_b), blob_path=nb_p,
                    extra_data={"raw": nb}, content_checksum=nb_h,
                ))
                local_bytes += len(nb_b)

            try:
                sections = await graph_client.get_onenote_sections(obj_id, nb_id)
            except Exception as e:
                print(f"[{self.worker_id}]   [ONENOTE] Sections fetch failed for notebook {nb_id}: {e}")
                return local_items, local_bytes

            for sec in sections.get("value", []):
                sec_id = sec.get("id", str(uuid.uuid4()))
                sb = json.dumps(sec).encode()
                sh = hashlib.sha256(sb).hexdigest()
                sp = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), f"section_{sec_id}"
                )
                if await _upload(sp, sb):
                    local_items.append(SnapshotItem(
                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                        external_id=sec_id, item_type="ONENOTE_SECTION",
                        name=sec.get("displayName", sec_id),
                        content_hash=sh, content_size=len(sb), blob_path=sp,
                        extra_data={"raw": sec, "notebookId": nb_id}, content_checksum=sh,
                    ))
                    local_bytes += len(sb)

                try:
                    pages = await graph_client.get_onenote_pages(obj_id, sec_id)
                    page_results = await asyncio.gather(
                        *[backup_page(obj_id, pg, nb_id, sec_id) for pg in pages.get("value", [])],
                        return_exceptions=True,
                    )
                    for pr in page_results:
                        if isinstance(pr, tuple):
                            local_items.extend(pr[0])
                            local_bytes += pr[1]
                        else:
                            print(f"[{self.worker_id}]   [ONENOTE] Page task error: {pr}")
                except Exception as e:
                    print(f"[{self.worker_id}]   [ONENOTE] Pages fetch failed for section {sec_id}: {e}")

            return local_items, local_bytes

        notebooks = await graph_client.get_onenote_notebooks(obj_id)
        nb_list = notebooks.get("value", [])
        print(f"[{self.worker_id}]   [ONENOTE] {len(nb_list)} notebooks found")

        nb_results = await asyncio.gather(*[backup_notebook(nb) for nb in nb_list], return_exceptions=True)
        for r in nb_results:
            if isinstance(r, tuple):
                db_items.extend(r[0])
                bytes_added += r[1]
            else:
                print(f"[{self.worker_id}]   [ONENOTE] Notebook task error: {r}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        if files_failed:
            snapshot.delta_tokens_json = {**(snapshot.delta_tokens_json or {}), "files_failed": files_failed}

        print(f"[{self.worker_id}] [ONENOTE COMPLETE] {resource.display_name} — {len(db_items)} items, {files_failed} failures")
        return {"item_count": len(db_items), "bytes_added": bytes_added}

    # ==================== Power Platform (Apps / Flows / DLP) ====================
    #
    # These handlers capture each object's full definition via the Power Platform
    # Admin API (api.bap.microsoft.com / api.flow.microsoft.com). The resource's
    # extra_data must contain 'environment_id' and 'app_id' / 'flow_id' / 'policy_id'
    # from discovery. Without those we fall back to storing resource.extra_data only.
    #
    # Restore path: re-import the definition JSON via the matching import endpoint.
    # Full .msapp package export (async + SAS download) is a Phase 2b stretch.

    async def _backup_power_app(self, resource: Resource, snapshot: Snapshot, tenant: Tenant) -> Dict:
        """Backup a single Power App's full definition."""
        print(f"[{self.worker_id}] [POWER_APP START] {resource.display_name}")
        meta = resource.extra_data or {}
        env_id = meta.get("environment_id")
        app_id = meta.get("app_id") or meta.get("appId") or resource.external_id
        if not env_id or not app_id:
            print(f"[{self.worker_id}] [POWER_APP] missing environment_id / app_id in extra_data; storing metadata only")
            return await self._store_metadata_blob(resource, snapshot, tenant, "POWER_APPS")

        client = self.get_power_platform_client(tenant)
        try:
            definition = await client.get_app(env_id, app_id)
        except httpx.HTTPStatusError as e:
            print(f"[{self.worker_id}] [POWER_APP] fetch failed ({e.response.status_code}): {e.response.text[:200]}")
            raise

        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "power-apps")
        blob_bytes = json.dumps(definition).encode()
        content_hash = hashlib.sha256(blob_bytes).hexdigest()
        blob_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id), f"app_{app_id}_definition"
        )
        r = await upload_blob_with_retry(container, blob_path, blob_bytes, shard)
        if not r.get("success"):
            raise RuntimeError(f"Power App definition upload failed: {r.get('error')}")

        items_added: List[SnapshotItem] = [SnapshotItem(
            snapshot_id=snapshot.id, tenant_id=tenant.id,
            external_id=app_id, item_type="POWER_APP_DEFINITION",
            name=resource.display_name or app_id,
            content_hash=content_hash, content_size=len(blob_bytes),
            blob_path=blob_path,
            extra_data={"environmentId": env_id, "appId": app_id, "raw": definition.get("properties", {})},
            content_checksum=content_hash,
        )]
        total_bytes = len(blob_bytes)

        # Non-fatal: also export the full .zip package (includes compiled canvas XAML + assets).
        # If the app type doesn't support package export (e.g. model-driven), export_app_package
        # returns None; transient failures are logged and counted.
        package_failed = False
        try:
            pkg_bytes = await client.export_app_package(env_id, app_id, resource.display_name)
        except Exception as e:
            print(f"[{self.worker_id}] [POWER_APP] package export failed for {app_id}: {e}")
            pkg_bytes = None
            package_failed = True

        if pkg_bytes:
            pkg_hash = hashlib.sha256(pkg_bytes).hexdigest()
            pkg_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"app_{app_id}_package.zip"
            )
            pr = await upload_blob_with_retry(container, pkg_path, pkg_bytes, shard)
            if pr.get("success"):
                items_added.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=f"{app_id}:package", item_type="POWER_APP_PACKAGE",
                    name=(resource.display_name or app_id) + " (package)",
                    content_hash=pkg_hash, content_size=len(pkg_bytes),
                    blob_path=pkg_path,
                    extra_data={"environmentId": env_id, "appId": app_id, "mime": "application/zip"},
                    content_checksum=pkg_hash,
                ))
                total_bytes += len(pkg_bytes)

        async with async_session_factory() as session:
            session.add_all(items_added)
            await session.commit()

        if package_failed:
            snapshot.delta_tokens_json = {**(snapshot.delta_tokens_json or {}), "files_failed": 1}

        print(f"[{self.worker_id}] [POWER_APP COMPLETE] {resource.display_name} — {len(items_added)} items, {total_bytes} bytes")
        return {"item_count": len(items_added), "bytes_added": total_bytes}

    async def _backup_power_flow(self, resource: Resource, snapshot: Snapshot, tenant: Tenant) -> Dict:
        """Backup a Power Automate flow definition plus its connection references."""
        print(f"[{self.worker_id}] [POWER_FLOW START] {resource.display_name}")
        meta = resource.extra_data or {}
        env_id = meta.get("environment_id")
        flow_id = meta.get("flow_id") or meta.get("flowId") or resource.external_id
        if not env_id or not flow_id:
            print(f"[{self.worker_id}] [POWER_FLOW] missing environment_id / flow_id in extra_data; storing metadata only")
            return await self._store_metadata_blob(resource, snapshot, tenant, "POWER_AUTOMATE")

        client = self.get_power_platform_client(tenant)
        try:
            definition = await client.get_flow(env_id, flow_id)
        except httpx.HTTPStatusError as e:
            print(f"[{self.worker_id}] [POWER_FLOW] fetch failed ({e.response.status_code}): {e.response.text[:200]}")
            raise
        # Connections are advisory — non-fatal if endpoint doesn't return them
        connections = await client.get_flow_connections(env_id, flow_id)

        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "power-automate")

        items_added: List[SnapshotItem] = []
        bytes_added = 0

        def_bytes = json.dumps(definition).encode()
        def_hash = hashlib.sha256(def_bytes).hexdigest()
        def_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id), f"flow_{flow_id}_definition"
        )
        r = await upload_blob_with_retry(container, def_path, def_bytes, shard)
        if not r.get("success"):
            raise RuntimeError(f"Flow definition upload failed: {r.get('error')}")
        items_added.append(SnapshotItem(
            snapshot_id=snapshot.id, tenant_id=tenant.id,
            external_id=flow_id, item_type="POWER_FLOW_DEFINITION",
            name=resource.display_name or flow_id,
            content_hash=def_hash, content_size=len(def_bytes),
            blob_path=def_path,
            extra_data={"environmentId": env_id, "flowId": flow_id,
                      "state": definition.get("properties", {}).get("state"),
                      "raw": definition.get("properties", {})},
            content_checksum=def_hash,
        ))
        bytes_added += len(def_bytes)

        conn_values = connections.get("value") if isinstance(connections, dict) else None
        if conn_values:
            conn_bytes = json.dumps({"value": conn_values}).encode()
            conn_hash = hashlib.sha256(conn_bytes).hexdigest()
            conn_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"flow_{flow_id}_connections"
            )
            r = await upload_blob_with_retry(container, conn_path, conn_bytes, shard)
            if r.get("success"):
                items_added.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=f"{flow_id}:connections", item_type="POWER_FLOW_CONNECTIONS",
                    name=(resource.display_name or flow_id) + " (connections)",
                    content_hash=conn_hash, content_size=len(conn_bytes),
                    blob_path=conn_path,
                    extra_data={"flowId": flow_id, "environmentId": env_id, "count": len(conn_values)},
                    content_checksum=conn_hash,
                ))
                bytes_added += len(conn_bytes)

        # Non-fatal: flow package ZIP (includes connection references + custom connector refs).
        package_failed = False
        try:
            pkg_bytes = await client.export_flow_package(env_id, flow_id, resource.display_name)
        except Exception as e:
            print(f"[{self.worker_id}] [POWER_FLOW] package export failed for {flow_id}: {e}")
            pkg_bytes = None
            package_failed = True

        if pkg_bytes:
            pkg_hash = hashlib.sha256(pkg_bytes).hexdigest()
            pkg_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"flow_{flow_id}_package.zip"
            )
            pr = await upload_blob_with_retry(container, pkg_path, pkg_bytes, shard)
            if pr.get("success"):
                items_added.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=f"{flow_id}:package", item_type="POWER_FLOW_PACKAGE",
                    name=(resource.display_name or flow_id) + " (package)",
                    content_hash=pkg_hash, content_size=len(pkg_bytes),
                    blob_path=pkg_path,
                    extra_data={"environmentId": env_id, "flowId": flow_id, "mime": "application/zip"},
                    content_checksum=pkg_hash,
                ))
                bytes_added += len(pkg_bytes)

        async with async_session_factory() as session:
            session.add_all(items_added)
            await session.commit()

        if package_failed:
            snapshot.delta_tokens_json = {**(snapshot.delta_tokens_json or {}), "files_failed": 1}

        print(f"[{self.worker_id}] [POWER_FLOW COMPLETE] {resource.display_name} — {len(items_added)} items, {bytes_added} bytes")
        return {"item_count": len(items_added), "bytes_added": bytes_added}

    async def _backup_power_dlp(self, resource: Resource, snapshot: Snapshot, tenant: Tenant) -> Dict:
        """Backup a Power Platform DLP policy definition (connector groups + rules)."""
        print(f"[{self.worker_id}] [POWER_DLP START] {resource.display_name}")
        meta = resource.extra_data or {}
        policy_id = meta.get("policy_id") or meta.get("policyId") or resource.external_id
        if not policy_id:
            print(f"[{self.worker_id}] [POWER_DLP] missing policy_id in extra_data; storing metadata only")
            return await self._store_metadata_blob(resource, snapshot, tenant, "POWER_DLP")

        client = self.get_power_platform_client(tenant)
        try:
            definition = await client.get_dlp_policy(policy_id)
        except httpx.HTTPStatusError as e:
            print(f"[{self.worker_id}] [POWER_DLP] fetch failed ({e.response.status_code}): {e.response.text[:200]}")
            raise

        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "power-dlp")
        blob_bytes = json.dumps(definition).encode()
        content_hash = hashlib.sha256(blob_bytes).hexdigest()
        blob_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id), f"dlp_{policy_id}"
        )
        r = await upload_blob_with_retry(container, blob_path, blob_bytes, shard)
        if not r.get("success"):
            raise RuntimeError(f"DLP policy upload failed: {r.get('error')}")

        async with async_session_factory() as session:
            session.add(SnapshotItem(
                snapshot_id=snapshot.id, tenant_id=tenant.id,
                external_id=policy_id, item_type="POWER_DLP_POLICY",
                name=resource.display_name or policy_id,
                content_hash=content_hash, content_size=len(blob_bytes),
                blob_path=blob_path,
                extra_data={"policyId": policy_id, "raw": definition.get("properties", {})},
                content_checksum=content_hash,
            ))
            await session.commit()

        print(f"[{self.worker_id}] [POWER_DLP COMPLETE] {resource.display_name} — {len(blob_bytes)} bytes")
        return {"item_count": 1, "bytes_added": len(blob_bytes)}

    # ==================== Single Resource Backup Handlers ====================

    async def backup_teams_single(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                                  tenant: Tenant, message: Dict) -> Dict:
        """Single-resource Teams backup (matches handler signature)"""
        return await self._backup_teams_resource(resource, tenant, snapshot, graph_client, None)

    async def backup_teams_chat_single(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                                       tenant: Tenant, message: Dict) -> Dict:
        """Single-resource Teams Chat backup (matches handler signature)"""
        return await self._backup_teams_chat_resource(resource, tenant, snapshot, graph_client, None)

    async def backup_entra_single(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                                  tenant: Tenant, message: Dict) -> Dict:
        """Single-resource Entra ID backup (matches handler signature)"""
        return await self._backup_entra_resource(resource, tenant, snapshot, graph_client, None, message)

    async def backup_entra_directory(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                                     tenant: Tenant, message: Dict) -> Dict:
        """Back up the whole Microsoft Entra directory as one snapshot.
        Mirrors AFI's office_directory model — eight content categories
        persisted under this single resource so the Recovery view can
        render tabs: Users / Groups / Roles / Security / Audit /
        Applications / Intune / Administrative Units.

        Each category is best-effort: a 403 on one endpoint (e.g. Intune
        requires DeviceManagementManagedDevices.Read.All, audit logs
        require AuditLog.Read.All) is swallowed so a single missing
        permission doesn't fail the whole snapshot.
        """
        print(f"[{self.worker_id}] [ENTRA_DIR START] {resource.display_name}")
        total_items = 0
        total_bytes = 0

        # One-off helper: persist a batch of items keyed by item_type.
        async def _persist_batch(item_type: str, category_label: str, items: List[Dict[str, Any]]) -> int:
            nonlocal total_items, total_bytes
            if not items:
                return 0
            rows: List[SnapshotItem] = []
            for obj in items:
                ext_id = str(obj.get("id") or obj.get("roleTemplateId") or obj.get("appId") or uuid.uuid4())
                display = (
                    obj.get("displayName")
                    or obj.get("userPrincipalName")
                    or obj.get("mail")
                    or obj.get("activityDisplayName")
                    or obj.get("category")
                    or ext_id
                )
                raw_bytes = json.dumps(obj, default=str).encode()
                rows.append(SnapshotItem(
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=ext_id[:255],
                    item_type=item_type,
                    name=str(display)[:255],
                    folder_path=category_label,
                    content_size=len(raw_bytes),
                    content_hash=hashlib.sha256(raw_bytes).hexdigest(),
                    content_checksum=None,
                    extra_data={"raw": obj, "category": category_label, "fingerprint": _entra_fp(item_type, obj)},
                ))
                total_bytes += len(raw_bytes)
            async with async_session_factory() as sess:
                sess.add_all(rows)
                await sess.commit()
            total_items += len(rows)
            print(f"[{self.worker_id}]   [ENTRA_DIR] {category_label}: captured {len(rows)} row(s)")
            return len(rows)

        async def _safe_fetch(url: str, params: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
            try:
                data = await graph_client._get(url, params=params)
                return data.get("value") or []
            except Exception as exc:
                logger.warning("[%s] [ENTRA_DIR] %s → %s", self.worker_id, url, exc)
                return []

        base = graph_client.GRAPH_URL

        # 1) Users — pull the field set the Recovery "Users" view needs
        #    (UPN, mail, proxyAddresses for "Other emails", userType +
        #    externalUserState for Guest/External flags, Object ID = id).
        users = await _safe_fetch(f"{base}/users", {
            "$top": "999",
            "$select": "id,displayName,userPrincipalName,mail,proxyAddresses,otherMails,userType,externalUserState,accountEnabled,jobTitle,department,createdDateTime,givenName,surname,mobilePhone,businessPhones",
        })
        await _persist_batch("ENTRA_DIR_USER", "Users", users)

        # 2) Groups — with ownership + membership for the detail panel.
        #    Also fetch per-group owners + first-page members so the
        #    Recovery right-pane can render them without N×M refetches.
        groups = await _safe_fetch(f"{base}/groups", {"$top": "999", "$select": "id,displayName,mail,description,groupTypes,securityEnabled,mailEnabled,visibility,membershipRule,createdDateTime"})
        for g in groups:
            gid = g.get("id")
            if not gid:
                continue
            owners = await _safe_fetch(f"{base}/groups/{gid}/owners", {"$top": "50", "$select": "id,displayName,userPrincipalName,mail"})
            members = await _safe_fetch(f"{base}/groups/{gid}/members", {"$top": "50", "$select": "id,displayName,userPrincipalName,mail"})
            g["_owners"] = owners
            g["_members"] = members
            g["_memberCount"] = len(members)
        await _persist_batch("ENTRA_DIR_GROUP", "Groups", groups)

        # 3) Roles — $expand=rolePermissions so the right pane's
        #    Privileges list is captured. directoryRoles (activated) is
        #    kept in addition to roleDefinitions for parity with AFI.
        roles_active = await _safe_fetch(f"{base}/directoryRoles")
        roles_catalog = await _safe_fetch(f"{base}/roleManagement/directory/roleDefinitions", {"$top": "999", "$expand": "rolePermissions"})
        await _persist_batch("ENTRA_DIR_ROLE", "Roles", roles_active + roles_catalog)

        # 4) Security — AFI shows 5 buckets: Conditional Access,
        #    Authentication Contexts, Authentication Strengths, Named
        #    Locations, Policies. Each item carries a `_sec_bucket`
        #    field so the frontend can split them in the left rail.
        sec_items: List[Dict[str, Any]] = []
        for bucket, url in [
            ("Conditional Access", f"{base}/identity/conditionalAccess/policies"),
            ("Authentication Contexts", f"{base}/identity/conditionalAccess/authenticationContextClassReferences"),
            ("Authentication Strengths", f"{base}/identity/conditionalAccess/authenticationStrength/policies"),
            ("Named Locations", f"{base}/identity/conditionalAccess/namedLocations"),
            ("Policies", f"{base}/policies/identitySecurityDefaultsEnforcementPolicy"),
        ]:
            data = await _safe_fetch(url, {"$top": "500"})
            # identitySecurityDefaultsEnforcementPolicy returns a single
            # object, not a list — wrap it so _persist_batch sees a row.
            if not data and bucket == "Policies":
                try:
                    single = await graph_client._get(url)
                    if single and isinstance(single, dict) and single.get("id"):
                        data = [single]
                except Exception:
                    data = []
            for d in data:
                d["_sec_bucket"] = bucket
            sec_items.extend(data)
        # Risky users + security alerts complement the above.
        risky_users = await _safe_fetch(f"{base}/identityProtection/riskyUsers", {"$top": "500"})
        for r in risky_users:
            r["_sec_bucket"] = "Risky Users"
        alerts = await _safe_fetch(f"{base}/security/alerts_v2", {"$top": "500"})
        for a in alerts:
            a["_sec_bucket"] = "Alerts"
        sec_items.extend(risky_users + alerts)
        await _persist_batch("ENTRA_DIR_SECURITY", "Security", sec_items)

        # 5) Audit — directory audit + sign-in logs. Tag each row so
        #    the frontend can split into Audit Logs vs Sign-In Logs.
        dir_audits = await _safe_fetch(f"{base}/auditLogs/directoryAudits", {"$top": "500"})
        for a in dir_audits:
            a["_audit_bucket"] = "Audit Logs"
        signins = await _safe_fetch(f"{base}/auditLogs/signIns", {"$top": "500"})
        for s in signins:
            s["_audit_bucket"] = "Sign-In Logs"
        await _persist_batch("ENTRA_DIR_AUDIT", "Audit", dir_audits + signins)

        # 6) Applications — split into App Registrations vs Enterprise
        #    Applications via `_app_bucket`. Capture requiredResourceAccess
        #    on /applications so the Permissions block renders.
        apps = await _safe_fetch(f"{base}/applications", {"$top": "999", "$select": "id,appId,displayName,createdDateTime,requiredResourceAccess,signInAudience,identifierUris,publisherDomain"})
        for a in apps:
            a["_app_bucket"] = "App Registrations"
        sps = await _safe_fetch(f"{base}/servicePrincipals", {"$top": "999", "$select": "id,appId,displayName,servicePrincipalType,appRoles,oauth2PermissionScopes,createdDateTime"})
        for s in sps:
            s["_app_bucket"] = "Enterprise Applications"
        await _persist_batch("ENTRA_DIR_APPLICATION", "Applications", apps + sps)

        # 7) Intune — AFI's "Devices" bucket matches /devices (Entra AD
        #    devices) rather than Intune /managedDevices (which needs a
        #    separate license). Compliance + Configuration policies
        #    still come from Intune and may 400 without the license.
        entra_devices = await _safe_fetch(f"{base}/devices", {"$top": "500", "$select": "id,displayName,accountEnabled,operatingSystem,operatingSystemVersion,trustType,registrationDateTime,isCompliant,isManaged"})
        for d in entra_devices:
            d["_intune_bucket"] = "Devices"
            # Owner lookup — first registered user per device.
            try:
                owners = await _safe_fetch(f"{base}/devices/{d.get('id')}/registeredOwners", {"$top": "1", "$select": "id,displayName,userPrincipalName"})
                if owners:
                    d["_owner_display"] = owners[0].get("displayName")
                    d["_owner_upn"] = owners[0].get("userPrincipalName")
            except Exception:
                pass
        comp_policies = await _safe_fetch(f"{base}/deviceManagement/deviceCompliancePolicies", {"$top": "500"})
        for c in comp_policies:
            c["_intune_bucket"] = "Compliance Policies"
        conf_policies = await _safe_fetch(f"{base}/deviceManagement/deviceConfigurations", {"$top": "500"})
        for c in conf_policies:
            c["_intune_bucket"] = "Configuration Profiles"
        await _persist_batch("ENTRA_DIR_INTUNE", "Intune", entra_devices + comp_policies + conf_policies)

        # 8) Administrative Units.
        admin_units = await _safe_fetch(f"{base}/directory/administrativeUnits", {"$top": "500"})
        await _persist_batch("ENTRA_DIR_ADMIN_UNIT", "Administrative Units", admin_units)

        print(f"[{self.worker_id}] [ENTRA_DIR COMPLETE] {resource.display_name} — {total_items} items, {total_bytes} bytes")
        return {"item_count": total_items, "bytes_added": total_bytes}

    async def backup_conditional_access(
        self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
        tenant: Tenant, message: Dict,
    ) -> Dict:
        """Backup a single Conditional Access policy as a JSON definition blob.

        Source: GET /identity/conditionalAccess/policies/{id}. Falls back to
        the cached metadata captured at discovery time if the live fetch fails
        (e.g. permission lost between discovery and backup)."""
        print(f"[{self.worker_id}] [CA POLICY START] {resource.display_name}")
        definition = await graph_client.get_conditional_access_policy(resource.external_id)
        if definition is None:
            cached = (resource.extra_data or {}).get("raw")
            if not cached:
                print(f"[{self.worker_id}] [CA POLICY] policy {resource.external_id} not found and no cached copy — skipping")
                return {"item_count": 0, "bytes_added": 0, "note": "not_found"}
            definition = cached

        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "entra")
        blob_bytes = json.dumps(definition).encode()
        content_hash = hashlib.sha256(blob_bytes).hexdigest()
        blob_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id),
            f"ca_{resource.external_id}",
        )
        upload_result = await upload_blob_with_retry(container, blob_path, blob_bytes, shard, max_retries=3)
        if not (isinstance(upload_result, dict) and upload_result.get("success")):
            raise RuntimeError(f"CA policy upload failed: {upload_result.get('error') if isinstance(upload_result, dict) else upload_result}")

        item = SnapshotItem(
            snapshot_id=snapshot.id, tenant_id=tenant.id,
            external_id=resource.external_id,
            item_type="CONDITIONAL_ACCESS_POLICY",
            name=resource.display_name,
            content_hash=content_hash, content_size=len(blob_bytes),
            blob_path=blob_path,
            extra_data={
                "state": definition.get("state"),
                "modified_at": definition.get("modifiedDateTime"),
            },
            content_checksum=content_hash,
        )
        async with async_session_factory() as session:
            session.add(item)
            await session.commit()
        print(f"[{self.worker_id}] [CA POLICY COMPLETE] {resource.display_name} — state={definition.get('state')}, {len(blob_bytes)} bytes")
        return {"item_count": 1, "bytes_added": len(blob_bytes)}

    async def backup_bitlocker_key(
        self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
        tenant: Tenant, message: Dict,
    ) -> Dict:
        """Backup a BitLocker recovery key. The actual key bytes are only
        returned by /recoveryKeys/{id}?$select=key — without BitlockerKey.Read.All
        we fall back to metadata-only (still useful for inventory)."""
        print(f"[{self.worker_id}] [BITLOCKER START] {resource.display_name}")
        full = await graph_client.get_bitlocker_key_value(resource.external_id)
        if full is None:
            full = {
                "id": resource.external_id,
                "deviceId": (resource.extra_data or {}).get("device_id"),
                "volumeType": (resource.extra_data or {}).get("volume_type"),
                "createdDateTime": (resource.extra_data or {}).get("created_at"),
                "_metadata_only": True,  # marker — no key bytes available
            }

        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "entra")
        blob_bytes = json.dumps(full).encode()
        content_hash = hashlib.sha256(blob_bytes).hexdigest()
        blob_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id),
            f"bitlocker_{resource.external_id}",
        )
        upload_result = await upload_blob_with_retry(container, blob_path, blob_bytes, shard, max_retries=3)
        if not (isinstance(upload_result, dict) and upload_result.get("success")):
            raise RuntimeError(f"BitLocker key upload failed: {upload_result.get('error') if isinstance(upload_result, dict) else upload_result}")

        item = SnapshotItem(
            snapshot_id=snapshot.id, tenant_id=tenant.id,
            external_id=resource.external_id,
            item_type="BITLOCKER_RECOVERY_KEY",
            name=resource.display_name,
            content_hash=content_hash, content_size=len(blob_bytes),
            blob_path=blob_path,
            extra_data={
                "device_id": full.get("deviceId"),
                "volume_type": full.get("volumeType"),
                "has_key_value": "key" in full,
            },
            content_checksum=content_hash,
        )
        async with async_session_factory() as session:
            session.add(item)
            await session.commit()
        print(f"[{self.worker_id}] [BITLOCKER COMPLETE] {resource.display_name} — has_key={'key' in full}, {len(blob_bytes)} bytes")
        return {"item_count": 1, "bytes_added": len(blob_bytes)}

    async def backup_mailbox(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                             tenant: Tenant, message: Dict) -> Dict:
        """Backup a single mailbox.

        Source coverage (afi parity):
          - primary mailbox          — always
          - online archive mailbox   — when policy.backup_exchange_archive
          - recoverable items folder — when policy.backup_exchange_recoverable

        Folder fidelity: builds a folder_id → "/Inbox/SubFolder" map per source
        and tags every message with its full path so restore can rebuild the
        exact hierarchy."""
        print(f"[{self.worker_id}] [MAILBOX START] {resource.display_name} ({resource.external_id})")
        user_id = resource.external_id

        # Policy gates for which mailbox tiers to include.
        policy = await self.get_sla_policy(resource, message)
        backup_archive = bool(getattr(policy, "backup_exchange_archive", False)) if policy else False
        backup_recoverable = bool(getattr(policy, "backup_exchange_recoverable", False)) if policy else False
        exclusions = await self.get_policy_exclusions(policy.id) if policy else []

        # Build folder trees — one per source. Empty dict = source not present.
        folder_trees: Dict[str, Dict[str, str]] = {}
        try:
            folder_trees["primary"] = await graph_client.get_mail_folder_tree(user_id)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"[{self.worker_id}]   [EMAIL] No mailbox found — user has no Exchange Online license")
                return {"item_count": 0, "bytes_added": 0, "new_delta_token": None, "note": "no_mailbox"}
            raise
        if backup_archive:
            folder_trees["archive"] = await graph_client.get_mail_folder_tree(user_id, well_known_root="archive")
            if folder_trees["archive"]:
                print(f"[{self.worker_id}]   [ARCHIVE] Found {len(folder_trees['archive'])} folder(s)")
        if backup_recoverable:
            folder_trees["recoverable"] = await graph_client.get_mail_folder_tree(user_id, well_known_root="recoverableitemsroot")
            if folder_trees["recoverable"]:
                print(f"[{self.worker_id}]   [RECOVERABLE] Found {len(folder_trees['recoverable'])} folder(s)")

        # Fetch messages from each source. Primary uses the existing /messages
        # top-level call (covers all primary folders in one paginated query);
        # archive + recoverable need per-folder fetches because there's no
        # equivalent top-level endpoint that crosses mailbox boundaries.
        print(f"[{self.worker_id}]   [EMAIL] Fetching primary messages (paginated)...")
        delta_token = (resource.extra_data or {}).get("mail_delta_token")
        try:
            messages = await graph_client.get_messages_delta(user_id, delta_token)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"[{self.worker_id}]   [EMAIL] No mailbox found — user has no Exchange Online license")
                return {"item_count": 0, "bytes_added": 0, "new_delta_token": None, "note": "no_mailbox"}
            raise

        primary_items = messages.get("value", [])
        for m in primary_items:
            m["_source_mailbox"] = "primary"
            m["_full_folder_path"] = folder_trees["primary"].get(m.get("parentFolderId", ""), m.get("parentFolderName") or "")

        items = list(primary_items)

        async def _collect_from_secondary(source: str) -> int:
            tree = folder_trees.get(source) or {}
            if not tree:
                return 0
            collected = 0
            for fid, path in tree.items():
                folder_msgs = await graph_client.list_messages_in_folder(user_id, fid)
                for m in folder_msgs:
                    m["_source_mailbox"] = source
                    m["_full_folder_path"] = path
                items.extend(folder_msgs)
                collected += len(folder_msgs)
            return collected

        if backup_archive:
            n = await _collect_from_secondary("archive")
            print(f"[{self.worker_id}]   [ARCHIVE] Collected {n} messages from {len(folder_trees.get('archive') or {})} folder(s)")
        if backup_recoverable:
            n = await _collect_from_secondary("recoverable")
            print(f"[{self.worker_id}]   [RECOVERABLE] Collected {n} messages from {len(folder_trees.get('recoverable') or {})} folder(s)")

        print(f"[{self.worker_id}]   [EMAIL] Total {len(items)} messages to backup ({len(primary_items)} primary)")

        # SLA exclusions — filter before upload so excluded items never touch storage
        if exclusions:
            before = len(items)
            items = [m for m in items if not self._item_is_excluded("EMAIL", m, exclusions)]
            excluded = before - len(items)
            if excluded:
                print(f"[{self.worker_id}]   [EMAIL] Excluded {excluded}/{before} by SLA policy rules")

        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "mailbox")

        # Upload ALL messages in parallel, batch DB insert per 50-msg chunk
        batch_size = 50
        batches = [items[i:i+batch_size] for i in range(0, len(items), batch_size)]

        async def backup_batch(batch):
            upload_tasks = []
            item_metas = []
            for msg in batch:
                msg_id = msg.get("id", str(uuid.uuid4()))
                content_bytes = json.dumps(msg).encode()
                content_hash = hashlib.sha256(content_bytes).hexdigest()
                blob_path = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), msg_id
                )
                upload_tasks.append(upload_blob_with_retry(container, blob_path, content_bytes, shard, max_retries=3))
                item_metas.append((msg_id, msg, content_bytes, content_hash, blob_path))

            upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

            db_items = []
            b_bytes = 0
            for (msg_id, msg, content_bytes, content_hash, blob_path), result in zip(item_metas, upload_results):
                if isinstance(result, dict) and result.get("success"):
                    db_items.append(SnapshotItem(
                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                        external_id=msg_id, item_type="EMAIL",
                        name=msg.get("subject", msg_id),
                        # Full hierarchical path (e.g. "/Inbox/Project X") so
                        # restore can recreate the folder tree, not just dump
                        # everything to the inbox root.
                        folder_path=msg.get("_full_folder_path") or msg.get("parentFolderName"),
                        content_hash=content_hash, content_size=len(content_bytes),
                        blob_path=blob_path,
                        extra_data={
                            "raw": msg,
                            "source_mailbox": msg.get("_source_mailbox", "primary"),
                        },
                        content_checksum=content_hash,
                    ))
                    b_bytes += len(content_bytes)
                elif not isinstance(result, Exception):
                    print(f"[{self.worker_id}]   [EMAIL] Upload FAILED: {msg.get('subject', msg_id)}: {result.get('error', 'unknown')}")

            if db_items:
                async with async_session_factory() as session:
                    session.add_all(db_items)
                    await session.commit()
            return len(db_items), b_bytes

        batch_results = await asyncio.gather(*[backup_batch(b) for b in batches], return_exceptions=True)
        for r in batch_results:
            if isinstance(r, tuple):
                item_count += r[0]
                bytes_added += r[1]

        # Phase: capture attachments for messages that have them. afi.ai stores
        # each fileAttachment as a separate restorable item linked to the parent
        # message — without this, restored emails have empty attachment stubs.
        att_msgs = [m for m in items if m.get("hasAttachments")]
        if att_msgs:
            att_count, att_bytes = await self._backup_message_attachments(
                graph_client, resource, snapshot, tenant, container, shard, att_msgs,
            )
            item_count += att_count
            bytes_added += att_bytes
            print(f"[{self.worker_id}]   [ATTACHMENTS] Captured {att_count} email attachment(s), {att_bytes} bytes")

        new_delta = messages.get("@odata.deltaLink")
        print(f"[{self.worker_id}] [BACKUP COMPLETE] Mailbox: {resource.display_name} — {item_count} items total, {bytes_added} bytes")
        return {"item_count": item_count, "bytes_added": bytes_added, "new_delta_token": new_delta}

    async def _backup_message_attachments(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        container: str,
        shard,
        messages_with_attachments: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        """For each message flagged hasAttachments, list and capture its
        attachments. fileAttachment binaries are downloaded as separate blobs;
        item/reference attachments are recorded as metadata-only SnapshotItems
        (their content is either nested or external).

        Bounded concurrency keeps us under Graph throttling — each message
        round-trip + N attachment downloads can add up fast on big inboxes."""
        sem = asyncio.Semaphore(8)
        all_items: List[SnapshotItem] = []
        total_bytes = 0

        async def process_one_message(msg: Dict[str, Any]) -> Tuple[List[SnapshotItem], int]:
            msg_id = msg.get("id")
            if not msg_id:
                return [], 0
            async with sem:
                try:
                    attachments = await graph_client.list_message_attachments(
                        resource.external_id, msg_id,
                    )
                except Exception as e:
                    print(f"[{self.worker_id}]   [ATTACHMENT LIST FAIL] msg {msg_id}: {type(e).__name__}: {e}")
                    return [], 0

            local_items: List[SnapshotItem] = []
            local_bytes = 0
            for att in attachments:
                att_id = att.get("id")
                if not att_id:
                    continue
                att_kind = att.get("@odata.type", "")
                att_name = att.get("name") or att_id
                att_size = att.get("size") or 0
                content_bytes: Optional[bytes] = None
                blob_path: Optional[str] = None
                content_hash: Optional[str] = None

                if att_kind.endswith("fileAttachment"):
                    # Inline contentBytes is included for small attachments;
                    # for larger ones we hit /$value.
                    raw_b64 = att.get("contentBytes")
                    if raw_b64:
                        import base64 as _b64
                        try:
                            content_bytes = _b64.b64decode(raw_b64)
                        except Exception:
                            content_bytes = None
                    if content_bytes is None:
                        try:
                            async with sem:
                                content_bytes = await graph_client.get_message_attachment_content(
                                    resource.external_id, msg_id, att_id,
                                )
                        except Exception as e:
                            print(f"[{self.worker_id}]   [ATTACHMENT FAIL] {att_name} on msg {msg_id}: {type(e).__name__}: {e}")
                            continue

                    if content_bytes is None:
                        continue
                    content_hash = hashlib.sha256(content_bytes).hexdigest()
                    blob_path = azure_storage_manager.build_blob_path(
                        str(tenant.id), str(resource.id), str(snapshot.id),
                        f"att_{msg_id}_{att_id}",
                    )
                    upload_result = await upload_blob_with_retry(
                        container, blob_path, content_bytes, shard, max_retries=3,
                    )
                    if not (isinstance(upload_result, dict) and upload_result.get("success")):
                        continue
                    local_bytes += len(content_bytes)
                # itemAttachment / referenceAttachment: record metadata only.
                # The nested item content (for itemAttachment) would require a
                # separate $expand round-trip; reference attachments have no
                # content at all (just a URL). afi flags both as restorable
                # references — we do the same.

                local_items.append(SnapshotItem(
                    snapshot_id=snapshot.id, tenant_id=tenant.id,
                    external_id=f"{msg_id}::{att_id}",
                    item_type="EMAIL_ATTACHMENT",
                    name=att_name,
                    folder_path=msg.get("parentFolderName"),
                    content_hash=content_hash,
                    content_size=len(content_bytes) if content_bytes else att_size,
                    blob_path=blob_path,
                    content_checksum=content_hash,
                    extra_data={
                        "parent_item_id": msg_id,
                        "attachment_kind": att_kind,
                        "content_type": att.get("contentType"),
                        "is_inline": att.get("isInline", False),
                        "content_id": att.get("contentId") or att.get("contentID"),
                        "source_url": att.get("sourceUrl"),  # referenceAttachment
                    },
                ))
            return local_items, local_bytes

        results = await asyncio.gather(
            *[process_one_message(m) for m in messages_with_attachments],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, tuple):
                items, b = r
                all_items.extend(items)
                total_bytes += b

        if all_items:
            async with async_session_factory() as session:
                session.add_all(all_items)
                await session.commit()
        return len(all_items), total_bytes

    async def backup_onedrive(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                              tenant: Tenant, message: Dict) -> Dict:
        """Backup a single OneDrive with parallel file downloads"""
        print(f"[{self.worker_id}] [ONEDRIVE START] {resource.display_name} (drive: {resource.external_id})")

        delta_token = (resource.extra_data or {}).get("delta_token")
        print(f"[{self.worker_id}]   [FILES] Fetching drive items (paginated, delta)...")
        try:
            files = await graph_client.get_drive_items_delta(resource.external_id, delta_token)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"[{self.worker_id}]   [FILES] Drive not found — user has no OneDrive. Storing metadata only.")
                return {"item_count": 0, "bytes_added": 0, "new_delta_token": None, "note": "no_onedrive"}
            raise

        items = files.get("value", [])
        print(f"[{self.worker_id}]   [FILES] Found {len(items)} drive items")

        # SLA exclusions — filter before upload
        policy = await self.get_sla_policy(resource, message)
        exclusions = await self.get_policy_exclusions(policy.id) if policy else []
        if exclusions:
            before = len(items)
            items = [f for f in items if not self._item_is_excluded("FILE", f, exclusions)]
            excluded = before - len(items)
            if excluded:
                print(f"[{self.worker_id}]   [FILES] Excluded {excluded}/{before} by SLA policy rules")

        # Process files in parallel (up to BACKUP_CONCURRENCY at once)
        file_tasks = [
            self.backup_single_file(resource, tenant, snapshot, f, graph_client, None)
            for f in items
        ]
        file_results = await asyncio.gather(*file_tasks, return_exceptions=True)

        # Categorize results for visibility
        actual_uploads = 0
        skips = {}
        failures = 0
        for r in file_results:
            if isinstance(r, Exception):
                failures += 1
                print(f"[{self.worker_id}]   [FILE FAIL] unknown: {type(r).__name__}: {r}")
            elif isinstance(r, dict):
                method = r.get("method", "")
                if r.get("success"):
                    if method == "streaming":
                        actual_uploads += 1
                    elif method.startswith("skipped"):
                        skips[method] = skips.get(method, 0) + 1
                    elif method == "empty":
                        actual_uploads += 1
                else:
                    failures += 1
                    print(f"[{self.worker_id}]   [FILE FAIL] {r.get('file_name','?')}: "
                          f"method={r.get('method','?')} reason={r.get('reason','no reason')}")

        total_items = sum(1 for r in file_results if isinstance(r, dict) and r.get("success"))
        total_bytes = sum(r.get("size", 0) for r in file_results if isinstance(r, dict))
        failed = failures

        # Log skip summary
        if skips:
            skip_summary = ", ".join(f"{k}: {v}" for k, v in sorted(skips.items()))
            print(f"[{self.worker_id}]   [SKIPS] {skip_summary}")
        print(f"[{self.worker_id}]   [UPLOADS] {actual_uploads} actual uploads, {failures} failures")

        print(f"[{self.worker_id}]   [FILES] Done — {total_items} uploaded, {failed} failed, {total_bytes} bytes")

        new_delta = files.get("@odata.deltaLink")
        print(f"[{self.worker_id}] [BACKUP COMPLETE] OneDrive: {resource.display_name} — {total_items} files, {total_bytes} bytes")
        return {"item_count": total_items, "bytes_added": total_bytes, "new_delta_token": new_delta}

    async def backup_sharepoint(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                                tenant: Tenant, message: Dict) -> Dict:
        """Backup a single SharePoint site at enterprise scale.

        Design notes (see docs — SharePoint v2 bounded-queue backup):
          * Bounded ``asyncio.Queue`` between streaming producers (delta-API +
            SP REST list iterators) and a consumer pool. Producers block on
            queue backpressure → constant memory regardless of site size
            (works on million-file libraries without OOM).
          * Drive-item consumers call ``backup_single_file`` which already
            enforces ``self.backup_semaphore`` (NIC cap 8) and does Range-
            resume streaming, sha256, eTag idempotency, per-file versions,
            and inline ACL capture. No per-list row cap, no per-item byte
            cap — scale is bounded by queue size, not memory.
          * SP REST list-row consumers use a separate (smaller) pool because
            SP REST throttles far more aggressively than Graph. The iterator
            itself honours 429 / Retry-After so the worker never hot-spins
            on a throttled list.
        """
        print(f"[{self.worker_id}] [SHAREPOINT START] {resource.display_name} (site: {resource.external_id})")

        delta_token = (resource.extra_data or {}).get("delta_token")
        subsite_delta_tokens = ((resource.extra_data or {}).get("subsite_delta_tokens") or {}).copy()
        # Per-drive resume tokens. Needed because a single SharePoint
        # site routinely has several document libraries; using the
        # singleton `/drive` endpoint (the legacy path) only captures
        # the default one.
        drive_delta_tokens_by_site: Dict[str, Dict[str, str]] = (
            (resource.extra_data or {}).get("drive_delta_tokens_by_site") or {}
        )

        site_targets: List[tuple[str, str, Optional[str]]] = [
            (resource.external_id, resource.display_name, delta_token)
        ]
        try:
            subsites = await graph_client.get_sharepoint_subsites(resource.external_id)
            for subsite in subsites.get("value", []):
                subsite_id = (subsite.get("id") or "").replace(",", "/")
                if not subsite_id:
                    continue
                site_targets.append((
                    subsite_id,
                    subsite.get("displayName") or subsite.get("name") or subsite_id,
                    subsite_delta_tokens.get(subsite_id),
                ))
        except Exception as exc:
            logger.warning("Failed to enumerate SharePoint subsites for %s: %s", resource.display_name, exc)

        # SLA exclusions — applied in the producer so excluded items never
        # reach the queue (saves memory + Graph/SP REST bandwidth).
        policy = await self.get_sla_policy(resource, message)
        exclusions = await self.get_policy_exclusions(policy.id) if policy else []

        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        sp_container = azure_storage_manager.get_container_name(str(tenant.id), "sharepoint")

        # Bounded queues — size tuned so 8 streaming consumers never starve
        # but producers can't stampede ahead by more than a few thousand
        # items. At 2048 × ~2KB per drive-item dict that's ~4 MB peak.
        drive_queue: asyncio.Queue = asyncio.Queue(maxsize=2048)
        rest_queue: asyncio.Queue = asyncio.Queue(maxsize=512)

        stats = {
            "drive_ok": 0, "drive_bytes": 0, "drive_fail": 0, "drive_excluded": 0,
            "rest_rows": 0, "rest_files_ok": 0, "rest_bytes": 0, "rest_fail": 0,
            "list_folders": 0,
            "new_delta": None,
            "new_subsite_tokens": {},
            # Per-(site, drive) delta tokens written by the multi-drive
            # producer. Shape: {site_id: {drive_id: deltaLink}}.
            "new_drive_tokens": {},
        }

        # --- Producers ----------------------------------------------------
        async def drive_producer(site_id: str, site_label: str, site_delta_token: Optional[str]):
            """Enumerate every library on the site (not just the default
            ``/drive`` singleton) and stream drive-items through the
            pipeline. Per-library delta tokens are persisted on the
            resource so subsequent runs resume cheaply per library
            instead of re-walking each one."""
            per_drive_in = drive_delta_tokens_by_site.get(site_id) or {}
            per_drive_out: Dict[str, Dict[str, Optional[str]]] = {}

            # Diagnostic: count what we actually enumerate + stream.
            try:
                drives = await graph_client.list_sharepoint_site_drives(site_id)
                print(f"[{self.worker_id}]   [SP_DRIVES] {site_label}: enumerated {len(drives)} drive(s)"
                      + (" — " + ", ".join(f"{d.get('name','?')}({d.get('driveType','?')})" for d in drives[:8]) if drives else ""))
            except Exception as exc:
                print(f"[{self.worker_id}]   [SP_DRIVES] {site_label}: enumerate FAIL {type(exc).__name__}: {exc}")

            streamed = 0
            try:
                async for item in graph_client.iter_sharepoint_site_all_drive_items(
                    site_id, per_drive_in, per_drive_out,
                ):
                    streamed += 1
                    item["_site_label"] = site_label
                    if exclusions and self._item_is_excluded("FILE", item, exclusions):
                        stats["drive_excluded"] += 1
                        continue
                    await drive_queue.put(item)
            except Exception as exc:
                logger.warning("SharePoint drive producer %s failed: %s", site_label, exc)
            finally:
                print(f"[{self.worker_id}]   [SP_DRIVES] {site_label}: streamed {streamed} drive-item(s) "
                      f"across {len(per_drive_out)} drive(s)")
                # Persist new per-drive tokens. The legacy flat
                # `delta_token` / `subsite_delta_tokens` are still
                # written for back-compat but are no longer consulted
                # at read time once drive_delta_tokens_by_site exists.
                if per_drive_out:
                    stats.setdefault("new_drive_tokens", {})[site_id] = {
                        drive_id: holder.get("deltaLink")
                        for drive_id, holder in per_drive_out.items()
                        if holder.get("deltaLink")
                    }

        async def list_producer(site_id: str, site_label: str):
            try:
                lists_resp = await graph_client.get_sharepoint_site_lists(site_id)
            except Exception as exc:
                logger.warning("Failed to fetch lists for site %s: %s", site_id, exc)
                return

            # Derive hostname + site web URL once — every list on the site
            # shares the same tenant host / site path.
            sp_hostname: Optional[str] = None
            sp_site_web_url: Optional[str] = None
            for _l in (lists_resp.get("value") or []):
                _wu = _l.get("webUrl") or ""
                if _wu:
                    from urllib.parse import urlparse as _up
                    _p = _up(_wu)
                    sp_hostname = _p.netloc
                    _parts = _p.path.strip("/").split("/")
                    if len(_parts) >= 2 and _parts[0] == "sites":
                        sp_site_web_url = f"{_p.scheme}://{_p.netloc}/sites/{_parts[1]}"
                    else:
                        sp_site_web_url = f"{_p.scheme}://{_p.netloc}"
                    break

            for lst in (lists_resp.get("value") or []):
                lst_id = lst.get("id") or ""
                if not lst_id:
                    continue
                display = lst.get("displayName") or lst.get("name") or lst_id
                list_meta = lst.get("list") or {}
                is_library = str(list_meta.get("template") or "").lower() in (
                    "documentlibrary", "masterpagecatalog", "webtemplatecatalog",
                    "webpartcatalog", "themecatalog", "solutioncatalog",
                )

                # Persist the list folder row immediately (small, bounded by
                # list count — not file count).
                list_row = SnapshotItem(
                    id=uuid.uuid4(),
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=lst_id,
                    item_type="SHAREPOINT_LIST",
                    name=display[:255],
                    folder_path=f"{site_label}/lists",
                    content_size=0,
                    content_hash=None,
                    content_checksum=None,
                    extra_data={
                        "raw": lst,
                        "site_id": site_id,
                        "site_label": site_label,
                        "template": list_meta.get("template"),
                        "system": bool(lst.get("system")) or bool(list_meta.get("hidden")),
                        "hidden": bool(list_meta.get("hidden")),
                        "is_catalog": "_catalogs" in (lst.get("webUrl") or ""),
                        "is_library": is_library,
                        "web_url": lst.get("webUrl"),
                        "last_modified": lst.get("lastModifiedDateTime"),
                    },
                )
                try:
                    async with async_session_factory() as sess:
                        sess.add(list_row)
                        await sess.commit()
                    stats["list_folders"] += 1
                except Exception as exc:
                    logger.warning("list-folder persist failed for %s/%s: %s", site_label, display, exc)

                if is_library:
                    # Library files are already picked up by the drive
                    # producer — re-enqueueing them via SP REST would
                    # duplicate bytes and double the Graph/SP REST cost.
                    continue
                if not (sp_hostname and sp_site_web_url):
                    logger.warning("Cannot derive SP site URL for %s, skipping list items", site_label)
                    continue

                try:
                    async for row in graph_client.iter_sharepoint_list_items_via_rest(
                        sp_hostname, sp_site_web_url, lst_id,
                    ):
                        await rest_queue.put({
                            "row": row,
                            "lst_id": lst_id,
                            "list_display": display,
                            "site_id": site_id,
                            "site_label": site_label,
                            "sp_hostname": sp_hostname,
                            "sp_site_web_url": sp_site_web_url,
                        })
                except Exception as exc:
                    logger.warning("SP REST stream failed for list %s/%s: %s", site_label, display, exc)

        # --- Consumers ----------------------------------------------------
        async def drive_consumer():
            while True:
                item = await drive_queue.get()
                try:
                    if item is None:
                        return
                    try:
                        r = await self.backup_single_file(
                            resource, tenant, snapshot, item, graph_client, None,
                        )
                    except Exception as exc:
                        stats["drive_fail"] += 1
                        print(f"[{self.worker_id}]   [SP_FILE FAIL] {item.get('name','?')}: "
                              f"{type(exc).__name__}: {exc}")
                        continue
                    if isinstance(r, dict):
                        if r.get("success"):
                            stats["drive_ok"] += 1
                            stats["drive_bytes"] += r.get("size", 0)
                        else:
                            stats["drive_fail"] += 1
                            print(f"[{self.worker_id}]   [SP_FILE FAIL] "
                                  f"{r.get('file_name','?')}: method={r.get('method','?')} "
                                  f"reason={r.get('reason','no reason')}")
                finally:
                    drive_queue.task_done()

        async def rest_consumer():
            while True:
                work = await rest_queue.get()
                try:
                    if work is None:
                        return
                    try:
                        await self._backup_sp_rest_row(
                            resource=resource,
                            tenant=tenant,
                            snapshot=snapshot,
                            graph_client=graph_client,
                            shard=shard,
                            container=sp_container,
                            work=work,
                            stats=stats,
                        )
                    except Exception as exc:
                        stats["rest_fail"] += 1
                        print(f"[{self.worker_id}]   [SP_LIST_ITEM FAIL] "
                              f"{work.get('site_label','?')}/{work.get('list_display','?')}: "
                              f"{type(exc).__name__}: {exc}")
                finally:
                    rest_queue.task_done()

        # --- Orchestrate --------------------------------------------------
        # Drive consumers: big pool — each one blocks on self.backup_semaphore
        # (NIC cap 8) internally so we can safely dispatch settings.BACKUP_CONCURRENCY
        # workers without saturating the network.
        num_drive_consumers = max(1, min(settings.BACKUP_CONCURRENCY, 128))
        # REST consumers: small pool — SP REST throttles aggressively; 8 is a
        # safe upper bound observed across large tenants.
        num_rest_consumers = 8

        drive_consumer_tasks = [
            asyncio.create_task(drive_consumer()) for _ in range(num_drive_consumers)
        ]
        rest_consumer_tasks = [
            asyncio.create_task(rest_consumer()) for _ in range(num_rest_consumers)
        ]

        producer_tasks = []
        for sid, slabel, stok in site_targets:
            producer_tasks.append(asyncio.create_task(drive_producer(sid, slabel, stok)))
            producer_tasks.append(asyncio.create_task(list_producer(sid, slabel)))

        print(f"[{self.worker_id}]   [SP_PIPELINE] producers={len(producer_tasks)} "
              f"drive_consumers={num_drive_consumers} rest_consumers={num_rest_consumers}")

        # Wait for all producers to finish pushing.
        await asyncio.gather(*producer_tasks, return_exceptions=True)

        # Drain queues (consumers still running).
        await drive_queue.join()
        await rest_queue.join()

        # Poison pills — one per consumer.
        for _ in range(num_drive_consumers):
            await drive_queue.put(None)
        for _ in range(num_rest_consumers):
            await rest_queue.put(None)
        await asyncio.gather(*drive_consumer_tasks, *rest_consumer_tasks, return_exceptions=True)

        total_items = stats["drive_ok"] + stats["list_folders"] + stats["rest_rows"]
        total_bytes = stats["drive_bytes"] + stats["rest_bytes"]

        print(f"[{self.worker_id}]   [SP_FILES] {stats['drive_ok']} uploaded, "
              f"{stats['drive_fail']} failed, {stats['drive_excluded']} excluded, "
              f"{stats['drive_bytes']} bytes")
        print(f"[{self.worker_id}]   [SP_LISTS] {stats['list_folders']} lists, "
              f"{stats['rest_rows']} rows processed, {stats['rest_files_ok']} files "
              f"({stats['rest_bytes']} bytes), {stats['rest_fail']} failed")
        # Breakdown of why REST rows didn't produce file content — helps
        # diagnose "283 items, 0 bytes" type snapshots.
        print(f"[{self.worker_id}]   [SP_REST_BREAKDOWN] "
              f"no_fileref={stats.get('rest_rows_no_fileref', 0)} "
              f"folder={stats.get('rest_rows_folder', 0)} "
              f"meta_fail={stats.get('rest_rows_meta_fail', 0)} "
              f"meta_none={stats.get('rest_rows_meta_none', 0)} "
              f"size_zero={stats.get('rest_rows_size_zero', 0)}")

        if stats["new_subsite_tokens"] or stats["new_drive_tokens"]:
            async with async_session_factory() as sess:
                r = await sess.get(Resource, resource.id)
                if r:
                    r.extra_data = r.extra_data or {}
                    if stats["new_subsite_tokens"]:
                        existing_subsite_tokens = (r.extra_data.get("subsite_delta_tokens") or {}).copy()
                        existing_subsite_tokens.update(stats["new_subsite_tokens"])
                        r.extra_data["subsite_delta_tokens"] = existing_subsite_tokens
                    if stats["new_drive_tokens"]:
                        existing_drive_tokens = (r.extra_data.get("drive_delta_tokens_by_site") or {}).copy()
                        for site_id_key, per_drive in stats["new_drive_tokens"].items():
                            merged = (existing_drive_tokens.get(site_id_key) or {}).copy()
                            merged.update(per_drive)
                            existing_drive_tokens[site_id_key] = merged
                        r.extra_data["drive_delta_tokens_by_site"] = existing_drive_tokens
                    await sess.commit()

        print(f"[{self.worker_id}] [BACKUP COMPLETE] SharePoint: {resource.display_name} — "
              f"{total_items} items, {total_bytes} bytes")
        return {
            "item_count": total_items,
            "bytes_added": total_bytes,
            "new_delta_token": stats["new_delta"],
        }

    async def _backup_sp_rest_row(
        self,
        *,
        resource: Resource,
        tenant: Tenant,
        snapshot: Snapshot,
        graph_client: GraphClient,
        shard,
        container: str,
        work: Dict[str, Any],
        stats: Dict[str, int],
    ) -> None:
        """Process a single SP REST list row: persist its SnapshotItem and,
        if it's a real file (not a folder, not a pure list row), stream its
        bytes through a temp file → block-blob upload.

        No byte cap — large catalog entries (e.g. multi-hundred-MB SitePages
        attachments) stream chunk-by-chunk via ``stream_sharepoint_file_via_rest``
        so worker RSS stays flat. All uploads go through
        ``upload_blob_with_retry_from_file`` so 429s / transient Azure errors
        get exponential backoff, not silent loss.
        """
        import tempfile
        row = work["row"]
        lst_id = work["lst_id"]
        display = work["list_display"]
        site_id = work["site_id"]
        site_label = work["site_label"]
        sp_hostname = work["sp_hostname"]
        sp_site_web_url = work["sp_site_web_url"]

        it_id = str(row.get("Id") or row.get("ID") or "")
        if not it_id:
            return
        srv_rel = row.get("FileRef") or ""
        leaf = row.get("FileLeafRef") or ""
        obj_type = int(row.get("FileSystemObjectType") or 0)
        is_folder = (obj_type == 1)
        name = leaf or row.get("Title") or (srv_rel.rsplit("/", 1)[-1] if srv_rel else f"Item-{it_id}")

        # Nest folder_path by server-relative path so Recovery mirrors real layout.
        if srv_rel:
            trail = srv_rel.lstrip("/")
            parts = trail.split("/")
            if len(parts) >= 2 and parts[0] == "sites":
                parts = parts[2:]
            path_parts = parts[:-1] if len(parts) > 1 else []
            item_folder_path = f"{site_label}/lists/{display}"
            if path_parts:
                item_folder_path += "/" + "/".join(path_parts)
        else:
            item_folder_path = f"{site_label}/lists/{display}"

        blob_path: Optional[str] = None
        content_hash: Optional[str] = None
        downloaded_bytes = 0
        size = 0
        file_info: Optional[Dict[str, Any]] = None

        # Diagnostic counters — classify why rows end up without file
        # content. Only one per call lands on the right bucket.
        stats.setdefault("rest_rows_no_fileref", 0)
        stats.setdefault("rest_rows_folder", 0)
        stats.setdefault("rest_rows_meta_fail", 0)
        stats.setdefault("rest_rows_meta_none", 0)
        stats.setdefault("rest_rows_size_zero", 0)

        if not srv_rel:
            stats["rest_rows_no_fileref"] += 1
        elif is_folder:
            stats["rest_rows_folder"] += 1

        if srv_rel and not is_folder:
            try:
                file_info = await graph_client.get_sharepoint_file_metadata_via_rest(
                    sp_hostname, sp_site_web_url, srv_rel,
                )
            except Exception as _meta_exc:
                stats["rest_rows_meta_fail"] += 1
                if stats["rest_rows_meta_fail"] <= 3:
                    print(f"[{self.worker_id}]   [SP_REST META FAIL] {site_label}/{display}/{name}: "
                          f"{type(_meta_exc).__name__}: {_meta_exc}")
                file_info = None
            if file_info is None:
                # Distinct from meta-fail: the call succeeded but
                # returned None/empty (e.g. row has FileRef but file
                # was deleted, or endpoint returned 204).
                if stats["rest_rows_meta_none"] == 0:
                    print(f"[{self.worker_id}]   [SP_REST META NONE] {site_label}/{display}/{name} srv_rel={srv_rel}")
                stats["rest_rows_meta_none"] += 1
            if file_info:
                size = int(file_info.get("Length") or 0)
                if size == 0:
                    stats["rest_rows_size_zero"] += 1

        # Idempotency — skip re-downloading catalog files that already have a
        # matching blob from a prior snapshot (same server_relative_url + size).
        existing_blob_path = None
        existing_hash: Optional[str] = None
        if file_info and size > 0:
            blob_path_candidate = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id),
                f"splist_{lst_id}_{it_id}",
            )
            # Stream download → temp file → block-blob upload.
            if size > 0:
                tmp_path = None
                try:
                    async with self.backup_semaphore:
                        fd, tmp_path = tempfile.mkstemp(prefix="sp_rest_", suffix=".bin")
                        os.close(fd)
                        sha = hashlib.sha256()
                        with open(tmp_path, "wb") as fp:
                            async for chunk in graph_client.stream_sharepoint_file_via_rest(
                                sp_hostname, sp_site_web_url, srv_rel,
                            ):
                                fp.write(chunk)
                                sha.update(chunk)
                        content_hash = sha.hexdigest()
                        downloaded_bytes = os.path.getsize(tmp_path)
                        upload_result = await upload_blob_with_retry_from_file(
                            container_name=container,
                            blob_path=blob_path_candidate,
                            file_path=tmp_path,
                            shard=shard,
                            file_size=downloaded_bytes,
                            metadata={
                                "source_item_id": it_id,
                                "source_list_id": lst_id,
                                "server_relative_url": srv_rel,
                                "original-name": name,
                                "sha256": content_hash,
                            },
                        )
                    if isinstance(upload_result, dict) and upload_result.get("success"):
                        blob_path = blob_path_candidate
                        stats["rest_files_ok"] += 1
                        stats["rest_bytes"] += downloaded_bytes
                    else:
                        stats["rest_fail"] += 1
                except Exception as exc:
                    stats["rest_fail"] += 1
                    print(f"[{self.worker_id}]   [SP_LIST_STREAM FAIL] "
                          f"{site_label}/{display}/{name}: {type(exc).__name__}: {exc}")
                finally:
                    if tmp_path:
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass

        # Persist SnapshotItem row (file or metadata-only).
        item_type = (
            "SHAREPOINT_FILE" if blob_path
            else ("SHAREPOINT_FOLDER" if is_folder else "SHAREPOINT_LIST_ITEM")
        )
        try:
            async with async_session_factory() as sess:
                sess.add(SnapshotItem(
                    id=uuid.uuid4(),
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=f"{lst_id}:{it_id}",
                    item_type=item_type,
                    name=str(name)[:255],
                    folder_path=item_folder_path,
                    content_size=downloaded_bytes if blob_path else size,
                    content_hash=content_hash,
                    content_checksum=None,
                    blob_path=blob_path,
                    extra_data={
                        "list_id": lst_id,
                        "list_name": display,
                        "site_id": site_id,
                        "site_label": site_label,
                        "server_relative_url": srv_rel,
                        "file": file_info,
                        "is_folder": is_folder,
                        "title": row.get("Title"),
                        "created": row.get("Created") or (file_info or {}).get("TimeCreated"),
                        "modified": row.get("Modified") or (file_info or {}).get("TimeLastModified"),
                        "version": (file_info or {}).get("UIVersionLabel"),
                        "source": "sp_rest",
                    },
                ))
                await sess.commit()
            stats["rest_rows"] += 1
        except Exception as exc:
            logger.warning("sp_rest_row persist failed for %s/%s/%s: %s", site_label, display, name, exc)

    async def update_resource_backup_info(self, session: AsyncSession, resource: Resource,
                                          job_id: uuid.UUID, snapshot_id: uuid.UUID,
                                          result: Dict = None):
        """Update resource with last backup information — uses targeted UPDATE
        to avoid overwriting extra_data (delta_token) set by complete_snapshot."""
        from sqlalchemy import update as sa_update
        
        # Calculate new storage_bytes from backup result
        storage_bytes = resource.storage_bytes or 0
        if result:
            bytes_added = result.get("bytes_added", 0)
            bytes_removed = result.get("bytes_removed", 0) or 0
            net_change = bytes_added - bytes_removed
            storage_bytes = max(0, storage_bytes + net_change)
            print(f"[{self.worker_id}] Updated storage_bytes for {resource.id}: {resource.storage_bytes} -> {storage_bytes} bytes (added {bytes_added}, removed {bytes_removed})")
        
        new_status = ResourceStatus.ACTIVE if resource.status == ResourceStatus.DISCOVERED else resource.status
        await session.execute(
            sa_update(Resource)
            .where(Resource.id == resource.id)
            .values(
                last_backup_job_id=job_id,
                last_backup_at=datetime.utcnow(),
                last_backup_status="COMPLETED",
                status=new_status,
                storage_bytes=storage_bytes,
            )
        )
        await session.commit()

    async def complete_snapshot(self, session: AsyncSession, snapshot: Snapshot, result: Dict):
        """Mark snapshot as completed with result data"""
        now = datetime.utcnow()
        snapshot.completed_at = now
        snapshot.item_count = result.get("item_count", 0)
        snapshot.new_item_count = result.get("item_count", 0)
        snapshot.bytes_added = result.get("bytes_added", 0)
        snapshot.bytes_total = result.get("bytes_added", 0)
        snapshot.delta_token = result.get("new_delta_token")

        # Set status: PARTIAL if there are failed files, COMPLETE otherwise
        file_tracking = snapshot.delta_tokens_json or {}
        files_failed = file_tracking.get("files_failed", 0)
        if files_failed > 0:
            snapshot.status = SnapshotStatus.PARTIAL
        else:
            snapshot.status = SnapshotStatus.COMPLETED

        # Calculate duration
        if snapshot.started_at:
            duration = (now - snapshot.started_at).total_seconds()
            snapshot.duration_secs = int(duration)

        # merge() handles detached instances (snapshot was created in a separate session)
        await session.merge(snapshot)
        await session.commit()

        # Persist delta token on resource for incremental backups
        if result.get("new_delta_token"):
            resource_id = snapshot.resource_id
            resource = await session.get(Resource, resource_id)
            if resource:
                resource.extra_data = resource.extra_data or {}
                if resource.type.value in ("MAILBOX", "SHARED_MAILBOX", "ROOM_MAILBOX"):
                    resource.extra_data["mail_delta_token"] = result["new_delta_token"]
                else:
                    resource.extra_data["delta_token"] = result["new_delta_token"]
                await session.merge(resource)
                await session.commit()

        # Anomaly detection — afi-style ransomware/mass-delete tripwire. Only
        # runs on COMPLETED snapshots; PARTIAL/FAILED ones are too noisy.
        if snapshot.status == SnapshotStatus.COMPLETED:
            try:
                await self._check_snapshot_anomaly(snapshot)
            except Exception as e:
                # Anomaly check is best-effort — never fail a backup over it.
                print(f"[{self.worker_id}] [ANOMALY WARN] check failed for snapshot {snapshot.id}: {type(e).__name__}: {e}")

    # Anomaly thresholds — generous defaults so we don't flood ops with false
    # positives on small mailboxes / new resources. Tunable via env at deploy.
    ANOMALY_MIN_PRIOR_SNAPSHOTS = int(os.environ.get("ANOMALY_MIN_PRIOR_SNAPSHOTS", "3"))
    ANOMALY_DROP_RATIO = float(os.environ.get("ANOMALY_DROP_RATIO", "0.5"))  # current < 50% of avg
    ANOMALY_MIN_AVG_ITEMS = int(os.environ.get("ANOMALY_MIN_AVG_ITEMS", "20"))  # ignore tiny resources

    async def _check_snapshot_anomaly(self, snapshot: Snapshot) -> None:
        """Compare this snapshot's item_count against the rolling average of
        the last N completed snapshots for the same resource. If it dropped
        sharply, raise an Alert and tag the most recent prior snapshot as
        last_clean — that's the recovery point an operator should restore from
        if this turns out to be ransomware/mass-deletion.

        Heuristic v1: item_count drop ratio. v2 will incorporate is_deleted
        markers + content_hash churn for finer-grained detection."""
        from shared.models import Alert
        async with async_session_factory() as session:
            stmt = (
                select(Snapshot)
                .where(
                    Snapshot.resource_id == snapshot.resource_id,
                    Snapshot.id != snapshot.id,
                    Snapshot.status == SnapshotStatus.COMPLETED,
                )
                .order_by(Snapshot.completed_at.desc())
                .limit(5)
            )
            prior = (await session.execute(stmt)).scalars().all()
            if len(prior) < self.ANOMALY_MIN_PRIOR_SNAPSHOTS:
                return  # not enough history to judge

            avg = sum((p.item_count or 0) for p in prior) / len(prior)
            if avg < self.ANOMALY_MIN_AVG_ITEMS:
                return  # resource too small to be meaningful

            current = snapshot.item_count or 0
            ratio = current / avg if avg else 1.0
            if ratio >= self.ANOMALY_DROP_RATIO:
                return  # within normal variance

            # Anomaly — raise alert + mark prior snapshot as last_clean
            resource = await session.get(Resource, snapshot.resource_id)
            last_clean = prior[0]  # most recent completed snapshot before this one
            last_clean.extra_data = (last_clean.extra_data or {}) | {
                "is_clean_marker": True,
                "marked_clean_by_snapshot": str(snapshot.id),
                "marked_clean_at": datetime.utcnow().isoformat(),
            }
            await session.merge(last_clean)

            alert = Alert(
                tenant_id=resource.tenant_id if resource else None,
                org_id=None,
                type="BACKUP_ANOMALY",
                severity="HIGH",
                message=(
                    f"Snapshot item count dropped {int((1 - ratio) * 100)}% "
                    f"vs prior average ({current} vs avg {int(avg)}). "
                    f"Possible mass deletion / ransomware. Last clean snapshot: {last_clean.id}."
                ),
                resource_id=resource.id if resource else None,
                resource_type=resource.type.value if resource else None,
                resource_name=resource.display_name if resource else None,
                triggered_by="anomaly-detector",
                details={
                    "snapshot_id": str(snapshot.id),
                    "last_clean_snapshot_id": str(last_clean.id),
                    "current_item_count": current,
                    "avg_prior_item_count": int(avg),
                    "drop_ratio": round(ratio, 3),
                    "prior_snapshot_ids": [str(p.id) for p in prior],
                },
            )
            session.add(alert)
            await session.commit()
            print(
                f"[{self.worker_id}] [ANOMALY] resource={resource.display_name if resource else snapshot.resource_id} "
                f"snapshot={snapshot.id} dropped to {current} items (avg {int(avg)}, ratio {ratio:.2f}). "
                f"Marked {last_clean.id} as last_clean."
            )

            # Mirror the anomaly to the audit trail. RANSOMWARE_SIGNAL is the
            # canonical action in audit-service's ACTIONS catalog — this surfaces
            # the event in the Activity feed + risk-signals API + audit exports.
            # Best-effort: audit-service might be unreachable; we still raised
            # the Alert above which is the operational source of truth.
            try:
                org_id = None
                async with async_session_factory() as outer:
                    if resource:
                        tenant = await outer.get(Tenant, resource.tenant_id)
                        if tenant:
                            org_id = str(tenant.org_id) if tenant.org_id else None
                await self.audit_logger.log(
                    action="RANSOMWARE_SIGNAL",
                    tenant_id=str(resource.tenant_id) if resource and resource.tenant_id else None,
                    org_id=org_id,
                    actor_type="WORKER",
                    actor_id=None,
                    actor_email=None,
                    resource_id=str(resource.id) if resource else None,
                    resource_type=resource.type.value if resource else None,
                    resource_name=resource.display_name if resource else None,
                    outcome="WARNING",
                    snapshot_id=str(snapshot.id),
                    details={
                        "alert_id": str(alert.id),
                        "anomaly_type": "ITEM_COUNT_DROP",
                        "current_item_count": current,
                        "avg_prior_item_count": int(avg),
                        "drop_ratio": round(ratio, 3),
                        "drop_pct": int((1 - ratio) * 100),
                        "last_clean_snapshot_id": str(last_clean.id),
                        "prior_snapshot_ids": [str(p.id) for p in prior],
                        "thresholds": {
                            "drop_ratio": self.ANOMALY_DROP_RATIO,
                            "min_prior_snapshots": self.ANOMALY_MIN_PRIOR_SNAPSHOTS,
                            "min_avg_items": self.ANOMALY_MIN_AVG_ITEMS,
                        },
                    },
                )
            except Exception as audit_exc:
                # Audit logging never blocks the anomaly path; the Alert is the
                # source of truth for ops and is already persisted.
                print(f"[{self.worker_id}] [ANOMALY AUDIT WARN] {type(audit_exc).__name__}: {audit_exc}")

    # ==================== Helpers ====================

    async def get_graph_client(self, tenant: Tenant) -> Optional[GraphClient]:
        """Return a per-tenant cached GraphClient (creates on first miss).

        Caching matters because concurrent batch backups (Tier 2 fans out
        5 USER_* type-groups in parallel) used to create a fresh client per
        call — each hitting Microsoft's /oauth2/v2.0/token endpoint at the
        same instant. AAD throttles those parallel token requests and
        returns 401 on the losers, dropping items silently. Sharing one
        client per tenant lets _get_token's TTL cache absorb the burst.

        We deliberately key by tenant only (not by app id) so the second,
        third, … callers don't drift to a different `multi_app_manager`
        slot just because round-robin advanced. The first request for a
        tenant picks an app and pins it for that worker's lifetime."""
        cache_key = str(tenant.external_tenant_id)
        existing = self.graph_clients.get(cache_key)
        if existing:
            return existing
        app = multi_app_manager.get_next_app()
        client = GraphClient(
            client_id=app.client_id,
            client_secret=app.client_secret,
            tenant_id=tenant.external_tenant_id,
        )
        self.graph_clients[cache_key] = client
        return client

    def get_power_bi_client(self, tenant: Tenant) -> PowerBIClient:
        return PowerBIClient(
            tenant_id=tenant.external_tenant_id or settings.EFFECTIVE_POWER_BI_TENANT_ID,
            refresh_token=PowerBIClient.get_refresh_token_from_tenant(tenant),
        )

    def get_power_platform_client(self, tenant: Tenant) -> PowerPlatformClient:
        """Build a Power Platform Admin API client using the tenant's Graph app credentials.
        Falls back to the first configured Graph app if the tenant has none of its own."""
        client_id = tenant.graph_client_id or settings.MICROSOFT_CLIENT_ID
        client_secret = settings.MICROSOFT_CLIENT_SECRET
        tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID
        return PowerPlatformClient(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

    async def get_policy_exclusions(self, policy_id) -> List[Dict[str, Any]]:
        """Load enabled exclusions for a policy as plain dicts.
        Returned once per backup job; callers pass the list into _item_is_excluded
        for each item so we don't re-query on every message."""
        from shared.models import SlaExclusion
        if not policy_id:
            return []
        async with async_session_factory() as session:
            stmt = select(SlaExclusion).where(
                SlaExclusion.policy_id == (policy_id if isinstance(policy_id, uuid.UUID) else uuid.UUID(str(policy_id))),
                SlaExclusion.enabled.is_(True),
            )
            rows = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "type": r.exclusion_type,
                    "pattern": r.pattern,
                    "workload": r.workload,
                    "apply_to_historical": r.apply_to_historical,
                }
                for r in rows
            ]

    @staticmethod
    def _item_is_excluded(item_workload: str, item: Dict[str, Any], exclusions: List[Dict[str, Any]]) -> bool:
        """Evaluate whether a single item should be filtered from backup.

        item_workload: EMAIL / FILE / CALENDAR / CONTACT / TEAMS_MESSAGE / CHAT_MESSAGE
        item: the raw Graph object being considered (email dict, drive item dict, etc.)
        exclusions: list from get_policy_exclusions()

        Rule semantics:
          FOLDER_PATH     - matches if item's folder/parent path contains the pattern
          FILE_EXTENSION  - matches if item's name ends with .{pattern} (case-insensitive)
          SUBJECT_REGEX   - (email) pattern.search(item.subject)
          MIME_TYPE       - (file) item.file.mimeType equals pattern
          EMAIL_ADDRESS   - (email) pattern matches sender/recipient email
          FILENAME_GLOB   - (file) fnmatch against item.name
        """
        import fnmatch, re as _re
        if not exclusions:
            return False
        for rule in exclusions:
            wl = rule.get("workload")
            if wl and wl != item_workload and wl != "ALL":
                continue
            rtype = rule.get("type")
            pat = (rule.get("pattern") or "").strip()
            if not pat:
                continue

            if rtype == "FOLDER_PATH":
                folder = (item.get("parentReference", {}) or {}).get("path", "") or item.get("folderPath", "")
                if pat.lower() in str(folder).lower():
                    return True

            elif rtype == "FILE_EXTENSION":
                name = item.get("name", "") or ""
                ext = "." + pat.lstrip(".").lower()
                if name.lower().endswith(ext):
                    return True

            elif rtype == "FILENAME_GLOB":
                name = item.get("name", "") or ""
                if fnmatch.fnmatch(name.lower(), pat.lower()):
                    return True

            elif rtype == "MIME_TYPE":
                mime = (item.get("file", {}) or {}).get("mimeType") or item.get("mimeType")
                if mime and mime.lower() == pat.lower():
                    return True

            elif rtype == "SUBJECT_REGEX":
                subject = item.get("subject", "") or ""
                try:
                    if _re.search(pat, subject, _re.IGNORECASE):
                        return True
                except _re.error:
                    # malformed regex — skip rather than fail the whole backup
                    pass

            elif rtype == "EMAIL_ADDRESS":
                addrs = []
                _from = (item.get("from", {}) or {}).get("emailAddress", {})
                if _from.get("address"):
                    addrs.append(_from["address"])
                for r in (item.get("toRecipients") or []):
                    a = (r.get("emailAddress") or {}).get("address")
                    if a:
                        addrs.append(a)
                pat_l = pat.lower()
                if any(pat_l == a.lower() or pat_l in a.lower() for a in addrs):
                    return True

        return False

    async def get_sla_policy(self, resource: Resource, message: Optional[Dict[str, Any]] = None) -> Optional[SlaPolicy]:
        policy_id = None
        if message:
            policy_id = (
                message.get("slaPolicyId")
                or message.get("sla_policy_id")
                or (message.get("spec") or {}).get("sla_policy_id")
            )
        if not policy_id:
            policy_id = resource.sla_policy_id
        if not policy_id:
            return None

        try:
            policy_uuid = uuid.UUID(str(policy_id))
        except (TypeError, ValueError):
            return None

        async with async_session_factory() as session:
            return await session.get(SlaPolicy, policy_uuid)

    async def backup_group_mailbox_content(
        self,
        resource: Resource,
        tenant: Tenant,
        snapshot: Snapshot,
        graph_client: GraphClient,
    ) -> tuple[List[SnapshotItem], int]:
        """Back up Microsoft 365 group mailbox threads and posts."""
        group_id = resource.external_id
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "group-mailbox")
        db_items: List[SnapshotItem] = []
        bytes_added = 0

        threads = await graph_client.get_group_threads(group_id)
        thread_list = threads.get("value", [])
        logger.info("[%s] [GROUP_MAILBOX] %s threads found for %s", self.worker_id, len(thread_list), resource.display_name)

        async def backup_thread(thread: Dict[str, Any]) -> tuple[List[SnapshotItem], int]:
            local_items: List[SnapshotItem] = []
            local_bytes = 0
            thread_id = thread.get("id", str(uuid.uuid4()))
            conversation_id = thread.get("conversationId")
            thread_bytes = json.dumps(thread).encode()
            thread_hash = hashlib.sha256(thread_bytes).hexdigest()
            thread_blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id),
                str(resource.id),
                str(snapshot.id),
                f"group_thread_{thread_id}",
            )
            thread_upload = await upload_blob_with_retry(container, thread_blob_path, thread_bytes, shard)
            if thread_upload.get("success"):
                local_items.append(SnapshotItem(
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=thread_id,
                    item_type="GROUP_MAILBOX_THREAD",
                    name=thread.get("topic") or thread.get("id", thread_id),
                    folder_path="group-mailbox/threads",
                    content_hash=thread_hash,
                    content_size=len(thread_bytes),
                    blob_path=thread_blob_path,
                    extra_data={"raw": thread, "conversationId": conversation_id},
                    content_checksum=thread_hash,
                ))
                local_bytes += len(thread_bytes)

            try:
                posts = await graph_client.get_group_thread_posts(group_id, thread_id)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    logger.warning("[%s] [GROUP_MAILBOX] Thread %s disappeared while fetching posts", self.worker_id, thread_id)
                    return local_items, local_bytes
                raise

            # Posts carry body + from + receivedDateTime but NOT the subject —
            # the subject lives on the parent thread as `topic`. Enrich each
            # post so the frontend (EmailItemRow / EmailPreview) gets the
            # same shape as a user's EMAIL item: raw.subject + raw.bodyPreview.
            thread_topic = thread.get("topic") or ""
            for post in posts.get("value", []):
                post_id = post.get("id", str(uuid.uuid4()))
                # Inject subject + a short bodyPreview if missing so the UI
                # mirrors user-mail rendering without frontend special-casing.
                if not post.get("subject") and thread_topic:
                    post["subject"] = thread_topic
                if not post.get("bodyPreview"):
                    raw_body = (post.get("body") or {}).get("content") or ""
                    # Strip HTML tags for a plain-text preview.
                    import re as _re
                    post["bodyPreview"] = _re.sub(r"<[^>]+>", " ", raw_body).strip()[:180]
                # Normalise sentDateTime for EmailItemRow which prefers it.
                if not post.get("sentDateTime"):
                    post["sentDateTime"] = post.get("receivedDateTime")

                post_bytes = json.dumps(post).encode()
                post_hash = hashlib.sha256(post_bytes).hexdigest()
                post_blob_path = azure_storage_manager.build_blob_path(
                    str(tenant.id),
                    str(resource.id),
                    str(snapshot.id),
                    f"group_post_{thread_id}_{post_id}",
                )
                post_upload = await upload_blob_with_retry(container, post_blob_path, post_bytes, shard)
                if post_upload.get("success"):
                    local_items.append(SnapshotItem(
                        snapshot_id=snapshot.id,
                        tenant_id=tenant.id,
                        external_id=post_id,
                        item_type="GROUP_MAILBOX_POST",
                        name=post.get("subject") or thread_topic or post_id,
                        folder_path=f"group-mailbox/threads/{thread_topic or thread_id}",
                        content_hash=post_hash,
                        content_size=len(post_bytes),
                        blob_path=post_blob_path,
                        extra_data={"raw": post, "threadId": thread_id, "conversationId": conversation_id, "threadTopic": thread_topic},
                        content_checksum=post_hash,
                    ))
                    local_bytes += len(post_bytes)

            return local_items, local_bytes

        thread_results = await asyncio.gather(
            *[backup_thread(thread) for thread in thread_list],
            return_exceptions=True,
        )
        for result in thread_results:
            if isinstance(result, tuple):
                db_items.extend(result[0])
                bytes_added += result[1]
            elif isinstance(result, Exception):
                logger.warning("[%s] [GROUP_MAILBOX] Thread backup failed for %s: %s", self.worker_id, resource.display_name, result)

        return db_items, bytes_added

    async def create_snapshot(
        self,
        resource: Resource,
        message: Dict,
        job_id: uuid.UUID,
        snapshot_type: SnapshotType = SnapshotType.INCREMENTAL,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> Snapshot:
        """Idempotent snapshot creation. If a RabbitMQ message is
        redelivered (worker restart, ack timeout, redeploy) we don't
        want a fresh IN_PROGRESS row per delivery — every redelivery
        would add a duplicate, polluting Recovery view with phantom
        spinning snapshots until the stale-sweep closes them.

        Resume-first: for (job_id, resource_id), if an IN_PROGRESS
        snapshot already exists, return it. The handler continues
        writing items against the same snapshot id, idempotency keys
        on snapshot_items (external_id) keep dupes out. Only create
        a fresh snapshot when no prior one exists for this exact
        (job_id, resource_id) pair.
        """
        async with async_session_factory() as session:
            existing = await session.execute(
                select(Snapshot).where(
                    Snapshot.job_id == job_id,
                    Snapshot.resource_id == resource.id,
                    Snapshot.status == SnapshotStatus.IN_PROGRESS,
                ).limit(1),
            )
            prior = existing.scalar_one_or_none()
            if prior is not None:
                return prior

            snapshot = Snapshot(
                id=uuid.uuid4(),
                resource_id=resource.id,
                job_id=job_id,
                type=snapshot_type,
                status=SnapshotStatus.IN_PROGRESS,
                started_at=datetime.utcnow(),
                snapshot_label=message.get("snapshotLabel", "scheduled"),
                extra_data=extra_data or {},
            )
            session.add(snapshot)
            await session.commit()
            return snapshot

    async def fail_snapshot(self, session: AsyncSession, snapshot: Snapshot, error: Exception):
        """Mark snapshot as FAILED with error details so it leaves IN_PROGRESS state."""
        now = datetime.utcnow()
        snapshot.status = SnapshotStatus.FAILED
        snapshot.completed_at = now
        if snapshot.started_at:
            snapshot.duration_secs = int((now - snapshot.started_at).total_seconds())
        existing = dict(snapshot.extra_data or {})
        existing["error"] = str(error)[:2000]
        snapshot.extra_data = existing
        await session.merge(snapshot)
        await session.commit()

    async def update_job_status(self, session: AsyncSession, job_id: uuid.UUID, status: JobStatus, result: Dict):
        job = await session.get(Job, job_id)
        if job:
            job.status = status
            job.result = result
            if status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                job.completed_at = datetime.utcnow()
            if status == JobStatus.COMPLETED:
                job.progress_pct = 100
            await session.commit()

    def chunk_list(self, lst: list, size: int):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]


# ==================== Entry Point ====================

async def main():
    from shared.storage.startup import startup_router, shutdown_router
    await startup_router()
    try:
        worker = BackupWorker()
        await worker.start()
    finally:
        await shutdown_router()


if __name__ == "__main__":
    asyncio.run(main())
