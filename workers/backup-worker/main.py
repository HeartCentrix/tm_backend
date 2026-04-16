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
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
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

logger = logging.getLogger(__name__)


class ProgressReporter:
    """Reports backup progress to the progress-tracker service"""

    def __init__(self):
        self.progress_url = f"{settings.PROGRESS_TRACKER_URL}/api/v1/progress/update"

    async def report(self, resource_id: str, job_id: str, **kwargs):
        payload = {"resource_id": resource_id, "job_id": job_id, **kwargs}
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(self.progress_url, json=payload)
        except Exception as e:
            print(f"[ProgressReporter] Failed to report: {e}")


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
        self.progress_reporter = ProgressReporter()
        self.audit_logger = AuditLogger()
        # Concurrency controls
        self.backup_semaphore = asyncio.Semaphore(8)  # 8 concurrent file streams per worker NIC
        self.copy_semaphore = asyncio.Semaphore(20)  # Azure Storage account ingress limit

    async def initialize(self):
        await message_bus.connect()
        if azure_storage_manager.shards:
            print(f"[{self.worker_id}] Azure Storage: {len(azure_storage_manager.shards)} shard(s) ready")
        print(f"[{self.worker_id}] Backup worker initialized (concurrency={settings.BACKUP_CONCURRENCY})")

    async def start(self):
        """Start consuming from all backup queues"""
        await self.initialize()

        queues = [
            ("backup.urgent", 10),
            ("backup.high", 20),
            ("backup.normal", 50),
            ("backup.low", 100),
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

    async def _process_single_backup(self, job_id: uuid.UUID, message: Dict, resource_id: str):
        """Process a backup for a single resource"""
        async with async_session_factory() as session:
            # Check if the job still exists (stale messages may reference deleted jobs)
            job = await session.get(Job, job_id)
            if not job:
                print(f"[{self.worker_id}] Job {job_id} not found, skipping stale message for {resource_id}")
                return

            resource = await session.get(Resource, uuid.UUID(resource_id))
            if not resource:
                print(f"[{self.worker_id}] Resource {resource_id} not found, skipping")
                return

            # Query tenant directly (avoid lazy loading issues)
            result = await session.execute(
                select(Tenant).where(Tenant.id == resource.tenant_id)
            )
            tenant = result.scalar_one_or_none()

            if not tenant:
                print(f"[{self.worker_id}] Tenant not found for resource {resource_id}, skipping")
                return

            graph_client = await self.get_graph_client(tenant)
            if not graph_client:
                print(f"[{self.worker_id}] Graph client not available for resource {resource_id}, skipping")
                return

            # Route to appropriate handler based on resource type
            resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
            print(f"[{self.worker_id}] Processing backup for {resource_id} (type={resource_type}, tenant={tenant.id})")

            snapshot = await self.create_snapshot(resource, message, job_id)

            handlers = {
                # Exchange
                "MAILBOX": self.backup_mailbox,
                "SHARED_MAILBOX": self.backup_mailbox,
                "ROOM_MAILBOX": self.backup_mailbox,
                # Files
                "ONEDRIVE": self.backup_onedrive,
                "SHAREPOINT_SITE": self.backup_sharepoint,
                # Teams
                "TEAMS_CHANNEL": self.backup_teams_single,
                "TEAMS_CHAT": self.backup_teams_single,
                # Entra ID
                "ENTRA_USER": self.backup_entra_single,
                "ENTRA_GROUP": self.backup_entra_single,
                "ENTRA_APP": self.backup_entra_single,
                "ENTRA_DEVICE": self.backup_entra_single,
                "ENTRA_SERVICE_PRINCIPAL": self.backup_entra_single,
                # Planner / Tasks / Copilot / Power Platform
                "PLANNER": self._backup_metadata_only,
                "TODO": self._backup_metadata_only,
                "ONENOTE": self._backup_metadata_only,
                "COPILOT": self._backup_metadata_only,
                "POWER_BI": self.backup_power_bi_workspace,
                "POWER_APPS": self._backup_metadata_only,
                "POWER_AUTOMATE": self._backup_metadata_only,
                "POWER_DLP": self._backup_metadata_only,
                # Azure workloads
                "AZURE_VM": self._backup_metadata_only,
                "AZURE_SQL": self._backup_metadata_only,
                "AZURE_SQL_DB": self._backup_metadata_only,
                "AZURE_POSTGRESQL": self._backup_metadata_only,
                # Other
                "RESOURCE_GROUP": self._backup_metadata_only,
                "DYNAMIC_GROUP": self._backup_metadata_only,
            }

            handler = handlers.get(resource_type, self._backup_metadata_only)

            try:
                print(f"[{self.worker_id}] Calling handler for {resource_type}: {resource.display_name}")
                result = await handler(graph_client, resource, snapshot, tenant, message)
            except Exception as e:
                error_str = str(e).lower()
                # Detect 404/423 errors — resource no longer exists or is locked
                is_inaccessible = any(kw in error_str for kw in [
                    "not found", "404", "resource_not_found",
                    "locked", "423", "account_locked",
                    "authorization_failed", "access_denied",
                ])

                if is_inaccessible:
                    print(f"[{self.worker_id}] Resource {resource.display_name} is INACCESSIBLE (404/423) — marking to skip future backups")
                    resource.status = "INACCESSIBLE"
                    await session.commit()

                # Mark snapshot FAILED so it doesn't stay IN_PROGRESS forever
                try:
                    await self.fail_snapshot(session, snapshot, e)
                except Exception as fail_exc:
                    print(f"[{self.worker_id}] Could not mark snapshot FAILED: {fail_exc}")

                # Mark job FAILED so the UI sees a terminal state
                try:
                    await self.update_job_status(session, job_id, JobStatus.FAILED, {"error": str(e)[:2000]})
                except Exception as job_exc:
                    print(f"[{self.worker_id}] Could not mark job FAILED: {job_exc}")

                print(f"[{self.worker_id}] Handler FAILED for {resource_type}: {resource.display_name} — {e}")
                import traceback
                traceback.print_exc()
                raise

            # Complete the snapshot with results
            await self.complete_snapshot(session, snapshot, result)

            await self.update_job_status(session, job_id, JobStatus.COMPLETED, result)
            await self.update_resource_backup_info(session, resource, job_id, snapshot.id, result)

            # Log audit event
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

        async def process_group(group_key, group_resources):
            async with semaphore:
                tenant_id_str, resource_type = group_key.split(":", 1)
                tenant = tenants_map.get(uuid.UUID(tenant_id_str))
                return await self._backup_resource_group(group_resources, tenant, message, job_id)

        tasks = [process_group(key, res_list) for key, res_list in groups.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        total_items = sum(r.get("item_count", 0) for r in results if isinstance(r, dict))
        total_bytes = sum(r.get("bytes_added", 0) for r in results if isinstance(r, dict))
        failed = sum(1 for r in results if isinstance(r, Exception))

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
        else:
            return await self._backup_generic_parallel(resources, graph_client, tenant, message, job_id)

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
                    snapshot, tenant, file_id, file_name, 0, blob_path, {}, file_item, drive_id
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
                {"sha256": sha256, "quickxor": qxh}, file_item, drive_id)
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
                                         drive_id: Optional[str] = None):
        """Create a SnapshotItem DB record for a successfully backed-up file."""
        metadata = MetadataExtractor.extract_sharepoint_item_metadata(file_item)
        metadata["drive_id"] = drive_id or (file_item.get("parentReference") or {}).get("driveId")
        if file_item.get("_site_label"):
            metadata["site_label"] = file_item["_site_label"]
        content_hash = hashes.get("sha256") or hashes.get("quickxor") or ""
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
            metadata={"structured": metadata},
            content_checksum=content_hash if content_hash else None,
        )
        async with async_session_factory() as session:
            session.add(snapshot_item)
            await session.commit()

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
                    folder_path=msg.get("parentFolderName"),
                    content_hash=content_hash, content_size=len(content_bytes),
                    blob_path=blob_path, metadata={"raw": msg}, content_checksum=content_hash,
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
                try:
                    snapshot = await self.create_snapshot(resource, message, job_id)
                    resource_type = resource.type.value
                    
                    # Route to appropriate handler
                    if resource_type.startswith("ENTRA"):
                        return await self._backup_entra_resource(resource, tenant, snapshot, graph_client, job_id, message)
                    elif resource_type.startswith("TEAMS"):
                        return await self._backup_teams_resource(resource, tenant, snapshot, graph_client, job_id)
                    elif resource_type == "POWER_BI":
                        return await self.backup_power_bi_workspace(graph_client, resource, snapshot, tenant, message)
                    else:
                        return await self._backup_metadata_only(graph_client, resource, snapshot, tenant, message)
                except Exception as e:
                    print(f"[{self.worker_id}] Generic backup failed for {resource.id}: {e}")
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

        elif resource_type in ("ENTRA_GROUP", "DYNAMIC_GROUP"):
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

            group_mail_enabled = bool((resource.extra_data or {}).get("mail_enabled"))
            if backup_group_mailbox and group_mail_enabled:
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
                    blob_path=blob_path, metadata={"raw": item_data}, content_checksum=content_hash,
                ))
                item_count += 1
                bytes_added += len(content_bytes)
            elif not isinstance(result, Exception):
                print(f"[{self.worker_id}]   [{item_type}] Upload FAILED: {result.get('error', 'unknown')}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        if db_items or item_count or bytes_added:
            async with async_session_factory() as session:
                await self.update_resource_backup_info(session, resource, job_id, snapshot.id, {
                    "item_count": item_count,
                    "bytes_added": bytes_added,
                })

        print(f"[{self.worker_id}] [ENTRA_{resource_type} COMPLETE] {resource.display_name} — {item_count} items, {bytes_added} bytes")
        return {"item_count": item_count, "bytes_added": bytes_added}

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
            channels = await graph_client.get_teams_channels(team_id)
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
                            metadata={"raw": mdata, "channelId": cid, "channelName": cname, "isReply": is_reply},
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

    async def _backup_single_chat(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                  graph_client: GraphClient) -> tuple:
        """Backup a single Teams chat — parallel uploads, single bulk DB insert."""
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "teams")
        chat_id = resource.external_id
        delta_token = (resource.extra_data or {}).get("chat_delta_token")
        chat_topic = resource.display_name or chat_id

        print(f"[{self.worker_id}]   [CHAT_MSG] {chat_topic} — fetching messages (delta)...")
        chat_msgs = await graph_client.get_chat_messages(chat_id, delta_token)
        msg_list = chat_msgs.get("value", [])
        print(f"[{self.worker_id}]   [CHAT_MSG] {chat_topic} — {len(msg_list)} messages")

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
                    metadata={"raw": msg, "chatId": chat_id, "chatTopic": chat_topic},
                    content_checksum=content_hash,
                ))
                bytes_added += len(content_bytes)
            elif not isinstance(result, Exception):
                print(f"[{self.worker_id}]   [CHAT_MSG] Upload FAILED for {msg_id}: {result.get('error', 'unknown')}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        # Save delta token for next incremental backup
        new_delta = chat_msgs.get("@odata.deltaLink")
        if new_delta:
            async with async_session_factory() as sess:
                r = await sess.get(Resource, resource.id)
                if r:
                    r.extra_data = r.extra_data or {}
                    r.extra_data["chat_delta_token"] = new_delta
                    await sess.commit()

        return len(db_items), bytes_added

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
        """
        Dispatch to type-specific backup handler.
        PLANNER / TODO / ONENOTE get real Graph API calls.
        Everything else (Copilot, Power Platform, Azure VMs) gets metadata stored.
        """
        resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
        obj_id = resource.external_id

        if resource_type == "PLANNER":
            return await self._backup_planner(graph_client, resource, snapshot, tenant, obj_id)
        elif resource_type == "TODO":
            return await self._backup_todo(graph_client, resource, snapshot, tenant, obj_id)
        elif resource_type == "ONENOTE":
            return await self._backup_onenote(graph_client, resource, snapshot, tenant, obj_id)
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
                    metadata={"extra_data": resource.extra_data or {}},
                    content_checksum=content_hash,
                ))
                await session.commit()
            print(f"[{self.worker_id}] [METADATA COMPLETE] {resource_type}: {resource.display_name} — 1 item")
            return {"item_count": 1, "bytes_added": len(content_bytes)}
        print(f"[{self.worker_id}] [METADATA FAILED] {resource_type}: {resource.display_name} — {result.get('error')}")
        return {"item_count": 0, "bytes_added": 0}

    async def _backup_planner(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                              tenant: Tenant, obj_id: str) -> Dict:
        """Backup Planner plans and tasks for a group."""
        print(f"[{self.worker_id}] [PLANNER START] {resource.display_name} ({obj_id})")
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "planner")
        db_items = []

        try:
            plans = await graph_client.get_planner_plans_for_group(obj_id)
            plan_list = plans.get("value", [])
            print(f"[{self.worker_id}]   [PLANNER] {len(plan_list)} plans found")

            for plan in plan_list:
                plan_id = plan.get("id", str(uuid.uuid4()))
                # Store plan metadata
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
                        blob_path=blob_path, metadata={"raw": plan}, content_checksum=content_hash,
                    ))
                    item_count += 1
                    bytes_added += len(content_bytes)

                # Fetch and store tasks for this plan
                try:
                    tasks = await graph_client.get_planner_tasks(plan_id=plan_id)
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
                                blob_path=tp, metadata={"raw": task, "planId": plan_id},
                                content_checksum=th,
                            ))
                            item_count += 1
                            bytes_added += len(tb)
                except Exception as e:
                    print(f"[{self.worker_id}]   [PLANNER] Tasks fetch failed for plan {plan_id}: {e}")

        except Exception as e:
            print(f"[{self.worker_id}] [PLANNER] Failed: {e}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        print(f"[{self.worker_id}] [PLANNER COMPLETE] {resource.display_name} — {item_count} items")
        return {"item_count": item_count, "bytes_added": bytes_added}

    async def _backup_todo(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                           tenant: Tenant, obj_id: str) -> Dict:
        """Backup Microsoft To Do lists and tasks for a user."""
        print(f"[{self.worker_id}] [TODO START] {resource.display_name} ({obj_id})")
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "todo")
        db_items = []

        try:
            lists = await graph_client.get_user_todo_lists(obj_id)
            list_items = lists.get("value", [])
            print(f"[{self.worker_id}]   [TODO] {len(list_items)} task lists found")

            async def backup_todo_list(lst):
                list_id = lst.get("id", str(uuid.uuid4()))
                lb = json.dumps(lst).encode()
                lh = hashlib.sha256(lb).hexdigest()
                lp = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), f"list_{list_id}"
                )
                lr = await upload_blob_with_retry(container, lp, lb, shard)
                local_items = []
                local_bytes = 0
                if lr.get("success"):
                    local_items.append(SnapshotItem(
                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                        external_id=list_id, item_type="TODO_LIST",
                        name=lst.get("displayName", list_id),
                        content_hash=lh, content_size=len(lb),
                        blob_path=lp, metadata={"raw": lst}, content_checksum=lh,
                    ))
                    local_bytes += len(lb)

                try:
                    tasks = await graph_client.get_user_todo_tasks(obj_id, list_id)
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
                                blob_path=tp, metadata={"raw": task, "listId": list_id},
                                content_checksum=th,
                            ))
                            local_bytes += len(tb)
                except Exception as e:
                    print(f"[{self.worker_id}]   [TODO] Tasks fetch failed for list {list_id}: {e}")

                return local_items, local_bytes

            list_results = await asyncio.gather(*[backup_todo_list(lst) for lst in list_items], return_exceptions=True)
            for r in list_results:
                if isinstance(r, tuple):
                    db_items.extend(r[0])
                    bytes_added += r[1]
            item_count = len(db_items)

        except Exception as e:
            print(f"[{self.worker_id}] [TODO] Failed: {e}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        print(f"[{self.worker_id}] [TODO COMPLETE] {resource.display_name} — {item_count} items")
        return {"item_count": item_count, "bytes_added": bytes_added}

    async def _backup_onenote(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                              tenant: Tenant, obj_id: str) -> Dict:
        """Backup OneNote notebooks, sections, and pages for a user."""
        print(f"[{self.worker_id}] [ONENOTE START] {resource.display_name} ({obj_id})")
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "onenote")
        db_items = []

        try:
            notebooks = await graph_client.get_onenote_notebooks(obj_id)
            nb_list = notebooks.get("value", [])
            print(f"[{self.worker_id}]   [ONENOTE] {len(nb_list)} notebooks found")

            async def backup_notebook(nb):
                nb_id = nb.get("id", str(uuid.uuid4()))
                nb_b = json.dumps(nb).encode()
                nb_h = hashlib.sha256(nb_b).hexdigest()
                nb_p = azure_storage_manager.build_blob_path(
                    str(tenant.id), str(resource.id), str(snapshot.id), f"notebook_{nb_id}"
                )
                nb_r = await upload_blob_with_retry(container, nb_p, nb_b, shard)
                local_items = []
                local_bytes = 0
                if nb_r.get("success"):
                    local_items.append(SnapshotItem(
                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                        external_id=nb_id, item_type="ONENOTE_NOTEBOOK",
                        name=nb.get("displayName", nb_id),
                        content_hash=nb_h, content_size=len(nb_b),
                        blob_path=nb_p, metadata={"raw": nb}, content_checksum=nb_h,
                    ))
                    local_bytes += len(nb_b)

                try:
                    sections = await graph_client.get_onenote_sections(obj_id, nb_id)
                    for sec in sections.get("value", []):
                        sec_id = sec.get("id", str(uuid.uuid4()))
                        sb = json.dumps(sec).encode()
                        sh = hashlib.sha256(sb).hexdigest()
                        sp = azure_storage_manager.build_blob_path(
                            str(tenant.id), str(resource.id), str(snapshot.id), f"section_{sec_id}"
                        )
                        sr = await upload_blob_with_retry(container, sp, sb, shard)
                        if sr.get("success"):
                            local_items.append(SnapshotItem(
                                snapshot_id=snapshot.id, tenant_id=tenant.id,
                                external_id=sec_id, item_type="ONENOTE_SECTION",
                                name=sec.get("displayName", sec_id),
                                content_hash=sh, content_size=len(sb),
                                blob_path=sp, metadata={"raw": sec, "notebookId": nb_id},
                                content_checksum=sh,
                            ))
                            local_bytes += len(sb)

                        try:
                            pages = await graph_client.get_onenote_pages(obj_id, sec_id)
                            for page in pages.get("value", []):
                                pg_id = page.get("id", str(uuid.uuid4()))
                                pb = json.dumps(page).encode()
                                ph = hashlib.sha256(pb).hexdigest()
                                pp = azure_storage_manager.build_blob_path(
                                    str(tenant.id), str(resource.id), str(snapshot.id), f"page_{pg_id}"
                                )
                                pr = await upload_blob_with_retry(container, pp, pb, shard)
                                if pr.get("success"):
                                    local_items.append(SnapshotItem(
                                        snapshot_id=snapshot.id, tenant_id=tenant.id,
                                        external_id=pg_id, item_type="ONENOTE_PAGE",
                                        name=page.get("title", pg_id),
                                        content_hash=ph, content_size=len(pb),
                                        blob_path=pp,
                                        metadata={"raw": page, "sectionId": sec_id, "notebookId": nb_id},
                                        content_checksum=ph,
                                    ))
                                    local_bytes += len(pb)
                        except Exception as e:
                            print(f"[{self.worker_id}]   [ONENOTE] Pages fetch failed for section {sec_id}: {e}")
                except Exception as e:
                    print(f"[{self.worker_id}]   [ONENOTE] Sections fetch failed for notebook {nb_id}: {e}")

                return local_items, local_bytes

            nb_results = await asyncio.gather(*[backup_notebook(nb) for nb in nb_list], return_exceptions=True)
            for r in nb_results:
                if isinstance(r, tuple):
                    db_items.extend(r[0])
                    bytes_added += r[1]
            item_count = len(db_items)

        except Exception as e:
            print(f"[{self.worker_id}] [ONENOTE] Failed: {e}")

        if db_items:
            async with async_session_factory() as session:
                session.add_all(db_items)
                await session.commit()

        print(f"[{self.worker_id}] [ONENOTE COMPLETE] {resource.display_name} — {item_count} items")
        return {"item_count": item_count, "bytes_added": bytes_added}

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

    async def backup_mailbox(self, graph_client: GraphClient, resource: Resource, snapshot: Snapshot,
                             tenant: Tenant, message: Dict) -> Dict:
        """Backup a single mailbox"""
        print(f"[{self.worker_id}] [MAILBOX START] {resource.display_name} ({resource.external_id})")

        print(f"[{self.worker_id}]   [EMAIL] Fetching messages (paginated)...")
        delta_token = (resource.extra_data or {}).get("mail_delta_token")
        try:
            messages = await graph_client.get_messages_delta(resource.external_id, delta_token)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"[{self.worker_id}]   [EMAIL] No mailbox found for this user — skipping (user has no Exchange Online license)")
                return {"item_count": 0, "bytes_added": 0, "new_delta_token": None, "note": "no_mailbox"}
            raise

        items = messages.get("value", [])
        print(f"[{self.worker_id}]   [EMAIL] Found {len(items)} messages to backup")

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
                        folder_path=msg.get("parentFolderName"),
                        content_hash=content_hash, content_size=len(content_bytes),
                        blob_path=blob_path, metadata={"raw": msg}, content_checksum=content_hash,
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

        new_delta = messages.get("@odata.deltaLink")
        print(f"[{self.worker_id}] [BACKUP COMPLETE] Mailbox: {resource.display_name} — {item_count} emails, {bytes_added} bytes")
        return {"item_count": item_count, "bytes_added": bytes_added, "new_delta_token": new_delta}

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
        """Backup a single SharePoint site"""
        print(f"[{self.worker_id}] [SHAREPOINT START] {resource.display_name} (site: {resource.external_id})")

        delta_token = (resource.extra_data or {}).get("delta_token")
        subsite_delta_tokens = ((resource.extra_data or {}).get("subsite_delta_tokens") or {}).copy()

        async def fetch_site_items(site_id: str, site_label: str, site_delta_token: Optional[str]) -> tuple[List[Dict[str, Any]], Optional[str], str]:
            files = await graph_client.get_sharepoint_site_drives(site_id, site_delta_token)
            items = files.get("value", [])
            for item in items:
                item["_site_label"] = site_label
            return items, files.get("@odata.deltaLink"), site_id

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

        print(f"[{self.worker_id}]   [SP_FILES] Fetching drive items for {len(site_targets)} site targets (root + subsites)...")
        site_results = await asyncio.gather(
            *[fetch_site_items(site_id, site_label, token) for site_id, site_label, token in site_targets],
            return_exceptions=True,
        )

        items: List[Dict[str, Any]] = []
        new_delta = None
        new_subsite_tokens: Dict[str, str] = {}
        for result in site_results:
            if isinstance(result, Exception):
                logger.warning("SharePoint site target fetch failed for %s: %s", resource.display_name, result)
                continue
            site_items, site_delta_link, site_id = result
            items.extend(site_items)
            if site_id == resource.external_id:
                new_delta = site_delta_link
            elif site_delta_link:
                new_subsite_tokens[site_id] = site_delta_link

        print(f"[{self.worker_id}]   [SP_FILES] Found {len(items)} site files across root and subsites")

        file_tasks = [
            self.backup_single_file(resource, tenant, snapshot, f, graph_client, None)
            for f in items
        ]
        file_results = await asyncio.gather(*file_tasks, return_exceptions=True)

        total_items = sum(1 for r in file_results if isinstance(r, dict) and r.get("success"))
        total_bytes = sum(r.get("size", 0) for r in file_results if isinstance(r, dict))
        failed = sum(1 for r in file_results if isinstance(r, Exception) or (isinstance(r, dict) and not r.get("success")))

        # Log per-file failures for debugging
        for r in file_results:
            if isinstance(r, Exception):
                print(f"[{self.worker_id}]   [SP_FILE FAIL] unknown: {type(r).__name__}: {r}")
            elif isinstance(r, dict) and not r.get("success"):
                print(f"[{self.worker_id}]   [SP_FILE FAIL] {r.get('file_name','?')}: "
                      f"method={r.get('method','?')} reason={r.get('reason','no reason')}")

        print(f"[{self.worker_id}]   [SP_FILES] Done — {total_items} uploaded, {failed} failed, {total_bytes} bytes")

        if new_subsite_tokens:
            async with async_session_factory() as sess:
                r = await sess.get(Resource, resource.id)
                if r:
                    r.extra_data = r.extra_data or {}
                    existing_subsite_tokens = (r.extra_data.get("subsite_delta_tokens") or {}).copy()
                    existing_subsite_tokens.update(new_subsite_tokens)
                    r.extra_data["subsite_delta_tokens"] = existing_subsite_tokens
                    await sess.commit()

        print(f"[{self.worker_id}] [BACKUP COMPLETE] SharePoint: {resource.display_name} — {total_items} files, {total_bytes} bytes")
        return {"item_count": total_items, "bytes_added": total_bytes, "new_delta_token": new_delta}

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

    # ==================== Helpers ====================

    async def get_graph_client(self, tenant: Tenant) -> Optional[GraphClient]:
        app = multi_app_manager.get_next_app()
        return GraphClient(
            client_id=app.client_id,
            client_secret=app.client_secret,
            tenant_id=tenant.external_tenant_id,
        )

    def get_power_bi_client(self, tenant: Tenant) -> PowerBIClient:
        return PowerBIClient(
            tenant_id=tenant.external_tenant_id or settings.EFFECTIVE_POWER_BI_TENANT_ID,
            refresh_token=PowerBIClient.get_refresh_token_from_tenant(tenant),
        )

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
                    metadata={"raw": thread, "conversationId": conversation_id},
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

            for post in posts.get("value", []):
                post_id = post.get("id", str(uuid.uuid4()))
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
                        name=post.get("subject") or post.get("id", post_id),
                        folder_path=f"group-mailbox/threads/{thread.get('topic') or thread_id}",
                        content_hash=post_hash,
                        content_size=len(post_bytes),
                        blob_path=post_blob_path,
                        metadata={"raw": post, "threadId": thread_id, "conversationId": conversation_id},
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
        async with async_session_factory() as session:
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
    worker = BackupWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
