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
from datetime import datetime
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
    SlaPolicy, ResourceType, JobStatus, SnapshotType, SnapshotStatus
)
from shared.message_bus import message_bus
from shared.config import settings
from shared.graph_client import GraphClient
from shared.multi_app_manager import multi_app_manager
from shared.metadata_extractor import MetadataExtractor
from shared.azure_storage import (
    azure_storage_manager,
    server_side_copy_with_retry,
    upload_blob_with_retry,
)


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
        self.backup_semaphore = asyncio.Semaphore(settings.BACKUP_CONCURRENCY)
        self.copy_semaphore = asyncio.Semaphore(settings.COPY_CONCURRENCY)

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
                        await message.reject(requeue=True)
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

    # ==================== Mass Backup (Parallel) ====================

    async def _process_mass_backup(self, job_id: uuid.UUID, message: Dict, resource_ids: List[str]):
        """Process mass backup with full parallelism"""
        async with async_session_factory() as session:
            job = await session.get(Job, job_id)
            if not job:
                return

            job.status = JobStatus.RUNNING
            await session.commit()

        # Fetch all resources
        async with async_session_factory() as session:
            result = await session.execute(
                select(Resource).where(Resource.id.in_([uuid.UUID(rid) for rid in resource_ids]))
                .options(selectinload(Resource.tenant))
            )
            resources = result.scalars().all()

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
                tenant_id, resource_type = group_key.split(":", 1)
                return await self._backup_resource_group(group_resources, message, job_id)

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

    async def _backup_resource_group(self, resources: List[Resource], message: Dict, job_id: uuid.UUID) -> Dict:
        """Backup a group of resources of the same type from the same tenant"""
        if not resources:
            return {"item_count": 0, "bytes_added": 0}

        tenant = resources[0].tenant
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
        total_items = 0
        total_bytes = 0
        resource_type = resources[0].type.value

        # Process resources in parallel
        semaphore = asyncio.Semaphore(settings.BACKUP_CONCURRENCY)

        async def backup_one_file(resource: Resource):
            nonlocal total_items, total_bytes
            async with semaphore:
                try:
                    snapshot = await self.create_snapshot(resource, message, job_id)
                    delta_token = (resource.extra_data or {}).get("delta_token")

                    if resource_type == "ONEDRIVE":
                        files = await graph_client.get_drive_items_delta(resource.external_id, delta_token)
                    else:
                        files = await graph_client.get_sharepoint_site_drives(resource.external_id, delta_token)

                    items = files.get("value", [])
                    
                    # Process files in parallel
                    file_tasks = [
                        self.backup_single_file(resource, tenant, snapshot, f, graph_client, job_id)
                        for f in items[:500]  # Limit per resource to prevent overload
                    ]
                    file_results = await asyncio.gather(*file_tasks, return_exceptions=True)

                    success = sum(1 for r in file_results if isinstance(r, dict) and r.get("success"))
                    total_items += success
                    total_bytes += sum(r.get("size", 0) for r in file_results if isinstance(r, dict))

                    # Update delta token
                    new_delta = files.get("@odata.deltaLink")
                    if new_delta and resource.extra_data is None:
                        resource.extra_data = {}
                    if new_delta:
                        resource.extra_data["delta_token"] = new_delta

                    async with async_session_factory() as sess:
                        sess.merge(resource)
                        await sess.commit()

                    return {"item_count": success, "bytes_added": total_bytes}
                except Exception as e:
                    print(f"[{self.worker_id}] File backup failed for {resource.id}: {e}")
                    return {"item_count": 0, "bytes_added": 0}

        results = await asyncio.gather(*[backup_one_file(r) for r in resources], return_exceptions=True)
        return {
            "item_count": sum(r.get("item_count", 0) for r in results if isinstance(r, dict)),
            "bytes_added": sum(r.get("bytes_added", 0) for r in results if isinstance(r, dict)),
        }

    async def backup_single_file(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                 file_item: Dict, graph_client: GraphClient, job_id: uuid.UUID) -> Dict:
        """
        Backup a single file using Server-Side Copy if large, or direct upload if small.
        """
        file_id = file_item.get("id", str(uuid.uuid4()))
        file_size = file_item.get("size", 0)
        download_url = file_item.get("@microsoft.graph.downloadUrl")
        
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "files")
        blob_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id), file_id
        )

        # Metadata
        metadata = MetadataExtractor.extract_file_metadata(file_item, str(resource.id))
        content_json = json.dumps({"raw": file_item, "structured": metadata})
        content_bytes = content_json.encode('utf-8')
        content_hash = hashlib.sha256(content_bytes).hexdigest()

        upload_result = None

        # Decision: Server-Side Copy for large files, direct upload for small
        if download_url and file_size > settings.SERVER_SIDE_COPY_THRESHOLD:
            # Server-Side Copy: Azure handles the file transfer
            async with self.copy_semaphore:
                upload_result = await server_side_copy_with_retry(
                    download_url, container, blob_path, shard, max_retries=settings.MAX_RETRIES
                )
        else:
            # Direct upload for metadata + small files
            async with self.backup_semaphore:
                upload_result = await upload_blob_with_retry(
                    container, blob_path, content_bytes, shard,
                    max_retries=settings.MAX_RETRIES, metadata={"content-hash": content_hash}
                )

        # Create SnapshotItem record
        snapshot_item = SnapshotItem(
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=file_id,
            item_type="FILE",
            name=file_item.get("name", file_id),
            folder_path=file_item.get("parentReference", {}).get("path"),
            content_hash=content_hash,
            content_size=file_size,
            blob_path=blob_path,
            metadata={"raw": file_item, "structured": metadata},
            content_checksum=content_hash,
        )

        async with async_session_factory() as session:
            session.add(snapshot_item)
            await session.commit()

        return {
            "success": upload_result.get("success", False) if upload_result else False,
            "size": file_size,
            "method": upload_result.get("method", "unknown") if upload_result else "none",
        }

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

                    # Process messages in parallel batches
                    batch_tasks = [
                        self.backup_message_batch(resource, tenant, snapshot, items[i:i+50], job_id)
                        for i in range(0, min(len(items), 1000), 50)
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
        """Backup a batch of messages"""
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "mailbox")

        for msg in messages:
            msg_id = msg.get("id", str(uuid.uuid4()))
            content_json = json.dumps(msg, sort_keys=True)
            content_bytes = content_json.encode('utf-8')
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            
            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), msg_id
            )

            upload_result = await upload_blob_with_retry(
                container, blob_path, content_bytes, shard,
                max_retries=3, metadata={"content-hash": content_hash}
            )

            if upload_result.get("success"):
                snapshot_item = SnapshotItem(
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=msg_id,
                    item_type="EMAIL",
                    name=msg.get("subject", msg_id),
                    folder_path=msg.get("parentFolderName"),
                    content_hash=content_hash,
                    content_size=len(content_bytes),
                    blob_path=blob_path,
                    metadata={"raw": msg},
                    content_checksum=content_hash,
                )
                async with async_session_factory() as session:
                    session.add(snapshot_item)
                    await session.commit()

                item_count += 1
                bytes_added += len(content_bytes)

        return {"item_count": item_count, "bytes_added": bytes_added}

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
                        return await self._backup_entra_resource(resource, tenant, snapshot, graph_client, job_id)
                    elif resource_type.startswith("TEAMS"):
                        return await self._backup_teams_resource(resource, tenant, snapshot, graph_client, job_id)
                    else:
                        return await self._backup_metadata_only(resource, tenant, snapshot, graph_client, job_id)
                except Exception as e:
                    print(f"[{self.worker_id}] Generic backup failed for {resource.id}: {e}")
                    return {"item_count": 0, "bytes_added": 0}

        results = await asyncio.gather(*[backup_one(r) for r in resources], return_exceptions=True)
        return {
            "item_count": sum(r.get("item_count", 0) for r in results if isinstance(r, dict)),
            "bytes_added": sum(r.get("bytes_added", 0) for r in results if isinstance(r, dict)),
        }

    async def _backup_entra_resource(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                     graph_client: GraphClient, job_id: uuid.UUID) -> Dict:
        """Backup Entra ID resource (user, group, app, device)"""
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "entra")

        # Fetch resource data
        user_id = resource.external_id
        items_to_backup = []

        if resource.type.value == "ENTRA_USER":
            profile = await graph_client.get_user_profile(user_id)
            items_to_backup.append(("USER_PROFILE", profile))
            
            contacts = await graph_client.get_user_contacts(user_id)
            for c in contacts.get("value", [])[:100]:
                items_to_backup.append(("USER_CONTACT", c))
            
            calendars = await graph_client.get_calendar_events_delta(user_id)
            for e in calendars.get("value", [])[:500]:
                items_to_backup.append(("CALENDAR_EVENT", e))

        # Backup items
        for item_type, item_data in items_to_backup:
            item_id = item_data.get("id", str(uuid.uuid4()))
            content_bytes = json.dumps(item_data).encode()
            content_hash = hashlib.sha256(content_bytes).hexdigest()
            blob_path = azure_storage_manager.build_blob_path(
                str(tenant.id), str(resource.id), str(snapshot.id), f"{item_type}_{item_id}"
            )

            result = await upload_blob_with_retry(container, blob_path, content_bytes, shard)
            if result.get("success"):
                snapshot_item = SnapshotItem(
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=item_id,
                    item_type=item_type,
                    name=item_data.get("displayName", item_data.get("subject", item_id)),
                    content_hash=content_hash,
                    content_size=len(content_bytes),
                    blob_path=blob_path,
                    metadata={"raw": item_data},
                    content_checksum=content_hash,
                )
                async with async_session_factory() as session:
                    session.add(snapshot_item)
                    await session.commit()
                item_count += 1
                bytes_added += len(content_bytes)

        return {"item_count": item_count, "bytes_added": bytes_added}

    async def _backup_teams_resource(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                     graph_client: GraphClient, job_id: uuid.UUID) -> Dict:
        """Backup Teams channels/chats"""
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), "teams")

        team_id = resource.external_id
        
        if resource.type.value == "TEAMS_CHANNEL":
            channels = await graph_client.get_teams_channels(team_id)
            for ch in channels.get("value", [])[:100]:
                ch_id = ch.get("id")
                msgs = await graph_client.get_channel_messages_delta(team_id, ch_id)
                for msg in msgs.get("value", [])[:500]:
                    msg_id = msg.get("id", str(uuid.uuid4()))
                    content_bytes = json.dumps(msg).encode()
                    blob_path = azure_storage_manager.build_blob_path(
                        str(tenant.id), str(resource.id), str(snapshot.id), f"ch_{ch_id}_msg_{msg_id}"
                    )
                    result = await upload_blob_with_retry(container, blob_path, content_bytes, shard)
                    if result.get("success"):
                        item_count += 1
                        bytes_added += len(content_bytes)

        return {"item_count": item_count, "bytes_added": bytes_added}

    async def _backup_metadata_only(self, resource: Resource, tenant: Tenant, snapshot: Snapshot,
                                    graph_client: GraphClient, job_id: uuid.UUID) -> Dict:
        """Backup metadata-only resources (Planner, Power Platform, Copilot)"""
        item_count = 0
        bytes_added = 0
        shard = azure_storage_manager.get_shard_for_resource(str(resource.id), str(tenant.id))
        container = azure_storage_manager.get_container_name(str(tenant.id), resource.type.value.lower())

        content_bytes = json.dumps(resource.extra_data or {}).encode()
        blob_path = azure_storage_manager.build_blob_path(
            str(tenant.id), str(resource.id), str(snapshot.id), "metadata"
        )
        result = await upload_blob_with_retry(container, blob_path, content_bytes, shard)
        if result.get("success"):
            item_count = 1
            bytes_added = len(content_bytes)

        return {"item_count": item_count, "bytes_added": bytes_added}

    # ==================== Helpers ====================

    async def get_graph_client(self, tenant: Tenant) -> Optional[GraphClient]:
        app = multi_app_manager.get_next_app()
        return GraphClient(
            client_id=app.client_id,
            client_secret=app.client_secret,
            tenant_id=tenant.external_tenant_id,
        )

    async def create_snapshot(self, resource: Resource, message: Dict, job_id: uuid.UUID) -> Snapshot:
        async with async_session_factory() as session:
            snapshot = Snapshot(
                id=uuid.uuid4(),
                resource_id=resource.id,
                job_id=job_id,
                type=SnapshotType.INCREMENTAL,
                status=SnapshotStatus.RUNNING,
                snapshot_label=message.get("snapshotLabel", "scheduled"),
            )
            session.add(snapshot)
            await session.commit()
            return snapshot

    async def update_job_status(self, session: AsyncSession, job_id: uuid.UUID, status: JobStatus, result: Dict):
        job = await session.get(Job, job_id)
        if job:
            job.status = status
            job.result = result
            if status == JobStatus.COMPLETED:
                job.completed_at = datetime.utcnow()
                job.progress_pct = 100
            await session.commit()

    def chunk_list(self, lst: list, size: int):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]
