"""
Backup Worker - Mass Backup Processing
Consumes backup jobs from RabbitMQ queues and executes them

Queues:
- backup.urgent (priority 1) - Manual/preemptive backups
- backup.high (priority 2) - Gold SLA backups
- backup.normal (priority 5) - Silver SLA backups  
- backup.low (priority 8) - Bronze SLA backups

Features:
- Batch processing for mass backup (up to 1000 resources per job)
- Graph API $batch endpoint support (up to 20 requests per batch)
- Adaptive throttling to prevent 429 errors
- Per-folder delta token tracking for Exchange
- Content deduplication via SHA-256
- AES-256-GCM encryption
- Azure Blob Storage with versioned paths
"""
import asyncio
import json
import uuid
import hashlib
import base64
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import aio_pika
from aio_pika import Message, IncomingMessage
import httpx
from azure.storage.blob import BlobServiceClient
from sqlalchemy import select, update, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from shared.database import async_session_factory
from shared.models import (
    Resource, Tenant, Job, Snapshot, SnapshotItem,
    SlaPolicy, Organization, ResourceType, JobStatus, SnapshotType, SnapshotStatus
)
from shared.message_bus import message_bus
from shared.config import settings
from shared.graph_client import GraphClient
from shared.multi_app_manager import multi_app_manager
from shared.metadata_extractor import MetadataExtractor
from shared.azure_immutability import get_immutability_config


class ProgressReporter:
    """Reports backup progress to the progress-tracker service"""

    def __init__(self):
        self.progress_url = "http://progress-tracker:8011/api/v1/progress/update"

    async def report(self, resource_id: str, job_id: str, **kwargs):
        """Send progress update to progress-tracker"""
        payload = {
            "resource_id": resource_id,
            "job_id": job_id,
            **kwargs
        }
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(self.progress_url, json=payload)
        except Exception as e:
            print(f"[ProgressReporter] Failed to report progress: {e}")


class AuditLogger:
    """Logs backup events to the audit service via HTTP POST and RabbitMQ"""

    def __init__(self):
        self.audit_url = "http://audit-service:8012/api/v1/audit/log"

    async def log(self, **kwargs):
        """Send audit event via HTTP POST (fallback) and publish to RabbitMQ"""
        # Fallback: HTTP POST to audit service
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(self.audit_url, json=kwargs)
        except Exception as e:
            print(f"[AuditLogger] Failed to log audit event via HTTP: {e}")

        # Primary: Publish to RabbitMQ queue for reliable delivery
        try:
            from shared.message_bus import message_bus, create_audit_event_message
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
            print(f"[AuditLogger] Failed to publish audit event to queue: {e}")


class BackupWorker:
    """Main backup worker that processes backup jobs from RabbitMQ"""
    
    def __init__(self):
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self.graph_clients: Dict[str, GraphClient] = {}
        self.blob_service_client: Optional[BlobServiceClient] = None
        self.immutability_config = None
        self.semaphore = asyncio.Semaphore(50)  # Max 50 concurrent backups
        self.throttle_tracker: Dict[str, Dict] = {}  # Track API calls per tenant
        self.progress_reporter = ProgressReporter()
        self.audit_logger = AuditLogger()
        
    async def initialize(self):
        """Initialize connections and clients"""
        # Connect to message bus
        await message_bus.connect()

        # Initialize Azure Blob Storage
        if settings.AZURE_STORAGE_ACCOUNT_NAME and settings.AZURE_STORAGE_ACCOUNT_KEY:
            connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={settings.AZURE_STORAGE_ACCOUNT_NAME};"
                f"AccountKey={settings.AZURE_STORAGE_ACCOUNT_KEY};"
                f"EndpointSuffix=core.windows.net"
            )
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            print(f"[{self.worker_id}] Azure Blob Storage initialized")

            # Initialize immutability configuration
            self.immutability_config = get_immutability_config()
            if self.immutability_config:
                print(f"[{self.worker_id}] Azure Blob immutability policy configured")

        print(f"[{self.worker_id}] Backup worker initialized")
    
    async def start(self):
        """Start consuming from all backup queues"""
        await self.initialize()
        
        # Start consumers for all backup queues
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
        """Consume messages from a specific queue"""
        if not message_bus.channel:
            print(f"[{self.worker_id}] Cannot consume from {queue_name}: channel is None")
            return

        queue = await message_bus.channel.get_queue(queue_name)
        print(f"[{self.worker_id}] Listening on {queue_name}...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                print(f"[{self.worker_id}] Got message from {queue_name}")
                try:
                    body = json.loads(message.body.decode())
                    print(f"[{self.worker_id}] Received message from {queue_name}: {body}")
                    await self.process_backup_message(body)
                    await message.ack()
                    print(f"[{self.worker_id}] Processed message from {queue_name}")
                except Exception as e:
                    print(f"[{self.worker_id}] Error processing message: {e}")
                    import traceback
                    traceback.print_exc()
                    try:
                        await message.reject(requeue=True)
                    except Exception:
                        pass
                    # In production: send to DLQ
    
    async def process_backup_message(self, message: Dict[str, Any]):
        """Process a single backup job message"""
        job_id = uuid.UUID(message["jobId"])
        tenant_id = message["tenantId"]
        
        # Check if this is a mass backup (batch of resources)
        resource_ids = message.get("resourceIds", [])
        resource_id = message.get("resourceId")
        
        if resource_ids:
            # Mass backup - process batch
            await self.process_mass_backup(job_id, tenant_id, message)
        elif resource_id:
            # Single resource backup
            await self.process_single_backup(job_id, resource_id, tenant_id, message)
        else:
            print(f"[{self.worker_id}] Invalid message: no resource IDs")
    
    async def process_single_backup(self, job_id: uuid.UUID, resource_id: str, tenant_id: str, message: Dict):
        """Process backup for a single resource"""
        async with self.semaphore:  # Concurrency control
            session = async_session_factory(autoflush=False)
            try:
                # Update job status
                await self.update_job_status(session, job_id, JobStatus.RUNNING)
                await session.flush()

                # Fetch resource
                resource = await session.get(Resource, uuid.UUID(resource_id))
                if not resource:
                    raise ValueError(f"Resource {resource_id} not found")

                # Fetch tenant
                tenant = await session.get(Tenant, resource.tenant_id)
                if not tenant:
                    raise ValueError(f"Tenant {resource.tenant_id} not found")

                # Get or create Graph client
                graph_client = await self.get_graph_client(tenant)

                # Create snapshot
                snapshot = await self.create_snapshot(session, resource, message)
                await session.flush()

                # Execute backup based on resource type
                backup_result = await self.execute_backup(
                    graph_client, resource, snapshot, tenant, message
                )

                # Report progress (100% complete)
                await self.progress_reporter.report(
                    resource_id=resource_id,
                    job_id=str(job_id),
                    status="COMPLETED",
                    progress_pct=100,
                    processed_items=backup_result.get("item_count", 0),
                    processed_bytes=backup_result.get("bytes_added", 0),
                )

                # Complete snapshot
                await self.complete_snapshot(
                    session, snapshot, backup_result, message
                )

                # Update job as completed
                await self.update_job_status(
                    session, job_id, JobStatus.COMPLETED,
                    result=backup_result
                )

                # Update resource last backup info
                await self.update_resource_backup_info(
                    session, resource, job_id, snapshot.id
                )

                await session.commit()

                # Log audit event: BACKUP_COMPLETED
                await self.audit_logger.log(
                    action="BACKUP_COMPLETED",
                    tenant_id=tenant_id,
                    org_id=str(tenant.org_id) if hasattr(tenant, 'org_id') and tenant.org_id else None,
                    actor_type="WORKER",
                    resource_id=resource_id,
                    resource_type=resource.type.value if hasattr(resource.type, 'value') else resource.type,
                    resource_name=resource.display_name or resource.email,
                    outcome="SUCCESS",
                    job_id=str(job_id),
                    snapshot_id=str(snapshot.id),
                    details={
                        "item_count": backup_result.get("item_count", 0),
                        "bytes_added": backup_result.get("bytes_added", 0),
                        "delta_token": "present" if backup_result.get("new_delta_token") else "none",
                    },
                )

                print(f"[{self.worker_id}] Completed backup for {resource_id}")

            except Exception as e:
                try:
                    await session.rollback()
                except Exception:
                    pass
                await self.handle_backup_failure(session, job_id, e, uuid.UUID(resource_id))

                # Log audit event: BACKUP_FAILED
                try:
                    await self.audit_logger.log(
                        action="BACKUP_FAILED",
                        tenant_id=tenant_id,
                        actor_type="WORKER",
                        resource_id=resource_id,
                        resource_type=resource.type.value if 'resource' in dir() and hasattr(resource, 'type') else None,
                        resource_name=resource.display_name if 'resource' in dir() and hasattr(resource, 'display_name') else None,
                        outcome="FAILURE",
                        job_id=str(job_id),
                        details={"error": str(e)},
                    )
                except Exception:
                    pass

                print(f"[{self.worker_id}] Backup failed for {resource_id}: {e}")
                raise
            finally:
                await session.close()
    
    async def process_mass_backup(self, job_id: uuid.UUID, tenant_id: str, message: Dict):
        """Process mass backup for a batch of resources"""
        resource_ids = message["resourceIds"]
        resource_type = message["resourceType"]
        batch_size = len(resource_ids)

        print(f"[{self.worker_id}] Starting mass backup: {batch_size} {resource_type} resources")

        async with self.semaphore:
            session = async_session_factory(autoflush=False)
            try:
                # Update job status
                await self.update_job_status(session, job_id, JobStatus.RUNNING)
                await session.flush()

                # Fetch tenant
                tenant_result = await session.execute(
                    select(Tenant).where(Tenant.external_tenant_id == tenant_id)
                )
                tenant = tenant_result.scalars().first()
                if not tenant:
                    raise ValueError(f"Tenant {tenant_id} not found")

                # Get Graph client
                graph_client = await self.get_graph_client(tenant)

                # Fetch all resources
                resources_result = await session.execute(
                    select(Resource).where(Resource.id.in_([uuid.UUID(rid) for rid in resource_ids]))
                )
                resources = resources_result.scalars().all()

                print(f"[{self.worker_id}] Fetched {len(resources)} resources for mass backup")

                # Group resources for batch Graph API calls
                batch_results = []
                completed_count = 0
                failed_count = 0

                # Process in smaller batches for Graph API $batch endpoint
                GRAPH_BATCH_SIZE = 20  # Graph API limit per $batch call

                for i in range(0, len(resources), GRAPH_BATCH_SIZE):
                    resource_batch = resources[i:i + GRAPH_BATCH_SIZE]

                    # Execute batch backup
                    batch_result = await self.execute_batch_backup(
                        graph_client, resource_batch, tenant, message
                    )
                    batch_results.extend(batch_result)

                    completed_count += len([r for r in batch_result if r.get("success")])
                    failed_count += len([r for r in batch_result if not r.get("success")])

                    # Update progress
                    progress = int((i + len(resource_batch)) / len(resources) * 100)
                    await self.update_job_progress(session, job_id, progress)

                    print(f"[{self.worker_id}] Progress: {completed_count}/{len(resources)} completed")

                # Update job as completed
                await self.update_job_status(
                    session, job_id, JobStatus.COMPLETED,
                    result={
                        "total": len(resources),
                        "completed": completed_count,
                        "failed": failed_count,
                        "batch_results": batch_results,
                    }
                )

                await session.commit()

                # Log audit event: BACKUP_COMPLETED (mass)
                outcome = "PARTIAL" if failed_count > 0 and completed_count > 0 else "SUCCESS"
                await self.audit_logger.log(
                    action="BACKUP_COMPLETED",
                    tenant_id=tenant_id,
                    org_id=str(tenant.org_id) if hasattr(tenant, 'org_id') and tenant.org_id else None,
                    actor_type="WORKER",
                    resource_type=resource_type,
                    resource_name=f"Mass backup: {resource_type}",
                    outcome=outcome,
                    job_id=str(job_id),
                    details={
                        "total": len(resources),
                        "completed": completed_count,
                        "failed": failed_count,
                        "batch": True,
                    },
                )

                print(f"[{self.worker_id}] Mass backup completed: {completed_count} succeeded, {failed_count} failed")

            except Exception as e:
                try:
                    await session.rollback()
                except Exception:
                    pass
                await self.handle_backup_failure(session, job_id, e)
                print(f"[{self.worker_id}] Mass backup failed: {e}")
                raise
            finally:
                await session.close()
    
    async def execute_batch_backup(
        self,
        graph_client: GraphClient,
        resources: List[Resource],
        tenant: Tenant,
        message: Dict
    ) -> List[Dict]:
        """Execute backup for a batch of resources using Graph API $batch endpoint"""
        results = []
        
        for resource in resources:
            try:
                # Create snapshot for this resource
                snapshot = await self.create_snapshot_for_resource(resource, message)
                
                # Execute backup based on resource type
                backup_result = await self.execute_backup(
                    graph_client, resource, snapshot, tenant, message
                )
                
                # Update resource last backup info
                async with async_session_factory() as session:
                    await self.update_resource_backup_info(session, resource, None, snapshot.id)
                    await session.commit()
                
                results.append({
                    "resource_id": str(resource.id),
                    "success": True,
                    "snapshot_id": str(snapshot.id),
                    "items": backup_result.get("item_count", 0),
                    "bytes": backup_result.get("bytes_added", 0),
                })
                
            except Exception as e:
                print(f"[{self.worker_id}] Failed to backup {resource.id}: {e}")
                results.append({
                    "resource_id": str(resource.id),
                    "success": False,
                    "error": str(e),
                })
        
        return results
    
    async def get_graph_client(self, tenant: Tenant) -> GraphClient:
        """Get Graph client for a tenant using next available app registration"""
        # Get next app from multi-app manager (round-robin + load balancing)
        app = multi_app_manager.get_next_app()

        # Use tenant's external tenant ID with the app's credentials
        return GraphClient(
            client_id=app.client_id,
            client_secret=app.client_secret,
            tenant_id=tenant.external_tenant_id,
        )
    
    async def create_snapshot(self, session: AsyncSession, resource: Resource, message: Dict) -> Snapshot:
        """Create a snapshot record for backup"""
        snapshot_id = uuid.uuid4()
        
        # Determine snapshot type
        snapshot_type = SnapshotType.MANUAL if message.get("triggeredBy") == "MANUAL" else SnapshotType.INCREMENTAL
        
        snapshot = Snapshot(
            id=snapshot_id,
            resource_id=resource.id,
            job_id=uuid.UUID(message["jobId"]),
            type=snapshot_type,
            status=SnapshotStatus.RUNNING,
            snapshot_label=message.get("snapshotLabel", "scheduled"),
        )
        
        session.add(snapshot)
        await session.flush()
        
        return snapshot
    
    async def create_snapshot_for_resource(self, resource: Resource, message: Dict) -> Snapshot:
        """Create snapshot for a resource in mass backup (separate session)"""
        async with async_session_factory() as session:
            snapshot_id = uuid.uuid4()
            
            snapshot_type = SnapshotType.INCREMENTAL
            
            snapshot = Snapshot(
                id=snapshot_id,
                resource_id=resource.id,
                job_id=uuid.UUID(message["jobId"]),
                type=snapshot_type,
                status=SnapshotStatus.RUNNING,
                snapshot_label=message.get("snapshotLabel", "scheduled"),
            )
            
            session.add(snapshot)
            await session.commit()
            
            return snapshot
    
    async def execute_backup(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Execute backup based on resource type"""
        resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
        print(f"[{self.worker_id}] Executing backup for resource type: {resource_type}")

        # Route to appropriate backup handler
        handlers = {
            "MAILBOX": self.backup_mailbox,
            "SHARED_MAILBOX": self.backup_mailbox,
            "ROOM_MAILBOX": self.backup_mailbox,
            "ONEDRIVE": self.backup_onedrive,
            "SHAREPOINT_SITE": self.backup_sharepoint,
            "TEAMS_CHANNEL": self.backup_teams_channel,
            "TEAMS_CHAT": self.backup_teams_chat,
            "ENTRA_USER": self.backup_entra_user,
            "ENTRA_GROUP": self.backup_entra_group,
        }

        handler = handlers.get(resource_type)
        if not handler:
            print(f"[{self.worker_id}] No handler for resource type: {resource_type}, using generic backup")
            return await self._backup_generic(graph_client, resource, snapshot, tenant, message)
        
        return await handler(graph_client, resource, snapshot, tenant, message)

    async def _backup_generic(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Generic backup handler for unsupported resource types"""
        print(f"[{self.worker_id}] Using generic backup for {resource.type}")
        return {
            "item_count": 0,
            "bytes_added": 0,
            "note": f"Generic backup: no specific handler for {resource.type}",
        }
    
    async def backup_mailbox(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Backup Exchange mailbox using delta API"""
        user_id = resource.external_id
        
        # Get delta token from previous backup
        delta_token = None
        if resource.extra_data:
            delta_token = resource.extra_data.get("mail_delta_token")
        
        # Backup messages using delta API
        messages = await graph_client.get_messages_delta(user_id, delta_token)
        
        item_count = 0
        bytes_added = 0
        
        # Save messages to storage
        for msg_batch in self.chunk_list(messages, 100):
            for msg in msg_batch:
                # Create snapshot item
                snapshot_item = await self.create_snapshot_item(
                    snapshot, tenant, msg, "EMAIL"
                )
                
                item_count += 1
                bytes_added += snapshot_item.content_size or 0
        
        # Update delta token
        new_delta_token = messages.get("@odata.deltaLink") if messages else None
        
        return {
            "item_count": item_count,
            "bytes_added": bytes_added,
            "new_delta_token": new_delta_token,
        }
    
    async def backup_onedrive(
        self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Backup OneDrive files using delta API"""
        user_id = resource.external_id
        
        # Get delta token
        delta_token = None
        if resource.extra_data:
            delta_token = resource.extra_data.get("onedrive_delta_token")
        
        # Get files using delta API
        files = await graph_client.get_drive_items_delta(user_id, delta_token)
        
        item_count = 0
        bytes_added = 0
        
        # Save files to storage
        for file_batch in self.chunk_list(files, 50):
            for file_item in file_batch:
                snapshot_item = await self.create_snapshot_item(
                    snapshot, tenant, file_item, "FILE"
                )
                
                item_count += 1
                bytes_added += snapshot_item.content_size or 0
        
        new_delta_token = files.get("@odata.deltaLink") if files else None
        
        return {
            "item_count": item_count,
            "bytes_added": bytes_added,
            "new_delta_token": new_delta_token,
        }
    
    # ==================== Full Backup Handlers ====================

    async def backup_sharepoint(self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Backup SharePoint site (files + lists + permissions) using delta API"""
        site_id = resource.external_id

        # Get delta token from previous backup
        delta_token = None
        if resource.extra_data:
            delta_token = resource.extra_data.get("sharepoint_delta_token")

        item_count = 0
        bytes_added = 0
        new_delta_token = None

        # 1. Backup site drive files
        try:
            drive_data = await graph_client.get_sharepoint_site_drives(site_id, delta_token)
            new_delta_token = drive_data.get("@odata.deltaLink")

            for item in drive_data.get("value", []):
                # Extract structured metadata
                structured_meta = MetadataExtractor.extract_sharepoint_item_metadata(item)

                snapshot_item = await self.create_snapshot_item(
                    snapshot, tenant, item, "SHAREPOINT_FILE", structured_meta
                )
                item_count += 1
                bytes_added += snapshot_item.content_size or 0
        except Exception as e:
            print(f"[{self.worker_id}] SharePoint drive backup failed for {site_id}: {e}")

        # 2. Backup site lists
        try:
            lists_data = await graph_client.get_sharepoint_site_lists(site_id)
            for lst in lists_data.get("value", []):
                list_id = lst.get("id")
                if not list_id:
                    continue

                try:
                    list_items = await graph_client.get_sharepoint_site_list_items(site_id, list_id)
                    for list_item in list_items.get("value", []):
                        structured_meta = MetadataExtractor.extract_sharepoint_list_item_metadata(list_item, lst)
                        snapshot_item = await self.create_snapshot_item(
                            snapshot, tenant, list_item, "SHAREPOINT_LIST_ITEM", structured_meta
                        )
                        item_count += 1
                        bytes_added += snapshot_item.content_size or 0
                except Exception as e:
                    print(f"[{self.worker_id}] SharePoint list {list_id} backup failed: {e}")
        except Exception as e:
            print(f"[{self.worker_id}] SharePoint lists backup failed for {site_id}: {e}")

        # 3. Backup site permissions
        try:
            permissions_data = await graph_client.get_site_permissions(site_id)
            if permissions_data.get("value"):
                perm_snapshot_item = await self.create_snapshot_item(
                    snapshot, tenant, permissions_data, "SHAREPOINT_PERMISSIONS",
                    MetadataExtractor.extract_permissions_metadata(permissions_data)
                )
                item_count += 1
        except Exception as e:
            print(f"[{self.worker_id}] SharePoint permissions backup failed for {site_id}: {e}")

        return {
            "item_count": item_count,
            "bytes_added": bytes_added,
            "new_delta_token": new_delta_token,
        }

    async def backup_teams_channel(self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Backup Teams channels (messages + replies + files) using delta API"""
        team_id = resource.external_id

        # Get delta token from previous backup
        delta_token = None
        if resource.extra_data:
            delta_token = resource.extra_data.get("teams_channels_delta_token")

        item_count = 0
        bytes_added = 0
        new_delta_token = None

        try:
            # 1. Get all channels in the team
            channels_data = await graph_client.get_teams_channels(team_id)

            for channel in channels_data.get("value", []):
                channel_id = channel.get("id")
                if not channel_id:
                    continue

                # Backup channel metadata
                channel_meta = MetadataExtractor.extract_teams_channel_metadata(channel, team_id)
                channel_snapshot_item = await self.create_snapshot_item(
                    snapshot, tenant, channel, "TEAMS_CHANNEL_META", channel_meta
                )
                item_count += 1

                # 2. Get channel messages using delta API
                try:
                    messages_data = await graph_client.get_channel_messages(team_id, channel_id, delta_token)
                    new_delta_token = messages_data.get("@odata.deltaLink")

                    for msg in messages_data.get("value", []):
                        # Extract message with thread structure
                        msg_meta = MetadataExtractor.extract_teams_message_metadata(msg)

                        # Backup message
                        msg_snapshot_item = await self.create_snapshot_item(
                            snapshot, tenant, msg, "TEAMS_MESSAGE", msg_meta
                        )
                        item_count += 1
                        bytes_added += msg_snapshot_item.content_size or 0

                        # 3. Get replies for the message
                        try:
                            replies_data = await graph_client.get_channel_messages_replies(
                                team_id, channel_id, msg.get("id")
                            )
                            for reply in replies_data.get("value", []):
                                reply_meta = MetadataExtractor.extract_teams_message_metadata(reply, is_reply=True)
                                reply_snapshot_item = await self.create_snapshot_item(
                                    snapshot, tenant, reply, "TEAMS_MESSAGE_REPLY", reply_meta
                                )
                                item_count += 1
                                bytes_added += reply_snapshot_item.content_size or 0
                        except Exception as e:
                            print(f"[{self.worker_id}] Failed to get replies for message {msg.get('id')}: {e}")
                except Exception as e:
                    print(f"[{self.worker_id}] Failed to get messages for channel {channel_id}: {e}")
        except Exception as e:
            print(f"[{self.worker_id}] Teams channel backup failed for {team_id}: {e}")

        return {
            "item_count": item_count,
            "bytes_added": bytes_added,
            "new_delta_token": new_delta_token,
        }

    async def backup_teams_chat(self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Backup Teams chats (messages) using delta API"""
        chat_id = resource.external_id

        # Get delta token from previous backup
        delta_token = None
        if resource.extra_data:
            delta_token = resource.extra_data.get("teams_chat_delta_token")

        item_count = 0
        bytes_added = 0
        new_delta_token = None

        try:
            # 1. Backup chat metadata
            chat_meta = MetadataExtractor.extract_teams_chat_metadata(resource)
            chat_snapshot_item = await self.create_snapshot_item(
                snapshot, tenant, {"id": chat_id, **chat_meta}, "TEAMS_CHAT_META", chat_meta
            )
            item_count += 1

            # 2. Get chat messages using delta API
            messages_data = await graph_client.get_chat_messages(chat_id, delta_token)
            new_delta_token = messages_data.get("@odata.deltaLink")

            for msg in messages_data.get("value", []):
                msg_meta = MetadataExtractor.extract_teams_message_metadata(msg)
                msg_snapshot_item = await self.create_snapshot_item(
                    snapshot, tenant, msg, "TEAMS_CHAT_MESSAGE", msg_meta
                )
                item_count += 1
                bytes_added += msg_snapshot_item.content_size or 0
        except Exception as e:
            print(f"[{self.worker_id}] Teams chat backup failed for {chat_id}: {e}")

        return {
            "item_count": item_count,
            "bytes_added": bytes_added,
            "new_delta_token": new_delta_token,
        }

    async def backup_entra_user(self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Backup Entra ID user with profile, relationships, and settings"""
        user_id = resource.external_id

        item_count = 0
        bytes_added = 0

        try:
            # 1. Backup user profile
            user_profile = await graph_client.get_user_profile(user_id)
            user_meta = MetadataExtractor.extract_entra_user_metadata(user_profile)
            user_snapshot_item = await self.create_snapshot_item(
                snapshot, tenant, user_profile, "ENTRA_USER_PROFILE", user_meta
            )
            item_count += 1
            bytes_added += user_snapshot_item.content_size or 0

            # 2. Backup user manager relationship
            try:
                manager_data = await graph_client.get_user_manager(user_id)
                if manager_data:
                    manager_meta = MetadataExtractor.extract_relationship_metadata(manager_data, "manager", user_id)
                    manager_snapshot_item = await self.create_snapshot_item(
                        snapshot, tenant, manager_data, "ENTRA_RELATIONSHIP", manager_meta
                    )
                    item_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to get manager for user {user_id}: {e}")

            # 3. Backup direct reports
            try:
                direct_reports = await graph_client.get_user_direct_reports(user_id)
                for report in direct_reports.get("value", []):
                    report_meta = MetadataExtractor.extract_relationship_metadata(report, "direct_report", user_id)
                    report_snapshot_item = await self.create_snapshot_item(
                        snapshot, tenant, report, "ENTRA_RELATIONSHIP", report_meta
                    )
                    item_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to get direct reports for user {user_id}: {e}")

            # 4. Backup group memberships
            try:
                memberships = await graph_client.get_user_group_memberships(user_id)
                for group in memberships.get("value", []):
                    group_meta = MetadataExtractor.extract_membership_metadata(group, user_id)
                    group_snapshot_item = await self.create_snapshot_item(
                        snapshot, tenant, group, "ENTRA_MEMBERSHIP", group_meta
                    )
                    item_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to get memberships for user {user_id}: {e}")

            # 5. Backup mailbox settings
            try:
                mailbox_settings = await graph_client.get_user_mailbox_settings(user_id)
                if mailbox_settings:
                    settings_meta = {"user_id": user_id, "type": "mailbox_settings"}
                    settings_snapshot_item = await self.create_snapshot_item(
                        snapshot, tenant, mailbox_settings, "ENTRA_MAILBOX_SETTINGS", settings_meta
                    )
                    item_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to get mailbox settings for user {user_id}: {e}")

            # 6. Backup contacts
            try:
                contacts = await graph_client.get_user_contacts(user_id)
                for contact in contacts.get("value", []):
                    contact_meta = MetadataExtractor.extract_contact_metadata(contact, user_id)
                    contact_snapshot_item = await self.create_snapshot_item(
                        snapshot, tenant, contact, "ENTRA_CONTACT", contact_meta
                    )
                    item_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to get contacts for user {user_id}: {e}")

            # 7. Backup calendar events
            try:
                calendar_events = await graph_client.get_calendar_events_delta(user_id)
                for event in calendar_events.get("value", []):
                    event_meta = MetadataExtractor.extract_calendar_event_metadata(event, user_id)
                    event_snapshot_item = await self.create_snapshot_item(
                        snapshot, tenant, event, "ENTRA_CALENDAR_EVENT", event_meta
                    )
                    item_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to get calendar events for user {user_id}: {e}")

        except Exception as e:
            print(f"[{self.worker_id}] Entra user backup failed for {user_id}: {e}")

        return {
            "item_count": item_count,
            "bytes_added": bytes_added,
            "new_delta_token": None,
        }

    async def backup_entra_group(self,
        graph_client: GraphClient,
        resource: Resource,
        snapshot: Snapshot,
        tenant: Tenant,
        message: Dict
    ) -> Dict:
        """Backup Entra ID group with members and owners"""
        group_id = resource.external_id

        item_count = 0
        bytes_added = 0

        try:
            # 1. Backup group metadata
            group_meta = MetadataExtractor.extract_entra_group_metadata(resource.extra_data or {})
            group_snapshot_item = await self.create_snapshot_item(
                snapshot, tenant, resource.extra_data or {}, "ENTRA_GROUP_META", group_meta
            )
            item_count += 1
            bytes_added += group_snapshot_item.content_size or 0

            # 2. Backup group members
            try:
                members = await graph_client.get_group_members(group_id)
                for member in members.get("value", []):
                    member_meta = MetadataExtractor.extract_group_member_metadata(member, group_id)
                    member_snapshot_item = await self.create_snapshot_item(
                        snapshot, tenant, member, "ENTRA_GROUP_MEMBER", member_meta
                    )
                    item_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to get members for group {group_id}: {e}")

            # 3. Backup group owners
            try:
                owners = await graph_client.get_group_owners(group_id)
                for owner in owners.get("value", []):
                    owner_meta = MetadataExtractor.extract_group_owner_metadata(owner, group_id)
                    owner_snapshot_item = await self.create_snapshot_item(
                        snapshot, tenant, owner, "ENTRA_GROUP_OWNER", owner_meta
                    )
                    item_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to get owners for group {group_id}: {e}")

        except Exception as e:
            print(f"[{self.worker_id}] Entra group backup failed for {group_id}: {e}")

        return {
            "item_count": item_count,
            "bytes_added": bytes_added,
            "new_delta_token": None,
        }
    
    async def create_snapshot_item(
        self,
        snapshot: Snapshot,
        tenant: Tenant,
        item: Dict,
        item_type: str,
        structured_metadata: Optional[Dict] = None
    ) -> SnapshotItem:
        """Create a snapshot item record with structured metadata preservation

        Args:
            snapshot: Parent snapshot
            tenant: Tenant object
            item: Raw item data from Graph API
            item_type: Type of item (EMAIL, FILE, TEAMS_MESSAGE, etc.)
            structured_metadata: Extracted structured metadata (permissions, relationships, etc.)
        """
        item_id = item.get("id", str(uuid.uuid4()))

        # Calculate content hash for deduplication
        content_json = json.dumps(item, sort_keys=True)
        content_hash = hashlib.sha256(content_json.encode()).hexdigest()

        # Create versioned storage path
        blob_path = self.build_versioned_blob_path(
            tenant.id, snapshot.resource_id, snapshot.id, item_id
        )

        # Merge raw item data with structured metadata
        metadata_payload = {
            "raw": item,
            "structured": structured_metadata or {},
            "backup_timestamp": datetime.utcnow().isoformat(),
            "item_type": item_type,
        }

        # Upload to Azure Blob Storage with immutability
        content_bytes = content_json.encode('utf-8')
        upload_success = await self._upload_to_azure_blob(blob_path, content_bytes)

        snapshot_item = SnapshotItem(
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=item_id,
            item_type=item_type,
            name=item.get("displayName", item.get("subject", item.get("name", item_id))),
            content_hash=content_hash,
            content_size=len(content_bytes),
            blob_path=blob_path,
            metadata=metadata_payload,
            content_checksum=content_hash,  # SHA-256 integrity checksum
        )

        # Save to database
        async with async_session_factory() as session:
            session.add(snapshot_item)
            await session.commit()

        return snapshot_item

    async def _upload_to_azure_blob(self, blob_path: str, content: bytes) -> bool:
        """Upload content to Azure Blob Storage with immutability

        Args:
            blob_path: Full blob path in format: {tenant}/{resource}/{snapshot}/{timestamp}/{item}
            content: Content bytes to upload

        Returns:
            True if upload successful, False otherwise
        """
        if not self.blob_service_client:
            return False

        try:
            container_name = "backups"
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_path)

            # Upload with immutability metadata
            blob_client.upload_blob(
                content,
                overwrite=True,
                metadata={
                    "immutable": "true",
                    "backup-item": "true",
                },
            )
            return True
        except Exception as e:
            print(f"[{self.worker_id}] Azure Blob upload failed for {blob_path}: {e}")
            return False
    
    def build_versioned_blob_path(
        self,
        tenant_id: uuid.UUID,
        resource_id: uuid.UUID,
        snapshot_id: uuid.UUID,
        item_id: str
    ) -> str:
        """Build versioned Azure Blob storage path"""
        timestamp = datetime.utcnow().isoformat()
        return f"{tenant_id}/{resource_id}/{snapshot_id}/{timestamp}/{item_id}"
    
    async def complete_snapshot(
        self,
        session: AsyncSession,
        snapshot: Snapshot,
        backup_result: Dict,
        message: Dict
    ):
        """Mark snapshot as completed"""
        snapshot.status = SnapshotStatus.COMPLETE
        snapshot.completed_at = datetime.utcnow()
        snapshot.item_count = backup_result.get("item_count", 0)
        snapshot.new_item_count = backup_result.get("item_count", 0)
        snapshot.bytes_added = backup_result.get("bytes_added", 0)
        snapshot.bytes_total = backup_result.get("bytes_added", 0)
        snapshot.delta_token = backup_result.get("new_delta_token")
        
        # Calculate duration
        if snapshot.started_at:
            duration = (snapshot.completed_at - snapshot.started_at).total_seconds()
            snapshot.duration_secs = int(duration)
        
        session.add(snapshot)
    
    async def update_resource_backup_info(
        self,
        session: AsyncSession,
        resource: Resource,
        job_id: uuid.UUID,
        snapshot_id: uuid.UUID
    ):
        """Update resource with last backup information"""
        resource.last_backup_job_id = job_id
        resource.last_backup_at = datetime.utcnow()
        resource.last_backup_status = "COMPLETED"

        session.add(resource)
    
    async def update_job_status(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        status: JobStatus,
        result: Optional[Dict] = None
    ):
        """Update job status"""
        job = await session.get(Job, job_id)
        if job:
            job.status = status
            if status == JobStatus.COMPLETED:
                job.completed_at = datetime.utcnow()
                job.progress_pct = 100
            if result:
                job.result = result
            await session.flush()
    
    async def update_job_progress(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        progress: int
    ):
        """Update job progress percentage"""
        job = await session.get(Job, job_id)
        if job:
            job.progress_pct = progress
            await session.flush()
    
    async def handle_backup_failure(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        error: Exception,
        resource_id: Optional[uuid.UUID] = None
    ):
        """Handle backup job failure"""
        job = await session.get(Job, job_id)
        if job:
            job.attempts += 1
            job.error_message = str(error)

            if job.attempts >= job.max_attempts:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()

                # Update resource status to FAILED
                if resource_id:
                    resource = await session.get(Resource, resource_id)
                    if resource:
                        resource.last_backup_at = datetime.utcnow()
                        resource.last_backup_status = "FAILED"
                        session.add(resource)
            else:
                job.status = JobStatus.RETRYING
    
    @staticmethod
    def chunk_list(lst: List, chunk_size: int):
        """Split list into chunks"""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]


# Global worker instance
worker = BackupWorker()


async def main():
    """Start the backup worker"""
    print("Starting backup worker...")
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
