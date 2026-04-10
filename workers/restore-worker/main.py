"""Restore Worker - Processes restore jobs from RabbitMQ queues

Handles different restore types:
- In-place restore (restore to original location)
- Cross-user restore (restore to different user/resource)
- Export (download as PST, ZIP, etc.)
"""
import asyncio
import json
import uuid
import zipfile
import io
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
    ResourceType, JobStatus, JobType, SnapshotStatus
)
from shared.message_bus import message_bus
from shared.config import settings
from shared.graph_client import GraphClient
from shared.multi_app_manager import multi_app_manager


class RestoreWorker:
    """Main restore worker that processes restore jobs from RabbitMQ queues"""

    def __init__(self):
        self.worker_id = f"restore-worker-{uuid.uuid4().hex[:8]}"
        self.graph_clients: Dict[str, GraphClient] = {}
        self.blob_service_client: Optional[BlobServiceClient] = None
        self.semaphore = asyncio.Semaphore(30)  # Max 30 concurrent restores

    async def initialize(self):
        """Initialize connections and clients"""
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

        print(f"[{self.worker_id}] Restore worker initialized")

    async def start(self):
        """Start consuming from restore queues"""
        # Wait for RabbitMQ to be ready (retry loop)
        max_retries = 30
        for attempt in range(max_retries):
            try:
                await self.initialize()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"[{self.worker_id}] RabbitMQ not ready (attempt {attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(5)
                else:
                    print(f"[{self.worker_id}] Failed to connect to RabbitMQ after {max_retries} attempts")
                    raise

        queues = [
            ("restore.urgent", 10),
            ("restore.normal", 30),
            ("restore.low", 50),
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
            return

        queue = await message_bus.channel.get_queue(queue_name)

        async for message in queue:
            async with message.process():
                try:
                    body = json.loads(message.body.decode())
                    await self.process_restore_message(body)
                except Exception as e:
                    print(f"[{self.worker_id}] Error processing restore message: {e}")

    async def process_restore_message(self, message: Dict[str, Any]):
        """Process a single restore job message"""
        job_id = uuid.UUID(message["jobId"])
        restore_type = message.get("restoreType", "IN_PLACE")
        spec = message.get("spec", {})

        async with self.semaphore:
            async with async_session_factory() as session:
                try:
                    # Update job status
                    await self.update_job_status(session, job_id, JobStatus.RUNNING)

                    # Fetch snapshot items to restore
                    snapshot_ids = message.get("snapshotIds", [])
                    item_ids = message.get("itemIds", [])

                    items_to_restore = await self.fetch_snapshot_items(session, snapshot_ids, item_ids)

                    if not items_to_restore:
                        raise ValueError("No snapshot items found to restore")

                    # Route to appropriate restore handler
                    handlers = {
                        "IN_PLACE": self.restore_in_place,
                        "CROSS_USER": self.restore_cross_user,
                        "CROSS_RESOURCE": self.restore_cross_resource,
                        "EXPORT_PST": self.export_as_pst,
                        "EXPORT_ZIP": self.export_as_zip,
                        "DOWNLOAD": self.export_download,
                    }

                    handler = handlers.get(restore_type, self.export_download)
                    result = await handler(session, items_to_restore, message, spec)

                    # Update job as completed
                    await self.update_job_status(session, job_id, JobStatus.COMPLETED, result)
                    await session.commit()

                    # Log audit event
                    await self.log_audit_event(job_id, message, result)

                    print(f"[{self.worker_id}] Restore job {job_id} completed: {restore_type}")

                except Exception as e:
                    await session.rollback()
                    await self.handle_restore_failure(session, job_id, e)
                    print(f"[{self.worker_id}] Restore job {job_id} failed: {e}")
                    raise

    async def fetch_snapshot_items(
        self,
        session: AsyncSession,
        snapshot_ids: List[str],
        item_ids: List[str]
    ) -> List[SnapshotItem]:
        """Fetch snapshot items to restore"""
        stmt = select(SnapshotItem)

        if item_ids:
            stmt = stmt.where(SnapshotItem.id.in_([uuid.UUID(iid) for iid in item_ids]))
        elif snapshot_ids:
            stmt = stmt.where(SnapshotItem.snapshot_id.in_([uuid.UUID(sid) for sid in snapshot_ids]))
        else:
            return []

        result = await session.execute(stmt)
        return result.scalars().all()

    # ==================== Restore Handlers ====================

    async def restore_in_place(
        self,
        session: AsyncSession,
        items: List[SnapshotItem],
        message: Dict,
        spec: Dict
    ) -> Dict:
        """Restore items to their original location"""
        restored_count = 0
        failed_count = 0

        # Group items by resource to batch restore
        resource_groups: Dict[str, List[SnapshotItem]] = {}
        for item in items:
            # Get resource from snapshot
            snapshot = await session.get(Snapshot, item.snapshot_id)
            if snapshot:
                resource_id = str(snapshot.resource_id)
                if resource_id not in resource_groups:
                    resource_groups[resource_id] = []
                resource_groups[resource_id].append(item)

        for resource_id, resource_items in resource_groups.items():
            # Fetch resource
            resource = await session.get(Resource, uuid.UUID(resource_id))
            if not resource:
                failed_count += len(resource_items)
                continue

            # Get Graph client
            tenant = await session.get(Tenant, resource.tenant_id)
            if not tenant:
                failed_count += len(resource_items)
                continue

            graph_client = await self.get_graph_client(tenant)

            # Route by item type
            for item in resource_items:
                try:
                    if item.item_type in ("EMAIL",):
                        await self._restore_email_to_mailbox(graph_client, resource, item)
                    elif item.item_type in ("FILE", "ONEDRIVE_FILE"):
                        await self._restore_file_to_onedrive(graph_client, resource, item)
                    elif item.item_type in ("SHAREPOINT_FILE", "SHAREPOINT_LIST_ITEM"):
                        await self._restore_file_to_sharepoint(graph_client, resource, item)
                    elif item.item_type in ("TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"):
                        # Teams messages cannot be restored via API - export only
                        print(f"[{self.worker_id}] Teams messages cannot be restored in-place - skipping")
                        failed_count += 1
                        continue
                    elif item.item_type in ("ENTRA_USER_PROFILE",):
                        await self._restore_entra_user(graph_client, resource, item)
                    elif item.item_type in ("ENTRA_GROUP_META",):
                        await self._restore_entra_group(graph_client, resource, item)
                    else:
                        print(f"[{self.worker_id}] Unknown item type for in-place restore: {item.item_type}")
                        failed_count += 1
                        continue

                    restored_count += 1
                except Exception as e:
                    print(f"[{self.worker_id}] Failed to restore item {item.id}: {e}")
                    failed_count += 1

        return {
            "restored_count": restored_count,
            "failed_count": failed_count,
            "restore_type": "IN_PLACE",
        }

    async def restore_cross_user(
        self,
        session: AsyncSession,
        items: List[SnapshotItem],
        message: Dict,
        spec: Dict
    ) -> Dict:
        """Restore items to a different user/resource"""
        target_user_id = spec.get("targetUserId") or spec.get("targetResourceId")
        if not target_user_id:
            raise ValueError("targetUserId is required for cross-user restore")

        restored_count = 0
        failed_count = 0

        # Fetch target resource
        target_resource = await session.execute(
            select(Resource).where(
                and_(
                    Resource.external_id == target_user_id,
                    Resource.status == "ACTIVE"
                )
            )
        )
        target_resource = target_resource.scalars().first()

        if not target_resource:
            raise ValueError(f"Target resource {target_user_id} not found")

        # Get Graph client
        tenant = await session.get(Tenant, target_resource.tenant_id)
        if not tenant:
            raise ValueError("Target tenant not found")

        graph_client = await self.get_graph_client(tenant)

        for item in items:
            try:
                if item.item_type in ("EMAIL",):
                    await self._restore_email_to_mailbox(graph_client, target_resource, item)
                elif item.item_type in ("FILE", "ONEDRIVE_FILE"):
                    await self._restore_file_to_onedrive(graph_client, target_resource, item)
                elif item.item_type in ("SHAREPOINT_FILE",):
                    await self._restore_file_to_sharepoint(graph_client, target_resource, item)
                else:
                    print(f"[{self.worker_id}] Cross-user restore not supported for: {item.item_type}")
                    failed_count += 1
                    continue

                restored_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to cross-restore item {item.id}: {e}")
                failed_count += 1

        return {
            "restored_count": restored_count,
            "failed_count": failed_count,
            "restore_type": "CROSS_USER",
            "target_resource_id": target_user_id,
        }

    async def restore_cross_resource(
        self,
        session: AsyncSession,
        items: List[SnapshotItem],
        message: Dict,
        spec: Dict
    ) -> Dict:
        """Restore items to a different resource type (e.g., mailbox to SharePoint)"""
        target_resource_id = spec.get("targetResourceId")
        if not target_resource_id:
            raise ValueError("targetResourceId is required for cross-resource restore")

        # Similar to cross-user but allows different resource types
        restored_count = 0
        failed_count = 0

        target_resource = await session.get(Resource, uuid.UUID(target_resource_id))
        if not target_resource:
            raise ValueError(f"Target resource {target_resource_id} not found")

        tenant = await session.get(Tenant, target_resource.tenant_id)
        if not tenant:
            raise ValueError("Target tenant not found")

        graph_client = await self.get_graph_client(tenant)

        for item in items:
            try:
                # Restore based on target resource type
                if target_resource.type.value in ("MAILBOX", "SHARED_MAILBOX"):
                    await self._restore_email_to_mailbox(graph_client, target_resource, item)
                elif target_resource.type.value == "ONEDRIVE":
                    await self._restore_file_to_onedrive(graph_client, target_resource, item)
                elif target_resource.type.value == "SHAREPOINT_SITE":
                    await self._restore_file_to_sharepoint(graph_client, target_resource, item)
                else:
                    failed_count += 1
                    continue

                restored_count += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed to cross-restore item {item.id}: {e}")
                failed_count += 1

        return {
            "restored_count": restored_count,
            "failed_count": failed_count,
            "restore_type": "CROSS_RESOURCE",
        }

    async def export_as_pst(
        self,
        session: AsyncSession,
        items: List[SnapshotItem],
        message: Dict,
        spec: Dict
    ) -> Dict:
        """Export items as PST file (for email backups)"""
        # Note: Actual PST generation requires Exchange Web Services or third-party library
        # For now, export as ZIP with MSG/EML files
        print(f"[{self.worker_id}] PST export requested - exporting as ZIP instead")
        return await self.export_as_zip(session, items, message, spec)

    async def export_as_zip(
        self,
        session: AsyncSession,
        items: List[SnapshotItem],
        message: Dict,
        spec: Dict
    ) -> Dict:
        """Export items as downloadable ZIP file"""
        zip_buffer = io.BytesIO()
        exported_count = 0

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for item in items:
                try:
                    # Get item content from metadata
                    metadata = item.metadata or {}
                    raw_data = metadata.get("raw", {})

                    # Create file based on item type
                    if item.item_type in ("EMAIL",):
                        # Create EML file
                        eml_content = self._create_eml_from_json(raw_data)
                        zip_file.writestr(
                            f"emails/{item.name or item.external_id}.eml",
                            eml_content
                        )
                    elif item.item_type in ("FILE", "ONEDRIVE_FILE", "SHAREPOINT_FILE"):
                        # Add file content if available
                        content = raw_data.get("content", json.dumps(raw_data, indent=2))
                        zip_file.writestr(
                            f"files/{item.name or item.external_id}.json",
                            content if isinstance(content, str) else json.dumps(content, indent=2)
                        )
                    elif item.item_type in ("TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY", "TEAMS_CHAT_MESSAGE"):
                        # Export Teams message as JSON
                        zip_file.writestr(
                            f"teams_messages/{item.external_id}.json",
                            json.dumps(raw_data, indent=2)
                        )
                    else:
                        # Generic JSON export
                        zip_file.writestr(
                            f"items/{item.item_type}/{item.external_id}.json",
                            json.dumps(raw_data, indent=2)
                        )

                    exported_count += 1
                except Exception as e:
                    print(f"[{self.worker_id}] Failed to export item {item.id}: {e}")

        zip_buffer.seek(0)
        zip_size = zip_buffer.getbuffer().nbytes

        # Upload ZIP to Azure Blob for download
        container_name = "exports"
        blob_name = f"exports/{message.get('jobId')}/export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"

        if self.blob_service_client:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(zip_buffer.getvalue(), overwrite=True)

        return {
            "exported_count": exported_count,
            "export_type": "ZIP",
            "download_url": f"/api/v1/exports/{message.get('jobId')}/download",
            "blob_path": blob_name,
            "file_size": zip_size,
        }

    async def export_download(
        self,
        session: AsyncSession,
        items: List[SnapshotItem],
        message: Dict,
        spec: Dict
    ) -> Dict:
        """Export items as direct download (JSON)"""
        export_data = []
        for item in items:
            metadata = item.metadata or {}
            export_data.append({
                "id": str(item.id),
                "type": item.item_type,
                "name": item.name,
                "external_id": item.external_id,
                "content": metadata.get("raw", {}),
                "structured_metadata": metadata.get("structured", {}),
            })

        return {
            "exported_count": len(export_data),
            "export_type": "JSON",
            "data": export_data,
        }

    # ==================== Low-Level Restore Methods ====================

    async def _restore_email_to_mailbox(
        self,
        graph_client: GraphClient,
        resource: Resource,
        item: SnapshotItem
    ):
        """Restore email to Exchange mailbox via Graph API"""
        metadata = item.metadata or {}
        raw_data = metadata.get("raw", {})

        user_id = resource.external_id

        # Create message in mailbox
        message_payload = {
            "subject": raw_data.get("subject"),
            "body": {
                "contentType": raw_data.get("body", {}).get("contentType", "HTML"),
                "content": raw_data.get("body", {}).get("content", ""),
            },
            "toRecipients": raw_data.get("toRecipients", []),
            "ccRecipients": raw_data.get("ccRecipients", []),
            "hasAttachments": raw_data.get("hasAttachments", False),
            "internetMessageId": raw_data.get("internetMessageId"),
            # Preserve original metadata
            "receivedDateTime": raw_data.get("receivedDateTime"),
            "sentDateTime": raw_data.get("sentDateTime"),
        }

        # POST to /users/{id}/messages
        await graph_client._post(
            f"{graph_client.GRAPH_URL}/users/{user_id}/messages",
            message_payload
        )

    async def _restore_file_to_onedrive(
        self,
        graph_client: GraphClient,
        resource: Resource,
        item: SnapshotItem
    ):
        """Restore file to OneDrive via Graph API"""
        metadata = item.metadata or {}
        raw_data = metadata.get("raw", {})

        user_id = resource.external_id

        # Upload file content
        file_content = raw_data.get("content", "")
        file_name = raw_data.get("name", item.name or f"restored_{item.external_id}")

        # Determine parent folder
        parent_ref = raw_data.get("parentReference", {})
        folder_id = parent_ref.get("id", "root")

        # PUT to /users/{id}/drive/root/children/{name}/content
        # or POST to /users/{id}/drive/root:/path/{name}:/content
        url = f"{graph_client.GRAPH_URL}/users/{user_id}/drive/root:/{file_name}:/content"

        await graph_client._put(
            url,
            content=file_content,
            headers={"Content-Type": "application/octet-stream"}
        )

    async def _restore_file_to_sharepoint(
        self,
        graph_client: GraphClient,
        resource: Resource,
        item: SnapshotItem
    ):
        """Restore file to SharePoint site via Graph API"""
        metadata = item.metadata or {}
        raw_data = metadata.get("raw", {})

        site_id = resource.external_id

        # Upload file content
        file_content = raw_data.get("content", "")
        file_name = raw_data.get("name", item.name or f"restored_{item.external_id}")

        # Determine parent folder
        parent_ref = raw_data.get("parentReference", {})
        folder_path = parent_ref.get("path", "")

        url = f"{graph_client.GRAPH_URL}/sites/{site_id}/drive/root:/{file_name}:/content"

        await graph_client._put(
            url,
            content=file_content,
            headers={"Content-Type": "application/octet-stream"}
        )

    async def _restore_entra_user(
        self,
        graph_client: GraphClient,
        resource: Resource,
        item: SnapshotItem
    ):
        """Restore Entra ID user profile via Graph API"""
        metadata = item.metadata or {}
        raw_data = metadata.get("raw", {})

        user_id = resource.external_id

        # PATCH user properties
        # Note: Some properties cannot be restored (e.g., createdDateTime)
        update_payload = {
            "displayName": raw_data.get("displayName"),
            "givenName": raw_data.get("givenName"),
            "surname": raw_data.get("surname"),
            "jobTitle": raw_data.get("jobTitle"),
            "department": raw_data.get("department"),
            "officeLocation": raw_data.get("officeLocation"),
            "mobilePhone": raw_data.get("mobilePhone"),
            "businessPhones": raw_data.get("businessPhones", []),
        }

        await graph_client._patch(
            f"{graph_client.GRAPH_URL}/users/{user_id}",
            update_payload
        )

    async def _restore_entra_group(
        self,
        graph_client: GraphClient,
        resource: Resource,
        item: SnapshotItem
    ):
        """Restore Entra ID group via Graph API"""
        metadata = item.metadata or {}
        raw_data = metadata.get("raw", {})

        group_id = resource.external_id

        # PATCH group properties
        update_payload = {
            "displayName": raw_data.get("displayName"),
            "description": raw_data.get("description"),
            "mailEnabled": raw_data.get("mailEnabled"),
            "securityEnabled": raw_data.get("securityEnabled"),
        }

        await graph_client._patch(
            f"{graph_client.GRAPH_URL}/groups/{group_id}",
            update_payload
        )

    # ==================== Utility Methods ====================

    def _create_eml_from_json(self, email_data: Dict) -> str:
        """Create EML file content from email JSON"""
        subject = email_data.get("subject", "No Subject")
        body = email_data.get("body", {}).get("content", "")
        from_addr = email_data.get("from", {}).get("emailAddress", {}).get("address", "unknown@unknown.com")
        to_addrs = ", ".join([
            r.get("emailAddress", {}).get("address", "")
            for r in email_data.get("toRecipients", [])
        ])
        date = email_data.get("sentDateTime", email_data.get("receivedDateTime", ""))

        eml = f"From: {from_addr}\r\n"
        eml += f"To: {to_addrs}\r\n"
        eml += f"Subject: {subject}\r\n"
        eml += f"Date: {date}\r\n"
        eml += f"Content-Type: text/html; charset=\"utf-8\"\r\n"
        eml += f"\r\n"
        eml += f"{body}\r\n"

        return eml

    async def get_graph_client(self, tenant: Tenant) -> GraphClient:
        """Get Graph client for a tenant using next available app registration"""
        app = multi_app_manager.get_next_app()
        return GraphClient(
            client_id=app.client_id,
            client_secret=app.client_secret,
            tenant_id=tenant.external_tenant_id,
        )

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

    async def handle_restore_failure(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        error: Exception
    ):
        """Handle restore job failure"""
        job = await session.get(Job, job_id)
        if job:
            job.attempts += 1
            job.error_message = str(error)

            if job.attempts >= job.max_attempts:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
            else:
                job.status = JobStatus.RETRYING

    async def log_audit_event(self, job_id: uuid.UUID, message: Dict, result: Dict):
        """Log restore audit event"""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post("http://audit-service:8012/api/v1/audit/log", json={
                    "action": "RESTORE_COMPLETED",
                    "tenant_id": message.get("tenantId"),
                    "org_id": None,
                    "actor_type": "WORKER",
                    "resource_id": message.get("resourceId"),
                    "resource_type": message.get("resourceType"),
                    "outcome": "SUCCESS",
                    "job_id": str(job_id),
                    "details": {
                        "restore_type": message.get("restoreType", "IN_PLACE"),
                        "restored_count": result.get("restored_count", result.get("exported_count", 0)),
                        "failed_count": result.get("failed_count", 0),
                    },
                })
        except Exception as e:
            print(f"[{self.worker_id}] Failed to log audit event: {e}")


# Global worker instance
worker = RestoreWorker()


async def main():
    """Start the restore worker"""
    print("Starting restore worker...")
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
