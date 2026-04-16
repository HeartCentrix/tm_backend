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
from shared.power_bi_client import PowerBIClient
from shared.power_platform_client import PowerPlatformClient
from shared.azure_storage import azure_storage_manager


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

            resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
            target_env_id = spec.get("targetEnvironmentId")
            if resource_type == "POWER_BI":
                power_bi_result = await self._restore_power_bi_items(session, resource, resource_items, tenant)
                restored_count += power_bi_result.get("restored_count", 0)
                failed_count += power_bi_result.get("failed_count", 0)
                continue
            if resource_type == "POWER_APPS":
                result = await self._restore_power_app_items(session, resource, resource_items, tenant, target_env_id)
                restored_count += result.get("restored_count", 0)
                failed_count += result.get("failed_count", 0)
                continue
            if resource_type == "POWER_AUTOMATE":
                result = await self._restore_power_flow_items(session, resource, resource_items, tenant, target_env_id)
                restored_count += result.get("restored_count", 0)
                failed_count += result.get("failed_count", 0)
                continue
            if resource_type == "POWER_DLP":
                result = await self._restore_power_dlp_items(session, resource, resource_items, tenant)
                restored_count += result.get("restored_count", 0)
                failed_count += result.get("failed_count", 0)
                continue

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

        target_resource_type = target_resource.type.value if hasattr(target_resource.type, "value") else str(target_resource.type)
        if target_resource_type == "POWER_BI":
            return await self._restore_power_bi_items(session, target_resource, items, tenant)

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
                    metadata = self._get_item_metadata(item)
                    raw_data = self._load_snapshot_item_payload(
                        item,
                        "power-bi" if item.item_type.startswith("POWER_BI") else "files",
                    )

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
                    elif item.item_type.startswith("POWER_BI"):
                        zip_file.writestr(
                            f"power_bi/{item.item_type}/{item.external_id}.json",
                            json.dumps(raw_data, indent=2),
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
            metadata = self._get_item_metadata(item)
            raw_data = self._load_snapshot_item_payload(
                item,
                "power-bi" if item.item_type.startswith("POWER_BI") else "files",
            )
            export_data.append({
                "id": str(item.id),
                "type": item.item_type,
                "name": item.name,
                "external_id": item.external_id,
                "content": raw_data,
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
        metadata = self._get_item_metadata(item)
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
        metadata = self._get_item_metadata(item)
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
        metadata = self._get_item_metadata(item)
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
        metadata = self._get_item_metadata(item)
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
        metadata = self._get_item_metadata(item)
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

    async def _restore_power_bi_items(
        self,
        session: AsyncSession,
        target_resource: Resource,
        items: List[SnapshotItem],
        tenant: Tenant,
    ) -> Dict[str, Any]:
        workspace_id = self._extract_power_bi_workspace_id(target_resource)
        if not workspace_id:
            raise ValueError(f"POWER_BI target resource {target_resource.id} is missing workspace_id")

        power_bi_client = self.get_power_bi_client(tenant)
        existing_items = await power_bi_client.list_fabric_items(workspace_id)
        existing_lookup = {
            (item.get("type"), item.get("displayName")): item
            for item in existing_items
            if item.get("type") and item.get("displayName")
        }

        restore_priority = {
            "POWER_BI_DATAFLOW": 10,
            "POWER_BI_SEMANTIC_MODEL": 20,
            "POWER_BI_REPORT": 30,
            "POWER_BI_PAGINATED_REPORT": 31,
            "POWER_BI_DASHBOARD": 40,
            "POWER_BI_TILE": 41,
        }
        ordered_items = sorted(items, key=lambda item: restore_priority.get(item.item_type, 100))

        restored_count = 0
        failed_count = 0
        manual_actions: List[str] = []
        semantic_model_map: Dict[str, str] = {}

        for item in ordered_items:
            metadata = self._get_item_metadata(item)
            if item.is_deleted:
                manual_actions.append(f"{item.name}: source artifact is deleted and cannot be replayed directly.")
                continue

            if not metadata.get("restore_supported"):
                manual_actions.extend(metadata.get("manual_actions", []) or [f"{item.name}: manual restore required."])
                continue

            try:
                payload = self._load_snapshot_item_payload(item, "power-bi")
                definition = payload.get("definition")
                artifact = payload.get("artifact", {})
                fabric_item_type = metadata.get("fabric_item_type")
                display_name = artifact.get("displayName") or artifact.get("name") or item.name

                if not definition or not fabric_item_type:
                    manual_actions.append(f"{item.name}: definition payload missing, manual restore required.")
                    continue

                existing = existing_lookup.get((fabric_item_type, display_name))
                if existing:
                    await power_bi_client.update_item_definition(
                        workspace_id,
                        existing["id"],
                        definition,
                        update_metadata=True,
                    )
                    restored_item_id = existing["id"]
                else:
                    created = await power_bi_client.create_item(
                        workspace_id,
                        display_name=display_name,
                        item_type=fabric_item_type,
                        definition=definition,
                        description=artifact.get("description"),
                    )
                    restored_item_id = created.get("id")
                    existing_lookup[(fabric_item_type, display_name)] = {
                        "id": restored_item_id,
                        "displayName": display_name,
                        "type": fabric_item_type,
                    }

                if item.item_type == "POWER_BI_SEMANTIC_MODEL":
                    semantic_model_map[item.external_id] = restored_item_id

                if item.item_type in ("POWER_BI_REPORT", "POWER_BI_PAGINATED_REPORT"):
                    original_dataset_id = artifact.get("datasetId")
                    rebound_dataset_id = semantic_model_map.get(original_dataset_id)
                    if rebound_dataset_id:
                        await power_bi_client.rebind_report_in_group(
                            workspace_id,
                            restored_item_id,
                            rebound_dataset_id,
                        )
                    else:
                        manual_actions.append(
                            f"{display_name}: semantic model rebind required because the referenced dataset was not restored in this run."
                        )

                restored_count += 1
            except Exception as exc:
                print(f"[{self.worker_id}] Failed to restore Power BI item {item.id}: {exc}")
                failed_count += 1

        if power_bi_client.refresh_token:
            tenant_record = await session.get(Tenant, tenant.id)
            if tenant_record:
                await PowerBIClient.persist_refresh_token(session, tenant_record, power_bi_client.refresh_token)

        return {
            "restored_count": restored_count,
            "failed_count": failed_count,
            "manual_actions": sorted(set(manual_actions)),
            "restore_type": "POWER_BI",
        }

    # ==================== Power Platform Restore (Apps / Flows / DLP) ====================

    async def _restore_power_app_items(
        self,
        session: AsyncSession,
        target_resource: Resource,
        items: List[SnapshotItem],
        tenant: Tenant,
        target_env_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Restore Power Apps. Prefers POWER_APP_PACKAGE (full fidelity ZIP import);
        falls back to informative failure when only POWER_APP_DEFINITION is available
        — definition-only restore would drop compiled canvas XAML and assets.

        target_env_id overrides the env stored on the item (e.g. restore to a different
        environment); if omitted, we use the source env from the backup."""
        client = self.get_power_platform_client(tenant)
        restored = 0
        failed = 0
        manual_actions: List[str] = []

        # Prefer package items; if multiple items target the same app, the package wins.
        by_app: Dict[str, SnapshotItem] = {}
        for item in items:
            meta = self._get_item_metadata(item)
            app_id = meta.get("appId") or item.external_id.split(":")[0]
            if not app_id:
                continue
            existing = by_app.get(app_id)
            if existing is None or (item.item_type == "POWER_APP_PACKAGE" and existing.item_type != "POWER_APP_PACKAGE"):
                by_app[app_id] = item

        for app_id, item in by_app.items():
            meta = self._get_item_metadata(item)
            env_id = target_env_id or meta.get("environmentId")
            if not env_id:
                manual_actions.append(f"{item.name}: cannot infer target environment; pass targetEnvironmentId in spec.")
                failed += 1
                continue

            try:
                if item.item_type == "POWER_APP_PACKAGE":
                    zip_bytes = self._load_snapshot_item_bytes(item, "power-apps")
                    if not zip_bytes:
                        manual_actions.append(f"{item.name}: package blob is missing or empty.")
                        failed += 1
                        continue
                    await client.import_app_package(env_id, zip_bytes, display_name=item.name)
                    restored += 1
                else:
                    # Definition-only — we have the JSON but no compiled assets.
                    # Power Apps has no public "create canvas app from definition JSON"
                    # endpoint that preserves full fidelity; this path is flagged so ops
                    # can decide whether to accept a degraded restore.
                    manual_actions.append(
                        f"{item.name}: only POWER_APP_DEFINITION was backed up (no package). "
                        "Re-run backup with package export enabled before restoring."
                    )
                    failed += 1
            except Exception as exc:
                print(f"[{self.worker_id}] Power App restore failed for {app_id}: {exc}")
                manual_actions.append(f"{item.name}: import failed — {str(exc)[:200]}")
                failed += 1

        return {
            "restored_count": restored,
            "failed_count": failed,
            "manual_actions": sorted(set(manual_actions)),
            "restore_type": "POWER_APPS",
        }

    async def _restore_power_flow_items(
        self,
        session: AsyncSession,
        target_resource: Resource,
        items: List[SnapshotItem],
        tenant: Tenant,
        target_env_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Restore Power Automate flows. Package import is the canonical path. If only
        POWER_FLOW_DEFINITION is in the snapshot, we attempt a best-effort 'create flow
        from definition' via the Flow management API — which works for simple cloud flows
        but may lose custom connector bindings."""
        client = self.get_power_platform_client(tenant)
        restored = 0
        failed = 0
        manual_actions: List[str] = []

        # Prefer package; only fall back to definition-only if no package for that flow.
        by_flow: Dict[str, Dict[str, SnapshotItem]] = {}
        for item in items:
            meta = self._get_item_metadata(item)
            flow_id = meta.get("flowId") or item.external_id.split(":")[0]
            if not flow_id:
                continue
            by_flow.setdefault(flow_id, {})[item.item_type] = item

        for flow_id, per_type in by_flow.items():
            package_item = per_type.get("POWER_FLOW_PACKAGE")
            definition_item = per_type.get("POWER_FLOW_DEFINITION")
            chosen = package_item or definition_item
            if not chosen:
                continue
            meta = self._get_item_metadata(chosen)
            env_id = target_env_id or meta.get("environmentId")
            if not env_id:
                manual_actions.append(f"{chosen.name}: cannot infer target environment.")
                failed += 1
                continue

            try:
                if package_item:
                    zip_bytes = self._load_snapshot_item_bytes(package_item, "power-automate")
                    if not zip_bytes:
                        manual_actions.append(f"{chosen.name}: flow package blob missing.")
                        failed += 1
                        continue
                    await client.import_flow_package(env_id, zip_bytes, display_name=chosen.name)
                    restored += 1
                else:
                    manual_actions.append(
                        f"{chosen.name}: only POWER_FLOW_DEFINITION available; package import is required "
                        "for full-fidelity restore. Manual recreation from definition JSON may work for simple flows."
                    )
                    failed += 1
            except Exception as exc:
                print(f"[{self.worker_id}] Flow restore failed for {flow_id}: {exc}")
                manual_actions.append(f"{chosen.name}: import failed — {str(exc)[:200]}")
                failed += 1

        return {
            "restored_count": restored,
            "failed_count": failed,
            "manual_actions": sorted(set(manual_actions)),
            "restore_type": "POWER_AUTOMATE",
        }

    async def _restore_power_dlp_items(
        self,
        session: AsyncSession,
        target_resource: Resource,
        items: List[SnapshotItem],
        tenant: Tenant,
    ) -> Dict[str, Any]:
        """Restore Power Platform DLP policies via upsert. Tenant-scoped, no env_id needed."""
        client = self.get_power_platform_client(tenant)
        restored = 0
        failed = 0
        manual_actions: List[str] = []

        for item in items:
            if item.item_type != "POWER_DLP_POLICY":
                continue
            try:
                payload = self._load_snapshot_item_payload(item, "power-dlp")
                if not payload:
                    manual_actions.append(f"{item.name}: policy definition is empty, cannot restore.")
                    failed += 1
                    continue
                await client.upsert_dlp_policy(payload)
                restored += 1
            except Exception as exc:
                print(f"[{self.worker_id}] DLP restore failed for {item.id}: {exc}")
                manual_actions.append(f"{item.name}: upsert failed — {str(exc)[:200]}")
                failed += 1

        return {
            "restored_count": restored,
            "failed_count": failed,
            "manual_actions": sorted(set(manual_actions)),
            "restore_type": "POWER_DLP",
        }

    # ==================== Utility Methods ====================

    def _extract_power_bi_workspace_id(self, resource: Resource) -> Optional[str]:
        metadata = resource.extra_data or {}
        workspace_id = metadata.get("workspace_id")
        if workspace_id:
            return workspace_id
        if resource.external_id and resource.external_id.startswith("pbi_ws_"):
            return resource.external_id.replace("pbi_ws_", "", 1)
        return resource.external_id

    def _get_item_metadata(self, item: SnapshotItem) -> Dict[str, Any]:
        return getattr(item, "extra_data", None) or getattr(item, "metadata", None) or {}

    def _load_snapshot_item_payload(self, item: SnapshotItem, resource_type: str) -> Dict[str, Any]:
        metadata = self._get_item_metadata(item)
        if metadata.get("raw"):
            return metadata["raw"]

        if not self.blob_service_client or not item.blob_path:
            return {}

        container_name = azure_storage_manager.get_container_name(str(item.tenant_id), resource_type)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=item.blob_path)
        payload_bytes = blob_client.download_blob().readall()
        return json.loads(payload_bytes.decode("utf-8"))

    def _load_snapshot_item_bytes(self, item: SnapshotItem, workload: str) -> Optional[bytes]:
        """Download a snapshot item's blob as raw bytes (for ZIP packages etc.)."""
        if not self.blob_service_client or not item.blob_path:
            return None
        container_name = azure_storage_manager.get_container_name(str(item.tenant_id), workload)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=item.blob_path)
        return blob_client.download_blob().readall()

    def get_power_platform_client(self, tenant: Tenant) -> PowerPlatformClient:
        """Build a Power Platform Admin API client using the tenant's Graph app credentials."""
        client_id = tenant.graph_client_id or settings.MICROSOFT_CLIENT_ID
        client_secret = settings.MICROSOFT_CLIENT_SECRET
        tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID
        return PowerPlatformClient(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

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

    def get_power_bi_client(self, tenant: Tenant) -> PowerBIClient:
        return PowerBIClient(
            tenant_id=tenant.external_tenant_id or settings.EFFECTIVE_POWER_BI_TENANT_ID,
            refresh_token=PowerBIClient.get_refresh_token_from_tenant(tenant),
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
