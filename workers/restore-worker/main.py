"""Restore Worker - Processes restore jobs from RabbitMQ queues"""
import asyncio
import json
import uuid
import zipfile
import io
from datetime import datetime
from typing import Dict, List, Any, Optional
import aio_pika
import httpx
from azure.storage.blob import BlobServiceClient
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import async_session_factory
from shared.models import (
    Resource, Tenant, Job, Snapshot, SnapshotItem,
    JobStatus, JobType
)
from shared.message_bus import message_bus
from shared.config import settings
from shared.graph_client import GraphClient
from shared.multi_app_manager import multi_app_manager


class RestoreWorker:
    def __init__(self):
        self.worker_id = f"restore-worker-{uuid.uuid4().hex[:8]}"
        self.blob_service_client: Optional[BlobServiceClient] = None
        self.semaphore = asyncio.Semaphore(30)

    async def initialize(self):
        await message_bus.connect()
        if settings.AZURE_STORAGE_ACCOUNT_NAME and settings.AZURE_STORAGE_ACCOUNT_KEY:
            conn_str = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={settings.AZURE_STORAGE_ACCOUNT_NAME};"
                f"AccountKey={settings.AZURE_STORAGE_ACCOUNT_KEY};"
                f"EndpointSuffix=core.windows.net"
            )
            self.blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        print(f"[{self.worker_id}] Initialized")

    async def start(self):
        for attempt in range(30):
            try:
                await self.initialize()
                break
            except Exception as e:
                if attempt < 29:
                    await asyncio.sleep(5)
                else:
                    raise

        queues = [("restore.urgent", 10), ("restore.normal", 30), ("restore.low", 50)]
        await asyncio.gather(*[asyncio.create_task(self.consume_queue(q, p)) for q, p in queues])

    async def consume_queue(self, queue_name: str, prefetch_count: int):
        if not message_bus.channel:
            return
        queue = await message_bus.channel.get_queue(queue_name)
        async for message in queue:
            async with message.process():
                try:
                    body = json.loads(message.body.decode())
                    await self.process_restore_message(body)
                except Exception as e:
                    print(f"[{self.worker_id}] Error processing message: {e}")

    async def process_restore_message(self, message: Dict[str, Any]):
        job_id = uuid.UUID(message["jobId"])
        restore_type = message.get("restoreType", "IN_PLACE")
        spec = message.get("spec", {})

        async with self.semaphore:
            async with async_session_factory() as session:
                try:
                    await self.update_job_status(session, job_id, JobStatus.RUNNING)

                    snapshot_ids = message.get("snapshotIds", [])
                    item_ids = message.get("itemIds", [])
                    items = await self.fetch_snapshot_items(session, snapshot_ids, item_ids)

                    if not items:
                        raise ValueError("No snapshot items found to restore")

                    handlers = {
                        "IN_PLACE": self.restore_in_place,
                        "CROSS_USER": self.restore_cross_user,
                        "CROSS_RESOURCE": self.restore_cross_resource,
                        "EXPORT_PST": self.export_as_zip,
                        "EXPORT_ZIP": self.export_as_zip,
                        "DOWNLOAD": self.export_download,
                    }
                    handler = handlers.get(restore_type, self.export_download)
                    result = await handler(session, items, message, spec)

                    await self.update_job_status(session, job_id, JobStatus.COMPLETED, result)
                    await session.commit()
                    await self.log_audit_event(job_id, message, result)
                    print(f"[{self.worker_id}] Job {job_id} completed: {restore_type}")

                except Exception as e:
                    await session.rollback()
                    await self.handle_restore_failure(session, job_id, e)
                    print(f"[{self.worker_id}] Job {job_id} failed: {e}")
                    raise

    async def fetch_snapshot_items(self, session, snapshot_ids, item_ids):
        stmt = select(SnapshotItem)
        if item_ids:
            stmt = stmt.where(SnapshotItem.id.in_([uuid.UUID(i) for i in item_ids]))
        elif snapshot_ids:
            stmt = stmt.where(SnapshotItem.snapshot_id.in_([uuid.UUID(s) for s in snapshot_ids]))
        else:
            return []
        result = await session.execute(stmt)
        return result.scalars().all()

    # ==================== Restore Handlers ====================

    async def restore_in_place(self, session, items, message, spec):
        restored, failed = 0, 0
        total = len(items)
        job_id = uuid.UUID(message["jobId"])

        resource_groups: Dict[str, List] = {}
        for item in items:
            snapshot = await session.get(Snapshot, item.snapshot_id)
            if snapshot:
                rid = str(snapshot.resource_id)
                resource_groups.setdefault(rid, []).append(item)

        for resource_id, resource_items in resource_groups.items():
            resource = await session.get(Resource, uuid.UUID(resource_id))
            if not resource:
                failed += len(resource_items)
                continue
            tenant = await session.get(Tenant, resource.tenant_id)
            if not tenant:
                failed += len(resource_items)
                continue
            graph_client = await self.get_graph_client(tenant)

            for item in resource_items:
                try:
                    await self._restore_item(graph_client, resource, item)
                    restored += 1
                except Exception as e:
                    print(f"[{self.worker_id}] Failed item {item.id}: {e}")
                    failed += 1
                # Update progress per item
                await self._update_progress(session, job_id, restored + failed, total)

        return {"restored_count": restored, "failed_count": failed, "restore_type": "IN_PLACE"}

    async def restore_cross_user(self, session, items, message, spec):
        target_id = spec.get("targetUserId") or spec.get("targetResourceId")
        if not target_id:
            raise ValueError("targetUserId is required for cross-user restore")

        result = await session.execute(
            select(Resource).where(and_(Resource.external_id == target_id))
        )
        target = result.scalars().first()
        if not target:
            raise ValueError(f"Target resource {target_id} not found")

        tenant = await session.get(Tenant, target.tenant_id)
        graph_client = await self.get_graph_client(tenant)

        restored, failed = 0, 0
        total = len(items)
        job_id = uuid.UUID(message["jobId"])

        for item in items:
            try:
                await self._restore_item(graph_client, target, item)
                restored += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed item {item.id}: {e}")
                failed += 1
            await self._update_progress(session, job_id, restored + failed, total)

        return {"restored_count": restored, "failed_count": failed, "restore_type": "CROSS_USER"}

    async def restore_cross_resource(self, session, items, message, spec):
        target_id = spec.get("targetResourceId")
        if not target_id:
            raise ValueError("targetResourceId is required")

        target = await session.get(Resource, uuid.UUID(target_id))
        if not target:
            raise ValueError(f"Target resource {target_id} not found")

        tenant = await session.get(Tenant, target.tenant_id)
        graph_client = await self.get_graph_client(tenant)

        restored, failed = 0, 0
        total = len(items)
        job_id = uuid.UUID(message["jobId"])

        for item in items:
            try:
                await self._restore_item(graph_client, target, item)
                restored += 1
            except Exception as e:
                print(f"[{self.worker_id}] Failed item {item.id}: {e}")
                failed += 1
            await self._update_progress(session, job_id, restored + failed, total)

        return {"restored_count": restored, "failed_count": failed, "restore_type": "CROSS_RESOURCE"}

    async def export_as_zip(self, session, items, message, spec):
        zip_buffer = io.BytesIO()
        exported = 0

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for item in items:
                try:
                    raw = (item.extra_data or {}).get("raw", {})
                    if item.item_type == "EMAIL":
                        zf.writestr(f"emails/{item.name or item.external_id}.eml", self._create_eml_from_json(raw))
                    elif item.item_type in ("FILE", "ONEDRIVE_FILE", "SHAREPOINT_FILE"):
                        # Try to get actual binary from blob
                        content = await self._download_blob(item.blob_path) if item.blob_path else None
                        if content:
                            zf.writestr(f"files/{item.name or item.external_id}", content)
                        else:
                            zf.writestr(f"files/{item.name or item.external_id}.json", json.dumps(raw, indent=2))
                    else:
                        zf.writestr(f"items/{item.item_type}/{item.external_id}.json", json.dumps(raw, indent=2))
                    exported += 1
                except Exception as e:
                    print(f"[{self.worker_id}] Export failed for {item.id}: {e}")

        zip_buffer.seek(0)
        blob_name = f"exports/{message.get('jobId')}/export.zip"
        if self.blob_service_client:
            try:
                container = self.blob_service_client.get_container_client("exports")
                try:
                    container.get_container_properties()
                except Exception:
                    container.create_container()
                container.get_blob_client(blob_name).upload_blob(zip_buffer.getvalue(), overwrite=True)
            except Exception as e:
                print(f"[{self.worker_id}] Failed to upload ZIP: {e}")

        return {
            "exported_count": exported,
            "export_type": "ZIP",
            "download_url": f"/api/v1/exports/{message.get('jobId')}/download",
            "blob_path": blob_name,
        }

    async def export_download(self, session, items, message, spec):
        data = [
            {
                "id": str(item.id),
                "type": item.item_type,
                "name": item.name,
                "external_id": item.external_id,
                "content": (item.extra_data or {}).get("raw", {}),
            }
            for item in items
        ]
        return {"exported_count": len(data), "export_type": "JSON", "data": data}

    # ==================== Per-type restore dispatch ====================

    async def _restore_item(self, graph_client: GraphClient, resource: Resource, item: SnapshotItem):
        """Dispatch to the correct restore method based on item_type"""
        t = item.item_type
        if t == "EMAIL":
            await self._restore_email_to_mailbox(graph_client, resource, item)
        elif t in ("FILE", "ONEDRIVE_FILE"):
            await self._restore_file_to_onedrive(graph_client, resource, item)
        elif t in ("SHAREPOINT_FILE", "SHAREPOINT_LIST_ITEM"):
            await self._restore_file_to_sharepoint(graph_client, resource, item)
        elif t == "ENTRA_USER_PROFILE":
            await self._restore_entra_user(graph_client, resource, item)
        elif t == "ENTRA_GROUP_META":
            await self._restore_entra_group(graph_client, resource, item)
        elif t in ("TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY", "TEAMS_CHAT_MESSAGE"):
            raise ValueError("Teams messages cannot be restored in-place via Graph API")
        else:
            raise ValueError(f"Unsupported item type for restore: {t}")

    # ==================== Low-level restore methods ====================

    async def _restore_email_to_mailbox(self, graph_client, resource, item):
        raw = (item.extra_data or {}).get("raw", {})
        user_id = resource.external_id

        # Ensure mailbox folder exists (inbox as fallback)
        folder_path = item.folder_path or "Inbox"
        try:
            await graph_client._get(f"{graph_client.GRAPH_URL}/users/{user_id}/mailFolders/Inbox")
        except Exception:
            pass  # Inbox always exists; custom folder recreation not required for in-place

        payload = {
            "subject": raw.get("subject"),
            "body": {
                "contentType": raw.get("body", {}).get("contentType", "HTML"),
                "content": raw.get("body", {}).get("content", ""),
            },
            "toRecipients": raw.get("toRecipients", []),
            "ccRecipients": raw.get("ccRecipients", []),
            "hasAttachments": raw.get("hasAttachments", False),
            "internetMessageId": raw.get("internetMessageId"),
        }
        await graph_client._post(f"{graph_client.GRAPH_URL}/users/{user_id}/messages", payload)

    async def _restore_file_to_onedrive(self, graph_client, resource, item):
        raw = (item.extra_data or {}).get("raw", {})
        user_id = resource.external_id
        file_name = raw.get("name") or item.name or f"restored_{item.external_id}"

        # Download actual binary from blob storage if available
        content = await self._download_blob(item.blob_path) if item.blob_path else None
        if content is None:
            content = raw.get("content", b"")
            if isinstance(content, str):
                content = content.encode()

        # Ensure drive root exists (it always does for active users)
        url = f"{graph_client.GRAPH_URL}/users/{user_id}/drive/root:/{file_name}:/content"
        await graph_client._put(url, content=content, headers={"Content-Type": "application/octet-stream"})

    async def _restore_file_to_sharepoint(self, graph_client, resource, item):
        raw = (item.extra_data or {}).get("raw", {})
        site_id = resource.external_id
        file_name = raw.get("name") or item.name or f"restored_{item.external_id}"

        content = await self._download_blob(item.blob_path) if item.blob_path else None
        if content is None:
            content = raw.get("content", b"")
            if isinstance(content, str):
                content = content.encode()

        # Check site exists
        try:
            await graph_client._get(f"{graph_client.GRAPH_URL}/sites/{site_id}")
        except Exception as e:
            raise ValueError(f"SharePoint site {site_id} not accessible: {e}")

        url = f"{graph_client.GRAPH_URL}/sites/{site_id}/drive/root:/{file_name}:/content"
        await graph_client._put(url, content=content, headers={"Content-Type": "application/octet-stream"})

    async def _restore_entra_user(self, graph_client, resource, item):
        raw = (item.extra_data or {}).get("raw", {})
        user_id = resource.external_id
        payload = {k: raw[k] for k in ("displayName", "givenName", "surname", "jobTitle", "department", "officeLocation", "mobilePhone", "businessPhones") if k in raw}
        await graph_client._patch(f"{graph_client.GRAPH_URL}/users/{user_id}", payload)

    async def _restore_entra_group(self, graph_client, resource, item):
        raw = (item.extra_data or {}).get("raw", {})
        group_id = resource.external_id
        payload = {k: raw[k] for k in ("displayName", "description", "mailEnabled", "securityEnabled") if k in raw}
        await graph_client._patch(f"{graph_client.GRAPH_URL}/groups/{group_id}", payload)

    # ==================== Utilities ====================

    async def _download_blob(self, blob_path: str) -> Optional[bytes]:
        """Download file content from Azure Blob Storage"""
        if not self.blob_service_client or not blob_path:
            return None
        try:
            # blob_path format: container/path/to/file
            parts = blob_path.split("/", 1)
            if len(parts) != 2:
                return None
            container_name, path = parts
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=path)
            stream = blob_client.download_blob()
            return stream.readall()
        except Exception as e:
            print(f"[{self.worker_id}] Blob download failed for {blob_path}: {e}")
            return None

    async def _update_progress(self, session, job_id: uuid.UUID, processed: int, total: int):
        """Update job progress_pct and items_processed"""
        job = await session.get(Job, job_id)
        if job and total > 0:
            job.progress_pct = int((processed / total) * 100)
            job.items_processed = processed
            await session.flush()

    def _create_eml_from_json(self, email_data: Dict) -> str:
        subject = email_data.get("subject", "No Subject")
        body = email_data.get("body", {}).get("content", "")
        from_addr = email_data.get("from", {}).get("emailAddress", {}).get("address", "unknown@unknown.com")
        to_addrs = ", ".join([r.get("emailAddress", {}).get("address", "") for r in email_data.get("toRecipients", [])])
        date = email_data.get("sentDateTime", email_data.get("receivedDateTime", ""))
        return f"From: {from_addr}\r\nTo: {to_addrs}\r\nSubject: {subject}\r\nDate: {date}\r\nContent-Type: text/html; charset=\"utf-8\"\r\n\r\n{body}\r\n"

    async def get_graph_client(self, tenant: Tenant) -> GraphClient:
        app = multi_app_manager.get_next_app()
        return GraphClient(client_id=app.client_id, client_secret=app.client_secret, tenant_id=tenant.external_tenant_id)

    async def update_job_status(self, session, job_id, status, result=None):
        job = await session.get(Job, job_id)
        if job:
            job.status = status
            if status == JobStatus.COMPLETED:
                job.completed_at = datetime.utcnow()
                job.progress_pct = 100
            if result:
                job.result = result
            await session.flush()

    async def handle_restore_failure(self, session, job_id, error):
        job = await session.get(Job, job_id)
        if job:
            job.attempts += 1
            job.error_message = str(error)
            job.status = JobStatus.FAILED if job.attempts >= job.max_attempts else JobStatus.RETRYING

    async def log_audit_event(self, job_id, message, result):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post("http://audit-service:8012/api/v1/audit/log", json={
                    "action": "RESTORE_COMPLETED",
                    "tenant_id": message.get("tenantId"),
                    "actor_type": "WORKER",
                    "resource_id": message.get("resourceId"),
                    "outcome": "SUCCESS",
                    "job_id": str(job_id),
                    "details": {
                        "restore_type": message.get("restoreType", "IN_PLACE"),
                        "restored_count": result.get("restored_count", result.get("exported_count", 0)),
                        "failed_count": result.get("failed_count", 0),
                    },
                })
        except Exception as e:
            print(f"[{self.worker_id}] Audit log failed: {e}")


worker = RestoreWorker()


async def main():
    print("Starting restore worker...")
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
