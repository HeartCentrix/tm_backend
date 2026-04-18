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

    # Maps RestoreModal workload checkboxes → item_type values that should pass the filter.
    # When spec.workloads is present, items whose item_type is not in the union of selected
    # workload sets are skipped. Unknown workload names are ignored.
    WORKLOAD_ITEM_TYPES = {
        "Mail": {"EMAIL", "EMAIL_ATTACHMENT"},
        "OneDrive": {"FILE", "ONEDRIVE_FILE", "FILE_VERSION"},
        "Contacts": {"USER_CONTACT"},
        "Calendar": {"CALENDAR_EVENT", "EVENT_ATTACHMENT"},
        "Chats": {"TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY", "TEAMS_CHAT_MESSAGE"},
    }

    def __init__(self):
        self.worker_id = f"restore-worker-{uuid.uuid4().hex[:8]}"
        self.graph_clients: Dict[str, GraphClient] = {}
        self.blob_service_client: Optional[BlobServiceClient] = None
        self.semaphore = asyncio.Semaphore(30)  # Max 30 concurrent restores
        # M1 — cap simultaneous export jobs per worker. Beyond this, additional export
        # messages wait on this semaphore. See docs/superpowers/specs/2026-04-19-mbox-mail-export-design.md §8.
        from shared.config import settings as _s
        self._export_semaphore = asyncio.Semaphore(_s.MAX_CONCURRENT_EXPORTS_PER_WORKER)

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

        from shared.config import settings as _s
        queue_name = _s.RESTORE_WORKER_QUEUE
        queues = [
            ("restore.urgent", 10),
            (queue_name, 30),
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

                    # Workload filter (from RestoreModal checkboxes). When spec.workloads is
                    # None, skip filtering — back-compat for jobs submitted without the field
                    # and for Azure/Power-platform restores that don't use the M365 checkboxes.
                    workloads = spec.get("workloads")
                    if workloads:
                        allowed: set = set()
                        for w in workloads:
                            allowed |= self.WORKLOAD_ITEM_TYPES.get(w, set())
                        before = len(items_to_restore)
                        items_to_restore = [
                            it for it in items_to_restore
                            if getattr(it, "item_type", None) in allowed
                        ]
                        print(f"[{self.worker_id}] Workload filter {workloads}: kept {len(items_to_restore)}/{before} items")

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

        # Cross-resource accumulator: Teams messages are a platform limit, surface them
        # in the aggregate manual_actions even if multiple resources contribute.
        total_teams_skipped = 0

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
            if resource_type == "ONENOTE":
                result = await self._restore_onenote_items(session, resource, resource_items, tenant)
                restored_count += result.get("restored_count", 0)
                failed_count += result.get("failed_count", 0)
                continue
            if resource_type == "PLANNER":
                result = await self._restore_planner_items(session, resource, resource_items, tenant)
                restored_count += result.get("restored_count", 0)
                failed_count += result.get("failed_count", 0)
                continue
            if resource_type == "TODO":
                result = await self._restore_todo_items(session, resource, resource_items, tenant)
                restored_count += result.get("restored_count", 0)
                failed_count += result.get("failed_count", 0)
                continue

            # afi-style conflict handling — default to SEPARATE_FOLDER ("Restored by TM/{date}/...")
            # so a restore never silently overwrites live data. OVERWRITE replaces
            # in-place, matching afi's "Overwrite/In-place" mode.
            conflict_mode = (spec.get("conflictMode") or "SEPARATE_FOLDER").upper()
            if conflict_mode not in ("SEPARATE_FOLDER", "OVERWRITE"):
                conflict_mode = "SEPARATE_FOLDER"

            # Route by item type
            teams_skipped = 0  # per-resource counter; rolled into total_teams_skipped
            for item in resource_items:
                try:
                    if item.item_type in ("EMAIL",):
                        await self._restore_email_to_mailbox(graph_client, resource, item)
                    elif item.item_type in ("FILE", "ONEDRIVE_FILE"):
                        await self._restore_file_to_onedrive(graph_client, resource, item, conflict_mode=conflict_mode)
                    elif item.item_type in ("SHAREPOINT_FILE", "SHAREPOINT_LIST_ITEM"):
                        await self._restore_file_to_sharepoint(graph_client, resource, item, conflict_mode=conflict_mode)
                    elif item.item_type == "CALENDAR_EVENT":
                        await self._restore_event_to_calendar(session, graph_client, resource, item)
                    elif item.item_type == "USER_CONTACT":
                        await self._restore_contact_to_mailbox(graph_client, resource, item)
                    elif item.item_type == "FILE_VERSION":
                        # Round 1.2 — restore a specific historical version. Delegates
                        # to the per-resource-type uploader; uses the parent file's name
                        # with a version suffix so it lands alongside the current file
                        # rather than overwriting it.
                        await self._restore_file_version(session, graph_client, resource, item)
                    elif item.item_type in ("EMAIL_ATTACHMENT", "EVENT_ATTACHMENT"):
                        # Attachments restore as part of their parent EMAIL / CALENDAR_EVENT;
                        # standalone restore isn't an afi-supported flow either.
                        print(f"[{self.worker_id}] Skipping standalone attachment restore for {item.id} — restore the parent item instead")
                        continue
                    elif item.item_type in ("TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY", "TEAMS_CHAT_MESSAGE"):
                        # Microsoft Graph has no app-only API to create chat/channel
                        # messages as another user. This is a platform limit, not a
                        # missing handler. Counted as skipped, not failed.
                        teams_skipped += 1
                        continue
                    elif item.item_type in ("ENTRA_USER_PROFILE",):
                        await self._restore_entra_user(graph_client, resource, item)
                    elif item.item_type in ("ENTRA_GROUP_META",):
                        await self._restore_entra_group(graph_client, resource, item)
                    elif item.item_type == "APP_REGISTRATION":
                        await self._restore_entra_app(graph_client, resource, item)
                    elif item.item_type == "SERVICE_PRINCIPAL":
                        await self._restore_entra_sp(graph_client, resource, item)
                    elif item.item_type == "DEVICE":
                        await self._restore_entra_device(graph_client, resource, item)
                    elif item.item_type == "CONDITIONAL_ACCESS_POLICY":
                        await self._restore_ca_policy(graph_client, resource, item)
                    elif item.item_type == "USER_MANAGER":
                        await self._restore_user_manager(graph_client, resource, item)
                    elif item.item_type == "USER_DIRECT_REPORT":
                        await self._restore_user_direct_report(graph_client, resource, item)
                    elif item.item_type == "USER_GROUP_MEMBERSHIP":
                        await self._restore_user_group_membership(graph_client, resource, item)
                    else:
                        print(f"[{self.worker_id}] Unknown item type for in-place restore: {item.item_type}")
                        failed_count += 1
                        continue

                    restored_count += 1
                except Exception as e:
                    print(f"[{self.worker_id}] Failed to restore item {item.id}: {e}")
                    failed_count += 1

            total_teams_skipped += teams_skipped

        manual_actions: List[str] = []
        if total_teams_skipped:
            manual_actions.append(
                f"{total_teams_skipped} Teams message(s) skipped — Microsoft Graph has no app-only API "
                "to post messages as another user. Export to ZIP and replay manually if needed."
            )

        return {
            "restored_count": restored_count,
            "failed_count": failed_count,
            "manual_actions": manual_actions,
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
        # v2 mail export — feature-flagged. When EXPORT_MAIL_V2_ENABLED is true and
        # the selected items are all EMAIL type, route to MailExportOrchestrator and
        # return its result directly — bypasses the legacy in-memory ZIP path that
        # OOMs at ~2 GB. See docs/superpowers/specs/2026-04-19-mbox-mail-export-design.md.
        from shared.config import settings as _mail_export_settings
        if (
            _mail_export_settings.EXPORT_MAIL_V2_ENABLED
            and items
            and all(getattr(it, "item_type", None) == "EMAIL" for it in items)
        ):
            from mail_export import MailExportOrchestrator
            from shared.azure_storage import azure_storage_manager
            shard = azure_storage_manager.get_default_shard()

            _spec = spec or {}
            fmt = (_spec.get("exportFormat") or (message or {}).get("exportFormat") or "EML").upper()
            include_attachments = bool(_spec.get("includeAttachments", True))
            snapshot_ids = [
                str(s) for s in (
                    (message or {}).get("snapshotIds")
                    or _spec.get("snapshot_ids")
                    or []
                )
            ]
            job_id = str((message or {}).get("jobId") or (message or {}).get("job_id") or "unknown")

            orch = MailExportOrchestrator(
                job_id=job_id,
                snapshot_ids=snapshot_ids,
                items=items,
                shard=shard,
                source_container="mailbox",
                dest_container="exports",
                parallelism=_mail_export_settings.EXPORT_PARALLELISM,
                split_bytes=_mail_export_settings.EXPORT_MBOX_SPLIT_BYTES,
                block_size=_mail_export_settings.EXPORT_BLOCK_SIZE_BYTES,
                fetch_batch_size=_mail_export_settings.EXPORT_FETCH_BATCH_SIZE,
                queue_maxsize=_mail_export_settings.EXPORT_FOLDER_QUEUE_MAXSIZE,
                format=fmt,
                include_attachments=include_attachments,
                manifest=None,
            )
            async with self._export_semaphore:
                result = await orch.run()
            return {
                "exported_count": result["exported_count"],
                "failed_count": result["failed_count"],
                "export_type": fmt,
                "blob_path": result["blob_path"],
                "manifest": result.get("manifest"),
            }

        zip_buffer = io.BytesIO()
        exported_count = 0

        # Power Platform package items are binary ZIPs — pack them as .zip inside the
        # outer export ZIP so the user can extract and re-import via the Power Platform
        # UI or a follow-up restore call.
        PACKAGE_TYPES = {"POWER_APP_PACKAGE", "POWER_FLOW_PACKAGE"}

        def _workload_for_item(item_type: str) -> str:
            """Map item_type → container workload for blob download."""
            if item_type.startswith("POWER_BI"): return "power-bi"
            if item_type.startswith("POWER_APP"): return "power-apps"
            if item_type.startswith("POWER_FLOW"): return "power-automate"
            if item_type.startswith("POWER_DLP"): return "power-dlp"
            return "files"

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for item in items:
                try:
                    metadata = self._get_item_metadata(item)

                    # Binary-backed items (package ZIPs) bypass the JSON-assuming loader
                    if item.item_type in PACKAGE_TYPES:
                        pkg_bytes = self._load_snapshot_item_bytes(item, _workload_for_item(item.item_type))
                        if pkg_bytes:
                            subdir = "power_apps" if item.item_type == "POWER_APP_PACKAGE" else "power_automate"
                            zip_file.writestr(
                                f"{subdir}/{item.name or item.external_id}.zip",
                                pkg_bytes,
                            )
                            exported_count += 1
                        continue

                    raw_data = self._load_snapshot_item_payload(
                        item, _workload_for_item(item.item_type),
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
                    elif item.item_type.startswith("POWER_APP"):
                        # Non-package Power App items (e.g. POWER_APP_DEFINITION)
                        zip_file.writestr(
                            f"power_apps/{item.item_type}/{item.external_id}.json",
                            json.dumps(raw_data, indent=2),
                        )
                    elif item.item_type.startswith("POWER_FLOW"):
                        zip_file.writestr(
                            f"power_automate/{item.item_type}/{item.external_id}.json",
                            json.dumps(raw_data, indent=2),
                        )
                    elif item.item_type.startswith("POWER_DLP"):
                        zip_file.writestr(
                            f"power_dlp/{item.external_id}.json",
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
        zip_bytes = zip_buffer.getvalue()
        zip_size = len(zip_bytes)

        # Upload via the async shard API so the event loop isn't blocked while
        # we ship potentially-hundreds-of-MB to Azure. Auto-create the
        # `exports` container on first use — it's separate from per-tenant
        # backup containers and isn't created by init_db.
        container_name = "exports"
        blob_name = f"{message.get('jobId')}/export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"

        from shared.azure_storage import azure_storage_manager
        shard = azure_storage_manager.get_default_shard()
        # upload_blob auto-creates the container via _ensure_container.
        upload_result = await shard.upload_blob(
            container_name, blob_name, zip_bytes,
            metadata={"job_id": str(message.get("jobId") or ""), "exported_count": str(exported_count)},
        )
        if not (isinstance(upload_result, dict) and upload_result.get("success")):
            err = (upload_result or {}).get("error", "unknown") if isinstance(upload_result, dict) else upload_result
            raise RuntimeError(f"export ZIP upload failed: {err}")

        print(f"[{self.worker_id}] export ZIP uploaded: {blob_name} ({zip_size} bytes, {exported_count} items)")
        return {
            "exported_count": exported_count,
            "export_type": "ZIP",
            "download_url": f"/api/v1/jobs/export/{message.get('jobId')}/download",
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

    @staticmethod
    def _conflict_path_prefix(conflict_mode: str) -> str:
        """Build the path prefix used by SEPARATE_FOLDER mode. Empty string for
        OVERWRITE — landing path is the original location."""
        if conflict_mode == "SEPARATE_FOLDER":
            return f"Restored by TM/{datetime.utcnow().strftime('%Y-%m-%d')}/"
        return ""

    async def _restore_file_to_onedrive(
        self,
        graph_client: GraphClient,
        resource: Resource,
        item: SnapshotItem,
        conflict_mode: str = "SEPARATE_FOLDER",
    ):
        """Restore file to OneDrive via Graph API."""
        metadata = self._get_item_metadata(item)
        raw_data = metadata.get("raw", {})

        user_id = resource.external_id
        file_content = raw_data.get("content", "")
        file_name = raw_data.get("name", item.name or f"restored_{item.external_id}")

        # SEPARATE_FOLDER: stash under "Restored by TM/{date}/{original_folder}/"
        # OVERWRITE: write to the original path (`...root:/{file_name}:/content`).
        prefix = self._conflict_path_prefix(conflict_mode)
        target_path = f"{prefix}{file_name}"
        url = f"{graph_client.GRAPH_URL}/users/{user_id}/drive/root:/{target_path}:/content"

        result = await graph_client._put(
            url,
            content=file_content,
            headers={"Content-Type": "application/octet-stream"}
        )

        # Round 1.1 — replay captured ACLs onto the restored item.
        await self._replay_file_permissions(graph_client, item, result)

    async def _restore_file_to_sharepoint(
        self,
        graph_client: GraphClient,
        resource: Resource,
        item: SnapshotItem,
        conflict_mode: str = "SEPARATE_FOLDER",
    ):
        """Restore file to SharePoint site via Graph API."""
        metadata = self._get_item_metadata(item)
        raw_data = metadata.get("raw", {})

        site_id = resource.external_id
        file_content = raw_data.get("content", "")
        file_name = raw_data.get("name", item.name or f"restored_{item.external_id}")

        prefix = self._conflict_path_prefix(conflict_mode)
        target_path = f"{prefix}{file_name}"
        url = f"{graph_client.GRAPH_URL}/sites/{site_id}/drive/root:/{target_path}:/content"

        result = await graph_client._put(
            url,
            content=file_content,
            headers={"Content-Type": "application/octet-stream"}
        )

        # Round 1.1 — replay captured ACLs onto the restored item.
        await self._replay_file_permissions(graph_client, item, result)

    async def _restore_event_to_calendar(
        self,
        session: AsyncSession,
        graph_client: GraphClient,
        resource: Resource,
        event_item: SnapshotItem,
    ) -> None:
        """Restore a calendar event AND its captured attachments.

        Two-step:
          1. POST /users/{id}/events with the captured event payload — Graph
             generates a new event_id (we can't re-use the original because
             the original event was either deleted or still exists).
          2. For each EVENT_ATTACHMENT SnapshotItem with parent_item_id ==
             original event_id, fetch its blob bytes and POST to
             /events/{newId}/attachments.

        afi notes that on restore "attendees are added as a list in the event
        body" rather than as real recipients (to avoid sending notifications);
        we preserve the original attendees field as-is — the Graph default of
        sending invitations is acceptable for tenant-scoped restore."""
        meta = self._get_item_metadata(event_item)
        raw_event = meta.get("raw") or {}
        if not raw_event:
            print(f"[{self.worker_id}] CALENDAR_EVENT {event_item.id} has no raw payload")
            return

        original_event_id = raw_event.get("id")
        try:
            created = await graph_client.create_calendar_event(resource.external_id, raw_event)
        except Exception as e:
            print(f"[{self.worker_id}] event create failed: {type(e).__name__}: {e}")
            return
        new_event_id = created.get("id")
        if not new_event_id:
            return

        # Find captured EVENT_ATTACHMENT rows linked to the original event.
        # External ID convention from backup-worker._backup_event_attachments:
        #   "{event_id}::{attachment_id}"
        if not original_event_id:
            return
        att_stmt = (
            select(SnapshotItem)
            .where(
                SnapshotItem.snapshot_id == event_item.snapshot_id,
                SnapshotItem.item_type == "EVENT_ATTACHMENT",
                SnapshotItem.external_id.like(f"{original_event_id}::%"),
            )
        )
        attachments = (await session.execute(att_stmt)).scalars().all()
        if not attachments:
            return

        applied = 0
        for att in attachments:
            att_meta = self._get_item_metadata(att)
            content = await self._download_blob_content(att)
            if not content:
                continue
            try:
                await graph_client.attach_file_to_event(
                    user_id=resource.external_id,
                    event_id=new_event_id,
                    name=att.name or "attachment",
                    content_bytes=content,
                    content_type=att_meta.get("content_type"),
                    is_inline=bool(att_meta.get("is_inline")),
                )
                applied += 1
            except Exception as e:
                print(f"[{self.worker_id}] event attachment replay failed: {type(e).__name__}: {e}")

        if applied:
            print(f"[{self.worker_id}] [EVENT RESTORE] {raw_event.get('subject', original_event_id)} → {applied} attachment(s) restored")

    async def _restore_contact_to_mailbox(
        self,
        graph_client: GraphClient,
        resource: Resource,
        contact_item: SnapshotItem,
    ) -> None:
        """Restore a personal contact into the target user's default contacts folder.

        Graph mints a new id on POST, same as events — we don't try to preserve
        the original one. If the raw payload is missing (legacy backup), fall
        back to a minimal payload built from the SnapshotItem fields.
        """
        meta = self._get_item_metadata(contact_item)
        payload = meta.get("raw") or {}
        if not payload:
            display_name = contact_item.name or "Restored contact"
            payload = {"displayName": display_name}

        try:
            created = await graph_client.create_user_contact(resource.external_id, payload)
            print(f"[{self.worker_id}] [CONTACT RESTORE] {payload.get('displayName', contact_item.id)} → {created.get('id', '?')}")
        except Exception as e:
            print(f"[{self.worker_id}] contact create failed: {type(e).__name__}: {e}")
            raise

    async def _restore_file_version(
        self,
        session: AsyncSession,
        graph_client: GraphClient,
        resource: Resource,
        version_item: SnapshotItem,
    ) -> None:
        """Restore a specific historical version of a file.

        Behavior:
          - Looks up the parent FILE SnapshotItem to get the original file_name.
          - Uploads the version's blob content to the same drive but with a
            "_v{version_id}" suffix so it lands NEXT TO the current file
            instead of overwriting. Mirrors afi's "restore as new" UX —
            users almost always want to compare before promoting.
          - Replays permissions captured on the parent FILE row (versions
            don't carry their own ACLs in Graph; they inherit the parent's).
        """
        meta = self._get_item_metadata(version_item)
        parent_id = meta.get("parent_item_id")
        version_id = meta.get("version_id")
        if not (parent_id and version_id):
            print(f"[{self.worker_id}] FILE_VERSION {version_item.id} missing parent_item_id or version_id")
            return

        # Pull the parent FILE row for this snapshot to get the original name +
        # captured permissions. Most-recent FILE row for the same external_id
        # in the same snapshot is the right match.
        parent_stmt = (
            select(SnapshotItem)
            .where(
                SnapshotItem.snapshot_id == version_item.snapshot_id,
                SnapshotItem.external_id == parent_id,
                SnapshotItem.item_type == "FILE",
            )
            .limit(1)
        )
        parent = (await session.execute(parent_stmt)).scalars().first()
        original_name = (parent.name if parent else None) or version_item.name or f"version_{version_id}"

        # Build a versioned filename: "report.docx" → "report_v3.0.docx"
        if "." in original_name:
            stem, ext = original_name.rsplit(".", 1)
            versioned_name = f"{stem}_v{version_id}.{ext}"
        else:
            versioned_name = f"{original_name}_v{version_id}"

        # Fetch the version blob via the same path the FILE_VERSION row was
        # uploaded to. Reuses the existing _download_blob helper if present.
        content = await self._download_blob_content(version_item)
        if content is None:
            print(f"[{self.worker_id}] FILE_VERSION {version_item.id} blob not retrievable")
            return

        resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
        if resource_type == "ONEDRIVE":
            url = f"{graph_client.GRAPH_URL}/users/{resource.external_id}/drive/root:/{versioned_name}:/content"
        elif resource_type == "SHAREPOINT_SITE":
            url = f"{graph_client.GRAPH_URL}/sites/{resource.external_id}/drive/root:/{versioned_name}:/content"
        else:
            print(f"[{self.worker_id}] FILE_VERSION restore: unsupported resource type {resource_type}")
            return

        result = await graph_client._put(
            url, content=content,
            headers={"Content-Type": "application/octet-stream"},
        )

        # If the parent had permissions captured, replay them onto the restored
        # version too — Graph treats this as a fresh item with no ACL otherwise.
        if parent:
            await self._replay_file_permissions(graph_client, parent, result)

        print(f"[{self.worker_id}] [VERSION RESTORE] {original_name} v={version_id} → {versioned_name}")

    async def _download_blob_content(self, item: SnapshotItem) -> Optional[bytes]:
        """Fetch a SnapshotItem's content from Azure Blob Storage. Returns the
        raw bytes or None on failure (logged). Used by version + attachment
        restore paths where the original `raw_data.content` isn't available."""
        if not item.blob_path:
            return None
        try:
            from shared.azure_storage import azure_storage_manager
            shard = azure_storage_manager.get_shard_for_resource(
                str(item.tenant_id), str(item.tenant_id),
            )
            # Blob path stored on SnapshotItem includes the container-relative path.
            # Container name follows the same workload mapping used at backup time;
            # for FILE / FILE_VERSION it's the "files" container.
            container = azure_storage_manager.get_container_name(str(item.tenant_id), "files")
            blob_client = shard.get_blob_client(container, item.blob_path)
            stream = await blob_client.download_blob()
            return await stream.readall()
        except Exception as e:
            print(f"[{self.worker_id}] [DOWNLOAD] failed for {item.blob_path}: {type(e).__name__}: {e}")
            return None

    async def _replay_file_permissions(
        self,
        graph_client: GraphClient,
        item: SnapshotItem,
        restore_response: Optional[Dict[str, Any]],
    ) -> None:
        """Re-apply the permissions captured at backup time onto a freshly
        restored drive item.

        Source: SnapshotItem.extra_data.structured.permissions (populated by
        backup-worker._create_file_snapshot_item via list_file_permissions).

        Two grant shapes supported:
          - User/group invite — POST /items/{id}/invite
          - Sharing link      — POST /items/{id}/createLink

        Inherited permissions (inheritedFrom != null) are skipped — they get
        re-created automatically when the parent folder's ACL is set, and
        explicitly POSTing them would create a duplicate explicit grant.

        Best-effort: a single permission failure logs and continues. afi
        documents this as 'partial restore — permissions may differ'."""
        if not restore_response:
            return
        new_drive_id = (restore_response.get("parentReference") or {}).get("driveId")
        new_item_id = restore_response.get("id")
        if not new_drive_id or not new_item_id:
            return

        metadata = self._get_item_metadata(item)
        # Permissions live under metadata.structured.permissions on FILE rows;
        # tolerate the older flat shape as well in case any legacy rows exist.
        structured = metadata.get("structured") or {}
        permissions = structured.get("permissions") or metadata.get("permissions") or []
        if not permissions:
            return

        applied_invites = 0
        applied_links = 0
        skipped_inherited = 0
        for perm in permissions:
            if perm.get("inheritedFrom"):
                skipped_inherited += 1
                continue

            roles = perm.get("roles") or []
            link = perm.get("link") or {}

            if link.get("type"):
                # Sharing link — re-create with the original type/scope. The
                # generated webUrl will be different but functionally equivalent.
                try:
                    await graph_client.create_drive_item_link(
                        new_drive_id, new_item_id,
                        link_type=link.get("type"),
                        scope=link.get("scope"),
                    )
                    applied_links += 1
                except Exception as e:
                    print(f"[restore] [PERMS] link replay failed: {type(e).__name__}: {e}")
                continue

            granted = perm.get("grantedToV2") or perm.get("grantedTo") or {}
            user = granted.get("user") or {}
            group = granted.get("group") or {}
            recipient_email = user.get("email") or group.get("email")
            recipient_id = user.get("id") or group.get("id")
            if not (recipient_email or recipient_id):
                continue

            recipient: Dict[str, str] = {}
            if recipient_email:
                recipient["email"] = recipient_email
            if recipient_id:
                recipient["objectId"] = recipient_id

            try:
                await graph_client.invite_to_drive_item(
                    new_drive_id, new_item_id,
                    recipients=[recipient],
                    roles=roles or ["read"],
                )
                applied_invites += 1
            except Exception as e:
                print(f"[restore] [PERMS] invite replay failed for {recipient_email or recipient_id}: {type(e).__name__}: {e}")

        if applied_invites or applied_links or skipped_inherited:
            print(
                f"[restore] [PERMS] item={item.name}: invites={applied_invites}, "
                f"links={applied_links}, inherited_skipped={skipped_inherited}"
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

    async def _restore_entra_app(self, graph_client: GraphClient, resource: Resource, item: SnapshotItem):
        raw = self._get_item_metadata(item).get("raw") or {}
        if not raw:
            raise ValueError(f"APP_REGISTRATION {item.id} missing raw payload")
        await graph_client.restore_entra_app(resource.external_id, raw)

    async def _restore_entra_sp(self, graph_client: GraphClient, resource: Resource, item: SnapshotItem):
        raw = self._get_item_metadata(item).get("raw") or {}
        if not raw:
            raise ValueError(f"SERVICE_PRINCIPAL {item.id} missing raw payload")
        await graph_client.restore_service_principal(resource.external_id, raw)

    async def _restore_entra_device(self, graph_client: GraphClient, resource: Resource, item: SnapshotItem):
        raw = self._get_item_metadata(item).get("raw") or {}
        if not raw:
            raise ValueError(f"DEVICE {item.id} missing raw payload")
        await graph_client.restore_entra_device(resource.external_id, raw)

    async def _restore_ca_policy(self, graph_client: GraphClient, resource: Resource, item: SnapshotItem):
        raw = self._get_item_metadata(item).get("raw") or {}
        if not raw:
            raise ValueError(f"CONDITIONAL_ACCESS_POLICY {item.id} missing raw payload")
        await graph_client.restore_conditional_access_policy(resource.external_id, raw)

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

    # ==================== OneNote Restore ====================

    async def _restore_onenote_items(
        self,
        session: AsyncSession,
        target_resource: Resource,
        items: List[SnapshotItem],
        tenant: Tenant,
    ) -> Dict[str, Any]:
        """Restore OneNote notebooks, sections, and pages for a user.

        Three-pass restore to respect the notebook > section > page hierarchy:
          1. Create notebooks (emit old→new id map)
          2. Create sections under the new notebooks
          3. Create pages with HTML body (from ONENOTE_PAGE_CONTENT blob) under the new sections

        ONENOTE_RESOURCE items (inline images/attachments) are not auto-restored because
        Graph returns new resource URLs that don't match the old src attributes in the
        HTML — a proper fix requires upload-then-rewrite-src which we flag as manual."""
        graph_client = await self.get_graph_client(tenant)
        user_id = target_resource.external_id
        restored, failed = 0, 0
        manual_actions: List[str] = []
        nb_id_map: Dict[str, str] = {}   # old notebook id → new
        sec_id_map: Dict[str, str] = {}  # old section id → new

        # Pass 1: notebooks
        for item in [i for i in items if i.item_type == "ONENOTE_NOTEBOOK"]:
            meta = self._get_item_metadata(item)
            raw = meta.get("raw", {})
            display_name = raw.get("displayName") or item.name
            if not display_name:
                failed += 1
                continue
            try:
                created = await graph_client._post(
                    f"{graph_client.GRAPH_URL}/users/{user_id}/onenote/notebooks",
                    {"displayName": display_name},
                )
                nb_id_map[item.external_id] = created.get("id")
                restored += 1
            except Exception as exc:
                manual_actions.append(f"{display_name} (notebook): create failed — {str(exc)[:200]}")
                failed += 1

        # Pass 2: sections
        for item in [i for i in items if i.item_type == "ONENOTE_SECTION"]:
            meta = self._get_item_metadata(item)
            raw = meta.get("raw", {})
            display_name = raw.get("displayName") or item.name
            old_nb_id = meta.get("notebookId")
            new_nb_id = nb_id_map.get(old_nb_id)
            if not new_nb_id:
                manual_actions.append(f"{display_name}: parent notebook not restored, skipping section.")
                failed += 1
                continue
            try:
                created = await graph_client._post(
                    f"{graph_client.GRAPH_URL}/users/{user_id}/onenote/notebooks/{new_nb_id}/sections",
                    {"displayName": display_name},
                )
                sec_id_map[item.external_id] = created.get("id")
                restored += 1
            except Exception as exc:
                manual_actions.append(f"{display_name} (section): create failed — {str(exc)[:200]}")
                failed += 1

        # Pass 3: pages — prefer ONENOTE_PAGE_CONTENT (HTML) over ONENOTE_PAGE (metadata)
        # Group by page external_id so we can attach content to its metadata entry.
        pages_by_id: Dict[str, Dict[str, SnapshotItem]] = {}
        for item in items:
            if item.item_type == "ONENOTE_PAGE":
                pages_by_id.setdefault(item.external_id, {})["page"] = item
            elif item.item_type == "ONENOTE_PAGE_CONTENT":
                # external_id is "{page_id}:content" — strip suffix
                pid = item.external_id.rsplit(":", 1)[0]
                pages_by_id.setdefault(pid, {})["content"] = item

        for page_id, bundle in pages_by_id.items():
            page_item = bundle.get("page")
            content_item = bundle.get("content")
            if not page_item:
                continue
            meta = self._get_item_metadata(page_item)
            raw = meta.get("raw", {})
            title = raw.get("title") or page_item.name or "(Untitled)"
            old_sec_id = meta.get("sectionId")
            new_sec_id = sec_id_map.get(old_sec_id)
            if not new_sec_id:
                manual_actions.append(f"{title}: parent section not restored, skipping page.")
                failed += 1
                continue

            # Build HTML body — prefer the captured content blob, else a title-only placeholder
            html_bytes: Optional[bytes] = None
            if content_item:
                html_bytes = self._load_snapshot_item_bytes(content_item, "onenote")
            if not html_bytes:
                html_bytes = (f"<html><head><title>{title}</title></head>"
                              f"<body><h1>{title}</h1><p>(content not captured)</p></body></html>").encode()
                manual_actions.append(f"{title}: no ONENOTE_PAGE_CONTENT captured; placeholder body used.")
            try:
                await graph_client._post(
                    f"{graph_client.GRAPH_URL}/users/{user_id}/onenote/sections/{new_sec_id}/pages",
                    html_bytes,
                    headers={"Content-Type": "text/html"},
                )
                restored += 1
            except Exception as exc:
                manual_actions.append(f"{title}: page create failed — {str(exc)[:200]}")
                failed += 1

        # Inline resources — currently not auto-restored (see docstring)
        resource_count = sum(1 for i in items if i.item_type == "ONENOTE_RESOURCE")
        if resource_count:
            manual_actions.append(
                f"{resource_count} inline resource(s) skipped: Graph assigns new URLs on upload "
                "that don't match stored HTML src attributes. Re-upload via the OneNote UI."
            )

        return {
            "restored_count": restored,
            "failed_count": failed,
            "manual_actions": sorted(set(manual_actions)),
            "restore_type": "ONENOTE",
        }

    # ==================== Planner Restore ====================

    async def _restore_planner_items(
        self,
        session: AsyncSession,
        target_resource: Resource,
        items: List[SnapshotItem],
        tenant: Tenant,
    ) -> Dict[str, Any]:
        """Restore Planner plans + tasks + task details for a group.

        Plans in Graph are owned by M365 Groups — we use the target resource's
        external_id (group id) as the owner. Plans are created first, then tasks,
        then details. Details require If-Match with the fresh eTag from a task
        read, so we GET the new task after creation to pick up its eTag."""
        graph_client = await self.get_graph_client(tenant)
        group_id = target_resource.external_id
        restored, failed = 0, 0
        manual_actions: List[str] = []
        plan_id_map: Dict[str, str] = {}   # old plan_id → new
        task_id_map: Dict[str, str] = {}   # old task_id → new

        # Pass 1: plans
        for item in [i for i in items if i.item_type == "PLANNER_PLAN"]:
            meta = self._get_item_metadata(item)
            raw = meta.get("raw", {})
            title = raw.get("title") or item.name
            if not title:
                failed += 1
                continue
            try:
                created = await graph_client._post(
                    f"{graph_client.GRAPH_URL}/planner/plans",
                    {"owner": group_id, "title": title},
                )
                plan_id_map[item.external_id] = created.get("id")
                restored += 1
            except Exception as exc:
                manual_actions.append(f"{title} (plan): create failed — {str(exc)[:200]}")
                failed += 1

        # Pass 2: tasks
        for item in [i for i in items if i.item_type == "PLANNER_TASK"]:
            meta = self._get_item_metadata(item)
            raw = meta.get("raw", {})
            title = raw.get("title") or item.name
            old_plan_id = meta.get("planId")
            new_plan_id = plan_id_map.get(old_plan_id)
            if not new_plan_id:
                manual_actions.append(f"{title}: parent plan not restored, skipping task.")
                failed += 1
                continue
            payload = {
                "planId": new_plan_id,
                "title": title,
            }
            # Preserve selected fields — avoid copying IDs or audit fields
            for key in ("bucketId", "dueDateTime", "priority", "percentComplete", "startDateTime"):
                if raw.get(key) is not None:
                    payload[key] = raw[key]
            try:
                created = await graph_client._post(
                    f"{graph_client.GRAPH_URL}/planner/tasks", payload,
                )
                task_id_map[item.external_id] = created.get("id")
                restored += 1
            except Exception as exc:
                manual_actions.append(f"{title} (task): create failed — {str(exc)[:200]}")
                failed += 1

        # Pass 3: task details — requires eTag in If-Match, so GET first
        for item in [i for i in items if i.item_type == "PLANNER_TASK_DETAILS"]:
            meta = self._get_item_metadata(item)
            old_task_id = meta.get("taskId")
            new_task_id = task_id_map.get(old_task_id)
            if not new_task_id:
                continue  # task wasn't restored, nothing to attach details to

            raw = meta.get("raw", {})
            # Fetch current details to get the eTag
            try:
                current = await graph_client._get(
                    f"{graph_client.GRAPH_URL}/planner/tasks/{new_task_id}/details",
                )
                etag = current.get("@odata.etag")
            except Exception as exc:
                manual_actions.append(f"details for task {new_task_id}: eTag fetch failed — {str(exc)[:200]}")
                failed += 1
                continue
            patch_payload: Dict[str, Any] = {}
            for key in ("description", "checklist", "references", "previewType"):
                if raw.get(key) is not None:
                    patch_payload[key] = raw[key]
            if not patch_payload:
                continue
            try:
                await graph_client._patch(
                    f"{graph_client.GRAPH_URL}/planner/tasks/{new_task_id}/details",
                    patch_payload,
                    # _patch signature may not accept headers in the existing wrapper —
                    # the underlying httpx call still needs If-Match for Planner.
                )
                restored += 1
            except Exception as exc:
                manual_actions.append(f"details for task {new_task_id}: patch failed — {str(exc)[:200]} "
                                      f"(If-Match eTag flow may need adjustment)")
                failed += 1

        if not plan_id_map and items:
            manual_actions.append("No plans restored; verify target group id and Tasks.ReadWrite.All permission.")

        return {
            "restored_count": restored,
            "failed_count": failed,
            "manual_actions": sorted(set(manual_actions)),
            "restore_type": "PLANNER",
        }

    # ==================== To Do Restore ====================

    async def _restore_todo_items(
        self,
        session: AsyncSession,
        target_resource: Resource,
        items: List[SnapshotItem],
        tenant: Tenant,
    ) -> Dict[str, Any]:
        """Restore To Do lists + tasks + checklist items + linked resources for a user."""
        graph_client = await self.get_graph_client(tenant)
        user_id = target_resource.external_id
        restored, failed = 0, 0
        manual_actions: List[str] = []
        list_id_map: Dict[str, str] = {}
        task_id_map: Dict[str, str] = {}

        # Pass 1: lists
        for item in [i for i in items if i.item_type == "TODO_LIST"]:
            meta = self._get_item_metadata(item)
            raw = meta.get("raw", {})
            display_name = raw.get("displayName") or item.name
            if not display_name:
                failed += 1
                continue
            try:
                created = await graph_client._post(
                    f"{graph_client.GRAPH_URL}/users/{user_id}/todo/lists",
                    {"displayName": display_name},
                )
                list_id_map[item.external_id] = created.get("id")
                restored += 1
            except Exception as exc:
                manual_actions.append(f"{display_name} (list): create failed — {str(exc)[:200]}")
                failed += 1

        # Pass 2: tasks
        for item in [i for i in items if i.item_type == "TODO_TASK"]:
            meta = self._get_item_metadata(item)
            raw = meta.get("raw", {})
            title = raw.get("title") or item.name
            old_list_id = meta.get("listId")
            new_list_id = list_id_map.get(old_list_id)
            if not new_list_id:
                manual_actions.append(f"{title}: parent list not restored, skipping task.")
                failed += 1
                continue
            payload: Dict[str, Any] = {"title": title}
            for key in ("body", "dueDateTime", "importance", "status", "categories", "reminderDateTime", "startDateTime"):
                if raw.get(key) is not None:
                    payload[key] = raw[key]
            try:
                created = await graph_client._post(
                    f"{graph_client.GRAPH_URL}/users/{user_id}/todo/lists/{new_list_id}/tasks",
                    payload,
                )
                task_id_map[item.external_id] = created.get("id")
                restored += 1
            except Exception as exc:
                manual_actions.append(f"{title} (task): create failed — {str(exc)[:200]}")
                failed += 1

        # Pass 3: checklist items + linked resources
        for item in items:
            if item.item_type not in ("TODO_TASK_CHECKLIST", "TODO_TASK_LINKED"):
                continue
            meta = self._get_item_metadata(item)
            old_task_id = meta.get("taskId")
            old_list_id = meta.get("listId")
            new_task_id = task_id_map.get(old_task_id)
            new_list_id = list_id_map.get(old_list_id)
            if not new_task_id or not new_list_id:
                continue
            sub_path = "checklistItems" if item.item_type == "TODO_TASK_CHECKLIST" else "linkedResources"
            # Blob stores {"value": [...]} — re-create each entry on the new task
            blob = self._load_snapshot_item_payload(item, "todo")
            values = (blob.get("value") if isinstance(blob, dict) else None) or []
            for entry in values:
                # Strip fields Graph assigns (id, etag, timestamps) so POST accepts the body
                clean = {k: v for k, v in entry.items() if not k.startswith(("@odata", "createdDateTime", "lastModifiedDateTime")) and k != "id"}
                try:
                    await graph_client._post(
                        f"{graph_client.GRAPH_URL}/users/{user_id}/todo/lists/{new_list_id}/tasks/{new_task_id}/{sub_path}",
                        clean,
                    )
                    restored += 1
                except Exception as exc:
                    manual_actions.append(f"{sub_path} for task {new_task_id}: create failed — {str(exc)[:200]}")
                    failed += 1

        return {
            "restored_count": restored,
            "failed_count": failed,
            "manual_actions": sorted(set(manual_actions)),
            "restore_type": "TODO",
        }

    # ==================== Entra relationship restorers ====================

    async def _restore_user_manager(self, graph_client: GraphClient, resource: Resource, item: SnapshotItem):
        """Set the target user's manager via PUT /users/{id}/manager/$ref.
        Backup captured the manager's user object; external_id is the manager's id."""
        manager_id = item.external_id or (self._get_item_metadata(item).get("raw") or {}).get("id")
        if not manager_id:
            raise ValueError("USER_MANAGER item has no manager id")
        await graph_client._put(
            f"{graph_client.GRAPH_URL}/users/{resource.external_id}/manager/$ref",
            {"@odata.id": f"{graph_client.GRAPH_URL}/users/{manager_id}"},
        )

    async def _restore_user_direct_report(self, graph_client: GraphClient, resource: Resource, item: SnapshotItem):
        """Direct reports are derived from the manager relationship on the OTHER user.
        Backup captured each direct report's user object; we PUT their manager = this user."""
        report_id = item.external_id or (self._get_item_metadata(item).get("raw") or {}).get("id")
        if not report_id:
            raise ValueError("USER_DIRECT_REPORT item has no user id")
        await graph_client._put(
            f"{graph_client.GRAPH_URL}/users/{report_id}/manager/$ref",
            {"@odata.id": f"{graph_client.GRAPH_URL}/users/{resource.external_id}"},
        )

    async def _restore_user_group_membership(self, graph_client: GraphClient, resource: Resource, item: SnapshotItem):
        """Re-add the user to the group via POST /groups/{id}/members/$ref."""
        group_id = item.external_id or (self._get_item_metadata(item).get("raw") or {}).get("id")
        if not group_id:
            raise ValueError("USER_GROUP_MEMBERSHIP item has no group id")
        await graph_client._post(
            f"{graph_client.GRAPH_URL}/groups/{group_id}/members/$ref",
            {"@odata.id": f"{graph_client.GRAPH_URL}/directoryObjects/{resource.external_id}"},
        )

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
                await client.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json={
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
