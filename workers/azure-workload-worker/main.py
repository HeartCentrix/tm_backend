"""Azure Workload Backup Worker — handles Azure VM, SQL, and PostgreSQL backups.

This worker is SEPARATE from the M365 backup-worker because:
- M365 backups operate at millisecond scale with thousands of concurrent Graph API calls
- Azure LROs (Long-Running Operations) are minute-to-hour scale
- Combining them would cause M365 throttle starvation when an Azure BACPAC export
  holds a slot for 2 hours

Queues consumed:
- azure.vm (Azure VM backups via Restore Points)
- azure.sql (Azure SQL Database backups via PITR/BACPAC)
- azure.postgres (Azure PostgreSQL backups via native API/pg_dump)
"""
import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any

import aio_pika
from sqlalchemy import select

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.models import Resource, Tenant, Job, Snapshot, SnapshotStatus, JobStatus
from shared.message_bus import message_bus
from shared.azure_storage import azure_storage_manager

from handlers.vm_handler import VmBackupHandler
from handlers.sql_handler import SqlBackupHandler
from handlers.sql_restore_handler import SqlRestoreHandler
from handlers.postgres_handler import PostgresBackupHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("azure-workload-worker")

# Queue configuration
QUEUES = [
    ("azure.vm", 5),       # Lower concurrency — VM backups are heavy LROs
    ("azure.sql", 3),      # Even lower — BACPAC exports can take hours
    ("azure.postgres", 3), # Similar to SQL
]


class AzureWorkloadWorker:
    """Processes Azure workload backup messages from RabbitMQ queues."""

    def __init__(self, worker_id: str = "azure-worker"):
        self.worker_id = worker_id
        self.vm_handler = VmBackupHandler(worker_id)
        self.sql_handler = SqlBackupHandler(worker_id)
        self.sql_restore_handler = SqlRestoreHandler(worker_id)
        self.pg_handler = PostgresBackupHandler(worker_id)

    async def start(self):
        """Connect to RabbitMQ and start consuming from all queues."""
        logger.info("=== Azure Workload Worker Starting ===")
        logger.info("DB: %s@%s:%s/%s", settings.DB_USERNAME, settings.DB_HOST, settings.DB_PORT, settings.DB_NAME)
        logger.info("RabbitMQ: %s", settings.RABBITMQ_ENABLED)
        logger.info("ARM Client ID: %s", settings.EFFECTIVE_ARM_CLIENT_ID or "NOT SET (will fallback)")
        logger.info("Backup RG: %s", settings.AZURE_BACKUP_RESOURCE_GROUP)

        # Ensure DB tables exist
        await init_db()

        # Connect to RabbitMQ
        max_retries = 30
        for attempt in range(max_retries):
            try:
                await message_bus.connect()
                if message_bus.channel:
                    break
                raise RuntimeError("Channel is None after connect")
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning("RabbitMQ not ready (attempt %d/%d): %s", attempt + 1, max_retries, e)
                    await asyncio.sleep(5)
                else:
                    logger.error("Failed to connect to RabbitMQ after %d attempts", max_retries)
                    raise

        # Start consumers for all queues
        tasks = []
        for queue_name, prefetch in QUEUES:
            task = asyncio.create_task(self.consume_queue(queue_name, prefetch))
            tasks.append(task)
            logger.info("Started consumer for %s (prefetch=%d)", queue_name, prefetch)

        logger.info("Azure Workload Worker ready, consuming from %d queues", len(QUEUES))
        await asyncio.gather(*tasks)

    async def consume_queue(self, queue_name: str, prefetch_count: int):
        """Consume messages from a specific queue."""
        if not message_bus.channel:
            return

        queue = await message_bus.channel.get_queue(queue_name)
        logger.info("[%s] Listening on %s...", self.worker_id, queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                try:
                    body = json.loads(message.body.decode())
                    await self.process_backup_message(body)
                    await message.ack()
                except Exception as e:
                    logger.exception("[%s] Error processing message from %s: %s", self.worker_id, queue_name, e)
                    try:
                        headers = message.headers or {}
                        retry_count = int(headers.get("x-retry-count", 0))
                        if retry_count >= 5:
                            logger.error("[%s] Message exceeded max retries (5), routing to DLQ", self.worker_id)
                            await message.reject(requeue=False)
                        else:
                            await message.nack(requeue=True)
                    except Exception:
                        pass

    async def process_backup_message(self, message: Dict[str, Any]):
        """Process a single Azure workload backup message."""
        job_id = uuid.UUID(message["jobId"])
        resource_id = message.get("resourceId")

        if not resource_id:
            logger.error("[%s] No resourceId in message, skipping", self.worker_id)
            return

        async with async_session_factory() as session:
            # Verify job exists
            job = await session.get(Job, job_id)
            if not job:
                logger.warning("[%s] Job %s not found, skipping stale message for %s",
                               self.worker_id, job_id, resource_id)
                return

            # Fetch resource
            resource = await session.get(Resource, uuid.UUID(resource_id))
            if not resource:
                logger.warning("[%s] Resource %s not found, skipping", self.worker_id, resource_id)
                return

            # Fetch tenant
            result = await session.execute(select(Tenant).where(Tenant.id == resource.tenant_id))
            tenant = result.scalar_one_or_none()
            if not tenant:
                logger.warning("[%s] Tenant not found for resource %s, skipping", self.worker_id, resource_id)
                return

            resource_type = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
            logger.info("[%s] Processing %s backup for %s (%s, tenant=%s)",
                        self.worker_id, resource_type, resource.display_name, resource_id, tenant.id)

            # Create snapshot
            snapshot = Snapshot(
                id=uuid.uuid4(),
                resource_id=resource.id,
                job_id=job_id,
                type=job.spec.get("fullBackup", False) and "FULL" or "INCREMENTAL",
                status=SnapshotStatus.IN_PROGRESS,
                started_at=datetime.utcnow(),
                snapshot_label=job.spec.get("note", "azure-workload-backup"),
            )
            session.add(snapshot)
            await session.commit()

            # Route to appropriate handler
            try:
                if resource_type == "AZURE_VM":
                    result = await self.vm_handler.backup(resource, tenant, snapshot, message)
                elif resource_type in ("AZURE_SQL_DB", "AZURE_SQL"):
                    result = await self.sql_handler.backup(resource, tenant, snapshot, message)
                elif resource_type in ("AZURE_POSTGRESQL", "AZURE_POSTGRESQL_SINGLE", "AZURE_PG"):
                    result = await self.pg_handler.backup(resource, tenant, snapshot, message)
                else:
                    logger.warning("[%s] Unknown Azure resource type: %s for %s",
                                   self.worker_id, resource_type, resource_id)
                    result = {"success": False, "error": f"Unsupported type: {resource_type}"}

                # Update snapshot
                if result.get("success"):
                    snapshot.status = SnapshotStatus.COMPLETED
                    snapshot.item_count = result.get("disks_copied", 1)
                    snapshot.bytes_added = result.get("size_bytes", 0)
                else:
                    snapshot.status = SnapshotStatus.FAILED

                # Update job
                job.status = JobStatus.COMPLETED if result.get("success") else JobStatus.FAILED
                job.result = result
                job.progress_pct = 100
                if job.status == JobStatus.COMPLETED:
                    job.completed_at = datetime.utcnow()

                # Update resource last backup info
                resource.last_backup_job_id = job_id
                resource.last_backup_at = datetime.utcnow()
                resource.last_backup_status = "COMPLETED" if result.get("success") else "FAILED"
                
                # Update storage_bytes from backup result
                bytes_added = result.get("size_bytes", 0) or result.get("total_size_bytes", 0) or 0
                bytes_removed = result.get("bytes_removed", 0) or 0
                net_change = bytes_added - bytes_removed
                current_storage = resource.storage_bytes or 0
                resource.storage_bytes = max(0, current_storage + net_change)
                
                logger.info("[%s] Updated storage_bytes for %s: %s -> %s bytes (added %s, removed %s)",
                            self.worker_id, resource.id, current_storage, resource.storage_bytes,
                            bytes_added, bytes_removed)

                await session.commit()
                logger.info("[%s] Backup %s for %s (%s)",
                            self.worker_id,
                            "completed" if result.get("success") else "failed",
                            resource.display_name, resource_id)

            except Exception as e:
                error_str = str(e).lower()
                # Detect 404/423 errors — resource no longer exists or is locked
                is_inaccessible = any(kw in error_str for kw in [
                    "not found", "404", "resourcenotfound", "parentresourcenotfound",
                    "locked", "423", "authorizationfailed",
                ])
                
                if is_inaccessible:
                    logger.warning("[%s] Resource %s is INACCESSIBLE (404/423) — marking to skip future backups",
                                   self.worker_id, resource_id)
                    resource.status = "INACCESSIBLE"
                
                snapshot.status = SnapshotStatus.FAILED
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                await session.commit()


async def main():
    worker = AzureWorkloadWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
