"""Discovery Worker - Consumes discovery.m365 queue and runs periodic discovery

Two modes:
1. Queue-driven: consumes discovery.m365 messages from RabbitMQ (primary mode for onboarding)
2. Periodic: runs discovery for all tenants every 24 hours (fallback for re-discovery)
"""
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from uuid import UUID
from typing import Dict, Any

import aio_pika
from aio_pika import IncomingMessage
from sqlalchemy import select, text

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.models import (
    Tenant, TenantStatus, TenantType,
    Resource, ResourceStatus, ResourceType,
)
from shared.graph_client import GraphClient
from shared.message_bus import message_bus
from workers.discovery_worker.azure_discovery import discover_all_azure_resources

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("discovery-worker")

TYPE_MAP: Dict[str, ResourceType] = {
    "MAILBOX": ResourceType.MAILBOX,
    "SHARED_MAILBOX": ResourceType.SHARED_MAILBOX,
    "ROOM_MAILBOX": ResourceType.ROOM_MAILBOX,
    "ONEDRIVE": ResourceType.ONEDRIVE,
    "SHAREPOINT_SITE": ResourceType.SHAREPOINT_SITE,
    "TEAMS_CHANNEL": ResourceType.TEAMS_CHANNEL,
    "TEAMS_CHAT": ResourceType.TEAMS_CHAT,
    "ENTRA_USER": ResourceType.ENTRA_USER,
    "ENTRA_GROUP": ResourceType.ENTRA_GROUP,
    "ENTRA_APP": ResourceType.ENTRA_APP,
    "ENTRA_DEVICE": ResourceType.ENTRA_DEVICE,
}


async def decrypt_tenant_secret(tenant: Tenant) -> str:
    """Decrypt the tenant's Graph client secret from the DB."""
    from shared.security import decrypt_secret
    if not tenant.graph_client_secret_encrypted:
        # Fall back to env var (legacy mode)
        return settings.MICROSOFT_CLIENT_SECRET or ""
    return decrypt_secret(tenant.graph_client_secret_encrypted)


async def discover_tenant(tenant_id: UUID) -> int:
    """Run discovery for a single tenant. Returns count of new resources."""
    async with async_session_factory() as db:
        try:
            stmt = select(Tenant).where(Tenant.id == tenant_id)
            result = await db.execute(stmt)
            tenant = result.scalar_one_or_none()

            if not tenant:
                logger.warning("Tenant %s not found", tenant_id)
                return 0

            # Use tenant-stored credentials (app-only auth)
            client_id = tenant.graph_client_id or settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
            client_secret = await decrypt_tenant_secret(tenant)
            ext_tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"

            if not client_id or not client_secret:
                logger.warning("No Microsoft credentials. Skipping tenant %s.", tenant_id)
                return 0

            logger.info(
                "Starting discovery for tenant %s (%s) using app-only auth...",
                tenant.display_name, tenant_id,
            )
            tenant.status = TenantStatus.DISCOVERING
            await db.flush()

            graph = GraphClient(client_id, client_secret, ext_tenant_id)
            resources = await graph.discover_all()
            logger.info("Discovered %d resources for tenant %s.", len(resources), tenant.display_name)

            count = 0
            for r in resources:
                rtype_str = r.get("type", "ENTRA_USER")
                rtype = TYPE_MAP.get(rtype_str, ResourceType.ENTRA_USER)
                ext_id = r["external_id"]

                # Check if resource already exists (same tenant + type + external_id)
                existing = await db.execute(
                    select(Resource).where(
                        Resource.tenant_id == tenant.id,
                        Resource.type == rtype,
                        Resource.external_id == ext_id,
                    )
                )
                existing_resource = existing.scalar_one_or_none()

                if existing_resource is None:
                    # Insert new resource
                    resource = Resource(
                        id=uuid.uuid4(),
                        tenant_id=tenant.id,
                        type=rtype,
                        external_id=ext_id,
                        display_name=r.get("display_name", "Unknown"),
                        email=r.get("email"),
                        metadata=r.get("metadata", {}),
                        status=ResourceStatus.DISCOVERED,
                        discovered_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    )
                    db.add(resource)
                else:
                    # Update existing resource
                    existing_resource.display_name = r.get("display_name", existing_resource.display_name)
                    existing_resource.email = r.get("email") or existing_resource.email
                    existing_resource.metadata = r.get("metadata", existing_resource.metadata)
                    existing_resource.discovered_at = datetime.now(timezone.utc).replace(tzinfo=None)

                count += 1

            tenant.status = TenantStatus.ACTIVE
            tenant.last_discovery_at = datetime.now(timezone.utc).replace(tzinfo=None)
            await db.commit()
            logger.info(
                "Discovery complete for tenant %s: %d resources upserted.",
                tenant.display_name, count,
            )
            return count

        except Exception as e:
            logger.error("Discovery failed for tenant %s: %s", tenant_id, str(e))
            await db.rollback()
            return 0


async def handle_discovery_message(message: Dict[str, Any]):
    """Handle a single discovery.m365 message from RabbitMQ."""
    tenant_id = UUID(message["tenantId"])
    logger.info(
        "[discovery-worker] Consumed discovery.m365 for tenant %s (jobId=%s)",
        tenant_id, message.get("jobId"),
    )
    result = await discover_tenant(tenant_id)
    logger.info(
        "[discovery-worker] Discovery result for tenant %s: %d resources",
        tenant_id, result,
    )


async def consume_discovery_queue():
    """Consume messages from discovery.m365 queue."""
    # Retry connecting to RabbitMQ
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await message_bus.connect()
            if message_bus.channel:
                break
            raise RuntimeError("Channel is None after connect")
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning("[discovery-worker] RabbitMQ not ready (attempt %d/%d): %s", attempt + 1, max_retries, e)
                await asyncio.sleep(5)
            else:
                logger.error("[discovery-worker] Failed to connect to RabbitMQ after %d attempts", max_retries)
                raise

    queue = await message_bus.channel.get_queue("discovery.m365")
    logger.info("[discovery-worker] Listening on discovery.m365...")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                body = json.loads(message.body.decode())
                tenant_id = UUID(body["tenantId"])
                logger.info(
                    "[discovery-worker] Consumed discovery.m365 for tenant %s (jobId=%s)",
                    tenant_id, body.get("jobId"),
                )
                # Process discovery and commit within this message scope
                result_count = await discover_tenant(tenant_id)
                # ONLY ack after successful DB commit
                await message.ack()
                logger.info(
                    "[discovery-worker] Discovery complete for tenant %s: %d resources upserted, message acked",
                    tenant_id, result_count,
                )
            except Exception as e:
                logger.exception("[discovery-worker] Error processing discovery message: %s", e)
                try:
                    headers = message.headers or {}
                    retry_count = int(headers.get("x-retry-count", 0))
                    if retry_count >= 5:
                        logger.error(
                            "[discovery-worker] Message exceeded max retries (5), routing to DLQ"
                        )
                        await message.reject(requeue=False)  # goes to DLQ
                    else:
                        await message.nack(requeue=True)
                except Exception:
                    pass


async def discover_azure_tenant(tenant_id: UUID) -> int:
    """
    Run Azure resource discovery for a specific tenant.
    Discovers VMs, SQL DBs, and PostgreSQL servers via ARM API.
    This is SEPARATE from M365/Graph discovery.
    """
    async with async_session_factory() as db:
        try:
            stmt = select(Tenant).where(Tenant.id == tenant_id)
            result = await db.execute(stmt)
            tenant = result.scalar_one_or_none()

            if not tenant:
                logger.warning("Azure tenant %s not found", tenant_id)
                return 0

            if not settings.EFFECTIVE_ARM_CLIENT_ID or not settings.EFFECTIVE_ARM_CLIENT_SECRET:
                logger.warning("No Azure ARM credentials. Skipping Azure discovery for tenant %s.", tenant_id)
                return 0

            logger.info("Starting Azure resource discovery for tenant %s (%s)...", tenant.display_name, tenant_id)
            tenant.status = TenantStatus.DISCOVERING
            await db.flush()

            azure_resources = await discover_all_azure_resources()
            logger.info("Discovered %d Azure resources for tenant %s.", len(azure_resources), tenant.display_name)

            count = 0
            for az_r in azure_resources:
                rtype_str = az_r.get("type", "")
                rtype = TYPE_MAP.get(rtype_str, ResourceType.ENTRA_USER)
                ext_id = az_r["external_id"]

                existing = await db.execute(
                    select(Resource).where(
                        Resource.tenant_id == tenant.id,
                        Resource.type == rtype,
                        Resource.external_id == ext_id,
                    )
                )
                existing_resource = existing.scalar_one_or_none()

                if existing_resource is None:
                    resource = Resource(
                        id=uuid.uuid4(),
                        tenant_id=tenant.id,
                        type=rtype,
                        external_id=ext_id,
                        display_name=az_r.get("display_name", "Unknown"),
                        email=az_r.get("email"),
                        metadata=az_r.get("metadata", {}),
                        status=ResourceStatus.DISCOVERED,
                        discovered_at=datetime.now(timezone.utc).replace(tzinfo=None),
                        azure_subscription_id=az_r.get("azure_subscription_id"),
                        azure_resource_group=az_r.get("azure_resource_group"),
                        azure_region=az_r.get("azure_region"),
                    )
                    db.add(resource)
                else:
                    existing_resource.display_name = az_r.get("display_name", existing_resource.display_name)
                    existing_resource.metadata = az_r.get("metadata", existing_resource.metadata)
                    existing_resource.azure_subscription_id = az_r.get("azure_subscription_id") or existing_resource.azure_subscription_id
                    existing_resource.azure_resource_group = az_r.get("azure_resource_group") or existing_resource.azure_resource_group
                    existing_resource.azure_region = az_r.get("azure_region") or existing_resource.azure_region
                    existing_resource.discovered_at = datetime.now(timezone.utc).replace(tzinfo=None)

                count += 1

            tenant.status = TenantStatus.ACTIVE
            tenant.last_discovery_at = datetime.now(timezone.utc).replace(tzinfo=None)
            await db.commit()
            logger.info("Azure discovery complete for tenant %s: %d resources upserted.", tenant.display_name, count)
            return count

        except Exception as e:
            logger.error("Azure discovery failed for tenant %s: %s", tenant_id, str(e))
            await db.rollback()
            return 0


async def consume_azure_discovery_queue():
    """Consume messages from discovery.azure queue for Azure-only tenants."""
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await message_bus.connect()
            if message_bus.channel:
                break
            raise RuntimeError("Channel is None after connect")
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning("[azure-discovery] RabbitMQ not ready (attempt %d/%d): %s", attempt + 1, max_retries, e)
                await asyncio.sleep(5)
            else:
                logger.error("[azure-discovery] Failed to connect to RabbitMQ after %d attempts", max_retries)
                raise

    try:
        queue = await message_bus.channel.get_queue("discovery.azure")
    except Exception:
        logger.warning("[azure-discovery] Queue discovery.azure not found, skipping Azure discovery consumer")
        return

    logger.info("[azure-discovery] Listening on discovery.azure...")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                body = json.loads(message.body.decode())
                tenant_id = UUID(body["tenantId"])
                logger.info("[azure-discovery] Consumed discovery.azure for tenant %s (jobId=%s)", tenant_id, body.get("jobId"))
                result_count = await discover_azure_tenant(tenant_id)
                await message.ack()
                logger.info("[azure-discovery] Azure discovery complete for tenant %s: %d resources upserted, message acked", tenant_id, result_count)
            except Exception as e:
                logger.exception("[azure-discovery] Error processing Azure discovery message: %s", e)
                try:
                    headers = message.headers or {}
                    retry_count = int(headers.get("x-retry-count", 0))
                    if retry_count >= 5:
                        logger.error("[azure-discovery] Message exceeded max retries (5), routing to DLQ")
                        await message.reject(requeue=False)
                    else:
                        await message.nack(requeue=True)
                except Exception:
                    pass


async def run_all_discoveries():
    """Run discovery for all eligible tenants (periodic fallback)."""
    async with async_session_factory() as db:
        try:
            stmt = select(Tenant).where(
                text("type IN ('M365', 'AZURE', 'BOTH')"),
                text("status IN ('ACTIVE', 'PENDING', 'DISCOVERING')"),
            )
            result = await db.execute(stmt)
            tenants = result.scalars().all()

            if not tenants:
                logger.info("No tenants found for discovery.")
                return

            logger.info("Running discovery for %d tenant(s)...", len(tenants))

            for tenant in tenants:
                try:
                    count = await discover_tenant(tenant.id)
                except Exception as e:
                    logger.error("Failed to process tenant %s: %s", tenant.display_name, e)
        except Exception as e:
            logger.error("Failed to fetch tenants: %s", e)


async def main():
    logger.info("=== Discovery Worker Starting ===")
    logger.info("DB: %s@%s:%s/%s", settings.DB_USERNAME, settings.DB_HOST, settings.DB_PORT, settings.DB_NAME)
    logger.info("Microsoft Client ID: %s", settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID or "NOT SET")
    logger.info("ARM Client ID: %s", settings.EFFECTIVE_ARM_CLIENT_ID or "NOT SET")
    logger.info("Schema: %s", settings.DB_SCHEMA)
    logger.info("RabbitMQ: %s", settings.RABBITMQ_ENABLED)

    # Ensure DB tables exist
    await init_db()

    # Start M365 queue consumer (primary mode)
    m365_consumer_task = asyncio.create_task(consume_discovery_queue())

    # Start Azure discovery queue consumer (separate from M365)
    azure_consumer_task = asyncio.create_task(consume_azure_discovery_queue())

    # Also run periodic M365 discovery as fallback (every 24 hours)
    async def periodic_discovery():
        while True:
            await asyncio.sleep(24 * 60 * 60)
            logger.info("=== Running scheduled periodic M365 discovery ===")
            await run_all_discoveries()

    periodic_task = asyncio.create_task(periodic_discovery())

    # Wait forever
    await asyncio.gather(m365_consumer_task, azure_consumer_task, periodic_task)


if __name__ == "__main__":
    asyncio.run(main())
