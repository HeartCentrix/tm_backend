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
from sqlalchemy.dialects.postgresql import insert

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.models import (
    Tenant, TenantStatus, TenantType,
    Resource, ResourceStatus, ResourceType,
)
from shared.graph_client import GraphClient
from shared.message_bus import message_bus

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
    "ENTRA_SERVICE_PRINCIPAL": ResourceType.ENTRA_SERVICE_PRINCIPAL,
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

                # Upsert using ON CONFLICT for idempotency
                insert_stmt = (
                    insert(Resource)
                    .values(
                        tenant_id=tenant.id,
                        type=rtype,
                        external_id=r["external_id"],
                        display_name=r.get("display_name", "Unknown"),
                        email=r.get("email"),
                        metadata=r.get("metadata", {}),
                        status=ResourceStatus.DISCOVERED,
                        discovered_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    )
                    .on_conflict_do_update(
                        constraint="uq_resources_tenant_type_external",
                        set_=dict(
                            display_name=text("EXCLUDED.display_name"),
                            email=text("EXCLUDED.email"),
                            metadata=text("EXCLUDED.metadata"),
                            discovered_at=text("NOW()"),
                        ),
                    )
                )
                await db.execute(insert_stmt)
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
    await message_bus.connect()

    queue = await message_bus.channel.get_queue("discovery.m365")
    logger.info("[discovery-worker] Listening on discovery.m365...")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                body = json.loads(message.body.decode())
                await handle_discovery_message(body)
                await message.ack()
            except Exception as e:
                logger.error("[discovery-worker] Error processing discovery message: %s", e)
                import traceback
                traceback.print_exc()
                try:
                    headers = message.headers or {}
                    delivery_count = headers.get("x-delivery-count", 0)
                    if delivery_count < 5:
                        await message.reject(requeue=True)
                    else:
                        logger.error(
                            "[discovery-worker] Message exceeded max retries, routing to DLQ"
                        )
                        await message.reject(requeue=False)
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
    logger.info("Schema: %s", settings.DB_SCHEMA)
    logger.info("RabbitMQ: %s", settings.RABBITMQ_ENABLED)

    # Ensure DB tables exist
    await init_db()

    # Start queue consumer (primary mode)
    consumer_task = asyncio.create_task(consume_discovery_queue())

    # Also run periodic discovery as fallback (every 24 hours)
    async def periodic_discovery():
        while True:
            await asyncio.sleep(24 * 60 * 60)
            logger.info("=== Running scheduled periodic discovery ===")
            await run_all_discoveries()

    periodic_task = asyncio.create_task(periodic_discovery())

    # Wait forever
    await asyncio.gather(consumer_task, periodic_task)


if __name__ == "__main__":
    asyncio.run(main())
