"""Periodic Discovery Worker - Runs discovery every 24 hours and on startup"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select, text

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.models import Tenant, TenantStatus, TenantType, Resource, ResourceStatus, ResourceType
from shared.graph_client import GraphClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("discovery-worker")

TYPE_MAP = {
    "MAILBOX": ResourceType.MAILBOX,
    "SHARED_MAILBOX": ResourceType.SHARED_MAILBOX,
    "ONEDRIVE": ResourceType.ONEDRIVE,
    "SHAREPOINT_SITE": ResourceType.SHAREPOINT_SITE,
    "TEAMS_CHANNEL": ResourceType.TEAMS_CHANNEL,
    "TEAMS_CHAT": ResourceType.TEAMS_CHAT,
    "ENTRA_USER": ResourceType.ENTRA_USER,
    "ENTRA_GROUP": ResourceType.ENTRA_GROUP,
    "ENTRA_APP": ResourceType.ENTRA_APP,
    "ENTRA_DEVICE": ResourceType.ENTRA_DEVICE,
}


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

            client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
            client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
            ext_tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"

            if not client_id or not client_secret:
                logger.warning("No Microsoft credentials. Skipping tenant %s.", tenant_id)
                return 0

            logger.info("Starting discovery for tenant %s (%s)...", tenant.display_name, tenant_id)
            tenant.status = TenantStatus.DISCOVERING
            await db.flush()

            graph = GraphClient(client_id, client_secret, ext_tenant_id)
            resources = await graph.discover_all()
            logger.info("Discovered %d resources for tenant %s.", len(resources), tenant.display_name)

            count = 0
            for r in resources:
                existing_stmt = select(Resource).where(
                    Resource.tenant_id == tenant.id,
                    Resource.external_id == r["external_id"],
                )
                existing_result = await db.execute(existing_stmt)
                existing = existing_result.scalar_one_or_none()

                if existing is None:
                    rtype = TYPE_MAP.get(r.get("type", "ENTRA_USER"), ResourceType.ENTRA_USER)
                    resource = Resource(
                        id=uuid.uuid4(),
                        tenant_id=tenant.id,
                        type=rtype,
                        external_id=r["external_id"],
                        display_name=r.get("display_name", "Unknown"),
                        email=r.get("email"),
                        metadata=r.get("metadata", {}),
                        status=ResourceStatus.DISCOVERED,
                    )
                    db.add(resource)
                    count += 1

            tenant.status = TenantStatus.ACTIVE
            tenant.last_discovery_at = datetime.now(timezone.utc).replace(tzinfo=None)
            await db.commit()
            logger.info("Discovery complete for tenant %s: %d new resources.", tenant.display_name, count)
            return count

        except Exception as e:
            logger.error("Discovery failed for tenant %s: %s", tenant_id, str(e))
            await db.rollback()
            return 0


async def run_all_discoveries():
    """Run discovery for all eligible tenants."""
    async with async_session_factory() as db:
        try:
            # Use text() to avoid SQLAlchemy enum casting issues
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

    # Ensure DB tables exist
    await init_db()

    # Run discovery immediately on startup
    logger.info("Running initial discovery...")
    await run_all_discoveries()

    # Schedule every 24 hours
    interval = 24 * 60 * 60  # 24 hours in seconds
    while True:
        logger.info("Next discovery in 24 hours. Sleeping...")
        await asyncio.sleep(interval)

        logger.info("=== Running scheduled discovery ===")
        await run_all_discoveries()


if __name__ == "__main__":
    asyncio.run(main())
