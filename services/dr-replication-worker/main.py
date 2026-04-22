"""
DR Replication Worker — Asynchronous cross-region backup replication

Scans for completed snapshots with dr_replication_status=pending,
initiates blob-to-blob server-side copy from primary to DR region.
Azure handles the actual transfer over Microsoft's backbone fiber.

Key principles:
- Asynchronous: Primary backup succeeds BEFORE DR replication runs
- Server-side copy: Bytes flow via Azure backbone, never through this worker
- Retry: 5-minute scan interval re-picks failed snapshots (up to 10 attempts)
- Isolation: Primary backup status unaffected by DR failures
"""
import asyncio
import logging
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import List
from uuid import UUID

from azure.storage.blob.aio import BlobClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from sqlalchemy import select, or_, text

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.models import Tenant, Resource, Snapshot, SnapshotItem, SlaPolicy
from shared.azure_storage import (
    azure_storage_manager, apply_legal_hold, apply_lifecycle_policy, AzureStorageShard,
    RESOURCE_TYPE_TO_WORKLOADS,
)
from shared.security import decrypt_secret

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [dr-worker] %(message)s",
)
logger = logging.getLogger("dr-replication-worker")

# Container-workload mapping lives in shared/azure_storage.py (RESOURCE_TYPE_TO_WORKLOADS).
# Do not redeclare here — the previous hand-rolled copy drifted from the backup worker's
# writes (used "onedrive"/"sharepoint" while backup-worker writes to "files"; used
# "azure-sql" while backup-worker writes "azure-sql-db") and caused silent 404s on every
# DR replication. The canonical map is:
#   resource_type -> (primary_workload, *fallback_workloads)
# For DR purposes we use the primary (first) workload per type.

def _primary_workload(resource_type: str) -> str:
    """Return the primary container-workload suffix for a resource type.
    Falls back to "files" for unknown types to keep replication best-effort."""
    candidates = RESOURCE_TYPE_TO_WORKLOADS.get(str(resource_type or "").upper(), ())
    return candidates[0] if candidates else "files"


MAX_REPLICATION_ATTEMPTS = 10
COPY_TIMEOUT_SECONDS = 1800  # 30 min per blob


async def scan_and_replicate():
    """
    Scan for pending/failed DR snapshots and replicate them.
    Runs every 5 minutes.
    """
    logger.info("[scan_and_replicate] === START: Scanning for pending DR snapshots ===")

    async with async_session_factory() as session:
        try:
            snapshots: List[Snapshot] = (
                await session.execute(
                    select(Snapshot).where(
                        text("status = 'COMPLETED'"),
                        or_(
                            text("dr_replication_status = 'pending'"),
                            text("dr_replication_status = 'failed'"),
                        ),
                        text("dr_replication_attempts < 10"),
                        text("created_at > NOW() - INTERVAL '2 days'"),
                    ).limit(500)
                )
            ).scalars().all()

            if not snapshots:
                logger.info("[scan_and_replicate] No pending snapshots to replicate — scan complete")
                return

            logger.info(
                "[scan_and_replicate] Found %d snapshots pending DR replication",
                len(snapshots),
            )

            success_count = 0
            fail_count = 0
            skip_count = 0

            for snapshot in snapshots:
                try:
                    # Get resource first, then tenant (Snapshot doesn't have tenant_id directly)
                    resource = await session.get(Resource, snapshot.resource_id)
                    if not resource:
                        snapshot.dr_replication_status = "failed"
                        snapshot.dr_error = "Resource not found"
                        snapshot.dr_replication_attempts = (snapshot.dr_replication_attempts or 0) + 1
                        fail_count += 1
                        logger.error(
                            "[scan_and_replicate] Snapshot %s — resource %s not found",
                            snapshot.id, snapshot.resource_id,
                        )
                        continue

                    tenant = await session.get(Tenant, resource.tenant_id)
                    if not tenant:
                        snapshot.dr_replication_status = "skipped"
                        snapshot.dr_error = "Tenant not found"
                        skip_count += 1
                        logger.warning(
                            "[scan_and_replicate] Snapshot %s — tenant %s not found, marking skipped",
                            snapshot.id, resource.tenant_id,
                        )
                        continue

                    if not tenant.dr_region_enabled:
                        snapshot.dr_replication_status = "skipped"
                        snapshot.dr_error = "DR region not enabled for this tenant"
                        skip_count += 1
                        logger.info(
                            "[scan_and_replicate] Snapshot %s — DR disabled for tenant %s, marking skipped",
                            snapshot.id, tenant.id,
                        )
                        continue

                    if not tenant.dr_storage_account_name:
                        snapshot.dr_replication_status = "failed"
                        snapshot.dr_error = "DR storage account name not configured"
                        snapshot.dr_replication_attempts = (snapshot.dr_replication_attempts or 0) + 1
                        fail_count += 1
                        logger.error(
                            "[scan_and_replicate] Snapshot %s — DR storage account not configured for tenant %s",
                            snapshot.id, tenant.id,
                        )
                        continue

                    logger.info(
                        "[scan_and_replicate] Processing snapshot %s (tenant=%s, attempt=%d, status=%s)",
                        snapshot.id, tenant.id,
                        (snapshot.dr_replication_attempts or 0) + 1,
                        snapshot.dr_replication_status,
                    )

                    await replicate_snapshot(snapshot, tenant, session)

                    if snapshot.dr_replication_status == "replicated":
                        success_count += 1
                    else:
                        fail_count += 1

                except Exception as e:
                    snapshot.dr_replication_status = "failed"
                    snapshot.dr_error = str(e)[:1000]
                    snapshot.dr_replication_attempts = (snapshot.dr_replication_attempts or 0) + 1
                    fail_count += 1
                    logger.exception(
                        "[scan_and_replicate] UNEXPECTED ERROR processing snapshot %s: %s\n%s",
                        snapshot.id, e, traceback.format_exc(),
                    )

            await session.commit()
            logger.info(
                "[scan_and_replicate] === COMPLETE: success=%d, failed=%d, skipped=%d ===",
                success_count, fail_count, skip_count,
            )

        except Exception as e:
            logger.exception(
                "[scan_and_replicate] FATAL ERROR during scan: %s\n%s",
                e, traceback.format_exc(),
            )
            await session.rollback()


async def replicate_snapshot(snapshot: Snapshot, tenant: Tenant, session):
    """
    Replicate a single snapshot to DR region.
    Uses server-side copy — bytes flow primary → DR via Azure backbone.
    """
    logger.info(
        "[replicate_snapshot] START — snapshot=%s, tenant=%s, container_source=%s",
        snapshot.id, tenant.id, snapshot.id,
    )

    snapshot.dr_replication_status = "in_progress"
    snapshot.dr_replication_attempts = (snapshot.dr_replication_attempts or 0) + 1
    await session.flush()

    # Get source shard and container. The source container depends on the snapshot's
    # resource type — backup-worker writes each workload to its own container, so DR
    # must replicate from the matching one. Previously hardcoded to "files", which
    # only worked for OneDrive/SharePoint and silently 404'd everything else.
    try:
        source_shard = azure_storage_manager.get_default_shard()
        if not source_shard:
            snapshot.dr_replication_status = "failed"
            snapshot.dr_error = "No source storage shard available"
            logger.error("[replicate_snapshot] No source storage shard available for snapshot %s", snapshot.id)
            return

        # Resolve the resource type for this snapshot to pick the right workload suffix.
        resource = await session.get(Resource, snapshot.resource_id)
        resource_type = resource.type.value if resource and hasattr(resource.type, "value") else (str(resource.type) if resource else "")
        workload = _primary_workload(resource_type)
        source_container = azure_storage_manager.get_container_name(str(tenant.id), workload)
        dr_container = f"{source_container}-dr"
    except Exception as e:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"Failed to resolve storage shard: {e}"
        logger.error("[replicate_snapshot] Failed to resolve storage shard: %s", e)
        return

    # Get DR credentials
    try:
        dr_account_key = decrypt_secret(tenant.dr_storage_account_key_encrypted)
    except Exception as e:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"Cannot decrypt DR storage key: {e}"
        logger.error(
            "[replicate_snapshot] DR credential decryption failed for tenant %s: %s",
            tenant.id, e,
        )
        return

    dr_account_name = tenant.dr_storage_account_name or ""
    if not dr_account_name:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = "DR storage account name not configured"
        logger.error(
            "[replicate_snapshot] DR storage account name missing for tenant %s", tenant.id,
        )
        return

    logger.info(
        "[replicate_snapshot] DR target — account=%s, container=%s",
        dr_account_name, dr_container,
    )

    # Get snapshot items
    try:
        items_result = await session.execute(
            select(SnapshotItem).where(SnapshotItem.snapshot_id == snapshot.id)
        )
        snapshot_items = items_result.scalars().all()
    except Exception as e:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"Failed to query snapshot items: {e}"
        logger.error("[replicate_snapshot] Failed to query snapshot items for %s: %s", snapshot.id, e)
        return

    if not snapshot_items:
        # Metadata-only snapshot, mark as replicated
        snapshot.dr_replication_status = "replicated"
        snapshot.dr_blob_path = f"{dr_container}/{snapshot.id}"
        snapshot.dr_replicated_at = datetime.now(timezone.utc)
        logger.info(
            "[replicate_snapshot] Snapshot %s has no items (metadata-only) — marking as replicated",
            snapshot.id,
        )
        return

    logger.info(
        "[replicate_snapshot] Replicating %d blob(s) for snapshot %s",
        len(snapshot_items), snapshot.id,
    )

    failed = 0
    total_bytes = 0
    replicated_items = 0

    for item in snapshot_items:
        if not item.blob_path:
            logger.debug(
                "[replicate_snapshot] Skipping item %s — no blob_path",
                item.id,
            )
            continue

        blob_path = item.blob_path
        item_start = time.monotonic()

        try:
            # Generate SAS URL for source blob (read-only, 4 hour expiry)
            source_sas = generate_blob_sas(
                account_name=source_shard.account_name,
                container_name=source_container,
                blob_name=blob_path,
                account_key=source_shard.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=4),
            )
            source_url = (
                f"https://{source_shard.account_name}.blob.core.windows.net/"
                f"{source_container}/{blob_path}?{source_sas}"
            )

            # DR blob URL
            dr_blob_url = f"https://{dr_account_name}.blob.core.windows.net/{dr_container}/{blob_path}"
            dr_blob_client = BlobClient.from_blob_url(dr_blob_url, credential=dr_account_key)

            logger.info(
                "[replicate_snapshot] Copying blob %s → DR...",
                blob_path,
            )

            # Server-side copy — bytes flow via Azure backbone
            poller = await dr_blob_client.start_copy_from_url(
                source_url=source_url,
                metadata={
                    "source_snapshot_id": str(snapshot.id),
                    "source_region": "primary",
                    "replicated_at": datetime.now(timezone.utc).isoformat(),
                },
            )

            # Wait for copy to complete
            await _wait_for_copy(dr_blob_client, timeout_seconds=COPY_TIMEOUT_SECONDS)

            elapsed = time.monotonic() - item_start
            total_bytes += item.content_size or 0
            replicated_items += 1

            logger.info(
                "[replicate_snapshot] Blob %s replicated — %.1fs, %d bytes",
                blob_path, elapsed, item.content_size or 0,
            )

            # Replicate legal hold if enabled
            if tenant.extra_data and tenant.extra_data.get("legal_hold_enabled"):
                try:
                    dr_shard = AzureStorageShard(
                        account_name=dr_account_name,
                        account_key=dr_account_key,
                    )
                    await apply_legal_hold(dr_container, blob_path, shard=dr_shard)
                    logger.info(
                        "[replicate_snapshot] Legal hold applied to DR blob %s",
                        blob_path,
                    )
                except Exception as lh_exc:
                    logger.warning(
                        "[replicate_snapshot] Legal hold FAILED for DR blob %s: %s (non-fatal)",
                        blob_path, lh_exc,
                    )
                    # Non-fatal — don't count as replication failure

        except TimeoutError as te:
            failed += 1
            logger.error(
                "[replicate_snapshot] TIMEOUT — blob %s copy exceeded %ds: %s",
                blob_path, COPY_TIMEOUT_SECONDS, te,
            )
        except Exception as e:
            failed += 1
            logger.error(
                "[replicate_snapshot] FAILED — blob %s: %s\n%s",
                blob_path, e, traceback.format_exc(),
            )

    # Determine final status
    total_items = len([i for i in snapshot_items if i.blob_path])

    if failed == 0:
        snapshot.dr_replication_status = "replicated"
        snapshot.dr_blob_path = f"{dr_container}/{snapshot.id}"
        snapshot.dr_replicated_at = datetime.now(timezone.utc)
        tenant.dr_last_replicated_at = datetime.now(timezone.utc)
        logger.info(
            "[replicate_snapshot] SUCCESS — snapshot %s replicated: %d items, %d bytes, DR container=%s",
            snapshot.id, replicated_items, total_bytes, dr_container,
        )
    elif failed < total_items:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"Partial: {failed}/{total_items} blobs failed"
        logger.warning(
            "[replicate_snapshot] PARTIAL FAILURE — snapshot %s: %d/%d blobs failed",
            snapshot.id, failed, total_items,
        )
    else:
        snapshot.dr_replication_status = "failed"
        snapshot.dr_error = f"All {total_items} blobs failed replication"
        logger.error(
            "[replicate_snapshot] TOTAL FAILURE — snapshot %s: all %d blobs failed",
            snapshot.id, total_items,
        )

    # Escalate if exceeded max attempts
    if snapshot.dr_replication_status == "failed" and snapshot.dr_replication_attempts >= MAX_REPLICATION_ATTEMPTS:
        logger.error(
            "[replicate_snapshot] MAX ATTEMPTS EXCEEDED — snapshot %s failed %d times, marking permanently failed",
            snapshot.id, snapshot.dr_replication_attempts,
        )
        snapshot.dr_error = f"Permanently failed after {MAX_REPLICATION_ATTEMPTS} attempts: {snapshot.dr_error}"


def _create_dr_shard(account_name: str, account_key: str) -> AzureStorageShard:
    """Create a temporary AzureStorageShard for the DR storage account."""
    return AzureStorageShard(account_name=account_name, account_key=account_key)


async def _wait_for_copy(blob_client: BlobClient, timeout_seconds: int = 1800):
    """Poll copy operation status until success, failure, or timeout."""
    start = time.monotonic()
    poll_count = 0

    while True:
        poll_count += 1
        elapsed = time.monotonic() - start

        try:
            props = await blob_client.get_blob_properties()
            status = getattr(props.copy, 'status', None)

            if status == "success":
                logger.debug("[_wait_for_copy] Copy succeeded after %.1fs (%d polls)", elapsed, poll_count)
                return
            if status in ("failed", "aborted"):
                status_desc = getattr(props.copy, 'status_description', 'unknown')
                raise RuntimeError(f"Copy {status}: {status_desc}")
            if elapsed > timeout_seconds:
                raise TimeoutError(f"Copy exceeded {timeout_seconds}s timeout (polls={poll_count})")

            # Log progress every 30 seconds
            if poll_count % 6 == 0:  # 6 * 5s = 30s
                pct = getattr(props.copy, 'progress', None)
                logger.info(
                    "[_wait_for_copy] Copy in progress — %.1fs elapsed, progress=%s",
                    elapsed, pct,
                )

        except (TimeoutError, RuntimeError):
            raise
        except Exception as e:
            logger.warning("[_wait_for_copy] Poll error (attempt %d, %.1fs): %s", poll_count, elapsed, e)

        await asyncio.sleep(5)


async def reconcile_dr_lifecycle_policies():
    """
    Every 6 hours: ensure DR containers have identical lifecycle policies as primary.
    """
    logger.info("[reconcile_dr_lifecycle_policies] === START: DR lifecycle reconciliation ===")

    async with async_session_factory() as session:
        try:
            tenants_result = await session.execute(select(Tenant).where(Tenant.dr_region_enabled == True))
            tenants = tenants_result.scalars().all()

            if not tenants:
                logger.info("[reconcile_dr_lifecycle_policies] No tenants with DR enabled — skipping")
                return

            logger.info("[reconcile_dr_lifecycle_policies] Found %d tenant(s) with DR enabled", len(tenants))

            success_count = 0
            fail_count = 0

            for tenant in tenants:
                try:
                    if not tenant.dr_storage_account_name:
                        logger.info(
                            "[reconcile_dr_lifecycle_policies] Tenant %s: DR storage account not configured, skipping",
                            tenant.id,
                        )
                        continue

                    try:
                        dr_key = decrypt_secret(tenant.dr_storage_account_key_encrypted)
                    except Exception as e:
                        logger.error(
                            "[reconcile_dr_lifecycle_policies] Tenant %s: Cannot decrypt DR key: %s",
                            tenant.id, e,
                        )
                        fail_count += 1
                        continue

                    dr_shard = _create_dr_shard(tenant.dr_storage_account_name, dr_key)

                    # Get tenant's SLA
                    sla_result = await session.execute(
                        select(SlaPolicy).where(
                            SlaPolicy.tenant_id == tenant.id,
                            SlaPolicy.enabled == True
                        ).limit(1)
                    )
                    sla = sla_result.scalar_one_or_none()
                    hot = sla.retention_hot_days if sla else 7
                    cool = sla.retention_cool_days if sla else 30
                    archive = sla.retention_archive_days if sla else None

                    logger.info(
                        "[reconcile_dr_lifecycle_policies] Tenant %s: hot=%dd, cool=%dd, archive=%s",
                        tenant.id, hot, cool, "unlimited" if archive is None else f"{archive}d",
                    )

                    # Iterate every workload that backup-worker may have written to.
                    # Previously hardcoded ["files","azure-vm","azure-sql","azure-postgres"]
                    # which was missing mailbox/teams/entra/onenote/planner/todo/power-*
                    # and had wrong suffixes ("azure-sql" vs actual "azure-sql-db",
                    # "azure-postgres" vs actual "azure-postgresql").
                    all_workloads = sorted({w for candidates in RESOURCE_TYPE_TO_WORKLOADS.values() for w in candidates})
                    for workload in all_workloads:
                        container = f"{azure_storage_manager.get_container_name(str(tenant.id), workload)}-dr"
                        try:
                            result = await apply_lifecycle_policy(container, hot, cool, archive, dr_shard)
                            if result.get("success"):
                                logger.info(
                                    "[reconcile_dr_lifecycle_policies] DR container %s: %d rules applied",
                                    container, result.get("rules_count", 0),
                                )
                                success_count += 1
                            else:
                                logger.warning(
                                    "[reconcile_dr_lifecycle_policies] DR container %s: policy apply failed — %s",
                                    container, result.get("error", "unknown"),
                                )
                                fail_count += 1
                        except Exception as e:
                            logger.error(
                                "[reconcile_dr_lifecycle_policies] DR container %s: exception — %s\n%s",
                                container, e, traceback.format_exc(),
                            )
                            fail_count += 1

                except Exception as e:
                    logger.error(
                        "[reconcile_dr_lifecycle_policies] Tenant %s reconciliation failed: %s\n%s",
                        tenant.id, e, traceback.format_exc(),
                    )
                    fail_count += 1

            logger.info(
                "[reconcile_dr_lifecycle_policies] === COMPLETE: success=%d, failed=%d ===",
                success_count, fail_count,
            )

        except Exception as e:
            logger.exception(
                "[reconcile_dr_lifecycle_policies] FATAL ERROR: %s\n%s",
                e, traceback.format_exc(),
            )


async def main():
    from shared.storage.startup import startup_router
    await startup_router()
    logger.info("=== DR Replication Worker Starting ===")
    logger.info("DB: %s@%s:%s/%s", settings.DB_USERNAME, settings.DB_HOST, settings.DB_PORT, settings.DB_NAME)
    logger.info("RABBITMQ_ENABLED: %s", settings.RABBITMQ_ENABLED)
    logger.info("MAX_REPLICATION_ATTEMPTS: %d", MAX_REPLICATION_ATTEMPTS)
    logger.info("COPY_TIMEOUT_SECONDS: %d", COPY_TIMEOUT_SECONDS)

    # Ensure DB tables exist
    try:
        await init_db()
        logger.info("[main] Database initialization complete")
    except Exception as e:
        logger.error("[main] Database initialization failed: %s\n%s", e, traceback.format_exc())
        raise

    # Start replication scan loop
    async def scan_loop():
        while True:
            try:
                await scan_and_replicate()
            except Exception as e:
                logger.exception("[scan_loop] Scan failed: %s\n%s", e, traceback.format_exc())
            await asyncio.sleep(300)  # every 5 min

    # Start DR lifecycle reconciliation (every 6 hours)
    async def dr_lifecycle_loop():
        while True:
            await asyncio.sleep(6 * 60 * 60)  # 6 hours
            try:
                await reconcile_dr_lifecycle_policies()
            except Exception as e:
                logger.exception("[dr_lifecycle_loop] DR lifecycle reconciliation failed: %s\n%s", e, traceback.format_exc())

    logger.info("[main] Starting scan loop (every 5 min) and DR lifecycle loop (every 6 hours)")
    await asyncio.gather(scan_loop(), dr_lifecycle_loop())


if __name__ == "__main__":
    asyncio.run(main())
