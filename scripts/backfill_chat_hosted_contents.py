"""Backfill CHAT_HOSTED_CONTENT snapshot_items for historical chat messages.

Requires a valid Graph token for the tenant (uses multi_app_manager app
credentials, same as the backup-worker). Only works while Microsoft still
has the hostedContent (i.e. the source message has not been deleted).

Usage:
    python -m scripts.backfill_chat_hosted_contents --tenant-id <uuid>
    python -m scripts.backfill_chat_hosted_contents --tenant-id <uuid> --resource-id <uuid>
"""
import argparse
import asyncio
import logging
import uuid as _uuid

from sqlalchemy import select
from sqlalchemy.orm import aliased

from shared.azure_storage import (
    azure_storage_manager,
    upload_blob_with_retry,
)
from shared.database import async_session_factory
from shared.graph_client import GraphClient
from shared.models import Resource, Snapshot, SnapshotItem, Tenant
from shared.multi_app_manager import multi_app_manager

log = logging.getLogger("backfill_hosted_contents")


def _resource_user_id(resource: Resource) -> str:
    """Mirror backup-worker: prefer extra_data.user_id, else external_id."""
    return (resource.extra_data or {}).get("user_id") or resource.external_id


def _build_graph_client(tenant: Tenant) -> GraphClient:
    """Construct a GraphClient using the same multi-app rotation the
    backup-worker uses. Falls back to the tenant's own graph_client_id if
    multi_app_manager has no apps configured."""
    app = multi_app_manager.get_next_app()
    return GraphClient(
        client_id=app.client_id,
        client_secret=app.client_secret,
        tenant_id=tenant.external_tenant_id,
    )


async def _exists(sess, snapshot_id, msg_id: str, hc_id: str) -> bool:
    q = select(SnapshotItem.id).where(
        SnapshotItem.snapshot_id == snapshot_id,
        SnapshotItem.item_type == "CHAT_HOSTED_CONTENT",
        SnapshotItem.external_id == f"{msg_id}:{hc_id}",
    )
    return (await sess.execute(q)).first() is not None


async def _drain_to_bytes(stream) -> bytes:
    """Collect an async byte iterator into bytes. Matches backup-worker's
    _upload_stream pattern (hostedContents are small enough to buffer)."""
    buf = bytearray()
    try:
        async for chunk in stream:
            if chunk:
                buf.extend(chunk)
    finally:
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            try:
                await aclose()
            except Exception:
                pass
    return bytes(buf)


async def _upload_hosted_blob(
    tenant_id: str, resource_id: str, blob_path: str, data: bytes
) -> None:
    """Upload via the same shard + container routing the backup-worker uses
    for teams workloads."""
    shard = azure_storage_manager.get_shard_for_resource(
        str(resource_id), str(tenant_id)
    )
    cname = azure_storage_manager.get_container_name(str(tenant_id), "teams")
    await upload_blob_with_retry(cname, blob_path, data, shard, max_retries=3)


async def backfill_resource(sess, resource: Resource, tenant: Tenant) -> int:
    gc = _build_graph_client(tenant)
    user_id = _resource_user_id(resource)

    # SnapshotItem has no resource_id column; JOIN through Snapshot.
    Sn = aliased(Snapshot)
    q = (
        select(SnapshotItem)
        .join(Sn, Sn.id == SnapshotItem.snapshot_id)
        .where(Sn.resource_id == resource.id)
        .where(SnapshotItem.item_type == "TEAMS_CHAT_MESSAGE")
    )
    created = 0
    result = await sess.stream(q)
    async for row in result.scalars():
        hc_ids = (row.extra_data or {}).get("hosted_content_ids") or []
        if not hc_ids:
            continue
        raw = (row.extra_data or {}).get("raw") or {}
        chat_id = raw.get("chatId") or (
            (raw.get("channelIdentity") or {}).get("channelId")
        ) or resource.external_id
        msg_id = row.external_id
        for hc_id in hc_ids:
            if await _exists(sess, row.snapshot_id, msg_id, hc_id):
                continue
            try:
                stream, ctype, size = await gc.get_hosted_content(
                    chat_id, msg_id, hc_id
                )
            except Exception as e:
                log.warning(
                    "hc_fetch_failed msg=%s hc=%s err=%s", msg_id, hc_id, e
                )
                continue
            try:
                data = await _drain_to_bytes(stream)
            except Exception as e:
                log.warning(
                    "hc_drain_failed msg=%s hc=%s err=%s", msg_id, hc_id, e
                )
                continue
            if not size:
                size = len(data)
            blob_path = (
                f"users/{user_id}/chats/{chat_id}/messages/"
                f"{msg_id}/hosted/{hc_id}"
            )
            try:
                await _upload_hosted_blob(
                    str(row.tenant_id), str(resource.id), blob_path, data
                )
            except Exception as e:
                log.warning(
                    "hc_upload_failed msg=%s hc=%s err=%s", msg_id, hc_id, e
                )
                continue
            sess.add(
                SnapshotItem(
                    id=_uuid.uuid4(),
                    snapshot_id=row.snapshot_id,
                    tenant_id=row.tenant_id,
                    item_type="CHAT_HOSTED_CONTENT",
                    external_id=f"{msg_id}:{hc_id}",
                    parent_external_id=msg_id,
                    name=f"inline-{hc_id}",
                    blob_path=blob_path,
                    content_size=size,
                    extra_data={
                        "content_type": ctype,
                        "source_message_id": msg_id,
                    },
                )
            )
            created += 1
            if created % 50 == 0:
                await sess.commit()
                log.info("backfilled=%d", created)
    await sess.commit()
    return created


async def main(tenant_id: str, resource_id: str | None) -> None:
    async with async_session_factory() as sess:
        tenant = (
            await sess.execute(select(Tenant).where(Tenant.id == tenant_id))
        ).scalar_one_or_none()
        if tenant is None:
            log.error("tenant_not_found id=%s", tenant_id)
            return

        q = select(Resource).where(Resource.tenant_id == tenant_id)
        if resource_id:
            q = q.where(Resource.id == resource_id)
        resources = (await sess.execute(q)).scalars().all()
        for r in resources:
            try:
                n = await backfill_resource(sess, r, tenant)
                log.info("resource=%s backfilled=%d", r.id, n)
            except Exception as e:
                log.exception(
                    "resource_backfill_failed resource=%s err=%s", r.id, e
                )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--resource-id")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(message)s"
    )
    asyncio.run(main(args.tenant_id, args.resource_id))
