"""Backfill event_detail + hosted_content_ids into existing TEAMS_CHAT_MESSAGE
snapshot_items by re-parsing captured raw NDJSON.

Offline, idempotent, resumable. Safe to run any time.

Usage:
    python -m scripts.backfill_event_detail --tenant-id <uuid>
    python -m scripts.backfill_event_detail --tenant-id <uuid> --dry-run
"""
import argparse
import asyncio
import logging

from sqlalchemy import select, update

from shared.database import async_session_factory
from shared.metadata_extractor import _build_event_detail
from shared.models import SnapshotItem

log = logging.getLogger("backfill_event_detail")


async def main(tenant_id: str, dry_run: bool) -> None:
    async with async_session_factory() as sess:
        q = (
            select(SnapshotItem)
            .where(SnapshotItem.item_type == "TEAMS_CHAT_MESSAGE")
            .where(SnapshotItem.tenant_id == tenant_id)
        )
        count = 0
        result = await sess.stream(q)
        async for row in result.scalars():
            meta = row.extra_data or {}
            raw = meta.get("raw") or {}
            if not raw:
                continue
            event_detail = _build_event_detail(raw)
            hc_ids = [
                hc.get("id")
                for hc in (raw.get("hostedContents") or [])
                if hc.get("id")
            ]
            if (
                meta.get("event_detail") == event_detail
                and meta.get("hosted_content_ids") == hc_ids
            ):
                continue
            new_meta = {
                **meta,
                "event_detail": event_detail,
                "hosted_content_ids": hc_ids,
            }
            if not dry_run:
                await sess.execute(
                    update(SnapshotItem)
                    .where(SnapshotItem.id == row.id)
                    .values(extra_data=new_meta)
                )
            count += 1
            if count % 500 == 0:
                if not dry_run:
                    await sess.commit()
                log.info("backfilled=%d", count)
        if not dry_run:
            await sess.commit()
        log.info("done total=%d dry_run=%s", count, dry_run)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(message)s"
    )
    asyncio.run(main(args.tenant_id, args.dry_run))
