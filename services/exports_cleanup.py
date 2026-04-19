"""Fallback scheduled cleanup for the `exports` container. Run daily by
backup-scheduler as a safety net — primary mechanism is Azure lifecycle rules
(see ops/azure-lifecycle-exports.json)."""
from __future__ import annotations

import datetime
from typing import Optional


async def cleanup_exports(
    *,
    shard,
    container: str = "exports",
    older_than: datetime.timedelta = datetime.timedelta(days=1),
    now: Optional[datetime.datetime] = None,
) -> list:
    """Delete every blob in `container` whose last_modified is older than
    `now - older_than`. Returns the list of deleted blob paths. Safe under
    concurrency — each delete is idempotent."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=datetime.timezone.utc)
    cutoff = now - older_than

    deleted: list = []
    async for name, props in shard.list_blobs_with_properties(container):
        last_mod = props.get("last_modified")
        if last_mod is None:
            continue
        if last_mod.tzinfo is None:
            last_mod = last_mod.replace(tzinfo=datetime.timezone.utc)
        if last_mod <= cutoff:
            await shard.delete_blob(container, name)
            deleted.append(name)
    return deleted
