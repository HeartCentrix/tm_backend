"""Content-addressable dedup for blob uploads.

At enterprise scale (5k users × 400 TiB) the same binary content
appears many times across a tenant: one attachment forwarded to 50
mailboxes, the company policy PDF shared across 500 OneDrives, the
same screenshot pasted into 30 chats. Uploading each copy wastes
bandwidth, storage, and the NIC seconds the worker could spend on
new content.

This helper lets callers check whether (tenant, content_hash) is
already backed by a blob somewhere in the DB and, if so, reuse the
existing blob_path instead of uploading. The lookup hits the
existing (tenant_id, content_checksum) index — `idx_snapshot_items_
tenant_checksum` — so it's O(log n) even at hundreds of millions of
rows.

Scoped to same-tenant only so one customer's files never leak into
another customer's blob namespace, even under path-collision edge
cases.
"""
from __future__ import annotations

import logging
import os
from typing import Dict, Iterable, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import SnapshotItem

log = logging.getLogger(__name__)


# Postgres caps bind parameters at 65,535, but real-world planners
# choke on IN-lists past ~1K. 500 keeps single-query latency under
# ~50ms even with the tenant_checksum index doing a bitmap scan, and
# lets us fan out 22 M hashes (5K users × ~4400 atts) in ~44K queries
# instead of 22 M one-at-a-time round-trips.
_BULK_CHUNK = int(os.getenv("BLOB_DEDUP_BULK_CHUNK", "500"))


async def find_existing_blob(
    session: AsyncSession,
    tenant_id: UUID,
    content_checksum: str,
    min_size_bytes: int = 1024,
) -> Optional[str]:
    """Look up an existing blob_path for this tenant's content hash.

    Returns the blob_path of the first matching row that actually
    has blob-backed bytes, or None if we should proceed with upload.

    ``min_size_bytes`` filters out trivial content where dedup savings
    don't outweigh the DB round-trip cost (tiny icons, 1-pixel GIFs).
    Default 1 KB — anything below that we just upload.
    """
    if not content_checksum:
        return None
    try:
        stmt = select(SnapshotItem.blob_path).where(
            SnapshotItem.tenant_id == tenant_id,
            SnapshotItem.content_checksum == content_checksum,
            SnapshotItem.blob_path.isnot(None),
            SnapshotItem.content_size >= min_size_bytes,
        ).limit(1)
        result = await session.execute(stmt)
        blob_path = result.scalar_one_or_none()
        return blob_path
    except Exception as exc:
        # Dedup is advisory — on DB hiccup we fall through to a
        # fresh upload rather than blocking the backup. Never
        # data-loss, at worst we miss a dedup opportunity.
        log.debug("find_existing_blob failed: %s", exc)
        return None


async def bulk_find_existing_blobs(
    session: AsyncSession,
    tenant_id: UUID,
    content_checksums: Iterable[str],
    min_size_bytes: int = 1024,
) -> Dict[str, str]:
    """Bulk variant of find_existing_blob — one query per ~500 hashes
    instead of one per hash.

    Returns ``{content_checksum: existing_blob_path}`` only for hashes
    that already have a blob-backed row in this tenant. Hashes with no
    match are simply absent from the dict; callers should treat
    ``map.get(hash)`` as "upload if None".

    Scoped to single tenant. Uses the (tenant_id, content_checksum)
    composite index, so it remains O(log N) even at hundreds of
    millions of SnapshotItem rows.

    Dedup is advisory — on DB hiccup we return whatever partial map we
    built and let the caller fall through to fresh uploads. Never
    data-loss; at worst we miss a dedup opportunity on this run.
    """
    out: Dict[str, str] = {}
    unique = {h for h in content_checksums if h}
    if not unique:
        return out

    chunk: list[str] = []
    for h in unique:
        chunk.append(h)
        if len(chunk) >= _BULK_CHUNK:
            await _run_chunk(session, tenant_id, chunk, min_size_bytes, out)
            chunk = []
    if chunk:
        await _run_chunk(session, tenant_id, chunk, min_size_bytes, out)
    return out


async def _run_chunk(
    session: AsyncSession,
    tenant_id: UUID,
    chunk: list[str],
    min_size_bytes: int,
    out: Dict[str, str],
) -> None:
    try:
        stmt = select(
            SnapshotItem.content_checksum, SnapshotItem.blob_path,
        ).where(
            SnapshotItem.tenant_id == tenant_id,
            SnapshotItem.content_checksum.in_(chunk),
            SnapshotItem.blob_path.isnot(None),
            SnapshotItem.content_size >= min_size_bytes,
        )
        result = await session.execute(stmt)
        for h, path in result.all():
            if h and path and h not in out:
                out[h] = path
    except Exception as exc:
        log.debug("bulk_find_existing_blobs chunk failed: %s", exc)
