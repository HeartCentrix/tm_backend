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
from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import SnapshotItem

log = logging.getLogger(__name__)


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
