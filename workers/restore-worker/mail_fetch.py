"""Shared helpers for fetching backed-up mail bodies + attachments from blob
storage.

Originally lived as private methods on ``FolderExportTask`` in
``mail_export.py`` (the MBOX/EML pipeline).  Extracted here so the new PST
writers (``pst_writers/mail.py``) can reuse the exact same fetch logic
without dragging in the whole folder-export task.

Both functions are kept dependency-free apart from ``shared.mime_builder`` —
the caller passes the shard, container, and (for attachments) the
``include_attachments`` toggle explicitly.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from typing import List, Optional

# The restore-worker runtime mounts /app/shared, so `from shared.X import ...`
# works in production. Tests import via shim that preloads the module; we
# also allow repo-root imports when PYTHONPATH=.
_HERE = os.path.dirname(__file__)
_SHARED = os.path.abspath(os.path.join(_HERE, "..", "..", "shared"))
if os.path.isdir(_SHARED) and os.path.dirname(_SHARED) not in sys.path:
    sys.path.insert(0, os.path.dirname(_SHARED))

from shared.mime_builder import AttachmentRef  # noqa: E402

logger = logging.getLogger(__name__)


async def fetch_message(item, shard, source_container: str) -> Optional[dict]:
    """Fetch raw Graph message JSON for a SnapshotItem.

    Resolution priority:

    1. ``item.extra_data["raw"]`` (or ``item.metadata["raw"]``) when the
       backup pipeline stored the message body inline on the row (Tier 2 —
       USER_MAIL).  No Azure round-trip.
    2. ``item.blob_path`` — download from ``shard``/``source_container``.
    3. Legacy fallback — list blobs under the snapshot prefix and match by
       external_id suffix.  Used for SnapshotItem rows created before
       ``blob_path`` was persisted.

    Returns ``None`` if all three fail (caller treats as a per-item failure).
    """
    # Fast path — Tier 2 USER_MAIL backup stores the Graph message JSON
    # directly on the SnapshotItem row in extra_data.raw; no Azure blob is
    # written for the message body (only attachments go to Azure). Use the
    # inline payload when it's present to skip Azure entirely.
    meta = getattr(item, "extra_data", None) or getattr(item, "metadata", None) or {}
    raw_inline = meta.get("raw") if isinstance(meta, dict) else None
    if raw_inline:
        ext_id = getattr(item, "external_id", "") or ""
        logger.debug(
            "fetch_message: using inline extra_data.raw ext_id=%s fields=%s",
            ext_id[:40],
            list(raw_inline.keys())[:8],
        )
        return raw_inline

    blob_path = getattr(item, "blob_path", None)

    # Legacy fallback — some SnapshotItem rows were created before blob_path
    # was persisted. Reconstruct by listing blobs under the snapshot's prefix
    # and matching on external_id (the last path segment of the canonical
    # backup path: tenant/resource/snapshot/timestamp/external_id).
    resolved_container = source_container
    if not blob_path:
        ext_id = getattr(item, "external_id", "") or ""
        snap_id = str(getattr(item, "snapshot_id", "") or "")
        logger.debug(
            "fetch_message: blob_path=NULL, attempting list-and-match ext_id=%s",
            ext_id[:40],
        )

        candidates = [source_container]
        fb = getattr(item, "_mailbox_fallback_container", None)
        if fb and fb not in candidates:
            candidates.append(fb)

        matched = None
        matched_container = None
        for cand in candidates:
            try:
                async for name in shard.list_blobs(cand):
                    if snap_id and snap_id not in name:
                        continue
                    if name.endswith(f"/{ext_id}"):
                        matched = name
                        matched_container = cand
                        break
                if matched:
                    break
                logger.debug("fetch_message: no match in container=%s", cand)
            except Exception as exc:
                logger.debug(
                    "fetch_message: list container=%s failed: %s: %s",
                    cand,
                    type(exc).__name__,
                    exc,
                )

        if matched:
            blob_path = matched
            resolved_container = matched_container
            logger.debug(
                "fetch_message: recovered blob_path=%s container=%s",
                blob_path,
                resolved_container,
            )
        else:
            logger.debug(
                "fetch_message: no blob matched ext_id=%s under snapshot=%s",
                ext_id[:40],
                snap_id,
            )

    if not blob_path:
        ext_id = getattr(item, "external_id", "") or ""
        logger.debug(
            "fetch_message: blob_path resolution FAILED ext_id=%s",
            ext_id[:40],
        )
        return None

    logger.debug(
        "fetch_message: container=%s path=%s",
        resolved_container,
        blob_path,
    )
    raw = await shard.download_blob(resolved_container, blob_path)
    if raw is None:
        logger.debug("fetch_message: blob MISSING path=%s", blob_path)
        return None
    logger.debug("fetch_message: blob fetched size=%d path=%s", len(raw), blob_path)
    return json.loads(raw.decode("utf-8"))


async def gather_attachments(
    att_paths: list,
    shard,
    source_container: str,
    include_attachments: bool = True,
) -> List[AttachmentRef]:
    """Build streaming :class:`AttachmentRef` objects for each attachment path.

    Returns ``[]`` when ``include_attachments`` is False or ``att_paths`` is
    empty.  Each returned ref carries an async generator that lazily streams
    the bytes from ``shard.download_blob_stream`` on demand — no bytes are
    downloaded until the consumer iterates.
    """
    if not include_attachments or not att_paths:
        return []
    out: List[AttachmentRef] = []
    for path in att_paths:
        async def _gen(p=path):
            async for chunk in shard.download_blob_stream(source_container, p):
                yield chunk
        out.append(AttachmentRef(
            name=path.rsplit("/", 1)[-1],
            content_type="application/octet-stream",
            data_stream=_gen(),
        ))
    return out
