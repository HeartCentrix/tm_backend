"""MailPstWriter: EMAIL items → Graph message JSON (with inlined
attachments) → pstwriter ``pst_convert mail``.

Per-item flow:
1. ``mail_fetch.fetch_message`` — load the Graph message JSON (inline or
   from blob storage).
2. ``mail_fetch.gather_attachments`` — build streaming refs for each
   attachment blob.
3. Drain each attachment ref into bytes, base64-encode, and inject as a
   ``#microsoft.graph.fileAttachment`` entry on the message JSON. The
   pstwriter library reads ``contentBytes`` directly out of these
   entries, so no EML round-trip is needed.

Returns the augmented message dict; the base class accumulates a list
of these dicts and ships them in one ``pst_convert`` call per chunk.
"""
from __future__ import annotations

import base64
import logging
import os
import sys

# Make the sibling restore-worker directory importable so `from mail_fetch
# import ...` resolves both in production (where the worker dir is on
# PYTHONPATH) and in tests.
_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(os.path.join(_HERE, ".."))
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)

from .base import PstWriterBase, MAX_ATTACHMENT_BYTES  # noqa: E402

logger = logging.getLogger(__name__)


async def _drain(ref) -> bytes:
    """Drain an ``AttachmentRef``'s async stream into a single bytes blob."""
    chunks: list[bytes] = []
    async for chunk in ref.data_stream:
        if chunk:
            chunks.append(chunk)
    return b"".join(chunks)


class MailPstWriter(PstWriterBase):
    item_type = "EMAIL"
    cli_kind = "mail"
    # Retained so callers / tests that introspect ``standard_folder_type``
    # (the Aspose-era field) keep working. pstwriter's CLI hard-codes the
    # folder name to "Inbox" for mail; this string is informational only.
    standard_folder_type = "Inbox"

    async def _collect_graph_item(self, item, shard, source_container: str):
        """Return the Graph message JSON for this item with attachment
        bytes inlined as base64. ``None`` when the body cannot be located."""
        from mail_fetch import fetch_message, gather_attachments

        msg_json = await fetch_message(item, shard, source_container)
        if msg_json is None:
            logger.warning(
                "mail_fetch returned None for item %s",
                getattr(item, "external_id", "?"),
            )
            return None

        # Defensive copy — fetch_message returns the SnapshotItem's own
        # extra_data dict in the inline path, and we don't want to mutate
        # caller-visible state by appending attachments to it.
        msg_json = dict(msg_json)

        att_paths = getattr(item, "attachment_blob_paths", []) or []
        if att_paths:
            try:
                refs = await gather_attachments(
                    att_paths, shard, source_container,
                    include_attachments=True,
                )
            except Exception as exc:
                logger.warning(
                    "gather_attachments failed for %s (continuing without): %s",
                    getattr(item, "external_id", "?"), exc,
                )
                refs = []

            inline_atts: list[dict] = []
            for ref in refs:
                try:
                    data = await _drain(ref)
                except Exception as exc:
                    logger.warning(
                        "attachment drain failed for %s/%s: %s",
                        getattr(item, "external_id", "?"),
                        getattr(ref, "name", "?"), exc,
                    )
                    continue
                if not data:
                    continue
                if len(data) > MAX_ATTACHMENT_BYTES:
                    # gather_attachments already replaces oversize blobs
                    # with text placeholders before we get here, but a
                    # belt-and-braces guard keeps a misconfigured shard
                    # from blowing the JSON payload up.
                    logger.warning(
                        "skipping oversize attachment %s (%d bytes > %d cap)",
                        getattr(ref, "name", "?"), len(data),
                        MAX_ATTACHMENT_BYTES,
                    )
                    continue
                inline_atts.append({
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": getattr(ref, "name", "attachment"),
                    "contentType": getattr(ref, "content_type", None)
                                   or "application/octet-stream",
                    "contentBytes": base64.b64encode(data).decode("ascii"),
                    "size": len(data),
                })

            if inline_atts:
                # Replace any metadata-only attachments array on the source
                # JSON with our resolved-bytes list. Graph delivers
                # attachments via a separate /attachments endpoint, so the
                # message body usually doesn't carry them at all; but if it
                # does, those entries lack contentBytes and can't be written.
                msg_json["attachments"] = inline_atts
                msg_json["hasAttachments"] = True

        return msg_json
