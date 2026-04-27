"""MailPstWriter: EMAIL items → MapiMessage → PST.

Per-item flow:
1. ``mail_fetch.fetch_message`` — load the Graph message JSON (inline or
   blob storage).
2. ``shared.mime_builder.build_eml`` (or ``build_eml_streaming`` when
   attachments are present) — render to RFC 5322 EML bytes.
3. ``aspose.email.mapi.MapiMessage.from_mime_message_bytes`` — convert
   EML to a MAPI item suitable for the Aspose PersonalStorage.

All ``aspose.*`` imports stay lazy so the module loads without
``aspose-email`` installed (tests substitute it via ``sys.modules``).
"""
from __future__ import annotations

import importlib
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

from .base import PstWriterBase  # noqa: E402

logger = logging.getLogger(__name__)


class MailPstWriter(PstWriterBase):
    item_type = "EMAIL"
    standard_folder_type = "Inbox"

    async def _build_mapi_item(self, item, shard, source_container: str):
        """Fetch JSON → EML bytes → MapiMessage.  Returns ``None`` when the
        message body cannot be located (caller bumps ``failed_count``)."""
        from mail_fetch import fetch_message, gather_attachments
        from shared.mime_builder import build_eml, build_eml_streaming

        msg_json = await fetch_message(item, shard, source_container)
        if msg_json is None:
            logger.warning(
                "mail_fetch returned None for item %s",
                getattr(item, "external_id", "?"),
            )
            return None

        att_paths = getattr(item, "attachment_blob_paths", []) or []
        include_attachments = True
        attachments = await gather_attachments(
            att_paths, shard, source_container, include_attachments
        )

        if attachments:
            pieces = []
            async for chunk in build_eml_streaming(msg_json, attachments):
                pieces.append(chunk)
            eml_bytes = b"".join(pieces)
        else:
            eml_bytes = build_eml(msg_json, attachments=[])

        MapiMessage = importlib.import_module("aspose.email.mapi").MapiMessage
        return MapiMessage.from_mime_message_bytes(eml_bytes)
