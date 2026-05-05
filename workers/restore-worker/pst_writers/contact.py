"""ContactPstWriter: USER_CONTACT items → Graph contact JSON →
pstwriter ``pst_convert contacts``.

The backup pipeline already stores the full Microsoft Graph contact
resource in ``SnapshotItem.extra_data["raw"]``; pstwriter parses that
shape natively (see ``vendor/pstwriter/src/graph_contact.cpp``). This
writer is a thin pass-through — no field mapping, no MAPI wrangling.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Optional

_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(os.path.join(_HERE, ".."))
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)

from .base import PstWriterBase  # noqa: E402

logger = logging.getLogger(__name__)


class ContactPstWriter(PstWriterBase):
    item_type = "USER_CONTACT"
    cli_kind = "contacts"
    standard_folder_type = "Contacts"   # informational; matches old API

    async def _collect_graph_item(self, item, shard, source_container: str) -> Optional[dict]:
        extra = getattr(item, "extra_data", None) or {}
        raw = extra.get("raw") if isinstance(extra, dict) else None
        if not raw:
            logger.warning(
                "contact: missing extra_data['raw'] for item %s",
                getattr(item, "external_id", "?"),
            )
            return None
        return raw
