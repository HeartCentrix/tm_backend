"""CalendarPstWriter: CALENDAR_EVENT items → Graph event JSON →
pstwriter ``pst_convert calendar``.

Like :mod:`contact`, this is a thin pass-through: pstwriter parses the
full Microsoft Graph event resource (see
``vendor/pstwriter/src/graph_event.cpp``).

Series children (``seriesMasterId`` non-null) are still skipped — the
master carries the recurrence rule and pstwriter expands it on read.
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


class CalendarPstWriter(PstWriterBase):
    item_type = "CALENDAR_EVENT"
    cli_kind = "calendar"
    # Events carry attendee/recurrence data but no recipient TC; the
    # PC + per-event tables fit in 2 structural blocks before any
    # subnode-promoted body or attachment is added.
    # _PER_ITEM_BASE_BLOCKS inherited from PstWriterBase (2).
    standard_folder_type = "Calendar"   # informational; matches old API

    async def _collect_graph_item(self, item, shard, source_container: str) -> Optional[dict]:
        extra = getattr(item, "extra_data", None) or {}
        raw = extra.get("raw") if isinstance(extra, dict) else None
        if not raw:
            logger.warning(
                "calendar: missing extra_data['raw'] for item %s",
                getattr(item, "external_id", "?"),
            )
            return None

        if raw.get("seriesMasterId"):
            logger.debug(
                "calendar: skipping child occurrence %s (parent=%s)",
                raw.get("id"),
                raw.get("seriesMasterId"),
            )
            return None

        return raw
