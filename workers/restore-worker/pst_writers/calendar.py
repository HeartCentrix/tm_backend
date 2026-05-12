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

from .base import IntentionalSkip, PstWriterBase  # noqa: E402

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
        ext_id = getattr(item, "external_id", "?")
        if not raw:
            print(
                f"[pst_writer] calendar SKIP missing extra_data['raw'] item={ext_id}",
                flush=True,
            )
            return None

        if raw.get("seriesMasterId"):
            # Child occurrence — pstwriter consumes the master and
            # expands the recurrence rule on read. The streamer
            # (stream_snapshot_items_by_group) auto-expands selected
            # children to include their masters; if a child still slips
            # through here it is because the master is not backed up
            # (parent mailbox not in scope, or master was hard-deleted
            # before the snapshot ran). Either way it's an intentional
            # drop, not a failure — surface it as ``skipped_count`` so
            # the manifest's ``failed_counts_by_type`` only reflects
            # genuine errors.
            raise IntentionalSkip(
                f"series-occurrence parent={raw.get('seriesMasterId')}"
            )

        # Stamp the SnapshotItem's folder_path onto the event JSON so
        # pst_convert can group events by source-calendar ("Calendar",
        # "Calendar/United States holidays", ...) and rebuild the
        # multi-calendar layout in Outlook. Without this, every event
        # lands in a flat "Calendar" folder regardless of which Graph
        # calendar resource it came from. Falls back to None if the
        # backup didn't capture folder_path so older snapshots stay
        # functional. The pstwriter side reads ``_folderPath`` under
        # this leading-underscore name to make it obvious the value was
        # injected by the exporter rather than coming from Graph itself.
        item_folder_path = getattr(item, "folder_path", None) or ""
        if item_folder_path:
            raw = dict(raw)
            raw["_folderPath"] = str(item_folder_path)

        return raw
