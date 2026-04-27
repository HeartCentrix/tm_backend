"""CalendarPstWriter: CALENDAR_EVENT items → MapiCalendar → PST.

Per-item flow:
1. Extract Microsoft Graph ``event`` JSON from ``SnapshotItem.extra_data["raw"]``.
2. Skip child occurrences (``seriesMasterId`` non-null) — Aspose expands a
   series from its master automatically when reading the PST.
3. Build a ``MapiCalendar`` directly from the Graph fields (subject,
   start/end, location, body, organizer, attendees, categories,
   recurrence).  We do *not* round-trip through ICS.

All ``aspose.*`` imports stay lazy so the module loads without
``aspose-email`` installed (tests substitute it via ``sys.modules``).
"""
from __future__ import annotations

import importlib
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Optional

# Make the sibling restore-worker directory importable so future helpers
# (analogous to ``mail_fetch``) resolve in both prod and tests.
_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(os.path.join(_HERE, ".."))
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)

from .base import PstWriterBase  # noqa: E402

logger = logging.getLogger(__name__)


async def _attach_to_mapi(mapi_mod, mapi_item, attachments) -> None:
    """Attach ``AttachmentRef`` items to a MAPI item (calendar or message).

    Each ``AttachmentRef`` exposes ``name`` and an async ``data_stream``.
    We stream the bytes, then call ``MapiAttachmentCollection.add()`` with
    the file name + bytes — the Aspose API used identically by mail and
    calendar items.
    """
    coll = getattr(mapi_item, "attachments", None)
    if coll is None:
        logger.debug("attachments collection not exposed on this MAPI type — skipping")
        return
    for ref in attachments:
        try:
            chunks = []
            async for chunk in ref.data_stream:
                chunks.append(chunk)
            data = b"".join(chunks)
            if not data:
                continue
            # MapiAttachmentCollection.add(name, data) — Aspose 26.x signature
            try:
                coll.add(ref.name, data)
            except Exception:
                # Some Aspose builds expose `append` instead — try that path.
                if hasattr(coll, "append"):
                    coll.append(ref.name, data)
                else:
                    raise
        except Exception as exc:
            logger.warning("attach failed for %s: %s", getattr(ref, "name", "?"), exc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Map Graph weekday names to MAPI ``DayOfWeek`` bit positions (Sunday=1,
# Monday=2, Tuesday=4, ..., Saturday=64).  These bit values are what
# ``MapiCalendarDayOfWeek`` exposes in the real aspose-email package; the
# numeric form is robust against the test-time MagicMock substitution and
# matches the underlying MS-OXOCAL spec.
_GRAPH_DAY_TO_MAPI_BIT = {
    "sunday": 1,
    "monday": 2,
    "tuesday": 4,
    "wednesday": 8,
    "thursday": 16,
    "friday": 32,
    "saturday": 64,
}


def _parse_graph_dt(node: dict) -> Optional[datetime]:
    """Parse a Graph ``{"dateTime": "...", "timeZone": "..."}`` block.

    Graph emits the wall-clock string without a Z/offset; the timezone is
    reported separately.  We attempt to attach it via ``zoneinfo`` and fall
    back to a naive ``datetime`` when the zone name is unknown (this still
    lets the MAPI layer accept the value — Outlook treats unzoned
    appointments as local time).
    """
    if not node:
        return None
    raw = node.get("dateTime")
    if not raw:
        return None
    try:
        # Trim sub-second precision aggressively — Graph occasionally returns
        # 7-digit fractional seconds which fromisoformat() rejects on <3.11.
        s = raw.replace("Z", "")
        if "." in s:
            head, frac = s.split(".", 1)
            frac = frac[:6]
            s = f"{head}.{frac}"
        dt = datetime.fromisoformat(s)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("calendar: failed to parse dateTime %r: %s", raw, exc)
        return None

    tz_name = node.get("timeZone")
    if tz_name and dt.tzinfo is None:
        try:
            from zoneinfo import ZoneInfo  # py>=3.9

            # Graph Windows-style names ("Pacific Standard Time") are not IANA;
            # ZoneInfo will raise.  Try IANA first, then leave naive.
            try:
                dt = dt.replace(tzinfo=ZoneInfo(tz_name))
            except Exception:
                # Graph's Windows zone names — leave naive; MAPI tz field
                # below carries the original name for Outlook.
                pass
        except Exception:  # pragma: no cover - 3.8 / unavailable
            pass
    return dt


def _set_if(obj: Any, attr: str, value: Any) -> None:
    """Best-effort attribute set; swallows failures from the mock surface."""
    try:
        setattr(obj, attr, value)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("calendar: failed to set %s: %s", attr, exc)


def _build_attendees_collection(mapi_mod, attendees: list):
    """Translate Graph attendees into the MAPI attendees collection."""
    try:
        MapiCalendarAttendees = mapi_mod.MapiCalendarAttendees
        MapiCalendarAttendee = mapi_mod.MapiCalendarAttendee
    except AttributeError:
        return None

    coll = MapiCalendarAttendees()
    for a in attendees or []:
        ea = (a or {}).get("emailAddress") or {}
        name = ea.get("name") or ""
        addr = ea.get("address") or ""
        try:
            attendee = MapiCalendarAttendee(addr, name)
        except Exception:
            try:
                attendee = MapiCalendarAttendee()
                _set_if(attendee, "address", addr)
                _set_if(attendee, "display_name", name)
            except Exception:
                continue
        # Required vs optional vs resource
        a_type = (a or {}).get("type", "required")
        _set_if(attendee, "attendee_type_string", a_type)
        try:
            coll.append(attendee)
        except Exception:
            try:
                coll.add(attendee)
            except Exception:
                pass
    return coll


def _build_organizer(mapi_mod, organizer: dict):
    """Translate Graph organizer into a MAPI organizer object."""
    if not organizer:
        return None
    ea = organizer.get("emailAddress") or {}
    name = ea.get("name") or ""
    addr = ea.get("address") or ""
    try:
        MapiCalendarOrganizer = mapi_mod.MapiCalendarOrganizer
    except AttributeError:
        return None
    try:
        return MapiCalendarOrganizer(addr, name)
    except Exception:
        try:
            org = MapiCalendarOrganizer()
            _set_if(org, "address", addr)
            _set_if(org, "display_name", name)
            return org
        except Exception:
            return None


def _build_recurrence(mapi_mod, recurrence: dict, start_dt: Optional[datetime]):
    """Build a ``MapiCalendarEventRecurrence`` from a Graph ``recurrence``.

    Returns the recurrence object, or ``None`` when the pattern is
    unsupported or construction fails.
    """
    if not recurrence:
        return None

    pattern = recurrence.get("pattern") or {}
    range_ = recurrence.get("range") or {}
    pattern_type = (pattern.get("type") or "").lower()
    interval = int(pattern.get("interval") or 1)

    try:
        MapiCalendarEventRecurrence = mapi_mod.MapiCalendarEventRecurrence
    except AttributeError:
        return None

    rec_pattern = None
    try:
        if pattern_type == "weekly":
            cls = mapi_mod.MapiCalendarWeeklyRecurrencePattern
            rec_pattern = cls()
            _set_if(rec_pattern, "period", interval)
            days = pattern.get("daysOfWeek") or []
            bitmask = 0
            for d in days:
                bitmask |= _GRAPH_DAY_TO_MAPI_BIT.get((d or "").lower(), 0)
            _set_if(rec_pattern, "day", bitmask)
        elif pattern_type == "daily":
            cls = mapi_mod.MapiCalendarDailyRecurrencePattern
            rec_pattern = cls()
            _set_if(rec_pattern, "period", interval)
        elif pattern_type in ("absolutemonthly", "relativemonthly"):
            cls = mapi_mod.MapiCalendarMonthlyRecurrencePattern
            rec_pattern = cls()
            _set_if(rec_pattern, "period", interval)
            day_of_month = pattern.get("dayOfMonth")
            if day_of_month is not None:
                _set_if(rec_pattern, "day", int(day_of_month))
        elif pattern_type in ("absoluteyearly", "relativeyearly"):
            cls = mapi_mod.MapiCalendarYearlyRecurrencePattern
            rec_pattern = cls()
            _set_if(rec_pattern, "period", interval)
        else:
            logger.warning(
                "calendar: unsupported recurrence pattern type %r — skipping recurrence",
                pattern_type,
            )
            return None
    except Exception as exc:
        logger.warning("calendar: failed to build recurrence pattern: %s", exc)
        return None

    # Range / end conditions
    range_type = (range_.get("type") or "").lower()
    occurrences = range_.get("numberOfOccurrences")
    end_date_raw = range_.get("endDate")
    start_date_raw = range_.get("startDate")

    try:
        if range_type == "enddate" and end_date_raw:
            try:
                end_dt = datetime.fromisoformat(end_date_raw)
                _set_if(rec_pattern, "end_date", end_dt)
            except Exception:
                pass
        elif range_type == "numbered" and occurrences:
            _set_if(rec_pattern, "occurrence_count", int(occurrences))
        # noend → leave defaults; some MAPI surfaces use a sentinel max date
        if start_date_raw:
            try:
                sd = datetime.fromisoformat(start_date_raw)
                _set_if(rec_pattern, "start_date", sd)
            except Exception:
                pass
    except Exception as exc:  # pragma: no cover
        logger.debug("calendar: recurrence range setup failed: %s", exc)

    try:
        rec = MapiCalendarEventRecurrence()
        _set_if(rec, "recurrence_pattern", rec_pattern)
        return rec
    except Exception as exc:
        logger.warning("calendar: failed to build event recurrence wrapper: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class CalendarPstWriter(PstWriterBase):
    """Convert ``CALENDAR_EVENT`` SnapshotItems into ``MapiCalendar`` entries."""

    item_type = "CALENDAR_EVENT"
    standard_folder_type = "Calendar"

    async def _build_mapi_item(self, item, shard, source_container: str):
        """Build a ``MapiCalendar`` from Graph event JSON.

        Returns the MAPI object on success or ``None`` when:
          * the item is a child occurrence of a recurring series (skipped
            silently — Aspose expands the series from its master);
          * ``extra_data["raw"]`` is missing;
          * any underlying call raises (logged at ERROR).
        """
        extra = getattr(item, "extra_data", None) or {}
        raw_event = extra.get("raw")
        if not raw_event:
            logger.warning(
                "calendar: missing extra_data['raw'] for item %s",
                getattr(item, "external_id", "?"),
            )
            return None

        # Series children are emitted by Aspose when the master is written.
        if raw_event.get("seriesMasterId"):
            logger.debug(
                "calendar: skipping child occurrence %s (parent=%s)",
                raw_event.get("id"),
                raw_event.get("seriesMasterId"),
            )
            return None

        try:
            mapi_mod = importlib.import_module("aspose.email.mapi")
            MapiCalendar = mapi_mod.MapiCalendar

            cal = MapiCalendar()

            # --- Basic scalar fields -----------------------------------
            subject = raw_event.get("subject") or ""
            _set_if(cal, "subject", subject)

            location = (raw_event.get("location") or {}).get("displayName")
            if location:
                _set_if(cal, "location", location)

            is_all_day = bool(raw_event.get("isAllDay"))
            _set_if(cal, "is_all_day", is_all_day)

            # --- Start / end -------------------------------------------
            start_dt = _parse_graph_dt(raw_event.get("start") or {})
            end_dt = _parse_graph_dt(raw_event.get("end") or {})
            if start_dt is not None:
                _set_if(cal, "start_date", start_dt)
            if end_dt is not None:
                _set_if(cal, "end_date", end_dt)

            # --- Body --------------------------------------------------
            body = raw_event.get("body") or {}
            body_content = body.get("content")
            body_type = (body.get("contentType") or "").lower()
            if body_content:
                _set_if(cal, "body", body_content)
                if body_type == "html":
                    _set_if(cal, "body_html", body_content)
            elif raw_event.get("bodyPreview"):
                _set_if(cal, "body", raw_event["bodyPreview"])

            # --- Categories --------------------------------------------
            categories = raw_event.get("categories") or []
            if categories:
                _set_if(cal, "categories", list(categories))

            # --- Organizer ---------------------------------------------
            organizer_obj = _build_organizer(mapi_mod, raw_event.get("organizer") or {})
            if organizer_obj is not None:
                _set_if(cal, "organizer", organizer_obj)

            # --- Attendees ---------------------------------------------
            attendees_coll = _build_attendees_collection(
                mapi_mod, raw_event.get("attendees") or []
            )
            if attendees_coll is not None:
                _set_if(cal, "attendees", attendees_coll)

            # --- Recurrence (series masters only) ----------------------
            recurrence = raw_event.get("recurrence")
            if recurrence:
                rec_obj = _build_recurrence(mapi_mod, recurrence, start_dt)
                if rec_obj is not None:
                    _set_if(cal, "recurrence", rec_obj)

            # --- Attachments -------------------------------------------
            # Graph events can have file attachments backed up to blob storage.
            # Same pattern as MailPstWriter: pull blob_paths from the item,
            # download bytes, attach to the MapiCalendar.attachments collection.
            try:
                att_paths = getattr(item, "attachment_blob_paths", None) or []
                # Some backup pipelines stash attachment paths under
                # extra_data (calendar events historically used this) — fall
                # back to that location for backward compat.
                if not att_paths:
                    att_paths = (
                        (extra.get("attachment_blob_paths") or [])
                        if isinstance(extra, dict) else []
                    )
                if att_paths and shard:
                    from mail_fetch import gather_attachments
                    attachments = await gather_attachments(
                        att_paths, shard, source_container, True
                    )
                    if attachments:
                        await _attach_to_mapi(mapi_mod, cal, attachments)
            except Exception as att_exc:
                logger.warning(
                    "calendar: attachment processing failed for %s (continuing without): %s",
                    raw_event.get("id"), att_exc,
                )

            return cal

        except Exception as exc:
            logger.error(
                "calendar: failed to build MapiCalendar for item %s: %s",
                getattr(item, "external_id", "?"),
                exc,
            )
            return None
