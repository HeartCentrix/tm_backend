"""Unit tests for ``pst_writers.calendar.CalendarPstWriter._build_mapi_item``.

The CalendarPstWriter translates Graph ``event`` JSON straight into a
MAPI calendar item.  ``aspose-email`` is NOT installed in the test
environment — we substitute ``aspose.email.mapi`` in ``sys.modules``
before importing the writer.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Path / sys.modules setup — must happen before importing the writer.
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(
    os.path.join(_HERE, "..", "..", "workers", "restore-worker")
)
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# Build a fresh aspose.email.mapi mock that records constructor calls so
# tests can assert "MapiCalendarWeeklyRecurrencePattern was instantiated".
# We share the same module across tests but reset attributes per-test in a
# fixture below.  Same notes as test_pst_writer_mail.py: do not register
# the parent `aspose` / `aspose.email` modules — let the license tests own
# those (setdefault).
_aspose_mapi_mock = sys.modules.setdefault("aspose.email.mapi", MagicMock())


# ---------------------------------------------------------------------------
# Load the pst_writers package without triggering restore-worker bootstrap.
# ---------------------------------------------------------------------------

_PST_WRITERS_DIR = os.path.join(_WORKER, "pst_writers")
if "pst_writers" not in sys.modules:
    _pkg = types.ModuleType("pst_writers")
    _pkg.__path__ = [_PST_WRITERS_DIR]
    sys.modules["pst_writers"] = _pkg

if "pst_writers.base" not in sys.modules:
    _base_spec = importlib.util.spec_from_file_location(
        "pst_writers.base", os.path.join(_PST_WRITERS_DIR, "base.py")
    )
    _base_mod = importlib.util.module_from_spec(_base_spec)
    sys.modules["pst_writers.base"] = _base_mod
    _base_spec.loader.exec_module(_base_mod)
else:
    _base_mod = sys.modules["pst_writers.base"]

_cal_spec = importlib.util.spec_from_file_location(
    "pst_writers.calendar", os.path.join(_PST_WRITERS_DIR, "calendar.py")
)
_cal_mod = importlib.util.module_from_spec(_cal_spec)
sys.modules["pst_writers.calendar"] = _cal_mod
_cal_spec.loader.exec_module(_cal_mod)

CalendarPstWriter = _cal_mod.CalendarPstWriter
PstWriterBase = _base_mod.PstWriterBase


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _Item:
    def __init__(self, ext_id="cal-1", extra_data=None):
        self.external_id = ext_id
        self.extra_data = extra_data or {}


@pytest.fixture
def aspose_mapi():
    """Reset the aspose.email.mapi mock between tests.

    We replace the module-level MagicMock with a fresh one each test so
    constructor call assertions don't leak across tests.
    """
    fresh = MagicMock(name="aspose.email.mapi")
    sys.modules["aspose.email.mapi"] = fresh

    # The writer reads ``importlib.import_module("aspose.email.mapi")`` per
    # call, so swapping sys.modules is sufficient.
    yield fresh

    # Restore the original mock (test_pst_writer_mail.py may rely on it).
    sys.modules["aspose.email.mapi"] = _aspose_mapi_mock


def _standalone_event(**overrides):
    """Build a representative standalone Graph event dict."""
    base = {
        "id": "AAMkAGI...",
        "subject": "Team standup",
        "start": {"dateTime": "2025-04-01T09:00:00", "timeZone": "Pacific Standard Time"},
        "end": {"dateTime": "2025-04-01T09:30:00", "timeZone": "Pacific Standard Time"},
        "location": {"displayName": "Conference Room A"},
        "organizer": {"emailAddress": {"name": "Alice", "address": "alice@contoso.com"}},
        "attendees": [
            {"emailAddress": {"name": "Bob", "address": "bob@contoso.com"}, "type": "required"}
        ],
        "body": {"content": "<html><body>hi</body></html>", "contentType": "html"},
        "bodyPreview": "hi",
        "isAllDay": False,
        "categories": ["Blue category"],
        "hasAttachments": False,
        "seriesMasterId": None,
        "recurrence": None,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Class-level constants
# ---------------------------------------------------------------------------

def test_calendar_writer_class_constants():
    """item_type and standard_folder_type are wired correctly."""
    assert CalendarPstWriter.item_type == "CALENDAR_EVENT"
    assert CalendarPstWriter.standard_folder_type == "Calendar"
    assert issubclass(CalendarPstWriter, PstWriterBase)


# ---------------------------------------------------------------------------
# Behavior tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_none_for_child_occurrence(aspose_mapi):
    """An event with seriesMasterId set is a child occurrence — skipped."""
    writer = CalendarPstWriter()
    item = _Item(extra_data={
        "raw": _standalone_event(seriesMasterId="MASTER-AAA", id="OCC-1"),
        "calendarName": "Work",
    })

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is None
    # MapiCalendar must NOT have been instantiated for the skipped child.
    aspose_mapi.MapiCalendar.assert_not_called()


@pytest.mark.asyncio
async def test_builds_mapi_calendar_for_standalone_event(aspose_mapi):
    """Standalone event: subject/start/end/location/body/categories all set."""
    writer = CalendarPstWriter()
    raw = _standalone_event()
    item = _Item(extra_data={"raw": raw, "calendarName": "Work"})

    fake_cal = MagicMock(name="MapiCalendar_instance")
    aspose_mapi.MapiCalendar = MagicMock(return_value=fake_cal)
    aspose_mapi.MapiCalendarOrganizer = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiCalendarAttendees = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiCalendarAttendee = MagicMock(return_value=MagicMock())

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_cal
    aspose_mapi.MapiCalendar.assert_called_once()

    # Subject / location / is_all_day / categories must be set.
    assert fake_cal.subject == "Team standup"
    assert fake_cal.location == "Conference Room A"
    assert fake_cal.is_all_day is False
    assert fake_cal.categories == ["Blue category"]

    # Start/end were assigned (datetimes — exact value depends on tz parse).
    assert fake_cal.start_date is not None
    assert fake_cal.end_date is not None

    # Body assigned (HTML content).
    assert fake_cal.body == "<html><body>hi</body></html>"


@pytest.mark.asyncio
async def test_builds_all_day_event(aspose_mapi):
    """isAllDay=True propagates to cal.is_all_day."""
    writer = CalendarPstWriter()
    raw = _standalone_event(
        isAllDay=True,
        start={"dateTime": "2025-04-01T00:00:00", "timeZone": "UTC"},
        end={"dateTime": "2025-04-02T00:00:00", "timeZone": "UTC"},
    )
    item = _Item(extra_data={"raw": raw})

    fake_cal = MagicMock()
    aspose_mapi.MapiCalendar = MagicMock(return_value=fake_cal)

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_cal
    assert fake_cal.is_all_day is True


@pytest.mark.asyncio
async def test_series_master_sets_recurrence_on_calendar(aspose_mapi):
    """A series master (recurrence present, seriesMasterId null) gets a
    recurrence object attached to the calendar."""
    writer = CalendarPstWriter()
    raw = _standalone_event(
        seriesMasterId=None,
        recurrence={
            "pattern": {"type": "weekly", "interval": 1, "daysOfWeek": ["Monday"]},
            "range": {
                "type": "endDate",
                "startDate": "2025-01-06",
                "endDate": "2025-12-29",
            },
        },
    )
    item = _Item(extra_data={"raw": raw})

    fake_cal = MagicMock()
    aspose_mapi.MapiCalendar = MagicMock(return_value=fake_cal)
    fake_recurrence = MagicMock(name="MapiCalendarEventRecurrence_instance")
    aspose_mapi.MapiCalendarEventRecurrence = MagicMock(return_value=fake_recurrence)
    aspose_mapi.MapiCalendarWeeklyRecurrencePattern = MagicMock(
        return_value=MagicMock(name="WeeklyPattern")
    )

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_cal
    # The wrapper recurrence object must end up on the calendar.
    assert fake_cal.recurrence is fake_recurrence


@pytest.mark.asyncio
async def test_weekly_recurrence_uses_weekly_pattern_class(aspose_mapi):
    """``pattern.type=='weekly'`` → instantiates MapiCalendarWeeklyRecurrencePattern."""
    writer = CalendarPstWriter()
    raw = _standalone_event(
        recurrence={
            "pattern": {
                "type": "weekly",
                "interval": 2,
                "daysOfWeek": ["Monday", "Wednesday"],
            },
            "range": {"type": "noEnd", "startDate": "2025-01-06"},
        },
    )
    item = _Item(extra_data={"raw": raw})

    aspose_mapi.MapiCalendar = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiCalendarEventRecurrence = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiCalendarWeeklyRecurrencePattern = MagicMock(
        return_value=MagicMock(name="WeeklyPattern")
    )
    aspose_mapi.MapiCalendarDailyRecurrencePattern = MagicMock()
    aspose_mapi.MapiCalendarMonthlyRecurrencePattern = MagicMock()

    await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    aspose_mapi.MapiCalendarWeeklyRecurrencePattern.assert_called_once_with()
    aspose_mapi.MapiCalendarDailyRecurrencePattern.assert_not_called()
    aspose_mapi.MapiCalendarMonthlyRecurrencePattern.assert_not_called()


@pytest.mark.asyncio
async def test_returns_none_when_raw_missing(aspose_mapi):
    """No ``extra_data['raw']`` → return None and don't construct MapiCalendar."""
    writer = CalendarPstWriter()
    item = _Item(extra_data={"calendarName": "Work"})  # no 'raw'

    aspose_mapi.MapiCalendar = MagicMock()

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is None
    aspose_mapi.MapiCalendar.assert_not_called()


@pytest.mark.asyncio
async def test_returns_none_when_build_raises(aspose_mapi):
    """An exception during MAPI construction is logged and yields None."""
    writer = CalendarPstWriter()
    item = _Item(extra_data={"raw": _standalone_event()})

    aspose_mapi.MapiCalendar = MagicMock(side_effect=ValueError("boom"))

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is None


@pytest.mark.asyncio
async def test_handles_missing_optional_fields(aspose_mapi):
    """Missing location/categories/attendees/body/organizer must not crash."""
    writer = CalendarPstWriter()
    raw = {
        "id": "AAMk...",
        "subject": "Bare event",
        "start": {"dateTime": "2025-04-01T09:00:00", "timeZone": "UTC"},
        "end": {"dateTime": "2025-04-01T09:30:00", "timeZone": "UTC"},
        "isAllDay": False,
        "seriesMasterId": None,
        # location, organizer, attendees, body, categories, recurrence omitted
    }
    item = _Item(extra_data={"raw": raw})

    fake_cal = MagicMock()
    aspose_mapi.MapiCalendar = MagicMock(return_value=fake_cal)

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_cal
    assert fake_cal.subject == "Bare event"
    # No exception, even though optional fields were omitted.
