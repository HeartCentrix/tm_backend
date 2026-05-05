"""Unit tests for ``pst_writers.calendar.CalendarPstWriter._collect_graph_item``.

Pass-through writer; the only logic worth testing is series-child
suppression (master carries the recurrence rule).
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Path setup — must happen before importing the writer.
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


class _Item:
    def __init__(self, ext_id="evt-1", extra_data=None):
        self.external_id = ext_id
        self.extra_data = extra_data or {}


# ---------------------------------------------------------------------------
# Class wiring
# ---------------------------------------------------------------------------

def test_class_constants():
    assert CalendarPstWriter.item_type == "CALENDAR_EVENT"
    assert CalendarPstWriter.cli_kind == "calendar"
    assert issubclass(CalendarPstWriter, PstWriterBase)


# ---------------------------------------------------------------------------
# Behavior
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_raw_event():
    raw = {
        "id": "e1",
        "subject": "Sprint planning",
        "start": {"dateTime": "2026-01-12T09:00:00", "timeZone": "UTC"},
        "end": {"dateTime": "2026-01-12T10:00:00", "timeZone": "UTC"},
    }
    item = _Item(extra_data={"raw": raw})
    out = await CalendarPstWriter()._collect_graph_item(item, shard=MagicMock(), source_container="c")
    assert out is raw


@pytest.mark.asyncio
async def test_skips_series_children():
    raw = {"id": "occ-1", "seriesMasterId": "master-99", "subject": "occurrence"}
    item = _Item(extra_data={"raw": raw})
    out = await CalendarPstWriter()._collect_graph_item(item, shard=MagicMock(), source_container="c")
    assert out is None


@pytest.mark.asyncio
async def test_returns_none_when_missing_raw():
    item = _Item(extra_data={})
    out = await CalendarPstWriter()._collect_graph_item(item, shard=MagicMock(), source_container="c")
    assert out is None
