"""Tests for the sub-calendar fetch throttle.

Pins the rule: a secondary calendar (Birthdays, US Holidays, shared,
group) is skipped on this run if its last successful fetch is within
the refresh window. Origin: 2026-05-15 case where every backup
re-pulled "Calendar/United States holidays" (74 events / 195 KiB)
because Microsoft Graph exposes /calendarView/delta only on the
default calendar.

backup-worker has a hyphen in the dir name, so the helper is loaded
via AST extraction — same pattern as test_anomaly_cursor_guard.py.
"""
from __future__ import annotations

import ast
import datetime as dt
import importlib.util
import pathlib
import sys

import pytest


_BW_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)


def _load_helper():
    source = _BW_MAIN.read_text()
    tree = ast.parse(source)
    helper = next(
        (n for n in tree.body
         if isinstance(n, ast.FunctionDef) and n.name == "_should_skip_subcalendar"),
        None,
    )
    if helper is None:
        pytest.skip(
            "_should_skip_subcalendar not found at module scope",
            allow_module_level=True,
        )
    module = type(sys)("backup_worker_subcal_helper_under_test")
    module.__dict__["datetime"] = dt.datetime
    module.__dict__["timezone"] = dt.timezone
    code = compile(
        ast.Module(body=[helper], type_ignores=[]),
        str(_BW_MAIN),
        "exec",
    )
    exec(code, module.__dict__)
    return module._should_skip_subcalendar


should_skip = _load_helper()


_NOW = dt.datetime(2026, 5, 15, 12, 0, 0, tzinfo=dt.timezone.utc)


def test_never_fetched_runs_fresh():
    """No prior fetch → don't skip — we must do an initial pull."""
    assert should_skip(
        last_fetched_iso=None, now_dt=_NOW, refresh_hours=24,
    ) is False


def test_empty_string_runs_fresh():
    """A bare empty string in DB is equivalent to None."""
    assert should_skip(
        last_fetched_iso="", now_dt=_NOW, refresh_hours=24,
    ) is False


def test_recent_fetch_within_window_is_skipped():
    """Fetched 1h ago, window 24h → skip."""
    last = (_NOW - dt.timedelta(hours=1)).isoformat()
    assert should_skip(
        last_fetched_iso=last, now_dt=_NOW, refresh_hours=24,
    ) is True


def test_fetch_exactly_at_window_runs_fresh():
    """Edge case: fetched exactly `refresh_hours` ago → elapsed not
    strictly less than threshold → re-fetch. Prevents drift where a
    24h cron lines up exactly with the window boundary and the
    sub-calendar would otherwise never get refreshed."""
    last = (_NOW - dt.timedelta(hours=24)).isoformat()
    assert should_skip(
        last_fetched_iso=last, now_dt=_NOW, refresh_hours=24,
    ) is False


def test_old_fetch_outside_window_runs_fresh():
    """Fetched 25h ago, window 24h → re-fetch."""
    last = (_NOW - dt.timedelta(hours=25)).isoformat()
    assert should_skip(
        last_fetched_iso=last, now_dt=_NOW, refresh_hours=24,
    ) is False


def test_naive_iso_is_normalised_to_utc():
    """ISO without tzinfo (legacy entries) is treated as UTC. We do
    NOT want to be tz-locale-sensitive — every fetched_at is written
    in UTC, but a regression that drops the tz suffix shouldn't
    silently flip the comparison."""
    last = (_NOW.replace(tzinfo=None) - dt.timedelta(hours=2)).isoformat()
    # 2h ago, window 24h → still skip
    assert should_skip(
        last_fetched_iso=last, now_dt=_NOW, refresh_hours=24,
    ) is True


def test_z_suffix_iso_parses():
    """Graph and some persisters use 'Z' suffix instead of '+00:00'."""
    last_dt = _NOW - dt.timedelta(hours=2)
    iso_z = last_dt.replace(tzinfo=None).isoformat() + "Z"
    assert should_skip(
        last_fetched_iso=iso_z, now_dt=_NOW, refresh_hours=24,
    ) is True


def test_unparseable_value_runs_fresh():
    """Corrupt DB value (e.g. someone wrote a free-text string)
    should not cause us to silently keep skipping forever. Fall
    through to a fresh fetch — the fresh fetch will overwrite the
    bad value with a valid timestamp."""
    assert should_skip(
        last_fetched_iso="not-an-iso-date",
        now_dt=_NOW, refresh_hours=24,
    ) is False


def test_zero_refresh_hours_always_runs_fresh():
    """An operator setting CALENDAR_SUBCALENDAR_REFRESH_HOURS=0
    should disable the throttle entirely — useful for forcing
    re-pulls during incident response."""
    last = (_NOW - dt.timedelta(seconds=30)).isoformat()
    assert should_skip(
        last_fetched_iso=last, now_dt=_NOW, refresh_hours=0,
    ) is False


def test_fractional_refresh_hours_supported():
    """Test sub-hour windows (e.g. 0.5h = 30 min) work correctly."""
    last_15min_ago = (_NOW - dt.timedelta(minutes=15)).isoformat()
    last_45min_ago = (_NOW - dt.timedelta(minutes=45)).isoformat()
    assert should_skip(
        last_fetched_iso=last_15min_ago, now_dt=_NOW, refresh_hours=0.5,
    ) is True
    assert should_skip(
        last_fetched_iso=last_45min_ago, now_dt=_NOW, refresh_hours=0.5,
    ) is False
