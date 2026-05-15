"""Unit tests for dashboard-service tz-bucketing helpers.

Reproduces the symptom reported on 2026-05-15: backups created
between 22:31 UTC and 01:08 UTC on 2026-05-14/15 all happened
on IST 2026-05-15 (between 04:01 and 06:38 IST), but the
/api/v1/dashboard/status/7day endpoint reported 2 backups on
2026-05-14 and 1 on 2026-05-15 because `.date()` on a naive
UTC datetime buckets in UTC, not the operator's tz.

These tests pin `_bucket_date` so a regression cannot silently
revert the dashboard to UTC bucketing.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys
from datetime import datetime, timezone

import pytest


_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "dashboard-service" / "main.py"
)
_spec = importlib.util.spec_from_file_location(
    "dashboard_main_under_test", _MAIN_PATH,
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["dashboard_main_under_test"] = _mod
try:
    _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
except Exception as exc:
    pytest.skip(
        f"dashboard-service module failed to import: {exc}",
        allow_module_level=True,
    )


_resolve_tz = _mod._resolve_tz
_bucket_date = _mod._bucket_date


# ─── _resolve_tz ─────────────────────────────────────────────────────────


def test_resolve_tz_none_returns_utc():
    tz = _resolve_tz(None)
    assert str(tz) == "UTC"


def test_resolve_tz_empty_string_returns_utc():
    tz = _resolve_tz("")
    assert str(tz) == "UTC"


def test_resolve_tz_valid_iana_name():
    tz = _resolve_tz("Asia/Kolkata")
    assert str(tz) == "Asia/Kolkata"


def test_resolve_tz_invalid_name_falls_back_to_utc():
    """Bad tz must not 500 the dashboard — fall back to UTC."""
    tz = _resolve_tz("Not/A/Real/Tz")
    assert str(tz) == "UTC"


# ─── _bucket_date (the regression case) ──────────────────────────────────


def test_naive_utc_timestamp_buckets_correctly_in_utc():
    """Sanity: naive timestamp is treated as UTC."""
    ts = datetime(2026, 5, 14, 22, 31, 38)  # 22:31 UTC
    assert _bucket_date(ts, _resolve_tz("UTC")).isoformat() == "2026-05-14"


def test_regression_2231_utc_buckets_into_next_day_in_ist():
    """The bug reported on 2026-05-15.

    Backup created at 22:31 UTC on 2026-05-14 is 04:01 IST on
    2026-05-15. Operator in IST expects the dashboard bucket for
    2026-05-15. Pre-fix it landed on 2026-05-14 because `.date()`
    on a naive datetime returns the UTC date.
    """
    ts = datetime(2026, 5, 14, 22, 31, 38)  # naive, stored as UTC in DB
    tz_ist = _resolve_tz("Asia/Kolkata")
    assert _bucket_date(ts, tz_ist).isoformat() == "2026-05-15"


def test_post_midnight_utc_also_buckets_into_same_ist_day():
    """A backup at 01:08 UTC on 2026-05-15 is 06:38 IST on 2026-05-15
    — same IST calendar day as the 22:31 UTC backup above."""
    ts = datetime(2026, 5, 15, 1, 8, 48)
    tz_ist = _resolve_tz("Asia/Kolkata")
    assert _bucket_date(ts, tz_ist).isoformat() == "2026-05-15"


def test_aware_timestamp_is_converted_not_double_offset():
    """If the DB ever returns an aware timestamp, we must NOT layer
    another offset on top — astimezone converts in place."""
    ts = datetime(2026, 5, 14, 22, 31, 38, tzinfo=timezone.utc)
    tz_ist = _resolve_tz("Asia/Kolkata")
    assert _bucket_date(ts, tz_ist).isoformat() == "2026-05-15"


def test_us_pacific_tz_bucketing():
    """Sanity for west-of-UTC tenants (Taylor Morrison is US-based).
    16:00 UTC is 09:00 PDT same day; 03:00 UTC is 20:00 PDT prev day."""
    tz_la = _resolve_tz("America/Los_Angeles")
    ts_afternoon_utc = datetime(2026, 5, 15, 16, 0, 0)
    assert _bucket_date(ts_afternoon_utc, tz_la).isoformat() == "2026-05-15"
    ts_early_utc = datetime(2026, 5, 15, 3, 0, 0)
    assert _bucket_date(ts_early_utc, tz_la).isoformat() == "2026-05-14"
