"""Tests for the archived-resource branch in retention_cleanup.

Covers the four archived_retention_mode variants: SAME / KEEP_ALL /
KEEP_LAST / CUSTOM. Uses lightweight stand-in objects with the same
attribute shape as the SQLAlchemy ORM rows the production code reads —
keeps tests fast and avoids a DB dependency.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from shared.retention_cleanup import (
    _archived_keep_ids,
    _is_archived,
)


def _snap(id_str: str, days_ago: int):
    """Stand-in Snapshot ORM object — only the fields _archived_keep_ids
    actually reads."""
    return SimpleNamespace(
        id=id_str,
        started_at=datetime.utcnow() - timedelta(days=days_ago),
        created_at=datetime.utcnow() - timedelta(days=days_ago),
    )


def _policy(mode: str, days=None):
    return SimpleNamespace(
        archived_retention_mode=mode,
        archived_retention_days=days,
    )


# ── KEEP_ALL ────────────────────────────────────────────────────────────

def test_keep_all_returns_every_snapshot():
    snaps = [_snap("a", 1), _snap("b", 30), _snap("c", 365)]
    keep = _archived_keep_ids(snaps, _policy("KEEP_ALL"))
    assert keep == {"a", "b", "c"}


def test_keep_all_with_no_snapshots():
    assert _archived_keep_ids([], _policy("KEEP_ALL")) == set()


# ── KEEP_LAST ───────────────────────────────────────────────────────────

def test_keep_last_returns_most_recent_only():
    snaps = [_snap("oldest", 100), _snap("middle", 50), _snap("newest", 1)]
    keep = _archived_keep_ids(snaps, _policy("KEEP_LAST"))
    assert keep == {"newest"}


def test_keep_last_with_single_snapshot():
    snaps = [_snap("only", 5)]
    assert _archived_keep_ids(snaps, _policy("KEEP_LAST")) == {"only"}


# ── CUSTOM ──────────────────────────────────────────────────────────────

def test_custom_keeps_within_window_plus_latest():
    snaps = [
        _snap("very-old", 365),
        _snap("recent", 5),
        _snap("ancient", 1000),
    ]
    keep = _archived_keep_ids(snaps, _policy("CUSTOM", days=30))
    # Within 30 days: "recent". Latest safety net also adds "recent" again.
    assert keep == {"recent"}


def test_custom_with_null_days_is_unlimited():
    snaps = [_snap("any", 999)]
    assert _archived_keep_ids(snaps, _policy("CUSTOM", days=None)) == {"any"}


def test_custom_always_keeps_latest_even_if_outside_window():
    snaps = [_snap("ancient", 5000)]
    keep = _archived_keep_ids(snaps, _policy("CUSTOM", days=7))
    # Latest safety net guarantees the resource never goes empty.
    assert keep == {"ancient"}


# ── SAME ────────────────────────────────────────────────────────────────

def test_same_returns_sentinel_none():
    """SAME means 'caller falls through to FLAT/GFS'. The function
    returns None as a sentinel — caller branches on it."""
    snaps = [_snap("a", 1)]
    keep = _archived_keep_ids(snaps, _policy("SAME"))
    assert keep is None


# ── _is_archived helper ─────────────────────────────────────────────────

def test_is_archived_with_string_status():
    res = SimpleNamespace(status="ARCHIVED")
    assert _is_archived(res) is True


def test_is_archived_with_enum_value():
    # Mimic SAEnum behavior — has .value attribute.
    enum_like = SimpleNamespace(value="ARCHIVED")
    res = SimpleNamespace(status=enum_like)
    assert _is_archived(res) is True


def test_is_archived_returns_false_for_other_statuses():
    for s in ("ACTIVE", "DISCOVERED", "INACCESSIBLE"):
        res = SimpleNamespace(status=s)
        assert _is_archived(res) is False, f"expected False for {s}"
