"""Tests for PstGroupPlanner (and _safe_name helper) in pst_export.py.

All aspose.* modules are absent in the test environment; pst_export.py itself
never imports them at module level, so no sys.modules patching is required for
these planner tests.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys
import types
import uuid
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Load pst_export without triggering the restore-worker main.py bootstrapping
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(__file__)
_PST_EXPORT_PATH = os.path.abspath(
    os.path.join(_HERE, "..", "..", "workers", "restore-worker", "pst_export.py")
)

# Ensure shared/ is importable (mirrors what conftest does for the real modules)
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

spec = importlib.util.spec_from_file_location("pst_export", _PST_EXPORT_PATH)
_pst_export_mod = importlib.util.module_from_spec(spec)
sys.modules["pst_export"] = _pst_export_mod
spec.loader.exec_module(_pst_export_mod)

PstGroupPlanner = _pst_export_mod.PstGroupPlanner
PstGroup = _pst_export_mod.PstGroup
_safe_name = _pst_export_mod._safe_name


# ---------------------------------------------------------------------------
# Minimal mock SnapshotItem
# ---------------------------------------------------------------------------

class _Item:
    """Lightweight stand-in for SnapshotItem — only fields used by planner."""

    def __init__(
        self,
        snapshot_id,
        external_id: str,
        item_type: str,
        folder_path: str | None = None,
        name: str = "",
    ):
        self.snapshot_id = snapshot_id
        self.external_id = external_id
        self.item_type = item_type
        self.folder_path = folder_path
        self.name = name


# Stable UUIDs for mailbox A and B
_SNAP_A = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000000")
_SNAP_B = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000000")


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------

def _items_for_snapshot(snap_id, count_per_type=1, folder="Inbox"):
    """Return one item of each PST type for the given snapshot."""
    items = []
    for i in range(count_per_type):
        items.append(_Item(snap_id, f"email-{snap_id}-{i}", "EMAIL", folder))
        items.append(_Item(snap_id, f"cal-{snap_id}-{i}", "CALENDAR_EVENT", folder))
        items.append(_Item(snap_id, f"contact-{snap_id}-{i}", "USER_CONTACT", folder))
    return items


# ===========================================================================
# Test 1 — MAILBOX granularity: 2 mailboxes × 3 types → 6 groups
# ===========================================================================

def test_mailbox_granularity_two_mailboxes():
    items = _items_for_snapshot(_SNAP_A) + _items_for_snapshot(_SNAP_B)
    planner = PstGroupPlanner()
    groups = planner.plan(items, "MAILBOX", PstGroupPlanner.PST_ITEM_TYPES)

    assert len(groups) == 6

    # All groups have exactly 1 item in this fixture
    for g in groups:
        assert len(g.items) == 1

    # Collect filenames and keys
    filenames = {g.pst_filename for g in groups}
    a_prefix = str(_SNAP_A)[:8]
    b_prefix = str(_SNAP_B)[:8]
    assert f"{a_prefix}-mail.pst" in filenames
    assert f"{a_prefix}-calendar.pst" in filenames
    assert f"{a_prefix}-contacts.pst" in filenames
    assert f"{b_prefix}-mail.pst" in filenames
    assert f"{b_prefix}-calendar.pst" in filenames
    assert f"{b_prefix}-contacts.pst" in filenames

    # Keys must be (snapshot_id_str, item_type)
    for g in groups:
        assert len(g.key) == 2
        assert g.key[1] in PstGroupPlanner.PST_ITEM_TYPES


def test_mailbox_granularity_multiple_items_same_group():
    """Multiple items with same snapshot+type land in the same group."""
    items = [
        _Item(_SNAP_A, "e1", "EMAIL", "Inbox"),
        _Item(_SNAP_A, "e2", "EMAIL", "Sent"),
        _Item(_SNAP_A, "e3", "EMAIL", "Drafts"),
    ]
    planner = PstGroupPlanner()
    groups = planner.plan(items, "MAILBOX", PstGroupPlanner.PST_ITEM_TYPES)
    assert len(groups) == 1
    assert len(groups[0].items) == 3
    assert groups[0].item_type == "EMAIL"


# ===========================================================================
# Test 2 — FOLDER granularity: one group per (snapshot_id, type, folder_path)
# ===========================================================================

def test_folder_granularity_groups_by_folder():
    items = [
        _Item(_SNAP_A, "e1", "EMAIL", "Inbox"),
        _Item(_SNAP_A, "e2", "EMAIL", "Inbox"),
        _Item(_SNAP_A, "e3", "EMAIL", "Sent Items"),
        _Item(_SNAP_A, "c1", "CALENDAR_EVENT", "Calendar"),
        _Item(_SNAP_A, "c2", "USER_CONTACT", "Contacts"),
    ]
    planner = PstGroupPlanner()
    groups = planner.plan(items, "FOLDER", PstGroupPlanner.PST_ITEM_TYPES)

    # Inbox/EMAIL, Sent Items/EMAIL, Calendar/CALENDAR_EVENT, Contacts/USER_CONTACT
    assert len(groups) == 4

    # The Inbox group has 2 items
    inbox_groups = [g for g in groups if "Inbox" in g.key]
    assert len(inbox_groups) == 1
    assert len(inbox_groups[0].items) == 2

    # Filenames contain safe folder names
    filenames = {g.pst_filename for g in groups}
    a_prefix = str(_SNAP_A)[:8]
    assert f"{a_prefix}-Inbox-mail.pst" in filenames
    assert f"{a_prefix}-Sent_Items-mail.pst" in filenames
    assert f"{a_prefix}-Calendar-calendar.pst" in filenames
    assert f"{a_prefix}-Contacts-contacts.pst" in filenames


def test_folder_granularity_none_folder_maps_to_root():
    items = [_Item(_SNAP_A, "e1", "EMAIL", None)]
    planner = PstGroupPlanner()
    groups = planner.plan(items, "FOLDER", PstGroupPlanner.PST_ITEM_TYPES)
    assert len(groups) == 1
    assert "root" in groups[0].key
    a_prefix = str(_SNAP_A)[:8]
    assert groups[0].pst_filename == f"{a_prefix}-root-mail.pst"


# ===========================================================================
# Test 3 — ITEM granularity: each item gets its own group
# ===========================================================================

def test_item_granularity_one_group_per_item():
    items = [
        _Item(_SNAP_A, "email-001", "EMAIL", "Inbox"),
        _Item(_SNAP_A, "cal-001", "CALENDAR_EVENT", "Calendar"),
        _Item(_SNAP_A, "cnt-001", "USER_CONTACT", "Contacts"),
    ]
    planner = PstGroupPlanner()
    groups = planner.plan(items, "ITEM", PstGroupPlanner.PST_ITEM_TYPES)

    assert len(groups) == 3
    for g in groups:
        assert len(g.items) == 1

    a_prefix = str(_SNAP_A)[:8]
    filenames = {g.pst_filename for g in groups}
    assert f"{a_prefix}-email-001-mail.pst" in filenames
    assert f"{a_prefix}-cal-001-calendar.pst" in filenames
    assert f"{a_prefix}-cnt-001-contacts.pst" in filenames


# ===========================================================================
# Test 4 — include_types filtering: only EMAIL when specified
# ===========================================================================

def test_include_types_filters_to_email_only():
    items = [
        _Item(_SNAP_A, "e1", "EMAIL", "Inbox"),
        _Item(_SNAP_A, "c1", "CALENDAR_EVENT", "Calendar"),
        _Item(_SNAP_A, "ct1", "USER_CONTACT", "Contacts"),
    ]
    planner = PstGroupPlanner()
    groups = planner.plan(items, "MAILBOX", {"EMAIL"})

    assert len(groups) == 1
    assert groups[0].item_type == "EMAIL"


def test_include_types_empty_set_returns_no_groups():
    items = _items_for_snapshot(_SNAP_A)
    planner = PstGroupPlanner()
    groups = planner.plan(items, "MAILBOX", set())
    assert groups == []


# ===========================================================================
# Test 5 — Unknown item types skipped silently
# ===========================================================================

def test_unknown_item_type_skipped_silently():
    items = [
        _Item(_SNAP_A, "t1", "TEAMS_MESSAGE", "General"),
        _Item(_SNAP_A, "e1", "EMAIL", "Inbox"),
        _Item(_SNAP_A, "sp1", "SHAREPOINT_FILE", None),
    ]
    planner = PstGroupPlanner()
    groups = planner.plan(items, "MAILBOX", PstGroupPlanner.PST_ITEM_TYPES)

    assert len(groups) == 1
    assert groups[0].item_type == "EMAIL"


# ===========================================================================
# Test 6 — Unknown granularity raises ValueError
# ===========================================================================

def test_unknown_granularity_raises_value_error():
    planner = PstGroupPlanner()
    with pytest.raises(ValueError, match="Unknown PST granularity"):
        planner.plan([], "UNKNOWN_GRAN", PstGroupPlanner.PST_ITEM_TYPES)


def test_unknown_granularity_message_item():
    planner = PstGroupPlanner()
    with pytest.raises(ValueError):
        planner.plan([], "item", PstGroupPlanner.PST_ITEM_TYPES)  # lowercase → invalid


# ===========================================================================
# Test 7 — ITEM granularity warn at >10k items
# ===========================================================================

def test_item_granularity_warning_above_10k(caplog):
    # Build 10001 distinct EMAIL items so we get 10001 groups
    items = [
        _Item(_SNAP_A, f"email-{i:06d}", "EMAIL", "Inbox")
        for i in range(10_001)
    ]
    planner = PstGroupPlanner()

    with caplog.at_level(logging.WARNING, logger="pst_export"):
        groups = planner.plan(items, "ITEM", PstGroupPlanner.PST_ITEM_TYPES)

    assert len(groups) == 10_001
    assert any("PST ITEM granularity" in r.message for r in caplog.records)
    assert any("10001" in r.message for r in caplog.records)


# ===========================================================================
# Test 8 — ITEM granularity reject at >100k items
# ===========================================================================

def test_item_granularity_rejected_above_100k():
    items = [
        _Item(_SNAP_A, f"email-{i:07d}", "EMAIL", "Inbox")
        for i in range(100_001)
    ]
    planner = PstGroupPlanner()
    with pytest.raises(ValueError, match="exceeds 100k limit"):
        planner.plan(items, "ITEM", PstGroupPlanner.PST_ITEM_TYPES)


# ===========================================================================
# Test 9 — Empty items list → empty groups list
# ===========================================================================

def test_empty_items_returns_empty_groups():
    planner = PstGroupPlanner()
    for gran in ("MAILBOX", "FOLDER", "ITEM"):
        groups = planner.plan([], gran, PstGroupPlanner.PST_ITEM_TYPES)
        assert groups == [], f"Expected [] for granularity={gran}"


# ===========================================================================
# Test 10 — _safe_name: special chars replaced, truncated at 64
# ===========================================================================

def test_safe_name_replaces_special_chars():
    assert _safe_name("Inbox/Work Items") == "Inbox_Work_Items"


def test_safe_name_keeps_allowed_chars():
    assert _safe_name("My-Folder_2") == "My-Folder_2"


def test_safe_name_truncates_to_64():
    long_str = "A" * 100
    result = _safe_name(long_str)
    assert len(result) == 64
    assert result == "A" * 64


def test_safe_name_unicode_replaced():
    result = _safe_name("Posteingang (über alles)")
    # Spaces, parentheses, and non-ASCII chars become underscores
    assert re.search(r"[^A-Za-z0-9\-_]", result) is None


def test_safe_name_empty_string():
    assert _safe_name("") == ""


def test_safe_name_slash_and_dots():
    assert _safe_name("a/b.c") == "a_b_c"


import re  # noqa: E402 — used in test body above, imported at module level here
