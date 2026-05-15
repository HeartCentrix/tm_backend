"""Tests for the anomaly-detector cursor guard.

Pins the rule: a 0-item snapshot for a resource that has any
incremental-backup cursor key in `extra_data` is a quiet delta, not
a mass deletion. Origin: 2026-05-15 Hemant Singh false-positive where
the guard only recognised the legacy `mail_delta_token` key and
missed `mail_delta_tokens_by_folder` (the current USER_MAIL key),
firing BACKUP_ANOMALY on every quiet mail day.

backup-worker has a hyphen in the dir name, so the helper is loaded
via importlib by AST-walking — same pattern as
test_group_batch_jobs_labels.py.
"""
from __future__ import annotations

import ast
import importlib.util
import pathlib
import sys
import textwrap

import pytest


_BW_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)


def _load_cursor_helper():
    """Extract INCREMENTAL_CURSOR_KEYS + _resource_has_incremental_cursor.

    AST-walks main.py, picks just the constant assignment and the
    helper function, and exec()'s them. Skips the rest (FastAPI,
    SQLAlchemy session, etc.) which can't initialise outside a real
    service env.
    """
    source = _BW_MAIN.read_text()
    tree = ast.parse(source)

    constant = None
    helper = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "INCREMENTAL_CURSOR_KEYS"
            for t in node.targets
        ):
            constant = node
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "INCREMENTAL_CURSOR_KEYS"
        ):
            constant = node
        elif (
            isinstance(node, ast.FunctionDef)
            and node.name == "_resource_has_incremental_cursor"
        ):
            helper = node

    if constant is None or helper is None:
        pytest.skip(
            "INCREMENTAL_CURSOR_KEYS / _resource_has_incremental_cursor "
            "not found at module scope",
            allow_module_level=True,
        )

    module = type(sys)("backup_worker_cursor_helper_under_test")
    code = compile(
        ast.Module(body=[constant, helper], type_ignores=[]),
        str(_BW_MAIN),
        "exec",
    )
    exec(code, module.__dict__)
    return module.INCREMENTAL_CURSOR_KEYS, module._resource_has_incremental_cursor


KEYS, has_cursor = _load_cursor_helper()


def test_empty_extra_data_returns_false():
    assert has_cursor({}) is False
    assert has_cursor(None) is False  # type: ignore[arg-type]


def test_legacy_mail_delta_token_recognised():
    """MAILBOX/SHARED_MAILBOX/ROOM_MAILBOX use this singular key."""
    assert has_cursor({"mail_delta_token": "abc"}) is True


def test_user_mail_per_folder_map_recognised():
    """USER_MAIL stores per-folder tokens here. THIS is the key whose
    omission caused the 2026-05-15 Hemant Singh false positives."""
    assert has_cursor({
        "mail_delta_tokens_by_folder": {
            "folder-1": "tok1",
            "folder-2": "tok2",
        }
    }) is True


def test_user_mail_fingerprint_cache_recognised():
    """Pre-table fingerprint cache — also signals incremental path."""
    assert has_cursor({"mail_folder_fingerprints": {"f1": "hash"}}) is True


def test_user_mail_baseline_timestamp_recognised():
    assert has_cursor({"mail_folder_baseline_at": "2026-05-15T00:00:00"}) is True


def test_user_chats_per_chat_map_recognised():
    assert has_cursor({"chat_delta_tokens": {"chat-1": "tok"}}) is True


def test_user_chats_skip_baseline_recognised():
    assert has_cursor({"chat_skip_baseline_at": "2026-05-15T00:00:00"}) is True


def test_onedrive_delta_link_recognised():
    assert has_cursor({"onedrive_delta_link": "https://graph..."}) is True


def test_sharepoint_per_site_map_recognised():
    assert has_cursor({"drive_delta_tokens_by_site": {"site-1": "tok"}}) is True


def test_unknown_key_returns_false():
    """A new incremental path that forgot to add its cursor key here
    is the regression we want to catch. Test pins the closed set."""
    assert has_cursor({"some_future_cursor_key": "tok"}) is False


def test_falsy_value_does_not_count_as_present():
    """Empty string / empty dict / None values aren't a real cursor —
    `.get()` returns them but `any()` treats them as falsy. Pinned
    so a partially-initialised resource doesn't accidentally bypass
    the detector."""
    assert has_cursor({"mail_delta_token": ""}) is False
    assert has_cursor({"mail_delta_tokens_by_folder": {}}) is False
    assert has_cursor({"chat_delta_tokens": None}) is False


def test_all_recognised_keys_are_in_constant():
    """Forward compatibility: every key tested above must remain in
    the published constant. If someone removes one, this fails loudly
    instead of silently re-introducing the 2026-05-15 false-positive."""
    expected = {
        "mail_delta_token",
        "mail_delta_tokens_by_folder",
        "mail_folder_fingerprints",
        "mail_folder_baseline_at",
        "chat_delta_tokens",
        "chat_skip_baseline_at",
        "onedrive_delta_link",
        "drive_delta_tokens_by_site",
    }
    assert expected.issubset(set(KEYS))
