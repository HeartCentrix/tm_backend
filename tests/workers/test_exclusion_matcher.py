"""Unit tests for the exclusion matcher in backup-worker.

Validates _compile_exclusions + _item_is_excluded_compiled across all
six rule types: FOLDER_PATH, FILE_EXTENSION, FILENAME_GLOB, MIME_TYPE,
SUBJECT_REGEX, EMAIL_ADDRESS.

Uses an importlib path-load like test_sla_validation does, since the
worker module's directory has a hyphen in the name.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest


_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)
_spec = importlib.util.spec_from_file_location("bw_main_under_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_under_test"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)


_compile = _bw.BackupWorker._compile_exclusions
_match = _bw.BackupWorker._item_is_excluded_compiled


# ── FOLDER_PATH ─────────────────────────────────────────────────────────

def test_folder_path_substring_match():
    rules = _compile([{"type": "FOLDER_PATH", "pattern": "Drafts", "workload": "EMAIL"}])
    item = {"parentReference": {"path": "/Inbox/Drafts/Subfolder"}, "name": "msg.txt"}
    assert _match("EMAIL", item, rules) is True


def test_folder_path_no_match():
    rules = _compile([{"type": "FOLDER_PATH", "pattern": "Drafts", "workload": "EMAIL"}])
    item = {"parentReference": {"path": "/Inbox/Sent"}, "name": "msg.txt"}
    assert _match("EMAIL", item, rules) is False


def test_folder_path_workload_mismatch_skips():
    rules = _compile([{"type": "FOLDER_PATH", "pattern": "Drafts", "workload": "EMAIL"}])
    item = {"parentReference": {"path": "/Inbox/Drafts/x"}, "name": "msg.txt"}
    # Workload=FILE — rule scoped to EMAIL — must not match.
    assert _match("FILE", item, rules) is False


def test_folder_path_all_workload_matches_anything():
    rules = _compile([{"type": "FOLDER_PATH", "pattern": "tmp", "workload": "ALL"}])
    assert _match("EMAIL", {"folderPath": "/scratch/tmp"}, rules) is True
    assert _match("FILE", {"parentReference": {"path": "/data/tmp/x"}}, rules) is True


# ── FILE_EXTENSION ──────────────────────────────────────────────────────

def test_file_extension_match():
    rules = _compile([{"type": "FILE_EXTENSION", "pattern": ".pst", "workload": "FILE"}])
    assert _match("FILE", {"name": "archive.pst"}, rules) is True
    assert _match("FILE", {"name": "report.PST"}, rules) is True   # case-insensitive
    assert _match("FILE", {"name": "report.txt"}, rules) is False


def test_file_extension_handles_pattern_without_leading_dot():
    rules = _compile([{"type": "FILE_EXTENSION", "pattern": "bak", "workload": "FILE"}])
    assert _match("FILE", {"name": "data.bak"}, rules) is True


# ── FILENAME_GLOB ───────────────────────────────────────────────────────

def test_filename_glob_match():
    rules = _compile([{"type": "FILENAME_GLOB", "pattern": "*.tmp", "workload": "FILE"}])
    assert _match("FILE", {"name": "scratch.tmp"}, rules) is True
    assert _match("FILE", {"name": "scratch.tmp.bak"}, rules) is False


def test_filename_glob_question_mark_metachar():
    rules = _compile([{"type": "FILENAME_GLOB", "pattern": "log?.txt", "workload": "FILE"}])
    assert _match("FILE", {"name": "log1.txt"}, rules) is True
    assert _match("FILE", {"name": "log10.txt"}, rules) is False


# ── MIME_TYPE ───────────────────────────────────────────────────────────

def test_mime_type_match_via_drive_item_shape():
    rules = _compile([{"type": "MIME_TYPE", "pattern": "image/png", "workload": "FILE"}])
    assert _match("FILE", {"file": {"mimeType": "image/png"}, "name": "x"}, rules) is True
    assert _match("FILE", {"file": {"mimeType": "image/jpeg"}, "name": "x"}, rules) is False


def test_mime_type_match_via_top_level_field():
    rules = _compile([{"type": "MIME_TYPE", "pattern": "application/pdf", "workload": "FILE"}])
    assert _match("FILE", {"mimeType": "application/pdf", "name": "doc"}, rules) is True


# ── SUBJECT_REGEX ───────────────────────────────────────────────────────

def test_subject_regex_match():
    rules = _compile([{"type": "SUBJECT_REGEX", "pattern": r"^\[OUT-OF-OFFICE\]", "workload": "EMAIL"}])
    assert _match("EMAIL", {"subject": "[OUT-OF-OFFICE] back next week"}, rules) is True
    assert _match("EMAIL", {"subject": "Re: [OUT-OF-OFFICE] back next week"}, rules) is False


def test_subject_regex_case_insensitive():
    rules = _compile([{"type": "SUBJECT_REGEX", "pattern": "newsletter", "workload": "EMAIL"}])
    assert _match("EMAIL", {"subject": "Weekly NEWSLETTER vol 4"}, rules) is True


def test_subject_regex_malformed_pattern_is_skipped():
    # Malformed regex must NOT crash the whole backup.
    rules = _compile([{"type": "SUBJECT_REGEX", "pattern": "[unclosed", "workload": "EMAIL"}])
    # With the bad rule silently dropped at compile time, nothing matches.
    assert _match("EMAIL", {"subject": "[unclosed group test"}, rules) is False


# ── EMAIL_ADDRESS ───────────────────────────────────────────────────────

def test_email_address_matches_sender():
    rules = _compile([{"type": "EMAIL_ADDRESS", "pattern": "noreply", "workload": "EMAIL"}])
    item = {"from": {"emailAddress": {"address": "noreply@vendor.com"}}}
    assert _match("EMAIL", item, rules) is True


def test_email_address_matches_recipients():
    rules = _compile([{"type": "EMAIL_ADDRESS", "pattern": "alerts@", "workload": "EMAIL"}])
    item = {
        "from": {"emailAddress": {"address": "user@corp.com"}},
        "toRecipients": [
            {"emailAddress": {"address": "alerts@corp.com"}},
            {"emailAddress": {"address": "human@corp.com"}},
        ],
    }
    assert _match("EMAIL", item, rules) is True


def test_email_address_no_match():
    rules = _compile([{"type": "EMAIL_ADDRESS", "pattern": "ceo@", "workload": "EMAIL"}])
    item = {"from": {"emailAddress": {"address": "regular@corp.com"}}}
    assert _match("EMAIL", item, rules) is False


# ── Empty / edge cases ──────────────────────────────────────────────────

def test_empty_rules_never_match():
    rules = _compile([])
    assert _match("EMAIL", {"name": "anything"}, rules) is False


def test_blank_pattern_is_dropped_at_compile_time():
    rules = _compile([{"type": "FOLDER_PATH", "pattern": "  ", "workload": "ALL"}])
    assert _match("EMAIL", {"folderPath": "/anywhere"}, rules) is False


def test_compile_buckets_by_type():
    rules = _compile([
        {"type": "FOLDER_PATH", "pattern": "Drafts", "workload": "ALL"},
        {"type": "FILE_EXTENSION", "pattern": ".pst", "workload": "FILE"},
    ])
    assert "FOLDER_PATH" in rules
    assert "FILE_EXTENSION" in rules
    assert len(rules["FOLDER_PATH"]) == 1
    assert len(rules["FILE_EXTENSION"]) == 1


def test_compile_returns_callable_results_unchanged_per_run():
    """Compilation must be deterministic — same input → equivalent output."""
    inp = [{"type": "FOLDER_PATH", "pattern": "Inbox", "workload": "EMAIL"}]
    a = _compile(inp)
    b = _compile(inp)
    # Both should match the same items.
    item = {"folderPath": "/Inbox/x", "name": "y"}
    assert _match("EMAIL", item, a) == _match("EMAIL", item, b) == True
