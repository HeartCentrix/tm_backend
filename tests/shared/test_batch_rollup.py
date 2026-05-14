"""Unit tests for the Activity-Manager batch-rollup state machine.

Pure-function tests — no DB. Each test pins one branch of the design's
§7 state machine so an accidental refactor that breaks the contract
fails loudly.

Imports from ``shared.batch_rollup`` — the same import pattern every
other shared helper uses.
"""
from __future__ import annotations

from shared.batch_rollup import (
    derive_batch_status,
    RollupCounts,
    build_batch_rollup_query,
)


def _r(**kw):
    """Helper: build a RollupCounts with sane defaults."""
    defaults = dict(
        all_jobs_terminal=True, any_cancelled=False, any_job_failed=False,
        snap_total=0, snap_done=0, snap_partial=0, snap_failed=0,
        snap_pending=0, parts_pending=0, missing_t2=0,
    )
    defaults.update(kw)
    return RollupCounts(**defaults)


# ─── state machine: In Progress branches ───────────────────────────────

def test_pending_jobs_means_in_progress():
    r = _r(all_jobs_terminal=False)
    assert derive_batch_status(r) == ("In Progress", None)


def test_pending_snapshots_means_in_progress():
    r = _r(snap_pending=1)
    assert derive_batch_status(r) == ("In Progress", None)


def test_pending_partitions_means_in_progress():
    r = _r(parts_pending=3)
    assert derive_batch_status(r) == ("In Progress", None)


def test_fanout_incomplete_means_in_progress_even_when_all_terminal():
    # The exact bug we're fixing: Tier-1 done, Tier-2 not yet spawned.
    r = _r(all_jobs_terminal=True, snap_done=9, missing_t2=45)
    assert derive_batch_status(r) == ("In Progress", None)


def test_no_children_yet_means_in_progress():
    # Click landed; Jobs created but not yet started.
    r = _r(all_jobs_terminal=False, snap_total=0)
    assert derive_batch_status(r) == ("In Progress", None)


def test_in_flight_beats_failed():
    # 4 jobs done, 1 failed, 1 still running — must show In Progress.
    r = _r(all_jobs_terminal=False, snap_done=4, snap_failed=1, snap_pending=1)
    assert derive_batch_status(r) == ("In Progress", None)


# ─── state machine: terminal branches ──────────────────────────────────

def test_all_cancelled_means_canceled():
    r = _r(any_cancelled=True, snap_total=0)
    assert derive_batch_status(r) == ("Canceled", None)


def test_cancelled_but_partial_success_is_done_with_warnings():
    # Operator cancelled mid-run; 3 snapshots completed before cancel landed.
    r = _r(any_cancelled=True, snap_done=3, snap_total=10)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings is not None
    assert warnings["failed"] == 0


def test_all_failed_means_failed():
    r = _r(snap_failed=5, snap_total=5)
    assert derive_batch_status(r) == ("Failed", None)


def test_clean_done():
    r = _r(snap_done=10, snap_total=10)
    assert derive_batch_status(r) == ("Done", None)


def test_partial_and_done_means_done_with_warnings():
    r = _r(snap_done=7, snap_partial=2, snap_failed=1, snap_total=10)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings == {"partial": 2, "failed": 1}


def test_partial_only_no_clean_done_still_done_with_warnings():
    # Mailbox folder 403s — all PARTIAL, no FAILED, no clean COMPLETED.
    r = _r(snap_partial=5, snap_total=5)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings == {"partial": 5, "failed": 0}


# ─── SQL builder smoke ─────────────────────────────────────────────────

def test_build_batch_rollup_query_returns_a_sql_object_with_named_params():
    """Builder must produce a parameterised statement containing every
    CTE the design promised. We don't execute it against a real DB
    here — that's the integration test's job. We assert structure so
    the handler can rely on the contract."""
    stmt = build_batch_rollup_query(
        tenant_id="00000000-0000-0000-0000-000000000000",
        start_date=None,
        end_date=None,
        operation=None,
        size=50,
        offset=0,
    )
    sql = str(stmt)
    for cte in (
        "filtered_jobs", "batches", "snap_roll",
        "parts_roll", "fanout",
    ):
        assert cte in sql, f"missing CTE: {cte}"
    # The status / warnings derivation is in Python, but the SQL must
    # surface the column inputs.
    for col in ("snap_pending", "parts_pending", "missing_t2"):
        assert col in sql, f"missing column: {col}"
