"""Decision-logic tests for the relaxed finalizer gate 1.

The full _finalize_batch_if_complete needs a DB. Here we test the
*decision predicate* as a pure function — same shape as
`compute_mail_skip_by_fp` in the prior mail-fp fix. The finalizer's
SQL is exercised end-to-end in the Railway smoke test in the spec.

The function `_decide_gate1` is a pure Python mirror of the gate-1
SQL in shared/batch_rollup.py. Keep them in sync.
"""
from __future__ import annotations

import uuid

import pytest

from shared.batch_pending import BatchPendingState


def _decide_gate1(scope, tier2_owners, pending_states):
    """Pure mirror of the finalizer's gate-1 SQL.

    Block list = users that have NO Tier-2 children AND whose pending
    row is missing OR still WAITING_DISCOVERY. Anything else (terminal
    pending state, or has Tier-2 children regardless of pending row)
    is acceptable.

    `pending_states`: dict[user_id, state] — omit a user to simulate
    the pending row not existing.
    """
    blocked = []
    for uid in scope:
        has_t2 = uid in tier2_owners
        state = pending_states.get(uid)
        if has_t2:
            continue
        if state is None or state == BatchPendingState.WAITING_DISCOVERY:
            blocked.append(uid)
    return blocked


@pytest.fixture
def ids():
    return [uuid.uuid4() for _ in range(4)]


def test_all_users_have_tier2_no_block(ids):
    blocked = _decide_gate1(ids, set(ids), {})
    assert blocked == []


def test_user_no_tier2_no_pending_row_blocks(ids):
    """Pre-fix in-flight batches: no pending rows exist. Must still
    block on users without Tier-2 children (backward-compat)."""
    blocked = _decide_gate1(ids, set(), {})
    assert sorted(blocked, key=str) == sorted(ids, key=str)


def test_user_no_tier2_with_waiting_pending_blocks(ids):
    blocked = _decide_gate1(
        ids, set(),
        {u: BatchPendingState.WAITING_DISCOVERY for u in ids},
    )
    assert sorted(blocked, key=str) == sorted(ids, key=str)


def test_user_no_tier2_with_no_content_passes(ids):
    blocked = _decide_gate1(
        ids, set(),
        {u: BatchPendingState.NO_CONTENT for u in ids},
    )
    assert blocked == []


def test_user_no_tier2_with_discovery_failed_passes(ids):
    blocked = _decide_gate1(
        ids, set(),
        {u: BatchPendingState.DISCOVERY_FAILED for u in ids},
    )
    assert blocked == []


def test_user_no_tier2_with_backup_enqueued_passes(ids):
    """Once discovery has chained backup, gate 1 stops blocking on
    this user. Gate 2 takes over and waits for the resulting snapshots."""
    blocked = _decide_gate1(
        ids, set(),
        {u: BatchPendingState.BACKUP_ENQUEUED for u in ids},
    )
    assert blocked == []


def test_mixed_scope_only_pre_terminal_blocks(ids):
    a, b, c, d = ids
    blocked = _decide_gate1(
        ids,
        tier2_owners={a},
        pending_states={
            b: BatchPendingState.NO_CONTENT,
            c: BatchPendingState.WAITING_DISCOVERY,  # blocks
            # d: no row at all → blocks
        },
    )
    assert set(blocked) == {c, d}


def test_regression_amit_batch_pattern():
    """The 2026-05-15 Railway incident shape: 54 users in scope,
    9 have Tier-2 children, 45 do not. Pre-fix the 45 blocked
    forever. Post-fix with NO_CONTENT for the 45 → gate passes."""
    scope = [uuid.uuid4() for _ in range(54)]
    tier2 = set(scope[:9])
    pending = {u: BatchPendingState.NO_CONTENT for u in scope[9:]}
    blocked = _decide_gate1(scope, tier2, pending)
    assert blocked == []
