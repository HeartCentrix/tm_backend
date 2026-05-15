"""Unit tests for the batch-creation scope classifier.

Pins the contract that decides which scoped users get an immediate
per-user backup and which get a `batch_pending_users` row with chained
discovery. Pure: given a scope list and a set of users known to have
Tier-2 children, the classifier splits deterministically.
"""
from __future__ import annotations

import uuid
from typing import List

import pytest


from shared.batch_pending import classify_scope, BatchPendingState


def _ids(n: int) -> List[uuid.UUID]:
    return [uuid.uuid4() for _ in range(n)]


def test_empty_scope_returns_empty():
    ready, deferred = classify_scope(scope=[], tier2_owners=set())
    assert ready == []
    assert deferred == []


def test_all_users_have_tier2_all_ready():
    ids = _ids(3)
    ready, deferred = classify_scope(scope=ids, tier2_owners=set(ids))
    assert sorted(ready, key=str) == sorted(ids, key=str)
    assert deferred == []


def test_no_users_have_tier2_all_deferred():
    ids = _ids(4)
    ready, deferred = classify_scope(scope=ids, tier2_owners=set())
    assert ready == []
    assert sorted(deferred, key=str) == sorted(ids, key=str)


def test_mixed_scope_splits_correctly():
    ids = _ids(5)
    tier2 = {ids[0], ids[2]}
    ready, deferred = classify_scope(scope=ids, tier2_owners=tier2)
    assert set(ready) == {ids[0], ids[2]}
    assert set(deferred) == {ids[1], ids[3], ids[4]}


def test_duplicate_scope_ids_dedup():
    """An operator clicking the same user twice (rare but possible via
    multi-select bugs) must not double-spawn pending rows."""
    a, b = uuid.uuid4(), uuid.uuid4()
    ready, deferred = classify_scope(scope=[a, b, a], tier2_owners={a})
    assert ready == [a]
    assert deferred == [b]


def test_states_enum_values():
    """Constants must match the SQL state strings used by the DB writer
    and the finalizer's WHERE clause. A typo here silently breaks the
    finalizer gate."""
    assert BatchPendingState.WAITING_DISCOVERY == "WAITING_DISCOVERY"
    assert BatchPendingState.BACKUP_ENQUEUED == "BACKUP_ENQUEUED"
    assert BatchPendingState.NO_CONTENT == "NO_CONTENT"
    assert BatchPendingState.DISCOVERY_FAILED == "DISCOVERY_FAILED"
    assert BatchPendingState.is_terminal("NO_CONTENT") is True
    assert BatchPendingState.is_terminal("DISCOVERY_FAILED") is True
    assert BatchPendingState.is_terminal("BACKUP_ENQUEUED") is True
    assert BatchPendingState.is_terminal("WAITING_DISCOVERY") is False
