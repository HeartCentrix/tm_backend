"""Batch-pending state machine + scope classification helpers.

Single source of truth for:
  - the state-string constants used by batch_pending_users.state
  - the pure-Python decision that splits a batch's scope into
    `ready` users (have Tier-2 children — back up immediately) vs
    `deferred` users (no Tier-2 children — wait for discovery to
    complete and chain backup off it)

Importing this module is free (no DB / no IO). The DB I/O lives in
services that call this; unit tests exercise the pure logic.

See docs/superpowers/specs/2026-05-15-backup-batch-race-fix-design.md
for the incident this design closes.
"""
from __future__ import annotations

import uuid
from typing import Iterable, List, Set, Tuple


class BatchPendingState:
    WAITING_DISCOVERY = "WAITING_DISCOVERY"
    BACKUP_ENQUEUED = "BACKUP_ENQUEUED"
    NO_CONTENT = "NO_CONTENT"
    DISCOVERY_FAILED = "DISCOVERY_FAILED"

    _TERMINAL = frozenset({BACKUP_ENQUEUED, NO_CONTENT, DISCOVERY_FAILED})

    @classmethod
    def is_terminal(cls, state: str) -> bool:
        """A terminal state means the finalizer can stop waiting on
        this user. BACKUP_ENQUEUED is terminal for pending-row
        purposes — the backup's own snapshot terminalisation is what
        the finalizer's gate 2 checks downstream."""
        return state in cls._TERMINAL


def classify_scope(
    scope: Iterable[uuid.UUID],
    tier2_owners: Set[uuid.UUID],
) -> Tuple[List[uuid.UUID], List[uuid.UUID]]:
    """Split a batch's scoped user IDs into (ready, deferred).

    `tier2_owners` is the set of user IDs that already have at least
    one non-archived Tier-2 child resource in `resources`. The caller
    computes that set with one DB query before invoking this pure function.

    Ready users → enqueue per-user backup Jobs immediately.
    Deferred users → insert `batch_pending_users` rows in
    WAITING_DISCOVERY state + publish chained discovery.tier2.

    Dedup: a duplicate ID in `scope` returns once. The DB layer also
    has ON CONFLICT DO NOTHING, but deduping at this layer keeps
    publish payloads small and the ready-enqueue clean.
    """
    seen: Set[uuid.UUID] = set()
    ready: List[uuid.UUID] = []
    deferred: List[uuid.UUID] = []
    for uid in scope:
        if uid in seen:
            continue
        seen.add(uid)
        if uid in tier2_owners:
            ready.append(uid)
        else:
            deferred.append(uid)
    return ready, deferred
