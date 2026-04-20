"""Single-query resolver for file selections that mix individually-picked
items and folder-scope ticks.

Frontend sends:
  * ``item_ids``            — files individually ticked
  * ``folder_paths``        — folders ticked (every descendant is in scope)
  * ``excluded_item_ids``   — files un-ticked inside a ticked folder

This module expands those into the concrete ``SnapshotItem`` rows in one
indexed SQL round-trip. Used by the restore-worker and the job-service
size soft-cap check.

The resolver is strict: empty inputs return an empty list. There is no
"everything in snapshot" shortcut — callers must be explicit.
"""
from __future__ import annotations

import uuid
from typing import Iterable, List, Tuple

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import SnapshotItem


def _to_uuids(values: Iterable[str]) -> List[uuid.UUID]:
    """Best-effort conversion. Malformed ids are dropped silently — the
    resolver is a filter, not a validator. Invalid ids simply produce no
    match rather than raising."""
    out: List[uuid.UUID] = []
    for v in values or []:
        try:
            out.append(uuid.UUID(v))
        except (ValueError, TypeError, AttributeError):
            continue
    return out


def _prefix_and_exact_for(folder_paths: Iterable[str]) -> Tuple[List[str], List[str]]:
    """Normalise folder paths into the two SQL forms the resolver needs:

    * ``prefixes``: ``<path>/%`` — descendant LIKE match. Trailing slash
      is stripped from ``<path>`` first so ``/Foo`` does NOT accidentally
      match a sibling ``/Foo Bar``.
    * ``exacts``:   the folder path itself. Catches items whose
      ``folder_path`` column equals the ticked path (files directly at
      the folder's top level rather than in a subfolder).

    Empty / whitespace-only inputs are dropped.
    """
    prefixes: List[str] = []
    exacts: List[str] = []
    for raw in folder_paths or []:
        if not raw or not str(raw).strip():
            continue
        stripped = str(raw).rstrip("/")
        if not stripped:
            # The root "/" case — descendants match everything, exact
            # match is the literal "/".
            prefixes.append("/%")
            exacts.append("/")
            continue
        prefixes.append(stripped + "/%")
        exacts.append(stripped)
    return prefixes, exacts


async def resolve_selection(
    session: AsyncSession,
    *,
    snapshot_id: str,
    item_ids: List[str],
    folder_paths: List[str],
    excluded_item_ids: List[str],
) -> List[SnapshotItem]:
    """Return every SnapshotItem in ``snapshot_id`` that matches any of
    the three inclusion clauses, minus anything in ``excluded_item_ids``:

    1. ``id`` listed in ``item_ids``, OR
    2. ``folder_path LIKE <prefix>`` for any ``prefix`` in
       ``_prefix_and_exact_for(folder_paths)``, OR
    3. ``folder_path`` equal to one of the exact folder paths.

    A single ``SELECT`` with ``OR``-combined clauses so the DB engine can
    plan one index scan per clause rather than forcing the app to union
    multiple round-trips.
    """
    if not item_ids and not folder_paths:
        return []

    item_uuids = _to_uuids(item_ids)
    excluded_uuids = set(_to_uuids(excluded_item_ids))
    prefixes, exacts = _prefix_and_exact_for(folder_paths)

    clauses = []
    if item_uuids:
        clauses.append(SnapshotItem.id.in_(item_uuids))
    for prefix in prefixes:
        clauses.append(SnapshotItem.folder_path.like(prefix))
    if exacts:
        clauses.append(SnapshotItem.folder_path.in_(exacts))

    if not clauses:
        return []

    snap_uuid = uuid.UUID(snapshot_id)
    stmt = select(SnapshotItem).where(
        SnapshotItem.snapshot_id == snap_uuid,
        or_(*clauses),
    )
    rows = (await session.execute(stmt)).scalars().all()
    if excluded_uuids:
        rows = [r for r in rows if r.id not in excluded_uuids]
    return list(rows)
