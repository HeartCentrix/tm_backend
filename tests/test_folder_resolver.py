"""Unit tests for the folder-scope resolver.

The SQL path is exercised via a mocked AsyncSession to keep these tests
independent of Postgres. The pure helpers are tested directly.
An ``INTEGRATION=1`` run will additionally exercise the live SQL via
``tests/integration/test_folder_resolver_integration.py`` (out of scope
for this unit file).
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.folder_resolver import (
    _prefix_and_exact_for,
    _to_uuids,
    resolve_selection,
)


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------

def test_prefix_and_exact_for_strips_trailing_slash():
    prefixes, exacts = _prefix_and_exact_for(["/Foo/", "/Bar"])
    assert prefixes == ["/Foo/%", "/Bar/%"]
    assert exacts == ["/Foo", "/Bar"]


def test_prefix_and_exact_for_sibling_boundary():
    """`/Foo` must NOT match a sibling `/Foo Bar` — the prefix carries a
    trailing slash so `LIKE '/Foo/%'` excludes `/Foo Bar/x`."""
    prefixes, _ = _prefix_and_exact_for(["/Foo"])
    assert prefixes == ["/Foo/%"]
    # Sanity: a pattern of "/Foo/%" does not match "/Foo Bar/x" in SQL
    # LIKE semantics — the resolver relies on this.


def test_prefix_and_exact_for_root():
    prefixes, exacts = _prefix_and_exact_for(["/"])
    assert prefixes == ["/%"]
    assert exacts == ["/"]


def test_prefix_and_exact_for_drops_empty():
    prefixes, exacts = _prefix_and_exact_for(["", None, "  ", "/Docs"])
    assert prefixes == ["/Docs/%"]
    assert exacts == ["/Docs"]


def test_prefix_and_exact_for_unicode():
    prefixes, exacts = _prefix_and_exact_for(["/Reports/Q4-报告"])
    assert prefixes == ["/Reports/Q4-报告/%"]
    assert exacts == ["/Reports/Q4-报告"]


def test_prefix_and_exact_for_empty_input():
    assert _prefix_and_exact_for([]) == ([], [])
    assert _prefix_and_exact_for(None) == ([], [])


def test_to_uuids_drops_malformed():
    good = uuid.uuid4()
    result = _to_uuids([str(good), "not-a-uuid", "", None, 42])
    assert result == [good]


# --------------------------------------------------------------------------
# resolve_selection() — with a mocked session
# --------------------------------------------------------------------------

def _mock_session_returning(items):
    session = MagicMock()
    scalars = MagicMock()
    scalars.all.return_value = items
    result = MagicMock()
    result.scalars.return_value = scalars
    session.execute = AsyncMock(return_value=result)
    return session


class _FakeItem:
    """Lightweight stand-in for SnapshotItem that only has the fields the
    resolver filters on. Lets us avoid booting SQLAlchemy mappers."""
    def __init__(self, iid, folder_path="/", name=""):
        self.id = iid
        self.folder_path = folder_path
        self.name = name


@pytest.mark.asyncio
async def test_empty_inputs_return_empty_list_without_querying():
    session = _mock_session_returning([])
    snap = str(uuid.uuid4())
    result = await resolve_selection(
        session,
        snapshot_id=snap,
        item_ids=[],
        folder_paths=[],
        excluded_item_ids=[],
    )
    assert result == []
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_item_ids_only_triggers_one_query():
    a, b = _FakeItem(uuid.uuid4()), _FakeItem(uuid.uuid4())
    session = _mock_session_returning([a, b])
    result = await resolve_selection(
        session,
        snapshot_id=str(uuid.uuid4()),
        item_ids=[str(a.id), str(b.id)],
        folder_paths=[],
        excluded_item_ids=[],
    )
    assert {r.id for r in result} == {a.id, b.id}
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_folder_paths_only_triggers_one_query():
    rows = [_FakeItem(uuid.uuid4(), folder_path="/Foo/nested"),
            _FakeItem(uuid.uuid4(), folder_path="/Foo")]
    session = _mock_session_returning(rows)
    result = await resolve_selection(
        session,
        snapshot_id=str(uuid.uuid4()),
        item_ids=[],
        folder_paths=["/Foo"],
        excluded_item_ids=[],
    )
    assert len(result) == 2
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_excluded_item_ids_subtract_after_fetch():
    keep = _FakeItem(uuid.uuid4(), folder_path="/Foo/keep.txt")
    drop = _FakeItem(uuid.uuid4(), folder_path="/Foo/drop.txt")
    session = _mock_session_returning([keep, drop])
    result = await resolve_selection(
        session,
        snapshot_id=str(uuid.uuid4()),
        item_ids=[],
        folder_paths=["/Foo"],
        excluded_item_ids=[str(drop.id)],
    )
    assert {r.id for r in result} == {keep.id}


@pytest.mark.asyncio
async def test_union_item_ids_and_folder_paths():
    """A mixed selection (some ticked files + some ticked folders) goes
    through one query and the OR-combined clause."""
    file_pick = _FakeItem(uuid.uuid4(), folder_path="/Random/loose.txt")
    folder_hit = _FakeItem(uuid.uuid4(), folder_path="/Foo/nested.txt")
    session = _mock_session_returning([file_pick, folder_hit])
    result = await resolve_selection(
        session,
        snapshot_id=str(uuid.uuid4()),
        item_ids=[str(file_pick.id)],
        folder_paths=["/Foo"],
        excluded_item_ids=[],
    )
    assert {r.id for r in result} == {file_pick.id, folder_hit.id}
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_malformed_item_ids_dont_cause_error():
    session = _mock_session_returning([])
    result = await resolve_selection(
        session,
        snapshot_id=str(uuid.uuid4()),
        item_ids=["not-a-uuid"],
        folder_paths=[],
        excluded_item_ids=[],
    )
    assert result == []
