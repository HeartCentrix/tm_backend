"""Tests for EntraRestoreEngine plan partitioning + outcome types."""
from types import SimpleNamespace

from workers.restore_worker.entra_restore import (
    EntraRestoreEngine,
    EntraOutcome,
    EntraPlan,
)


def _mk(item_type, external_id, raw=None, fingerprint="fp", bucket=None):
    ed = {"raw": raw or {"id": external_id}, "fingerprint": fingerprint}
    if bucket:
        # Which bucket key depends on item_type; both coexist safely.
        ed["_sec_bucket"] = bucket
        ed["_intune_bucket"] = bucket
    return SimpleNamespace(
        id=f"iid-{external_id}",
        item_type=item_type,
        external_id=external_id,
        extra_data=ed,
    )


def test_plan_partitions_by_item_type():
    items = [
        _mk("ENTRA_DIR_USER", "u1"),
        _mk("ENTRA_DIR_USER", "u2"),
        _mk("ENTRA_DIR_GROUP", "g1"),
        _mk("ENTRA_DIR_ADMIN_UNIT", "au1"),
    ]
    plan = EntraRestoreEngine.build_plan(items, sections=None)
    assert len(plan.sections["ENTRA_DIR_USER"]) == 2
    assert len(plan.sections["ENTRA_DIR_GROUP"]) == 1
    assert len(plan.sections["ENTRA_DIR_ADMIN_UNIT"]) == 1


def test_plan_filters_by_sections_when_given():
    """When the modal passes entraSections=['users'], only users are
    restored — group/admin-unit rows are dropped."""
    items = [
        _mk("ENTRA_DIR_USER", "u1"),
        _mk("ENTRA_DIR_GROUP", "g1"),
    ]
    plan = EntraRestoreEngine.build_plan(items, sections=["users"])
    assert "ENTRA_DIR_USER" in plan.sections
    assert "ENTRA_DIR_GROUP" not in plan.sections


def test_plan_drops_read_only_sections_even_when_selected():
    """Audit / risky users / alerts are download-only. Even if the
    caller passes them, the engine silently drops them."""
    items = [
        _mk("ENTRA_DIR_USER", "u1"),
        _mk("ENTRA_DIR_AUDIT", "a1"),
        _mk("ENTRA_DIR_SECURITY", "alert1", bucket="Alerts"),
        _mk("ENTRA_DIR_SECURITY", "cap1", bucket="Conditional Access"),
        _mk("ENTRA_DIR_INTUNE", "dev1", bucket="Devices"),
    ]
    plan = EntraRestoreEngine.build_plan(items, sections=None)
    assert "ENTRA_DIR_USER" in plan.sections
    assert "ENTRA_DIR_AUDIT" not in plan.sections
    sec = plan.sections.get("ENTRA_DIR_SECURITY", [])
    sec_ids = {i.external_id for i in sec}
    assert "cap1" in sec_ids
    assert "alert1" not in sec_ids
    intune = plan.sections.get("ENTRA_DIR_INTUNE", [])
    assert all(i.extra_data.get("_intune_bucket") != "Devices" for i in intune)


def test_classify_outcome_match_unchanged():
    from workers.restore_worker.entra_restore import classify_outcome
    snap = _mk("ENTRA_DIR_USER", "u1", fingerprint="FP1")
    assert classify_outcome(snap, exists_live=True, live_fingerprint="FP1") == "unchanged"


def test_classify_outcome_match_drift():
    from workers.restore_worker.entra_restore import classify_outcome
    snap = _mk("ENTRA_DIR_USER", "u1", fingerprint="FP1")
    assert classify_outcome(snap, exists_live=True, live_fingerprint="FP2") == "updated"


def test_classify_outcome_deleted_in_tenant():
    from workers.restore_worker.entra_restore import classify_outcome
    snap = _mk("ENTRA_DIR_USER", "u1", fingerprint="FP1")
    assert classify_outcome(snap, exists_live=False, live_fingerprint=None) == "created"


from unittest.mock import AsyncMock, MagicMock
import pytest

from workers.restore_worker.entra_restore import EntraRestoreEngine


@pytest.mark.asyncio
async def test_run_skips_unchanged_and_patches_drifted_and_creates_deleted():
    """Three-user plan: u1 unchanged (fingerprint match), u2 drifted
    (fingerprint differs), u3 missing (create)."""
    gc = MagicMock()
    from shared.entra_fingerprint import fingerprint_object

    snap_u1 = _mk("ENTRA_DIR_USER", "u1",
                  raw={"id": "u1", "displayName": "A", "jobTitle": "Eng", "userPrincipalName": "a@x"})
    snap_u2 = _mk("ENTRA_DIR_USER", "u2",
                  raw={"id": "u2", "displayName": "B", "jobTitle": "Eng", "userPrincipalName": "b@x"})
    snap_u3 = _mk("ENTRA_DIR_USER", "u3",
                  raw={"id": "u3", "displayName": "C", "userPrincipalName": "c@x"})

    for s in (snap_u1, snap_u2, snap_u3):
        s.extra_data["fingerprint"] = fingerprint_object("ENTRA_DIR_USER", s.extra_data["raw"])

    # Build the live-object body for u1 identical to snapshot, for u2
    # with a different jobTitle to force drift.
    live_u1 = {"id": "u1", "displayName": "A", "jobTitle": "Eng", "userPrincipalName": "a@x"}
    live_u2 = {"id": "u2", "displayName": "B", "jobTitle": "DIFF", "userPrincipalName": "b@x"}

    gc._post = AsyncMock(side_effect=[
        # sieve_existence batch (existence check)
        {"responses": [
            {"id": "exist-0", "status": 200, "body": {"id": "u1"}},
            {"id": "exist-1", "status": 200, "body": {"id": "u2"}},
            {"id": "exist-2", "status": 404, "body": {}},
        ]},
        # live fingerprint fetch batch (2 existing)
        {"responses": [
            {"id": "fp-0", "status": 200, "body": live_u1},
            {"id": "fp-1", "status": 200, "body": live_u2},
        ]},
        # create_user POST for u3
        {"id": "new-u3"},
    ])
    gc._patch = AsyncMock(return_value={})
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    target = SimpleNamespace(external_id="tenant-1", id="r1", tenant_id="t1")
    engine = EntraRestoreEngine(gc, target)
    summary = await engine.run([snap_u1, snap_u2, snap_u3])

    assert summary["unchanged"] == 1, summary
    assert summary["updated"] == 1, summary
    assert summary["created"] == 1, summary
    assert summary["failed"] == 0, summary
