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
