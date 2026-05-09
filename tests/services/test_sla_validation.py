"""Unit tests for the SLA-policy validation gates.

Covers:
  - WORM-Lock typed-name re-validation (stale-confirmation defense)
  - Retention payload validation (negative, oversize, GFS-all-zero)
  - Archived-mode CUSTOM requires days
  - Encryption-mode enum + CMK requires keyVaultUri/keyName

Imports directly from `shared.sla_validation` — that module is
side-effect-free (no FastAPI app, no DB session) so these tests run
process-local without the full resource-service import dance.
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException

from shared.sla_validation import (
    gate_immutability_lock as _gate_lock,
    validate_policy_payload as _validate,
)


# ─── Validation: name + retention + GFS ──────────────────────────────────

def test_validate_rejects_empty_name():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "", "frequency": "DAILY"})
    assert exc_info.value.status_code == 422
    assert "name" in str(exc_info.value.detail).lower()


def test_validate_rejects_whitespace_only_name():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "   ", "frequency": "DAILY"})
    assert exc_info.value.status_code == 422


def test_validate_rejects_negative_retention():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "x", "retentionHotDays": -1})
    assert exc_info.value.status_code == 422
    assert "negative" in str(exc_info.value.detail)


def test_validate_rejects_oversize_retention():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "x", "retentionHotDays": 365 * 200})
    assert exc_info.value.status_code == 422
    assert "100-year" in str(exc_info.value.detail) or "exceeds" in str(exc_info.value.detail)


def test_validate_accepts_valid_flat():
    _validate({"name": "GoldX", "retentionMode": "FLAT",
               "retentionHotDays": 7, "retentionCoolDays": 30,
               "retentionArchiveDays": 365})


def test_validate_rejects_all_zero_gfs():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "x", "retentionMode": "GFS",
                   "gfsDailyCount": 0, "gfsWeeklyCount": 0,
                   "gfsMonthlyCount": 0, "gfsYearlyCount": 0})
    assert exc_info.value.status_code == 422
    assert "All-zero" in str(exc_info.value.detail) or "GFS" in str(exc_info.value.detail)


def test_validate_accepts_partial_gfs():
    _validate({"name": "GFSPolicy", "retentionMode": "GFS",
               "gfsDailyCount": 7, "gfsWeeklyCount": 0,
               "gfsMonthlyCount": 0, "gfsYearlyCount": 0})


def test_validate_rejects_unknown_retention_mode():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "x", "retentionMode": "BLOOPER"})
    assert exc_info.value.status_code == 422


def test_validate_rejects_archived_custom_without_days():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "x", "archivedRetentionMode": "CUSTOM"})
    assert exc_info.value.status_code == 422
    assert "archivedRetentionDays" in str(exc_info.value.detail)


def test_validate_accepts_archived_custom_with_days():
    _validate({"name": "x", "archivedRetentionMode": "CUSTOM",
               "archivedRetentionDays": 60})


def test_validate_rejects_byok_without_vault():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "x", "encryptionMode": "CUSTOMER_KEY"})
    assert exc_info.value.status_code == 422


def test_validate_rejects_byok_with_non_https_vault():
    with pytest.raises(HTTPException) as exc_info:
        _validate({"name": "x", "encryptionMode": "CUSTOMER_KEY",
                   "keyVaultUri": "http://not-https.example.com",
                   "keyName": "mykey"})
    assert exc_info.value.status_code == 422
    assert "HTTPS" in str(exc_info.value.detail)


def test_validate_accepts_byok_full():
    _validate({"name": "x", "encryptionMode": "CUSTOMER_KEY",
               "keyVaultUri": "https://my-vault.vault.azure.net",
               "keyName": "policy-key"})


# ─── WORM-Lock gate ──────────────────────────────────────────────────────

def test_gate_passthrough_when_not_locked():
    _gate_lock({"immutabilityMode": "Unlocked"}, prior_mode="None", current_name="x")
    _gate_lock({"immutabilityMode": "None"}, prior_mode="None", current_name="x")
    _gate_lock({}, prior_mode="None", current_name="x")


def test_gate_idempotent_when_already_locked():
    # Re-saving an already-Locked policy must NOT require re-confirm.
    _gate_lock({"immutabilityMode": "Locked"}, prior_mode="Locked", current_name="GoldZ")


def test_gate_refuses_lock_without_confirm():
    with pytest.raises(HTTPException) as exc_info:
        _gate_lock({"immutabilityMode": "Locked", "name": "Policy A"},
                   prior_mode="None", current_name="Policy A")
    assert exc_info.value.status_code == 400
    assert "irreversible" in str(exc_info.value.detail).lower()


def test_gate_refuses_lock_when_typed_name_missing():
    with pytest.raises(HTTPException) as exc_info:
        _gate_lock({"immutabilityMode": "Locked",
                    "confirmImmutabilityLock": True,
                    "name": "Policy A"},
                   prior_mode="None", current_name="Policy A")
    assert exc_info.value.status_code == 400
    assert "typed name" in str(exc_info.value.detail).lower()


def test_gate_refuses_lock_when_typed_name_mismatch():
    # Operator typed "Old Name" in the modal, then renamed in the form.
    with pytest.raises(HTTPException) as exc_info:
        _gate_lock(
            {
                "immutabilityMode": "Locked",
                "confirmImmutabilityLock": True,
                "immutabilityLockTypedName": "Old Name",
                "name": "New Name",
            },
            prior_mode="None",
            current_name="New Name",
        )
    assert exc_info.value.status_code == 400
    assert "match" in str(exc_info.value.detail).lower()


def test_gate_passes_with_full_confirmation():
    _gate_lock(
        {
            "immutabilityMode": "Locked",
            "confirmImmutabilityLock": True,
            "immutabilityLockTypedName": "Production-Compliance",
            "name": "Production-Compliance",
        },
        prior_mode="None",
        current_name="Production-Compliance",
    )
