"""SLA-policy validation + WORM-Lock gate.

Pure helpers: no FastAPI app instance, no SQLAlchemy session, no logging
side effects on import. This is the shared module both the API layer
(resource-service) and the test suite import from. Keeping the gates
side-effect-free is the whole point — the previous setup couldn't be
unit-tested because importing resource-service/main.py spun up FastAPI.

Both functions raise `fastapi.HTTPException` with `status_code` so the
API layer can let them bubble; tests can catch them by class without
caring about the response model.
"""
from __future__ import annotations

from typing import Optional

from fastapi import HTTPException


# Hard maxima — set well above any sensible operator value so we catch
# garbage (negative, decimals fed as strings, accidental millisecond
# inputs) without surprising legitimate ones. 100y == ~36500 days.
_MAX_DAYS = 365 * 100
_VALID_RETENTION_MODES = {"FLAT", "GFS", "ITEM_LEVEL", "HYBRID"}
_VALID_ARCHIVED_MODES = {"SAME", "KEEP_ALL", "KEEP_LAST", "CUSTOM"}
_VALID_IMMUTABILITY = {"None", "Unlocked", "Locked"}
_VALID_ENCRYPTION = {"VAULT_MANAGED", "CUSTOMER_KEY"}


def gate_immutability_lock(
    request: dict,
    prior_mode: Optional[str] = None,
    current_name: Optional[str] = None,
) -> None:
    """Hard-block transitions to immutability_mode='Locked' unless the
    request includes confirmImmutabilityLock=True AND a typed name that
    matches the policy's current name.

    Re-checking the typed name at the API boundary defends against the
    stale-confirmation case where the operator typed the name in the
    modal, then changed the form's name field before clicking Save.

    Locked Azure container immutability policies CANNOT be reversed —
    at 250 TiB scale a misclick locks all six-figures-of-storage worth
    of backups for the full retention window. Two independent
    confirmations (typed name + flag) is the correct bar.

    Idempotent: already-Locked → Locked saves are allowed without
    re-confirmation (e.g. an unrelated frequency change on a Locked
    policy must not be blocked).
    """
    incoming_mode = (request.get("immutabilityMode") or
                     request.get("immutability_mode") or "").strip()
    if incoming_mode != "Locked":
        return
    if (prior_mode or "").strip() == "Locked":
        return  # already locked; idempotent re-save
    confirmed = bool(
        request.get("confirmImmutabilityLock")
        or request.get("confirm_immutability_lock")
    )
    if not confirmed:
        raise HTTPException(
            status_code=400,
            detail=(
                "Setting immutability_mode='Locked' is irreversible. "
                "Re-submit with confirmImmutabilityLock=true (the SLA "
                "wizard collects this via a typed-name confirmation)."
            ),
        )
    typed = (request.get("immutabilityLockTypedName")
             or request.get("immutability_lock_typed_name") or "").strip()
    expected = (current_name
                or request.get("name")
                or request.get("policyName")
                or "").strip()
    if not typed or not expected or typed != expected:
        raise HTTPException(
            status_code=400,
            detail=(
                "Locked-confirmation typed name does not match the policy's "
                "current name. If you renamed the policy after typing, "
                "please retype the new name in the confirmation dialog."
            ),
        )


def validate_policy_payload(request: dict) -> None:
    """Reject malformed SLA-policy payloads at the API boundary so they
    never land as a poison row in the DB.

    Rejected:
      - empty/whitespace name
      - retention values: negative, non-int, > 100 years
      - GFS mode with all four counts at 0 (silent indefinite retention)
      - archivedRetentionMode=CUSTOM without archivedRetentionDays
      - unknown enum values for retention/immutability/encryption modes
      - encryptionMode=CUSTOMER_KEY without keyVaultUri+keyName
      - keyVaultUri not HTTPS

    Raises HTTPException(422) with a precise field-level message.
    """
    def get_val(camel: str, snake: str, default=None):
        return request.get(camel, request.get(snake, default))

    name = (get_val("name", "name") or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="Policy name is required.")
    if len(name) > 200:
        raise HTTPException(status_code=422, detail="Policy name must be ≤200 chars.")

    day_fields = [
        ("retentionHotDays", "retention_hot_days"),
        ("retentionCoolDays", "retention_cool_days"),
        ("retentionArchiveDays", "retention_archive_days"),
        ("retentionDays", "retention_days"),
        ("itemRetentionDays", "item_retention_days"),
        ("archivedRetentionDays", "archived_retention_days"),
    ]
    for camel, snake in day_fields:
        v = get_val(camel, snake)
        if v is None:
            continue
        try:
            iv = int(v)
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail=f"{camel} must be an integer or null.")
        if iv < 0:
            raise HTTPException(status_code=422, detail=f"{camel} cannot be negative (got {iv}).")
        if iv > _MAX_DAYS:
            raise HTTPException(
                status_code=422,
                detail=f"{camel}={iv} exceeds the 100-year hard cap ({_MAX_DAYS} days).",
            )

    mode = (get_val("retentionMode", "retention_mode") or "FLAT").upper()
    if mode not in _VALID_RETENTION_MODES:
        raise HTTPException(status_code=422, detail=f"Unknown retentionMode '{mode}'.")
    if mode == "GFS":
        gfs_counts = [
            get_val("gfsDailyCount", "gfs_daily_count") or 0,
            get_val("gfsWeeklyCount", "gfs_weekly_count") or 0,
            get_val("gfsMonthlyCount", "gfs_monthly_count") or 0,
            get_val("gfsYearlyCount", "gfs_yearly_count") or 0,
        ]
        for c in gfs_counts:
            try:
                iv = int(c)
            except (TypeError, ValueError):
                raise HTTPException(status_code=422, detail="GFS counts must be integers.")
            if iv < 0:
                raise HTTPException(status_code=422, detail="GFS counts cannot be negative.")
        if sum(int(c) for c in gfs_counts) == 0:
            raise HTTPException(
                status_code=422,
                detail=(
                    "GFS mode requires at least one of daily/weekly/monthly/yearly "
                    "counts to be > 0. All-zero would silently keep only the most "
                    "recent snapshot — pick a different retentionMode if that's the intent."
                ),
            )

    hot = get_val("retentionHotDays", "retention_hot_days")
    cool = get_val("retentionCoolDays", "retention_cool_days")
    if hot is not None and cool is not None:
        try:
            if int(hot) > 0 and int(cool) > 0 and int(hot) > int(cool) * 100:
                raise HTTPException(
                    status_code=422,
                    detail=f"retentionHotDays ({hot}) is implausibly larger than "
                           f"retentionCoolDays ({cool}). Check the values.",
                )
        except (TypeError, ValueError):
            pass

    arch_mode = (get_val("archivedRetentionMode", "archived_retention_mode") or "SAME").upper()
    if arch_mode not in _VALID_ARCHIVED_MODES:
        raise HTTPException(status_code=422, detail=f"Unknown archivedRetentionMode '{arch_mode}'.")
    if arch_mode == "CUSTOM":
        days = get_val("archivedRetentionDays", "archived_retention_days")
        if days is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "archivedRetentionMode='CUSTOM' requires archivedRetentionDays "
                    "to be set (use KEEP_ALL for unlimited)."
                ),
            )

    immut = (get_val("immutabilityMode", "immutability_mode") or "None")
    if immut not in _VALID_IMMUTABILITY:
        raise HTTPException(status_code=422, detail=f"Unknown immutabilityMode '{immut}'.")
    enc = (get_val("encryptionMode", "encryption_mode") or "VAULT_MANAGED").upper()
    if enc not in _VALID_ENCRYPTION:
        raise HTTPException(status_code=422, detail=f"Unknown encryptionMode '{enc}'.")
    if enc == "CUSTOMER_KEY":
        kvu = (get_val("keyVaultUri", "key_vault_uri") or "").strip()
        kn = (get_val("keyName", "key_name") or "").strip()
        if not kvu or not kn:
            raise HTTPException(
                status_code=422,
                detail=(
                    "encryptionMode='CUSTOMER_KEY' requires keyVaultUri and keyName."
                ),
            )
        if not kvu.startswith("https://"):
            raise HTTPException(
                status_code=422,
                detail="keyVaultUri must be a full HTTPS URL (e.g. https://my-vault.vault.azure.net).",
            )
