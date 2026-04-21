"""Shared retention helpers.

Translates SLA-policy retention settings into the two values every backend
needs when applying WORM locks:

  - retention_until : absolute datetime (Azure expiry_time / S3 RetainUntilDate)
  - mode            : 'Locked' (Azure) / 'COMPLIANCE' (S3) — immutable
                      'Unlocked' (Azure) / 'GOVERNANCE' (S3) — extendable
"""
from __future__ import annotations

from datetime import datetime, timedelta


def compute_retention_until(sla_policy, created_at: datetime) -> datetime:
    """Absolute datetime the lock should expire.

    Reads sla_policy.retention_days (integer). Defaults to 30 days if
    unset, so callers without an explicit policy still get ransomware
    protection.
    """
    days = getattr(sla_policy, "retention_days", None) or 30
    return created_at + timedelta(days=int(days))


def compute_immutability_mode(sla_policy) -> str:
    """Return 'Locked' | 'Unlocked'.

    SLA sets immutability_mode = 'Locked' or 'Unlocked' — 'Locked' means
    even a compromised root admin cannot shorten the retention window
    (maps to S3 COMPLIANCE). Anything else (including None / 'Unlocked')
    allows manual extension via GOVERNANCE bypass on S3 or Unlocked
    policy on Azure.
    """
    mode = getattr(sla_policy, "immutability_mode", None)
    if mode and str(mode).lower() == "locked":
        return "Locked"
    return "Unlocked"
