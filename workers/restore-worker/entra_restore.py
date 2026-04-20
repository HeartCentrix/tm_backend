"""EntraRestoreEngine — AFI-parity restore pipeline for the 8 Entra
Directory sections. Mirrors the MailRestoreEngine architecture.

Five phases:
    plan             filter items by item_type + read-only buckets,
                     honour UI section allowlist
    sieve            live-fetch existence + fingerprints via $batch
    diff             classify each object: unchanged | updated | created
    dispatch         PATCH / POST per section in dependency order
    rebind           group + admin-unit membership reconcile
    report           per-section counters + per-item results

See docs/superpowers/specs/2026-04-20-entra-download-and-restore-design.md
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from shared.config import settings
from shared.graph_client import GraphClient
from shared._graph_retry import _is_retryable, _retry_after_seconds
from shared.graph_entra import SECTION_SPECS, SectionSpec


# Map UI section labels → the item_type those rows were backed up as.
# The download allowlist is wider than restore; here we only list
# restorable sections.
UI_SECTION_TO_ITEM_TYPE: Dict[str, str] = {
    "users": "ENTRA_DIR_USER",
    "groups": "ENTRA_DIR_GROUP",
    "roles": "ENTRA_DIR_ROLE",
    "applications": "ENTRA_DIR_APPLICATION",
    "security": "ENTRA_DIR_SECURITY",
    "adminunits": "ENTRA_DIR_ADMIN_UNIT",
    "intune": "ENTRA_DIR_INTUNE",
}

# Buckets within multi-bucket sections that are read-only (download
# only). Dropped from the plan even if selected.
_READ_ONLY_BUCKETS: Dict[str, set] = {
    "ENTRA_DIR_SECURITY": {"Alerts", "Risky Users"},
    "ENTRA_DIR_INTUNE": {"Devices"},
}

# Section restore order — earlier sections' objects may be referenced
# by later sections (role assignments reference users/groups, etc.).
SECTION_ORDER: Tuple[str, ...] = (
    "ENTRA_DIR_ROLE",          # custom role definitions first
    "ENTRA_DIR_USER",
    "ENTRA_DIR_GROUP",
    "ENTRA_DIR_ADMIN_UNIT",
    "ENTRA_DIR_APPLICATION",
    "ENTRA_DIR_SECURITY",
    "ENTRA_DIR_INTUNE",
)


# ---- Plan types ----

@dataclass
class EntraPlan:
    """Phase-1 output: items bucketed by item_type."""
    sections: Dict[str, List[Any]] = field(default_factory=dict)


@dataclass
class EntraOutcome:
    item_id: str
    external_id: str
    section: str
    outcome: str             # "unchanged" | "updated" | "created" | "failed" | "skipped"
    graph_id: Optional[str] = None
    reason: Optional[str] = None


def classify_outcome(
    snap_item: Any,
    *,
    exists_live: bool,
    live_fingerprint: Optional[str],
) -> str:
    """Single-item outcome classifier. Pure function, unit-tested."""
    snap_fp = (getattr(snap_item, "extra_data", None) or {}).get("fingerprint")
    if not exists_live:
        return "created"
    if snap_fp and live_fingerprint and snap_fp == live_fingerprint:
        return "unchanged"
    return "updated"


# ---- Engine ----

class EntraRestoreEngine:
    """One instance per restore job + target tenant."""

    def __init__(
        self,
        graph_client: GraphClient,
        target_resource: Any,
        *,
        worker_id: str = "",
        sections: Optional[List[str]] = None,
        include_group_membership: bool = True,
        include_au_membership: bool = True,
    ):
        self.graph = graph_client
        self.target = target_resource
        self.worker_id = worker_id
        self.sections = sections
        self.include_group_membership = include_group_membership
        self.include_au_membership = include_au_membership

    @staticmethod
    def build_plan(items: List[Any], sections: Optional[List[str]]) -> EntraPlan:
        """Phase 1. Partition items by restorable item_type, honouring
        the UI section allowlist and dropping read-only buckets."""
        plan = EntraPlan()
        allowed_types: Optional[set] = None
        if sections:
            allowed_types = {UI_SECTION_TO_ITEM_TYPE[s] for s in sections if s in UI_SECTION_TO_ITEM_TYPE}

        for it in items:
            kind = getattr(it, "item_type", None)
            if kind not in SECTION_SPECS:  # read-only section (AUDIT, etc.)
                continue
            if allowed_types is not None and kind not in allowed_types:
                continue
            # Drop read-only buckets within multi-bucket sections.
            if kind in _READ_ONLY_BUCKETS:
                extra = getattr(it, "extra_data", None) or {}
                bucket_key = "_sec_bucket" if kind == "ENTRA_DIR_SECURITY" else "_intune_bucket"
                if extra.get(bucket_key) in _READ_ONLY_BUCKETS[kind]:
                    continue
            plan.sections.setdefault(kind, []).append(it)
        return plan
