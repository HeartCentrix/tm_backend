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

    # ---- phase 2 + 3: sieve + diff ----

    async def _sieve_section(
        self, item_type: str, items: List[Any],
    ) -> Dict[str, Tuple[bool, Optional[str]]]:
        """Return `{external_id: (exists, live_fingerprint)}` for each
        item in the section via two /$batch rounds.

        ENTRA_DIR_SECURITY is a union of 5 sub-buckets (CA policies,
        Named Locations, Auth Strengths, Auth Contexts, Security
        Defaults). Each lives under a different Graph endpoint, so the
        single SectionSpec URL can't serve all of them — we route per
        bucket here and merge results."""
        from shared.graph_entra import sieve_existence
        from shared.graph_batch import BatchRequest, batch_requests
        from shared.entra_fingerprint import fingerprint_object

        if item_type == "ENTRA_DIR_SECURITY":
            return await self._sieve_security_buckets(items)

        spec: SectionSpec = SECTION_SPECS[item_type]

        ids = [it.external_id for it in items]
        exists_map = await sieve_existence(self.graph, spec, ids)

        # Second round: pull raw fields for existing objects so we can
        # fingerprint-compare.
        live_existing_ids = [oid for oid, e in exists_map.items() if e]
        fingerprints: Dict[str, Optional[str]] = {oid: None for oid in ids}
        if live_existing_ids:
            reqs = [
                BatchRequest(id=f"fp-{i}", method="GET",
                             url=spec.object_url_template.format(id=oid))
                for i, oid in enumerate(live_existing_ids)
            ]
            fp_responses = await batch_requests(self.graph._post, reqs)
            for req, resp in zip(reqs, fp_responses):
                oid = req.url.rsplit("/", 1)[-1]
                body = (resp or {}).get("body") or {}
                if body:
                    fingerprints[oid] = fingerprint_object(item_type, body)

        return {oid: (exists_map.get(oid, False), fingerprints.get(oid)) for oid in ids}

    async def _sieve_security_buckets(
        self, items: List[Any],
    ) -> Dict[str, Tuple[bool, Optional[str]]]:
        """Per-bucket sieve for ENTRA_DIR_SECURITY. Objects in each
        bucket live under a different Graph path:

          Conditional Access       /identity/conditionalAccess/policies/{id}
          Named Locations          /identity/conditionalAccess/namedLocations/{id}
          Authentication Contexts  /identity/conditionalAccess/authenticationContextClassReferences/{id}
          Authentication Strengths /identity/conditionalAccess/authenticationStrength/policies/{id}
          Security Defaults        /policies/identitySecurityDefaultsEnforcementPolicy  (singleton — no id)

        Unknown / empty buckets default to the CA policies path (same
        shape as before this fix)."""
        from shared.graph_batch import BatchRequest, batch_requests
        from shared.entra_fingerprint import fingerprint_object

        bucket_url = {
            "Conditional Access":       "/identity/conditionalAccess/policies/{id}",
            "Named Locations":          "/identity/conditionalAccess/namedLocations/{id}",
            "Authentication Contexts":  "/identity/conditionalAccess/authenticationContextClassReferences/{id}",
            "Authentication Strengths": "/identity/conditionalAccess/authenticationStrength/policies/{id}",
            # Security Defaults is a tenant singleton — no id in path.
            "Policies":                 "/policies/identitySecurityDefaultsEnforcementPolicy",
        }
        default_url = "/identity/conditionalAccess/policies/{id}"

        # Build one /$batch of existence GETs using the correct per-item
        # URL.
        exist_reqs: List[BatchRequest] = []
        for i, it in enumerate(items):
            ed = it.extra_data or {}
            bucket = ed.get("_sec_bucket")
            tmpl = bucket_url.get(bucket, default_url)
            url = tmpl.format(id=it.external_id) + ("?$select=id" if "{id}" in tmpl else "")
            exist_reqs.append(BatchRequest(id=f"sec-exist-{i}", method="GET", url=url))
        exist_resps = await batch_requests(self.graph._post, exist_reqs)

        exists_map: Dict[str, bool] = {}
        for it, resp in zip(items, exist_resps):
            status = (resp or {}).get("status")
            exists_map[it.external_id] = status is not None and 200 <= status < 300

        # Second round: fetch raw for drift on existing objects.
        fp_ids: List[Tuple[Any, str]] = [(it, bucket_url.get((it.extra_data or {}).get("_sec_bucket"), default_url))
                                         for it in items if exists_map.get(it.external_id)]
        fingerprints: Dict[str, Optional[str]] = {it.external_id: None for it in items}
        if fp_ids:
            fp_reqs = [
                BatchRequest(id=f"sec-fp-{i}", method="GET",
                             url=tmpl.format(id=it.external_id))
                for i, (it, tmpl) in enumerate(fp_ids)
            ]
            fp_resps = await batch_requests(self.graph._post, fp_reqs)
            for (it, _tmpl), resp in zip(fp_ids, fp_resps):
                body = (resp or {}).get("body") or {}
                if body:
                    fingerprints[it.external_id] = fingerprint_object("ENTRA_DIR_SECURITY", body)

        return {it.external_id: (exists_map.get(it.external_id, False),
                                 fingerprints.get(it.external_id))
                for it in items}

    async def _dispatch_one(
        self, item_type: str, snap_item: Any, outcome: str,
    ) -> EntraOutcome:
        """Phase 4: apply the classified outcome to Graph."""
        raw = (snap_item.extra_data or {}).get("raw") or {}
        external_id = snap_item.external_id

        try:
            if outcome == "unchanged":
                return EntraOutcome(
                    item_id=str(snap_item.id),
                    external_id=external_id,
                    section=item_type,
                    outcome="unchanged",
                    graph_id=external_id,
                )
            if outcome == "updated":
                await self._patch_dispatch(item_type, external_id, raw)
                return EntraOutcome(
                    item_id=str(snap_item.id),
                    external_id=external_id,
                    section=item_type,
                    outcome="updated",
                    graph_id=external_id,
                )
            # created
            new_id = await self._create_dispatch(item_type, raw)
            return EntraOutcome(
                item_id=str(snap_item.id),
                external_id=external_id,
                section=item_type,
                outcome="created" if new_id else "failed",
                graph_id=new_id,
                reason=None if new_id else "create_returned_no_id",
            )
        except Exception as e:
            if _is_retryable(e):
                raise
            return EntraOutcome(
                item_id=str(snap_item.id),
                external_id=external_id,
                section=item_type,
                outcome="failed",
                reason=f"{type(e).__name__}: {e}",
            )

    async def _patch_dispatch(self, item_type: str, object_id: str, raw: Dict[str, Any]) -> None:
        from shared.graph_entra import (
            patch_user, patch_group, patch_admin_unit,
            patch_application, patch_ca_policy, patch_intune_policy,
            patch_role_definition,
        )
        PATCH_MAP = {
            "ENTRA_DIR_USER": patch_user,
            "ENTRA_DIR_GROUP": patch_group,
            "ENTRA_DIR_ADMIN_UNIT": patch_admin_unit,
            "ENTRA_DIR_APPLICATION": patch_application,
            "ENTRA_DIR_SECURITY": patch_ca_policy,
            "ENTRA_DIR_INTUNE": patch_intune_policy,
            "ENTRA_DIR_ROLE": patch_role_definition,
        }
        fn = PATCH_MAP.get(item_type)
        if fn is None:
            raise RuntimeError(f"no patch handler for {item_type}")
        await fn(self.graph, object_id, raw)

    async def _create_dispatch(self, item_type: str, raw: Dict[str, Any]) -> Optional[str]:
        from shared.graph_entra import (
            create_user, create_group, create_admin_unit,
            create_application, create_ca_policy, create_intune_policy,
            create_role_definition,
        )
        CREATE_MAP = {
            "ENTRA_DIR_USER": create_user,
            "ENTRA_DIR_GROUP": create_group,
            "ENTRA_DIR_ADMIN_UNIT": create_admin_unit,
            "ENTRA_DIR_APPLICATION": create_application,
            "ENTRA_DIR_SECURITY": create_ca_policy,
            "ENTRA_DIR_INTUNE": create_intune_policy,
            "ENTRA_DIR_ROLE": create_role_definition,
        }
        fn = CREATE_MAP.get(item_type)
        if fn is None:
            raise RuntimeError(f"no create handler for {item_type}")
        return await fn(self.graph, raw)

    # ---- phase 5: run() ----

    async def run(self, items: List[Any]) -> Dict[str, Any]:
        plan = self.build_plan(items, self.sections)
        summary = {"unchanged": 0, "updated": 0, "created": 0, "failed": 0, "skipped": 0}
        all_outcomes: List[EntraOutcome] = []

        global_sem = asyncio.Semaphore(settings.ENTRA_RESTORE_GLOBAL_POOL)
        tenant_sem = asyncio.Semaphore(settings.ENTRA_RESTORE_PER_TENANT)

        async def one(item_type, snap_item, exists, live_fp):
            outcome = classify_outcome(snap_item,
                                       exists_live=exists,
                                       live_fingerprint=live_fp)
            async with global_sem, tenant_sem:
                attempt = 0
                while True:
                    try:
                        return await self._dispatch_one(item_type, snap_item, outcome)
                    except Exception as e:
                        if _is_retryable(e) and attempt < settings.ENTRA_RESTORE_MAX_RETRIES:
                            delay = _retry_after_seconds(e)
                            if delay is None:
                                delay = min(1.0 * (2 ** attempt), 16.0)
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        return EntraOutcome(
                            item_id=str(snap_item.id),
                            external_id=snap_item.external_id,
                            section=item_type,
                            outcome="failed",
                            reason=f"exhausted: {type(e).__name__}: {e}",
                        )

        # Drive sections in dependency order.
        for item_type in SECTION_ORDER:
            section_items = plan.sections.get(item_type)
            if not section_items:
                continue
            sieve = await self._sieve_section(item_type, section_items)
            tasks = [
                asyncio.create_task(
                    one(item_type, it, sieve[it.external_id][0], sieve[it.external_id][1])
                )
                for it in section_items
            ]
            outcomes = await asyncio.gather(*tasks)
            for o in outcomes:
                summary[o.outcome] = summary.get(o.outcome, 0) + 1
                all_outcomes.append(o)

            if item_type == "ENTRA_DIR_GROUP" and self.include_group_membership:
                rb = await self._reconcile_group_memberships(outcomes, section_items)
                summary["members_added"] = summary.get("members_added", 0) + rb["added"]
                summary["members_removed"] = summary.get("members_removed", 0) + rb["removed"]
                summary["members_failed"] = summary.get("members_failed", 0) + rb["failed"]
            elif item_type == "ENTRA_DIR_ADMIN_UNIT" and self.include_au_membership:
                rb = await self._reconcile_au_memberships(outcomes, section_items)
                summary["members_added"] = summary.get("members_added", 0) + rb["added"]
                summary["members_removed"] = summary.get("members_removed", 0) + rb["removed"]
                summary["members_failed"] = summary.get("members_failed", 0) + rb["failed"]

        summary["items"] = [o.__dict__ for o in all_outcomes]
        return summary

    # ---- phase 6: membership rebind ----

    async def _reconcile_group_memberships(
        self, group_outcomes: List[EntraOutcome], snap_items: List[Any],
    ) -> Dict[str, int]:
        """Reconcile membership for each successfully-restored group.
        Adds missing members, removes extras. Counts rolled into
        summary at the run-level."""
        from shared.graph_entra import set_group_members
        from shared.graph_batch import BatchRequest, batch_requests

        snap_by_ext = {s.external_id: s for s in snap_items}
        to_rebind = []
        for o in group_outcomes:
            if o.outcome in ("failed", "skipped"):
                continue
            snap = snap_by_ext.get(o.external_id)
            if not snap:
                continue
            raw = (snap.extra_data or {}).get("raw") or {}
            desired = self._extract_member_ids(raw.get("members") or [])
            to_rebind.append((o.graph_id or o.external_id, desired))

        if not to_rebind:
            return {"added": 0, "removed": 0, "failed": 0}

        # Fetch live members via /$batch. One request per group.
        reqs = [
            BatchRequest(id=f"members-{i}", method="GET",
                         url=f"/groups/{gid}/members?$select=id")
            for i, (gid, _) in enumerate(to_rebind)
        ]
        responses = await batch_requests(self.graph._post, reqs)
        totals = {"added": 0, "removed": 0, "failed": 0}
        for (gid, desired), resp in zip(to_rebind, responses):
            body = (resp or {}).get("body") or {}
            live_ids = [m.get("id") for m in (body.get("value") or []) if m.get("id")]
            result = await set_group_members(
                self.graph, group_id=gid,
                desired_member_ids=desired, live_member_ids=live_ids,
            )
            for k in totals:
                totals[k] += result.get(k, 0)
        return totals

    async def _reconcile_au_memberships(
        self, au_outcomes: List[EntraOutcome], snap_items: List[Any],
    ) -> Dict[str, int]:
        from shared.graph_entra import set_admin_unit_members
        from shared.graph_batch import BatchRequest, batch_requests

        snap_by_ext = {s.external_id: s for s in snap_items}
        to_rebind = []
        for o in au_outcomes:
            if o.outcome in ("failed", "skipped"):
                continue
            snap = snap_by_ext.get(o.external_id)
            if not snap:
                continue
            raw = (snap.extra_data or {}).get("raw") or {}
            desired = self._extract_member_ids(raw.get("members") or [])
            to_rebind.append((o.graph_id or o.external_id, desired))

        if not to_rebind:
            return {"added": 0, "removed": 0, "failed": 0}

        reqs = [
            BatchRequest(id=f"au-members-{i}", method="GET",
                         url=f"/directory/administrativeUnits/{aid}/members?$select=id")
            for i, (aid, _) in enumerate(to_rebind)
        ]
        responses = await batch_requests(self.graph._post, reqs)
        totals = {"added": 0, "removed": 0, "failed": 0}
        for (aid, desired), resp in zip(to_rebind, responses):
            body = (resp or {}).get("body") or {}
            live_ids = [m.get("id") for m in (body.get("value") or []) if m.get("id")]
            result = await set_admin_unit_members(
                self.graph, au_id=aid,
                desired_member_ids=desired, live_member_ids=live_ids,
            )
            for k in totals:
                totals[k] += result.get(k, 0)
        return totals

    @staticmethod
    def _extract_member_ids(members_field: Any) -> List[str]:
        """Flatten snapshot member lists. Accepts either a list of
        dicts ({"id": "..."}) or a list of odata.bind URLs ending in
        the object id. Returns a plain list of ids."""
        out: List[str] = []
        if isinstance(members_field, list):
            for m in members_field:
                if isinstance(m, dict) and "id" in m:
                    out.append(m["id"])
                elif isinstance(m, str):
                    if "directoryObjects/" in m:
                        out.append(m.rsplit("/", 1)[-1])
                    else:
                        out.append(m)
        return out
