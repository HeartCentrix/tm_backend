"""Retention cleanup — delete Snapshots that fall outside their SLA policy's
retention window.

Called daily from backup-scheduler. Safe to run multiple times (idempotent:
already-deleted snapshots are gone from the DB and won't be re-processed).

Retention modes (SlaPolicy.retention_mode):
  FLAT        — keep snapshots newer than retention_days
                (fallback: retention_hot_days + retention_cool_days + retention_archive_days)
  GFS         — keep N most recent daily + N weekly + N monthly + N yearly
  ITEM_LEVEL  — operates per-item (not in this module); snapshots kept indefinitely
                unless an outer FLAT cutoff is also set
  HYBRID      — FLAT for snapshots + item-level pruning inside kept snapshots
                (item-level pass is TODO; for now behaves like FLAT on snapshots)

Legal hold + immutability:
  - legal_hold_enabled + (legal_hold_until is NULL or in the future) → skip pruning
  - immutability_mode == "Locked" → skip pruning (honor WORM)
  - immutability_mode == "Unlocked" → prune normally (user-managed)
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set, Tuple
import uuid

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import SlaPolicy, Snapshot, Resource, SnapshotItem, Tenant


def _is_on_hold(policy: SlaPolicy) -> bool:
    """Legal hold / immutable policy — don't delete anything."""
    if policy.legal_hold_enabled:
        if policy.legal_hold_until is None:
            return True
        if policy.legal_hold_until > datetime.utcnow():
            return True
    if (policy.immutability_mode or "").lower() == "locked":
        return True
    return False


def _flat_keep_ids(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    """FLAT: keep snapshots within retention_days (or tiered hot+cool+archive sum)."""
    if not snapshots:
        return set()
    keep_days = policy.retention_days
    if not keep_days:
        keep_days = (policy.retention_hot_days or 0) + (policy.retention_cool_days or 0)
        if policy.retention_archive_days is not None:
            keep_days += policy.retention_archive_days
        else:
            # unlimited archive → keep everything
            return {s.id for s in snapshots}
    if keep_days <= 0:
        return {s.id for s in snapshots}
    cutoff = datetime.utcnow() - timedelta(days=keep_days)
    kept = {s.id for s in snapshots if (s.started_at or s.created_at or datetime.utcnow()) >= cutoff}
    # Always keep the most recent snapshot as a safety net
    latest = max(snapshots, key=lambda s: s.started_at or s.created_at or datetime.min)
    kept.add(latest.id)
    return kept


def _gfs_keep_ids(snapshots: List[Snapshot], policy: SlaPolicy) -> Set[uuid.UUID]:
    """GFS: keep N most-recent daily + N weekly (Sunday) + N monthly (1st) + N yearly (Jan 1).
    A snapshot can count toward multiple buckets; it's kept if *any* bucket claims it."""
    if not snapshots:
        return set()
    n_daily = policy.gfs_daily_count or 0
    n_weekly = policy.gfs_weekly_count or 0
    n_monthly = policy.gfs_monthly_count or 0
    n_yearly = policy.gfs_yearly_count or 0

    sorted_snaps = sorted(
        snapshots,
        key=lambda s: s.started_at or s.created_at or datetime.min,
        reverse=True,
    )

    def _ts(s: Snapshot) -> datetime:
        return s.started_at or s.created_at or datetime.min

    keep: Set[uuid.UUID] = set()
    # Always keep the most recent
    keep.add(sorted_snaps[0].id)

    # Daily: first snapshot per calendar day, cap at n_daily
    seen_days: Dict[str, uuid.UUID] = {}
    for s in sorted_snaps:
        key = _ts(s).strftime("%Y-%m-%d")
        if key not in seen_days:
            seen_days[key] = s.id
            if len(seen_days) >= n_daily:
                break
    keep.update(seen_days.values())

    # Weekly: first snapshot per ISO week
    seen_weeks: Dict[str, uuid.UUID] = {}
    for s in sorted_snaps:
        iso = _ts(s).isocalendar()
        key = f"{iso[0]}-W{iso[1]}"
        if key not in seen_weeks:
            seen_weeks[key] = s.id
            if len(seen_weeks) >= n_weekly:
                break
    keep.update(seen_weeks.values())

    # Monthly: first snapshot per calendar month
    seen_months: Dict[str, uuid.UUID] = {}
    for s in sorted_snaps:
        key = _ts(s).strftime("%Y-%m")
        if key not in seen_months:
            seen_months[key] = s.id
            if len(seen_months) >= n_monthly:
                break
    keep.update(seen_months.values())

    # Yearly: first snapshot per year
    seen_years: Dict[str, uuid.UUID] = {}
    for s in sorted_snaps:
        key = _ts(s).strftime("%Y")
        if key not in seen_years:
            seen_years[key] = s.id
            if len(seen_years) >= n_yearly:
                break
    keep.update(seen_years.values())

    return keep


async def _delete_snapshots(session: AsyncSession, snap_ids: Set[uuid.UUID]) -> int:
    """Delete snapshot rows and their items. Blob cleanup is handled by Azure
    lifecycle policies (applied separately) — we just drop the DB rows here."""
    if not snap_ids:
        return 0
    ids = list(snap_ids)
    await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id.in_(ids)))
    result = await session.execute(delete(Snapshot).where(Snapshot.id.in_(ids)))
    return result.rowcount or 0


async def enforce_retention_for_tenant(session: AsyncSession, tenant_id: uuid.UUID) -> Dict[str, int]:
    """Walk all resources for a tenant, apply each resource's SLA policy,
    delete snapshots outside retention. Returns per-mode stats."""
    stats = {"checked_resources": 0, "held": 0, "deleted_snapshots": 0, "kept_snapshots": 0}

    resources = (await session.execute(
        select(Resource).where(Resource.tenant_id == tenant_id)
    )).scalars().all()

    # Preload all policies for the tenant once
    pol_rows = (await session.execute(
        select(SlaPolicy).where(SlaPolicy.tenant_id == tenant_id)
    )).scalars().all()
    policies_by_id = {p.id: p for p in pol_rows}
    default_policy = next((p for p in pol_rows if p.is_default), None)

    for res in resources:
        stats["checked_resources"] += 1
        policy = policies_by_id.get(res.sla_policy_id) or default_policy
        if policy is None:
            continue
        if _is_on_hold(policy):
            stats["held"] += 1
            continue

        snaps = (await session.execute(
            select(Snapshot).where(Snapshot.resource_id == res.id)
        )).scalars().all()
        if not snaps:
            continue

        mode = (policy.retention_mode or "FLAT").upper()
        if mode == "GFS":
            keep = _gfs_keep_ids(snaps, policy)
        else:
            keep = _flat_keep_ids(snaps, policy)

        to_delete = {s.id for s in snaps} - keep
        deleted = await _delete_snapshots(session, to_delete)
        stats["deleted_snapshots"] += deleted
        stats["kept_snapshots"] += len(keep)

    await session.commit()
    return stats


async def enforce_retention_all_tenants(session_factory) -> Dict[str, Dict[str, int]]:
    """Entry point for the scheduler. Runs retention for every tenant."""
    results: Dict[str, Dict[str, int]] = {}
    async with session_factory() as session:
        tenants = (await session.execute(select(Tenant))).scalars().all()
    for t in tenants:
        async with session_factory() as session:
            try:
                results[str(t.id)] = await enforce_retention_for_tenant(session, t.id)
            except Exception as exc:
                results[str(t.id)] = {"error": str(exc)}
    return results
