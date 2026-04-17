"""Preset SLA policies seeded on tenant creation.

Mirrors afi.ai's default tiering (Gold / Silver / Bronze / Manual) so every
tenant has a sane policy to attach on day one. Called from auth-service after
tenant.flush(); safe to re-run (idempotent on name+tenant).

M365 and Azure tenants get different preset shapes — Azure tenants don't need
Exchange/OneDrive toggles, and M365 tenants don't run VM/SQL backups.
"""

from __future__ import annotations
from typing import List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.models import SlaPolicy


def _m365_presets(tenant_id) -> List[dict]:
    """Gold = hourly, long retention. Silver = daily. Bronze = weekly. Manual = user-triggered."""
    common_workloads = dict(
        backup_exchange=True, backup_onedrive=True, backup_sharepoint=True,
        backup_teams=True, backup_teams_chats=True, backup_entra_id=True,
        contacts=True, calendars=True, group_mailbox=True, planner=True,
        backup_power_platform=False, backup_copilot=False,
    )
    return [
        dict(
            name="Gold",
            service_type="m365",
            frequency="THREE_DAILY",
            retention_mode="GFS",
            gfs_daily_count=14, gfs_weekly_count=8, gfs_monthly_count=12, gfs_yearly_count=7,
            retention_hot_days=30, retention_cool_days=180, retention_archive_days=2555,
            is_default=True, enabled=True,
            **common_workloads,
        ),
        dict(
            name="Silver",
            service_type="m365",
            frequency="DAILY",
            retention_mode="GFS",
            gfs_daily_count=7, gfs_weekly_count=4, gfs_monthly_count=12, gfs_yearly_count=3,
            retention_hot_days=14, retention_cool_days=90, retention_archive_days=1095,
            is_default=False, enabled=True,
            **common_workloads,
        ),
        dict(
            name="Bronze",
            service_type="m365",
            frequency="DAILY",
            retention_mode="FLAT",
            retention_hot_days=7, retention_cool_days=30, retention_archive_days=365,
            is_default=False, enabled=True,
            **common_workloads,
        ),
        dict(
            name="Manual",
            service_type="m365",
            frequency="MANUAL",
            retention_mode="FLAT",
            retention_hot_days=7, retention_cool_days=30, retention_archive_days=90,
            is_default=False, enabled=True,
            **common_workloads,
        ),
    ]


def _azure_presets(tenant_id) -> List[dict]:
    common_workloads = dict(
        backup_azure_vm=True, backup_azure_sql=True, backup_azure_postgresql=True,
        backup_exchange=False, backup_onedrive=False, backup_sharepoint=False,
        backup_teams=False, backup_teams_chats=False, backup_entra_id=False,
        contacts=False, calendars=False, group_mailbox=False, planner=False,
    )
    return [
        dict(
            name="Gold",
            service_type="azure",
            frequency="THREE_DAILY",
            retention_mode="GFS",
            gfs_daily_count=14, gfs_weekly_count=8, gfs_monthly_count=12, gfs_yearly_count=7,
            retention_hot_days=30, retention_cool_days=180, retention_archive_days=2555,
            is_default=True, enabled=True,
            **common_workloads,
        ),
        dict(
            name="Silver",
            service_type="azure",
            frequency="DAILY",
            retention_mode="GFS",
            gfs_daily_count=7, gfs_weekly_count=4, gfs_monthly_count=12, gfs_yearly_count=3,
            retention_hot_days=14, retention_cool_days=90, retention_archive_days=1095,
            is_default=False, enabled=True,
            **common_workloads,
        ),
        dict(
            name="Bronze",
            service_type="azure",
            frequency="DAILY",
            retention_mode="FLAT",
            retention_hot_days=7, retention_cool_days=30, retention_archive_days=365,
            is_default=False, enabled=True,
            **common_workloads,
        ),
        dict(
            name="Manual",
            service_type="azure",
            frequency="MANUAL",
            retention_mode="FLAT",
            retention_hot_days=7, retention_cool_days=30, retention_archive_days=90,
            is_default=False, enabled=True,
            **common_workloads,
        ),
    ]


async def seed_preset_policies(db: AsyncSession, tenant_id, tenant_type: str) -> int:
    """Create Gold/Silver/Bronze/Manual if they don't already exist for this tenant.
    Returns count of newly-inserted policies. Safe to call repeatedly."""
    tenant_type = (tenant_type or "").upper()
    presets = _m365_presets(tenant_id) if tenant_type == "M365" else _azure_presets(tenant_id)

    existing = (await db.execute(
        select(SlaPolicy.name).where(SlaPolicy.tenant_id == tenant_id)
    )).scalars().all()
    existing_names = {n.lower() for n in existing}

    inserted = 0
    for spec in presets:
        if spec["name"].lower() in existing_names:
            continue
        db.add(SlaPolicy(tenant_id=tenant_id, **spec))
        inserted += 1

    if inserted:
        await db.flush()
    return inserted
