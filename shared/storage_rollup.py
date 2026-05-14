"""Shared filters for resource-storage rollups.

The backup pipeline writes `storage_bytes` for both a Tier-1 row
(ONEDRIVE / MAILBOX — top-level peers of ENTRA_USER) and a Tier-2 row
(USER_ONEDRIVE / USER_MAIL — children of ENTRA_USER) for the same user.
A naive `SUM(Resource.storage_bytes)` therefore double-counts user drive
and mail content.

These helpers centralise the dedupe logic so every rollup path
(dashboard, report, resource-list subtree) agrees on the same total.

Rule applied here: the Tier-1 row is canonical for drives + mailboxes
(it is the historical full-walk path; Tier-2 captures partition/delta
work). When a Tier-1 peer exists for the same `(tenant_id, email)`, the
Tier-2 dupe is excluded from sums and rolled in only via the Tier-1
value.

Resource types that exist ONLY at Tier-2 (USER_CHATS, USER_CONTACTS,
USER_CALENDAR) are never excluded.
"""
from __future__ import annotations

from sqlalchemy import and_, exists, not_, or_, select
from sqlalchemy.orm import aliased

from shared.models import Resource, ResourceType


def exclude_tier2_storage_dupes_clause():
    """SQLAlchemy WHERE clause excluding double-counted Tier-2 rows.

    Two patterns are excluded so a flat ``SUM(Resource.storage_bytes)``
    over all of a tenant's resources yields one number per user / drive:

    1. **Legacy peer pattern** — USER_ONEDRIVE / USER_MAIL rows whose
       Tier-1 ONEDRIVE / MAILBOX peer exists for the same
       (tenant_id, email). The Tier-1 row keeps the canonical full-walk
       number; the Tier-2 row is a duplicate of the same drive/mailbox.

    2. **ENTRA_USER hierarchy pattern** — USER_MAIL / USER_ONEDRIVE /
       USER_CHATS / USER_CALENDAR / USER_CONTACTS rows whose
       ``parent_resource_id`` points at an ENTRA_USER row. The
       ENTRA_USER row carries the full subtree rollup (its own
       metadata + every Tier-2 child's bytes); summing both the
       parent and the children double-counts the children. The
       backup-worker write path is responsible for keeping the
       ENTRA_USER rollup current (re-rolled on every child's
       finalize — see ``update_resource_backup_info`` in
       workers/backup-worker/main.py).
    """
    R1 = aliased(Resource)
    tier1_onedrive = (
        select(R1.id)
        .where(
            R1.tenant_id == Resource.tenant_id,
            R1.email == Resource.email,
            R1.type == ResourceType.ONEDRIVE,
            R1.archived_at.is_(None),
        )
        .exists()
    )
    tier1_mailbox = (
        select(R1.id)
        .where(
            R1.tenant_id == Resource.tenant_id,
            R1.email == Resource.email,
            R1.type == ResourceType.MAILBOX,
            R1.archived_at.is_(None),
        )
        .exists()
    )
    entra_parent = (
        select(R1.id)
        .where(
            R1.id == Resource.parent_resource_id,
            R1.type == ResourceType.ENTRA_USER,
            R1.archived_at.is_(None),
        )
        .exists()
    )
    user_tier2_types = (
        ResourceType.USER_MAIL,
        ResourceType.USER_ONEDRIVE,
        ResourceType.USER_CHATS,
        ResourceType.USER_CALENDAR,
        ResourceType.USER_CONTACTS,
    )
    return not_(
        or_(
            and_(Resource.type == ResourceType.USER_ONEDRIVE, tier1_onedrive),
            and_(Resource.type == ResourceType.USER_MAIL, tier1_mailbox),
            and_(Resource.type.in_(user_tier2_types), entra_parent),
        )
    )
