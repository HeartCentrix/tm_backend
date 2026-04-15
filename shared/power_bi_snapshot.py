"""Helpers for Power BI snapshot chaining and incremental assembly."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional


POWER_BI_INCREMENTAL_STRATEGY_VERSION = 1


@dataclass(frozen=True)
class SnapshotLike:
    id: str
    created_at: datetime
    type: str


def build_power_bi_item_key(item_type: str, external_id: str) -> str:
    return f"{item_type}:{external_id}"


def _get_value(source: Any, key: str) -> Any:
    if isinstance(source, dict):
        return source.get(key)
    return getattr(source, key)


def should_force_power_bi_full_snapshot(
    latest_full_created_at: Optional[datetime],
    latest_snapshot_extra: Optional[Dict[str, Any]],
    now: Optional[datetime] = None,
    max_age_days: int = 7,
    strategy_version: int = POWER_BI_INCREMENTAL_STRATEGY_VERSION,
) -> tuple[bool, str]:
    if latest_full_created_at is None:
        return True, "no_previous_full"

    now = now or datetime.utcnow()
    if latest_full_created_at <= now - timedelta(days=max_age_days):
        return True, "full_snapshot_too_old"

    extra = latest_snapshot_extra or {}
    if extra.get("incremental_strategy_version") != strategy_version:
        return True, "strategy_version_changed"

    if extra.get("base_full_snapshot_id") is None:
        return True, "missing_base_full_reference"

    return False, "incremental_ok"


def assemble_power_bi_items(
    snapshots: Iterable[Any],
    items: Iterable[Any],
    up_to_snapshot_id: Optional[str] = None,
) -> List[Any]:
    snapshot_rows = [
        SnapshotLike(
            id=str(_get_value(snapshot, "id")),
            created_at=_get_value(snapshot, "created_at"),
            type=(
                getattr(getattr(snapshot, "type", None), "value", None)
                or _get_value(snapshot, "type")
            ),
        )
        for snapshot in snapshots
    ]
    snapshot_rows.sort(key=lambda snapshot: snapshot.created_at)

    if not snapshot_rows:
        return []

    target_snapshot_id = up_to_snapshot_id or snapshot_rows[-1].id
    target_snapshot = next((snapshot for snapshot in snapshot_rows if snapshot.id == target_snapshot_id), None)
    if target_snapshot is None:
        return []

    eligible_snapshots = [snapshot for snapshot in snapshot_rows if snapshot.created_at <= target_snapshot.created_at]
    base_full = None
    for snapshot in eligible_snapshots:
        if snapshot.type == "FULL":
            base_full = snapshot

    if base_full is None:
        ordered_snapshots = eligible_snapshots
    else:
        ordered_snapshots = [snapshot for snapshot in eligible_snapshots if snapshot.created_at >= base_full.created_at]

    snapshot_index = {snapshot.id: index for index, snapshot in enumerate(ordered_snapshots)}
    current_state: Dict[str, Any] = {}

    ordered_items = sorted(
        [
            item for item in items
            if str(_get_value(item, "snapshot_id")) in snapshot_index
        ],
        key=lambda item: (
            snapshot_index[str(_get_value(item, "snapshot_id"))],
            _get_value(item, "created_at"),
        ),
    )

    for item in ordered_items:
        key = build_power_bi_item_key(
            str(_get_value(item, "item_type")),
            str(_get_value(item, "external_id")),
        )
        if bool(_get_value(item, "is_deleted")):
            current_state.pop(key, None)
            continue
        current_state[key] = item

    assembled = list(current_state.values())
    assembled.sort(
        key=lambda item: (
            str(_get_value(item, "item_type")),
            str((_get_value(item, "folder_path") or "")),
            str(_get_value(item, "name")),
        )
    )
    return assembled
