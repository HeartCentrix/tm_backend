import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace

from shared.power_bi_snapshot import (
    POWER_BI_INCREMENTAL_STRATEGY_VERSION,
    assemble_power_bi_items,
    build_power_bi_item_key,
    should_force_power_bi_full_snapshot,
)


class PowerBISnapshotTests(unittest.TestCase):
    def test_build_power_bi_item_key(self):
        self.assertEqual(
            build_power_bi_item_key("POWER_BI_REPORT", "abc"),
            "POWER_BI_REPORT:abc",
        )

    def test_force_full_when_no_previous_full_exists(self):
        should_force, reason = should_force_power_bi_full_snapshot(
            latest_full_created_at=None,
            latest_snapshot_extra=None,
            now=datetime(2026, 1, 1, 12, 0, 0),
        )
        self.assertTrue(should_force)
        self.assertEqual(reason, "no_previous_full")

    def test_force_full_when_previous_full_is_stale(self):
        should_force, reason = should_force_power_bi_full_snapshot(
            latest_full_created_at=datetime(2025, 12, 20, 12, 0, 0),
            latest_snapshot_extra={
                "incremental_strategy_version": POWER_BI_INCREMENTAL_STRATEGY_VERSION,
                "base_full_snapshot_id": "full-1",
            },
            now=datetime(2026, 1, 1, 12, 0, 0),
            max_age_days=7,
        )
        self.assertTrue(should_force)
        self.assertEqual(reason, "full_snapshot_too_old")

    def test_incremental_allowed_for_recent_compatible_chain(self):
        should_force, reason = should_force_power_bi_full_snapshot(
            latest_full_created_at=datetime(2025, 12, 29, 12, 0, 0),
            latest_snapshot_extra={
                "incremental_strategy_version": POWER_BI_INCREMENTAL_STRATEGY_VERSION,
                "base_full_snapshot_id": "full-1",
            },
            now=datetime(2026, 1, 1, 12, 0, 0),
            max_age_days=7,
        )
        self.assertFalse(should_force)
        self.assertEqual(reason, "incremental_ok")

    def test_assembled_latest_view_uses_full_plus_incremental_chain(self):
        base_time = datetime(2026, 1, 1, 9, 0, 0)
        snapshots = [
            SimpleNamespace(id="full-1", created_at=base_time, type=SimpleNamespace(value="FULL")),
            SimpleNamespace(id="inc-1", created_at=base_time + timedelta(hours=1), type=SimpleNamespace(value="INCREMENTAL")),
            SimpleNamespace(id="inc-2", created_at=base_time + timedelta(hours=2), type=SimpleNamespace(value="INCREMENTAL")),
        ]
        items = [
            SimpleNamespace(
                id="item-1",
                snapshot_id="full-1",
                item_type="POWER_BI_REPORT",
                external_id="report-1",
                is_deleted=False,
                created_at=base_time,
                folder_path="reports",
                name="Sales",
            ),
            SimpleNamespace(
                id="item-2",
                snapshot_id="inc-1",
                item_type="POWER_BI_REPORT",
                external_id="report-1",
                is_deleted=False,
                created_at=base_time + timedelta(hours=1),
                folder_path="reports",
                name="Sales v2",
            ),
            SimpleNamespace(
                id="item-3",
                snapshot_id="inc-2",
                item_type="POWER_BI_DATAFLOW",
                external_id="df-1",
                is_deleted=False,
                created_at=base_time + timedelta(hours=2),
                folder_path="dataflows",
                name="Finance Flow",
            ),
        ]

        assembled = assemble_power_bi_items(snapshots, items, up_to_snapshot_id="inc-2")

        self.assertEqual(len(assembled), 2)
        assembled_by_key = {
            build_power_bi_item_key(item.item_type, item.external_id): item
            for item in assembled
        }
        self.assertEqual(assembled_by_key["POWER_BI_REPORT:report-1"].name, "Sales v2")
        self.assertEqual(assembled_by_key["POWER_BI_DATAFLOW:df-1"].name, "Finance Flow")

    def test_assembled_latest_view_drops_deleted_items(self):
        base_time = datetime(2026, 1, 1, 9, 0, 0)
        snapshots = [
            SimpleNamespace(id="full-1", created_at=base_time, type=SimpleNamespace(value="FULL")),
            SimpleNamespace(id="inc-1", created_at=base_time + timedelta(hours=1), type=SimpleNamespace(value="INCREMENTAL")),
        ]
        items = [
            SimpleNamespace(
                id="item-1",
                snapshot_id="full-1",
                item_type="POWER_BI_DASHBOARD",
                external_id="dash-1",
                is_deleted=False,
                created_at=base_time,
                folder_path="dashboards",
                name="Executive",
            ),
            SimpleNamespace(
                id="item-2",
                snapshot_id="inc-1",
                item_type="POWER_BI_DASHBOARD",
                external_id="dash-1",
                is_deleted=True,
                created_at=base_time + timedelta(hours=1),
                folder_path="dashboards",
                name="Executive",
            ),
        ]

        assembled = assemble_power_bi_items(snapshots, items, up_to_snapshot_id="inc-1")
        self.assertEqual(assembled, [])


if __name__ == "__main__":
    unittest.main()
