from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from shared.retention import compute_immutability_mode, compute_retention_until


def test_compute_retention_until_uses_sla_days():
    created = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sla = SimpleNamespace(retention_days=30)
    assert compute_retention_until(sla, created) == created + timedelta(days=30)


def test_compute_retention_until_default_30_when_missing():
    created = datetime(2026, 1, 1, tzinfo=timezone.utc)
    sla = SimpleNamespace()
    assert compute_retention_until(sla, created) == created + timedelta(days=30)


def test_compute_immutability_mode_locked():
    sla = SimpleNamespace(immutability_mode="Locked")
    assert compute_immutability_mode(sla) == "Locked"


def test_compute_immutability_mode_unlocked_default():
    for v in (None, "Unlocked", "unlocked", ""):
        sla = SimpleNamespace(immutability_mode=v)
        assert compute_immutability_mode(sla) == "Unlocked"
