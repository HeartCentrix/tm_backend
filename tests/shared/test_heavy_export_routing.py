"""Verify that pick_export_queue routes heavy jobs to restore.heavy when enabled."""
import importlib
import pytest


def test_pick_queue_for_size_under_threshold(monkeypatch):
    monkeypatch.setenv("HEAVY_EXPORT_ENABLED", "true")
    monkeypatch.setenv("HEAVY_EXPORT_THRESHOLD_BYTES", str(10 * 1024 * 1024 * 1024))
    from shared import config
    importlib.reload(config)
    from services import job_service_utils
    importlib.reload(job_service_utils)
    queue = job_service_utils.pick_export_queue(total_bytes=5 * 1024 * 1024 * 1024, include_attachments=True)
    assert queue == "restore.normal"


def test_pick_queue_for_size_over_threshold_with_attachments(monkeypatch):
    monkeypatch.setenv("HEAVY_EXPORT_ENABLED", "true")
    monkeypatch.setenv("HEAVY_EXPORT_THRESHOLD_BYTES", str(10 * 1024 * 1024 * 1024))
    from shared import config
    importlib.reload(config)
    from services import job_service_utils
    importlib.reload(job_service_utils)
    queue = job_service_utils.pick_export_queue(total_bytes=20 * 1024 * 1024 * 1024, include_attachments=True)
    assert queue == "restore.heavy"


def test_heavy_routing_disabled_always_normal(monkeypatch):
    monkeypatch.setenv("HEAVY_EXPORT_ENABLED", "false")
    from shared import config
    importlib.reload(config)
    from services import job_service_utils
    importlib.reload(job_service_utils)
    queue = job_service_utils.pick_export_queue(total_bytes=200 * 1024 * 1024 * 1024, include_attachments=True)
    assert queue == "restore.normal"


def test_no_attachments_always_normal_even_if_huge(monkeypatch):
    monkeypatch.setenv("HEAVY_EXPORT_ENABLED", "true")
    monkeypatch.setenv("HEAVY_EXPORT_THRESHOLD_BYTES", str(10 * 1024 * 1024 * 1024))
    from shared import config
    importlib.reload(config)
    from services import job_service_utils
    importlib.reload(job_service_utils)
    queue = job_service_utils.pick_export_queue(total_bytes=500 * 1024 * 1024 * 1024, include_attachments=False)
    assert queue == "restore.normal"
