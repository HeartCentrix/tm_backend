"""Verify mail-export config values default correctly and respond to env vars."""
import importlib
import os
import pytest


def _reload_settings():
    import shared.config as cfg
    return importlib.reload(cfg).settings


def test_defaults(monkeypatch):
    for var in [
        "EXPORT_PARALLELISM",
        "EXPORT_MBOX_SPLIT_BYTES",
        "EXPORT_BLOCK_SIZE_BYTES",
        "EXPORT_FOLDER_QUEUE_MAXSIZE",
        "MAX_CONCURRENT_EXPORTS_PER_WORKER",
        "EXPORT_FETCH_BATCH_SIZE",
        "EXPORT_MEMORY_SOFT_LIMIT_PCT",
        "EXPORT_MEMORY_KILL_GRACE_SECONDS",
        "EXPORT_MAIL_V2_ENABLED",
    ]:
        monkeypatch.delenv(var, raising=False)

    s = _reload_settings()
    assert s.EXPORT_PARALLELISM == 12
    assert s.EXPORT_MBOX_SPLIT_BYTES == 5_368_709_120
    assert s.EXPORT_BLOCK_SIZE_BYTES == 4_194_304
    assert s.EXPORT_FOLDER_QUEUE_MAXSIZE == 20
    assert s.MAX_CONCURRENT_EXPORTS_PER_WORKER == 2
    assert s.EXPORT_FETCH_BATCH_SIZE == 50
    assert s.EXPORT_MEMORY_SOFT_LIMIT_PCT == 80
    assert s.EXPORT_MEMORY_KILL_GRACE_SECONDS == 60
    assert s.EXPORT_MAIL_V2_ENABLED is False


def test_override(monkeypatch):
    monkeypatch.setenv("EXPORT_PARALLELISM", "24")
    monkeypatch.setenv("EXPORT_MAIL_V2_ENABLED", "true")
    s = _reload_settings()
    assert s.EXPORT_PARALLELISM == 24
    assert s.EXPORT_MAIL_V2_ENABLED is True
