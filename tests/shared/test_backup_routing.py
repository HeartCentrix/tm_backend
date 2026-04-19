"""Unit tests for pick_backup_queue — heavy-pool routing for OneDrive drives."""
import importlib


def _reload_settings(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    from shared import config as _cfg
    importlib.reload(_cfg)
    from shared import export_routing as _r
    importlib.reload(_r)
    return _r


def test_heavy_disabled_always_returns_normal(monkeypatch):
    r = _reload_settings(
        monkeypatch,
        BACKUP_HEAVY_ENABLED="false",
        BACKUP_WORKER_QUEUE="backup.normal",
        BACKUP_HEAVY_QUEUE="backup.heavy",
        BACKUP_HEAVY_THRESHOLD_BYTES="1",
    )
    assert r.pick_backup_queue(drive_bytes_estimate=10**12, resource_type="USER_ONEDRIVE") == "backup.normal"


def test_non_onedrive_never_goes_heavy(monkeypatch):
    r = _reload_settings(
        monkeypatch,
        BACKUP_HEAVY_ENABLED="true",
        BACKUP_WORKER_QUEUE="backup.normal",
        BACKUP_HEAVY_QUEUE="backup.heavy",
        BACKUP_HEAVY_THRESHOLD_BYTES="1",
    )
    assert r.pick_backup_queue(drive_bytes_estimate=10**12, resource_type="MAILBOX") == "backup.normal"


def test_small_onedrive_goes_normal(monkeypatch):
    r = _reload_settings(
        monkeypatch,
        BACKUP_HEAVY_ENABLED="true",
        BACKUP_WORKER_QUEUE="backup.normal",
        BACKUP_HEAVY_QUEUE="backup.heavy",
        BACKUP_HEAVY_THRESHOLD_BYTES=str(100 * 1024**3),
    )
    assert r.pick_backup_queue(drive_bytes_estimate=5 * 1024**3, resource_type="USER_ONEDRIVE") == "backup.normal"


def test_large_onedrive_goes_heavy(monkeypatch):
    r = _reload_settings(
        monkeypatch,
        BACKUP_HEAVY_ENABLED="true",
        BACKUP_WORKER_QUEUE="backup.normal",
        BACKUP_HEAVY_QUEUE="backup.heavy",
        BACKUP_HEAVY_THRESHOLD_BYTES=str(100 * 1024**3),
    )
    assert r.pick_backup_queue(drive_bytes_estimate=500 * 1024**3, resource_type="USER_ONEDRIVE") == "backup.heavy"
