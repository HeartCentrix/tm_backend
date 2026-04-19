"""Verify OneDrive export + backup config defaults and env overrides."""
import importlib


def _reload_settings():
    import shared.config as cfg
    return importlib.reload(cfg).settings


def test_onedrive_export_defaults(monkeypatch):
    for v in [
        "EXPORT_ONEDRIVE_V2_ENABLED",
        "EXPORT_ONEDRIVE_MISSING_POLICY",
        "EXPORT_ONEDRIVE_INCLUDE_VERSIONS",
        "EXPORT_ONEDRIVE_MAX_FILE_BYTES",
        "EXPORT_ONEDRIVE_PATH_MAX_LEN",
        "EXPORT_ONEDRIVE_SANITIZE_CHARS",
    ]:
        monkeypatch.delenv(v, raising=False)
    s = _reload_settings()
    assert s.EXPORT_ONEDRIVE_V2_ENABLED is False
    assert s.EXPORT_ONEDRIVE_MISSING_POLICY == "skip"
    assert s.EXPORT_ONEDRIVE_INCLUDE_VERSIONS is False
    assert s.EXPORT_ONEDRIVE_MAX_FILE_BYTES == 214_748_364_800
    assert s.EXPORT_ONEDRIVE_PATH_MAX_LEN == 260
    assert s.EXPORT_ONEDRIVE_SANITIZE_CHARS == '<>:"/\\|?*'


def test_onedrive_backup_defaults(monkeypatch):
    for v in [
        "ONEDRIVE_BACKUP_V2_ENABLED",
        "ONEDRIVE_BACKUP_FILE_CONCURRENCY",
        "MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER",
        "ONEDRIVE_BACKUP_FILE_TIMEOUT_SECONDS",
        "ONEDRIVE_BACKUP_CHECKPOINT_EVERY_FILES",
        "ONEDRIVE_BACKUP_CHECKPOINT_EVERY_BYTES",
        "BACKUP_HEAVY_ENABLED",
        "BACKUP_HEAVY_THRESHOLD_BYTES",
        "BACKUP_HEAVY_QUEUE",
        "BACKUP_WORKER_QUEUE",
        "RABBITMQ_CONSUMER_HEARTBEAT_SECONDS",
        "RABBITMQ_CONSUMER_TIMEOUT_MS",
    ]:
        monkeypatch.delenv(v, raising=False)
    s = _reload_settings()
    assert s.ONEDRIVE_BACKUP_V2_ENABLED is False
    assert s.ONEDRIVE_BACKUP_FILE_CONCURRENCY == 16
    assert s.MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER == 4
    assert s.ONEDRIVE_BACKUP_FILE_TIMEOUT_SECONDS == 21600
    assert s.ONEDRIVE_BACKUP_CHECKPOINT_EVERY_FILES == 500
    assert s.ONEDRIVE_BACKUP_CHECKPOINT_EVERY_BYTES == 1_073_741_824
    assert s.BACKUP_HEAVY_ENABLED is False
    assert s.BACKUP_HEAVY_THRESHOLD_BYTES == 107_374_182_400
    assert s.BACKUP_HEAVY_QUEUE == "backup.heavy"
    assert s.BACKUP_WORKER_QUEUE == "backup.normal"
    assert s.RABBITMQ_CONSUMER_HEARTBEAT_SECONDS == 604_800
    assert s.RABBITMQ_CONSUMER_TIMEOUT_MS == 604_800_000


def test_env_override(monkeypatch):
    monkeypatch.setenv("EXPORT_ONEDRIVE_V2_ENABLED", "true")
    monkeypatch.setenv("ONEDRIVE_BACKUP_FILE_CONCURRENCY", "32")
    monkeypatch.setenv("EXPORT_ONEDRIVE_MISSING_POLICY", "retry")
    s = _reload_settings()
    assert s.EXPORT_ONEDRIVE_V2_ENABLED is True
    assert s.ONEDRIVE_BACKUP_FILE_CONCURRENCY == 32
    assert s.EXPORT_ONEDRIVE_MISSING_POLICY == "retry"
