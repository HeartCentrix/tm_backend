"""Defaults + env-override tests for contact backup config flags."""
import importlib


def _reload_settings():
    import shared.config as cfg
    return importlib.reload(cfg).settings


def test_contacts_backup_flag_defaults(monkeypatch):
    for v in ["BACKUP_CONTACTS_INCLUDE_DELETED", "BACKUP_CONTACTS_INCLUDE_RECOVERABLE"]:
        monkeypatch.delenv(v, raising=False)
    s = _reload_settings()
    assert s.BACKUP_CONTACTS_INCLUDE_DELETED is True
    assert s.BACKUP_CONTACTS_INCLUDE_RECOVERABLE is True


def test_contacts_backup_flag_env_override(monkeypatch):
    monkeypatch.setenv("BACKUP_CONTACTS_INCLUDE_DELETED", "false")
    monkeypatch.setenv("BACKUP_CONTACTS_INCLUDE_RECOVERABLE", "0")
    s = _reload_settings()
    assert s.BACKUP_CONTACTS_INCLUDE_DELETED is False
    assert s.BACKUP_CONTACTS_INCLUDE_RECOVERABLE is False
