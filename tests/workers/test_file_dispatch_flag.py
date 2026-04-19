"""Feature-flag gating for OneDrive export v2 dispatch."""
import importlib


def test_flag_off_by_default(monkeypatch):
    monkeypatch.delenv("EXPORT_ONEDRIVE_V2_ENABLED", raising=False)
    from shared import config
    importlib.reload(config)
    assert config.settings.EXPORT_ONEDRIVE_V2_ENABLED is False


def test_flag_on_when_env_true(monkeypatch):
    monkeypatch.setenv("EXPORT_ONEDRIVE_V2_ENABLED", "true")
    from shared import config
    importlib.reload(config)
    assert config.settings.EXPORT_ONEDRIVE_V2_ENABLED is True
