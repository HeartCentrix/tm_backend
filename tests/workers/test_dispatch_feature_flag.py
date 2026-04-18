"""Verify that the feature flag toggle is readable from settings."""
import importlib
import os
import pytest


def test_flag_off_routes_to_legacy(monkeypatch):
    monkeypatch.setenv("EXPORT_MAIL_V2_ENABLED", "false")
    from shared import config as cfg
    importlib.reload(cfg)
    assert cfg.settings.EXPORT_MAIL_V2_ENABLED is False


def test_flag_on_routes_to_v2(monkeypatch):
    monkeypatch.setenv("EXPORT_MAIL_V2_ENABLED", "true")
    from shared import config as cfg
    importlib.reload(cfg)
    assert cfg.settings.EXPORT_MAIL_V2_ENABLED is True
