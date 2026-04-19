"""Env knob defaults + feature flag for Graph API throttle hardening."""
import importlib


def test_hardening_flag_default_false(monkeypatch):
    monkeypatch.delenv("GRAPH_HARDENING_ENABLED", raising=False)
    from shared import config
    importlib.reload(config)
    assert config.settings.GRAPH_HARDENING_ENABLED is False


def test_hardening_flag_on(monkeypatch):
    monkeypatch.setenv("GRAPH_HARDENING_ENABLED", "true")
    from shared import config
    importlib.reload(config)
    assert config.settings.GRAPH_HARDENING_ENABLED is True


def test_pace_defaults(monkeypatch):
    for k in (
        "GRAPH_APP_PACE_REQS_PER_SEC",
        "GRAPH_STREAM_PACE_REQS_PER_SEC",
        "GRAPH_MAX_RETRIES",
        "GRAPH_THROTTLE_BACKOFF_SECONDS",
        "GRAPH_TRANSIENT_BACKOFF_SECONDS",
        "GRAPH_JITTER_RATIO",
        "GRAPH_POST_THROTTLE_BRAKE_MS",
        "GRAPH_MAX_CUMULATIVE_WAIT_SECONDS",
        "GRAPH_MAX_THROTTLE_WAIT_SECONDS",
        "GRAPH_STICKY_PAGES_BEFORE_RETURN",
        "GRAPH_BATCH_MAX_SIZE",
    ):
        monkeypatch.delenv(k, raising=False)
    from shared import config
    importlib.reload(config)
    s = config.settings
    assert s.GRAPH_APP_PACE_REQS_PER_SEC == 2.5
    assert s.GRAPH_STREAM_PACE_REQS_PER_SEC == 1.0
    assert s.GRAPH_MAX_RETRIES == 5
    assert s.GRAPH_THROTTLE_BACKOFF_SECONDS == [60, 120, 240, 480, 600]
    assert s.GRAPH_TRANSIENT_BACKOFF_SECONDS == [2, 4, 8, 16, 32]
    assert s.GRAPH_JITTER_RATIO == 0.2
    assert s.GRAPH_POST_THROTTLE_BRAKE_MS == 500
    assert s.GRAPH_MAX_CUMULATIVE_WAIT_SECONDS == 14400
    assert s.GRAPH_MAX_THROTTLE_WAIT_SECONDS == 1800
    assert s.GRAPH_STICKY_PAGES_BEFORE_RETURN == 50
    assert s.GRAPH_BATCH_MAX_SIZE == 20


def test_throttle_backoff_parsed_from_csv(monkeypatch):
    monkeypatch.setenv("GRAPH_THROTTLE_BACKOFF_SECONDS", "10,30,90,270,600")
    from shared import config
    importlib.reload(config)
    assert config.settings.GRAPH_THROTTLE_BACKOFF_SECONDS == [10, 30, 90, 270, 600]
