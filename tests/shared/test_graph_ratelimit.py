"""RateLimitPolicy: Retry-After parsing, jitter, backoff walker, cumulative cap."""
import time

import pytest

from shared.graph_ratelimit import (
    parse_retry_after,
    jittered,
)


def test_parse_retry_after_int_seconds():
    assert parse_retry_after("60") == 60.0
    assert parse_retry_after("0") == 0.0
    assert parse_retry_after("  300  ") == 300.0


def test_parse_retry_after_http_date(monkeypatch):
    fixed = 1_700_000_000.0
    monkeypatch.setattr("time.time", lambda: fixed)
    future_epoch = fixed + 90
    import email.utils
    header = email.utils.formatdate(future_epoch, usegmt=True)
    assert abs(parse_retry_after(header) - 90.0) < 1.5


def test_parse_retry_after_missing_returns_none():
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None
    assert parse_retry_after("garbage-not-a-date") is None


def test_jittered_ratio_zero_is_exact():
    assert jittered(100.0, ratio=0.0) == 100.0


def test_jittered_ratio_bounds():
    for _ in range(50):
        v = jittered(100.0, ratio=0.2)
        assert 80.0 <= v <= 120.0
