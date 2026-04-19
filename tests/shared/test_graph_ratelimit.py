"""RateLimitPolicy: Retry-After parsing, jitter, backoff walker, cumulative cap."""
import time

import pytest

from shared.graph_ratelimit import (
    parse_retry_after,
    jittered,
    BackoffWalker,
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


def test_walker_walks_sequence():
    w = BackoffWalker([60, 120, 240, 480, 600], jitter_ratio=0.0)
    assert w.next() == 60
    assert w.next() == 120
    assert w.next() == 240
    assert w.next() == 480
    assert w.next() == 600


def test_walker_loops_back_when_exhausted():
    w = BackoffWalker([10, 20, 30], jitter_ratio=0.0)
    assert [w.next() for _ in range(6)] == [10, 20, 30, 10, 20, 30]


def test_walker_jitter_range():
    w = BackoffWalker([100], jitter_ratio=0.2)
    for _ in range(50):
        v = w.next()
        assert 80.0 <= v <= 120.0


def test_walker_cumulative_wait():
    w = BackoffWalker([10, 20, 30], jitter_ratio=0.0)
    w.next(); w.next(); w.next()
    assert w.cumulative_wait() == 60.0


def test_walker_exceeded_cap():
    w = BackoffWalker([10], jitter_ratio=0.0)
    for _ in range(5):
        w.next()
    assert w.exceeded_cumulative_cap(40) is True
    assert w.exceeded_cumulative_cap(60) is False


import asyncio


@pytest.mark.asyncio
async def test_bucket_admits_first_request_immediately():
    from shared.graph_ratelimit import AsyncTokenBucket
    b = AsyncTokenBucket(rate_per_sec=1.0, capacity=1)
    t0 = asyncio.get_event_loop().time()
    await b.acquire()
    elapsed = asyncio.get_event_loop().time() - t0
    assert elapsed < 0.05


@pytest.mark.asyncio
async def test_bucket_paces_second_request():
    from shared.graph_ratelimit import AsyncTokenBucket
    b = AsyncTokenBucket(rate_per_sec=5.0, capacity=1)
    await b.acquire()
    t0 = asyncio.get_event_loop().time()
    await b.acquire()
    elapsed = asyncio.get_event_loop().time() - t0
    assert 0.15 < elapsed < 0.35


@pytest.mark.asyncio
async def test_bucket_burst_up_to_capacity():
    from shared.graph_ratelimit import AsyncTokenBucket
    b = AsyncTokenBucket(rate_per_sec=1.0, capacity=3)
    t0 = asyncio.get_event_loop().time()
    for _ in range(3):
        await b.acquire()
    assert asyncio.get_event_loop().time() - t0 < 0.05


@pytest.mark.asyncio
async def test_bucket_rate_zero_disables_pacing():
    from shared.graph_ratelimit import AsyncTokenBucket
    b = AsyncTokenBucket(rate_per_sec=0.0, capacity=1)
    t0 = asyncio.get_event_loop().time()
    for _ in range(20):
        await b.acquire()
    assert asyncio.get_event_loop().time() - t0 < 0.05


@pytest.mark.asyncio
async def test_policy_decide_throttle_with_retry_after():
    from shared.graph_ratelimit import RateLimitPolicy
    p = RateLimitPolicy(
        stream_rate=0.0, app_rate=0.0,
        throttle_sequence=[60], transient_sequence=[1],
        jitter_ratio=0.0, cumulative_cap_s=10_000,
    )
    action = p.decide(status_code=429, retry_after="5")
    assert action.should_sleep is True
    assert action.sleep_seconds == 5.0
    assert action.exhausted is False


@pytest.mark.asyncio
async def test_policy_decide_throttle_without_retry_after():
    from shared.graph_ratelimit import RateLimitPolicy
    p = RateLimitPolicy(
        stream_rate=0.0, app_rate=0.0,
        throttle_sequence=[60, 120], transient_sequence=[1],
        jitter_ratio=0.0, cumulative_cap_s=10_000,
    )
    a1 = p.decide(status_code=429, retry_after=None)
    a2 = p.decide(status_code=429, retry_after=None)
    assert a1.sleep_seconds == 60.0
    assert a2.sleep_seconds == 120.0


@pytest.mark.asyncio
async def test_policy_decide_raises_on_cumulative_cap():
    from shared.graph_ratelimit import RateLimitPolicy
    p = RateLimitPolicy(
        stream_rate=0.0, app_rate=0.0,
        throttle_sequence=[60], transient_sequence=[1],
        jitter_ratio=0.0, cumulative_cap_s=30,
    )
    p.decide(status_code=429, retry_after=None)
    action = p.decide(status_code=429, retry_after=None)
    assert action.exhausted is True


@pytest.mark.asyncio
async def test_policy_decide_timeout_uses_transient_sequence():
    from shared.graph_ratelimit import RateLimitPolicy
    p = RateLimitPolicy(
        stream_rate=0.0, app_rate=0.0,
        throttle_sequence=[60], transient_sequence=[2, 4, 8],
        jitter_ratio=0.0, cumulative_cap_s=10_000,
    )
    a = p.decide_transient_error()
    assert a.sleep_seconds == 2.0
    a = p.decide_transient_error()
    assert a.sleep_seconds == 4.0
