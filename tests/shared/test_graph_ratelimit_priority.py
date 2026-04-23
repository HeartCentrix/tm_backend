"""Priority-aware AsyncTokenBucket tests.

Verifies that HIGH/URGENT callers wake up ahead of NORMAL callers
waiting on the same contended bucket. Backwards compatibility is
covered by the default-priority path matching pre-priority behaviour
exactly (priority=0 is identical to omitting the kwarg).
"""
from __future__ import annotations

import asyncio
import time

import pytest

from shared.graph_ratelimit import AsyncTokenBucket
from shared.graph_priority import (
    PRIORITY_NORMAL,
    PRIORITY_HIGH,
    PRIORITY_URGENT,
    priority_for_queue,
)


@pytest.mark.asyncio
async def test_priority_default_is_backward_compatible():
    """priority=0 must behave exactly like no priority arg — single-caller
    uncontended bucket returns immediately."""
    bucket = AsyncTokenBucket(rate_per_sec=10.0, capacity=1)
    start = time.monotonic()
    await bucket.acquire()
    await bucket.acquire(priority=0)
    elapsed = time.monotonic() - start
    # First call is a free pre-filled token; second needs one refill
    # (~0.1s at 10 rps). Well under the uncontended timeout.
    assert elapsed < 0.3


@pytest.mark.asyncio
async def test_high_priority_wins_contention():
    """When NORMAL and HIGH are both waiting on an empty bucket, HIGH
    acquires the next refilled token first."""
    bucket = AsyncTokenBucket(rate_per_sec=2.0, capacity=1)
    # Drain the initial free token so both callers start contending
    await bucket.acquire()

    completion_order: list[str] = []

    async def normal_caller():
        await bucket.acquire(priority=PRIORITY_NORMAL)
        completion_order.append("normal")

    async def high_caller():
        # Start slightly later so NORMAL registers its sleep first —
        # this is the adversarial case: NORMAL "got there first" but
        # HIGH must still win.
        await asyncio.sleep(0.05)
        await bucket.acquire(priority=PRIORITY_HIGH)
        completion_order.append("high")

    await asyncio.gather(normal_caller(), high_caller())
    assert completion_order[0] == "high", (
        f"HIGH priority should complete first, got {completion_order}"
    )


@pytest.mark.asyncio
async def test_urgent_beats_high():
    """URGENT (2) wakes up sooner than HIGH (1) under contention."""
    bucket = AsyncTokenBucket(rate_per_sec=2.0, capacity=1)
    await bucket.acquire()

    completion_order: list[str] = []

    async def high_caller():
        await bucket.acquire(priority=PRIORITY_HIGH)
        completion_order.append("high")

    async def urgent_caller():
        await asyncio.sleep(0.05)
        await bucket.acquire(priority=PRIORITY_URGENT)
        completion_order.append("urgent")

    await asyncio.gather(high_caller(), urgent_caller())
    assert completion_order[0] == "urgent", (
        f"URGENT priority should complete first, got {completion_order}"
    )


@pytest.mark.asyncio
async def test_rate_zero_ignores_priority():
    """Kill-switch (rate=0) bypasses the bucket entirely — priority
    must not cause any wait or error."""
    bucket = AsyncTokenBucket(rate_per_sec=0.0, capacity=1)
    start = time.monotonic()
    await bucket.acquire(priority=PRIORITY_URGENT)
    await bucket.acquire(priority=PRIORITY_HIGH)
    await bucket.acquire(priority=PRIORITY_NORMAL)
    elapsed = time.monotonic() - start
    assert elapsed < 0.05


@pytest.mark.asyncio
async def test_negative_priority_treated_as_normal():
    """Negative priority must not crash or misbehave (defensive
    programming — caller might pass -1 for LOW in the future)."""
    bucket = AsyncTokenBucket(rate_per_sec=10.0, capacity=1)
    await bucket.acquire(priority=-5)  # must not raise


def test_queue_priority_mapping():
    """Queue → priority map matches the architectural design."""
    assert priority_for_queue("restore.urgent") == PRIORITY_URGENT
    assert priority_for_queue("backup.urgent") == PRIORITY_HIGH
    assert priority_for_queue("backup.normal") == PRIORITY_NORMAL
    assert priority_for_queue("discovery.m365") == PRIORITY_HIGH
    assert priority_for_queue("export.normal") == PRIORITY_HIGH
    # Unknown queue defaults safe (NORMAL)
    assert priority_for_queue("nonexistent.queue") == PRIORITY_NORMAL
    assert priority_for_queue(None) == PRIORITY_NORMAL
    assert priority_for_queue("") == PRIORITY_NORMAL
