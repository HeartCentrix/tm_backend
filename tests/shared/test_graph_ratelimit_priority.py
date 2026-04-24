"""Priority-aware AsyncTokenBucket tests.

Verifies that HIGH/URGENT callers jump ahead of NORMAL callers
waiting on the same contended bucket. Tests are event-driven, not
timing-driven — they use asyncio.Event to guarantee the lower-priority
caller has registered as a waiter BEFORE the higher-priority caller
attempts to acquire. This makes the ordering invariant a pure
correctness check, not a race against the scheduler.

Backwards compatibility: priority=0 (default, kwarg omitted) behaves
identically to pre-priority code for single-priority workloads.
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
    uncontended bucket returns within one refill period."""
    bucket = AsyncTokenBucket(rate_per_sec=10.0, capacity=1)
    start = time.monotonic()
    await bucket.acquire()
    await bucket.acquire(priority=0)
    elapsed = time.monotonic() - start
    # First call is a free pre-filled token; second needs one refill
    # (~0.1s at 10 rps). Well under the uncontended timeout.
    assert elapsed < 0.3


async def _wait_until_n_waiters(bucket: AsyncTokenBucket, n: int, timeout: float = 1.0) -> None:
    """Spin until the bucket has at least `n` parked waiters. Avoids
    timing-based sleeps in the tests — gives a deterministic barrier
    instead of 'sleep 50ms and hope'."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if sum(bucket._waiting.values()) >= n:  # type: ignore[attr-defined]
            return
        await asyncio.sleep(0.001)
    raise AssertionError(
        f"Timed out waiting for {n} parked waiters "
        f"(saw {sum(bucket._waiting.values())})"  # type: ignore[attr-defined]
    )


@pytest.mark.asyncio
async def test_high_priority_wins_contention():
    """When NORMAL and HIGH are both parked on an empty bucket, HIGH
    acquires the next refilled token first — regardless of which
    registered as a waiter first."""
    bucket = AsyncTokenBucket(rate_per_sec=2.0, capacity=1)
    # Drain the initial free token so both callers park on the bucket.
    await bucket.acquire()

    completion_order: list[str] = []
    normal_started = asyncio.Event()

    async def normal_caller():
        normal_started.set()
        await bucket.acquire(priority=PRIORITY_NORMAL)
        completion_order.append("normal")

    async def high_caller():
        # Deterministic barrier: wait until NORMAL is PARKED on the
        # bucket before HIGH attempts acquire. This is the adversarial
        # case (NORMAL "got there first") but HIGH must still win.
        await normal_started.wait()
        await _wait_until_n_waiters(bucket, 1)
        await bucket.acquire(priority=PRIORITY_HIGH)
        completion_order.append("high")

    await asyncio.gather(normal_caller(), high_caller())
    assert completion_order == ["high", "normal"], (
        f"HIGH must complete before NORMAL; got {completion_order}"
    )


@pytest.mark.asyncio
async def test_urgent_beats_high():
    """URGENT jumps ahead of HIGH deterministically. Uses event-based
    waiter-count barriers instead of timing — this is a correctness
    check, not a race against the scheduler."""
    bucket = AsyncTokenBucket(rate_per_sec=2.0, capacity=1)
    await bucket.acquire()

    completion_order: list[str] = []
    high_started = asyncio.Event()

    async def high_caller():
        high_started.set()
        await bucket.acquire(priority=PRIORITY_HIGH)
        completion_order.append("high")

    async def urgent_caller():
        # Wait until HIGH is definitively parked before URGENT arrives.
        await high_started.wait()
        await _wait_until_n_waiters(bucket, 1)
        await bucket.acquire(priority=PRIORITY_URGENT)
        completion_order.append("urgent")

    await asyncio.gather(high_caller(), urgent_caller())
    assert completion_order == ["urgent", "high"], (
        f"URGENT must complete before HIGH; got {completion_order}"
    )


@pytest.mark.asyncio
async def test_three_way_priority_ordering():
    """NORMAL + HIGH + URGENT all contending → served URGENT, HIGH, NORMAL."""
    bucket = AsyncTokenBucket(rate_per_sec=2.0, capacity=1)
    await bucket.acquire()

    completion_order: list[str] = []
    normal_started = asyncio.Event()
    high_started = asyncio.Event()

    async def normal_caller():
        normal_started.set()
        await bucket.acquire(priority=PRIORITY_NORMAL)
        completion_order.append("normal")

    async def high_caller():
        await normal_started.wait()
        await _wait_until_n_waiters(bucket, 1)
        high_started.set()
        await bucket.acquire(priority=PRIORITY_HIGH)
        completion_order.append("high")

    async def urgent_caller():
        await high_started.wait()
        await _wait_until_n_waiters(bucket, 2)
        await bucket.acquire(priority=PRIORITY_URGENT)
        completion_order.append("urgent")

    await asyncio.gather(normal_caller(), high_caller(), urgent_caller())
    assert completion_order == ["urgent", "high", "normal"], (
        f"Served order should be URGENT → HIGH → NORMAL; got {completion_order}"
    )


@pytest.mark.asyncio
async def test_same_priority_callers_share_fairly():
    """Two HIGH callers parked simultaneously — neither starves."""
    bucket = AsyncTokenBucket(rate_per_sec=5.0, capacity=1)
    await bucket.acquire()

    completion_order: list[str] = []

    async def high_a():
        await bucket.acquire(priority=PRIORITY_HIGH)
        completion_order.append("a")

    async def high_b():
        await bucket.acquire(priority=PRIORITY_HIGH)
        completion_order.append("b")

    await asyncio.gather(high_a(), high_b())
    # Both complete; order is scheduler-dependent but both must run.
    assert set(completion_order) == {"a", "b"}
    assert len(completion_order) == 2


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
    """Negative priority is clamped to 0 — must not crash or misbehave
    (defensive programming against callers passing -1 for LOW)."""
    bucket = AsyncTokenBucket(rate_per_sec=10.0, capacity=1)
    await bucket.acquire(priority=-5)  # must not raise


@pytest.mark.asyncio
async def test_deregister_on_exception():
    """If a caller's task is cancelled mid-acquire, its waiter slot
    must be released so subsequent callers don't deadlock on a
    phantom 'higher-priority waiter'."""
    bucket = AsyncTokenBucket(rate_per_sec=1.0, capacity=1)
    await bucket.acquire()  # drain

    async def doomed():
        await bucket.acquire(priority=PRIORITY_URGENT)

    t = asyncio.create_task(doomed())
    await _wait_until_n_waiters(bucket, 1)
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass

    # Waiter slot must have been released (finally block in acquire).
    assert bucket._waiting == {}, (  # type: ignore[attr-defined]
        f"Cancelled caller left phantom waiter: {bucket._waiting}"  # type: ignore[attr-defined]
    )
    # Next caller must proceed without blocking on phantom URGENT.
    start = time.monotonic()
    await bucket.acquire(priority=PRIORITY_NORMAL)
    assert time.monotonic() - start < 2.0


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
