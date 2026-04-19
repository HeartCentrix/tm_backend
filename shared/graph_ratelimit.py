"""Graph API rate-limit policy — central retry / backoff / pacing brain.

One `RateLimitPolicy` instance per GraphClient. Owns the decision of how
long to sleep after a 429/503 and when to give up and raise so the
RabbitMQ redeliver path can resume.

Spec: docs/superpowers/specs/2026-04-19-graph-api-throttle-hardening-design.md
"""
from __future__ import annotations

import asyncio
import email.utils
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional


def parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    """Parse a Retry-After header value (seconds-int OR HTTP-date).

    Returns seconds-to-wait as float, or None if the value is missing /
    unparseable. Microsoft Graph always sends seconds today, but the HTTP
    spec allows dates — we handle both so a future format change doesn't
    silently crash the worker.
    """
    if header_value is None:
        return None
    raw = header_value.strip()
    if not raw:
        return None
    try:
        return float(int(raw))
    except ValueError:
        pass
    # Try HTTP-date.
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt is None:
            return None
        delta = dt.timestamp() - time.time()
        return max(delta, 0.0)
    except (TypeError, ValueError):
        return None


def jittered(base_seconds: float, ratio: float) -> float:
    """Apply uniform ±ratio jitter.

    Result is `base * uniform(1 - ratio, 1 + ratio)`. Caps at 0 so we never
    return negative sleep.
    """
    if ratio <= 0:
        return base_seconds
    factor = 1.0 + random.uniform(-ratio, ratio)
    return max(base_seconds * factor, 0.0)


@dataclass
class BackoffWalker:
    """Walks a configured sleep-sequence, tracks cumulative wait, loops on exhaustion.

    Used when 429/503 arrives without a Retry-After header. Each call to
    `.next()` returns the next jittered sleep duration and records it so
    callers can check `.exceeded_cumulative_cap()` before sleeping again.
    """

    sequence: List[int]
    jitter_ratio: float = 0.2
    _index: int = field(default=0, init=False)
    _cumulative: float = field(default=0.0, init=False)

    def next(self) -> float:
        if not self.sequence:
            return 0.0
        base = float(self.sequence[self._index % len(self.sequence)])
        self._index += 1
        wait = jittered(base, self.jitter_ratio)
        self._cumulative += wait
        return wait

    def cumulative_wait(self) -> float:
        return self._cumulative

    def exceeded_cumulative_cap(self, cap_seconds: float) -> bool:
        return self._cumulative >= cap_seconds

    def reset(self) -> None:
        self._index = 0
        self._cumulative = 0.0
