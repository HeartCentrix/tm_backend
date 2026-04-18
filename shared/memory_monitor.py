"""Polls process RSS and triggers a callback when the configured limit is
exceeded for the grace period. Used by mail-export orchestrator to perform a
soft shutdown before Docker OOM-kills the worker mid-commit."""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Optional

import psutil


class MemoryMonitor:
    def __init__(
        self,
        *,
        limit_bytes: int,
        soft_limit_pct: int,
        grace_seconds: float,
        poll_interval_seconds: float,
        on_breach: Callable[[], Awaitable[None]],
    ):
        self._threshold = limit_bytes * soft_limit_pct // 100
        self._grace = grace_seconds
        self._poll = poll_interval_seconds
        self._on_breach = on_breach
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task
            self._task = None

    async def _run(self) -> None:
        proc = psutil.Process()
        over_since: Optional[float] = None
        loop = asyncio.get_event_loop()
        while not self._stop.is_set():
            rss = proc.memory_info().rss
            now = loop.time()
            if rss >= self._threshold:
                if over_since is None:
                    over_since = now
                elif now - over_since >= self._grace:
                    await self._on_breach()
                    return
            else:
                over_since = None
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll)
            except asyncio.TimeoutError:
                pass
