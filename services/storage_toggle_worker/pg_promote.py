"""Postgres promote orchestration — pluggable per deployment.

Strategies (PG_PROMOTE_STRATEGY env):
  railway  — no-op (pilot uses logical replication; promote not needed)
  patroni  — POST /failover to PATRONI_TARGET_URL
  manual   — sleep 30s and let a human handle it
  noop     — synonym for railway
"""
from __future__ import annotations

import asyncio
import logging
import os

import aiohttp

log = logging.getLogger("toggle.pg_promote")


async def promote_target(target_kind: str) -> None:
    strategy = os.getenv("PG_PROMOTE_STRATEGY", "noop")
    if strategy in ("railway", "noop"):
        log.info("PG_PROMOTE_STRATEGY=%s — skipping", strategy)
        return
    if strategy == "patroni":
        url = os.getenv("PATRONI_TARGET_URL")
        if not url:
            raise RuntimeError("PATRONI_TARGET_URL required for strategy=patroni")
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{url}/failover", json={"candidate": target_kind}) as r:
                if r.status >= 400:
                    raise RuntimeError(f"patroni failover failed: {await r.text()}")
        log.info("patroni failover request sent")
        return
    if strategy == "manual":
        log.warning("PG_PROMOTE_STRATEGY=manual — waiting 30s for human")
        await asyncio.sleep(30)
        return
    raise ValueError(f"unknown PG_PROMOTE_STRATEGY: {strategy}")
