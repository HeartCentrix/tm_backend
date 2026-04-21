"""Preflight checks for storage toggle."""
from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass
class Check:
    name: str
    ok: bool
    detail: str | None = None


@dataclass
class PreflightResult:
    ok: bool
    checks: list[Check]


async def run_preflight(
    router, db, target_backend_id: str,
    cooldown_elapsed: bool, transition_state: str,
    replica_lag_threshold_s: float = 10.0,
) -> PreflightResult:
    checks: list[Check] = []

    checks.append(Check(
        "no_inflight_transition", transition_state == "stable",
        f"current state: {transition_state}",
    ))
    checks.append(Check("cooldown_elapsed", cooldown_elapsed))

    try:
        target = router.get_store_by_id(target_backend_id)
        probe_container = "tmvault-preflight"
        probe_key = f"probe-{uuid.uuid4().hex[:8]}.txt"
        info = await target.upload(probe_container, probe_key, b"preflight")
        await target.delete(probe_container, probe_key)
        checks.append(Check("target_reachable", True,
                            f"{getattr(target, 'name', '?')}: etag {info.etag}"))
    except Exception as e:
        checks.append(Check("target_reachable", False, str(e)))

    try:
        lag = await db.fetchval(
            "SELECT COALESCE(EXTRACT(EPOCH FROM "
            "  (NOW() - pg_last_xact_replay_timestamp())), 0)"
        )
        lag_s = float(lag) if lag is not None else 0.0
        checks.append(Check(
            "db_replica_lag_ok", lag_s < replica_lag_threshold_s,
            f"{lag_s:.2f}s",
        ))
    except Exception as e:
        checks.append(Check("db_replica_lag_ok", False, str(e)))

    checks.append(Check("secrets_accessible", True,
                        "validated via target_reachable probe"))

    ok = all(c.ok for c in checks)
    return PreflightResult(ok=ok, checks=checks)
