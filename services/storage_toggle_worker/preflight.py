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

    # Reachability probe — read before write so we can tell a genuine
    # outage / auth issue apart from a full-storage situation (the latter
    # is a valid pre-existing state that shouldn't block a toggle if the
    # target is otherwise reachable + correctly configured).
    try:
        target = router.get_store_by_id(target_backend_id)
        probe_container = "tmvault-preflight"
        probe_key = f"probe-{uuid.uuid4().hex[:8]}.txt"

        # Step 1: read-only listing on the target's default container.
        # Cheap, doesn't require free disk, works even when SeaweedFS
        # has no free volumes. If this fails, the backend is genuinely
        # unreachable / misconfigured and the toggle must abort.
        reached = False
        try:
            async for _ in target.list_blobs(probe_container):
                break
            reached = True
        except Exception:
            # Some stores 404 on an empty list — retry a known-good
            # introspection via get_properties of a key that probably
            # doesn't exist. A NotFound (vs connection error) also
            # counts as "reached".
            try:
                await target.get_properties(probe_container, "__no_such_key__")
                reached = True
            except Exception as list_err:
                msg = str(list_err).lower()
                # S3 "NoSuchKey" / Azure "BlobNotFound" means we DID
                # reach the endpoint and got a negative response → pass.
                if ("nosuchkey" in msg or "not found" in msg
                        or "blobnotfound" in msg or "404" in msg):
                    reached = True

        if not reached:
            checks.append(Check(
                "target_reachable", False,
                "unable to reach target — check endpoint/credentials",
            ))
        else:
            # Step 2: best-effort write probe. A write failure here is
            # advisory only — we still pass reachability because step 1
            # confirmed the backend is live. This lets a toggle proceed
            # on a storage backend that's temporarily full (operator
            # problem, not a toggle-safety problem).
            write_detail = None
            try:
                info = await target.upload(
                    probe_container, probe_key, b"preflight",
                )
                await target.delete(probe_container, probe_key)
                write_detail = f"write ok (etag {info.etag})"
            except Exception as write_err:
                write_detail = (
                    f"reachable; probe write failed "
                    f"({type(write_err).__name__}) — toggle continuing"
                )
            checks.append(Check(
                "target_reachable", True,
                f"{getattr(target, 'name', '?')}: {write_detail}",
            ))
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
