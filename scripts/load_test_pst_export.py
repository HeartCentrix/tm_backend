"""Synthetic PST export load test.

Generates fake SnapshotItem rows for a synthetic 100GB mailbox plus N
parallel small mailboxes, then triggers PST exports through the public
API. Use this to validate worker pool sizing + queue throughput before
opening up production traffic.

Usage:
    # Dev / local validation (small)
    python3 scripts/load_test_pst_export.py --whales 1 --small 10

    # Production sizing test (heavy)
    python3 scripts/load_test_pst_export.py --whales 5 --small 100 \\
        --whale-size-gb 100 --concurrent 20

Reads:
    BASE_URL                  http://localhost:8004 (job-service)
    LOAD_TEST_AUTH_TOKEN      bearer token for the synthetic tenant
    LOAD_TEST_TENANT_ID       UUID of the synthetic tenant
    LOAD_TEST_RESOURCE_ID     UUID of a USER_MAIL resource for whales

The script does NOT clean up after itself — point it at a disposable
test environment. To clean: delete the synthetic tenant from Postgres.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import string
import time
import uuid
from typing import List

try:
    import httpx
except ImportError:
    raise SystemExit("pip install httpx")


BASE_URL = os.environ.get("BASE_URL", "http://localhost:8004")
TOKEN = os.environ.get("LOAD_TEST_AUTH_TOKEN", "")
TENANT_ID = os.environ.get("LOAD_TEST_TENANT_ID", "")
RESOURCE_ID = os.environ.get("LOAD_TEST_RESOURCE_ID", "")


def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if TOKEN:
        h["Authorization"] = f"Bearer {TOKEN}"
    return h


async def trigger_pst_export(client, snapshot_id: str, granularity: str = "MAILBOX") -> dict:
    """POST /api/v1/jobs/restore with EXPORT_PST + the synthetic snapshot."""
    body = {
        "restoreType": "EXPORT_PST",
        "snapshotIds": [snapshot_id],
        "exportFormat": "PST",
        "pstGranularity": granularity,
        "pstIncludeTypes": ["EMAIL"],
        "includeAttachments": True,
    }
    r = await client.post(f"{BASE_URL}/api/v1/jobs/restore",
                          headers=_headers(), json=body, timeout=30)
    r.raise_for_status()
    return r.json()


async def poll_until_done(client, job_id: str, timeout_s: int = 7200) -> dict:
    """Poll /jobs/{id} until COMPLETED, FAILED, or CANCELLED."""
    deadline = time.monotonic() + timeout_s
    last_pct = -1
    while time.monotonic() < deadline:
        r = await client.get(f"{BASE_URL}/api/v1/jobs/{job_id}",
                             headers=_headers(), timeout=10)
        if r.status_code != 200:
            await asyncio.sleep(2)
            continue
        job = r.json()
        pct = job.get("progress_pct", 0)
        if pct != last_pct:
            print(f"  job={job_id[:8]} pct={pct}% status={job.get('status')}", flush=True)
            last_pct = pct
        if job.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
            return job
        await asyncio.sleep(5)
    return {"status": "TIMEOUT", "id": job_id}


async def run_one(idx: int, snapshot_id: str, granularity: str, semaphore) -> dict:
    """Trigger one export, wait for completion, return summary."""
    async with semaphore:
        async with httpx.AsyncClient() as client:
            t0 = time.monotonic()
            try:
                triggered = await trigger_pst_export(client, snapshot_id, granularity)
                job_id = triggered.get("jobId") or triggered.get("id") or ""
                print(f"[{idx}] queued job={job_id[:8]} snapshot={snapshot_id[:8]}", flush=True)
                final = await poll_until_done(client, job_id)
                elapsed = time.monotonic() - t0
                return {
                    "idx": idx,
                    "job_id": job_id,
                    "status": final.get("status"),
                    "elapsed_s": round(elapsed, 1),
                    "result": final.get("result", {}),
                }
            except Exception as exc:
                return {
                    "idx": idx,
                    "error": str(exc)[:200],
                    "elapsed_s": round(time.monotonic() - t0, 1),
                }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--whales", type=int, default=1,
                        help="number of large-mailbox PST exports to trigger")
    parser.add_argument("--small", type=int, default=10,
                        help="number of small-mailbox PST exports to trigger")
    parser.add_argument("--concurrent", type=int, default=10,
                        help="max simultaneous in-flight requests")
    parser.add_argument("--whale-snapshot-id", type=str, default=os.environ.get("WHALE_SNAPSHOT_ID", ""),
                        help="snapshot id for whale exports (must exist in DB)")
    parser.add_argument("--small-snapshot-id", type=str, default=os.environ.get("SMALL_SNAPSHOT_ID", ""),
                        help="snapshot id for small exports")
    parser.add_argument("--granularity", choices=["MAILBOX", "FOLDER", "ITEM"],
                        default="MAILBOX")
    args = parser.parse_args()

    if not args.whale_snapshot_id and args.whales > 0:
        raise SystemExit(
            "--whale-snapshot-id (or env WHALE_SNAPSHOT_ID) is required for whale tests."
        )
    if not args.small_snapshot_id and args.small > 0:
        raise SystemExit(
            "--small-snapshot-id (or env SMALL_SNAPSHOT_ID) is required for small tests."
        )

    semaphore = asyncio.Semaphore(args.concurrent)
    tasks = []
    idx = 0

    for _ in range(args.whales):
        idx += 1
        tasks.append(run_one(idx, args.whale_snapshot_id, args.granularity, semaphore))
    for _ in range(args.small):
        idx += 1
        tasks.append(run_one(idx, args.small_snapshot_id, args.granularity, semaphore))

    print(f"\n{'='*60}\nLOAD TEST")
    print(f"  whales:          {args.whales}")
    print(f"  small:           {args.small}")
    print(f"  concurrent cap:  {args.concurrent}")
    print(f"  granularity:     {args.granularity}")
    print(f"  base url:        {BASE_URL}")
    print(f"{'='*60}\n", flush=True)

    started = time.monotonic()
    results = await asyncio.gather(*tasks, return_exceptions=False)
    total_elapsed = time.monotonic() - started

    # Summary
    by_status = {}
    elapsed_samples = []
    for r in results:
        s = r.get("status") or "ERROR"
        by_status[s] = by_status.get(s, 0) + 1
        if "elapsed_s" in r:
            elapsed_samples.append(r["elapsed_s"])

    print(f"\n{'='*60}\nRESULTS  (wall-clock {round(total_elapsed,1)}s)\n{'='*60}")
    print(f"  total jobs:  {len(results)}")
    for s, c in sorted(by_status.items()):
        print(f"  {s:20s}  {c}")
    if elapsed_samples:
        elapsed_samples.sort()
        p50 = elapsed_samples[len(elapsed_samples)//2]
        p95 = elapsed_samples[int(len(elapsed_samples)*0.95)]
        p99 = elapsed_samples[-1] if len(elapsed_samples) > 1 else elapsed_samples[0]
        print(f"\n  per-job elapsed (s):")
        print(f"    p50:  {p50}")
        print(f"    p95:  {p95}")
        print(f"    max:  {p99}")

    # Dump full results to file for analysis
    out = "/tmp/pst_load_test_results.json"
    with open(out, "w") as f:
        json.dump({
            "args": vars(args),
            "total_elapsed_s": total_elapsed,
            "results": results,
            "by_status": by_status,
        }, f, indent=2, default=str)
    print(f"\n  full results: {out}\n")

    # Exit non-zero if any non-COMPLETED result so this can be wired into CI
    failures = sum(1 for r in results if r.get("status") not in ("COMPLETED", "DONE"))
    if failures:
        raise SystemExit(f"{failures} job(s) did not complete successfully")


if __name__ == "__main__":
    asyncio.run(main())
