#!/usr/bin/env python3
"""Smoke script for a newly-provisioned SeaweedFS cluster (Railway pilot
or client DC).

Runs: upload → download → roundtrip check → retention → delete-attempt
(should fail with ImmutableBlobError while lock is active) → legal hold
apply/remove.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from datetime import datetime, timedelta, timezone

from shared.storage.errors import ImmutableBlobError
from shared.storage.seaweedfs import SeaweedStore


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--verify-tls", action="store_true")
    args = ap.parse_args()

    s = SeaweedStore(
        backend_id="smoke", name="smoke",
        endpoint=args.endpoint,
        access_key=args.access_key, secret_key=args.secret_key,
        buckets=[args.bucket], verify_tls=args.verify_tls,
    ).shard_for("smoke-tenant", "smoke-res")

    container = "smoke"
    key = f"smoke-{uuid.uuid4().hex[:8]}.txt"
    body = b"pilot smoke ok"
    exit_code = 0
    try:
        print("1. upload ...")
        info = await s.upload(container, key, body, metadata={"kind": "smoke"})
        assert info.size == len(body)
        print("   ok size=", info.size)

        print("2. download ...")
        got = await s.download(container, key)
        assert got == body
        print("   ok")

        print("3. apply retention (1 day, Unlocked/GOVERNANCE) ...")
        until = datetime.now(timezone.utc) + timedelta(days=1)
        await s.apply_immutability(container, key, until, mode="Unlocked")
        print("   ok until=", until.isoformat())

        print("4. delete attempt (should raise ImmutableBlobError) ...")
        try:
            await s.delete(container, key)
            print("   !!! delete unexpectedly succeeded — WORM not enforced !!!",
                  file=sys.stderr)
            exit_code = 1
        except ImmutableBlobError:
            print("   ok — WORM correctly blocked delete")

        print("5. apply legal hold ...")
        await s.apply_legal_hold(container, key)
        print("   ok")

        print("6. remove legal hold ...")
        await s.remove_legal_hold(container, key)
        print("   ok")

        print("smoke complete — blob stays until retention expires.")
    finally:
        await s.close()
    return exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
