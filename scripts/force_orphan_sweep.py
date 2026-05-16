#!/usr/bin/env python3
"""One-shot driver for ``shared.reconciler.sweep_orphans``.

Useful when:
  * a redeploy has left orphan rows and you want to close them
    immediately rather than wait for the next 60 s sweeper tick
  * verifying the sweep in dry-run mode before turning the loop on
    (set ``RECONCILER_DRY_RUN=true``)

Usage::

    DATABASE_URL=postgresql://... python -m scripts.force_orphan_sweep

After successful sweep, also cascades to ``_finalize_batch_if_complete``
for any backup_batches still IN_PROGRESS, so the Activity feed flips
to Done on the same call.
"""
from __future__ import annotations

import asyncio
import sys
from sqlalchemy import text


async def main() -> int:
    from shared.database import async_session_factory
    from shared.reconciler import sweep_orphans, republish_partition_messages
    from shared.batch_rollup import _finalize_batch_if_complete
    try:
        from shared.message_bus import message_bus
    except Exception:
        message_bus = None  # CLI fallback — partitions won't be re-published

    async with async_session_factory() as session:
        stats = await sweep_orphans(session)

    print(
        f"swept: parts_requeue={stats.partitions_requeued} "
        f"parts_dlq={stats.partitions_dlq} "
        f"snaps_final={stats.snapshots_finalized} "
        f"jobs_final={stats.jobs_finalized}"
    )

    if stats.requeue_payloads and message_bus is not None:
        try:
            await message_bus.connect()
            n = await republish_partition_messages(stats, message_bus=message_bus)
            print(f"republished {n} partition messages")
        except Exception as exc:
            print(f"republish failed: {exc}")
        finally:
            try:
                await message_bus.disconnect()
            except Exception:
                pass

    if stats.snapshots_finalized or stats.jobs_finalized:
        async with async_session_factory() as session:
            rows = (await session.execute(text(
                "SELECT id FROM backup_batches WHERE status='IN_PROGRESS' "
                "ORDER BY created_at DESC LIMIT 100"
            ))).all()
            closed = 0
            for row in rows:
                ns = await _finalize_batch_if_complete(row.id, session)
                if ns:
                    closed += 1
            print(f"batch finalize cascade closed={closed} of {len(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
