"""Worker heartbeat — liveness signal for the reconciliation sweeper.

Each worker spawns a single ``HeartbeatThread`` after AMQP connect and
before it starts consuming. The thread UPSERTs a row into
``worker_heartbeats`` every ``HEARTBEAT_INTERVAL_S`` seconds with the
current timestamp. The reconciler in ``shared/reconciler.py`` reads
those rows to decide whether a held lease belongs to a live worker
(safe to leave alone) or a dead one (sweep-eligible).

Stop-on-SIGTERM tombstones the row by writing ``last_seen_at = NOW() -
INTERVAL '1 hour'`` so the sweeper picks up orphans immediately instead
of waiting the full 60 s staleness window.

Design ref: docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md §5.
"""
from __future__ import annotations

import asyncio
import os
import uuid
from typing import Iterable, Optional

from sqlalchemy import text

from shared.database import async_session_factory


HEARTBEAT_INTERVAL_S = int(os.getenv("HEARTBEAT_INTERVAL_S", "10"))


def _resolve_replica_id() -> str:
    """Stable id across worker process restarts inside the same container.

    Railway exposes ``RAILWAY_REPLICA_ID``; the Compose / k8s case sets
    ``HOSTNAME``. Fallback is a fresh UUID — that breaks startup
    reclaim (we won't recognise our own old lease) but keeps the
    process bootable in dev / CI.
    """
    for k in ("RAILWAY_REPLICA_ID", "HOSTNAME", "POD_NAME"):
        v = os.getenv(k)
        if v:
            return v
    return str(uuid.uuid4())


class HeartbeatThread:
    def __init__(
        self,
        *,
        worker_id: str,
        service_name: str,
        queues: Iterable[str] = (),
        version: Optional[str] = None,
        interval_s: int = HEARTBEAT_INTERVAL_S,
    ):
        # worker_id must be a UUID for the PK column; if the caller
        # passed a free-form string (e.g. "worker-3889f71f") we derive
        # a stable UUIDv5 from it so re-runs hit the same row.
        try:
            self.worker_uuid = uuid.UUID(worker_id)
        except (ValueError, TypeError):
            self.worker_uuid = uuid.uuid5(
                uuid.NAMESPACE_OID, f"tmvault-worker:{worker_id}"
            )
        self.worker_label = str(worker_id)
        self.replica_id = _resolve_replica_id()
        self.service_name = service_name
        self.queues = list(queues)
        self.pid = os.getpid()
        self.version = version or os.getenv("RAILWAY_GIT_COMMIT_SHA")
        self.interval_s = interval_s
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def _beat_once(self) -> None:
        async with async_session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO worker_heartbeats
                        (worker_id, replica_id, service_name, pid,
                         queues, last_seen_at, version)
                    VALUES
                        (cast(:wid AS uuid), :rep, :svc, :pid,
                         :queues, NOW(), :ver)
                    ON CONFLICT (worker_id) DO UPDATE
                       SET last_seen_at = NOW(),
                           replica_id   = EXCLUDED.replica_id,
                           service_name = EXCLUDED.service_name,
                           pid          = EXCLUDED.pid,
                           queues       = EXCLUDED.queues,
                           version      = EXCLUDED.version
                    """
                ),
                {
                    "wid": str(self.worker_uuid),
                    "rep": self.replica_id,
                    "svc": self.service_name,
                    "pid": self.pid,
                    "queues": self.queues,
                    "ver": self.version,
                },
            )
            await session.commit()

    async def _loop(self) -> None:
        # First beat immediately so the row exists before any work is
        # claimed under this worker_id.
        try:
            await self._beat_once()
        except Exception as exc:
            print(f"[HEARTBEAT] initial beat failed: {exc}")
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
                break  # _stop was set
            except asyncio.TimeoutError:
                pass
            try:
                await self._beat_once()
            except Exception as exc:
                # Best-effort. A missed beat just shortens our liveness
                # window; the next tick recovers. We do NOT crash the
                # worker on heartbeat failure.
                print(f"[HEARTBEAT] beat failed: {exc}")

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop(), name="heartbeat")
        print(
            f"[HEARTBEAT] started worker={self.worker_label} "
            f"replica={self.replica_id} svc={self.service_name} "
            f"every={self.interval_s}s queues={self.queues}"
        )

    async def stop(self) -> None:
        """Tombstone our row + cancel the loop. Best-effort."""
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        try:
            async with async_session_factory() as session:
                await session.execute(
                    text(
                        """
                        UPDATE worker_heartbeats
                           SET last_seen_at = NOW() - INTERVAL '1 hour'
                         WHERE worker_id = cast(:wid AS uuid)
                        """
                    ),
                    {"wid": str(self.worker_uuid)},
                )
                await session.commit()
        except Exception as exc:
            # Tombstone is best-effort — the staleness window
            # (HEARTBEAT_STALE_S) will catch us anyway.
            print(f"[HEARTBEAT] tombstone failed: {exc}")
