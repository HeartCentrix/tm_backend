"""Reconciler sweep — bottom-up orphan finalization.

Runs once per tick (default every 60 s) in Backup_scheduler. Walks the
work-row tree (partitions → snapshots → jobs) and finalizes any row
that is "orphaned": stuck in a non-terminal status, with no live
worker holding the lease, and whose children are all terminal.

Layer ordering matters. We sweep bottom-up so a parent row never
finalizes before its children — otherwise the parent might pick
COMPLETED while a still-pending child would later flip to FAILED, and
we'd have to re-open the parent (which is out of scope).

Design ref: docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md §7.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

from sqlalchemy import text


RECONCILER_BATCH = int(os.getenv("RECONCILER_BATCH", "200"))
RECONCILER_MAX_REQUEUE = int(os.getenv("RECONCILER_MAX_REQUEUE", "3"))
HEARTBEAT_STALE_S = int(os.getenv("HEARTBEAT_STALE_S", "60"))
# Age fallback for legacy rows that don't have leases stamped yet.
# Once every writer is lease-aware this becomes belt-and-braces, but
# it's the primary path on the day this PR lands (current in-flight
# data has lease_owner_id IS NULL on rows already RUNNING).
JOB_LEGACY_AGE_S = int(os.getenv("RECONCILER_JOB_LEGACY_AGE_S", "900"))   # 15 min
SNAP_LEGACY_AGE_S = int(os.getenv("RECONCILER_SNAP_LEGACY_AGE_S", "900"))  # 15 min
DRY_RUN = os.getenv("RECONCILER_DRY_RUN", "false").lower() in ("true", "1", "yes")


@dataclass
class SweepStats:
    partitions_requeued: int = 0
    partitions_dlq: int = 0
    snapshots_finalized: int = 0
    jobs_finalized: int = 0
    requeue_payloads: List[dict] = field(default_factory=list)

    def total(self) -> int:
        return (
            self.partitions_requeued
            + self.partitions_dlq
            + self.snapshots_finalized
            + self.jobs_finalized
        )


async def sweep_orphans(session) -> SweepStats:
    """Single sweep tick. Caller owns the session + commit timing."""
    stats = SweepStats()

    # ------------------------------------------------------------------
    # STEP A — orphan partitions
    # ------------------------------------------------------------------
    # Re-publish partition messages whose owning worker is dead. Cap
    # at RECONCILER_MAX_REQUEUE → after that, mark FAILED + DLQ.
    requeue_rows = (
        await session.execute(
            text(
                """
                WITH stale AS (
                    SELECT sp.id, sp.snapshot_id, sp.partition_type,
                           sp.partition_index, sp.drive_id, sp.file_ids,
                           sp.payload, sp.requeue_count
                      FROM snapshot_partitions sp
                      LEFT JOIN worker_heartbeats wh
                             ON wh.worker_id = sp.lease_owner_id
                            AND wh.last_seen_at > NOW() - (:stale_s * INTERVAL '1 second')
                     WHERE sp.status IN ('QUEUED','IN_PROGRESS')
                       AND wh.worker_id IS NULL
                       AND (
                            sp.lease_expires_at IS NULL
                         OR sp.lease_expires_at < NOW()
                       )
                       AND sp.enqueued_at < NOW() - INTERVAL '5 minutes'
                       AND sp.requeue_count < :max_rq
                     ORDER BY sp.enqueued_at
                     LIMIT :batch
                     FOR UPDATE OF sp SKIP LOCKED
                )
                UPDATE snapshot_partitions sp
                   SET status         = 'QUEUED',
                       lease_owner_id = NULL,
                       lease_expires_at = NULL,
                       lease_token    = sp.lease_token + 1,
                       requeue_count  = sp.requeue_count + 1
                  FROM stale
                 WHERE sp.id = stale.id
                RETURNING sp.id, sp.snapshot_id, sp.partition_type,
                          sp.partition_index, sp.drive_id, sp.file_ids,
                          sp.payload, sp.requeue_count
                """
            ),
            {
                "stale_s": HEARTBEAT_STALE_S,
                "max_rq": RECONCILER_MAX_REQUEUE,
                "batch": RECONCILER_BATCH,
            },
        )
    ).all()
    for r in requeue_rows:
        stats.requeue_payloads.append(
            {
                "kind": "partition",
                "id": str(r.id),
                "snapshot_id": str(r.snapshot_id),
                "partition_type": r.partition_type,
                "partition_index": r.partition_index,
                "drive_id": r.drive_id,
                "file_ids": r.file_ids,
                "payload": r.payload,
            }
        )
    stats.partitions_requeued = len(requeue_rows)

    # Partitions that exceeded the requeue cap → mark FAILED + DLQ.
    poison_rows = (
        await session.execute(
            text(
                """
                UPDATE snapshot_partitions
                   SET status = 'FAILED',
                       lease_owner_id = NULL,
                       lease_expires_at = NULL,
                       lease_token = lease_token + 1
                 WHERE status IN ('QUEUED','IN_PROGRESS')
                   AND requeue_count >= :max_rq
                   AND (lease_expires_at IS NULL OR lease_expires_at < NOW())
                RETURNING id
                """
            ),
            {"max_rq": RECONCILER_MAX_REQUEUE},
        )
    ).all()
    for r in poison_rows:
        await session.execute(
            text(
                """
                INSERT INTO work_dead_letter (work_kind, work_id, reason)
                VALUES ('partition', cast(:wid AS uuid), 'poison_pill')
                """
            ),
            {"wid": str(r.id)},
        )
    stats.partitions_dlq = len(poison_rows)

    # ------------------------------------------------------------------
    # STEP B — orphan snapshots
    # ------------------------------------------------------------------
    # A snapshot is orphan when:
    #   - status = IN_PROGRESS
    #   - EITHER no live worker owns the lease (lease expired / legacy
    #     age exceeded / no live heartbeat), OR all of its partitions
    #     have already reached terminal — in which case no worker
    #     handler is going to advance it (in particular, partitions
    #     marked FAILED by STEP A's poison-pill branch never get a
    #     finalize call). The fast-path on all-terminal-partitions
    #     prevents 5k-user runs from waiting out the full lease TTL
    #     after the reconciler itself poisons the last partition.
    #   - no in-flight partitions remain
    snap_rows = (
        await session.execute(
            text(
                """
                -- snapshot_partitions.status is plain VARCHAR (not an
                -- enum) — the publisher writes 'QUEUED', workers flip
                -- to 'IN_PROGRESS', then 'COMPLETED'/'FAILED'. No
                -- 'CANCELLED' value is produced today, so we don't
                -- match on it. (See shared/models.py SnapshotPartition.)
                -- snapshotstatus enum: IN_PROGRESS, COMPLETED, FAILED,
                -- PARTIAL, PENDING_DELETION. The CASE only produces
                -- values that exist on snapshotstatus.
                UPDATE snapshots s
                   SET status = CASE
                           WHEN NOT EXISTS (
                               SELECT 1 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'COMPLETED'::snapshotstatus
                           WHEN (
                               SELECT bool_and(sp.status = 'COMPLETED')
                                 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'COMPLETED'::snapshotstatus
                           WHEN (
                               SELECT bool_and(sp.status = 'FAILED')
                                 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'FAILED'::snapshotstatus
                           WHEN (
                               SELECT bool_or(sp.status = 'FAILED')
                                 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'PARTIAL'::snapshotstatus
                           ELSE 'FAILED'::snapshotstatus
                       END,
                       completed_at     = NOW(),
                       lease_owner_id   = NULL,
                       lease_expires_at = NULL,
                       lease_token      = s.lease_token + 1
                 WHERE s.status = 'IN_PROGRESS'::snapshotstatus
                   AND NOT EXISTS (
                       SELECT 1 FROM snapshot_partitions sp
                        WHERE sp.snapshot_id = s.id
                          AND sp.status IN ('QUEUED','IN_PROGRESS')
                   )
                   AND (
                       -- Fast path: snapshot is partitioned AND all
                       -- partitions terminal. Skip lease check — no
                       -- worker handler will call finalize on a
                       -- partition that the reconciler itself moved
                       -- to FAILED via poison-pill (STEP A above).
                       EXISTS (
                           SELECT 1 FROM snapshot_partitions sp
                            WHERE sp.snapshot_id = s.id
                       )
                       OR (
                           -- Slow path (non-partitioned snapshot):
                           -- require lease/age based staleness so we
                           -- don't race a live worker.
                           (
                               (s.lease_expires_at IS NOT NULL
                                AND s.lease_expires_at < NOW())
                               OR (s.lease_owner_id IS NULL
                                   AND s.started_at <
                                       NOW() - (:legacy_s * INTERVAL '1 second'))
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM worker_heartbeats wh
                                WHERE wh.worker_id = s.lease_owner_id
                                  AND wh.last_seen_at >
                                      NOW() - (:stale_s * INTERVAL '1 second')
                           )
                       )
                   )
                RETURNING s.id, s.status::text
                """
            ),
            {
                "stale_s": HEARTBEAT_STALE_S,
                "legacy_s": SNAP_LEGACY_AGE_S,
            },
        )
    ).all()
    stats.snapshots_finalized = len(snap_rows)

    # ------------------------------------------------------------------
    # STEP C — orphan jobs
    # ------------------------------------------------------------------
    # Resolve status from leaf snapshots. Use bool_or for PARTIAL/FAILED
    # detection so a single bad child taints the parent appropriately.
    job_rows = (
        await session.execute(
            text(
                """
                -- jobstatus enum values: QUEUED, PENDING, RUNNING,
                -- COMPLETED, FAILED, CANCELLED, CANCELLING, RETRYING.
                -- *No* PARTIAL — that lives on snapshotstatus only.
                -- Mixed-outcome roll-up is the batch row's job (see
                -- shared.batch_rollup.derive_batch_status), so at the
                -- job level we collapse to:
                --   all snapshots COMPLETED               → COMPLETED
                --   at least one COMPLETED + others mixed → COMPLETED
                --   zero COMPLETED (all PARTIAL/FAILED)   → FAILED
                -- snapshotstatus enum: IN_PROGRESS, COMPLETED, FAILED,
                -- PARTIAL, PENDING_DELETION. No QUEUED, no CANCELLED.
                -- Lease check intentionally dropped: the outer WHERE
                -- already requires every child snapshot to be terminal,
                -- so there is no live work for the lease holder to do
                -- on this job. The worker that completed the last
                -- snapshot normally calls _finalize_bulk_parent_if_complete;
                -- this branch is the safety net for when STEP B above
                -- flipped a partitioned snapshot terminal on the
                -- reconciler's behalf and no worker was left to bubble
                -- up to the job. (Without this, a poisoned-partition
                -- batch parent would wait out the full lease TTL at
                -- 5k-user scale.)
                UPDATE jobs j
                   SET status = CASE
                           WHEN EXISTS (
                               SELECT 1 FROM snapshots s
                                WHERE s.job_id = j.id
                                  AND s.status = 'COMPLETED'::snapshotstatus
                           ) THEN 'COMPLETED'::jobstatus
                           ELSE 'FAILED'::jobstatus
                       END,
                       completed_at     = NOW(),
                       lease_owner_id   = NULL,
                       lease_expires_at = NULL,
                       lease_token      = j.lease_token + 1
                 WHERE j.status = 'RUNNING'::jobstatus
                   AND NOT EXISTS (
                       SELECT 1 FROM snapshots s
                        WHERE s.job_id = j.id
                          AND s.status = 'IN_PROGRESS'::snapshotstatus
                   )
                   AND EXISTS (
                       SELECT 1 FROM snapshots s WHERE s.job_id = j.id
                   )
                RETURNING j.id, j.status::text
                """
            ),
            {
                "stale_s": HEARTBEAT_STALE_S,
                "legacy_s": JOB_LEGACY_AGE_S,
            },
        )
    ).all()
    stats.jobs_finalized = len(job_rows)

    # ------------------------------------------------------------------
    # STEP D — terminal-snapshot integrity sweep
    # ------------------------------------------------------------------
    # Catches snapshots that were flipped COMPLETED via a path that
    # observed all-COMPLETED partitions at the moment of the CTE — but
    # whose partitions later regressed (e.g. via a race between the
    # finalize CTE read and a concurrent _mark_partition_terminal
    # commit; or via STEP A's poison-pill which marks late partitions
    # FAILED after the parent snapshot has already terminalised).
    # Observed in prod 2026-05-16: Vinay/Ranjeet CHATS had snap.status =
    # COMPLETED with all 4 partitions FAILED, item_count populated. The
    # CTE in _finalize_partitioned_snapshot guards against forward race
    # (IN_PROGRESS only) but cannot retroactively correct a terminal
    # row. This step closes that gap.
    #
    # Idempotent: only writes when the derived status differs from the
    # stored status, and only for snapshots that have partition rows.
    # Single GROUP BY pass — cheaper than 4 correlated subqueries per
    # candidate at 5k-user prod scale (~30k snapshots/batch). Filters
    # out partitions that are still pending and snapshots already in
    # the right state inside the same statement.
    snap_integrity_rows = (
        await session.execute(
            text(
                """
                WITH part_agg AS (
                    SELECT sp.snapshot_id,
                           COUNT(*)                                      AS n,
                           bool_or(sp.status IN ('QUEUED','IN_PROGRESS')) AS any_pending,
                           bool_and(sp.status = 'COMPLETED')             AS all_completed,
                           bool_and(sp.status = 'FAILED')                AS all_failed,
                           bool_or(sp.status = 'FAILED')                 AS any_failed
                      FROM snapshot_partitions sp
                     GROUP BY sp.snapshot_id
                )
                UPDATE snapshots s
                   SET status = CASE
                           WHEN pa.all_completed THEN 'COMPLETED'::snapshotstatus
                           WHEN pa.all_failed    THEN 'FAILED'::snapshotstatus
                           WHEN pa.any_failed    THEN 'PARTIAL'::snapshotstatus
                           ELSE s.status
                       END,
                       lease_owner_id   = NULL,
                       lease_expires_at = NULL,
                       lease_token      = s.lease_token + 1
                  FROM part_agg pa
                 WHERE s.id = pa.snapshot_id
                   AND pa.n > 0
                   AND NOT pa.any_pending
                   AND s.status IN ('COMPLETED'::snapshotstatus,
                                    'PARTIAL'::snapshotstatus,
                                    'FAILED'::snapshotstatus)
                   AND s.status != (CASE
                           WHEN pa.all_completed THEN 'COMPLETED'::snapshotstatus
                           WHEN pa.all_failed    THEN 'FAILED'::snapshotstatus
                           WHEN pa.any_failed    THEN 'PARTIAL'::snapshotstatus
                           ELSE s.status
                       END)
                RETURNING s.id, s.status::text AS new_status
                """
            ),
        )
    ).all()
    snaps_corrected = len(snap_integrity_rows)
    for r in snap_integrity_rows:
        print(
            f"[RECONCILER][INTEGRITY] snapshot={r.id} status "
            f"corrected → {r.new_status}"
        )

    if DRY_RUN:
        await session.rollback()
        print(
            f"[RECONCILER][DRY] parts_requeue={stats.partitions_requeued} "
            f"parts_dlq={stats.partitions_dlq} "
            f"snaps_final={stats.snapshots_finalized} "
            f"snaps_corrected={snaps_corrected} "
            f"jobs_final={stats.jobs_finalized}"
        )
        return stats

    await session.commit()
    if stats.total() or snaps_corrected:
        print(
            f"[RECONCILER] parts_requeue={stats.partitions_requeued} "
            f"parts_dlq={stats.partitions_dlq} "
            f"snaps_final={stats.snapshots_finalized} "
            f"snaps_corrected={snaps_corrected} "
            f"jobs_final={stats.jobs_finalized}"
        )
    return stats


# Mirror of the publish-side routing in
# shared/message_bus.py / shared/config.py. Centralised here so the
# reconciler doesn't drift out of sync — if a new partition type ever
# lands, add its queue here AND extend the test in
# tests/shared/test_reconciler.py.
_PARTITION_QUEUE_BY_TYPE = {
    "ONEDRIVE_FILES":     "backup.onedrive_partition",
    "CHATS":              "backup.chats_partition",
    "MAIL_FOLDERS":       "backup.mail_partition",
    "SHAREPOINT_DRIVES":  "backup.sharepoint_partition",
}


# messageType the backup-worker's `process_backup_message` dispatcher
# expects for each partition type. Without this key the worker can't
# route to the matching ``_process_*_partition_message`` handler and
# the message falls through to the generic backup-message path which
# requires ``jobId`` — none of our partition payloads carry one, so
# the message KeyError-crashes and (in classic queues) loops because
# requeue doesn't bump x-delivery-count (2026-05-16 incident:
# reconciler-requeued CHATS partitions poisoned worker-9f90b1b2).
_MESSAGE_TYPE_BY_PARTITION_TYPE = {
    "ONEDRIVE_FILES":     "BACKUP_ONEDRIVE_PARTITION",
    "CHATS":              "BACKUP_CHATS_PARTITION",
    "MAIL_FOLDERS":       "BACKUP_MAIL_PARTITION",
    "SHAREPOINT_DRIVES":  "BACKUP_SHAREPOINT_PARTITION",
}


def _queue_for_partition_type(ptype: Optional[str]) -> Optional[str]:
    if not ptype:
        return None
    return _PARTITION_QUEUE_BY_TYPE.get(ptype)


def _message_type_for_partition_type(ptype: Optional[str]) -> Optional[str]:
    if not ptype:
        return None
    return _MESSAGE_TYPE_BY_PARTITION_TYPE.get(ptype)


async def republish_partition_messages(
    stats: SweepStats,
    *,
    message_bus,
) -> int:
    """Publish AMQP messages for partitions the sweeper re-queued.

    Caller passes the global ``message_bus`` so this module stays
    free of AMQP imports — keeps the unit tests pure.
    """
    published = 0
    for p in stats.requeue_payloads:
        queue = _queue_for_partition_type(p.get("partition_type"))
        msg_type = _message_type_for_partition_type(p.get("partition_type"))
        if queue is None or msg_type is None:
            print(
                f"[RECONCILER] no queue/messageType mapping for "
                f"partition_type={p.get('partition_type')!r}; "
                f"skipping {p['id']}"
            )
            continue
        try:
            await message_bus.publish(
                queue,
                {
                    # messageType lets the backup-worker's
                    # process_backup_message dispatcher route to the
                    # right ``_process_*_partition_message`` handler.
                    # Without it the worker falls through to the
                    # generic path that requires jobId (which a bare
                    # partition envelope lacks) and crash-loops on
                    # the same message in classic queues.
                    "messageType":    msg_type,
                    "snapshotId":     p["snapshot_id"],
                    "partitionId":    p["id"],
                    "partitionType":  p["partition_type"],
                    "partitionIndex": p["partition_index"],
                    "driveId":        p.get("drive_id"),
                    "fileIds":        p.get("file_ids"),
                    "payload":        p.get("payload"),
                    "_reconciler_requeue": True,
                },
            )
            published += 1
        except Exception as exc:
            print(f"[RECONCILER] republish failed for partition {p['id']}: {exc}")
    return published
