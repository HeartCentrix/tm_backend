"""Batch-rollup logic for the Activity Manager.

Pure functions + one SQL builder, isolated from any FastAPI handler so
the state machine is unit-testable without a database. Imported by
``services/audit-service/main.py`` (the only consumer today).

The rollup considers THREE state sources:

1. Jobs       — the 3 fan-out Jobs of a "Backup all" click share
                ``spec.batch_id`` (Tier-1 ENTRA_USER bulk, Tier-2-urgent
                for mail/calendar/contacts, Tier-2-heavy for OneDrive/
                chats).
2. Snapshots  — per-resource state under each Job.
3. snapshot_partitions — per-shard state under partitioned snapshots
                (OneDrive / Chats / Mail / SharePoint).

A batch is "Done" only when ALL three are terminal AND no expected
Tier-2 child Job is still missing from the fan-out (the
``fanout_incomplete`` gate fixes the 3-5 s Tier-1 → Tier-2 handoff
flicker that the operator sees today).

See docs/superpowers/specs/2026-05-15-activity-batch-rollup-design.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text


@dataclass
class RollupCounts:
    """Pre-computed rollup counts for one batch.

    Built by ``build_batch_rollup_query``; consumed by
    ``derive_batch_status`` and ``shape_batch_row``. Fields map 1:1 to
    the CTE output columns so the SQL → Python boundary is mechanical.
    """
    all_jobs_terminal: bool
    any_cancelled: bool
    any_job_failed: bool
    snap_total: int
    snap_done: int
    snap_partial: int
    snap_failed: int
    snap_pending: int
    parts_pending: int
    missing_t2: int


def derive_batch_status(
    r: RollupCounts,
) -> Tuple[str, Optional[Dict[str, int]]]:
    """Map RollupCounts → (status, warnings).

    Returns ``(status_label, warnings_dict_or_None)``. Status labels
    match the existing frontend enum:

        "In Progress" | "Done" | "Failed" | "Canceled"

    ``warnings`` is non-None only when status == "Done" and at least
    one child snapshot landed in PARTIAL or FAILED. Shape:

        {"partial": N, "failed": M}
    """
    # 1. Anything still moving → In Progress. ``missing_t2`` is the
    # new gate: even if every CURRENT Job is terminal, if Tier-2
    # children haven't been spawned yet for the Tier-1 ENTRA_USERs in
    # this batch, we are still in the handoff window.
    if (
        not r.all_jobs_terminal
        or r.snap_pending > 0
        or r.parts_pending > 0
        or r.missing_t2 > 0
    ):
        return ("In Progress", None)

    # 2. Cancellation: if operator cancelled and NOTHING got persisted,
    # show Canceled. If some snapshots completed before the cancel
    # landed, treat those as a partial-success backup so the operator
    # can still see (and recover) what we managed to capture.
    has_successes = r.snap_done > 0 or r.snap_partial > 0
    if r.any_cancelled and not has_successes:
        return ("Canceled", None)

    # 3. No successes at all → Failed.
    if not has_successes:
        return ("Failed", None)

    # 4. Mixed outcome → Done with warnings chip. Cancellation that
    # landed AFTER some snapshots completed is also a "not-clean" Done
    # — we surface a warning so the operator sees the truncation even
    # if every committed snapshot itself was clean.
    if r.snap_partial > 0 or r.snap_failed > 0 or r.any_cancelled:
        return ("Done", {"partial": r.snap_partial, "failed": r.snap_failed})

    # 5. Everything clean.
    return ("Done", None)


def build_batch_rollup_query(
    *,
    tenant_id: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    operation: Optional[str],
    size: int,
    offset: int,
) -> Any:
    """Build the single CTE-driven query that returns one row per batch.

    Returns a SQLAlchemy ``text()`` clause with named bind params:
        :tid, :start_date, :end_date, :op, :size, :off

    Output columns (one row per batch):
        batch_id, tenant_id, started_at, jobs_max_completed_at,
        snaps_max_completed_at, parts_max_completed_at,
        job_ids (uuid[]), entra_user_count, total_resource_count,
        bytes_added, items_added, all_jobs_terminal, any_cancelled,
        any_job_failed, snap_total, snap_done, snap_partial,
        snap_failed, snap_pending, parts_pending, missing_t2

    Performance: relies on existing indexes ``ix_jobs_tenant_started``,
    ``ix_snapshots_job_id``, ``ix_snapshot_partitions_snapshot``.
    Measured at ~80 ms on a 10 k-job tenant in similar audit-service
    aggregate queries.
    """
    op_filter = "AND j.type::text = :op" if operation else ""
    sql = f"""
    WITH filtered_jobs AS (
        SELECT *
        FROM jobs j
        WHERE (CAST(:tid AS UUID) IS NULL OR j.tenant_id = CAST(:tid AS UUID))
          AND (CAST(:start_date AS TIMESTAMP) IS NULL OR j.created_at >= CAST(:start_date AS TIMESTAMP))
          AND (CAST(:end_date   AS TIMESTAMP) IS NULL OR j.created_at <= CAST(:end_date AS TIMESTAMP))
          {op_filter}
          AND j.type = 'BACKUP'
    ),
    batches AS (
        SELECT
            COALESCE(j.spec->>'batch_id', j.id::text)              AS batch_id,
            j.tenant_id                                            AS tenant_id,
            MIN(j.created_at)                                      AS started_at,
            MAX(j.completed_at)                                    AS jobs_max_completed_at,
            ARRAY_AGG(j.id ORDER BY j.created_at)                  AS job_ids,
            BOOL_OR(j.status::text = 'CANCELLED')                  AS any_cancelled,
            BOOL_OR(j.status::text = 'FAILED')                     AS any_job_failed,
            BOOL_AND(j.status::text IN ('COMPLETED','FAILED','CANCELLED'))
                                                                    AS all_jobs_terminal,
            -- For non-bulk single-Job batches (no batch_resource_ids),
            -- carry through job.resource_id so we can resolve a real
            -- display name instead of falling back to "Bulk Operation".
            -- COUNT()=1 + MIN()=MAX() guarantees single Job.
            COUNT(*)                                               AS job_count,
            -- PG has no MIN/MAX for uuid; array_agg + [1] pulls the
            -- single resource_id when job_count=1.
            (ARRAY_AGG(j.resource_id))[1]                          AS single_resource_id
        FROM filtered_jobs j
        GROUP BY 1, 2
    ),
    single_res AS (
        SELECT
            b.batch_id,
            r.display_name AS single_resource_name,
            r.type::text   AS single_resource_type
        FROM batches b
        LEFT JOIN resources r ON r.id = b.single_resource_id
        WHERE b.job_count = 1 AND b.single_resource_id IS NOT NULL
    ),
    batch_res AS (
        -- Flatten batch_resource_ids per batch via lateral unnest.
        -- Avoids ARRAY_AGG(uuid[]) which fails on jagged arrays and
        -- the unnest(uuid[][]) → scalar gotcha. NULLs coalesced to
        -- empty array so the LATERAL stays safe.
        SELECT
            COALESCE(j.spec->>'batch_id', j.id::text) AS batch_id,
            COALESCE(
                ARRAY_AGG(DISTINCT bid) FILTER (WHERE bid IS NOT NULL),
                ARRAY[]::uuid[]
            ) AS all_res_ids
        FROM filtered_jobs j
        LEFT JOIN LATERAL unnest(COALESCE(j.batch_resource_ids, ARRAY[]::uuid[])) AS bid ON TRUE
        GROUP BY 1
    ),
    snap_roll AS (
        SELECT
            b.batch_id,
            COUNT(s.id)                                             AS snap_total,
            COUNT(*) FILTER (WHERE s.status::text = 'COMPLETED')    AS snap_done,
            COUNT(*) FILTER (WHERE s.status::text = 'PARTIAL')      AS snap_partial,
            COUNT(*) FILTER (WHERE s.status::text = 'FAILED')       AS snap_failed,
            COUNT(*) FILTER (WHERE s.status::text IN ('QUEUED','IN_PROGRESS'))
                                                                    AS snap_pending,
            MAX(s.completed_at)                                     AS snaps_max_completed_at,
            COALESCE(SUM(s.bytes_added), 0)                         AS bytes_added,
            COALESCE(SUM(s.item_count), 0)                          AS items_added
        FROM batches b
        LEFT JOIN snapshots s ON s.job_id = ANY(b.job_ids)
        GROUP BY 1
    ),
    parts_roll AS (
        SELECT
            b.batch_id,
            COUNT(*) FILTER (WHERE sp.status::text NOT IN ('COMPLETED','FAILED'))
                                                                    AS parts_pending,
            MAX(sp.completed_at)                                    AS parts_max_completed_at
        FROM batches b
        LEFT JOIN snapshot_partitions sp ON sp.job_id = ANY(b.job_ids)
        GROUP BY 1
    ),
    expected_t2 AS (
        -- Per batch, the Cartesian product of (ENTRA_USERs in the
        -- batch's batch_resource_ids) × (their Tier-2 child resources
        -- from `resources.parent_resource_id`). One row per (batch,
        -- child) tells us which Tier-2 children we EXPECT to see
        -- regardless of whether the Tier-2 Jobs have spawned yet.
        SELECT
            br.batch_id,
            r2.id AS child_resource_id
        FROM batch_res br
        CROSS JOIN LATERAL unnest(br.all_res_ids) AS bid
        JOIN resources r1 ON r1.id = bid AND r1.type::text = 'ENTRA_USER'
        JOIN resources r2 ON r2.parent_resource_id = r1.id
                         AND r2.type::text IN (
                             'USER_MAIL','USER_ONEDRIVE','USER_CHATS',
                             'USER_CALENDAR','USER_CONTACTS')
    ),
    observed_t2 AS (
        SELECT DISTINCT
            br.batch_id,
            bid AS child_resource_id
        FROM batch_res br
        CROSS JOIN LATERAL unnest(br.all_res_ids) AS bid
    ),
    fanout AS (
        SELECT
            e.batch_id,
            COUNT(*) FILTER (WHERE o.child_resource_id IS NULL) AS missing_t2
        FROM expected_t2 e
        LEFT JOIN observed_t2 o
               ON o.batch_id = e.batch_id
              AND o.child_resource_id = e.child_resource_id
        GROUP BY 1
    ),
    entra_count AS (
        SELECT
            br.batch_id,
            COUNT(*) FILTER (WHERE r.type::text = 'ENTRA_USER') AS entra_user_count,
            COUNT(*)                                            AS total_resource_count
        FROM batch_res br
        CROSS JOIN LATERAL unnest(br.all_res_ids) AS bid
        LEFT JOIN resources r ON r.id = bid
        GROUP BY 1
    )
    SELECT
        b.batch_id,
        b.tenant_id,
        b.started_at,
        b.jobs_max_completed_at,
        sr.snaps_max_completed_at,
        pr.parts_max_completed_at,
        b.job_ids,
        COALESCE(ec.entra_user_count, 0)    AS entra_user_count,
        COALESCE(ec.total_resource_count, 0) AS total_resource_count,
        COALESCE(sr.bytes_added, 0)         AS bytes_added,
        COALESCE(sr.items_added, 0)         AS items_added,
        b.all_jobs_terminal,
        b.any_cancelled,
        b.any_job_failed,
        COALESCE(sr.snap_total, 0)          AS snap_total,
        COALESCE(sr.snap_done, 0)           AS snap_done,
        COALESCE(sr.snap_partial, 0)        AS snap_partial,
        COALESCE(sr.snap_failed, 0)         AS snap_failed,
        COALESCE(sr.snap_pending, 0)        AS snap_pending,
        COALESCE(pr.parts_pending, 0)       AS parts_pending,
        COALESCE(f.missing_t2, 0)           AS missing_t2,
        sres.single_resource_name           AS single_resource_name,
        sres.single_resource_type           AS single_resource_type
    FROM batches b
    LEFT JOIN snap_roll   sr ON sr.batch_id = b.batch_id
    LEFT JOIN parts_roll  pr ON pr.batch_id = b.batch_id
    LEFT JOIN fanout       f ON f.batch_id  = b.batch_id
    LEFT JOIN entra_count ec ON ec.batch_id = b.batch_id
    LEFT JOIN single_res  sres ON sres.batch_id = b.batch_id
    ORDER BY b.started_at DESC NULLS LAST
    LIMIT :size OFFSET :off
    """
    binds: Dict[str, Any] = dict(
        tid=tenant_id,
        start_date=start_date,
        end_date=end_date,
        size=size,
        off=offset,
    )
    # Only bind :op when the SQL string actually references it,
    # otherwise SQLAlchemy raises ArgumentError on the unused param.
    if operation:
        binds["op"] = operation
    return text(sql).bindparams(**binds)


def shape_batch_row(row: Any) -> Dict[str, Any]:
    """Convert one CTE row → the ActivityItem JSON the frontend renders.

    Maps the SQL columns into the shape documented in §6.1 of the spec.
    The state-machine call lives here so callers don't have to re-derive.
    """
    counts = RollupCounts(
        all_jobs_terminal=bool(row.all_jobs_terminal),
        any_cancelled=bool(row.any_cancelled),
        any_job_failed=bool(row.any_job_failed),
        snap_total=int(row.snap_total or 0),
        snap_done=int(row.snap_done or 0),
        snap_partial=int(row.snap_partial or 0),
        snap_failed=int(row.snap_failed or 0),
        snap_pending=int(row.snap_pending or 0),
        parts_pending=int(row.parts_pending or 0),
        missing_t2=int(row.missing_t2 or 0),
    )
    status, warnings = derive_batch_status(counts)

    # finish_time is set only when truly terminal (invariant I3). Max
    # across Jobs / Snapshots / Partitions terminal timestamps — the
    # LAST thing that finished. For a Failed batch with no work
    # produced, falls back to jobs_max_completed_at.
    finish_iso = ""
    if status in ("Done", "Failed", "Canceled"):
        candidates = [
            row.parts_max_completed_at,
            row.snaps_max_completed_at,
            row.jobs_max_completed_at,
        ]
        ts = max([c for c in candidates if c is not None], default=None)
        finish_iso = ts.isoformat() if ts else ""

    # Resource-count display: "9 users" if the batch targets
    # ENTRA_USERs (the M365 click scope), else the total. Never the
    # post-fan-out leaf count — that's noise from the user's POV.
    # For non-bulk single-Job backups (per-resource "Backup now"),
    # prefer the resource display name to match legacy UX.
    entra = int(row.entra_user_count or 0)
    total = int(row.total_resource_count or 0)
    single_name = getattr(row, "single_resource_name", None)
    if entra > 0:
        obj_label = f"{entra} user" if entra == 1 else f"{entra} users"
    elif total > 0:
        obj_label = f"{total} resource" if total == 1 else f"{total} resources"
    elif single_name:
        obj_label = single_name
    else:
        obj_label = "Bulk Operation"

    # progress_pct: bytes-weighted when totals known, else fall back to
    # the terminal/total ratio of child snapshots. Server returns a
    # single number; the client clamps it monotonically across polls.
    bytes_added = int(row.bytes_added or 0)
    bytes_expected = bytes_added  # No bytes_expected column today.
    if bytes_expected > 0:
        progress_pct = min(100, int(100 * bytes_added / bytes_expected))
    elif counts.snap_total > 0:
        terminal = counts.snap_done + counts.snap_partial + counts.snap_failed
        progress_pct = min(100, int(100 * terminal / counts.snap_total))
    else:
        progress_pct = 0
    if status in ("Done", "Failed", "Canceled"):
        progress_pct = 100

    # Phase chip — v1 collapses to in_progress / done. Refining to
    # discovering / urgent / heavy needs Tier-1 vs Tier-2 Job-type
    # discrimination which we can add as a follow-up; for now the
    # operator gets the binary signal.
    phase = "done" if status in ("Done", "Failed", "Canceled") else "in_progress"

    # Cancel button shows only while at least one Job can still be
    # cancelled.
    cancellable = not (counts.all_jobs_terminal or counts.any_cancelled)

    job_ids = [str(j) for j in (row.job_ids or [])]

    return {
        "id":             row.batch_id,
        "batchId":        row.batch_id,
        "jobIds":         job_ids,
        "start_time":     row.started_at.isoformat() if row.started_at else "",
        "operation":      "BACKUP",
        "object":         obj_label,
        "status":         status,
        "finish_time":    finish_iso,
        "details":        _format_details(status, bytes_added, counts),
        "data_backed_up": bytes_added,
        "total_data":     bytes_expected,
        "phase":          phase,
        "counts": {
            "total":       counts.snap_total,
            "done":        counts.snap_done,
            "partial":     counts.snap_partial,
            "failed":      counts.snap_failed,
            "in_progress": counts.snap_pending,
            "queued":      0,  # combined into snap_pending today
        },
        "warnings":     warnings,
        "progress_pct": progress_pct,
        "cancellable":  cancellable,
    }


def _format_details(status: str, bytes_added: int, counts: RollupCounts) -> str:
    """Human-readable summary line — mirrors the legacy ``_group_batch_jobs``."""
    if status == "Done":
        if bytes_added > 0:
            return f"{_fmt_bytes(bytes_added)} backed up"
        return "Completed"
    if status == "Failed":
        return "Failed"
    if status == "Canceled":
        return "Cancelled"
    # In Progress
    if counts.snap_total > 0:
        terminal = counts.snap_done + counts.snap_partial + counts.snap_failed
        pct = int(100 * terminal / counts.snap_total) if counts.snap_total else 0
        return f"Progress: {pct}% ({_fmt_bytes(bytes_added)} so far)"
    return "Progress: 0%"


def _fmt_bytes(n: int) -> str:
    """1024-base formatter; matches the legacy helper in audit-service."""
    if n < 1024:
        return f"{n} B"
    units = ["KiB", "MiB", "GiB", "TiB", "PiB"]
    v = float(n) / 1024.0
    for u in units:
        if v < 1024.0:
            return f"{v:.1f} {u}"
        v /= 1024.0
    return f"{v:.1f} EiB"
