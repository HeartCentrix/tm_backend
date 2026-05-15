"""Activity-row shaping for BACKUP operations.

Single source of truth for Activity rows backed by `backup_batches`.
The legacy `_group_batch_jobs` reconstruction in audit-service/main.py
is no longer used for BACKUP — it produced a different user count
AND different progress number than `list_backup_batches` for the
same operator click, which surfaced in the 2026-05-15 incident
(card said 100%, detail said 78%; "9 users" displayed instead of
the operator's 54-user click count).

See docs/superpowers/specs/2026-05-15-backup-batch-race-fix-design.md.
"""
from __future__ import annotations

from typing import Any, Dict


_STATUS_LABEL = {
    "IN_PROGRESS": "In Progress",
    "COMPLETED": "Done",
    "PARTIAL": "Partial",
    "FAILED": "Failed",
    "CANCELLED": "Canceled",
}


def _fmt_bytes(n: int) -> str:
    """Minimal SI bytes formatter for the activity row. Matches the
    output style of audit-service's existing _fmt_bytes (binary units,
    two significant digits below 100, one digit at or above)."""
    if n is None:
        return "0 B"
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    f = float(n)
    for u in units:
        if abs(f) < 1024.0 or u == units[-1]:
            if f >= 100:
                return f"{f:.1f} {u}".replace(".0 ", " ")
            return f"{f:.2f} {u}"
        f /= 1024.0
    return f"{f:.2f} PiB"


def shape_activity_row(row) -> Dict[str, Any]:
    """Build one Activity-feed dict from a backup_batches row.

    `row` exposes (attribute access, like a SQLAlchemy Row):
      - batch_id (str), created_at (datetime), completed_at (datetime|None)
      - status (str — backup_batches.status raw value)
      - source (str), actor_email (str|None)
      - scope_user_ids (list[uuid|str])
      - bytes_expected (int|None), bytes_done (int)
      - job_ids (list[uuid|str])
      - waiting_discovery_count (int)
      - total_scope_count (int)
    """
    scope_count = int(row.total_scope_count or 0)
    user_label = (
        f"{scope_count} user" if scope_count == 1 else f"{scope_count} users"
    )

    bytes_done = int(row.bytes_done or 0)
    bytes_expected = int(row.bytes_expected) if row.bytes_expected else None
    progress_pct = None
    if bytes_expected and bytes_expected > 0:
        progress_pct = min(100, int(100 * bytes_done / bytes_expected))

    status_label = _STATUS_LABEL.get(row.status, row.status)

    if row.status == "COMPLETED":
        details = (
            f"{_fmt_bytes(bytes_done)} backed up" if bytes_done else "Completed"
        )
    elif row.status == "FAILED":
        details = "Failed"
    elif row.status == "CANCELLED":
        details = "Cancelled"
    elif row.status == "PARTIAL":
        details = f"Partial — {_fmt_bytes(bytes_done)} backed up"
    else:
        # IN_PROGRESS — include progress and discovery sub-hint when
        # relevant. The sub-hint surfaces stuck users so the operator
        # knows the batch isn't hung silently.
        bits = []
        if progress_pct is not None:
            bits.append(f"Progress: {progress_pct}%")
            if bytes_done:
                bits.append(
                    f"({_fmt_bytes(bytes_done)} of "
                    f"{_fmt_bytes(bytes_expected)})"
                )
        elif bytes_done:
            bits.append(f"{_fmt_bytes(bytes_done)} so far")
        else:
            bits.append("In progress")
        waiting = int(row.waiting_discovery_count or 0)
        if waiting > 0:
            bits.append(f"— discovering {waiting} of {scope_count}")
        details = " ".join(bits)

    return {
        "id": str(row.batch_id),
        "batchId": str(row.batch_id),
        "start_time": row.created_at.isoformat() if row.created_at else None,
        "finish_time": (
            row.completed_at.isoformat() if row.completed_at else None
        ),
        "status": status_label,
        "operation": "BACKUP",
        "object": user_label,
        "details": details,
        "batchSource": row.source,
        "jobIds": [str(j) for j in (row.job_ids or [])],
        "progressPct": progress_pct,
        "bytesDone": bytes_done,
        "bytesExpected": bytes_expected,
    }
