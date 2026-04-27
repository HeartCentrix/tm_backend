"""PST export — planner + orchestrator shell.

PstGroupPlanner groups SnapshotItems into PstGroups by mailbox, folder, or
individual item.  PstExportOrchestrator (shell only for Task 3) plans the
groups and returns the plan; writers are implemented in subsequent tasks.

All Aspose imports are lazy (never at module top level) because aspose-email
is not installed in the local dev environment.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _safe_name(s: str) -> str:
    """Replace non-alphanumeric chars (except ``-`` and ``_``) with ``_``,
    truncate to 64 characters."""
    sanitized = re.sub(r"[^A-Za-z0-9\-_]", "_", s)
    return sanitized[:64]


@dataclass
class PstGroup:
    key: tuple          # the grouping key
    item_type: str      # "EMAIL" | "CALENDAR_EVENT" | "USER_CONTACT"
    items: list = field(default_factory=list)   # list of SnapshotItem objects
    pst_filename: str = ""                      # e.g. "snapshot123-mail.pst"


class PstGroupPlanner:
    PST_ITEM_TYPES = {"EMAIL", "CALENDAR_EVENT", "USER_CONTACT"}
    TYPE_SUFFIX = {
        "EMAIL": "mail",
        "CALENDAR_EVENT": "calendar",
        "USER_CONTACT": "contacts",
    }

    def plan(
        self,
        items: list,
        granularity: str,
        include_types: set,
    ) -> list:
        """Group items into PstGroups.

        Parameters
        ----------
        items:
            List of SnapshotItem-like objects.
        granularity:
            ``"MAILBOX"``, ``"FOLDER"``, or ``"ITEM"``.
        include_types:
            Subset of ``PST_ITEM_TYPES`` to include; anything else is silently
            skipped.  Pass the full ``PST_ITEM_TYPES`` set to include all.

        Returns
        -------
        list[PstGroup]
            Sorted by ``(item_type, key)``.

        Raises
        ------
        ValueError
            If *granularity* is unknown, or if ITEM granularity exceeds the
            100 k-group hard limit.
        """
        if granularity not in ("MAILBOX", "FOLDER", "ITEM"):
            raise ValueError(
                f"Unknown PST granularity: {granularity!r}. "
                "Expected MAILBOX, FOLDER, or ITEM."
            )

        effective_types = self.PST_ITEM_TYPES & include_types

        # Filter items first
        filtered = [
            it for it in items
            if getattr(it, "item_type", None) in effective_types
        ]

        # ITEM granularity guardrails (checked before building the full dict)
        if granularity == "ITEM":
            count = len(filtered)
            if count > 100_000:
                raise ValueError(
                    f"PST ITEM granularity rejected: {count} items exceeds 100k limit"
                )
            if count > 10_000:
                logger.warning(
                    "PST ITEM granularity: %d groups will create ~%d MB of PST overhead",
                    count,
                    count * 2,
                )

        groups: dict[tuple, PstGroup] = {}

        for it in filtered:
            snap_prefix = str(it.snapshot_id)[:8]
            item_type = it.item_type
            suffix = self.TYPE_SUFFIX[item_type]

            if granularity == "MAILBOX":
                key = (str(it.snapshot_id), item_type)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{suffix}.pst",
                    )

            elif granularity == "FOLDER":
                folder = it.folder_path or "root"
                key = (str(it.snapshot_id), item_type, folder)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{_safe_name(folder)}-{suffix}.pst",
                    )

            else:  # ITEM
                key = (str(it.snapshot_id), item_type, it.external_id)
                if key not in groups:
                    groups[key] = PstGroup(
                        key=key,
                        item_type=item_type,
                        pst_filename=f"{snap_prefix}-{it.external_id[:16]}-{suffix}.pst",
                    )

            groups[key].items.append(it)

        result = sorted(groups.values(), key=lambda g: (g.item_type, g.key))
        return result


class PstExportOrchestrator:
    """Entry point for PST export.  Writers are not yet implemented (Task 4+).
    This shell plans the groups and returns a planning result dict.
    """

    def __init__(
        self,
        job_id: str,
        items: list,
        spec: dict,
        storage_shard=None,
    ):
        self.job_id = job_id
        self.items = items
        self.spec = spec
        self.storage_shard = storage_shard
        self.granularity = spec.get("pstGranularity", "MAILBOX").upper()
        self.split_gb = float(spec.get("pstSplitSizeGb", 45))
        self.include_types = set(
            spec.get("pstIncludeTypes", ["EMAIL", "CALENDAR_EVENT", "USER_CONTACT"])
        )
        self.workdir = Path(f"/tmp/pst-export/{job_id}")

    async def run(self) -> dict:
        """Plan groups, dispatch writers (stub), zip output, return result dict.

        Writers are not yet implemented — this shell just plans and returns.
        """
        from shared.aspose_license import apply_license  # lazy import
        apply_license()

        self.workdir.mkdir(parents=True, exist_ok=True)
        planner = PstGroupPlanner()
        groups = planner.plan(self.items, self.granularity, self.include_types)

        # Writers not yet implemented — return planning result for now
        return {
            "job_id": self.job_id,
            "status": "planned",
            "group_count": len(groups),
            "groups": [
                {
                    "key": g.key,
                    "item_type": g.item_type,
                    "item_count": len(g.items),
                    "pst_filename": g.pst_filename,
                }
                for g in groups
            ],
            "granularity": self.granularity,
        }
