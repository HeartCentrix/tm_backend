"""Base class for per-type PST writers (pstwriter CLI backend).

Each concrete writer (``MailPstWriter``, ``CalendarPstWriter``,
``ContactPstWriter``) overrides :meth:`_collect_graph_item` to translate
one SnapshotItem into a Microsoft Graph JSON dict (the format pstwriter
consumes). The base class accumulates dicts, splits into chunks bounded
by ``split_gb``, and shells out to the bundled ``pst_convert`` binary
once per chunk.

The previous Aspose.Email implementation lived inline in this file and
opened a ``PersonalStorage`` per group, inserting items one at a time.
It was replaced by the standalone C++ writer (vendored under
``vendor/pstwriter``); the orchestrator contract (``write(...) ->
PstWriteResult``) is unchanged.

Tunables (env vars):
    PST_ITEM_RETRY_MAX           — unused; retained for back-compat.
    PST_MAX_ATTACHMENT_BYTES     — per-attachment cap (mail writer only).
    PSTWRITER_CLI                — override path to pst_convert binary.
    PSTWRITER_CLI_TIMEOUT_S      — wall-clock cap on a single CLI call.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Per-attachment hard cap (used by the mail writer to skip oversize
# attachments rather than OOM the worker). Retained from the Aspose era;
# applied in mail.py before base64-inlining.
MAX_ATTACHMENT_BYTES = int(
    os.environ.get("PST_MAX_ATTACHMENT_BYTES", str(50 * 1024 * 1024))
)

# Make the shared/ tree importable when this module is loaded outside
# the production worker container (tests, ad-hoc scripts).
_HERE = os.path.dirname(__file__)
_SHARED = os.path.abspath(os.path.join(_HERE, "..", "..", "..", "shared"))
if os.path.isdir(_SHARED) and os.path.dirname(_SHARED) not in sys.path:
    sys.path.insert(0, os.path.dirname(_SHARED))

logger = logging.getLogger(__name__)


@dataclass
class PstWriteResult:
    pst_paths: list = field(default_factory=list)   # list[Path]
    item_count: int = 0
    failed_count: int = 0


class PstWriterBase:
    """Abstract base. Subclasses set ``item_type`` + ``cli_kind`` and
    implement :meth:`_collect_graph_item`."""

    item_type: str = ""        # "EMAIL" | "CALENDAR_EVENT" | "USER_CONTACT"
    cli_kind: str = ""         # "mail" | "calendar" | "contacts"

    # Per-item byte cost used to decide chunk boundaries when ``split_gb``
    # would otherwise produce a single huge PST. Cheap heuristic — the
    # PST file is almost always larger than the sum of inputs (tables,
    # node IDs, padding) but ``split_gb * 0.95`` already builds in slack.
    _DEFAULT_PER_ITEM_BYTES = 8 * 1024

    async def write(
        self,
        group,                 # PstGroup from pst_export.py
        workdir: Path,
        split_gb: float = 45.0,
        shard=None,
        source_container: str = "",
    ) -> PstWriteResult:
        """Collect Graph JSON for every item in *group*, chunk by
        ``split_gb``, invoke ``pst_convert`` once per chunk, return all
        produced paths in a :class:`PstWriteResult`."""
        from shared.pstwriter_cli import convert_to_pst, PstWriterCliError

        result = PstWriteResult()
        workdir.mkdir(parents=True, exist_ok=True)
        split_bytes = int(split_gb * 0.95 * 1024 ** 3)

        # ------------------------------------------------------------------
        # Phase 1: collect Graph JSON for every item. Per-item exceptions
        # bump failed_count; the rest of the group still ships.
        # ------------------------------------------------------------------
        collected: list[dict] = []
        per_item_bytes: list[int] = []

        for item in group.items:
            ext_id = getattr(item, "external_id", "?")
            try:
                graph_obj = await self._collect_graph_item(
                    item, shard, source_container,
                )
            except Exception as exc:
                logger.error(
                    "[pst_writer] collect FAILED item=%s type=%s: %s",
                    ext_id, self.item_type, exc,
                )
                result.failed_count += 1
                continue
            if graph_obj is None:
                # Permanent skip (None body, missing extra_data, etc.)
                result.failed_count += 1
                continue
            collected.append(graph_obj)
            per_item_bytes.append(
                getattr(item, "content_size", 0) or self._DEFAULT_PER_ITEM_BYTES
            )

        if not collected:
            logger.warning(
                "[pst_writer] group %s produced 0 collectable items "
                "(failed=%d)", group.key, result.failed_count,
            )
            return result

        # ------------------------------------------------------------------
        # Phase 2: chunk by estimated size + dispatch to the CLI.
        # Each chunk → one pst_convert invocation → one .pst file.
        # ------------------------------------------------------------------
        chunks = self._chunk_by_bytes(collected, per_item_bytes, split_bytes)
        stem = group.pst_filename[:-4] if group.pst_filename.endswith(".pst") \
            else group.pst_filename
        # Reuse the planner's chosen filename for chunk #1 (back-compat with
        # callers that index by group.pst_filename); subsequent chunks get
        # the -NNN.pst suffix.
        for idx, chunk in enumerate(chunks, start=1):
            if idx == 1 and len(chunks) == 1:
                pst_path = workdir / group.pst_filename
            elif idx == 1:
                pst_path = workdir / f"{stem}-001.pst"
            else:
                pst_path = workdir / f"{stem}-{idx:03d}.pst"
            if pst_path.exists():
                pst_path.unlink()

            try:
                written = await convert_to_pst(
                    self.cli_kind, chunk, pst_path, workdir=workdir,
                )
                result.item_count += written
                result.pst_paths.append(pst_path)
                logger.info(
                    "[pst_writer] wrote chunk %d/%d type=%s items=%d path=%s",
                    idx, len(chunks), self.item_type, written, pst_path.name,
                )
            except (PstWriterCliError, asyncio.TimeoutError) as exc:
                # Whole-chunk failure — pstwriter doesn't expose
                # per-item insert. Bump failed_count by chunk size and
                # keep going so other chunks still ship.
                logger.error(
                    "[pst_writer] chunk %d/%d FAILED type=%s items=%d: %s",
                    idx, len(chunks), self.item_type, len(chunk), exc,
                )
                result.failed_count += len(chunk)
                # Defensive cleanup — pst_convert may have left a
                # half-written file behind.
                try:
                    pst_path.unlink(missing_ok=True)
                except Exception:
                    pass

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _chunk_by_bytes(
        items: list,
        per_item_bytes: list[int],
        split_bytes: int,
    ) -> list[list]:
        """Greedy bin-pack items into chunks bounded by ``split_bytes``.

        Always returns at least one chunk. Items larger than ``split_bytes``
        on their own get their own chunk (we never split a single item).
        """
        if split_bytes <= 0 or not items:
            return [items] if items else []
        chunks: list[list] = []
        current: list = []
        current_bytes = 0
        for item, sz in zip(items, per_item_bytes):
            if current and current_bytes + sz > split_bytes:
                chunks.append(current)
                current = []
                current_bytes = 0
            current.append(item)
            current_bytes += sz
        if current:
            chunks.append(current)
        return chunks

    # ------------------------------------------------------------------
    # Subclass contract
    # ------------------------------------------------------------------

    async def _collect_graph_item(
        self, item, shard, source_container: str,
    ) -> Optional[dict]:
        """Translate one SnapshotItem into a Microsoft Graph JSON dict
        (mail message / contact / event) suitable for ``pst_convert``.

        Return ``None`` to skip this item (caller bumps ``failed_count``).
        Raise to signal a transient/unexpected failure (also counted as
        a failure but logged at ERROR with stack).
        """
        raise NotImplementedError
