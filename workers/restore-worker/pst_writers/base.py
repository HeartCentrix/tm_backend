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

    # Per-PST block budget. The vendored writer's BBT supports at most
    # 400 data blocks (kBbtMaxEntriesPerLeaf × kBtMaxEntriesPerPage =
    # 20 × 20). Reserve ~20 blocks for the folder tree + IPM Subtree +
    # store-level nodes, leaving ~380 for message data. We chunk by
    # *estimated blocks per item* rather than item count so light
    # attachment-free mail packs hundreds of messages into one PST while
    # attachment-heavy mail naturally splits across several. This is the
    # "one PST when feasible, multi-PST only when truly needed" target.
    _MAX_BLOCKS_PER_CHUNK = 380

    # Per-item PST data-block estimate.
    #   * Each message PC + Recipient TC ~= 2 structural blocks
    #   * Each (body+attachments) byte above _SUBNODE_INLINE_LIMIT is
    #     subnode-promoted and split into _BLOCK_PAYLOAD_BYTES chunks.
    # Subclasses (calendar, contacts) override the per-item base.
    _PER_ITEM_BASE_BLOCKS = 2
    _BLOCK_PAYLOAD_BYTES  = 7000
    _SUBNODE_INLINE_LIMIT = 3580

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
            per_item_bytes.append(self._estimate_item_size(item, graph_obj))

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

        # FIFO of chunks pending write. On failure we bisect a chunk and
        # reinsert both halves, so the chunker's pre-flight estimate
        # doesn't have to be perfect — actual writer overflow gets
        # rescued by progressively smaller chunks. Single-item chunks
        # that still fail get counted against ``failed_count`` so the
        # caller knows data was dropped.
        pending: list[list] = [list(c) for c in chunks]
        emitted = 0  # number of PSTs successfully written so far
        # planned_pst_count is just for the human-readable filename when
        # only a single chunk was planned (back-compat with callers that
        # index by group.pst_filename verbatim).
        single_planned = (len(chunks) == 1)

        while pending:
            chunk = pending.pop(0)
            next_idx = emitted + 1
            if single_planned and next_idx == 1 and not pending:
                pst_path = workdir / group.pst_filename
            elif next_idx == 1:
                pst_path = workdir / f"{stem}-001.pst"
            else:
                pst_path = workdir / f"{stem}-{next_idx:03d}.pst"
            if pst_path.exists():
                pst_path.unlink()

            try:
                written = await convert_to_pst(
                    self.cli_kind, chunk, pst_path, workdir=workdir,
                )
                emitted += 1
                result.item_count += written
                result.pst_paths.append(pst_path)
                logger.info(
                    "[pst_writer] wrote PST %d type=%s items=%d path=%s",
                    emitted, self.item_type, written, pst_path.name,
                )
            except (PstWriterCliError, asyncio.TimeoutError) as exc:
                # Defensive cleanup — pst_convert may have left a
                # half-written file behind.
                try:
                    pst_path.unlink(missing_ok=True)
                except Exception:
                    pass
                if len(chunk) > 1:
                    # Bisect and retry both halves. New halves go to the
                    # front of the queue so we surface the failure early
                    # if a single message still can't write.
                    half = len(chunk) // 2
                    logger.warning(
                        "[pst_writer] chunk type=%s items=%d FAILED, "
                        "bisecting into %d + %d and retrying: %s",
                        self.item_type, len(chunk), half, len(chunk) - half, exc,
                    )
                    pending.insert(0, chunk[half:])
                    pending.insert(0, chunk[:half])
                else:
                    # Single item that still doesn't fit — drop it but
                    # log loudly so the user sees what was lost.
                    logger.error(
                        "[pst_writer] dropping unwriteable item type=%s: %s",
                        self.item_type, exc,
                    )
                    result.failed_count += 1

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _estimate_item_size(self, item, graph_obj) -> int:
        """Total bytes the writer will materialise for this item, including
        attachments inlined into the Graph dict. Used for chunk planning.
        Subclasses can override to tighten the estimate."""
        base = getattr(item, "content_size", 0) or self._DEFAULT_PER_ITEM_BYTES
        if isinstance(graph_obj, dict):
            atts = graph_obj.get("attachments")
            if isinstance(atts, list):
                for a in atts:
                    if isinstance(a, dict):
                        try:
                            base += int(a.get("size") or 0)
                        except (TypeError, ValueError):
                            pass
        return int(base)

    @classmethod
    def _estimate_item_blocks(cls, item_bytes: int) -> int:
        """Rough estimate of how many PST data blocks one item consumes.

        Block 0 covers the PC + Recipient TC + small body inline. Bytes
        above ``_SUBNODE_INLINE_LIMIT`` are subnode-promoted and split
        into ``_BLOCK_PAYLOAD_BYTES`` chunks.
        """
        promoted = max(0, int(item_bytes) - cls._SUBNODE_INLINE_LIMIT)
        promoted_blocks = (promoted + cls._BLOCK_PAYLOAD_BYTES - 1) \
                          // cls._BLOCK_PAYLOAD_BYTES
        return cls._PER_ITEM_BASE_BLOCKS + promoted_blocks

    @classmethod
    def _chunk_by_bytes(
        cls,
        items: list,
        per_item_bytes: list[int],
        split_bytes: int,
    ) -> list[list]:
        """Greedy bin-pack items into chunks bounded by both ``split_bytes``
        AND ``cls._MAX_BLOCKS_PER_CHUNK``.

        The block cap reflects the vendored pstwriter's 400-block BBT
        ceiling — once a chunk's estimated block consumption would
        exceed it, we start a new chunk. This lets:
          * light attachment-free mail pack hundreds of messages into one PST
          * attachment-heavy mail naturally split across several PSTs
          * a single PST always span multiple folders when they fit

        Sort by ``_folderPath`` first so each chunk keeps related folders
        together. Falls back to original order for non-mail items.
        """
        if not items:
            return []
        # Optional env override for live tuning without rebuild.
        max_blocks = cls._MAX_BLOCKS_PER_CHUNK
        try:
            env_cap = int(os.environ.get("PST_MAX_BLOCKS_PER_CHUNK", "0") or 0)
            if env_cap > 0:
                max_blocks = env_cap
        except ValueError:
            pass

        # Stable-sort by Graph "_folderPath" so each chunk keeps siblings
        # together. Items without the field (calendar/contacts) keep
        # their original order via empty-string sort key.
        order = list(range(len(items)))
        order.sort(key=lambda i: (
            str(items[i].get("_folderPath", "") or "") if isinstance(items[i], dict) else ""
        ))
        ordered_items = [items[i] for i in order]
        ordered_bytes = [per_item_bytes[i] for i in order]

        chunks: list[list] = []
        current: list = []
        current_bytes = 0
        current_blocks = 0
        for item, sz in zip(ordered_items, ordered_bytes):
            est_blocks = cls._estimate_item_blocks(sz)
            byte_overflow = (
                split_bytes > 0
                and current
                and current_bytes + sz > split_bytes
            )
            block_overflow = (
                max_blocks > 0
                and current
                and current_blocks + est_blocks > max_blocks
            )
            if byte_overflow or block_overflow:
                chunks.append(current)
                current = []
                current_bytes = 0
                current_blocks = 0
            current.append(item)
            current_bytes += sz
            current_blocks += est_blocks
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
