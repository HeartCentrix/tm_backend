"""Base class for per-type PST writers.

Concrete writers (``MailPstWriter``, ``CalendarPstWriter``,
``ContactPstWriter``) override ``_build_mapi_item`` to translate one
SnapshotItem into an aspose-email MAPI item.  The base class handles
PST creation, folder creation, item insertion, and size-driven rotation
into multiple ``.pst`` parts when the writer crosses ``split_gb``.

All ``aspose.*`` imports are lazy (inside methods, via
``importlib.import_module``) because aspose-email is not installed in
the dev/test environment.

Tunable env vars:
    PST_ITEM_RETRY_MAX           (default 3)   — per-item attempt cap
    PST_ITEM_RETRY_BACKOFF_BASE  (default 0.5) — base seconds, doubles
    PST_MAX_ATTACHMENT_BYTES     (default 50MB) — skip larger attachments
                                                 to bound memory; emit a
                                                 .txt placeholder instead
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Per-item retry policy. Transient failures (blob read timeout, MAPI
# convert hiccup) get up to N attempts with exponential backoff. Hard
# failures (item type unknown, raw missing) bypass retry —
# _build_mapi_item returns None on those and we never enter the retry loop.
ITEM_RETRY_MAX = int(os.environ.get("PST_ITEM_RETRY_MAX", "3"))
ITEM_RETRY_BACKOFF_BASE = float(os.environ.get("PST_ITEM_RETRY_BACKOFF_BASE", "0.5"))

# Hard cap on per-attachment bytes. Aspose's MailMessage.load needs the
# full EML bytes in memory; a single multi-GB attachment in a 100GB
# mailbox would OOM the worker. Items exceeding this skip the
# attachment (replaced by a small text placeholder) so the rest of
# the message still gets exported. Default = 50 MB; raise via env for
# specialized deployments with more memory headroom.
MAX_ATTACHMENT_BYTES = int(
    os.environ.get("PST_MAX_ATTACHMENT_BYTES", str(50 * 1024 * 1024))
)


@dataclass
class PstWriteResult:
    pst_paths: list = field(default_factory=list)   # list[Path] — one or more rotated .pst files
    item_count: int = 0
    failed_count: int = 0


class PstWriterBase:
    """Abstract base for ``MailPstWriter``, ``CalendarPstWriter``,
    ``ContactPstWriter``.  Subclasses implement ``_build_mapi_item``."""

    item_type: str = ""             # "EMAIL" | "CALENDAR_EVENT" | "USER_CONTACT"
    standard_folder_type: str = ""  # "Inbox" | "Calendar" | "Contacts"

    async def write(
        self,
        group,                    # PstGroup from pst_export.py
        workdir: Path,
        split_gb: float = 45.0,
        shard=None,
        source_container: str = "",
    ) -> PstWriteResult:
        """Write all items in *group* to one or more ``.pst`` files under
        *workdir*.  Rotates to a new PST when estimated size exceeds
        ``split_gb * 0.95`` GB.  Returns a :class:`PstWriteResult`.
        """
        import importlib
        aspose_pst = importlib.import_module("aspose.email.storage.pst")
        PersonalStorage = aspose_pst.PersonalStorage
        FileFormatVersion = aspose_pst.FileFormatVersion

        result = PstWriteResult()
        part = 1
        pst_path = workdir / group.pst_filename
        split_bytes = split_gb * 0.95 * 1024 ** 3

        def _open_pst(path: Path):
            if path.exists():
                path.unlink()
            return PersonalStorage.create(str(path), FileFormatVersion.UNICODE)

        pst = _open_pst(pst_path)
        # Per-PST folder cache so each item lands in the folder that mirrors
        # its source `folder_path` instead of being lumped into a single
        # standard folder. Cache is reset on rotation.
        folder_cache = self._build_folder_cache(pst)
        size_estimate = 0

        try:
            for item in group.items:
                ext_id = getattr(item, "external_id", "?")
                # Per-item retry with exponential backoff. Transient failures
                # (blob read timeouts, .NET hiccups) get re-tried; permanent
                # ones (None mapi_item, malformed JSON) skip immediately.
                mapi_item = None
                last_exc = None
                for attempt in range(ITEM_RETRY_MAX):
                    try:
                        mapi_item = await self._build_mapi_item(
                            item, shard, source_container
                        )
                        last_exc = None
                        break
                    except Exception as exc:
                        last_exc = exc
                        if attempt + 1 < ITEM_RETRY_MAX:
                            delay = ITEM_RETRY_BACKOFF_BASE * (2 ** attempt)
                            logger.warning(
                                "build attempt %d/%d failed for %s: %s "
                                "(retrying in %.1fs)",
                                attempt + 1, ITEM_RETRY_MAX, ext_id, exc, delay,
                            )
                            await asyncio.sleep(delay)
                        else:
                            logger.error(
                                "build FAILED after %d attempts for %s: %s",
                                ITEM_RETRY_MAX, ext_id, exc,
                            )

                if mapi_item is None:
                    # Either build returned None (permanent skip) or all
                    # retries exhausted (last_exc holds the cause).
                    result.failed_count += 1
                    continue

                # PST insert can also fail transiently (rare). Single retry
                # without backoff — the .NET state is local; retry is cheap.
                try:
                    target = self._resolve_target_folder(folder_cache, pst, item)
                    target.add_mapi_message_item(mapi_item)
                except Exception as insert_exc:
                    logger.warning(
                        "first add_mapi_message_item failed for %s: %s — retrying once",
                        ext_id, insert_exc,
                    )
                    try:
                        target = self._resolve_target_folder(folder_cache, pst, item)
                        target.add_mapi_message_item(mapi_item)
                    except Exception as exc2:
                        logger.error("PST insert FAILED for %s: %s", ext_id, exc2)
                        result.failed_count += 1
                        continue

                size_estimate += getattr(item, "content_size", 0) or 1024
                result.item_count += 1

                if size_estimate >= split_bytes:
                    pst.__exit__(None, None, None)
                    result.pst_paths.append(pst_path)
                    part += 1
                    stem = group.pst_filename[:-4]
                    pst_path = workdir / f"{stem}-{part:03d}.pst"
                    pst = _open_pst(pst_path)
                    folder_cache = self._build_folder_cache(pst)
                    size_estimate = 0
        finally:
            pst.__exit__(None, None, None)
            result.pst_paths.append(pst_path)
        return result

    def _build_folder_cache(self, pst) -> dict:
        """Initialise the folder cache for a freshly-created PST.

        The cache maps normalised path strings → Aspose folder handles. The
        empty-string key points at the user-visible root ('Top of Personal
        Folders'). For predefined Outlook folders (Inbox / Sent Items /
        Drafts / Deleted Items / Calendar / Contacts) we pre-create the
        STANDARD typed variant so Outlook recognises them as native folders;
        the cache aliases their canonical name → that handle.
        """
        import importlib
        aspose_pst = importlib.import_module("aspose.email.storage.pst")
        StandardIpmFolder = aspose_pst.StandardIpmFolder

        cache: dict = {}
        # Seed with the PST root so items lacking folder_path still land
        # somewhere sensible.
        root = pst.root_folder
        cache[""] = root

        # Pre-create the type-appropriate predefined folder so Outlook gets
        # the correct icon / IPM class. Per-type writers override
        # ``standard_folder_type``; the predefined call returns the folder.
        type_map = {
            "Inbox": StandardIpmFolder.INBOX,
            "Sent Items": StandardIpmFolder.SENT_ITEMS,
            "Drafts": StandardIpmFolder.DRAFTS,
            "Deleted Items": StandardIpmFolder.DELETED_ITEMS,
            "Outbox": StandardIpmFolder.OUTBOX,
            "Junk Email": StandardIpmFolder.JUNK_EMAIL,
            "Calendar": StandardIpmFolder.APPOINTMENTS,
            "Contacts": StandardIpmFolder.CONTACTS,
            "Tasks": StandardIpmFolder.TASKS,
            "Notes": StandardIpmFolder.NOTES,
            "Journal": StandardIpmFolder.JOURNAL,
        }
        try:
            primary = pst.create_predefined_folder(
                self.standard_folder_type,
                type_map.get(self.standard_folder_type, StandardIpmFolder.INBOX),
            )
            cache[self.standard_folder_type] = primary
        except Exception:
            pass
        return cache

    def _resolve_target_folder(self, cache: dict, pst, item):
        """Walk ``item.folder_path`` from the personal-folders root,
        creating sub-folders on demand. Promotes any segment that matches
        a standard Outlook folder name to its predefined typed variant
        the first time it's seen, so 'Deleted Items' lands as the actual
        Outlook Deleted Items folder rather than a generic subfolder.
        """
        import importlib
        aspose_pst = importlib.import_module("aspose.email.storage.pst")
        StandardIpmFolder = aspose_pst.StandardIpmFolder
        type_map = {
            "Inbox": StandardIpmFolder.INBOX,
            "Sent Items": StandardIpmFolder.SENT_ITEMS,
            "Drafts": StandardIpmFolder.DRAFTS,
            "Deleted Items": StandardIpmFolder.DELETED_ITEMS,
            "Outbox": StandardIpmFolder.OUTBOX,
            "Junk Email": StandardIpmFolder.JUNK_EMAIL,
            "Calendar": StandardIpmFolder.APPOINTMENTS,
            "Contacts": StandardIpmFolder.CONTACTS,
            "Tasks": StandardIpmFolder.TASKS,
            "Notes": StandardIpmFolder.NOTES,
            "Journal": StandardIpmFolder.JOURNAL,
        }

        raw = getattr(item, "folder_path", "") or ""
        # Normalise: strip leading/trailing slashes, split.
        parts = [p for p in str(raw).replace("\\", "/").strip("/").split("/") if p]
        if not parts:
            return cache.get(self.standard_folder_type) or cache[""]

        # Walk segments under the pst root, creating/typing each as needed.
        running = ""
        current = cache[""]
        for seg in parts:
            running = (running + "/" if running else "") + seg
            if running in cache:
                current = cache[running]
                continue
            # First-segment promotion to a predefined folder type when the
            # name matches a standard Outlook folder (Inbox, Sent Items, …).
            ipm_type = type_map.get(seg) if running == seg else None
            if ipm_type is not None:
                try:
                    current = pst.create_predefined_folder(seg, ipm_type)
                except Exception:
                    current = current.add_sub_folder(seg)
            else:
                # Custom or nested folder — plain sub-folder.
                try:
                    current = current.add_sub_folder(seg)
                except Exception as exc:
                    logger.error("add_sub_folder(%s) failed: %s", seg, exc)
                    return cache.get(self.standard_folder_type) or cache[""]
            cache[running] = current
        return current

    def _get_or_create_folder(self, pst, folder_name: str):
        """Create or retrieve a standard Outlook folder by name.

        Tries ``create_predefined_folder`` for Inbox/Calendar/Contacts so
        Outlook recognises the folder as a standard IPM type, then falls
        back to plain ``create_folder``.
        """
        import importlib
        aspose_pst = importlib.import_module("aspose.email.storage.pst")
        try:
            StandardIpmFolder = aspose_pst.StandardIpmFolder
            folder_map = {
                "Inbox": StandardIpmFolder.INBOX,
                "Calendar": StandardIpmFolder.APPOINTMENTS,
                "Contacts": StandardIpmFolder.CONTACTS,
            }
            if folder_name in folder_map:
                return pst.create_predefined_folder(folder_name, folder_map[folder_name])
        except Exception:
            pass
        return pst.create_folder(folder_name)

    async def _build_mapi_item(self, item, shard, source_container: str):
        """Override in subclass.  Returns a MAPI item or ``None`` on failure."""
        raise NotImplementedError
