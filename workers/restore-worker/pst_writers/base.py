"""Base class for per-type PST writers.

Concrete writers (``MailPstWriter``, ``CalendarPstWriter``,
``ContactPstWriter``) override ``_build_mapi_item`` to translate one
SnapshotItem into an aspose-email MAPI item.  The base class handles
PST creation, folder creation, item insertion, and size-driven rotation
into multiple ``.pst`` parts when the writer crosses ``split_gb``.

All ``aspose.*`` imports are lazy (inside methods, via
``importlib.import_module``) because aspose-email is not installed in
the dev/test environment.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


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
        pst = PersonalStorage.create(str(pst_path), FileFormatVersion.UNICODE)
        folder = self._get_or_create_folder(pst, self.standard_folder_type)
        size_estimate = 0
        split_bytes = split_gb * 0.95 * 1024 ** 3

        try:
            for item in group.items:
                try:
                    mapi_item = await self._build_mapi_item(item, shard, source_container)
                    if mapi_item is None:
                        result.failed_count += 1
                        continue
                    folder.add_mapi_message_item(mapi_item)
                    size_estimate += getattr(item, "content_size", 0) or 1024
                    result.item_count += 1

                    # Rotate when nearing split threshold
                    if size_estimate >= split_bytes:
                        pst.dispose()
                        result.pst_paths.append(pst_path)
                        part += 1
                        stem = group.pst_filename[:-4]   # strip ".pst"
                        base = f"{stem}-{part:03d}.pst"
                        pst_path = workdir / base
                        pst = PersonalStorage.create(str(pst_path), FileFormatVersion.UNICODE)
                        folder = self._get_or_create_folder(pst, self.standard_folder_type)
                        size_estimate = 0

                except Exception as exc:
                    logger.error(
                        "PST write failed for item %s: %s",
                        getattr(item, "external_id", "?"),
                        exc,
                    )
                    result.failed_count += 1
        finally:
            pst.dispose()
            result.pst_paths.append(pst_path)
        return result

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
