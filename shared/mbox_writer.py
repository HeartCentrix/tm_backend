"""mboxrd writer with From-escape + size-based rollover.

mboxrd is the RFC 976 variant where body lines beginning with one or more '>'
followed by 'From ' get an additional '>' prepended. This is the widest-compat
flavor — Thunderbird, Apple Mail, Aid4Mail, and mbox-utils all accept it.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Callable, Optional


_FROM_LINE_RE = re.compile(rb"^(>*)From ", re.MULTILINE)


def _escape_from_lines(body: bytes) -> bytes:
    """Prepend '>' to any line beginning with '>*From '. Runs in one regex pass."""
    return _FROM_LINE_RE.sub(lambda m: b">" + m.group(0), body)


def _format_from_separator(sender_addr: str, sent_at_epoch: int) -> bytes:
    """Build the 'From {addr} {asctime}' separator. Uses UTC for determinism."""
    dt = datetime.fromtimestamp(sent_at_epoch, tz=timezone.utc)
    asctime = dt.strftime("%a %b %d %H:%M:%S %Y")
    return f"From {sender_addr} {asctime}\n".encode("ascii", errors="replace")


class MboxWriter:
    """Appends EML messages into an mboxrd stream. Delivers bytes to `emit` in
    chunks. Fires `on_rollover(new_index)` when cumulative size exceeds
    `split_bytes` — caller is responsible for switching destination and resetting."""

    def __init__(
        self,
        emit: Callable[[bytes], None],
        split_bytes: int,
        on_rollover: Optional[Callable[[int], None]] = None,
    ):
        self._emit = emit
        self._split_bytes = split_bytes
        self._on_rollover = on_rollover
        self._current_bytes = 0
        self._part_index = 0

    def append_message(self, eml_bytes: bytes, *, sender_addr: str, sent_at_epoch: int) -> None:
        separator = _format_from_separator(sender_addr, sent_at_epoch)
        escaped = _escape_from_lines(eml_bytes)
        if not escaped.endswith(b"\r\n"):
            escaped = escaped + b"\r\n"

        self._emit(separator)
        self._emit(escaped)
        self._current_bytes += len(separator) + len(escaped)

        if self._current_bytes >= self._split_bytes:
            self._part_index += 1
            if self._on_rollover:
                self._on_rollover(self._part_index)
            self._current_bytes = 0

    def close(self) -> None:
        """Emit final terminator. Called once at end of folder export."""
        self._emit(b"\r\n")
