"""Async wrapper around the bundled ``pst_convert`` CLI.

Replaces the old Aspose.Email-based writer path. The restore-worker
Docker image bakes the binary at ``/usr/local/bin/pst_convert`` (built
in the multi-stage Dockerfile from ``vendor/pstwriter``); the path is
overridable via the ``PSTWRITER_CLI`` env var so local dev can point at
``d:/Work Qfion/PST Dev/build/gcc/bin/pst_convert.exe``.

The CLI signature is:

    pst_convert <kind> <input.json> <output.pst>
    kind ∈ {mail, contacts, calendar}

Input JSON accepts a single Graph object, a bare array, or a Graph list
envelope ``{"value": [...]}``. We always emit the envelope form so the
caller doesn't have to care.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)


class PstWriterCliError(RuntimeError):
    """Raised when ``pst_convert`` exits non-zero or cannot be launched."""

    def __init__(self, message: str, *, returncode: int | None = None,
                 stderr: str = "", stdout: str = ""):
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout


_VALID_KINDS = {"mail", "contacts", "calendar"}

# Default in the worker image; override via env. Resolved per-call so
# tests can monkeypatch the env var between runs.
_DEFAULT_BIN = "/usr/local/bin/pst_convert"

# Wall-clock cap on a single CLI invocation. Overridable via env so an
# operator can raise it for whale exports without a code change.
_DEFAULT_TIMEOUT_S = int(os.environ.get("PSTWRITER_CLI_TIMEOUT_S", "3600"))


def _resolve_binary() -> str:
    """Look up the pst_convert binary path. Honours ``PSTWRITER_CLI``;
    falls back to whichever ``pst_convert`` is on PATH; finally to the
    image's baked-in location. Raises FileNotFoundError if none works."""
    override = os.environ.get("PSTWRITER_CLI", "").strip()
    if override:
        if not os.path.exists(override):
            raise FileNotFoundError(
                f"PSTWRITER_CLI={override!r} does not exist on disk"
            )
        return override
    found = shutil.which("pst_convert")
    if found:
        return found
    if os.path.exists(_DEFAULT_BIN):
        return _DEFAULT_BIN
    raise FileNotFoundError(
        "pst_convert binary not found. Set PSTWRITER_CLI or rebuild the "
        "restore-worker image (vendor/pstwriter must compile)."
    )


async def convert_to_pst(
    kind: str,
    graph_items: Iterable[dict],
    output_path: Path,
    *,
    workdir: Path | None = None,
    timeout_s: int | None = None,
) -> int:
    """Run ``pst_convert <kind>`` over ``graph_items`` → ``output_path``.

    Parameters
    ----------
    kind:
        One of ``mail``, ``contacts``, ``calendar``.
    graph_items:
        Iterable of Graph JSON dicts. Wrapped in ``{"value": [...]}``
        before serialisation so the CLI's list path is taken.
    output_path:
        Where to write the produced ``.pst``. Parent directory must exist.
    workdir:
        Where to drop the temp ``.json`` payload. Defaults to
        ``output_path.parent`` so cleanup happens alongside the artefact.
    timeout_s:
        Overrides ``PSTWRITER_CLI_TIMEOUT_S`` for this call.

    Returns
    -------
    int
        Number of items written (length of ``graph_items``).

    Raises
    ------
    PstWriterCliError
        Non-zero exit or process spawn failure.
    asyncio.TimeoutError
        Wall-clock cap exceeded.
    """
    if kind not in _VALID_KINDS:
        raise ValueError(f"unknown kind {kind!r}; expected one of {_VALID_KINDS}")

    items = list(graph_items)
    if not items:
        raise ValueError("convert_to_pst called with no items")

    bin_path = _resolve_binary()
    work = Path(workdir) if workdir else output_path.parent
    work.mkdir(parents=True, exist_ok=True)

    # Use NamedTemporaryFile for an atomic, distinct path even when many
    # writers run concurrently in one worker. delete=False so we control
    # the unlink in the finally block (Windows can't unlink an open file).
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", dir=str(work),
        delete=False, encoding="utf-8",
    )
    json_path = Path(tmp.name)
    try:
        try:
            json.dump({"value": items}, tmp, ensure_ascii=False)
        finally:
            tmp.close()

        cmd = [bin_path, kind, str(json_path), str(output_path)]
        logger.info(
            "[pstwriter_cli] launching: %s items=%d out=%s",
            " ".join(cmd[:2] + ["<json>", str(output_path)]), len(items),
            output_path.name,
        )

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=timeout_s or _DEFAULT_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise
        stdout = stdout_b.decode("utf-8", errors="replace")
        stderr = stderr_b.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            logger.error(
                "[pstwriter_cli] FAILED rc=%s kind=%s stderr=%s",
                proc.returncode, kind, stderr.strip()[:500],
            )
            raise PstWriterCliError(
                f"pst_convert {kind} exited rc={proc.returncode}",
                returncode=proc.returncode, stderr=stderr, stdout=stdout,
            )

        if not output_path.exists():
            raise PstWriterCliError(
                f"pst_convert {kind} returned 0 but output {output_path} missing",
                returncode=proc.returncode, stderr=stderr, stdout=stdout,
            )
        logger.info(
            "[pstwriter_cli] OK kind=%s items=%d size=%d out=%s",
            kind, len(items), output_path.stat().st_size, output_path.name,
        )
        return len(items)
    finally:
        try:
            json_path.unlink(missing_ok=True)
        except Exception:
            pass


def cli_available() -> bool:
    """Cheap probe used by tests + healthchecks. Never raises."""
    try:
        _resolve_binary()
        return True
    except FileNotFoundError:
        return False
