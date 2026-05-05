"""Tests for ``shared.pstwriter_cli`` — the async wrapper around the
bundled ``pst_convert`` binary.

The C++ binary is not available in CI; we fake it with a tiny script
written into a tempdir and pointed at via the ``PSTWRITER_CLI`` env
override.
"""
from __future__ import annotations

import json
import os
import stat
import sys
import textwrap
from pathlib import Path

import pytest

from shared.pstwriter_cli import (
    PstWriterCliError,
    cli_available,
    convert_to_pst,
)


def _write_fake_cli(workdir: Path, *, exit_code: int = 0,
                    write_output: bool = True,
                    extra_lines: str = "") -> Path:
    """Create a tiny Python script that mimics pst_convert's CLI surface.

    Honours argv: ``<kind> <input.json> <output.pst>``. When
    ``write_output`` is true it touches the output path so the wrapper's
    existence check passes.
    """
    script = workdir / ("fake_pst_convert.py")
    script.write_text(textwrap.dedent(f"""
        #!{sys.executable}
        import sys, pathlib
        kind, in_json, out_pst = sys.argv[1], sys.argv[2], sys.argv[3]
        assert kind in {{"mail", "contacts", "calendar"}}, kind
        assert pathlib.Path(in_json).exists()
        {extra_lines}
        if {write_output!r}:
            pathlib.Path(out_pst).write_bytes(b"FAKEPST")
        sys.exit({exit_code})
    """).lstrip())
    # Wrap so we can invoke directly even on Windows where shebangs
    # aren't honoured: emit a sibling .cmd / .sh pointing at the python
    # script and return that. On POSIX we just chmod +x and return as-is.
    if os.name == "nt":
        wrapper = workdir / "fake_pst_convert.cmd"
        wrapper.write_text(
            f'@echo off\r\n"{sys.executable}" "{script}" %*\r\n'
        )
        return wrapper
    script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP)
    return script


@pytest.mark.asyncio
async def test_convert_to_pst_invokes_cli_and_returns_count(tmp_path, monkeypatch):
    fake = _write_fake_cli(tmp_path)
    monkeypatch.setenv("PSTWRITER_CLI", str(fake))

    out = tmp_path / "out.pst"
    items = [{"id": "m1", "subject": "hi"}, {"id": "m2", "subject": "yo"}]
    written = await convert_to_pst("mail", items, out, workdir=tmp_path)

    assert written == 2
    assert out.exists()
    assert out.read_bytes() == b"FAKEPST"


@pytest.mark.asyncio
async def test_convert_to_pst_serialises_envelope_and_cleans_temp(tmp_path, monkeypatch):
    """The CLI receives a {"value": [...]} envelope, and the temp .json
    is deleted on success regardless of platform."""
    fake = _write_fake_cli(
        tmp_path,
        extra_lines=(
            "import json, pathlib\n"
            "data = json.loads(pathlib.Path(in_json).read_text())\n"
            "assert isinstance(data, dict) and 'value' in data, data\n"
            "assert len(data['value']) == 1\n"
            "assert data['value'][0]['id'] == 'c1', data\n"
        ),
    )
    monkeypatch.setenv("PSTWRITER_CLI", str(fake))

    out = tmp_path / "contacts.pst"
    await convert_to_pst("contacts", [{"id": "c1"}], out, workdir=tmp_path)

    leftover_jsons = list(tmp_path.glob("*.json"))
    assert leftover_jsons == [], f"unexpected leftover temp files: {leftover_jsons}"


@pytest.mark.asyncio
async def test_convert_to_pst_raises_on_nonzero_exit(tmp_path, monkeypatch):
    fake = _write_fake_cli(tmp_path, exit_code=2, write_output=False)
    monkeypatch.setenv("PSTWRITER_CLI", str(fake))

    with pytest.raises(PstWriterCliError) as ei:
        await convert_to_pst("mail", [{"id": "m1"}], tmp_path / "out.pst",
                             workdir=tmp_path)
    assert ei.value.returncode == 2


@pytest.mark.asyncio
async def test_convert_to_pst_raises_when_output_missing(tmp_path, monkeypatch):
    """rc=0 but no output file → the wrapper raises so the caller can
    record a chunk failure instead of silently moving on."""
    fake = _write_fake_cli(tmp_path, exit_code=0, write_output=False)
    monkeypatch.setenv("PSTWRITER_CLI", str(fake))

    with pytest.raises(PstWriterCliError):
        await convert_to_pst("mail", [{"id": "m1"}], tmp_path / "out.pst",
                             workdir=tmp_path)


@pytest.mark.asyncio
async def test_convert_to_pst_rejects_bad_kind(tmp_path):
    with pytest.raises(ValueError):
        await convert_to_pst("bogus", [{"id": "m1"}], tmp_path / "x.pst")


@pytest.mark.asyncio
async def test_convert_to_pst_rejects_empty_items(tmp_path, monkeypatch):
    fake = _write_fake_cli(tmp_path)
    monkeypatch.setenv("PSTWRITER_CLI", str(fake))
    with pytest.raises(ValueError):
        await convert_to_pst("mail", [], tmp_path / "x.pst")


def test_cli_available_false_when_path_missing(monkeypatch, tmp_path):
    """Override pointed at a nonexistent path → cli_available() returns False."""
    monkeypatch.setenv("PSTWRITER_CLI", str(tmp_path / "definitely-not-here"))
    # Also clear the system PATH so the on-PATH fallback doesn't accidentally
    # find a real pst_convert.
    monkeypatch.setenv("PATH", "")
    assert cli_available() is False
