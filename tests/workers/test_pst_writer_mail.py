"""Unit tests for ``pst_writers.mail.MailPstWriter._collect_graph_item``.

The Aspose.Email-based per-item MAPI build was retired; the writer now
returns a Microsoft Graph message JSON dict (with attachment bytes
inlined as base64) for the base class to feed to ``pst_convert``.

These tests don't run any C++ — they exercise the Python collect path.
"""
from __future__ import annotations

import base64
import importlib.util
import os
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Path setup — must happen before importing the writer.
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(
    os.path.join(_HERE, "..", "..", "workers", "restore-worker")
)
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# Load the pst_writers package without triggering restore-worker bootstrap.
_PST_WRITERS_DIR = os.path.join(_WORKER, "pst_writers")
if "pst_writers" not in sys.modules:
    _pkg = types.ModuleType("pst_writers")
    _pkg.__path__ = [_PST_WRITERS_DIR]
    sys.modules["pst_writers"] = _pkg

if "pst_writers.base" not in sys.modules:
    _base_spec = importlib.util.spec_from_file_location(
        "pst_writers.base", os.path.join(_PST_WRITERS_DIR, "base.py")
    )
    _base_mod = importlib.util.module_from_spec(_base_spec)
    sys.modules["pst_writers.base"] = _base_mod
    _base_spec.loader.exec_module(_base_mod)
else:
    _base_mod = sys.modules["pst_writers.base"]

_mail_spec = importlib.util.spec_from_file_location(
    "pst_writers.mail", os.path.join(_PST_WRITERS_DIR, "mail.py")
)
_mail_mod = importlib.util.module_from_spec(_mail_spec)
sys.modules["pst_writers.mail"] = _mail_mod
_mail_spec.loader.exec_module(_mail_mod)

MailPstWriter = _mail_mod.MailPstWriter
PstWriterBase = _base_mod.PstWriterBase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Item:
    def __init__(self, ext_id="m1", attachment_blob_paths=None):
        self.external_id = ext_id
        self.attachment_blob_paths = attachment_blob_paths or []


class _AttachmentRef:
    """Mimics shared.mime_builder.AttachmentRef just enough for the test."""
    def __init__(self, name: str, content_type: str, payload: bytes):
        self.name = name
        self.content_type = content_type
        self._payload = payload

    @property
    def data_stream(self):
        async def _gen(p=self._payload):
            yield p
        return _gen()


# ---------------------------------------------------------------------------
# Class wiring
# ---------------------------------------------------------------------------

def test_class_constants():
    assert MailPstWriter.item_type == "EMAIL"
    assert MailPstWriter.cli_kind == "mail"
    assert issubclass(MailPstWriter, PstWriterBase)


# ---------------------------------------------------------------------------
# _collect_graph_item — behavior tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_none_when_fetch_returns_none():
    writer = MailPstWriter()
    item = _Item()
    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=None)) as fetch_mock, \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=[])) as gather_mock:
        result = await writer._collect_graph_item(item, shard=MagicMock(), source_container="c")
    assert result is None
    fetch_mock.assert_awaited_once()
    gather_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_passes_through_message_json_when_no_attachments():
    """No attachment_blob_paths → no gather_attachments call, msg_json returned verbatim (as a copy)."""
    writer = MailPstWriter()
    item = _Item(attachment_blob_paths=[])
    msg_json = {"id": "m1", "subject": "hi", "body": {"contentType": "text", "content": "yo"}}

    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=msg_json)), \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=[])) as gather_mock:
        result = await writer._collect_graph_item(item, shard=MagicMock(), source_container="c")

    assert result == msg_json
    # Defensive copy — mutating result must not touch caller's dict.
    assert result is not msg_json
    gather_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_inlines_attachments_as_base64():
    """With attachment_blob_paths → drains each ref and inlines as
    {@odata.type, name, contentType, contentBytes (b64), size}."""
    writer = MailPstWriter()
    item = _Item(attachment_blob_paths=["folder/a.bin", "folder/b.txt"])
    msg_json = {"id": "m1", "subject": "att test"}

    payload_a = b"\x00\x01binary"
    payload_b = b"hello world"
    refs = [
        _AttachmentRef("a.bin", "application/octet-stream", payload_a),
        _AttachmentRef("b.txt", "text/plain", payload_b),
    ]

    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=msg_json)), \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=refs)):
        result = await writer._collect_graph_item(item, shard=MagicMock(), source_container="c")

    assert result is not None
    assert result["hasAttachments"] is True
    assert len(result["attachments"]) == 2

    a, b = result["attachments"]
    assert a["@odata.type"] == "#microsoft.graph.fileAttachment"
    assert a["name"] == "a.bin"
    assert a["contentType"] == "application/octet-stream"
    assert base64.b64decode(a["contentBytes"]) == payload_a
    assert a["size"] == len(payload_a)

    assert b["name"] == "b.txt"
    assert b["contentType"] == "text/plain"
    assert base64.b64decode(b["contentBytes"]) == payload_b


@pytest.mark.asyncio
async def test_skips_oversize_attachment_via_max_bytes_guard(monkeypatch):
    """Attachments larger than MAX_ATTACHMENT_BYTES get logged + dropped
    rather than ballooning the JSON payload."""
    monkeypatch.setattr(_mail_mod, "MAX_ATTACHMENT_BYTES", 16)
    writer = MailPstWriter()
    item = _Item(attachment_blob_paths=["a", "b"])
    msg_json = {"id": "m1"}

    refs = [
        _AttachmentRef("small.txt", "text/plain", b"tiny"),       # 4 bytes — OK
        _AttachmentRef("huge.bin", "application/octet-stream",
                       b"x" * 1024),                               # 1KB — dropped
    ]
    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=msg_json)), \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=refs)):
        result = await writer._collect_graph_item(item, shard=MagicMock(), source_container="c")

    names = [a["name"] for a in result["attachments"]]
    assert names == ["small.txt"]


@pytest.mark.asyncio
async def test_gather_failure_does_not_kill_message():
    """gather_attachments raising should still let the message ship without atts."""
    writer = MailPstWriter()
    item = _Item(attachment_blob_paths=["a.bin"])
    msg_json = {"id": "m1", "subject": "hi"}

    async def _boom(*_a, **_kw):
        raise RuntimeError("blob shard offline")

    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=msg_json)), \
         patch("mail_fetch.gather_attachments", new=_boom):
        result = await writer._collect_graph_item(item, shard=MagicMock(), source_container="c")

    assert result == msg_json
    assert "attachments" not in result   # writer didn't add an empty list


# ---------------------------------------------------------------------------
# write() integration — verifies the base class shells out to the CLI wrapper
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_dispatches_to_pst_convert(tmp_path):
    """End-to-end (with the CLI subprocess mocked) — base.write() collects,
    chunks, and calls shared.pstwriter_cli.convert_to_pst per chunk."""
    writer = MailPstWriter()
    items = [_Item(ext_id=f"m{i}") for i in range(3)]
    msg_jsons = [{"id": f"m{i}", "subject": f"s{i}"} for i in range(3)]

    # Build a fake group that mirrors PstGroup's shape.
    group = MagicMock()
    group.items = items
    group.pst_filename = "fake-mail.pst"
    group.key = ("snap", "EMAIL")

    async def _fetch_seq(item, *_a, **_kw):
        return msg_jsons[int(item.external_id[1:])]

    captured: list[tuple[str, list[dict]]] = []

    async def _fake_convert(kind, graph_items, output, **_kw):
        items_list = list(graph_items)
        captured.append((kind, items_list))
        # Touch the output path so the writer's existence check passes.
        output.write_bytes(b"PSTSTUB")
        return len(items_list)

    with patch("mail_fetch.fetch_message", new=AsyncMock(side_effect=_fetch_seq)), \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=[])), \
         patch("shared.pstwriter_cli.convert_to_pst", side_effect=_fake_convert):
        result = await writer.write(group=group, workdir=tmp_path, split_gb=45.0)

    assert result.item_count == 3
    assert result.failed_count == 0
    assert len(result.pst_paths) == 1
    assert result.pst_paths[0].name == "fake-mail.pst"
    assert len(captured) == 1
    assert captured[0][0] == "mail"
    assert [c["id"] for c in captured[0][1]] == ["m0", "m1", "m2"]
