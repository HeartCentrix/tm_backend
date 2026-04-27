"""Unit tests for ``pst_writers.mail.MailPstWriter._build_mapi_item``.

The MailPstWriter performs three operations per item:
  1. fetch the message JSON (mail_fetch.fetch_message)
  2. fetch attachments (mail_fetch.gather_attachments)
  3. build EML bytes via shared.mime_builder
  4. convert EML to MapiMessage via aspose

aspose-email is NOT installed in the test environment.  We stub the
``aspose.email.mapi`` module in ``sys.modules`` BEFORE importing the
writer, then patch the helpers to return controllable values and assert
the build path.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Path / sys.modules setup — must happen before importing the writer.
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


# Stub `aspose.email.mapi` so `importlib.import_module("aspose.email.mapi")`
# resolves to a MagicMock that records calls.
#
# IMPORTANT: do NOT pre-register `aspose` or `aspose.email` here.
# tests/shared/test_aspose_license.py registers those itself with
# `sys.modules.setdefault(...)` and ALSO holds a local reference to its mock
# under the name `_aspose_email_mock`. If we register `aspose.email` first
# then their setdefault becomes a no-op while their local reference still
# points to a different (orphaned) MagicMock — breaking their monkeypatching
# of `License`. Letting them register their mock first keeps both test
# files independent. We only need `aspose.email.mapi`, which they don't use.
_aspose_mapi_mock = sys.modules.setdefault("aspose.email.mapi", MagicMock())


# ---------------------------------------------------------------------------
# Load the pst_writers package without triggering restore-worker bootstrap.
# `workers/restore-worker` is hyphenated, so we register it as a package
# path manually under the name `pst_writers`.
# ---------------------------------------------------------------------------

# Define `pst_writers` as a namespace package pointing at the worker dir.
_PST_WRITERS_DIR = os.path.join(_WORKER, "pst_writers")
_pkg = types.ModuleType("pst_writers")
_pkg.__path__ = [_PST_WRITERS_DIR]
sys.modules["pst_writers"] = _pkg

_base_spec = importlib.util.spec_from_file_location(
    "pst_writers.base", os.path.join(_PST_WRITERS_DIR, "base.py")
)
_base_mod = importlib.util.module_from_spec(_base_spec)
sys.modules["pst_writers.base"] = _base_mod
_base_spec.loader.exec_module(_base_mod)

_mail_spec = importlib.util.spec_from_file_location(
    "pst_writers.mail", os.path.join(_PST_WRITERS_DIR, "mail.py")
)
_mail_mod = importlib.util.module_from_spec(_mail_spec)
sys.modules["pst_writers.mail"] = _mail_mod
_mail_spec.loader.exec_module(_mail_mod)

MailPstWriter = _mail_mod.MailPstWriter
PstWriterBase = _base_mod.PstWriterBase


# ---------------------------------------------------------------------------
# Class-level constants
# ---------------------------------------------------------------------------

def test_mail_writer_class_constants():
    """item_type and standard_folder_type are wired correctly."""
    assert MailPstWriter.item_type == "EMAIL"
    assert MailPstWriter.standard_folder_type == "Inbox"
    assert issubclass(MailPstWriter, PstWriterBase)


# ---------------------------------------------------------------------------
# _build_mapi_item — behavior tests
# ---------------------------------------------------------------------------

class _Item:
    def __init__(self, ext_id="m1", attachment_blob_paths=None):
        self.external_id = ext_id
        self.attachment_blob_paths = attachment_blob_paths or []


@pytest.mark.asyncio
async def test_build_mapi_item_returns_none_when_fetch_message_returns_none():
    writer = MailPstWriter()
    item = _Item()

    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=None)) as fetch_mock, \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=[])) as gather_mock, \
         patch("shared.mime_builder.build_eml") as build_eml_mock:
        result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is None
    fetch_mock.assert_awaited_once()
    # When fetch returns None we short-circuit before gather/build_eml
    gather_mock.assert_not_awaited()
    build_eml_mock.assert_not_called()


@pytest.mark.asyncio
async def test_build_mapi_item_calls_build_eml_when_no_attachments():
    """No attachments → use synchronous build_eml, not the streaming variant."""
    writer = MailPstWriter()
    item = _Item(attachment_blob_paths=[])
    msg_json = {"id": "m1", "subject": "hi"}
    eml_bytes = b"From: a@x\r\nSubject: hi\r\n\r\nbody"

    fake_mapi_obj = MagicMock(name="MapiMessage_instance")
    _aspose_mapi_mock.MapiMessage = MagicMock()
    _aspose_mapi_mock.MapiMessage.from_mime_message_bytes = MagicMock(return_value=fake_mapi_obj)

    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=msg_json)), \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=[])), \
         patch("shared.mime_builder.build_eml", return_value=eml_bytes) as build_eml_mock, \
         patch("shared.mime_builder.build_eml_streaming") as streaming_mock:
        result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_mapi_obj
    build_eml_mock.assert_called_once_with(msg_json, attachments=[])
    streaming_mock.assert_not_called()
    _aspose_mapi_mock.MapiMessage.from_mime_message_bytes.assert_called_once_with(eml_bytes)


@pytest.mark.asyncio
async def test_build_mapi_item_returns_mapi_from_mime_message_bytes_result():
    """Verify we return whatever MapiMessage.from_mime_message_bytes returned."""
    writer = MailPstWriter()
    item = _Item()
    msg_json = {"id": "m1"}
    eml_bytes = b"raw eml bytes"
    sentinel = object()

    _aspose_mapi_mock.MapiMessage = MagicMock()
    _aspose_mapi_mock.MapiMessage.from_mime_message_bytes = MagicMock(return_value=sentinel)

    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=msg_json)), \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=[])), \
         patch("shared.mime_builder.build_eml", return_value=eml_bytes):
        result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is sentinel


@pytest.mark.asyncio
async def test_build_mapi_item_uses_streaming_builder_when_attachments_present():
    """With attachments returned by gather_attachments → build_eml_streaming."""
    writer = MailPstWriter()
    item = _Item(attachment_blob_paths=["a.bin", "b.bin"])
    msg_json = {"id": "m1", "subject": "att-test"}

    # Fake AttachmentRef-like objects (only truthiness matters for the branch).
    fake_attachments = [object(), object()]

    async def _streaming_gen(_msg, _atts):
        yield b"part-1-"
        yield b"part-2"

    fake_mapi_obj = MagicMock(name="MapiMessage_instance")
    _aspose_mapi_mock.MapiMessage = MagicMock()
    _aspose_mapi_mock.MapiMessage.from_mime_message_bytes = MagicMock(return_value=fake_mapi_obj)

    with patch("mail_fetch.fetch_message", new=AsyncMock(return_value=msg_json)), \
         patch("mail_fetch.gather_attachments", new=AsyncMock(return_value=fake_attachments)), \
         patch("shared.mime_builder.build_eml") as sync_mock, \
         patch("shared.mime_builder.build_eml_streaming", side_effect=_streaming_gen) as streaming_mock:
        result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_mapi_obj
    sync_mock.assert_not_called()
    streaming_mock.assert_called_once()
    # Streaming mock was passed (msg_json, attachments)
    args, _kwargs = streaming_mock.call_args
    assert args[0] == msg_json
    assert args[1] == fake_attachments
    # Final EML bytes = concatenation of yielded chunks
    _aspose_mapi_mock.MapiMessage.from_mime_message_bytes.assert_called_once_with(
        b"part-1-part-2"
    )
