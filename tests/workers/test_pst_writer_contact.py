"""Unit tests for ``pst_writers.contact.ContactPstWriter._collect_graph_item``.

The post-Aspose writer is a pass-through: it returns the Graph contact
JSON stored on the SnapshotItem and lets ``pst_convert`` build the PST.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from unittest.mock import MagicMock

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

_contact_spec = importlib.util.spec_from_file_location(
    "pst_writers.contact", os.path.join(_PST_WRITERS_DIR, "contact.py")
)
_contact_mod = importlib.util.module_from_spec(_contact_spec)
sys.modules["pst_writers.contact"] = _contact_mod
_contact_spec.loader.exec_module(_contact_mod)

ContactPstWriter = _contact_mod.ContactPstWriter
PstWriterBase = _base_mod.PstWriterBase


class _Item:
    def __init__(self, ext_id="contact-1", extra_data=None):
        self.external_id = ext_id
        self.extra_data = extra_data or {}


# ---------------------------------------------------------------------------
# Class wiring
# ---------------------------------------------------------------------------

def test_class_constants():
    assert ContactPstWriter.item_type == "USER_CONTACT"
    assert ContactPstWriter.cli_kind == "contacts"
    assert issubclass(ContactPstWriter, PstWriterBase)


# ---------------------------------------------------------------------------
# _collect_graph_item — behavior tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_raw_when_present():
    raw = {
        "id": "c1",
        "displayName": "Ada Lovelace",
        "givenName": "Ada",
        "surname": "Lovelace",
        "emailAddresses": [{"address": "ada@example.com", "name": "Ada"}],
        "businessPhones": ["+1-555-0100"],
    }
    item = _Item(extra_data={"raw": raw})
    out = await ContactPstWriter()._collect_graph_item(item, shard=MagicMock(), source_container="c")
    assert out is raw


@pytest.mark.asyncio
async def test_returns_none_when_missing_raw():
    item = _Item(extra_data={})
    out = await ContactPstWriter()._collect_graph_item(item, shard=MagicMock(), source_container="c")
    assert out is None


@pytest.mark.asyncio
async def test_returns_none_when_extra_data_is_none():
    item = _Item()
    item.extra_data = None
    out = await ContactPstWriter()._collect_graph_item(item, shard=MagicMock(), source_container="c")
    assert out is None
