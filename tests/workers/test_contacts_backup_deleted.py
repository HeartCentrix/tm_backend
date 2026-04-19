"""Tests for deleted/recoverable contact backup expansion."""
import importlib
import sys
from pathlib import Path

WORKER_DIR = Path(__file__).resolve().parents[2] / "workers" / "backup-worker"
sys.path.insert(0, str(WORKER_DIR))

main = importlib.import_module("main")
_message_to_contact_shape = main._message_to_contact_shape


def test_message_to_contact_shape_minimal():
    """A bare IPM.Contact message in Deleted Items must produce a usable shape."""
    msg = {"id": "AAMkAG=", "subject": "Jane Doe", "itemClass": "IPM.Contact"}
    shape = _message_to_contact_shape(msg)
    assert shape["id"] == "AAMkAG="
    assert shape["displayName"] == "Jane Doe"
    assert isinstance(shape.get("emailAddresses"), list)


def test_message_to_contact_shape_uses_subject_as_display_name():
    msg = {"id": "x", "subject": "Bob Smith", "itemClass": "IPM.Contact"}
    assert _message_to_contact_shape(msg)["displayName"] == "Bob Smith"


def test_message_to_contact_shape_handles_missing_subject():
    msg = {"id": "x", "itemClass": "IPM.Contact"}
    shape = _message_to_contact_shape(msg)
    assert shape["displayName"] == "(deleted contact)"


def test_message_to_contact_shape_extracts_email_from_from_field():
    """Some deleted contacts retain a 'from' field with the contact's email."""
    msg = {
        "id": "x",
        "subject": "Jane",
        "from": {"emailAddress": {"address": "jane@x.com", "name": "Jane"}},
        "itemClass": "IPM.Contact",
    }
    shape = _message_to_contact_shape(msg)
    assert any(e.get("address") == "jane@x.com" for e in shape["emailAddresses"])


def test_message_to_contact_shape_passes_through_extended_props():
    """singleValueExtendedProperties survives unchanged for downstream use."""
    msg = {
        "id": "x", "subject": "Jane", "itemClass": "IPM.Contact",
        "singleValueExtendedProperties": [{"id": "X", "value": "Y"}],
    }
    shape = _message_to_contact_shape(msg)
    assert shape.get("singleValueExtendedProperties") == [{"id": "X", "value": "Y"}]
