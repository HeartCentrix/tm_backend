"""Tests for deleted/recoverable contact backup expansion."""
import importlib
import importlib.util
import sys
from pathlib import Path


def _load_module(name: str, path: Path):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    if str(path.parent) not in sys.path:
        sys.path.insert(0, str(path.parent))
    spec.loader.exec_module(mod)
    return mod


_BACKUP_MAIN_PATH = (
    Path(__file__).resolve().parents[2] / "workers" / "backup-worker" / "main.py"
)
main = _load_module("backup_worker_main", _BACKUP_MAIN_PATH)
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


# ───── Integration tests for _backup_contacts_for_user ─────

import pytest


class _StubGraphClient:
    """In-memory stand-in for GraphClient. Routes URLs to canned responses."""
    GRAPH_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, responses):
        self._responses = responses
        self.calls = []

    async def _get(self, url, params=None):
        self.calls.append(url)
        for prefix, resp in self._responses.items():
            if prefix in url:
                return resp
        return {"value": []}


def _live_contact(cid, name, folder_id):
    return {"id": cid, "displayName": name, "parentFolderId": folder_id}


def _ipm_contact_msg(mid, subject):
    return {"id": mid, "subject": subject, "itemClass": "IPM.Contact"}


def _reload_main_with_env(monkeypatch, env):
    """Re-execute shared.config + worker main against new env values.
    `importlib.reload` doesn't work on modules loaded via spec_from_file_location
    because Python's normal finder can't relocate them, so we re-run the spec."""
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import shared.config as cfg
    importlib.reload(cfg)
    spec = importlib.util.spec_from_file_location(
        "backup_worker_main", _BACKUP_MAIN_PATH
    )
    new_mod = importlib.util.module_from_spec(spec)
    sys.modules["backup_worker_main"] = new_mod
    spec.loader.exec_module(new_mod)
    return new_mod


@pytest.mark.asyncio
async def test_backup_includes_deleted_items_contacts(monkeypatch):
    m = _reload_main_with_env(monkeypatch, {
        "BACKUP_CONTACTS_INCLUDE_DELETED": "true",
        "BACKUP_CONTACTS_INCLUDE_RECOVERABLE": "false",
    })
    graph = _StubGraphClient({
        "/contactFolders": {"value": [{"id": "F1", "displayName": "Contacts"}]},
        "/users/u1/contacts": {"value": [_live_contact("C1", "Live Jane", "F1")]},
        "/contactFolders/F1/contacts": {"value": [_live_contact("C1", "Live Jane", "F1")]},
        "/mailFolders('deleteditems')/messages": {
            "value": [_ipm_contact_msg("M1", "Deleted Bob")]
        },
    })
    rows = await m._backup_contacts_for_user(graph, "u1", item_limit=999)
    by_name = {r[1]: r for r in rows}
    assert "Live Jane" in by_name
    assert "Deleted Bob" in by_name
    assert by_name["Deleted Bob"][4] == "Deleted Items"


@pytest.mark.asyncio
async def test_backup_skips_deleted_when_flag_off(monkeypatch):
    m = _reload_main_with_env(monkeypatch, {
        "BACKUP_CONTACTS_INCLUDE_DELETED": "false",
        "BACKUP_CONTACTS_INCLUDE_RECOVERABLE": "false",
    })
    graph = _StubGraphClient({
        "/contactFolders": {"value": []},
        "/users/u1/contacts": {"value": []},
        "/mailFolders('deleteditems')/messages": {
            "value": [_ipm_contact_msg("M1", "Should not appear")]
        },
    })
    rows = await m._backup_contacts_for_user(graph, "u1", item_limit=999)
    assert all(r[1] != "Should not appear" for r in rows)
    assert not any("deleteditems" in c for c in graph.calls)


@pytest.mark.asyncio
async def test_backup_includes_recoverable_when_flag_on(monkeypatch):
    m = _reload_main_with_env(monkeypatch, {
        "BACKUP_CONTACTS_INCLUDE_DELETED": "false",
        "BACKUP_CONTACTS_INCLUDE_RECOVERABLE": "true",
    })
    graph = _StubGraphClient({
        "/contactFolders": {"value": []},
        "/users/u1/contacts": {"value": []},
        "/mailFolders('recoverableitemsdeletions')/messages": {
            "value": [_ipm_contact_msg("M2", "Recoverable Carol")]
        },
    })
    rows = await m._backup_contacts_for_user(graph, "u1", item_limit=999)
    assert any(r[1] == "Recoverable Carol" and r[4] == "Recoverable Items" for r in rows)


@pytest.mark.asyncio
async def test_dedup_when_same_id_in_both_live_and_deleted(monkeypatch):
    """Live wins over deleted when ids collide (rare, defensive)."""
    m = _reload_main_with_env(monkeypatch, {
        "BACKUP_CONTACTS_INCLUDE_DELETED": "true",
        "BACKUP_CONTACTS_INCLUDE_RECOVERABLE": "false",
    })
    graph = _StubGraphClient({
        "/contactFolders": {"value": [{"id": "F1", "displayName": "Contacts"}]},
        "/users/u1/contacts": {"value": [_live_contact("DUP", "Live", "F1")]},
        "/contactFolders/F1/contacts": {"value": [_live_contact("DUP", "Live", "F1")]},
        "/mailFolders('deleteditems')/messages": {
            "value": [_ipm_contact_msg("DUP", "Deleted Dup")]
        },
    })
    rows = await m._backup_contacts_for_user(graph, "u1", item_limit=999)
    dup_rows = [r for r in rows if r[2] == "DUP"]
    assert len(dup_rows) == 1
    assert dup_rows[0][1] == "Live"
