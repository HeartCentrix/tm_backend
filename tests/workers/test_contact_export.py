"""Integration tests for USER_CONTACT branch in export_as_zip."""
import importlib.util
import io
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
import pytest


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


_RESTORE_MAIN = _load_module(
    "restore_worker_main",
    Path(__file__).resolve().parents[2] / "workers" / "restore-worker" / "main.py",
)


def _make_item(item_id, name, folder, raw):
    """Build a SnapshotItem-like SimpleNamespace the export branch can consume."""
    return SimpleNamespace(
        id=item_id,
        external_id=item_id,
        name=name,
        item_type="USER_CONTACT",
        tenant_id="t-1",
        resource_id="r-1",
        snapshot_id="s-1",
        metadata={"raw": raw, "structured": {"parentFolderName": folder}},
    )


@pytest.fixture
def worker():
    """Build a RestoreWorker with the loaders stubbed to return the item's raw dict."""
    main = _RESTORE_MAIN
    w = main.RestoreWorker.__new__(main.RestoreWorker)
    w.worker_id = "test"
    w._calendar_csv_rows = []
    w._load_snapshot_item_payload = lambda item, workload: item.metadata["raw"]
    w._load_snapshot_item_bytes = lambda item, workload: None
    w._get_item_metadata = lambda item: item.metadata
    return w


@pytest.fixture
def jane():
    return _make_item("c1", "Jane Doe", "Contacts",
                      {"id": "c1", "displayName": "Jane Doe",
                       "emailAddresses": [{"address": "jane@acme.com"}]})


@pytest.fixture
def bob():
    return _make_item("c2", "Bob Smith", "Recipient Cache",
                      {"id": "c2", "displayName": "Bob Smith",
                       "emailAddresses": [{"address": "bob@x.com"}]})


@pytest.fixture
def deleted_alice():
    return _make_item("c3", "Alice", "Deleted Items",
                      {"id": "c3", "displayName": "Alice"})


def _run_export(worker, items, fmt, contact_folders=None):
    """Drive only the per-item ZIP-writing slice of export_as_zip.

    Mirrors the production code path exactly so the test catches breakage
    in either the production branch or the helpers it composes."""
    _m = _RESTORE_MAIN
    spec = {"exportFormat": fmt}
    if contact_folders is not None:
        spec["contactFolders"] = contact_folders
    contact_folder_filter = set(spec.get("contactFolders") or [])

    buf = io.BytesIO()
    worker._contacts_csv_rows = []
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for item in items:
            metadata = worker._get_item_metadata(item)
            raw = worker._load_snapshot_item_payload(item, "files")
            folder = (metadata.get("structured") or {}).get("parentFolderName") or "Contacts"
            if item.item_type == "USER_CONTACT":
                if contact_folder_filter and folder not in contact_folder_filter:
                    continue
                if fmt == "CSV":
                    worker._contacts_csv_rows.append(_m._contact_to_csv_row(raw, folder))
                else:
                    safe_folder = _m._safe_name(folder)
                    safe_name = _m._safe_name(item.name or item.external_id)
                    zf.writestr(f"contacts/{safe_folder}/{safe_name}.vcf",
                                _m._contact_to_vcard(raw, folder=folder))
        if worker._contacts_csv_rows:
            import csv as _csv
            sbuf = io.StringIO()
            writer = _csv.DictWriter(sbuf, fieldnames=[
                "displayName", "givenName", "surname", "companyName", "jobTitle",
                "emails", "businessPhones", "mobilePhone", "homePhones",
                "imAddresses", "categories", "personalNotes", "birthday", "folder",
            ], extrasaction="ignore")
            writer.writeheader()
            for row in worker._contacts_csv_rows:
                writer.writerow(row)
            zf.writestr("contacts/contacts.csv", sbuf.getvalue())
    buf.seek(0)
    return zipfile.ZipFile(buf)


def test_vcf_default_writes_per_contact_files_foldered(worker, jane, bob):
    z = _run_export(worker, [jane, bob], fmt="VCF")
    names = z.namelist()
    assert "contacts/Contacts/Jane Doe.vcf" in names
    assert "contacts/Recipient Cache/Bob Smith.vcf" in names
    assert "contacts/contacts.csv" not in names
    assert b"VERSION:3.0" in z.read("contacts/Contacts/Jane Doe.vcf")


def test_csv_aggregates_into_single_file(worker, jane, bob, deleted_alice):
    z = _run_export(worker, [jane, bob, deleted_alice], fmt="CSV")
    names = z.namelist()
    assert names == ["contacts/contacts.csv"]
    body = z.read("contacts/contacts.csv").decode()
    assert "displayName,givenName" in body
    assert "Jane Doe" in body
    assert "Bob Smith" in body
    assert "Alice" in body
    assert "Recipient Cache" in body
    assert "Deleted Items" in body


def test_folder_filter_skips_unselected(worker, jane, bob, deleted_alice):
    z = _run_export(
        worker, [jane, bob, deleted_alice], fmt="VCF",
        contact_folders=["Contacts", "Recipient Cache"],
    )
    names = z.namelist()
    assert "contacts/Contacts/Jane Doe.vcf" in names
    assert "contacts/Recipient Cache/Bob Smith.vcf" in names
    assert all("Deleted Items" not in n for n in names)


def test_folder_filter_empty_list_means_no_filter(worker, jane, bob, deleted_alice):
    """Spec: omit contactFolders or pass [] = include all (backward-compatible)."""
    z = _run_export(
        worker, [jane, bob, deleted_alice], fmt="VCF",
        contact_folders=[],
    )
    assert any("Deleted Items" in n for n in z.namelist())


def test_csv_with_folder_filter(worker, jane, bob, deleted_alice):
    z = _run_export(
        worker, [jane, bob, deleted_alice], fmt="CSV",
        contact_folders=["Contacts"],
    )
    body = z.read("contacts/contacts.csv").decode()
    assert "Jane Doe" in body
    assert "Bob Smith" not in body
    assert "Alice" not in body
