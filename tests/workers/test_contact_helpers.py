"""Pure-function tests for vCard 3.0 + CSV contact helpers."""
import importlib
import sys
from pathlib import Path

# Add restore-worker to import path (worker dirs aren't packages)
WORKER_DIR = Path(__file__).resolve().parents[2] / "workers" / "restore-worker"
sys.path.insert(0, str(WORKER_DIR))

main = importlib.import_module("main")
_contact_to_vcard = main._contact_to_vcard
_contact_to_csv_row = main._contact_to_csv_row


def test_vcard_minimum_required_fields():
    """A contact with only displayName must still produce a parseable vCard 3.0."""
    raw = {"id": "abc", "displayName": "Jane Doe"}
    vcard = _contact_to_vcard(raw, folder="Contacts")
    assert vcard.startswith("BEGIN:VCARD\r\n")
    assert "VERSION:3.0\r\n" in vcard
    assert "FN:Jane Doe\r\n" in vcard
    assert vcard.endswith("END:VCARD\r\n")


def test_vcard_full_fields():
    raw = {
        "id": "abc",
        "displayName": "Jane Doe",
        "givenName": "Jane",
        "surname": "Doe",
        "companyName": "Acme",
        "jobTitle": "Engineer",
        "emailAddresses": [
            {"address": "jane@acme.com", "name": "Jane Doe"},
            {"address": "jane@home.com"},
        ],
        "businessPhones": ["+1-555-0100"],
        "mobilePhone": "+1-555-0101",
        "homePhones": ["+1-555-0102"],
        "imAddresses": ["jane.doe@skype"],
        "personalNotes": "VIP client",
        "birthday": "1980-05-15T00:00:00Z",
        "categories": ["Work", "VIP"],
    }
    vcard = _contact_to_vcard(raw, folder="Contacts")
    assert "N:Doe;Jane;;;\r\n" in vcard
    assert "ORG:Acme\r\n" in vcard
    assert "TITLE:Engineer\r\n" in vcard
    assert "EMAIL;TYPE=INTERNET:jane@acme.com\r\n" in vcard
    assert "EMAIL;TYPE=INTERNET:jane@home.com\r\n" in vcard
    assert "TEL;TYPE=WORK,VOICE:+1-555-0100\r\n" in vcard
    assert "TEL;TYPE=CELL,VOICE:+1-555-0101\r\n" in vcard
    assert "TEL;TYPE=HOME,VOICE:+1-555-0102\r\n" in vcard
    assert "IMPP:jane.doe@skype\r\n" in vcard
    assert "NOTE:VIP client\r\n" in vcard
    assert "BDAY:19800515\r\n" in vcard
    assert "CATEGORIES:Work,VIP\r\n" in vcard


def test_vcard_escapes_special_chars():
    """Per RFC 6350 §3.4 — escape comma, semicolon, backslash, newline."""
    raw = {
        "id": "abc",
        "displayName": "Doe, Jane; PhD",
        "personalNotes": "Line 1\nLine 2; with semi, and comma\\backslash",
    }
    vcard = _contact_to_vcard(raw, folder="Contacts")
    assert "FN:Doe\\, Jane\\; PhD\r\n" in vcard
    assert "NOTE:Line 1\\nLine 2\\; with semi\\, and comma\\\\backslash\r\n" in vcard


def test_vcard_empty_collections_omitted():
    raw = {"id": "abc", "displayName": "Jane", "emailAddresses": [], "businessPhones": []}
    vcard = _contact_to_vcard(raw, folder="Contacts")
    assert "EMAIL" not in vcard
    assert "TEL" not in vcard


def test_csv_row_minimum():
    raw = {"id": "abc", "displayName": "Jane Doe"}
    row = _contact_to_csv_row(raw, folder="Contacts")
    assert row["displayName"] == "Jane Doe"
    assert row["folder"] == "Contacts"
    assert row["emails"] == ""
    assert row["businessPhones"] == ""


def test_csv_row_joins_lists_with_semicolons():
    raw = {
        "id": "abc",
        "displayName": "Jane",
        "emailAddresses": [{"address": "a@x.com"}, {"address": "b@x.com"}],
        "businessPhones": ["+1", "+2"],
        "homePhones": ["+3"],
        "imAddresses": ["sip:jane"],
        "categories": ["Work", "VIP"],
    }
    row = _contact_to_csv_row(raw, folder="Recipient Cache")
    assert row["emails"] == "a@x.com;b@x.com"
    assert row["businessPhones"] == "+1;+2"
    assert row["homePhones"] == "+3"
    assert row["imAddresses"] == "sip:jane"
    assert row["categories"] == "Work;VIP"
    assert row["folder"] == "Recipient Cache"


def test_csv_row_birthday_iso_to_date_only():
    raw = {"id": "abc", "displayName": "Jane", "birthday": "1980-05-15T00:00:00Z"}
    row = _contact_to_csv_row(raw, folder="Contacts")
    assert row["birthday"] == "1980-05-15"
