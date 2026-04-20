"""Tests for EntraExportPipeline CSV formatters + ZIP layout."""
import io
import json
import zipfile

from workers.restore_worker.entra_export import (
    format_section_csv,
    format_section_json,
    EntraExportPipeline,
    CSV_CAPABLE_SECTIONS,
)


def _items(*pairs):
    """Build a list of fake SnapshotItem-ish objects."""
    from types import SimpleNamespace
    return [
        SimpleNamespace(
            item_type=item_type,
            external_id=raw.get("id", "?"),
            extra_data={"raw": raw},
        )
        for item_type, raw in pairs
    ]


def test_format_users_csv_includes_documented_columns():
    items = _items(
        ("ENTRA_DIR_USER", {
            "id": "u1",
            "userPrincipalName": "a@x",
            "displayName": "A",
            "mail": "a@x",
            "jobTitle": "Eng",
            "department": "X",
            "officeLocation": "Remote",
            "accountEnabled": True,
            "userType": "Member",
            "externalUserState": None,
            "createdDateTime": "2026-01-01T00:00:00Z",
            "proxyAddresses": ["smtp:a@x", "SMTP:A@X"],
        }),
    )
    csv_text = format_section_csv("users", items)
    header = csv_text.splitlines()[0]
    for col in ["id", "userPrincipalName", "displayName", "mail", "jobTitle",
                "department", "officeLocation", "accountEnabled", "userType",
                "externalUserState", "createdDateTime", "proxyAddresses"]:
        assert col in header
    assert "smtp:a@x;SMTP:A@X" in csv_text


def test_format_groups_csv_joins_groupTypes():
    items = _items(
        ("ENTRA_DIR_GROUP", {
            "id": "g1",
            "displayName": "Team",
            "description": "",
            "mailNickname": "team",
            "mailEnabled": True,
            "securityEnabled": False,
            "groupTypes": ["Unified"],
            "visibility": "Private",
            "membershipRule": None,
            "createdDateTime": None,
        }),
    )
    csv_text = format_section_csv("groups", items)
    assert "Unified" in csv_text


def test_format_section_json_is_verbatim_raw():
    items = _items(
        ("ENTRA_DIR_USER", {"id": "u1", "displayName": "A"}),
        ("ENTRA_DIR_USER", {"id": "u2", "displayName": "B"}),
    )
    payload = format_section_json("users", items)
    parsed = json.loads(payload)
    assert len(parsed) == 2
    assert parsed[0]["id"] == "u1"


def test_csv_capable_sections_exclude_deep_nested_ones():
    # Sections that are JSON-only per spec.
    assert "security" not in CSV_CAPABLE_SECTIONS
    assert "roles" not in CSV_CAPABLE_SECTIONS
    assert "users" in CSV_CAPABLE_SECTIONS
    assert "groups" in CSV_CAPABLE_SECTIONS


def test_zip_layout_includes_manifest_and_only_selected_sections():
    items_by_section = {
        "users": _items(("ENTRA_DIR_USER", {"id": "u1", "displayName": "A",
                                            "userPrincipalName": "a@x"})),
        "groups": _items(("ENTRA_DIR_GROUP", {"id": "g1", "displayName": "T",
                                              "mailEnabled": False,
                                              "securityEnabled": True})),
    }
    pipeline = EntraExportPipeline(
        snapshot_id="snap-1",
        format="csv",
        include_nested_detail=False,
    )
    buf = io.BytesIO()
    pipeline.build_zip(buf, items_by_section)
    buf.seek(0)
    with zipfile.ZipFile(buf) as zf:
        names = set(zf.namelist())
        assert "MANIFEST.json" in names
        assert "users.csv" in names
        assert "groups.csv" in names
        assert "roles.json" not in names  # section not included
        manifest = json.loads(zf.read("MANIFEST.json"))
        assert manifest["format"] == "csv"
        assert manifest["counts"]["users"] == 1
        assert manifest["counts"]["groups"] == 1
