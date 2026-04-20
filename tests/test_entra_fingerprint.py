"""Fingerprint stability + coverage per-section."""
from shared.entra_fingerprint import fingerprint_object, MUTABLE_FIELDS


def test_fingerprint_is_deterministic_and_order_insensitive():
    raw1 = {
        "id": "abc",
        "displayName": "Amit",
        "jobTitle": "Eng",
        "department": "X",
        "extra": "ignored",
    }
    raw2 = {
        "extra": "different",
        "department": "X",
        "id": "abc",
        "jobTitle": "Eng",
        "displayName": "Amit",
    }
    assert fingerprint_object("ENTRA_DIR_USER", raw1) == fingerprint_object("ENTRA_DIR_USER", raw2)


def test_fingerprint_changes_when_mutable_field_changes():
    base = {"id": "abc", "displayName": "Amit", "jobTitle": "Eng"}
    other = {"id": "abc", "displayName": "Amit", "jobTitle": "Lead"}
    assert fingerprint_object("ENTRA_DIR_USER", base) != fingerprint_object("ENTRA_DIR_USER", other)


def test_fingerprint_ignores_volatile_fields():
    """Graph re-serialises timestamps / etag-ish fields on every read;
    the fingerprint must ignore them so a stable object doesn't show
    as drifted on every restore."""
    base = {"id": "abc", "displayName": "Amit", "jobTitle": "Eng"}
    noisy = {
        "id": "abc",
        "displayName": "Amit",
        "jobTitle": "Eng",
        "lastSignInDateTime": "2026-04-20T00:00:00Z",
        "@odata.etag": "xyz",
        "refreshTokensValidFromDateTime": "2026-04-20T00:00:00Z",
    }
    assert fingerprint_object("ENTRA_DIR_USER", base) == fingerprint_object("ENTRA_DIR_USER", noisy)


def test_mutable_fields_cover_all_sections_we_restore():
    """If we add a new section to the restore allowlist and forget to
    add its mutable-field list here, the plan fails loudly."""
    required = {
        "ENTRA_DIR_USER", "ENTRA_DIR_GROUP", "ENTRA_DIR_ROLE",
        "ENTRA_DIR_APPLICATION", "ENTRA_DIR_SECURITY",
        "ENTRA_DIR_ADMIN_UNIT", "ENTRA_DIR_INTUNE",
    }
    assert required.issubset(set(MUTABLE_FIELDS.keys()))
