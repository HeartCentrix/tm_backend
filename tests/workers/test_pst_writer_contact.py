"""Unit tests for ``pst_writers.contact.ContactPstWriter._build_mapi_item``.

The ContactPstWriter translates Graph ``contact`` JSON straight into a
MAPI contact item.  ``aspose-email`` is NOT installed in the test
environment — we substitute ``aspose.email.mapi`` in ``sys.modules``
before importing the writer.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from datetime import datetime
from unittest.mock import MagicMock, patch

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


# Register the aspose.email.mapi mock before the writer module is imported.
_aspose_mapi_mock = sys.modules.setdefault("aspose.email.mapi", MagicMock())


# ---------------------------------------------------------------------------
# Load the pst_writers package without triggering restore-worker bootstrap.
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _Item:
    def __init__(self, ext_id="contact-1", extra_data=None):
        self.external_id = ext_id
        self.extra_data = extra_data or {}


@pytest.fixture
def aspose_mapi():
    """Reset the aspose.email.mapi mock between tests.

    We replace the module-level MagicMock with a fresh one each test so
    constructor call assertions don't leak across tests.
    """
    fresh = MagicMock(name="aspose.email.mapi")

    # Make attribute access on instances return distinct mocks so we can
    # assert on them independently (e.g. contact.name_info.given_name).
    # MagicMock already does this by default — the fixture is mainly for
    # isolation between test runs.
    sys.modules["aspose.email.mapi"] = fresh
    yield fresh
    sys.modules["aspose.email.mapi"] = _aspose_mapi_mock


def _full_contact(**overrides):
    """Build a representative full Graph contact dict."""
    base = {
        "id": "AAMkAGI...",
        "displayName": "Bob Smith",
        "givenName": "Bob",
        "surname": "Smith",
        "middleName": None,
        "companyName": "Contoso",
        "jobTitle": "Engineer",
        "department": "R&D",
        "emailAddresses": [
            {"name": "Bob Smith", "address": "bob@contoso.com"},
        ],
        "businessPhones": ["+1 425-555-0100"],
        "mobilePhone": "+1 206-555-0110",
        "homePhones": [],
        "imAddresses": ["bob@contoso.com"],
        "birthday": "1985-04-15T00:00:00Z",
        "personalNotes": "Met at conference 2023",
        "categories": ["VIP"],
        "photo": None,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# 1. Class-level constants
# ---------------------------------------------------------------------------

def test_contact_writer_class_constants():
    """item_type and standard_folder_type are wired correctly."""
    assert ContactPstWriter.item_type == "USER_CONTACT"
    assert ContactPstWriter.standard_folder_type == "Contacts"
    assert issubclass(ContactPstWriter, PstWriterBase)


# ---------------------------------------------------------------------------
# 2. Builds MapiContact with all main fields set
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_builds_mapi_contact_with_all_fields(aspose_mapi):
    """All main fields are mapped from the Graph contact JSON."""
    writer = ContactPstWriter()
    raw = _full_contact()
    item = _Item(extra_data={"raw": raw})

    fake_contact = MagicMock(name="MapiContact_instance")
    fake_name_info = MagicMock(name="name_info")
    fake_prof_info = MagicMock(name="professional_info")
    fake_telephones = MagicMock(name="telephones")
    fake_elec_addrs = MagicMock(name="electronic_addresses")
    fake_email1 = MagicMock(name="email1")
    fake_personal_info = MagicMock(name="personal_info")

    aspose_mapi.MapiContact = MagicMock(return_value=fake_contact)
    aspose_mapi.MapiContactNamePropertySet = MagicMock(return_value=fake_name_info)
    aspose_mapi.MapiContactProfessionalPropertySet = MagicMock(return_value=fake_prof_info)
    aspose_mapi.MapiContactTelephonePropertySet = MagicMock(return_value=fake_telephones)
    aspose_mapi.MapiContactElectronicAddressPropertySet = MagicMock(return_value=fake_elec_addrs)
    aspose_mapi.MapiContactElectronicAddress = MagicMock(return_value=fake_email1)

    # personal_info is accessed via getattr on the already-created contact
    fake_contact.personal_info = fake_personal_info

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_contact
    aspose_mapi.MapiContact.assert_called_once()

    # display_name
    assert fake_contact.display_name == "Bob Smith"

    # name_info
    assert fake_name_info.given_name == "Bob"
    assert fake_name_info.surname == "Smith"
    assert fake_contact.name_info is fake_name_info

    # professional_info
    assert fake_prof_info.company_name == "Contoso"
    assert fake_prof_info.title == "Engineer"
    assert fake_prof_info.department_name == "R&D"
    assert fake_contact.professional_info is fake_prof_info

    # telephones
    assert fake_telephones.business_telephone_number == "+1 425-555-0100"
    assert fake_telephones.mobile_telephone_number == "+1 206-555-0110"
    assert fake_contact.telephones is fake_telephones

    # electronic_addresses
    assert fake_email1.address_type == "SMTP"
    assert fake_email1.email_address == "bob@contoso.com"

    # categories
    assert fake_contact.categories == ["VIP"]


# ---------------------------------------------------------------------------
# 3. Multiple email addresses → email1, email2, email3
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_multiple_email_addresses(aspose_mapi):
    """Three email addresses are mapped to email1, email2, email3 slots."""
    writer = ContactPstWriter()
    raw = _full_contact(emailAddresses=[
        {"name": "Bob Work", "address": "bob@work.com"},
        {"name": "Bob Personal", "address": "bob@home.com"},
        {"name": "Bob Alt", "address": "bob@alt.com"},
    ])
    item = _Item(extra_data={"raw": raw})

    fake_contact = MagicMock(name="MapiContact_instance")
    fake_contact.personal_info = MagicMock()
    fake_elec_addrs = MagicMock(name="electronic_addresses")
    ea_calls = []

    def make_ea():
        m = MagicMock(name="MapiContactElectronicAddress")
        ea_calls.append(m)
        return m

    aspose_mapi.MapiContact = MagicMock(return_value=fake_contact)
    aspose_mapi.MapiContactNamePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactProfessionalPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactTelephonePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddressPropertySet = MagicMock(return_value=fake_elec_addrs)
    aspose_mapi.MapiContactElectronicAddress = MagicMock(side_effect=make_ea)

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_contact
    # Three EA objects created
    assert len(ea_calls) == 3
    # Each gets address_type = SMTP and a distinct email_address
    assert ea_calls[0].address_type == "SMTP"
    assert ea_calls[0].email_address == "bob@work.com"
    assert ea_calls[1].email_address == "bob@home.com"
    assert ea_calls[2].email_address == "bob@alt.com"
    # They are assigned to the correct slot names
    assert fake_elec_addrs.email1 is ea_calls[0]
    assert fake_elec_addrs.email2 is ea_calls[1]
    assert fake_elec_addrs.email3 is ea_calls[2]


# ---------------------------------------------------------------------------
# 4. Missing optional fields handled gracefully
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_optional_fields_no_crash(aspose_mapi):
    """middleName=None, homePhones=[], birthday=None, categories=[], imAddresses=[]
    should all be handled without raising."""
    writer = ContactPstWriter()
    raw = {
        "id": "sparse-id",
        "displayName": "Minimal Contact",
        "givenName": "Minimal",
        "surname": "Contact",
        "middleName": None,
        "companyName": "",
        "jobTitle": "",
        "department": "",
        "emailAddresses": [],
        "businessPhones": [],
        "mobilePhone": None,
        "homePhones": [],
        "imAddresses": [],
        "birthday": None,
        "personalNotes": None,
        "categories": [],
    }
    item = _Item(extra_data={"raw": raw})

    fake_contact = MagicMock()
    fake_contact.personal_info = MagicMock()
    aspose_mapi.MapiContact = MagicMock(return_value=fake_contact)
    aspose_mapi.MapiContactNamePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactProfessionalPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactTelephonePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddressPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddress = MagicMock(return_value=MagicMock())

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    # Should still return a contact, no exception
    assert result is fake_contact
    assert fake_contact.display_name == "Minimal Contact"


# ---------------------------------------------------------------------------
# 5. Returns None when extra_data["raw"] is missing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_none_when_raw_missing(aspose_mapi):
    """No extra_data['raw'] → return None and don't construct MapiContact."""
    writer = ContactPstWriter()
    item = _Item(extra_data={"structured": {"parentFolderName": "Work Contacts"}})

    aspose_mapi.MapiContact = MagicMock()

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is None
    aspose_mapi.MapiContact.assert_not_called()


# ---------------------------------------------------------------------------
# 6. Returns None when build raises exception
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_none_when_build_raises(aspose_mapi):
    """An exception during MAPI construction is logged and yields None."""
    writer = ContactPstWriter()
    item = _Item(extra_data={"raw": _full_contact()})

    aspose_mapi.MapiContact = MagicMock(side_effect=RuntimeError("aspose exploded"))

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is None


# ---------------------------------------------------------------------------
# 7. Birthday parsing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_birthday_valid_iso_string_sets_datetime(aspose_mapi):
    """A valid ISO birthday string results in a datetime set on personal_info."""
    writer = ContactPstWriter()
    raw = _full_contact(birthday="1985-04-15T00:00:00Z")
    item = _Item(extra_data={"raw": raw})

    fake_contact = MagicMock()
    fake_personal_info = MagicMock()
    fake_contact.personal_info = fake_personal_info

    aspose_mapi.MapiContact = MagicMock(return_value=fake_contact)
    aspose_mapi.MapiContactNamePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactProfessionalPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactTelephonePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddressPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddress = MagicMock(return_value=MagicMock())

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_contact
    # Birthday must have been set as a datetime
    assert isinstance(fake_personal_info.birthday, datetime)
    assert fake_personal_info.birthday.year == 1985
    assert fake_personal_info.birthday.month == 4
    assert fake_personal_info.birthday.day == 15


@pytest.mark.asyncio
async def test_null_birthday_is_skipped(aspose_mapi):
    """A null birthday does not set personal_info.birthday."""
    writer = ContactPstWriter()
    raw = _full_contact(birthday=None)
    item = _Item(extra_data={"raw": raw})

    fake_contact = MagicMock()
    fake_personal_info = MagicMock(spec=[])   # no attributes — setattr would fail
    fake_contact.personal_info = fake_personal_info

    aspose_mapi.MapiContact = MagicMock(return_value=fake_contact)
    aspose_mapi.MapiContactNamePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactProfessionalPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactTelephonePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddressPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddress = MagicMock(return_value=MagicMock())

    # Should not raise even though fake_personal_info has no birthday attribute
    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")
    assert result is fake_contact


# ---------------------------------------------------------------------------
# 8. Phone field mappings
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_business_phone_mapped_correctly(aspose_mapi):
    """A single business phone is set; absent mobile/home cause no crash."""
    writer = ContactPstWriter()
    raw = _full_contact(
        businessPhones=["+1 425-555-9999"],
        mobilePhone=None,
        homePhones=[],
    )
    item = _Item(extra_data={"raw": raw})

    fake_contact = MagicMock()
    fake_contact.personal_info = MagicMock()
    fake_telephones = MagicMock()

    aspose_mapi.MapiContact = MagicMock(return_value=fake_contact)
    aspose_mapi.MapiContactNamePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactProfessionalPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactTelephonePropertySet = MagicMock(return_value=fake_telephones)
    aspose_mapi.MapiContactElectronicAddressPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddress = MagicMock(return_value=MagicMock())

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_contact
    assert fake_telephones.business_telephone_number == "+1 425-555-9999"
    # mobile was not set (None input) — ensure no AttributeError was raised
    # (MagicMock records any setattr but we just need no exception)


@pytest.mark.asyncio
async def test_no_phone_fields_no_crash(aspose_mapi):
    """All phone fields absent/empty → no crash, contact still returned."""
    writer = ContactPstWriter()
    raw = _full_contact(
        businessPhones=[],
        mobilePhone=None,
        homePhones=[],
    )
    item = _Item(extra_data={"raw": raw})

    fake_contact = MagicMock()
    fake_contact.personal_info = MagicMock()

    aspose_mapi.MapiContact = MagicMock(return_value=fake_contact)
    aspose_mapi.MapiContactNamePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactProfessionalPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactTelephonePropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddressPropertySet = MagicMock(return_value=MagicMock())
    aspose_mapi.MapiContactElectronicAddress = MagicMock(return_value=MagicMock())

    result = await writer._build_mapi_item(item, shard=MagicMock(), source_container="c")

    assert result is fake_contact
