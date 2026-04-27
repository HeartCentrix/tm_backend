"""ContactPstWriter: USER_CONTACT items → MapiContact → PST.

Per-item flow:
1. Extract Microsoft Graph ``contact`` JSON from ``SnapshotItem.extra_data["raw"]``.
2. Build a ``MapiContact`` directly from Graph fields:
   - Name parts (display, given, surname, middle)
   - Professional info (company, title, department)
   - Email addresses (up to 3)
   - Phone numbers (business, mobile, home)
   - Birthday, personal notes, categories

All ``aspose.*`` imports stay lazy so the module loads without
``aspose-email`` installed (tests substitute it via ``sys.modules``).
"""
from __future__ import annotations

import importlib
import logging
import os
import sys
from datetime import datetime
from typing import Any, Optional

_HERE = os.path.dirname(__file__)
_WORKER = os.path.abspath(os.path.join(_HERE, ".."))
if _WORKER not in sys.path:
    sys.path.insert(0, _WORKER)

from .base import PstWriterBase  # noqa: E402

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _set_if(obj: Any, attr: str, value: Any) -> None:
    """Best-effort attribute set; swallows failures from the mock surface."""
    try:
        setattr(obj, attr, value)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("contact: failed to set %s: %s", attr, exc)


def _parse_birthday(birthday_str: Optional[str]) -> Optional[datetime]:
    """Parse Graph birthday ISO string (e.g. '1985-04-15T00:00:00Z') → datetime.

    Returns None when the string is absent, null, or unparseable.
    """
    if not birthday_str:
        return None
    try:
        s = birthday_str.replace("Z", "")
        if "." in s:
            head, frac = s.split(".", 1)
            frac = frac[:6]
            s = f"{head}.{frac}"
        return datetime.fromisoformat(s)
    except Exception as exc:
        logger.warning("contact: failed to parse birthday %r: %s", birthday_str, exc)
        return None


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class ContactPstWriter(PstWriterBase):
    """Convert ``USER_CONTACT`` SnapshotItems into ``MapiContact`` entries."""

    item_type = "USER_CONTACT"
    standard_folder_type = "Contacts"

    async def _build_mapi_item(self, item, shard, source_container: str):
        """Build a ``MapiContact`` from Graph contact JSON.

        Returns the MAPI object on success or ``None`` when:
          * ``extra_data["raw"]`` is missing;
          * any underlying call raises (logged at ERROR).
        """
        extra = getattr(item, "extra_data", None) or {}
        raw = extra.get("raw")
        if not raw:
            logger.warning(
                "contact: missing extra_data['raw'] for item %s",
                getattr(item, "external_id", "?"),
            )
            return None

        try:
            mapi_mod = importlib.import_module("aspose.email.mapi")
            MapiContact = mapi_mod.MapiContact
            MapiContactNamePropertySet = mapi_mod.MapiContactNamePropertySet
            MapiContactProfessionalPropertySet = mapi_mod.MapiContactProfessionalPropertySet
            MapiContactTelephonePropertySet = mapi_mod.MapiContactTelephonePropertySet
            MapiContactElectronicAddressPropertySet = mapi_mod.MapiContactElectronicAddressPropertySet
            MapiContactElectronicAddress = mapi_mod.MapiContactElectronicAddress

            contact = MapiContact()

            # --- Display name ------------------------------------------
            display_name = raw.get("displayName") or ""
            _set_if(contact, "display_name", display_name)

            # --- Name info ---------------------------------------------
            try:
                name_info = MapiContactNamePropertySet()
                _set_if(name_info, "given_name", raw.get("givenName") or "")
                _set_if(name_info, "surname", raw.get("surname") or "")
                middle = raw.get("middleName")
                if middle:
                    _set_if(name_info, "middle_name", middle)
                _set_if(contact, "name_info", name_info)
            except Exception as exc:
                logger.debug("contact: failed to build name_info: %s", exc)

            # --- Professional info -------------------------------------
            try:
                prof_info = MapiContactProfessionalPropertySet()
                _set_if(prof_info, "company_name", raw.get("companyName") or "")
                _set_if(prof_info, "title", raw.get("jobTitle") or "")
                _set_if(prof_info, "department_name", raw.get("department") or "")
                _set_if(contact, "professional_info", prof_info)
            except Exception as exc:
                logger.debug("contact: failed to build professional_info: %s", exc)

            # --- Telephones -------------------------------------------
            try:
                telephones = MapiContactTelephonePropertySet()
                business_phones = raw.get("businessPhones") or []
                if business_phones:
                    _set_if(telephones, "business_telephone_number", business_phones[0])
                mobile_phone = raw.get("mobilePhone")
                if mobile_phone:
                    _set_if(telephones, "mobile_telephone_number", mobile_phone)
                home_phones = raw.get("homePhones") or []
                if home_phones:
                    _set_if(telephones, "home_telephone_number", home_phones[0])
                _set_if(contact, "telephones", telephones)
            except Exception as exc:
                logger.debug("contact: failed to build telephones: %s", exc)

            # --- Electronic addresses (email) -------------------------
            try:
                email_addresses = raw.get("emailAddresses") or []
                elec_addrs = MapiContactElectronicAddressPropertySet()
                email_slots = ["email1", "email2", "email3"]
                for slot, email_entry in zip(email_slots, email_addresses):
                    addr_str = (email_entry or {}).get("address") or ""
                    if not addr_str:
                        continue
                    ea = MapiContactElectronicAddress()
                    _set_if(ea, "address_type", "SMTP")
                    _set_if(ea, "email_address", addr_str)
                    _set_if(elec_addrs, slot, ea)
                _set_if(contact, "electronic_addresses", elec_addrs)
            except Exception as exc:
                logger.debug("contact: failed to build electronic_addresses: %s", exc)

            # --- IM addresses → note on email1 display_name if no email1 ---
            im_addresses = raw.get("imAddresses") or []
            if im_addresses:
                try:
                    ea_obj = getattr(
                        getattr(contact, "electronic_addresses", None),
                        "email1",
                        None,
                    )
                    if ea_obj is not None:
                        _set_if(ea_obj, "display_name", im_addresses[0])
                except Exception as exc:
                    logger.debug("contact: failed to set im_address on email1: %s", exc)

            # --- Personal info (birthday, notes) ----------------------
            try:
                personal_info = getattr(contact, "personal_info", None)
                birthday_str = raw.get("birthday")
                if birthday_str:
                    bday = _parse_birthday(birthday_str)
                    if bday is not None and personal_info is not None:
                        _set_if(personal_info, "birthday", bday)

                notes = raw.get("personalNotes")
                if notes and personal_info is not None:
                    _set_if(personal_info, "notes", notes)
            except Exception as exc:
                logger.debug("contact: failed to set personal_info fields: %s", exc)

            # --- Categories ------------------------------------------
            categories = raw.get("categories") or []
            if categories:
                _set_if(contact, "categories", list(categories))

            return contact

        except Exception as exc:
            logger.error(
                "contact: failed to build MapiContact for item %s: %s",
                getattr(item, "external_id", "?"),
                exc,
            )
            return None
