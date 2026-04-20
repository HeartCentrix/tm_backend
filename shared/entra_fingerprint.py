"""Fingerprint helper for Entra snapshot items.

The restore engine compares `SnapshotItem.extra_data.fingerprint`
against a live-fetched Graph object to decide `unchanged | updated`.
We compute the fingerprint once at backup time so the restore path
is O(1) per object.

Volatile fields (timestamps Graph updates on every read, etag-ish
metadata) are deliberately excluded — otherwise every restore would
report every object as drifted."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable


# Fields that Graph rewrites on every read. Never part of the
# fingerprint. Apply across every section.
_ALWAYS_IGNORE = frozenset({
    "@odata.etag", "@odata.context", "@odata.id", "@odata.type",
    "lastSignInDateTime", "refreshTokensValidFromDateTime",
    "lastPasswordChangeDateTime", "signInSessionsValidFromDateTime",
    "lastModifiedDateTime",  # most Graph objects — server-managed
})


# Per-item-type mutable-field allowlist. The fingerprint hashes only
# these fields. Buckets that share an item_type (e.g. ENTRA_DIR_SECURITY
# covers CA policies + named locations + alerts) pick the superset.
MUTABLE_FIELDS: Dict[str, Iterable[str]] = {
    "ENTRA_DIR_USER": (
        "displayName", "givenName", "surname", "jobTitle", "department",
        "officeLocation", "mobilePhone", "businessPhones",
        "userPrincipalName", "mail", "accountEnabled", "usageLocation",
        "userType", "streetAddress", "city", "state", "country",
        "postalCode", "companyName", "employeeId",
    ),
    "ENTRA_DIR_GROUP": (
        "displayName", "description", "mailNickname", "mailEnabled",
        "securityEnabled", "groupTypes", "visibility", "membershipRule",
        "membershipRuleProcessingState",
    ),
    "ENTRA_DIR_ROLE": (
        "displayName", "description", "isEnabled", "rolePermissions",
        "templateId", "resourceScopes",
    ),
    "ENTRA_DIR_APPLICATION": (
        # Applications
        "displayName", "signInAudience", "tags", "identifierUris",
        "requiredResourceAccess", "web", "api", "appRoles",
        # Service principals (same item_type, different _app_bucket)
        "servicePrincipalType", "accountEnabled", "appRoleAssignmentRequired",
        "preferredSingleSignOnMode", "replyUrls",
    ),
    "ENTRA_DIR_SECURITY": (
        # Conditional Access + named locations + auth strengths + auth contexts
        "displayName", "state", "conditions", "grantControls",
        "sessionControls", "countriesAndRegions", "includeUnknownCountriesAndRegions",
        "isTrusted", "ipRanges", "allowedCombinations", "combinationConfigurations",
        "description", "isAvailable",
        # Security defaults
        "isEnabled",
    ),
    "ENTRA_DIR_ADMIN_UNIT": (
        "displayName", "description", "visibility",
    ),
    "ENTRA_DIR_INTUNE": (
        "displayName", "description", "scheduledActionsForRule",
        "deviceCompliancePolicyAssignments", "deviceConfigurationAssignments",
        "@odata.type", "settings", "roleScopeTagIds",
    ),
    "ENTRA_DIR_AUDIT": (),  # not restorable; fingerprint irrelevant, empty allowed.
}


def fingerprint_object(item_type: str, raw: Dict[str, Any]) -> str:
    """Return the SHA-256 fingerprint of the mutable fields for the
    given item_type. Deterministic + order-insensitive."""
    fields = MUTABLE_FIELDS.get(item_type, ())
    projection = {}
    for k in fields:
        if k in _ALWAYS_IGNORE:
            continue
        if k in raw:
            projection[k] = raw[k]
    # sort_keys → order-insensitive; default=str handles dates/UUIDs.
    payload = json.dumps(projection, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
