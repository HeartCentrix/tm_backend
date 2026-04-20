"""Entra-specific Graph wrappers used by EntraRestoreEngine.

Each section owns a SectionSpec describing where to GET live state,
what fields are patchable, and what extra fields are needed for
create (POST). The module also exposes per-section typed helpers so
the engine doesn't have to know Graph URL shapes."""
from __future__ import annotations

import secrets
import string
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from shared.entra_fingerprint import MUTABLE_FIELDS
from shared.graph_batch import BatchRequest, batch_requests


@dataclass(frozen=True)
class SectionSpec:
    """Immutable description of a restorable Entra section's Graph
    endpoints. Used by sieve + PATCH + POST generic wrappers."""
    item_type: str
    list_url_template: str           # "/users" or "/groups" — list endpoint
    object_url_template: str         # "/users/{id}"
    create_url: str                  # where to POST for create
    # Optional extra fields required by Graph on POST (e.g. password
    # profile for users). Overridden by per-section create_payload().
    extra_create_fields: Tuple[str, ...] = ()


SECTION_SPECS: Dict[str, SectionSpec] = {
    "ENTRA_DIR_USER": SectionSpec(
        item_type="ENTRA_DIR_USER",
        list_url_template="/users",
        object_url_template="/users/{id}",
        create_url="/users",
    ),
    "ENTRA_DIR_GROUP": SectionSpec(
        item_type="ENTRA_DIR_GROUP",
        list_url_template="/groups",
        object_url_template="/groups/{id}",
        create_url="/groups",
    ),
    "ENTRA_DIR_ROLE": SectionSpec(
        item_type="ENTRA_DIR_ROLE",
        list_url_template="/roleManagement/directory/roleDefinitions",
        object_url_template="/roleManagement/directory/roleDefinitions/{id}",
        create_url="/roleManagement/directory/roleDefinitions",
    ),
    "ENTRA_DIR_APPLICATION": SectionSpec(
        item_type="ENTRA_DIR_APPLICATION",
        list_url_template="/applications",
        object_url_template="/applications/{id}",
        create_url="/applications",
    ),
    "ENTRA_DIR_SECURITY": SectionSpec(
        item_type="ENTRA_DIR_SECURITY",
        list_url_template="/identity/conditionalAccess/policies",
        object_url_template="/identity/conditionalAccess/policies/{id}",
        create_url="/identity/conditionalAccess/policies",
    ),
    "ENTRA_DIR_ADMIN_UNIT": SectionSpec(
        item_type="ENTRA_DIR_ADMIN_UNIT",
        list_url_template="/directory/administrativeUnits",
        object_url_template="/directory/administrativeUnits/{id}",
        create_url="/directory/administrativeUnits",
    ),
    "ENTRA_DIR_INTUNE": SectionSpec(
        item_type="ENTRA_DIR_INTUNE",
        list_url_template="/deviceManagement/deviceCompliancePolicies",
        object_url_template="/deviceManagement/deviceCompliancePolicies/{id}",
        create_url="/deviceManagement/deviceCompliancePolicies",
    ),
}


# ---- Sieve (existence check) ----

async def sieve_existence(
    graph_client: Any, spec: SectionSpec, ids: List[str],
) -> Dict[str, bool]:
    """Return `{id: exists}` by issuing a chunked /$batch of GETs.
    404 → absent; 2xx → present; anything else → treated as absent
    (caller will retry on real failures via the outer loop)."""
    requests = [
        BatchRequest(
            id=f"exist-{i}",
            method="GET",
            url=spec.object_url_template.format(id=oid) + "?$select=id",
        )
        for i, oid in enumerate(ids)
    ]
    responses = await batch_requests(graph_client._post, requests)
    out: Dict[str, bool] = {}
    for (req, resp) in zip(requests, responses):
        oid = req.url.split("?")[0].rsplit("/", 1)[-1]
        status = (resp or {}).get("status")
        out[oid] = status is not None and 200 <= status < 300
    return out


# ---- Users ----

def _random_password(length: int = 20) -> str:
    """Generate a reasonably strong random password for deleted-user
    re-creation. The user is created with `forceChangePasswordNextSignIn`
    so this is only a placeholder."""
    alphabet = string.ascii_letters + string.digits + "!@#$%&*+"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _project(raw: Dict[str, Any], fields: Iterable[str]) -> Dict[str, Any]:
    return {k: raw[k] for k in fields if k in raw}


async def patch_user(graph_client: Any, user_id: str, raw: Dict[str, Any]) -> None:
    payload = _project(raw, MUTABLE_FIELDS["ENTRA_DIR_USER"])
    if not payload:
        return
    await graph_client._patch(
        f"{graph_client.GRAPH_URL}/users/{user_id}", payload,
    )


async def create_user(graph_client: Any, raw: Dict[str, Any]) -> Optional[str]:
    """Recreate a deleted user. Created disabled with a random password
    and forceChangePasswordNextSignIn so the tenant admin owns re-enabling."""
    payload = _project(raw, MUTABLE_FIELDS["ENTRA_DIR_USER"])
    # Graph requires these specific fields on create regardless.
    if "userPrincipalName" not in payload:
        return None
    payload.setdefault("accountEnabled", False)
    payload.setdefault("mailNickname", raw.get("mailNickname") or payload["userPrincipalName"].split("@")[0])
    payload["accountEnabled"] = False
    payload["passwordProfile"] = {
        "forceChangePasswordNextSignIn": True,
        "password": _random_password(),
    }
    resp = await graph_client._post(f"{graph_client.GRAPH_URL}/users", payload)
    return resp.get("id") if isinstance(resp, dict) else None


# ---- Groups ----

async def patch_group(graph_client: Any, group_id: str, raw: Dict[str, Any]) -> None:
    payload = _project(raw, MUTABLE_FIELDS["ENTRA_DIR_GROUP"])
    if not payload:
        return
    await graph_client._patch(
        f"{graph_client.GRAPH_URL}/groups/{group_id}", payload,
    )


async def create_group(graph_client: Any, raw: Dict[str, Any]) -> Optional[str]:
    """POST /groups requires displayName + mailEnabled + securityEnabled
    + mailNickname. Everything else is optional."""
    payload = _project(raw, MUTABLE_FIELDS["ENTRA_DIR_GROUP"])
    if "displayName" not in payload:
        return None
    payload.setdefault("mailEnabled", False)
    payload.setdefault("securityEnabled", True)
    payload.setdefault("mailNickname", raw.get("mailNickname") or f"restored-{raw.get('id', 'grp')[-8:]}")
    resp = await graph_client._post(f"{graph_client.GRAPH_URL}/groups", payload)
    return resp.get("id") if isinstance(resp, dict) else None


async def set_group_members(
    graph_client: Any,
    *,
    group_id: str,
    desired_member_ids: List[str],
    live_member_ids: List[str],
) -> Dict[str, int]:
    """Reconcile membership: add missing, remove extras. Returns
    `{added, removed, failed}` counters."""
    desired: Set[str] = set(desired_member_ids)
    live: Set[str] = set(live_member_ids)

    to_add = desired - live
    to_remove = live - desired

    added = removed = failed = 0
    for mid in to_add:
        try:
            await graph_client._post(
                f"{graph_client.GRAPH_URL}/groups/{group_id}/members/$ref",
                {"@odata.id": f"{graph_client.GRAPH_URL}/directoryObjects/{mid}"},
            )
            added += 1
        except Exception:
            failed += 1
    for mid in to_remove:
        try:
            await graph_client._delete(
                f"{graph_client.GRAPH_URL}/groups/{group_id}/members/{mid}/$ref",
            )
            removed += 1
        except Exception:
            failed += 1
    return {"added": added, "removed": removed, "failed": failed}


# ---- Admin Units ----

async def patch_admin_unit(graph_client: Any, au_id: str, raw: Dict[str, Any]) -> None:
    payload = _project(raw, MUTABLE_FIELDS["ENTRA_DIR_ADMIN_UNIT"])
    if not payload:
        return
    await graph_client._patch(
        f"{graph_client.GRAPH_URL}/directory/administrativeUnits/{au_id}", payload,
    )


async def create_admin_unit(graph_client: Any, raw: Dict[str, Any]) -> Optional[str]:
    payload = _project(raw, MUTABLE_FIELDS["ENTRA_DIR_ADMIN_UNIT"])
    if "displayName" not in payload:
        return None
    resp = await graph_client._post(
        f"{graph_client.GRAPH_URL}/directory/administrativeUnits", payload,
    )
    return resp.get("id") if isinstance(resp, dict) else None


async def set_admin_unit_members(
    graph_client: Any,
    *,
    au_id: str,
    desired_member_ids: List[str],
    live_member_ids: List[str],
) -> Dict[str, int]:
    desired = set(desired_member_ids)
    live = set(live_member_ids)
    added = removed = failed = 0
    for mid in desired - live:
        try:
            await graph_client._post(
                f"{graph_client.GRAPH_URL}/directory/administrativeUnits/{au_id}/members/$ref",
                {"@odata.id": f"{graph_client.GRAPH_URL}/directoryObjects/{mid}"},
            )
            added += 1
        except Exception:
            failed += 1
    for mid in live - desired:
        try:
            await graph_client._delete(
                f"{graph_client.GRAPH_URL}/directory/administrativeUnits/{au_id}/members/{mid}/$ref",
            )
            removed += 1
        except Exception:
            failed += 1
    return {"added": added, "removed": removed, "failed": failed}


# ---- Applications / Service Principals ----

async def patch_application(graph_client: Any, app_object_id: str, raw: Dict[str, Any]) -> None:
    app_fields = {
        "displayName", "signInAudience", "tags", "identifierUris",
        "requiredResourceAccess", "web", "api", "appRoles",
    }
    payload = _project(raw, app_fields)
    if not payload:
        return
    await graph_client._patch(
        f"{graph_client.GRAPH_URL}/applications/{app_object_id}", payload,
    )


async def create_application(graph_client: Any, raw: Dict[str, Any]) -> Optional[str]:
    app_fields = {
        "displayName", "signInAudience", "tags", "identifierUris",
        "requiredResourceAccess", "web", "api", "appRoles",
    }
    payload = _project(raw, app_fields)
    if "displayName" not in payload:
        return None
    resp = await graph_client._post(f"{graph_client.GRAPH_URL}/applications", payload)
    return resp.get("id") if isinstance(resp, dict) else None


# ---- Conditional Access / Named Locations / Auth Strengths / Auth Contexts ----

async def patch_ca_policy(graph_client: Any, policy_id: str, raw: Dict[str, Any]) -> None:
    ca_fields = {"displayName", "state", "conditions", "grantControls", "sessionControls"}
    payload = _project(raw, ca_fields)
    if not payload:
        return
    await graph_client._patch(
        f"{graph_client.GRAPH_URL}/identity/conditionalAccess/policies/{policy_id}", payload,
    )


async def create_ca_policy(graph_client: Any, raw: Dict[str, Any]) -> Optional[str]:
    ca_fields = {"displayName", "state", "conditions", "grantControls", "sessionControls"}
    payload = _project(raw, ca_fields)
    if "displayName" not in payload:
        return None
    resp = await graph_client._post(
        f"{graph_client.GRAPH_URL}/identity/conditionalAccess/policies", payload,
    )
    return resp.get("id") if isinstance(resp, dict) else None


# ---- Intune ----

async def patch_intune_policy(graph_client: Any, policy_id: str, raw: Dict[str, Any]) -> None:
    # Compliance vs configuration policies live at different paths; we
    # detect via @odata.type.
    odata = raw.get("@odata.type") or ""
    if "deviceCompliancePolicy" in odata or "compliance" in odata.lower():
        base = f"{graph_client.GRAPH_URL}/deviceManagement/deviceCompliancePolicies"
    else:
        base = f"{graph_client.GRAPH_URL}/deviceManagement/deviceConfigurations"
    fields = {"displayName", "description", "scheduledActionsForRule",
              "settings", "roleScopeTagIds"}
    payload = _project(raw, fields)
    if not payload:
        return
    await graph_client._patch(f"{base}/{policy_id}", payload)


async def create_intune_policy(graph_client: Any, raw: Dict[str, Any]) -> Optional[str]:
    odata = raw.get("@odata.type") or ""
    if "deviceCompliancePolicy" in odata or "compliance" in odata.lower():
        base = f"{graph_client.GRAPH_URL}/deviceManagement/deviceCompliancePolicies"
    else:
        base = f"{graph_client.GRAPH_URL}/deviceManagement/deviceConfigurations"
    fields = {"displayName", "description", "@odata.type", "settings", "roleScopeTagIds"}
    payload = _project(raw, fields)
    if "displayName" not in payload:
        return None
    resp = await graph_client._post(base, payload)
    return resp.get("id") if isinstance(resp, dict) else None


# ---- Roles (custom only) ----

async def patch_role_definition(graph_client: Any, role_id: str, raw: Dict[str, Any]) -> None:
    if raw.get("isBuiltIn"):
        return  # built-in roles are immutable
    fields = {"displayName", "description", "isEnabled", "rolePermissions", "resourceScopes"}
    payload = _project(raw, fields)
    if not payload:
        return
    await graph_client._patch(
        f"{graph_client.GRAPH_URL}/roleManagement/directory/roleDefinitions/{role_id}", payload,
    )


async def create_role_definition(graph_client: Any, raw: Dict[str, Any]) -> Optional[str]:
    if raw.get("isBuiltIn"):
        return None
    fields = {"displayName", "description", "isEnabled", "rolePermissions", "resourceScopes"}
    payload = _project(raw, fields)
    if "displayName" not in payload:
        return None
    resp = await graph_client._post(
        f"{graph_client.GRAPH_URL}/roleManagement/directory/roleDefinitions", payload,
    )
    return resp.get("id") if isinstance(resp, dict) else None
