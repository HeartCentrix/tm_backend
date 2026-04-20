"""Tests for Entra-specific Graph wrappers. Graph calls are mocked."""
from unittest.mock import AsyncMock, MagicMock
import pytest

from shared.graph_entra import (
    sieve_existence,
    patch_user,
    create_user,
    patch_group,
    create_group,
    set_group_members,
    SectionSpec,
    SECTION_SPECS,
)


@pytest.mark.asyncio
async def test_sieve_existence_issues_one_batch_per_20_ids():
    """For 35 ids, two chunks of 20 + 15."""
    gc = MagicMock()
    gc._post = AsyncMock(side_effect=[
        # Chunk 1: 20 responses, ids 0..19. First 15 exist, last 5 missing.
        {"responses": [
            {"id": f"exist-{i}", "status": 200 if i < 15 else 404, "body": {"id": str(i)} if i < 15 else {}}
            for i in range(20)
        ]},
        # Chunk 2: 15 responses, ids 20..34. All exist.
        {"responses": [
            {"id": f"exist-{i}", "status": 200, "body": {"id": str(i)}}
            for i in range(20, 35)
        ]},
    ])

    spec = SECTION_SPECS["ENTRA_DIR_USER"]
    result = await sieve_existence(gc, spec, ids=[str(i) for i in range(35)])

    assert gc._post.await_count == 2
    assert result[str(0)] is True
    assert result[str(14)] is True
    assert result[str(15)] is False
    assert result[str(19)] is False
    assert result[str(34)] is True


@pytest.mark.asyncio
async def test_patch_user_only_sends_whitelisted_fields():
    gc = MagicMock()
    gc._patch = AsyncMock(return_value={})
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    raw = {
        "id": "ignored-by-patch",
        "displayName": "Amit",
        "department": "X",
        "userPrincipalName": "new.upn@example.com",
        "lastSignInDateTime": "2026-04-20T00:00:00Z",  # volatile; must drop
        "never_in_the_whitelist": True,
    }
    await patch_user(gc, "uid-1", raw)

    call = gc._patch.await_args
    url = call.args[0]
    payload = call.args[1]
    assert url.endswith("/users/uid-1")
    assert set(payload.keys()) <= {
        "displayName", "givenName", "surname", "jobTitle", "department",
        "officeLocation", "mobilePhone", "businessPhones",
        "userPrincipalName", "mail", "accountEnabled", "usageLocation",
        "userType", "streetAddress", "city", "state", "country",
        "postalCode", "companyName", "employeeId",
    }
    assert "lastSignInDateTime" not in payload
    assert "never_in_the_whitelist" not in payload


@pytest.mark.asyncio
async def test_create_user_supplies_disabled_password_profile():
    """AFI-parity: create deleted users with accountEnabled=false and
    a random password so the tenant admin can re-invite them."""
    gc = MagicMock()
    gc._post = AsyncMock(return_value={"id": "new-uid"})
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    raw = {
        "id": "old-uid",
        "displayName": "Amit",
        "userPrincipalName": "amit@x.com",
        "mailNickname": "amit",
    }
    new_id = await create_user(gc, raw)

    assert new_id == "new-uid"
    payload = gc._post.await_args.args[1]
    assert payload["accountEnabled"] is False
    assert payload["userPrincipalName"] == "amit@x.com"
    assert payload["mailNickname"] == "amit"
    assert payload["passwordProfile"]["forceChangePasswordNextSignIn"] is True
    assert len(payload["passwordProfile"]["password"]) >= 16


@pytest.mark.asyncio
async def test_set_group_members_computes_add_and_remove_sets():
    """Membership rebind: given desired + live sets, emit only the
    diff — add what's missing, remove what's extra."""
    gc = MagicMock()
    gc._post = AsyncMock(return_value={})
    gc._delete = AsyncMock(return_value=None)
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    await set_group_members(
        gc,
        group_id="gid",
        desired_member_ids=["a", "b", "c"],
        live_member_ids=["b", "c", "d"],
    )

    # "a" is missing live → POST.
    assert gc._post.await_count == 1
    post_url, post_body = gc._post.await_args.args
    assert post_url.endswith("/groups/gid/members/$ref")
    assert "directoryObjects/a" in post_body["@odata.id"]

    # "d" is extra live → DELETE.
    assert gc._delete.await_count == 1
    delete_url = gc._delete.await_args.args[0]
    assert delete_url.endswith("/groups/gid/members/d/$ref")


@pytest.mark.asyncio
async def test_create_group_sends_mail_fields():
    gc = MagicMock()
    gc._post = AsyncMock(return_value={"id": "new-gid"})
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"
    raw = {
        "id": "old-gid",
        "displayName": "Team X",
        "mailNickname": "teamx",
        "mailEnabled": True,
        "securityEnabled": False,
        "groupTypes": ["Unified"],
        "description": "",
    }
    new_id = await create_group(gc, raw)
    assert new_id == "new-gid"
    payload = gc._post.await_args.args[1]
    assert payload["mailNickname"] == "teamx"
    assert payload["mailEnabled"] is True


def test_section_spec_catalog_is_complete():
    """Every restorable item_type from the spec must have a SectionSpec."""
    restorable = {
        "ENTRA_DIR_USER", "ENTRA_DIR_GROUP", "ENTRA_DIR_ROLE",
        "ENTRA_DIR_APPLICATION", "ENTRA_DIR_SECURITY",
        "ENTRA_DIR_ADMIN_UNIT", "ENTRA_DIR_INTUNE",
    }
    for it in restorable:
        spec = SECTION_SPECS[it]
        assert isinstance(spec, SectionSpec)
        assert spec.list_url_template
