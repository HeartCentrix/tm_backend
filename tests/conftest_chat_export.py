"""Chat-export test fixtures — seed tenants, resource, snapshot, msgs.

Not auto-loaded as a conftest. Tests opt in by importing fixtures from
this module, e.g.:

    pytest_plugins = ("tests.conftest_chat_export",)

Or individual tests declare the fixture via direct import. Keeping it out
of the auto-discovered conftest avoids breaking every test run when the DB
isn't available locally.

This file assumes the presence of `client`, `auth_header`, and
`other_auth_header` fixtures. Those DO NOT YET EXIST in the repo's top-
level conftest.py — T22 will wire a TestClient + JWT helper there. Until
then, tests that use them collect-skip with a clear reason.
"""
import os
import uuid
from datetime import datetime, timezone

import pytest


@pytest.fixture
async def seeded():
    """Insert a TEAMS_USER_CHATS resource + snapshot + 3 single-thread msgs +
    1 off-thread msg so MULTI_THREAD_NOT_SUPPORTED_YET can be exercised.

    Skips when DB isn't reachable or required tables don't exist. Production
    value is through INTEGRATION=1 runs against a live Postgres; the unit-
    test path is gated.
    """
    if not os.environ.get("INTEGRATION"):
        pytest.skip("seeded fixture needs a real DB; set INTEGRATION=1 to run")

    # Imports deferred — shared.database requires Postgres env vars, and we
    # don't want import-time side effects for the non-integration path.
    from shared.database import async_session_factory  # type: ignore
    from shared.models import (  # type: ignore
        Resource,
        ResourceType,
        Snapshot,
        SnapshotItem,
        Tenant,
    )

    async with async_session_factory() as s:
        t = Tenant(
            id=uuid.uuid4(),
            org_id=uuid.uuid4(),
            display_name="Test Tenant",
            external_tenant_id=f"ext-{uuid.uuid4()}",
            extra_data={"limits": {"chat_export_concurrent": 5}},
        )
        u_id = uuid.uuid4()
        other_id = uuid.uuid4()
        r = Resource(
            id=uuid.uuid4(),
            tenant_id=t.id,
            type=ResourceType.TEAMS_CHAT_EXPORT,
            external_id=f"chat-{uuid.uuid4()}",
            display_name="Akshat Verma",
        )
        snap = Snapshot(
            id=uuid.uuid4(),
            resource_id=r.id,
            started_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        s.add_all([t, r, snap])
        await s.flush()

        msgs = []
        for i in range(3):
            msgs.append(
                SnapshotItem(
                    id=uuid.uuid4(),
                    snapshot_id=snap.id,
                    tenant_id=t.id,
                    item_type="TEAMS_CHAT_MESSAGE",
                    external_id=f"m{i}",
                    name=f"msg-{i}",
                    folder_path="chats/Test",
                    created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    content_size=100,
                    extra_data={
                        "from": {"display_name": "Amit"},
                        "body": {"content_type": "html", "content_preview": f"<p>hi{i}</p>"},
                        "raw": {"body": {"contentType": "html", "content": f"<p>hi{i}</p>"}},
                    },
                )
            )
        other_thread = SnapshotItem(
            id=uuid.uuid4(),
            snapshot_id=snap.id,
            tenant_id=t.id,
            item_type="TEAMS_CHAT_MESSAGE",
            external_id="om1",
            name="other-msg",
            folder_path="chats/Other",
            created_at=datetime.now(timezone.utc).replace(tzinfo=None),
            content_size=100,
            extra_data={
                "from": {"display_name": "X"},
                "raw": {"body": {"contentType": "html", "content": "<p>x</p>"}},
            },
        )
        s.add_all(msgs + [other_thread])
        await s.commit()
        yield {
            "tenant_id": str(t.id),
            "user_id": str(u_id),
            "other_user_id": str(other_id),
            "resource_id": str(r.id),
            "snapshot_id": str(snap.id),
            "thread_path": "chats/Test",
            "message_count": 3,
            "multi_thread_ids": [str(msgs[0].id), str(other_thread.id)],
        }


@pytest.fixture
async def seeded_missing_att(seeded):
    """Add a CHAT_ATTACHMENT item with a blob_path that does not exist in
    storage so the export worker exercises the soft-fail / placeholder path.
    """
    if not os.environ.get("INTEGRATION"):
        pytest.skip("seeded_missing_att fixture needs a real DB; set INTEGRATION=1 to run")

    from shared.database import async_session_factory  # type: ignore
    from shared.models import SnapshotItem  # type: ignore

    async with async_session_factory() as s:
        s.add(SnapshotItem(
            id=uuid.uuid4(),
            snapshot_id=seeded["snapshot_id"],
            tenant_id=seeded["tenant_id"],
            item_type="CHAT_ATTACHMENT",
            external_id="att1",
            parent_external_id="m0",
            name="gone.pdf",
            blob_path="missing/not/here.pdf",
            content_size=10,
            extra_data={"name": "gone.pdf", "content_type": "application/pdf"},
        ))
        await s.commit()
    return seeded
