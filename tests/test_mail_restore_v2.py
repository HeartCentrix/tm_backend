"""Unit tests for mail-restore v2 helpers.

Graph calls are mocked — no real HTTP. Tests focus on the logic of the
folder-path resolver, dedup-sieve construction, payload shaping, and
retry classifier."""
from unittest.mock import AsyncMock, MagicMock
import pytest

from shared.graph_client import GraphClient


@pytest.mark.asyncio
async def test_ensure_mail_folder_path_returns_cached_id_without_graph_call():
    """Second lookup of the same (user, tier, path) must hit the cache
    and issue zero Graph requests."""
    gc = GraphClient.__new__(GraphClient)
    gc._get = AsyncMock(return_value={"value": [{"id": "FOLDER-INBOX", "displayName": "Inbox"}]})
    gc._post = AsyncMock()
    gc._mail_folder_path_cache = {}

    fid1 = await gc.ensure_mail_folder_path("user-1", "primary", "/Inbox")
    fid2 = await gc.ensure_mail_folder_path("user-1", "primary", "/Inbox")

    assert fid1 == "FOLDER-INBOX"
    assert fid2 == "FOLDER-INBOX"
    assert gc._get.await_count == 1  # Second call served from cache.


@pytest.mark.asyncio
async def test_ensure_mail_folder_path_creates_missing_segments():
    """When a segment doesn't exist, POST childFolders and use the
    returned id for the next segment."""
    gc = GraphClient.__new__(GraphClient)
    # First lookup: Inbox exists. Second lookup (Project X under Inbox):
    # empty -> create. Third lookup (Sub under Project X): empty -> create.
    gc._get = AsyncMock(side_effect=[
        {"value": [{"id": "FOLDER-INBOX", "displayName": "Inbox"}]},
        {"value": []},
        {"value": []},
    ])
    gc._post = AsyncMock(side_effect=[
        {"id": "FOLDER-PROJX", "displayName": "Project X"},
        {"id": "FOLDER-SUB", "displayName": "Sub"},
    ])
    gc._mail_folder_path_cache = {}
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    fid = await gc.ensure_mail_folder_path("user-1", "primary", "/Inbox/Project X/Sub")

    assert fid == "FOLDER-SUB"
    # Two POSTs issued — one per missing segment.
    assert gc._post.await_count == 2
    # First POST targets the Inbox id; second targets the Project X id.
    first_url = gc._post.await_args_list[0].args[0]
    second_url = gc._post.await_args_list[1].args[0]
    assert "/mailFolders/FOLDER-INBOX/childFolders" in first_url
    assert "/mailFolders/FOLDER-PROJX/childFolders" in second_url


@pytest.mark.asyncio
async def test_ensure_mail_folder_path_uses_archive_root_for_archive_tier():
    gc = GraphClient.__new__(GraphClient)
    gc._get = AsyncMock(return_value={"value": [{"id": "A-INBOX", "displayName": "Inbox"}]})
    gc._post = AsyncMock()
    gc._mail_folder_path_cache = {}
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    await gc.ensure_mail_folder_path("user-1", "archive", "/Inbox")

    first_url = gc._get.await_args_list[0].args[0]
    assert "mailFolders('archive')/childFolders" in first_url


@pytest.mark.asyncio
async def test_ensure_mail_folder_path_creates_primary_top_level_without_childFolders():
    """Regression: POST /mailFolders/childFolders → 405. Primary tier's
    top-level collection is just /mailFolders; the /childFolders suffix
    only applies to singleton tier roots (archive/recoverable) and to
    nested folders once we have a parent_id."""
    gc = GraphClient.__new__(GraphClient)
    # Parent segment "Restored by TM" doesn't exist → Graph returns empty,
    # triggering a POST to create it at the primary root.
    gc._get = AsyncMock(side_effect=[{"value": []}])
    gc._post = AsyncMock(return_value={"id": "FOLDER-ROOT", "displayName": "Restored by TM"})
    gc._mail_folder_path_cache = {}
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    fid = await gc.ensure_mail_folder_path("user-1", "primary", "/Restored by TM")

    assert fid == "FOLDER-ROOT"
    create_url = gc._post.await_args.args[0]
    # Must POST /users/{u}/mailFolders directly — not /mailFolders/childFolders.
    assert create_url.endswith("/users/user-1/mailFolders")
    assert "childFolders" not in create_url


@pytest.mark.asyncio
async def test_ensure_mail_folder_path_creates_archive_top_level_via_childFolders():
    """Archive + recoverable tiers are singletons, so their top-level
    creates go through /mailFolders('archive')/childFolders."""
    gc = GraphClient.__new__(GraphClient)
    gc._get = AsyncMock(side_effect=[{"value": []}])
    gc._post = AsyncMock(return_value={"id": "A-TOP", "displayName": "Recovered"})
    gc._mail_folder_path_cache = {}
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    fid = await gc.ensure_mail_folder_path("user-1", "archive", "/Recovered")

    assert fid == "A-TOP"
    create_url = gc._post.await_args.args[0]
    assert "mailFolders('archive')/childFolders" in create_url


@pytest.mark.asyncio
async def test_list_folder_internet_message_ids_merges_pages():
    """Handles Graph pagination by following @odata.nextLink."""
    gc = GraphClient.__new__(GraphClient)
    gc._get = AsyncMock(side_effect=[
        {
            "value": [
                {"id": "M1", "internetMessageId": "<a@x>"},
                {"id": "M2", "internetMessageId": "<b@x>"},
            ],
            "@odata.nextLink": "https://graph.microsoft.com/next-page-token",
        },
        {
            "value": [
                {"id": "M3", "internetMessageId": "<c@x>"},
                {"id": "M4", "internetMessageId": None},
            ],
        },
    ])
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    sieve = await gc.list_folder_internet_message_ids("user-1", "FOLDER-ID")

    assert sieve == {"<a@x>": "M1", "<b@x>": "M2", "<c@x>": "M3"}
    assert gc._get.await_count == 2


@pytest.mark.asyncio
async def test_create_message_in_folder_posts_to_correct_url():
    gc = GraphClient.__new__(GraphClient)
    gc._post = AsyncMock(return_value={"id": "NEW-MSG-ID"})
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    msg_id = await gc.create_message_in_folder(
        "user-1", "FOLDER-1", {"subject": "Test", "body": {"contentType": "HTML", "content": "x"}}
    )

    assert msg_id == "NEW-MSG-ID"
    called_url = gc._post.await_args.args[0]
    assert called_url.endswith("/users/user-1/mailFolders/FOLDER-1/messages")


@pytest.mark.asyncio
async def test_patch_message_metadata_only_sends_whitelisted_fields():
    """AFI parity: we only patch mutable metadata on matched messages.
    Body / recipients are not touched. Unknown fields must be filtered."""
    gc = GraphClient.__new__(GraphClient)
    gc._patch = AsyncMock(return_value={})
    gc.GRAPH_URL = "https://graph.microsoft.com/v1.0"

    snapshot_raw = {
        "subject": "should not patch",
        "body": {"contentType": "HTML", "content": "should not patch"},
        "toRecipients": [{"emailAddress": {"address": "x@y"}}],
        "isRead": True,
        "flag": {"flagStatus": "flagged"},
        "importance": "high",
        "categories": ["Red"],
        "random": "ignored",
    }

    await gc.patch_message_metadata("user-1", "EXISTING-ID", snapshot_raw)

    payload = gc._patch.await_args.args[1]
    assert set(payload.keys()) == {"isRead", "flag", "importance", "categories"}
    assert payload["isRead"] is True
    assert payload["flag"] == {"flagStatus": "flagged"}


from types import SimpleNamespace


def _mk_item(*, item_type, external_id, folder_path, tier="primary", raw=None, parent=None):
    """Build a minimal SnapshotItem-like object for engine tests."""
    ed = {"raw": raw or {}}
    if tier != "primary":
        ed["_source_mailbox"] = tier
    if parent:
        ed["parent_item_id"] = parent
    return SimpleNamespace(
        id="iid-" + external_id,
        item_type=item_type,
        external_id=external_id,
        name=(raw or {}).get("subject", ""),
        folder_path=folder_path,
        content_size=0,
        blob_path=None,
        extra_data=ed,
    )


def test_plan_partitions_by_tier_and_folder_and_groups_attachments():
    from workers.restore_worker.mail_restore import MailRestoreEngine

    items = [
        _mk_item(item_type="EMAIL", external_id="m1",
                 folder_path="/Inbox", raw={"internetMessageId": "<a@x>"}),
        _mk_item(item_type="EMAIL", external_id="m2",
                 folder_path="/Inbox/ProjectX", tier="archive",
                 raw={"internetMessageId": "<b@x>"}),
        _mk_item(item_type="EMAIL_ATTACHMENT", external_id="m1::a1",
                 folder_path="/Inbox", parent="m1"),
    ]

    plan = MailRestoreEngine.build_plan(items)

    # 2 message buckets, keyed by (tier, folder_path).
    assert set(plan.messages.keys()) == {
        ("primary", "/Inbox"),
        ("archive", "/Inbox/ProjectX"),
    }
    primary_inbox = plan.messages[("primary", "/Inbox")]
    assert [m.external_id for m in primary_inbox] == ["m1"]
    # Attachment grouped under its parent message id.
    assert plan.attachments["m1"] == [items[2]]
    assert plan.attachments.get("m2", []) == []


@pytest.mark.asyncio
async def test_ensure_folders_calls_graph_per_unique_bucket():
    """One ensure_mail_folder_path call per unique (tier, target_path),
    not per message."""
    from workers.restore_worker.mail_restore import MailRestoreEngine, MODE_OVERWRITE

    gc = MagicMock()
    gc.ensure_mail_folder_path = AsyncMock(side_effect=["FID-INBOX", "FID-SUB"])
    target = SimpleNamespace(external_id="u1", id="r1")
    engine = MailRestoreEngine(gc, target, MODE_OVERWRITE)

    plan = MailRestoreEngine.build_plan([
        _mk_item(item_type="EMAIL", external_id="m1", folder_path="/Inbox"),
        _mk_item(item_type="EMAIL", external_id="m2", folder_path="/Inbox"),
        _mk_item(item_type="EMAIL", external_id="m3", folder_path="/Inbox/Sub"),
    ])

    folder_map = await engine.ensure_folders(plan)

    assert folder_map[("primary", "/Inbox")] == "FID-INBOX"
    assert folder_map[("primary", "/Inbox/Sub")] == "FID-SUB"
    # 2 unique paths → 2 calls even with 3 messages.
    assert gc.ensure_mail_folder_path.await_count == 2


@pytest.mark.asyncio
async def test_ensure_folders_reports_archive_not_provisioned():
    from workers.restore_worker.mail_restore import MailRestoreEngine, MODE_OVERWRITE

    gc = MagicMock()
    gc.ensure_mail_folder_path = AsyncMock(return_value=None)   # tier root missing
    target = SimpleNamespace(external_id="u1", id="r1")
    engine = MailRestoreEngine(gc, target, MODE_OVERWRITE)

    plan = MailRestoreEngine.build_plan([
        _mk_item(item_type="EMAIL", external_id="m1", folder_path="/Inbox", tier="archive"),
    ])

    folder_map = await engine.ensure_folders(plan)
    assert folder_map.get(("archive", "/Inbox")) is None


@pytest.mark.asyncio
async def test_build_sieve_overwrite_only():
    from workers.restore_worker.mail_restore import MailRestoreEngine, MODE_SEPARATE, MODE_OVERWRITE

    gc = MagicMock()
    gc.list_folder_internet_message_ids = AsyncMock(return_value={"<a@x>": "EX-1"})
    target = SimpleNamespace(external_id="u1", id="r1")
    folder_map = {("primary", "/Inbox"): "FID-INBOX"}

    engine_sep = MailRestoreEngine(gc, target, MODE_SEPARATE)
    sieve = await engine_sep.build_sieve(folder_map)
    assert sieve == {}   # Separate mode never needs a sieve.
    gc.list_folder_internet_message_ids.assert_not_called()

    engine_ov = MailRestoreEngine(gc, target, MODE_OVERWRITE)
    sieve = await engine_ov.build_sieve(folder_map)
    assert sieve == {"FID-INBOX": {"<a@x>": "EX-1"}}


@pytest.mark.asyncio
async def test_restore_one_message_patches_on_overwrite_match():
    """Overwrite mode + IMID in sieve → PATCH existing, do not create or
    replay attachments."""
    from workers.restore_worker.mail_restore import MailRestoreEngine, MODE_OVERWRITE, ItemOutcome

    gc = MagicMock()
    gc.create_message_in_folder = AsyncMock()
    gc.patch_message_metadata = AsyncMock()
    target = SimpleNamespace(external_id="u1", id="r1", tenant_id="t1")
    engine = MailRestoreEngine(gc, target, MODE_OVERWRITE)

    msg = _mk_item(item_type="EMAIL", external_id="m1", folder_path="/Inbox",
                   raw={"internetMessageId": "<a@x>", "subject": "s",
                        "body": {"contentType": "HTML", "content": "<p/>"},
                        "isRead": True})
    sieve = {"FID-INBOX": {"<a@x>": "EXISTING"}}

    outcome: ItemOutcome = await engine.restore_one_message(
        msg, folder_id="FID-INBOX", sieve=sieve, attachments=[],
    )

    assert outcome.outcome == "updated"
    assert outcome.graph_message_id == "EXISTING"
    gc.patch_message_metadata.assert_awaited_once()
    gc.create_message_in_folder.assert_not_called()


@pytest.mark.asyncio
async def test_restore_one_message_creates_on_separate_mode_ignoring_sieve():
    from workers.restore_worker.mail_restore import MailRestoreEngine, MODE_SEPARATE

    gc = MagicMock()
    gc.create_message_in_folder = AsyncMock(return_value="NEW-ID")
    gc.patch_message_metadata = AsyncMock()
    target = SimpleNamespace(external_id="u1", id="r1", tenant_id="t1")
    engine = MailRestoreEngine(gc, target, MODE_SEPARATE)

    msg = _mk_item(item_type="EMAIL", external_id="m1", folder_path="/Inbox",
                   raw={"internetMessageId": "<a@x>", "subject": "s",
                        "body": {"contentType": "HTML", "content": "<p/>"}})
    outcome = await engine.restore_one_message(
        msg, folder_id="FID-TARGET", sieve={}, attachments=[],
    )

    assert outcome.outcome == "created"
    assert outcome.graph_message_id == "NEW-ID"
    gc.create_message_in_folder.assert_awaited_once()
    gc.patch_message_metadata.assert_not_called()


def test_shape_message_payload_preserves_mutable_and_recipient_fields():
    from workers.restore_worker.mail_restore import MailRestoreEngine

    raw = {
        "subject": "Hello",
        "body": {"contentType": "HTML", "content": "<p>hi</p>"},
        "toRecipients": [{"emailAddress": {"address": "x@y"}}],
        "ccRecipients": [],
        "bccRecipients": [{"emailAddress": {"address": "hidden@y"}}],
        "sentDateTime": "2026-01-01T00:00:00Z",
        "receivedDateTime": "2026-01-01T00:01:00Z",
        "internetMessageId": "<a@x>",
        "isRead": True,
        "flag": {"flagStatus": "flagged"},
        "importance": "high",
        "categories": ["Red"],
        "conversationId": "conv-1",
        "id": "old-id",
    }
    payload = MailRestoreEngine.shape_message_payload(raw)
    assert payload["subject"] == "Hello"
    assert payload["bccRecipients"][0]["emailAddress"]["address"] == "hidden@y"
    assert "id" not in payload
    assert "conversationId" not in payload


def test_is_retryable_error_classifies_429_and_5xx():
    from workers.restore_worker.mail_restore import _is_retryable

    class FakeHTTPErr(Exception):
        def __init__(self, code):
            self.response = SimpleNamespace(status_code=code, headers={})

    assert _is_retryable(FakeHTTPErr(429)) is True
    assert _is_retryable(FakeHTTPErr(500)) is True
    assert _is_retryable(FakeHTTPErr(502)) is True
    assert _is_retryable(FakeHTTPErr(503)) is True
    assert _is_retryable(FakeHTTPErr(504)) is True
    assert _is_retryable(FakeHTTPErr(400)) is False
    assert _is_retryable(FakeHTTPErr(404)) is False
    assert _is_retryable(ValueError("x")) is False


@pytest.mark.asyncio
async def test_run_aggregates_outcomes_by_category():
    from workers.restore_worker.mail_restore import MailRestoreEngine, MODE_OVERWRITE

    gc = MagicMock()
    gc.ensure_mail_folder_path = AsyncMock(return_value="FID-INBOX")
    gc.list_folder_internet_message_ids = AsyncMock(return_value={"<dup@x>": "EX1"})
    gc.patch_message_metadata = AsyncMock()
    gc.create_message_in_folder = AsyncMock(return_value="NEW-ID")
    target = SimpleNamespace(external_id="u1", id="r1", tenant_id="t1")
    engine = MailRestoreEngine(gc, target, MODE_OVERWRITE)

    items = [
        _mk_item(item_type="EMAIL", external_id="m1",
                 folder_path="/Inbox", raw={"internetMessageId": "<dup@x>", "subject": "a"}),
        _mk_item(item_type="EMAIL", external_id="m2",
                 folder_path="/Inbox", raw={"internetMessageId": "<new@x>",
                                            "subject": "b",
                                            "body": {"contentType": "HTML", "content": ""}}),
    ]

    summary = await engine.run(items)

    assert summary["updated"] == 1
    assert summary["created"] == 1
    assert summary["failed"] == 0


@pytest.mark.asyncio
async def test_run_retries_on_429_then_succeeds(monkeypatch):
    """Regression guard: 429 from Graph must trigger backoff + retry
    via the outer run() loop. restore_one_message re-raises retryables
    so this loop can actually see them."""
    from workers.restore_worker import mail_restore as mr

    # Skip the backoff sleep so the test stays fast.
    async def _no_sleep(_):
        return None
    monkeypatch.setattr(mr.asyncio, "sleep", _no_sleep)

    class Throttled(Exception):
        def __init__(self):
            self.response = SimpleNamespace(status_code=429, headers={"Retry-After": "0"})

    gc = MagicMock()
    gc.ensure_mail_folder_path = AsyncMock(return_value="FID-INBOX")
    gc.list_folder_internet_message_ids = AsyncMock(return_value={})
    # First call 429, second call succeeds.
    gc.create_message_in_folder = AsyncMock(side_effect=[Throttled(), "NEW-ID"])
    target = SimpleNamespace(external_id="u1", id="r1", tenant_id="t1")
    engine = mr.MailRestoreEngine(gc, target, mr.MODE_OVERWRITE)

    items = [
        _mk_item(item_type="EMAIL", external_id="m1", folder_path="/Inbox",
                 raw={"internetMessageId": "<new@x>", "subject": "a",
                      "body": {"contentType": "HTML", "content": ""}}),
    ]
    summary = await engine.run(items)

    assert summary["created"] == 1
    assert summary["failed"] == 0
    assert gc.create_message_in_folder.await_count == 2  # Retried exactly once.
