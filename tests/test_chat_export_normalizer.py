import pytest
from datetime import datetime, timezone
from workers.chat_export_worker.render.normalizer import (
    normalize_messages, RenderMessage, EventDetail,
)


def _item(**over):
    base = dict(
        id="s1", external_id="m1",
        created_at=datetime(2026, 3, 12, 10, 42, tzinfo=timezone.utc),
        last_modified_at=None, deleted_at=None,
        content_size=0,
        folder_path="chats/Thread",
        metadata={
            "type": "teams_chat_message",
            "message_id": "m1",
            "created_at": "2026-03-12T10:42:00Z",
            "message_type": "message",
            "from": {"user_id": "u1", "display_name": "Amit Mishra"},
            "body": {"content_type": "html", "content_preview": "<p>hi</p>"},
            "raw": {"body": {"contentType": "html", "content": "<p>hi</p>"}},
            "event_detail": None,
            "hosted_content_ids": [],
        },
    )
    base.update(over); return base


def test_normalize_single_message_produces_render_message():
    msgs = normalize_messages([_item()], attachments_by_msg={}, hosted_by_msg={}, layout="single_thread")
    assert len(msgs) == 1
    rm = msgs[0]
    assert isinstance(rm, RenderMessage)
    assert rm.sender_name == "Amit Mishra"
    assert rm.sender_initials == "AM"
    assert rm.body_html.startswith("<p>hi")
    assert rm.is_reply is False
    assert rm.event is None


def test_normalize_resolves_reply_quoted_parent():
    parent = _item(external_id="m1", metadata={**_item()["metadata"], "raw": {"body": {"contentType": "html", "content": "<p>Can you share the doc?</p>"}}})
    reply_raw = {**_item()["metadata"]["raw"], "replyToId": "m1", "body": {"contentType": "html", "content": "<p>sure</p>"}}
    reply = _item(external_id="m2", metadata={**_item()["metadata"], "message_id": "m2", "reply_to_id": "m1", "raw": reply_raw})
    msgs = normalize_messages([parent, reply], attachments_by_msg={}, hosted_by_msg={}, layout="single_thread")
    quoted = msgs[1].quoted_parent
    assert quoted is not None
    assert "share the doc" in quoted.snippet
    assert quoted.sender_name == "Amit Mishra"


def test_normalize_system_event_message_renders_event():
    it = _item(metadata={
        **_item()["metadata"],
        "message_type": "systemEventMessage",
        "event_detail": {
            "raw_odata_type": "#microsoft.graph.callEndedEventMessageDetail",
            "kind": "call_ended",
            "initiator": {"display_name": "Amit Mishra"},
            "participants": [{"display_name": "Amit"}, {"display_name": "Akshat"}],
            "call_duration": "PT3M12S",
            "members": [], "new_chat_name": None, "call_event_type": "call", "reason": None,
        },
    })
    rm = normalize_messages([it], attachments_by_msg={}, hosted_by_msg={}, layout="single_thread")[0]
    assert rm.event is not None
    assert rm.event.kind == "call_ended"
    assert rm.event.duration_seconds == 192


def test_normalize_rewrites_inline_img_src():
    it = _item(metadata={
        **_item()["metadata"],
        "hosted_content_ids": ["hc1"],
        "raw": {"body": {
            "contentType": "html",
            "content": '<p><img src="https://graph.microsoft.com/chats/c/messages/m1/hostedContents/hc1/$value"></p>',
        }},
    })
    hosted_by_msg = {"m1": [{"hc_id": "hc1", "local_path": "./attachments/inline/hc1.png"}]}
    rm = normalize_messages([it], attachments_by_msg={}, hosted_by_msg=hosted_by_msg, layout="single_thread")[0]
    assert './attachments/inline/hc1.png' in rm.body_html


def test_sender_initials_and_color_stable():
    a = _item(metadata={**_item()["metadata"], "from": {"display_name": "Amit Mishra", "user_id": "u1"}})
    b = _item(metadata={**_item()["metadata"], "from": {"display_name": "Amit Mishra", "user_id": "u1"}})
    m = normalize_messages([a, b], attachments_by_msg={}, hosted_by_msg={}, layout="single_thread")
    assert m[0].sender_color == m[1].sender_color
    assert m[0].sender_initials == "AM"
