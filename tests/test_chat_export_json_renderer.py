from datetime import datetime, timezone
import json
from workers.chat_export_worker.render.json_renderer import render_thread_json, render_message_json
from workers.chat_export_worker.render.normalizer import RenderMessage, AttachmentRef


def _rm():
    return RenderMessage(
        id="1", external_id="m1",
        created_at=datetime(2026, 3, 12, 10, 42, tzinfo=timezone.utc),
        last_edited_at=None, deleted_at=None,
        is_reply=False, reply_to_id=None,
        sender_name="Amit", sender_email="a@x",
        sender_initials="A", sender_color="hsl(1deg,50%,55%)",
        body_html="<p>hi</p>", body_is_html=True,
        attachments=[AttachmentRef("report.pdf", "./attachments/report.pdf", "application/pdf", 100)],
    )


def test_render_thread_json_schema_v1():
    doc = json.loads(render_thread_json([_rm()], thread_path="chats/X", exported_at=datetime(2026, 4, 19, tzinfo=timezone.utc)))
    assert doc["schema_version"] == "1.0"
    assert doc["thread_path"] == "chats/X"
    assert doc["exported_at"] == "2026-04-19T00:00:00Z"
    m = doc["messages"][0]
    assert m["id"] == "1"
    assert m["sender"] == {"name": "Amit", "email": "a@x"}
    assert m["attachments"][0]["local_path"] == "./attachments/report.pdf"
    assert m["body"]["is_html"] is True


def test_render_message_json_single_message_shape():
    doc = json.loads(render_message_json(_rm(), thread_path="chats/X"))
    assert doc["thread_path"] == "chats/X"
    assert doc["message"]["id"] == "1"
