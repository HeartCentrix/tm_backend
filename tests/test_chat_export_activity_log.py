import json
from datetime import datetime, timezone
from workers.chat_export_worker.render.activity_log import (
    build_activity_text, build_activity_json, build_manifest,
)
from workers.chat_export_worker.render.normalizer import RenderMessage, EventDetail, Reaction


def _evt(kind, **o):
    return EventDetail(kind=kind, initiator=o.get("initiator"), participants=o.get("participants", []),
                       duration_seconds=o.get("duration_seconds"), new_chat_name=o.get("new_chat_name"),
                       members=o.get("members", []), raw_odata_type="")


def _rm(kind=None, **o):
    return RenderMessage(
        id=o.get("id", "x"), external_id=o.get("ext", "x"),
        created_at=o.get("at", datetime(2026, 3, 12, 10, 42, tzinfo=timezone.utc)),
        last_edited_at=o.get("edited"), deleted_at=o.get("deleted"),
        is_reply=False, reply_to_id=None,
        sender_name=o.get("sender", "Amit"), sender_email=o.get("email"),
        sender_initials="AM", sender_color="hsl(0deg,50%,55%)",
        body_html=o.get("body", "<p>x</p>"), body_is_html=True,
        reactions=o.get("reactions", []),
        event=_evt(kind, **o) if kind else None,
    )


def test_activity_text_header_and_events():
    ctx = dict(
        user_email="rohit.sharma@qfion.com", tenant_name="Qfion",
        resource_name="Akshat Verma", scope="single_thread — chats/X",
        snapshot_at=datetime(2026, 4, 18, 3, 0, tzinfo=timezone.utc),
        format="HTML", attachments=0, inline_images=0, size_bytes=0, sha256="abc",
    )
    msgs = [
        _rm("call_ended", initiator="Amit", duration_seconds=192, participants=["Amit", "Akshat"]),
        _rm("chat_renamed", initiator="Amit", new_chat_name="Project Alpha"),
        _rm(),
    ]
    out = build_activity_text(messages=msgs, **ctx)
    assert "TMvault chat export activity log" in out
    assert "rohit.sharma@qfion.com" in out
    assert "call_ended" in out
    assert "chat_renamed" in out
    assert "Project Alpha" in out


def test_activity_json_round_trip():
    ctx = dict(user_email="u@x", tenant_name="T", resource_name="R",
               scope="s", snapshot_at=datetime(2026, 4, 18, tzinfo=timezone.utc),
               format="HTML", attachments=1, inline_images=2, size_bytes=100, sha256="h")
    doc = json.loads(build_activity_json(messages=[_rm()], **ctx))
    assert doc["export"]["user"] == "u@x"
    assert doc["stats"]["messages"] == 1


def test_build_manifest_includes_file_list_and_sha():
    files = [{"path": "a.html", "sha256": "hh", "bytes": 10},
             {"path": "a-attachments/x.pdf", "sha256": "ii", "bytes": 20}]
    doc = json.loads(build_manifest(job_id="j", generated_at=datetime(2026, 4, 19, tzinfo=timezone.utc), files=files, zip_sha256="top"))
    assert doc["total_files"] == 2
    assert doc["total_bytes"] == 30
    assert doc["sha256"] == "top"
    assert doc["files"][0]["path"] == "a.html"
