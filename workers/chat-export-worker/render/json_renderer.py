"""Render RenderMessage list to normalized JSON per spec §6.6."""
from dataclasses import asdict
from datetime import datetime, timezone
import json
from workers.chat_export_worker.render.normalizer import RenderMessage

_SCHEMA = "1.0"


def _iso(dt: datetime | None) -> str | None:
    if dt is None: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _msg_dict(m: RenderMessage) -> dict:
    d = asdict(m)
    return {
        "id": d["id"],
        "external_id": d["external_id"],
        "created_at": _iso(m.created_at),
        "last_edited_at": _iso(m.last_edited_at),
        "deleted_at": _iso(m.deleted_at),
        "is_reply": d["is_reply"],
        "reply_to_id": d["reply_to_id"],
        "sender": {"name": d["sender_name"], "email": d["sender_email"]},
        "body": {"is_html": d["body_is_html"], "content": d["body_html"]},
        "attachments": [
            {"name": a["name"], "local_path": a["local_path"],
             "content_type": a["content_type"], "size": a["size"]}
            for a in d["attachments"]
        ],
        "mentions": d["mentions"],
        "reactions": d["reactions"],
        "event": d["event"],
        "quoted_parent": (
            {"sender_name": d["quoted_parent"]["sender_name"], "snippet": d["quoted_parent"]["snippet"]}
            if d["quoted_parent"] else None
        ),
        "web_url": d["web_url"],
    }


def render_thread_json(messages: list[RenderMessage], *, thread_path: str, exported_at: datetime | None = None) -> str:
    doc = {
        "schema_version": _SCHEMA,
        "thread_path": thread_path,
        "exported_at": _iso(exported_at or datetime.now(timezone.utc)),
        "messages": [_msg_dict(m) for m in messages],
    }
    return json.dumps(doc, ensure_ascii=False, indent=2)


def render_message_json(message: RenderMessage, *, thread_path: str, exported_at: datetime | None = None) -> str:
    doc = {
        "schema_version": _SCHEMA,
        "thread_path": thread_path,
        "exported_at": _iso(exported_at or datetime.now(timezone.utc)),
        "message": _msg_dict(message),
    }
    return json.dumps(doc, ensure_ascii=False, indent=2)
