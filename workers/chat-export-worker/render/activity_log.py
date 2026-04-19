"""Build _meta/activity.log, _meta/activity.json, _meta/manifest.json."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from typing import Iterable

from workers.chat_export_worker.render.normalizer import RenderMessage


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stats(messages: Iterable[RenderMessage]) -> dict:
    msgs = list(messages)
    deleted = sum(1 for m in msgs if m.deleted_at)
    edited = sum(1 for m in msgs if m.last_edited_at)
    replies = sum(1 for m in msgs if m.is_reply)
    reactions: Counter = Counter()
    senders: Counter = Counter()
    min_at, max_at = None, None
    for m in msgs:
        for r in m.reactions:
            if r.emoji:
                reactions[r.emoji] += 1
        senders[m.sender_name] += 1
        if m.created_at:
            if min_at is None or m.created_at < min_at:
                min_at = m.created_at
            if max_at is None or m.created_at > max_at:
                max_at = m.created_at
    return {
        "messages": len(msgs),
        "deleted": deleted,
        "edited": edited,
        "replies": replies,
        "reactions_total": sum(reactions.values()),
        "reactions_by_emoji": dict(reactions),
        "senders": dict(senders),
        "date_range": [_iso(min_at), _iso(max_at)],
    }


def build_activity_text(*, messages, user_email, tenant_name, resource_name,
                        scope, snapshot_at, format, attachments, inline_images,
                        size_bytes, sha256, generated_at: datetime | None = None) -> str:
    gen = generated_at or datetime.now(timezone.utc)
    s = _stats(messages)
    lines = [
        "# TMvault chat export activity log",
        f"# Generated {_iso(gen)}",
        "",
        "## Export",
        f"  user:          {user_email}",
        f"  tenant:        {tenant_name}",
        f"  resource:      {resource_name}",
        f"  scope:         {scope}",
        f"  snapshot:      {_iso(snapshot_at)}",
        f"  format:        {format}",
        f"  messages:      {s['messages']}",
        f"  attachments:   {attachments}",
        f"  inline images: {inline_images}",
        f"  size:          {size_bytes} bytes",
        f"  sha256:        {sha256}",
        "",
        "## Thread activity events",
    ]
    for m in messages:
        if not m.event:
            continue
        ts = _iso(m.created_at) or ""
        extra = []
        if m.event.initiator:
            extra.append(f"initiator={m.event.initiator}")
        if m.event.duration_seconds is not None:
            mm, ss = divmod(m.event.duration_seconds, 60)
            extra.append(f"duration={mm}m {ss}s")
        if m.event.participants:
            extra.append(f"participants=[{', '.join(m.event.participants)}]")
        if m.event.members:
            extra.append(f"members=[{', '.join(m.event.members)}]")
        if m.event.new_chat_name:
            extra.append(f'new_name="{m.event.new_chat_name}"')
        lines.append(f"{ts}  {m.event.kind:20s}  " + ", ".join(extra))
    lines += [
        "",
        "## Message stats",
        f"  deleted:   {s['deleted']}",
        f"  edited:    {s['edited']}",
        f"  replies:   {s['replies']}",
        f"  reactions: {s['reactions_total']} ({s['reactions_by_emoji']})",
        f"  senders:   {s['senders']}",
        f"  range:     {s['date_range'][0]} \u2192 {s['date_range'][1]}",
        "",
    ]
    return "\n".join(lines)


def build_activity_json(*, messages, user_email, tenant_name, resource_name,
                        scope, snapshot_at, format, attachments, inline_images,
                        size_bytes, sha256, generated_at: datetime | None = None) -> str:
    gen = generated_at or datetime.now(timezone.utc)
    events = []
    for m in messages:
        if not m.event:
            continue
        events.append({
            "at": _iso(m.created_at),
            "kind": m.event.kind,
            "initiator": m.event.initiator,
            "participants": m.event.participants,
            "duration_seconds": m.event.duration_seconds,
            "new_chat_name": m.event.new_chat_name,
            "members": m.event.members,
            "raw_odata_type": m.event.raw_odata_type,
        })
    doc = {
        "schema_version": "1.0",
        "generated_at": _iso(gen),
        "export": {
            "user": user_email,
            "tenant": tenant_name,
            "resource": resource_name,
            "scope": scope,
            "snapshot_at": _iso(snapshot_at),
            "format": format,
            "attachments": attachments,
            "inline_images": inline_images,
            "size_bytes": size_bytes,
            "sha256": sha256,
        },
        "events": events,
        "stats": _stats(messages),
    }
    return json.dumps(doc, ensure_ascii=False, indent=2)


def build_manifest(*, job_id: str, generated_at: datetime, files: list[dict], zip_sha256: str) -> str:
    total_bytes = sum(f["bytes"] for f in files)
    doc = {
        "schema_version": "1.0",
        "job_id": job_id,
        "generated_at": _iso(generated_at),
        "total_files": len(files),
        "total_bytes": total_bytes,
        "sha256": zip_sha256,
        "files": files,
    }
    return json.dumps(doc, ensure_ascii=False, indent=2)
