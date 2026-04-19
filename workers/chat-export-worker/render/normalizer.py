"""Turn snapshot_items + related attachment/hosted rows into RenderMessage
instances, ordered chronologically, with reply-parent resolved and inline
image <img src> rewritten to offline paths.

Pure function — no IO. Consumers pre-fetch attachment + hosted dicts.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha1
from typing import Iterable, Literal, Optional
import html as ihtml
import re

import bleach


EventKind = Literal[
    "call_started", "call_ended", "call_recording_ended",
    "members_added", "members_removed", "chat_renamed",
    "teams_app_installed", "teams_app_removed", "unknown",
]

_ALLOWED_TAGS = {
    "p", "br", "div", "span", "a", "img", "strong", "b", "em", "i", "u",
    "code", "pre", "blockquote", "ul", "ol", "li", "table", "tr", "td", "th",
    "thead", "tbody", "h1", "h2", "h3", "h4", "h5", "h6", "hr",
}
_ALLOWED_ATTRS = {"*": ["class", "title"], "a": ["href", "target", "rel"], "img": ["src", "alt"]}


@dataclass
class Mention:
    text: str
    user_display_name: Optional[str]
    user_email: Optional[str]


@dataclass
class Reaction:
    emoji: str
    display_name: Optional[str]
    at: Optional[str]


@dataclass
class AttachmentRef:
    name: str
    local_path: str
    content_type: Optional[str]
    size: int


@dataclass
class QuotedParent:
    sender_name: str
    snippet: str


@dataclass
class EventDetail:
    kind: EventKind
    initiator: Optional[str]
    participants: list[str]
    duration_seconds: Optional[int]
    new_chat_name: Optional[str]
    members: list[str]
    raw_odata_type: str


@dataclass
class RenderMessage:
    id: str
    external_id: str
    created_at: datetime
    last_edited_at: Optional[datetime]
    deleted_at: Optional[datetime]
    is_reply: bool
    reply_to_id: Optional[str]
    sender_name: str
    sender_email: Optional[str]
    sender_initials: str
    sender_color: str
    body_html: str
    body_is_html: bool
    mentions: list[Mention] = field(default_factory=list)
    reactions: list[Reaction] = field(default_factory=list)
    attachments: list[AttachmentRef] = field(default_factory=list)
    event: Optional[EventDetail] = None
    quoted_parent: Optional[QuotedParent] = None
    web_url: Optional[str] = None


def _initials(name: str) -> str:
    parts = [p for p in re.split(r"\s+", (name or "").strip()) if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def _hsl_color(seed: str) -> str:
    h = int(sha1((seed or "anon").encode()).hexdigest()[:8], 16) % 360
    return f"hsl({h}deg, 50%, 55%)"


def _strip_html(h: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", h or "")).strip()


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _iso_duration_to_seconds(iso: Optional[str]) -> Optional[int]:
    if not iso or not iso.startswith("PT"):
        return None
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso)
    if not m:
        return None
    h, mn, s = (int(g) if g else 0 for g in m.groups())
    return h * 3600 + mn * 60 + s


def _rewrite_inline_imgs(html: str, hosted_for_msg: list[dict]) -> str:
    if not hosted_for_msg:
        return html
    def _one(match: re.Match) -> str:
        src = match.group(1)
        for hc in hosted_for_msg:
            if f"/hostedContents/{hc['hc_id']}/" in src or src.endswith(f"/{hc['hc_id']}/$value"):
                return f'src="{hc["local_path"]}"'
        return match.group(0)
    return re.sub(r'src="([^"]+hostedContents[^"]+)"', _one, html or "")


def _sanitize(html: str) -> str:
    return bleach.clean(
        html or "",
        tags=_ALLOWED_TAGS,
        attributes=_ALLOWED_ATTRS,
        strip=True,
        strip_comments=True,
    )


def _event_from_meta(ed: Optional[dict]) -> Optional[EventDetail]:
    if not ed:
        return None
    return EventDetail(
        kind=ed.get("kind") or "unknown",
        initiator=(ed.get("initiator") or {}).get("display_name"),
        participants=[p.get("display_name") for p in ed.get("participants") or [] if p],
        duration_seconds=_iso_duration_to_seconds(ed.get("call_duration")),
        new_chat_name=ed.get("new_chat_name"),
        members=[m.get("display_name") for m in ed.get("members") or [] if m],
        raw_odata_type=ed.get("raw_odata_type") or "",
    )


def normalize_messages(
    items: Iterable[object],
    *,
    attachments_by_msg: dict[str, list[dict]],
    hosted_by_msg: dict[str, list[dict]],
    layout: str,
) -> list[RenderMessage]:
    result: list[RenderMessage] = []
    by_ext_id: dict[str, RenderMessage] = {}

    def g(item, key, default=None):
        if isinstance(item, dict):
            return item.get(key, default)
        return getattr(item, key, default)

    for it in items:
        meta = g(it, "metadata") or {}
        raw = meta.get("raw") or {}
        body_raw = (raw.get("body") or {}).get("content") or meta.get("body", {}).get("content_preview") or ""
        body_is_html = (raw.get("body") or {}).get("contentType") == "html" or meta.get("body", {}).get("content_type") == "html"
        hosted = hosted_by_msg.get(meta.get("message_id") or g(it, "external_id"), [])
        body_rewritten = _rewrite_inline_imgs(body_raw, hosted) if body_is_html else ihtml.escape(body_raw)
        body_safe = _sanitize(body_rewritten) if body_is_html else f"<p>{ihtml.escape(body_raw)}</p>"

        sender = (meta.get("from") or {})
        sender_name = sender.get("display_name") or "Unknown"
        sender_email = sender.get("email") or sender.get("user_id")
        atts_meta = attachments_by_msg.get(meta.get("message_id") or g(it, "external_id"), [])
        attachments = [
            AttachmentRef(
                name=a["name"], local_path=a["local_path"],
                content_type=a.get("content_type"), size=int(a.get("size") or 0),
            ) for a in atts_meta
        ]

        mentions = [
            Mention(
                text=m.get("mention_text") or "",
                user_display_name=(m.get("mentioned") or {}).get("user", {}).get("displayName"),
                user_email=(m.get("mentioned") or {}).get("user", {}).get("userPrincipalName"),
            )
            for m in (raw.get("mentions") or meta.get("mentions") or [])
        ]
        reactions = [
            Reaction(
                emoji=r.get("reactionType") or "",
                display_name=(r.get("user") or {}).get("user", {}).get("displayName"),
                at=r.get("createdDateTime"),
            )
            for r in (raw.get("reactions") or meta.get("reactions") or [])
        ]

        ext_id = meta.get("message_id") or g(it, "external_id")
        reply_to_id = meta.get("reply_to_id") or raw.get("replyToId")

        rm = RenderMessage(
            id=str(g(it, "id")),
            external_id=ext_id,
            created_at=_parse_iso(meta.get("created_at") or raw.get("createdDateTime")) or datetime.min,
            last_edited_at=_parse_iso(raw.get("lastEditedDateTime") or meta.get("last_modified_at")),
            deleted_at=_parse_iso(raw.get("deletedDateTime") or meta.get("deleted_at")),
            is_reply=bool(reply_to_id),
            reply_to_id=reply_to_id,
            sender_name=sender_name,
            sender_email=sender_email,
            sender_initials=_initials(sender_name),
            sender_color=_hsl_color(sender_email or sender_name),
            body_html=body_safe,
            body_is_html=body_is_html,
            mentions=mentions,
            reactions=reactions,
            attachments=attachments,
            event=_event_from_meta(meta.get("event_detail")),
            web_url=raw.get("webUrl") or meta.get("web_url"),
        )
        by_ext_id[ext_id] = rm
        result.append(rm)

    for rm in result:
        if not rm.is_reply or not rm.reply_to_id:
            continue
        parent = by_ext_id.get(rm.reply_to_id)
        if parent is None:
            rm.quoted_parent = QuotedParent(
                sender_name="(unknown)",
                snippet="[referenced message not in backup]",
            )
        else:
            snippet = _strip_html(parent.body_html)
            if len(snippet) > 200:
                snippet = snippet[:200].rstrip() + "…"
            rm.quoted_parent = QuotedParent(sender_name=parent.sender_name, snippet=snippet)

    result.sort(key=lambda r: r.created_at)
    return result
