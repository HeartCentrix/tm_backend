"""Build RFC 5322 MIME multipart EML bytes from a Graph API message dict.

Used by the mail-export pipeline (restore-worker) to convert each backed-up
message blob into a standards-compliant email that Outlook, Thunderbird, Apple
Mail and eDiscovery tooling can parse without loss.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import format_datetime, formataddr, parsedate_to_datetime
from typing import AsyncIterator, Iterable, List, Optional


@dataclass
class AttachmentRef:
    """Streaming attachment reference: name, content-type, and a bytes source.

    `data_stream` is an async generator that yields raw chunks; use when the
    attachment lives in Azure blob and we want to avoid loading it whole.
    `data_bytes` is the non-streaming alternative for tests and small blobs.
    """
    name: str
    content_type: str
    data_bytes: Optional[bytes] = None
    data_stream: Optional[AsyncIterator[bytes]] = None


def _format_addr(entry: dict) -> str:
    """Graph's emailAddress shape → RFC 5322 address with display-name encoded."""
    addr = entry.get("emailAddress", {}) or {}
    return formataddr((addr.get("name") or "", addr.get("address") or ""))


def _format_addr_list(entries) -> str:
    return ", ".join(_format_addr(e) for e in (entries or []))


def _parse_date(value: Optional[str]):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except Exception:
        from datetime import datetime
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None


def build_eml(graph_msg: dict, attachments: Iterable[AttachmentRef]) -> bytes:
    """Build a fully RFC 5322 compliant EML. Attachments are base64-encoded inline.

    For streaming attachments use build_eml_streaming instead — this function
    materializes attachment bytes in memory and is intended for tests or small
    messages only.
    """
    msg = EmailMessage(policy=SMTP)

    msg["Subject"] = graph_msg.get("subject") or ""
    sender = graph_msg.get("from") or graph_msg.get("sender") or {}
    msg["From"] = _format_addr(sender)
    msg["To"] = _format_addr_list(graph_msg.get("toRecipients"))
    cc = _format_addr_list(graph_msg.get("ccRecipients"))
    if cc:
        msg["Cc"] = cc
    bcc = _format_addr_list(graph_msg.get("bccRecipients"))
    if bcc:
        msg["Bcc"] = bcc

    sent = _parse_date(graph_msg.get("sentDateTime") or graph_msg.get("receivedDateTime"))
    if sent:
        msg["Date"] = format_datetime(sent)

    imi = graph_msg.get("internetMessageId")
    if imi:
        msg["Message-ID"] = imi

    for h in graph_msg.get("internetMessageHeaders") or []:
        name, value = h.get("name"), h.get("value")
        if name and value and name.lower() in {"in-reply-to", "references", "return-path", "received"}:
            msg[name] = value

    body = graph_msg.get("body") or {}
    body_content = body.get("content") or ""
    body_type = (body.get("contentType") or "text").lower()
    if body_type == "html":
        import re
        plain = re.sub(r"<[^>]+>", "", body_content)
        msg.set_content(plain, subtype="plain", charset="utf-8")
        msg.add_alternative(body_content, subtype="html", charset="utf-8")
    else:
        msg.set_content(body_content, subtype="plain", charset="utf-8")

    for att in attachments:
        data = att.data_bytes
        if data is None:
            raise ValueError(
                f"AttachmentRef {att.name} has no data_bytes; "
                "use build_eml_streaming for streaming attachments."
            )
        maintype, _, subtype = (att.content_type or "application/octet-stream").partition("/")
        msg.add_attachment(
            data,
            maintype=maintype or "application",
            subtype=subtype or "octet-stream",
            filename=att.name,
        )

    return bytes(msg)
