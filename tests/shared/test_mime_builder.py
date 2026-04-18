"""Unit tests for mime_builder — Graph JSON → RFC 5322 EML bytes."""
import email
from email.parser import BytesParser

from shared.mime_builder import build_eml


def _parse(raw: bytes):
    return BytesParser().parsebytes(raw)


def test_text_only_eml():
    graph_msg = {
        "subject": "Hello world",
        "from": {"emailAddress": {"address": "alice@example.com", "name": "Alice"}},
        "toRecipients": [{"emailAddress": {"address": "bob@example.com", "name": "Bob"}}],
        "body": {"contentType": "text", "content": "Plain body\r\nLine two"},
        "receivedDateTime": "2026-04-10T12:00:00Z",
        "internetMessageId": "<msg-001@example.com>",
    }

    raw = build_eml(graph_msg, attachments=[])
    assert isinstance(raw, bytes)

    msg = _parse(raw)
    assert msg["Subject"] == "Hello world"
    assert "alice@example.com" in msg["From"]
    assert "bob@example.com" in msg["To"]
    assert msg["Message-ID"] == "<msg-001@example.com>"
    assert "Plain body" in msg.get_payload(decode=True).decode("utf-8")
