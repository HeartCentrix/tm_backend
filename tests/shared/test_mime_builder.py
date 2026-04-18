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


import asyncio


def test_html_with_attachment():
    graph_msg = {
        "subject": "Report attached",
        "from": {"emailAddress": {"address": "a@x.com"}},
        "toRecipients": [{"emailAddress": {"address": "b@x.com"}}],
        "body": {"contentType": "html", "content": "<p>See attached</p>"},
        "receivedDateTime": "2026-04-10T12:00:00Z",
    }
    from shared.mime_builder import AttachmentRef, build_eml
    att = AttachmentRef(name="report.pdf", content_type="application/pdf", data_bytes=b"%PDF-1.7\n...")
    raw = build_eml(graph_msg, attachments=[att])
    msg = _parse(raw)
    parts = list(msg.walk())
    names = [p.get_filename() for p in parts if p.get_filename()]
    assert "report.pdf" in names


def test_non_ascii_subject_and_address():
    graph_msg = {
        "subject": "Café meeting — é",
        "from": {"emailAddress": {"address": "alice@example.com", "name": "Alíçe"}},
        "toRecipients": [{"emailAddress": {"address": "bob@example.com"}}],
        "body": {"contentType": "text", "content": "ok"},
    }
    from shared.mime_builder import build_eml
    raw = build_eml(graph_msg, attachments=[])
    raw.decode("ascii")  # must not raise — non-ASCII header fields must be MIME-encoded


def test_streaming_builder_yields_bytes():
    from shared.mime_builder import AttachmentRef, build_eml_streaming

    async def run():
        graph_msg = {
            "subject": "Big",
            "from": {"emailAddress": {"address": "a@x.com"}},
            "toRecipients": [{"emailAddress": {"address": "b@x.com"}}],
            "body": {"contentType": "text", "content": "body"},
        }

        async def stream():
            yield b"A" * 4096
            yield b"B" * 4096

        att = AttachmentRef(name="big.bin", content_type="application/octet-stream", data_stream=stream())
        chunks = []
        async for chunk in build_eml_streaming(graph_msg, attachments=[att]):
            chunks.append(chunk)
        combined = b"".join(chunks)
        combined.decode("ascii")
        assert b"Content-Disposition: attachment" in combined
        assert b"big.bin" in combined

    asyncio.run(run())
