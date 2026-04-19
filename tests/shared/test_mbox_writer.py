"""Unit tests for MboxWriter — mboxrd serialization + >From escaping + size rollover."""
import mailbox
import os
import tempfile

import pytest

from shared.mbox_writer import MboxWriter


def test_single_message_roundtrip_via_stdlib(tmp_path):
    eml = (
        b"From: alice@example.com\r\n"
        b"To: bob@example.com\r\n"
        b"Subject: Hi\r\n"
        b"Date: Wed, 10 Apr 2026 12:00:00 +0000\r\n"
        b"\r\n"
        b"body\r\n"
    )
    out = tmp_path / "one.mbox"
    chunks = []

    def emit(data: bytes):
        chunks.append(data)

    w = MboxWriter(emit=emit, split_bytes=10**9)
    w.append_message(eml, sender_addr="alice@example.com", sent_at_epoch=1_712_750_400)
    w.close()

    out.write_bytes(b"".join(chunks))
    mbox = mailbox.mbox(str(out))
    assert len(mbox) == 1
    msg = mbox[0]
    assert msg["Subject"] == "Hi"


def test_from_escape_applied_to_body_lines():
    """A body line that begins with 'From ' must be escaped to '>From ' (mboxrd)."""
    eml = (
        b"From: a@x.com\r\n"
        b"To: b@x.com\r\n"
        b"Subject: t\r\n"
        b"\r\n"
        b"Hello\r\nFrom the server\r\n>From nested\r\ndone\r\n"
    )
    chunks = []
    w = MboxWriter(emit=lambda d: chunks.append(d), split_bytes=10**9)
    w.append_message(eml, sender_addr="a@x.com", sent_at_epoch=1_712_750_400)
    w.close()
    data = b"".join(chunks)
    assert b"\r\n>From the server\r\n" in data
    # Existing '>From' gets an additional '>', yielding '>>From'
    assert b"\r\n>>From nested\r\n" in data


def test_size_rollover_triggers_new_file():
    """When accumulated bytes hit split_bytes, writer fires on_rollover and resets counter."""
    big_eml = b"From: a@x.com\r\nSubject: s\r\n\r\n" + (b"X" * 2000) + b"\r\n"
    rollovers = []
    chunks = []

    def on_rollover(index: int):
        rollovers.append(index)

    w = MboxWriter(
        emit=lambda d: chunks.append(d),
        split_bytes=5000,
        on_rollover=on_rollover,
    )
    for _ in range(3):
        w.append_message(big_eml, sender_addr="a@x.com", sent_at_epoch=1_712_750_400)
    w.close()

    assert len(rollovers) >= 1
