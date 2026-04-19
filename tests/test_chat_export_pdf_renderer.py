from datetime import datetime, timezone
import pytest
from workers.chat_export_worker.render.pdf_renderer import render_thread_pdf
from workers.chat_export_worker.render.normalizer import RenderMessage


def _rm():
    return RenderMessage(
        id="1", external_id="m1",
        created_at=datetime(2026, 3, 12, 10, 42, tzinfo=timezone.utc),
        last_edited_at=None, deleted_at=None,
        is_reply=False, reply_to_id=None,
        sender_name="Amit", sender_email="a@x",
        sender_initials="A", sender_color="hsl(1deg,50%,55%)",
        body_html="<p>hello</p>", body_is_html=True,
    )


def test_pdf_renderer_returns_pdf_bytes():
    buf = render_thread_pdf([_rm()], thread_name="t")
    assert buf[:4] == b"%PDF"


def test_pdf_renderer_multi_message_has_content():
    msgs = [_rm() for _ in range(5)]
    buf = render_thread_pdf(msgs, thread_name="t")
    assert len(buf) > 500
