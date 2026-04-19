from datetime import datetime, timezone
from workers.chat_export_worker.render.html_renderer import render_thread_html, render_message_html
from workers.chat_export_worker.render.normalizer import RenderMessage, AttachmentRef, EventDetail, QuotedParent


def _rm(**over):
    base = RenderMessage(
        id="1", external_id="m1",
        created_at=datetime(2026, 3, 12, 10, 42, tzinfo=timezone.utc),
        last_edited_at=None, deleted_at=None,
        is_reply=False, reply_to_id=None,
        sender_name="Amit Mishra", sender_email="a@x",
        sender_initials="AM", sender_color="hsl(200deg,50%,55%)",
        body_html="<p>hello</p>", body_is_html=True,
    )
    for k, v in over.items():
        setattr(base, k, v)
    return base


def test_thread_html_renders_sender_bubble_and_timestamp():
    html = render_thread_html([_rm()], thread_name="Akshat Verma | Amit Mishra")
    assert "Amit Mishra" in html
    assert "hello" in html
    assert "AM" in html
    assert "2026-03-12" in html or "10:42" in html


def test_thread_html_renders_quoted_parent_for_reply():
    reply = _rm(is_reply=True, reply_to_id="m1",
                body_html="<p>sure</p>",
                quoted_parent=QuotedParent(sender_name="Amit Mishra", snippet="Can you share the doc?"))
    html = render_thread_html([reply], thread_name="t")
    assert "In reply to" in html
    assert "Can you share the doc?" in html


def test_thread_html_renders_call_ended_event_pill():
    evt = EventDetail(kind="call_ended", initiator="Amit Mishra",
                      participants=["Amit", "Akshat"], duration_seconds=192,
                      new_chat_name=None, members=[], raw_odata_type="")
    rm = _rm(event=evt, body_html="", sender_name="system")
    html = render_thread_html([rm], thread_name="t")
    assert "started a call" in html.lower() or "call ended" in html.lower()
    assert "3 min 12 s" in html or "3m 12s" in html


def test_thread_html_renders_attachment_chip_with_relative_path():
    rm = _rm(attachments=[AttachmentRef(name="report.pdf",
             local_path="./t-attachments/report.pdf",
             content_type="application/pdf", size=1234567)])
    html = render_thread_html([rm], thread_name="t")
    assert 'href="./t-attachments/report.pdf"' in html
    assert "1.2 MB" in html


def test_thread_html_renders_deleted_marker():
    rm = _rm(deleted_at=datetime(2026, 4, 1, tzinfo=timezone.utc), body_html="")
    html = render_thread_html([rm], thread_name="t")
    assert "This message was deleted" in html


def test_message_html_renders_single_bubble_wrapper():
    html = render_message_html(_rm(), thread_name="t")
    assert "<html" in html.lower()
    assert "hello" in html


def test_thread_html_has_no_absolute_paths():
    html = render_thread_html([_rm()], thread_name="t")
    assert "file:///" not in html
