import io, zipfile
from datetime import datetime, timezone
import pytest
from workers.chat_export_worker.packager.thread_packager import ThreadPackager, AttachmentSource
from workers.chat_export_worker.render.normalizer import RenderMessage, AttachmentRef


def _rm():
    return RenderMessage(
        id="1", external_id="m1",
        created_at=datetime(2026, 3, 12, 10, 42, tzinfo=timezone.utc),
        last_edited_at=None, deleted_at=None,
        is_reply=False, reply_to_id=None,
        sender_name="Amit", sender_email="a@x",
        sender_initials="A", sender_color="hsl(1deg,50%,55%)",
        body_html="<p>hi</p>", body_is_html=True,
    )


class _Src(AttachmentSource):
    def __init__(self, blobs): self.blobs = blobs
    async def open(self, path):
        async def gen():
            yield self.blobs[path]
        return gen(), "application/pdf", len(self.blobs[path])


@pytest.mark.asyncio
async def test_single_thread_zip_structure():
    msgs = [_rm()]
    src = _Src({"att/report.pdf": b"PDF-DATA"})
    amap = {"m1": [{"name": "report.pdf", "blob_path": "att/report.pdf",
                    "content_type": "application/pdf", "size": 8}]}
    buf = io.BytesIO()
    pkg = ThreadPackager(layout="single_thread", thread_name="T",
                         thread_path="chats/T", format="HTML", attachment_source=src)
    await pkg.write(msgs, attachment_map=amap, hosted_map={}, out=buf)
    z = zipfile.ZipFile(io.BytesIO(buf.getvalue()))
    names = z.namelist()
    assert "T.html" in names
    assert "T-attachments/report.pdf" in names
    assert "_meta/activity.log" in names
    assert "_meta/manifest.json" in names
    html = z.read("T.html").decode()
    assert './T-attachments/report.pdf' in html


@pytest.mark.asyncio
async def test_per_message_zip_structure():
    msgs = [_rm()]
    src = _Src({"att/report.pdf": b"X"})
    amap = {"m1": [{"name": "report.pdf", "blob_path": "att/report.pdf",
                    "content_type": "application/pdf", "size": 1}]}
    buf = io.BytesIO()
    pkg = ThreadPackager(layout="per_message", thread_name="T",
                         thread_path="chats/T", format="HTML", attachment_source=src)
    await pkg.write(msgs, attachment_map=amap, hosted_map={}, out=buf)
    names = zipfile.ZipFile(io.BytesIO(buf.getvalue())).namelist()
    assert any(n.startswith("per-message/T/") and n.endswith(".html") for n in names)
    assert any("/attachments/report.pdf" in n for n in names)
