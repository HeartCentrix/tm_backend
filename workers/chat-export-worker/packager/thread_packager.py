"""Stream ZIP — v1 one ZIP per job. Layouts: single_thread | per_message."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import AsyncIterator, Protocol
import io, re, zipfile

from workers.chat_export_worker.render.normalizer import RenderMessage, AttachmentRef
from workers.chat_export_worker.render.html_renderer import render_thread_html, render_message_html
from workers.chat_export_worker.render.json_renderer import render_thread_json, render_message_json
from workers.chat_export_worker.render.activity_log import (
    build_activity_text, build_activity_json, build_manifest,
)

# PDF renderer is imported lazily because WeasyPrint needs system deps.
_STYLES = Path(__file__).resolve().parents[1] / "templates" / "styles.css"


def _safe_name(name: str, fallback: str = "item") -> str:
    s = re.sub(r'[\\/:*?"<>|]+', "_", (name or fallback).strip())
    s = re.sub(r"\s+", " ", s)
    return (s[:120] or fallback)


class AttachmentSource(Protocol):
    async def open(self, blob_path: str) -> tuple[AsyncIterator[bytes], str, int]: ...


@dataclass
class ThreadPackager:
    layout: str
    thread_name: str
    thread_path: str
    format: str
    attachment_source: AttachmentSource

    async def write(self, messages: list[RenderMessage], *, attachment_map: dict,
                    hosted_map: dict, out: io.BufferedIOBase, context: dict | None = None) -> dict:
        ctx = context or {}
        zf = zipfile.ZipFile(out, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True)
        per_file: list[dict] = []
        total_bytes = 0
        tsafe = _safe_name(self.thread_name)

        def _add(path: str, data: bytes):
            nonlocal total_bytes
            zf.writestr(path, data)
            per_file.append({"path": path, "sha256": sha256(data).hexdigest(), "bytes": len(data)})
            total_bytes += len(data)

        async def _read(src_path: str) -> bytes:
            ait, _ctype, _size = await self.attachment_source.open(src_path)
            buf = b""
            async for chunk in ait: buf += chunk
            return buf

        inline_count = 0; attachment_count = 0
        for m in messages:
            att_dir = (f"{tsafe}-attachments" if self.layout == "single_thread"
                       else f"per-message/{tsafe}/{_safe_name(m.external_id)}/attachments")
            new_atts = []
            for att in attachment_map.get(m.external_id, []):
                data = await _read(att["blob_path"])
                target = f"{att_dir}/{_safe_name(att['name'])}"
                _add(target, data)
                new_atts.append(AttachmentRef(
                    name=_safe_name(att["name"]), local_path=f"./{target}",
                    content_type=att.get("content_type"), size=len(data),
                ))
                attachment_count += 1
            m.attachments = new_atts
            for hc in hosted_map.get(m.external_id, []):
                data = await _read(hc["blob_path"])
                target = f"{att_dir}/inline/{hc['hc_id']}{hc.get('ext', '.bin')}"
                _add(target, data); inline_count += 1

        _add("styles.css", _STYLES.read_bytes())

        if self.layout == "single_thread":
            if self.format == "HTML":
                _add(f"{tsafe}.html", render_thread_html(messages, self.thread_name).encode())
            elif self.format == "JSON":
                _add(f"{tsafe}.json", render_thread_json(messages, thread_path=self.thread_path).encode())
            elif self.format == "PDF":
                from workers.chat_export_worker.render.pdf_renderer import render_thread_pdf
                _add(f"{tsafe}.pdf", render_thread_pdf(messages, self.thread_name))
        else:
            for m in messages:
                folder = f"per-message/{tsafe}/{_safe_name(m.external_id)}"
                base = _safe_name(m.external_id)
                if self.format == "HTML":
                    _add(f"{folder}/{base}.html", render_message_html(m, self.thread_name).encode())
                elif self.format == "JSON":
                    _add(f"{folder}/{base}.json",
                         render_message_json(m, thread_path=self.thread_path).encode())
                elif self.format == "PDF":
                    from workers.chat_export_worker.render.pdf_renderer import render_message_pdf
                    _add(f"{folder}/{base}.pdf", render_message_pdf(m, self.thread_name))

        gen_at = datetime.now(timezone.utc)
        kwargs = dict(
            messages=messages,
            user_email=ctx.get("user_email", ""),
            tenant_name=ctx.get("tenant_name", ""),
            resource_name=ctx.get("resource_name", self.thread_name),
            scope=ctx.get("scope", self.thread_path),
            snapshot_at=ctx.get("snapshot_at"),
            format=self.format,
            attachments=attachment_count, inline_images=inline_count,
            size_bytes=total_bytes, sha256="(pending)", generated_at=gen_at,
        )
        _add("_meta/activity.log", build_activity_text(**kwargs).encode())
        _add("_meta/activity.json", build_activity_json(**kwargs).encode())
        zip_sha = sha256(b"".join(f["sha256"].encode() for f in per_file)).hexdigest()
        _add("_meta/manifest.json", build_manifest(
            job_id=ctx.get("job_id", "unknown"),
            generated_at=gen_at, files=per_file, zip_sha256=zip_sha,
        ).encode())
        zf.close()

        return {"attachment_count": attachment_count, "inline_image_count": inline_count,
                "total_bytes": total_bytes, "sha256": zip_sha}
