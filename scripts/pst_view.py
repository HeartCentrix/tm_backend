#!/usr/bin/env python3
"""pst_view.py — list, view, or extract the contents of a PST file.

Backed by pypff (libpff). Install once:

    pip install libpff-python

Usage:
    pst_view.py <file.pst>                    # tree summary (folders + counts)
    pst_view.py <file.pst> --list             # one row per message
    pst_view.py <file.pst> --show N           # full dump of message #N (0-based, depth-first)
    pst_view.py <file.pst> --extract <outdir> # dump every message to outdir/<folder>/<n>.eml

Add --html to prefer the HTML body when both are present (default: plain).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from email.message import EmailMessage
from email.utils import format_datetime
from pathlib import Path

try:
    import pypff
except ImportError:
    sys.stderr.write(
        "pypff not installed. Run: pip install libpff-python\n"
    )
    sys.exit(2)


# ---------------------------------------------------------------------------
# Walking helpers
# ---------------------------------------------------------------------------

def walk_folders(folder, path=""):
    """Yield (path, folder) for every folder, depth-first."""
    name = (folder.name or "").strip() or "<root>"
    full = f"{path}/{name}" if path else name
    yield full, folder
    for i in range(folder.number_of_sub_folders):
        try:
            sub = folder.get_sub_folder(i)
        except Exception as exc:
            print(f"  ! sub_folder #{i} error: {exc}", file=sys.stderr)
            continue
        yield from walk_folders(sub, full)


def iter_messages(root):
    """Yield (n, folder_path, folder, msg_index, msg) across the whole PST."""
    n = 0
    for path, folder in walk_folders(root):
        for j in range(folder.number_of_sub_messages):
            try:
                msg = folder.get_sub_message(j)
            except Exception as exc:
                print(f"  ! {path}#{j} get_sub_message error: {exc}", file=sys.stderr)
                continue
            yield n, path, folder, j, msg
            n += 1


# ---------------------------------------------------------------------------
# Message helpers
# ---------------------------------------------------------------------------

def safe(getter, default=None):
    try:
        return getter()
    except Exception:
        return default


def msg_summary(msg) -> dict:
    return {
        "subject": safe(lambda: msg.subject) or "",
        "sender": safe(lambda: msg.sender_name) or "",
        "received": safe(lambda: msg.delivery_time),
        "submitted": safe(lambda: msg.client_submit_time),
        "size": safe(lambda: msg.get_message_size_extended) or 0,
    }


def msg_bodies(msg) -> tuple[str, str]:
    """Return (plain, html). Either may be ''."""
    plain = ""
    html = ""
    raw = safe(lambda: msg.plain_text_body) or b""
    if raw:
        plain = raw.decode("utf-8", errors="replace")
    raw = safe(lambda: msg.html_body) or b""
    if raw:
        html = raw.decode("utf-8", errors="replace")
    return plain, html


def msg_recipients(msg):
    """List of dicts: {type, name, address}. Type is best-effort."""
    out = []
    for rsi in range(msg.number_of_record_sets):
        rs = msg.get_record_set(rsi)
        cur = {}
        for j in range(rs.number_of_entries):
            try:
                e = rs.get_entry(j)
                tag = e.entry_type
                if tag == 0x3001:
                    cur["name"] = safe(e.get_data_as_string, "")
                elif tag == 0x3003:
                    cur["address"] = safe(e.get_data_as_string, "")
                elif tag == 0x0C15:
                    cur["type"] = safe(e.get_data_as_integer, 0)
            except Exception:
                pass
        if cur:
            out.append(cur)
    return [r for r in out if "address" in r or "name" in r]


def msg_attachments(msg):
    out = []
    n = safe(lambda: msg.number_of_attachments, 0) or 0
    for i in range(n):
        try:
            a = msg.get_attachment(i)
            entry = {
                "name": safe(lambda: a.name) or f"attachment_{i}",
                "size": safe(lambda: a.size) or 0,
            }
            try:
                entry["data"] = a.read_buffer(entry["size"]) if entry["size"] else b""
            except Exception:
                entry["data"] = b""
            out.append(entry)
        except Exception as exc:
            out.append({"name": f"attachment_{i}", "size": 0, "data": b"", "error": str(exc)})
    return out


# ---------------------------------------------------------------------------
# Output modes
# ---------------------------------------------------------------------------

def cmd_tree(root):
    for path, folder in walk_folders(root):
        msgs = folder.number_of_sub_messages
        subs = folder.number_of_sub_folders
        depth = path.count("/")
        indent = "  " * depth
        print(f"{indent}{path.split('/')[-1]:<40}  msgs={msgs}  subs={subs}")


def cmd_list(root):
    print(f"{'#':>4}  {'folder':<40}  {'sender':<25}  subject")
    for n, path, _folder, _j, msg in iter_messages(root):
        s = msg_summary(msg)
        print(f"{n:>4}  {path[:40]:<40}  {(s['sender'] or '')[:25]:<25}  {s['subject']}")


def cmd_show(root, target_n: int, prefer_html: bool):
    for n, path, _folder, _j, msg in iter_messages(root):
        if n != target_n:
            continue
        s = msg_summary(msg)
        plain, html = msg_bodies(msg)
        recips = msg_recipients(msg)
        atts = msg_attachments(msg)

        sep = "-" * 78
        print(sep)
        print(f"# message {n} in {path}")
        print(f"Subject : {s['subject']}")
        print(f"From    : {s['sender']}")
        print(f"Received: {s['received']}")
        print(f"Sent    : {s['submitted']}")
        for r in recips:
            tname = {1: "To", 2: "Cc", 3: "Bcc"}.get(r.get("type", 0), "?")
            print(f"{tname:<8}: {r.get('name','')} <{r.get('address','')}>")
        if atts:
            print(f"Attach. : {len(atts)} file(s)")
            for a in atts:
                print(f"          - {a['name']} ({a['size']} bytes)")
        print(sep)
        body = html if (prefer_html and html) else (plain or html)
        print(body or "(empty body)")
        return
    print(f"no message at index {target_n}", file=sys.stderr)
    sys.exit(1)


_RX_BAD = re.compile(r"[^A-Za-z0-9._-]+")

def slugify(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "_"
    return _RX_BAD.sub("_", s)[:80] or "_"


def cmd_extract(root, outdir: Path, prefer_html: bool):
    outdir.mkdir(parents=True, exist_ok=True)
    written = 0
    for n, path, _folder, _j, msg in iter_messages(root):
        s = msg_summary(msg)
        plain, html = msg_bodies(msg)
        recips = msg_recipients(msg)
        atts = msg_attachments(msg)

        em = EmailMessage()
        em["Subject"] = s["subject"] or "(no subject)"
        em["From"] = s["sender"] or "unknown@local"
        for r in recips:
            tname = {1: "To", 2: "Cc", 3: "Bcc"}.get(r.get("type", 0), "To")
            who = r.get("name") or r.get("address") or ""
            addr = r.get("address") or ""
            em.add_header(tname, f"{who} <{addr}>" if addr else who)
        if s.get("submitted"):
            try:
                em["Date"] = format_datetime(s["submitted"])
            except Exception:
                pass

        if prefer_html and html:
            em.set_content(plain or html)
            em.add_alternative(html, subtype="html")
        elif html and plain:
            em.set_content(plain)
            em.add_alternative(html, subtype="html")
        elif html:
            em.set_content(html, subtype="html")
        else:
            em.set_content(plain or "")

        for a in atts:
            try:
                em.add_attachment(
                    a["data"],
                    maintype="application",
                    subtype="octet-stream",
                    filename=a["name"],
                )
            except Exception:
                pass

        folder_dir = outdir / Path(*[slugify(p) for p in path.split("/") if p])
        folder_dir.mkdir(parents=True, exist_ok=True)
        eml_path = folder_dir / f"{n:05d}_{slugify(s['subject'])}.eml"
        eml_path.write_bytes(bytes(em))
        written += 1

    print(f"wrote {written} .eml file(s) under {outdir}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    p = argparse.ArgumentParser(description="View or extract a PST file.")
    p.add_argument("pst", help="path to .pst")
    p.add_argument("--list", action="store_true", help="list every message")
    p.add_argument("--show", type=int, metavar="N", help="dump message N (0-based)")
    p.add_argument("--extract", metavar="OUTDIR", help="write every message as .eml to OUTDIR")
    p.add_argument("--html", action="store_true", help="prefer HTML body when both exist")
    args = p.parse_args(argv)

    if not os.path.exists(args.pst):
        sys.stderr.write(f"file not found: {args.pst}\n")
        sys.exit(1)

    f = pypff.file()
    f.open(args.pst)
    root = f.get_root_folder()
    try:
        if args.show is not None:
            cmd_show(root, args.show, args.html)
        elif args.extract:
            cmd_extract(root, Path(args.extract), args.html)
        elif args.list:
            cmd_list(root)
        else:
            cmd_tree(root)
    finally:
        f.close()


if __name__ == "__main__":
    main()
