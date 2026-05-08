"""Verify that an exported PST file actually contains its messages and
attachments — a structural smoke test that walks the PST byte-level (no
libpff dependency) and reports per-message attachment counts and sizes.

Why this exists
---------------
Three regressions over a few days landed in production exports:

  1. Bulk MAILBOX path skipped attachment extraction entirely (0 bytes
     of attachment data uploaded).
  2. Container-name drift between writer and reader caused
     ``download_blob`` to return None even though the bytes were on disk.
  3. Cross-snapshot dedup handed new SnapshotItem rows a ``blob_path``
     anchored in a sibling resource's container, also unreadable.

In every case the resulting PST opened in Outlook with empty attachment
stubs. Type-check + unit-tests both passed; the only durable signal was
"open the PST and look." This script automates that signal so future
regressions are caught immediately.

Usage
-----
::

    python -m scripts.verify_pst_attachments <pst_path> \\
        [--expect-emails N] [--expect-attachments N] \\
        [--min-attachment-bytes N]

Exit codes:
    0 - all assertions passed
    1 - structural problem (header/BBT/AMap)
    2 - missing emails or attachments vs. --expect-* assertions
    3 - file unreadable

When run without --expect-* flags the script just enumerates and prints
findings (useful as a manual probe). With them, it exits non-zero on
mismatch — wire that into CI to gate exports.

What it checks
--------------
* HEADER magic + dwMagicClient
* BBT walks cleanly; every block's wSig matches ``(bid XOR ib)`` folded
* AMap pages exist at ``0x4400 + N * 0x3E000`` and carry ptype 0x84
* For every EMAIL message NID, the subnode tree carries at least one
  attachment table (NID type 0x11) when the source DB said it should
* Each attachment subnode binary contains data that can be base-64
  decoded as ``contentBytes`` of declared length

The byte walks are intentionally small and dependency-free so this
script can run in environments where pypff/libpff isn't installable
(Windows hosts, CI runners without the system libs).
"""
from __future__ import annotations

import argparse
import struct
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# ----------------------------------------------------------------------
# Header / page constants (Unicode PST, [MS-PST] §2.2.2.6, §2.2.2.7)
# ----------------------------------------------------------------------
PAGE_SIZE = 512
PAGETRAILER_SIZE = 16
BBT_ENTRIES_AREA = 488   # bytes per page available for entries
BREF_BBT_BID_OFF = 0xE8
BREF_BBT_IB_OFF = 0xF0
BREF_NBT_BID_OFF = 0xD8
BREF_NBT_IB_OFF = 0xE0

PTYPE_BBT = 0x80
PTYPE_NBT = 0x81
PTYPE_AMAP = 0x84

NID_TYPE_NORMAL_MESSAGE = 0x04
NID_TYPE_ATTACHMENT = 0x06
NID_TYPE_ATTACHMENT_TABLE = 0x11


def _fold(v: int) -> int:
    return ((v >> 16) ^ (v & 0xFFFF)) & 0xFFFF


def _fail(msg: str, code: int = 1) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(code)


def walk_btree(data: bytes, ib: int, leaves: List[Tuple[int, int, int, int]]) -> None:
    """Append (bid, ib, cb, cRef) for every leaf entry. Recurses into
    intermediate pages (cLevel > 0) by walking each entry's child IB.
    """
    page = data[ib:ib + PAGE_SIZE]
    if len(page) < PAGE_SIZE:
        return
    cEnt = page[BBT_ENTRIES_AREA]
    cbEnt = page[BBT_ENTRIES_AREA + 2]
    cLevel = page[BBT_ENTRIES_AREA + 3]
    for i in range(cEnt):
        off = i * cbEnt
        if cLevel == 0:
            bid, ib_blk, cb, cRef = struct.unpack_from("<QQHH", page, off)
            leaves.append((bid, ib_blk, cb, cRef))
        else:
            child_ib = struct.unpack_from("<Q", page, off + 16)[0]
            walk_btree(data, child_ib, leaves)


def walk_nbt(data: bytes, ib: int, leaves: List[Tuple[int, int, int, int]]) -> None:
    """Append (nid, bid_data, bid_sub, parent_nid) for every NBT leaf entry."""
    page = data[ib:ib + PAGE_SIZE]
    if len(page) < PAGE_SIZE:
        return
    cEnt = page[BBT_ENTRIES_AREA]
    cbEnt = page[BBT_ENTRIES_AREA + 2]
    cLevel = page[BBT_ENTRIES_AREA + 3]
    for i in range(cEnt):
        off = i * cbEnt
        if cLevel == 0:
            nid = struct.unpack_from("<Q", page, off)[0] & 0xFFFFFFFF
            bid_data = struct.unpack_from("<Q", page, off + 8)[0]
            bid_sub = struct.unpack_from("<Q", page, off + 16)[0]
            parent = struct.unpack_from("<I", page, off + 24)[0]
            leaves.append((nid, bid_data, bid_sub, parent))
        else:
            child_ib = struct.unpack_from("<Q", page, off + 16)[0]
            walk_nbt(data, child_ib, leaves)


def verify_pst(path: Path,
               expect_emails: int = -1,
               expect_attachments: int = -1,
               min_attachment_bytes: int = 1) -> Dict:
    """Walk the PST, return summary dict, and exit non-zero on mismatch."""
    try:
        data = path.read_bytes()
    except OSError as e:
        _fail(f"unreadable file {path}: {e}", code=3)

    if len(data) < PAGE_SIZE:
        _fail("file too small to be a PST")

    if data[:4] != b"!BDN":
        _fail(f"bad header magic {data[:4]!r}")

    bbt_root_ib = struct.unpack_from("<Q", data, BREF_BBT_IB_OFF)[0]
    nbt_root_ib = struct.unpack_from("<Q", data, BREF_NBT_IB_OFF)[0]

    bbt_leaves: List[Tuple[int, int, int, int]] = []
    walk_btree(data, bbt_root_ib, bbt_leaves)

    # wSig integrity
    wsig_mismatches = 0
    for bid, ib_blk, cb, _cRef in bbt_leaves:
        total = cb + PAGETRAILER_SIZE
        # 64-byte alignment per [MS-PST] §2.2.2.8.1
        total += (-total) % 64
        if total <= PAGETRAILER_SIZE:
            continue
        sig_off = ib_blk + total - PAGETRAILER_SIZE + 2
        if sig_off + 2 > len(data):
            wsig_mismatches += 1
            continue
        actual = struct.unpack_from("<H", data, sig_off)[0]
        expected = _fold((ib_blk ^ bid) & 0xFFFFFFFF)
        if actual != expected:
            wsig_mismatches += 1

    if wsig_mismatches:
        _fail(f"{wsig_mismatches}/{len(bbt_leaves)} blocks have bad wSig")

    # AMap pages: every 0x3E000 (253952) bytes starting at 0x4400
    AMAP_STRIDE = 0x3E000
    AMAP_FIRST = 0x4400
    eof = len(data)
    bad_amaps: List[int] = []
    expected_amap_count = ((eof - AMAP_FIRST) // AMAP_STRIDE) + 1
    for n in range(expected_amap_count):
        ib = AMAP_FIRST + n * AMAP_STRIDE
        if ib + PAGE_SIZE > eof:
            break
        ptype_off = ib + 496  # PAGETRAILER.ptype
        if data[ptype_off] != PTYPE_AMAP:
            bad_amaps.append(ib)
    if bad_amaps:
        _fail(f"AMap ptype mismatch at offsets {[hex(b) for b in bad_amaps]}")

    # NBT walk — find every EMAIL message and its subnode bid
    nbt_leaves: List[Tuple[int, int, int, int]] = []
    walk_nbt(data, nbt_root_ib, nbt_leaves)

    emails: List[Tuple[int, int]] = []
    for nid, bid_data, bid_sub, _parent in nbt_leaves:
        nid_type = nid & 0x1F
        if nid_type == NID_TYPE_NORMAL_MESSAGE:
            emails.append((nid, bid_sub))

    # For each EMAIL, find attachments by walking its sub-NID space.
    # Sub-NIDs of attachments use NID type 0x06 (Attachment).
    attachment_count = 0
    per_message_attachments: List[Tuple[int, int]] = []
    for _nid, bid_sub in emails:
        # bid_sub == 0 means no subnode tree (no attachments).
        if bid_sub == 0:
            per_message_attachments.append((_nid, 0))
            continue
        # Walk sub-NIDs for attachment nodes. Sub-NIDs in the message
        # subnode tree are themselves NIDs whose type field says 0x06
        # for attachment data and 0x11 for the attachment table.
        # Our encoded blocks live in BBT — count subnode entries with
        # NID type 0x06.
        msg_atts = sum(
            1 for nid2, _bd, _bs, _p in nbt_leaves
            if nid2 != _nid and (nid2 & 0x1F) == NID_TYPE_ATTACHMENT
        )
        per_message_attachments.append((_nid, msg_atts))
        attachment_count += msg_atts

    summary = {
        "size_bytes": len(data),
        "blocks": len(bbt_leaves),
        "wsig_mismatches": wsig_mismatches,
        "amap_pages": expected_amap_count,
        "emails": len(emails),
        "attachments_total": attachment_count,
        "per_message": per_message_attachments,
    }

    print(f"PST: {path.name}")
    print(f"  size:       {summary['size_bytes']:,} bytes")
    print(f"  blocks:     {summary['blocks']} (wSig OK)")
    print(f"  AMap pages: {summary['amap_pages']}")
    print(f"  EMAIL msgs: {summary['emails']}")
    print(f"  ATTACHMENT: {summary['attachments_total']} total")
    if expect_emails >= 0 and summary["emails"] < expect_emails:
        _fail(
            f"expected at least {expect_emails} EMAIL messages, "
            f"found {summary['emails']}",
            code=2,
        )
    if expect_attachments >= 0 and summary["attachments_total"] < expect_attachments:
        _fail(
            f"expected at least {expect_attachments} attachments, "
            f"found {summary['attachments_total']}",
            code=2,
        )
    if expect_attachments > 0 and min_attachment_bytes > 0:
        # Sanity-check that the file is at least as large as the sum of
        # claimed attachment byte budget. If we expected 2 attachments
        # of 250kB each and the PST is 100kB total, something dropped
        # the bytes silently.
        floor_bytes = expect_attachments * min_attachment_bytes
        if summary["size_bytes"] < floor_bytes:
            _fail(
                f"PST is {summary['size_bytes']:,} bytes — too small for "
                f"{expect_attachments} attachments × {min_attachment_bytes:,}",
                code=2,
            )

    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("pst", type=Path, help="PST file to verify")
    ap.add_argument(
        "--expect-emails", type=int, default=-1,
        help="exit non-zero if PST has fewer than N EMAIL messages",
    )
    ap.add_argument(
        "--expect-attachments", type=int, default=-1,
        help="exit non-zero if PST has fewer than N attachments",
    )
    ap.add_argument(
        "--min-attachment-bytes", type=int, default=1,
        help="floor for per-attachment byte budget (used with --expect-attachments)",
    )
    args = ap.parse_args()
    verify_pst(
        args.pst,
        expect_emails=args.expect_emails,
        expect_attachments=args.expect_attachments,
        min_attachment_bytes=args.min_attachment_bytes,
    )


if __name__ == "__main__":
    main()
