#!/usr/bin/env python3
"""Codemod: rewrite ``azure_storage_manager`` call sites to use StorageRouter.

Usage:
  # Dry-run (print what would change, no writes):
  python scripts/migrate_to_router.py

  # Apply a single worker:
  python scripts/migrate_to_router.py --apply --path workers/backup-worker

  # Full repo:
  python scripts/migrate_to_router.py --apply --path .

The script is intentionally conservative. It only rewrites literal patterns
that are safe to auto-transform; anything unexpected is flagged for
manual review. After running, grep for ``azure_storage_manager`` to
see what's left.

Exits 0 with no changes pending, 1 on any rewrite, 2 when flagged
references remain for manual fixup.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


# Directories/files we never rewrite.
EXCLUDE_PREFIXES = (
    "shared/azure_storage.py",
    "shared/storage/",
    "scripts/migrate_to_router.py",
    "docs/",
    "tests/shared/storage/",
)


REPLACEMENTS: list[tuple[re.Pattern, str]] = [
    # Import line variants
    (re.compile(r"^from shared\.azure_storage import azure_storage_manager\s*$", re.M),
     "from shared.storage.router import router"),
    (re.compile(r"(\s)azure_storage_manager\.get_default_shard\(\)"),
     r"\1router.get_active_store()"),
    (re.compile(r"(\s)azure_storage_manager\.get_shard_for_resource\(\s*([^,]+),\s*([^)]+)\)"),
     r"\1router.get_active_store().shard_for(\3, \2)"),
    (re.compile(r"(\s)azure_storage_manager\.get_shard_for_item\(\s*([^)]+)\)"),
     r"\1router.get_store_for_item(\2)"),

    # Shard method renames -> BackendStore method names.
    (re.compile(r"\.upload_blob_from_file\("),  ".upload_from_file("),
    (re.compile(r"\.upload_blob\("),            ".upload("),
    (re.compile(r"\.download_blob_stream\("),   ".download_stream("),
    (re.compile(r"\.download_blob\("),          ".download("),
    (re.compile(r"\.get_blob_properties\("),    ".get_properties("),
    (re.compile(r"\.delete_blob\("),            ".delete("),
    (re.compile(r"\.get_blob_sas_url\("),       ".presigned_url("),
]


def rewrite_file(path: Path, apply: bool) -> tuple[bool, list[str]]:
    text = path.read_text()
    original = text
    for pat, repl in REPLACEMENTS:
        text = pat.sub(repl, text)
    flagged: list[str] = []
    # These `azure_storage_manager` usages are benign and stay on the manager
    # (pure helpers that don't touch storage IO). Don't flag them.
    BENIGN = (".get_container_name(", ".build_blob_path(", "shard_manager=azure_storage_manager")
    for n, line in enumerate(text.splitlines(), 1):
        if "azure_storage_manager" in line and not any(b in line for b in BENIGN):
            flagged.append(f"{path}:{n}: {line.rstrip()}")
    if apply and text != original:
        path.write_text(text)
    return (text != original), flagged


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--path", default=".", help="root to scan")
    args = ap.parse_args()

    root = Path(args.path).resolve()
    repo_root = root if (root / "shared").exists() else root.parent
    changed: list[Path] = []
    flagged_all: list[str] = []

    for py in root.rglob("*.py"):
        rel = py.resolve().relative_to(repo_root).as_posix()
        if any(rel.startswith(x) for x in EXCLUDE_PREFIXES):
            continue
        did_change, flags = rewrite_file(py, apply=args.apply)
        if did_change:
            changed.append(py)
        flagged_all.extend(flags)

    print(f"Files changed: {len(changed)}" + (" (dry-run)" if not args.apply else ""))
    for c in sorted(changed):
        print(f"  {c}")
    if flagged_all:
        print(f"\nUnresolved `azure_storage_manager` references ({len(flagged_all)} sites — manual fix needed):")
        for f in flagged_all[:40]:
            print(f"  {f}")
        if len(flagged_all) > 40:
            print(f"  ... and {len(flagged_all) - 40} more")
        sys.exit(2)
    if not changed:
        print("No changes.")
    sys.exit(1 if changed else 0)


if __name__ == "__main__":
    main()
