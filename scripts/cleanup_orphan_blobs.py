"""Sweep orphaned blobs from the active storage backend.

Lists every object in every container known to the shard manager,
cross-references each ``(container, key)`` against
``snapshot_items.blob_path`` + ``extra_data.attachment_blob_paths``,
and deletes anything not referenced.

Safe to re-run: missing keys are no-ops, referenced keys are skipped.
Defaults to ``--dry-run`` so the first invocation just reports what
would be deleted.

Usage from the backup-worker / job-service container::

    docker compose exec backup-worker python -m scripts.cleanup_orphan_blobs --dry-run
    docker compose exec backup-worker python -m scripts.cleanup_orphan_blobs --commit

Optional flags::

    --containers <name>,<name>   limit sweep to specific container(s)
    --min-age-min <int>          skip blobs newer than N minutes (avoid
                                 racing in-flight backups). Default: 5.
    --batch-size <int>           list/delete batch size (default 500).

Output: counts per container + a tail of orphan keys deleted (or
would-be-deleted in dry-run).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Set

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import text


async def _build_referenced_set(session_factory, batch: int) -> Set[str]:
    """Pull every ``blob_path`` + every ``attachment_blob_paths`` entry
    from snapshot_items in batches. Returns a set of all referenced
    keys (ignoring container — false positives only mean we skip a
    delete, never delete a referenced blob).
    """
    keys: Set[str] = set()
    last_id = ""
    async with session_factory() as session:
        # snapshot_items.blob_path — one query, paginated.
        while True:
            rows = (await session.execute(
                text("""
                    SELECT id::text, blob_path, metadata
                    FROM snapshot_items
                    WHERE id::text > :last_id
                    ORDER BY id
                    LIMIT :batch
                """),
                {"last_id": last_id, "batch": batch},
            )).all()
            if not rows:
                break
            for _rid, bp, ed in rows:
                if bp:
                    keys.add(bp)
                if ed:
                    try:
                        atts = (
                            ed.get("attachment_blob_paths")
                            if isinstance(ed, dict) else None
                        ) or []
                    except Exception:
                        atts = []
                    for a in atts:
                        if a:
                            keys.add(a)
            last_id = rows[-1][0]
    return keys


async def _list_containers(shard) -> list[str]:
    """Return every container/bucket the shard exposes. Falls back to
    a hardcoded list for shards that don't expose a list-buckets call."""
    if hasattr(shard, "list_containers"):
        try:
            return [c async for c in shard.list_containers()]
        except Exception:
            pass
    # Common TMvault containers — covers all production paths
    return [
        "email", "mailbox", "files", "sharepoint",
        "exports", "chat-export", "package",
    ]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true",
                    help="Actually delete. Without this, dry-run only.")
    ap.add_argument("--containers", default="",
                    help="Comma-separated container list to limit the sweep.")
    ap.add_argument("--min-age-min", type=int, default=5,
                    help="Skip blobs younger than this (minutes). Avoids racing in-flight backups.")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--print-keys", type=int, default=10,
                    help="How many orphan keys to print per container.")
    args = ap.parse_args()

    # Resolve the ACTIVE backend (SeaweedFS or Azure depending on
    # system_config.active_backend_id). The legacy azure_storage_manager
    # always returns the Azure shard regardless — wrong on SeaweedFS-
    # active deployments.
    from shared.storage.startup import startup_router
    from shared.storage.router import router
    from shared.database import async_session_factory

    await startup_router()
    shard = router.get_active_store()
    print(f"[cleanup] active shard kind={getattr(shard, 'kind', '?')}")

    print("[cleanup] building referenced-blob set from DB...")
    t0 = time.monotonic()
    referenced = await _build_referenced_set(async_session_factory, args.batch_size)
    print(f"[cleanup] referenced set size={len(referenced)} (built in {time.monotonic()-t0:.1f}s)")

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=args.min_age_min)
    grand_total = {"listed": 0, "orphaned": 0, "bytes": 0, "deleted": 0, "errors": 0}

    # SeaweedFS: ONE physical bucket holds many logical "containers" as the
    # first path segment. DB blob_path stores the relpath WITHOUT the
    # logical container prefix. We strip the leading segment from each
    # listed key before comparing to the referenced set, and delete via
    # the raw S3 client (BackendStore.delete needs (container, path) but
    # the (container, path) split is exactly what we strip).
    is_seaweed = getattr(shard, "kind", "") == "seaweedfs"

    if is_seaweed:
        physical_buckets = list(getattr(shard, "_buckets", []) or [])
        print(f"[cleanup] seaweedfs physical buckets: {physical_buckets}")
        for bucket in physical_buckets:
            listed = 0
            orphaned = 0
            bytes_orph = 0
            deleted = 0
            errors = 0
            sample: list[str] = []

            async with shard._client_ctx() as s3:
                paginator = s3.get_paginator("list_objects_v2")
                async for page in paginator.paginate(Bucket=bucket):
                    contents = page.get("Contents") or []
                    to_delete: list[dict] = []
                    for obj in contents:
                        full_key = obj["Key"]
                        size = obj.get("Size", 0) or 0
                        last_mod = obj.get("LastModified")
                        listed += 1

                        # Strip the leading "<container>/" segment so
                        # the relpath matches DB blob_path format.
                        relpath = full_key.split("/", 1)[1] \
                            if "/" in full_key else full_key

                        if relpath in referenced:
                            continue
                        if last_mod and last_mod > cutoff:
                            continue

                        orphaned += 1
                        bytes_orph += size
                        if len(sample) < args.print_keys:
                            sample.append(full_key)
                        if args.commit:
                            to_delete.append({"Key": full_key})

                    if args.commit and to_delete:
                        # S3 batch-delete: 1000 keys max per call.
                        for i in range(0, len(to_delete), 1000):
                            chunk = to_delete[i:i + 1000]
                            try:
                                resp = await s3.delete_objects(
                                    Bucket=bucket,
                                    Delete={"Objects": chunk, "Quiet": True},
                                )
                                err_list = resp.get("Errors") or []
                                deleted += len(chunk) - len(err_list)
                                errors += len(err_list)
                                if err_list and errors <= 5:
                                    print(f"  delete errors sample: {err_list[:3]}")
                            except Exception as exc:
                                errors += len(chunk)
                                print(f"  delete batch failed on {bucket}: {exc}")

            print(
                f"[cleanup] bucket={bucket}: listed={listed} "
                f"orphaned={orphaned} ({bytes_orph / 1024 / 1024:.1f} MiB) "
                f"deleted={deleted} errors={errors}"
            )
            if sample:
                print(f"  sample: {sample[:args.print_keys]}")

            grand_total["listed"] += listed
            grand_total["orphaned"] += orphaned
            grand_total["bytes"] += bytes_orph
            grand_total["deleted"] += deleted
            grand_total["errors"] += errors
    else:
        # Azure / multi-bucket backend: per-container sweep.
        if args.containers:
            containers = [c.strip() for c in args.containers.split(",") if c.strip()]
        else:
            containers = await _list_containers(shard)
        print(f"[cleanup] containers to sweep: {containers}")

        for container in containers:
            listed = 0
            orphaned = 0
            bytes_orph = 0
            deleted = 0
            errors = 0
            sample: list[str] = []

            try:
                walker = shard.list_with_props(container) \
                    if hasattr(shard, "list_with_props") else None
                if walker is None:
                    async for key in shard.list_blobs(container):
                        listed += 1
                        if key in referenced:
                            continue
                        orphaned += 1
                        if args.commit:
                            try:
                                if hasattr(shard, "delete"):
                                    await shard.delete(container, key)
                                else:
                                    await shard.delete_blob(container, key)
                                deleted += 1
                            except Exception as exc:
                                errors += 1
                                if errors <= 3:
                                    print(f"  delete error {container}/{key}: {exc}")
                        if len(sample) < args.print_keys:
                            sample.append(key)
                else:
                    async for key, props in walker:
                        listed += 1
                        if key in referenced:
                            continue
                        if props and getattr(props, "last_modified", None):
                            if props.last_modified > cutoff:
                                continue
                        orphaned += 1
                        bytes_orph += getattr(props, "size", 0) or 0
                        if args.commit:
                            try:
                                if hasattr(shard, "delete"):
                                    await shard.delete(container, key)
                                else:
                                    await shard.delete_blob(container, key)
                                deleted += 1
                            except Exception as exc:
                                errors += 1
                                if errors <= 3:
                                    print(f"  delete error {container}/{key}: {exc}")
                        if len(sample) < args.print_keys:
                            sample.append(key)

            except Exception as exc:
                print(f"[cleanup] container={container} list failed: {exc}")
                continue

            print(
                f"[cleanup] {container}: listed={listed} orphaned={orphaned} "
                f"({bytes_orph / 1024 / 1024:.1f} MiB) deleted={deleted} errors={errors}"
            )
            if sample:
                print(f"  sample: {sample[:args.print_keys]}")

            grand_total["listed"] += listed
            grand_total["orphaned"] += orphaned
            grand_total["bytes"] += bytes_orph
            grand_total["deleted"] += deleted
            grand_total["errors"] += errors

    mode = "COMMIT" if args.commit else "DRY-RUN"
    gb = grand_total["bytes"] / 1024 / 1024 / 1024
    print(
        f"\n[cleanup] === {mode} TOTAL === listed={grand_total['listed']} "
        f"orphaned={grand_total['orphaned']} ({gb:.2f} GiB) "
        f"deleted={grand_total['deleted']} errors={grand_total['errors']}"
    )
    if not args.commit:
        print("[cleanup] re-run with --commit to actually delete.")


if __name__ == "__main__":
    asyncio.run(main())
