"""Resolve job payload -> (messages, attachment_map, hosted_map, layout).

SnapshotItem.resource_id does not exist -- JOIN Snapshot to filter by resource.
"""
from dataclasses import dataclass
import mimetypes
from sqlalchemy import select
from sqlalchemy.orm import aliased
from shared.models import SnapshotItem, Snapshot


@dataclass
class ResolvedScope:
    layout: str
    thread_path: str
    messages: list
    attachment_map: dict
    hosted_map: dict


async def resolve(sess, *, resource_id, snapshot_ids, thread_path: str | None,
                  item_ids: list[str]) -> ResolvedScope:
    if thread_path and item_ids:
        raise ValueError("INVALID_SELECTION")
    if not thread_path and not item_ids:
        raise ValueError("SCOPE_EMPTY")

    Sn = aliased(Snapshot)
    CHAT_TYPES = ["TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"]

    if thread_path:
        q = (select(SnapshotItem)
             .join(Sn, Sn.id == SnapshotItem.snapshot_id)
             .where(Sn.resource_id == resource_id)
             .where(SnapshotItem.snapshot_id.in_(snapshot_ids))
             .where(SnapshotItem.item_type.in_(CHAT_TYPES))
             .where(SnapshotItem.folder_path == thread_path)
             .order_by(SnapshotItem.created_at.asc()))
        msgs = list((await sess.execute(q)).scalars())
        layout = "single_thread"
        effective = thread_path
    else:
        q = (select(SnapshotItem)
             .join(Sn, Sn.id == SnapshotItem.snapshot_id)
             .where(Sn.resource_id == resource_id)
             .where(SnapshotItem.id.in_(item_ids))
             .order_by(SnapshotItem.created_at.asc()))
        msgs = list((await sess.execute(q)).scalars())
        paths = {m.folder_path for m in msgs}
        if len(paths) > 1:
            raise ValueError("MULTI_THREAD_NOT_SUPPORTED_YET")
        if not paths:
            raise ValueError("SCOPE_EMPTY")
        effective = next(iter(paths))
        layout = "per_message"

    ext_ids = [m.external_id for m in msgs]
    att_map: dict = {}
    hosted_map: dict = {}
    if ext_ids:
        aq = (select(SnapshotItem)
              .where(SnapshotItem.item_type == "CHAT_ATTACHMENT")
              .where(SnapshotItem.parent_external_id.in_(ext_ids)))
        for a in (await sess.execute(aq)).scalars():
            ed = a.extra_data or {}
            att_map.setdefault(a.parent_external_id, []).append({
                "name": ed.get("name") or a.name or a.external_id,
                "blob_path": a.blob_path,
                "content_type": ed.get("content_type"),
                "size": a.content_size or 0,
            })
        hq = (select(SnapshotItem)
              .where(SnapshotItem.item_type == "CHAT_HOSTED_CONTENT")
              .where(SnapshotItem.parent_external_id.in_(ext_ids)))
        for h in (await sess.execute(hq)).scalars():
            ed = h.extra_data or {}
            ctype = ed.get("content_type") or "application/octet-stream"
            ext = mimetypes.guess_extension(ctype) or ".bin"
            hc_id = h.external_id.split(":", 1)[1] if ":" in h.external_id else h.external_id
            hosted_map.setdefault(h.parent_external_id, []).append({
                "hc_id": hc_id,
                "blob_path": h.blob_path,
                "ext": ext,
                "content_type": ctype,
            })
    return ResolvedScope(layout=layout, thread_path=effective, messages=msgs,
                         attachment_map=att_map, hosted_map=hosted_map)
