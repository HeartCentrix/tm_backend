"""Resolve job payload -> (messages, attachment_map, hosted_map, layout).

SnapshotItem.resource_id does not exist -- JOIN Snapshot to filter by resource.

Chat message bodies were relocated to ``chat_thread_messages`` in the 2026-05-13
Level 2 refactor. snapshot_items now carries thin pointer rows; we hydrate
each row's ``extra_data['raw']`` from ``chat_thread_messages.metadata_raw``
before handing the list to the renderer.
"""
from dataclasses import dataclass
import mimetypes
from sqlalchemy import select, text
from sqlalchemy.orm import aliased
from shared.models import SnapshotItem, Snapshot, Resource


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

    # Accept parent ENTRA_USER or child USER_CHATS id interchangeably.
    child_ids = [
        r[0] for r in (await sess.execute(
            select(Resource.id).where(Resource.parent_resource_id == resource_id)
        ))
    ]
    resource_ids = [resource_id, *child_ids]

    if thread_path:
        q = (select(SnapshotItem)
             .join(Sn, Sn.id == SnapshotItem.snapshot_id)
             .where(Sn.resource_id.in_(resource_ids))
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
             .where(Sn.resource_id.in_(resource_ids))
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

    # Hydrate each message's raw payload from chat_thread_messages. The
    # renderer expects ``extra_data['raw']`` to carry the full Graph
    # message dict (body, from, attachments, mentions, reactions, etc.).
    # Without this step every exported message would render as "(empty)".
    if msgs:
        ext_ids_raw = [m.external_id for m in msgs if m.external_id]
        if ext_ids_raw:
            hydrate_rows = (await sess.execute(
                text(
                    "SELECT ct.tenant_id, ct.chat_id, "
                    "       ctm.message_external_id, ctm.metadata_raw "
                    "  FROM chat_thread_messages ctm "
                    "  JOIN chat_threads ct ON ct.id = ctm.chat_thread_id "
                    " WHERE ctm.message_external_id = ANY(:ext_ids)"
                ),
                {"ext_ids": ext_ids_raw},
            )).all()
            # Key by (tenant_id, chat_id, message_external_id) — message_external_id
            # alone collides across tenants in theory, and we already have the
            # other identifiers on each SnapshotItem.
            raw_by_key: dict = {}
            for hr in hydrate_rows:
                raw_by_key[(str(hr.tenant_id), hr.chat_id, hr.message_external_id)] = hr.metadata_raw
            for m in msgs:
                raw = raw_by_key.get(
                    (str(m.tenant_id), m.parent_external_id, m.external_id)
                )
                if raw is None:
                    continue
                ed = dict(m.extra_data or {})
                ed["raw"] = raw if isinstance(raw, dict) else {}
                # SQLAlchemy lets us mutate the JSON column even on a
                # detached row; the renderer reads it as a plain dict.
                m.extra_data = ed

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
