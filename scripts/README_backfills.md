# Chat export backfills

Two one-off scripts to enrich existing snapshots for the v1 chat-export feature.

## 1. `backfill_event_detail.py` (offline, safe)

Reparses `extra_data["raw"]` on every `TEAMS_CHAT_MESSAGE` for a tenant, fills in
`event_detail` + `hosted_content_ids`. No Graph calls. Idempotent. Run any time.

    python -m scripts.backfill_event_detail --tenant-id <uuid>

Add `--dry-run` to preview counts without writing.

## 2. `backfill_chat_hosted_contents.py` (calls Graph)

Fetches hostedContent binaries for messages whose `hosted_content_ids` are
populated but have no `CHAT_HOSTED_CONTENT` rows yet. Requires a live Graph
token on the tenant (uses the same `multi_app_manager` rotation the
backup-worker does). Microsoft expires hostedContents when a message is
deleted — those are skipped with a warning.

    python -m scripts.backfill_chat_hosted_contents --tenant-id <uuid>
    python -m scripts.backfill_chat_hosted_contents --tenant-id <uuid> --resource-id <uuid>

Run off-peak. Bounded concurrency per worker honors
`chat_hosted_content_concurrency`.

**Run order:** `backfill_event_detail` first (populates hosted_content_ids),
then `backfill_chat_hosted_contents`.
