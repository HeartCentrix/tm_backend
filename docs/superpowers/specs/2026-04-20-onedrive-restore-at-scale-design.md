# OneDrive Restore at Scale — Design

**Date**: 2026-04-20
**Scope**: Restore pipeline for OneDrive (`USER_ONEDRIVE` / `ONEDRIVE` /
`FILE` / `ONEDRIVE_FILE` / `FILE_VERSION` source rows) across three user-
facing modes, tuned for 3–5k users per tenant and ~400 TiB aggregate
OneDrive content.

## Problem

The current `_restore_file_to_onedrive` in `workers/restore-worker/main.py`
is unusable for real backups:

1. It reads `raw_data.get("content", "")` (an inline JSON field that's
   only populated by a legacy fixture path — real backups store bytes in
   Azure Blob via `SnapshotItem.blob_path`). So most restores silently
   upload an empty file or a JSON blob.
2. It uses a single `PUT /drive/root:/{file_name}:/content`, which Graph
   caps at 4 MB. Any file ≥ 4 MB returns 413.
3. It drops `item.folder_path`, so every restored file lands in the
   drive root. Original folder hierarchy is lost.
4. It supports neither cross-user restore, nor user-chosen target
   folder, nor any conflict behaviour other than "overlay at root".
5. No bounded concurrency, no throttle awareness, no
   upload-session resumability — any transient error on a large file
   re-uploads from byte 0.

## Non-goals

- Restoring historical `FILE_VERSION` rows. The backup captures them;
  restore UI is current-version-only until a follow-up spec.
- Restoring to SharePoint drives from a OneDrive source. That's
  `_restore_file_to_sharepoint` territory; this design is OneDrive →
  OneDrive.

## Modes (from the RestoreModal)

| Modal state | `spec.destination` | `spec.overwrite` | `spec.targetFolder` | `spec.targetUserId` |
|---|---|---|---|---|
| Restore to original · Overwrite | `original` | `true`  | — | — |
| Restore to original · Separate folder | `original` | `false` | `<name>` | — |
| Restore to another resource · Overwrite | `another` | `true`  | — | `<uuid>` |
| Restore to another resource · Separate folder | `another` | `false` | `<name>` | `<uuid>` |

`<uuid>` is the `Resource.id` (DB UUID) of the target OneDrive resource,
chosen from a dropdown the frontend populates from
`getResourcesByType(tenantId, 'ONEDRIVE', …, 'active')`.

**Separate-folder always preserves the original tree.** A file captured
at `/Projects/2024/Q1/report.docx` lands at
`<targetFolder>/Projects/2024/Q1/report.docx`. Original tree collapse
isn't an option — "restore" implies the tree comes with you.

## Frontend changes (`tm_vault/src/components/RestoreModal.tsx`)

1. Extend the existing "mailboxes dropdown" branch (lines 426–472, today
   gated on `resourceKind in {mailbox, shared_mailbox, room_mailbox}`)
   to also match `resourceKind === 'onedrive'`. Reuse the same
   `mailboxTargets*` state vars — rename to `targetResources*` — and
   populate via a single `getResourcesByType` call keyed on the source
   resource's kind.
2. When `destination === 'another'` and `resourceKind === 'onedrive'`,
   render the existing `originalSub` radio pair (`overwrite` /
   `separate_folder`) beneath the dropdown. Same state var, same UI,
   same label text.
3. Validation: `destination === 'another'` requires a dropdown
   selection; `originalSub === 'separate_folder'` requires a non-empty
   `folderName`.
4. Spec payload mapping is unchanged — the four existing fields
   (`destination`, `overwrite`, `targetFolder`, `targetUserId`) cover
   every cell of the table above.

No new API endpoints; job-service's `/api/v1/jobs/restore/sharepoint`
(which also handles OneDrive per current routing) already plumbs all
four fields through to `spec`.

## Backend architecture

### New unit: `OneDriveRestoreEngine`

Lives at `workers/restore-worker/onedrive_restore.py`. One instance per
(job, target drive). Mirrors `MailRestoreEngine`'s contract:

```python
class OneDriveRestoreEngine:
    def __init__(
        self,
        graph_client: GraphClient,
        source_resource: Resource,
        target_drive_user_id: str,          # Graph user id (source or cross-user target)
        tenant_id: str,
        mode: Mode,                         # OVERWRITE | SEPARATE_FOLDER
        *,
        separate_folder_root: Optional[str],
        worker_id: str,
        is_cross_user: bool,
    ): ...
    async def run(self, items: List[SnapshotItem]) -> Dict[str, Any]: ...
```

### What it does, phase by phase

1. **Plan** — partition items by top-level folder so per-folder
   consumers amortise per-prefix folder lookups.
2. **Stream uploads** through a bounded `asyncio.Queue(maxsize=2048)`
   producer/consumer. Producer enqueues `SnapshotItem` rows; consumer
   pool of `ONEDRIVE_RESTORE_CONCURRENCY` (default 16) drains them.
3. **Per consumer**: resolve target path → stream blob bytes into
   Graph → PATCH fileSystemInfo → replay ACL (same-tenant only).
4. **Aggregate** per-file outcomes (`created | overwritten | renamed |
   skipped | failed`) into a job summary.

### Upload engine

For each file:

1. **Resolve target path**:
   - `original + overwrite` → `file_path_on_source_drive` (from
     `item.folder_path` + `item.name`).
   - `original + separate_folder` → `<targetFolder>/<file_path>`.
   - `another + overwrite` → `<file_path>` on target drive.
   - `another + separate_folder` → `<targetFolder>/<file_path>` on
     target drive.
2. **< 4 MB path**: stream blob from Azure → PUT to
   `/users/{target_user}/drive/root:/{path}:/content` with
   `@microsoft.graph.conflictBehavior: replace|rename` header.
3. **≥ 4 MB path**:
   - POST `/drive/root:/{path}:/createUploadSession` with
     `item.@microsoft.graph.conflictBehavior`.
   - Stream blob in **10 MB chunks** using `Content-Range: bytes X-Y/total`.
   - Retry each chunk up to 3× on 500/503/connection reset; the upload
     session URL stays valid across retries, so we never re-upload
     earlier chunks on a mid-file failure.
   - On 429 / Retry-After during a chunk, sleep the advised interval
     and resume the same chunk.
4. **Blob read is streaming**: `download_blob_stream` yields chunks,
   never materialise the whole file in worker memory. A 10 GB restore
   keeps RSS flat.

### Post-upload metadata

Graph's `PATCH /drives/{driveId}/items/{itemId}` accepts
`fileSystemInfo: { createdDateTime, lastModifiedDateTime }`. We populate
both from `extra_data.raw` so Explorer/Outlook shows the original
timestamps, not "restored today".

ACL replay (`_replay_file_permissions`) only runs when
`is_cross_user == False`. Source sharing links / SP group grants don't
translate to a different user's drive and would at best succeed as
no-ops, at worst error on every file.

### Concurrency

- **Per-job consumer pool**: `settings.ONEDRIVE_RESTORE_CONCURRENCY`
  (default 16) worker tasks draining the queue.
- **Worker-wide NIC cap**: shared `self.restore_semaphore =
  asyncio.Semaphore(8)` on `RestoreWorker` so the NIC can't be
  saturated by four concurrent restore jobs each issuing 16 parallel
  uploads (would be 64 streams; the cap holds it to 8 simultaneous
  upload-session chunks).
- **Per-target-user sub-semaphore (5)**: when 5k jobs restore
  simultaneously across distinct target users, this ensures any one
  target's drive sees ≤ 5 concurrent writes — prevents single-user
  Graph 429 cascades.
- **Retry-After / RateLimit-Remaining honored on every Graph call** via
  the existing `_parse_retry_after` helper.

### Job routing

Extend `shared/export_routing.py` with `pick_restore_queue(total_bytes)`:
- `> 50 GB` in job scope → `restore.heavy` (consumed by
  `restore-worker-heavy`).
- Otherwise → `restore.normal`.

Mirrors the existing `pick_backup_queue` / `pick_export_queue` pattern
so big OneDrive restores don't starve quick mail restores on the same
queue.

### Item resolution

Uses the already-landed sibling fan-out in `fetch_snapshot_items`
(`workers/restore-worker/main.py:458`). Picking an incremental snapshot
replays the full running state (delta + everything prior, newest-wins
per `external_id`), matching what the Recovery UI browses.

### Where the new engine hooks in

`restore_in_place` (in-place) and `restore_cross_user` (cross-user) both
currently call `_restore_file_to_onedrive` per item. Replace both call
sites with a single `OneDriveRestoreEngine` invocation pattern, exactly
like `MailRestoreEngine` does for `EMAIL` items:

```python
onedrive_items: List[SnapshotItem] = []
if resource_type in ("ONEDRIVE", "USER_ONEDRIVE"):
    for it in list(resource_items):
        if it.item_type in ("FILE", "ONEDRIVE_FILE"):
            onedrive_items.append(it)
            resource_items.remove(it)

if onedrive_items:
    target_drive_user_id = _resolve_onedrive_target_user(
        session, source_resource, spec,
    )
    engine = OneDriveRestoreEngine(
        graph_client, source_resource, target_drive_user_id,
        tenant_id, Mode.OVERWRITE if spec.get("overwrite") else Mode.SEPARATE_FOLDER,
        separate_folder_root=spec.get("targetFolder"),
        worker_id=self.worker_id,
        is_cross_user=bool(spec.get("targetUserId")),
    )
    summary = await engine.run(onedrive_items)
    restored_count += summary["created"] + summary["overwritten"] + summary["renamed"]
    failed_count += summary["failed"]
```

`_resolve_onedrive_target_user` walks the spec in one of two ways:
- `spec.targetUserId` unset → use `source_resource.external_id` (Graph
  user id of the source drive's owner).
- `spec.targetUserId` set → `session.get(Resource, uuid)`, validate
  `type == ONEDRIVE`, return its `external_id`. Raise if the target
  isn't an active OneDrive resource in the same tenant.

Legacy `_restore_file_to_onedrive` stays as a per-item shim used only
by resource types we don't yet route through the engine (belt and
braces for forward compatibility), rewritten to use the new streaming
+ fileSystemInfo PATCH path so anyone still hitting it doesn't land on
a broken code path.

## Error handling

Per-file outcomes:
- `created` — new file written at target.
- `overwritten` — existing file replaced (`replace` mode matched).
- `renamed` — target existed and Graph auto-suffixed (`rename` mode).
- `skipped` — blob path missing, item excluded by policy.
- `failed` — Graph 4xx (400/403/404/413/507) or exhausted retries.

Individual failures log reason, don't fail the job. The job fails
(`JobStatus.FAILED`) only when:
- target user not found / not active / wrong tenant.
- every file failed with 401 or 403 (credential / permission issue).
- `total_items == 0` post-filter.

Otherwise we report `JobStatus.COMPLETED` with a summary body:

```json
{
  "restored": 12483,
  "failed": 2,
  "skipped": 1,
  "bytes": 48192384512,
  "engine": "onedrive-v2",
  "failed_items": [{"id": "…", "name": "…", "reason": "…"}, …]
}
```

## Testing

Unit:
- Path derivation for all four modes (same-user overwrite, same-user
  separate-folder, cross-user overwrite, cross-user separate-folder),
  including deeply nested `folder_path` values.
- Upload-session chunk boundary math (10 MB × N chunks of a 25 MB
  file; last chunk smaller than chunk size).
- `fileSystemInfo` PATCH payload shape against captured
  `createdDateTime` / `lastModifiedDateTime` strings (including Z /
  +00:00 normalisation).

Integration (mock Graph):
- < 4 MB PUT path with `replace` header → expect one PUT, single
  request.
- ≥ 4 MB upload-session path → expect POST session, N PUT chunks with
  correct Content-Range headers, final 201 on last chunk.
- Mid-upload 503 → chunk retried, session URL reused, eventual
  success.
- Cross-user with target not found → raises, job FAILS.
- Mixed batch of 1000 small + 5 large files → bounded queue drains,
  concurrency never exceeds configured limit, all uploads succeed.

## Config surface

Added to `shared/config.py`:

```python
self.ONEDRIVE_RESTORE_CONCURRENCY = int(os.getenv("ONEDRIVE_RESTORE_CONCURRENCY", "16"))
self.ONEDRIVE_RESTORE_CHUNK_BYTES = int(os.getenv("ONEDRIVE_RESTORE_CHUNK_BYTES", str(10 * 1024 * 1024)))
self.ONEDRIVE_RESTORE_PER_TARGET_USER_CAP = int(os.getenv("ONEDRIVE_RESTORE_PER_TARGET_USER_CAP", "5"))
self.ONEDRIVE_RESTORE_ENGINE_ENABLED = os.getenv("ONEDRIVE_RESTORE_ENGINE_ENABLED", "true").lower() == "true"
```

Feature-flag `ONEDRIVE_RESTORE_ENGINE_ENABLED` defaults on. Setting it
`false` routes back to the legacy per-item shim — escape hatch for any
production regression.

## Migration / rollout

1. Ship the engine with the flag on. Legacy shim stays in place,
   unreachable via normal routing while the flag is on.
2. Monitor `[ONEDRIVE-RESTORE]` logs for Retry-After events and per-file
   failure reasons over the first 48 hours of restore traffic.
3. After a week of clean telemetry, delete the legacy shim.

## Out of scope for this spec

- Versioned restore (`FILE_VERSION` replay).
- OneDrive → SharePoint cross-type restore.
- Schema changes. All new fields are in-memory spec / extra_data.

---

**Word count check**: every non-obvious rule has a one-sentence WHY
inline. No TBDs, no contradictions between the sections. Scope is one
engine + one modal tweak + one config block — single implementation
plan, not a multi-part rollout.
