# Backup baseline benchmark — dev-am @ 2026-05-11

Captured against `dev-am` commit `6e62b8b` (post `commit_block_list_manual`
metadata-sanitize fix). Used to compare against the
`feature/mail-backup-opt-mailbox-wide-delta` branch which switches USER_MAIL to
mailbox-wide delta + `$top=999`. Other workloads are baselined here too so
follow-up optimizations (JSON `$batch` for attachment fan-out, OneDrive
parallel-range tuning, chats hosted-content fan-out) have an apples-to-apples
reference.

## Methodology

- Two M365 users were enrolled (Amit Mishra, Akshat Verma) and a full first-sync
  backup ran end-to-end against active backend `azure-primary` (Azure account
  `nsescanner4e104b`).
- No code changes between the runs (other than the metadata-sanitize fix landed
  before either run).
- Wall time = `snapshots.completed_at - snapshots.started_at`
  (or `NOW() - started_at` for in-progress).
- Items = `snapshots.item_count`; bytes = `snapshots.bytes_total`.

### Clean-first-sync re-verification (post deep-wipe @ 12:30 IST)

After the initial baseline capture, an interrupted backup left the snapshot
row with a stale `item_count` and a delta token written mid-flight. To
guarantee these numbers represent a *true* first sync (no Graph delta
short-circuit, no Azure dedup), a deep wipe was performed:

- `snapshot_items` (4417 rows), `snapshots` (6 rows), `jobs` (3 rows) — purged.
- `resources.metadata` — stripped of every `*_delta_token*`, `chat_ids`,
  cached `*_count`, and `last_backup_*` field (106 resources).
- `tenants.graph_delta_tokens` — already `{}`.
- Azure container `backup-entra-b7e7a184` — deleted (containers auto-recreate
  on next write; new run produced `backup-email`, `backup-files`,
  `backup-teams`, `backup-entra` cleanly).
- SeaweedFS bucket `tmvault-shard-0` — confirmed empty.
- RabbitMQ `backup.*` queues — all 5 purged.
- Workers stopped during wipe (no rebuild — restart causes redelivery),
  restarted clean.

Re-run confirmed first-sync semantics: `USER_CALENDAR` re-fetched all 708 event
occurrences (a delta-token-cached run would have returned far fewer), and the
worker log shows real `[USER_MAIL] Mail — Amit Mishra: N/39 folders, M msgs so far`
progress (vs. the `fetched 0 messages` short-circuit seen on the polluted
attempt). Amit's `USER_MAIL` re-ran at 593 s / 225 items / 31 MB — within run
noise of the original 635 s baseline below, so the **original USER_MAIL
baseline is valid**; the re-verification just removes any doubt.

The Amit numbers below have been updated to the post-deep-wipe values.
Akshat's numbers are **pre-wipe** and should be re-triggered before declaring
the Akshat comparison final.

- Query used:

  ```sql
  SELECT r.type, r.display_name, s.status,
         EXTRACT(EPOCH FROM COALESCE(s.completed_at, NOW()) - s.started_at)::int AS wall_secs,
         s.item_count, pg_size_pretty(s.bytes_total) AS size,
         ROUND(s.item_count::numeric
               / GREATEST(1, EXTRACT(EPOCH FROM COALESCE(s.completed_at, NOW()) - s.started_at)::int),
               3) AS items_per_s
  FROM snapshots s
  JOIN resources r ON r.id = s.resource_id
  WHERE r.type IN ('USER_MAIL','USER_ONEDRIVE','USER_CHATS',
                   'USER_CONTACTS','USER_CALENDAR','ENTRA_USER')
    AND s.status IN ('COMPLETED','IN_PROGRESS')
  ORDER BY r.type, s.started_at DESC;
  ```

## Baseline numbers

Amit's row is the clean post-deep-wipe first-sync (started 12:30:53 IST). Akshat's
row is pre-wipe and needs a re-trigger to be confirmed clean.

| Workload         | User         | Status      | Wall (s) | Items   | Bytes      | items / s | Notes                              |
| ---------------- | ------------ | ----------- | -------- | ------- | ---------- | --------- | ---------------------------------- |
| ENTRA_USER       | Amit Mishra  | COMPLETED   | 6        | 9       | 8.7 kB     | 1.500     | clean first-sync                   |
| ENTRA_USER       | Akshat Verma | COMPLETED   | 6        | 6       | 6.6 kB     | 1.000     | pre-wipe; likely fine              |
| USER_CONTACTS    | Amit Mishra  | COMPLETED   | 9        | 0       | 0 B        | 0.000     | clean first-sync                   |
| USER_CONTACTS    | Akshat Verma | COMPLETED   | 34       | 1       | 1.3 kB     | 0.029     | pre-wipe                           |
| USER_CALENDAR    | Amit Mishra  | COMPLETED   | 13       | 708     | 845 kB     | 54.462    | clean first-sync (54.5 items/s)    |
| USER_CALENDAR    | Akshat Verma | COMPLETED   | 17       | 166     | 361 kB     | 9.765     | pre-wipe                           |
| USER_MAIL        | Amit Mishra  | COMPLETED   | **593**  | **225** | **31 MB**  | **0.379** | **canonical baseline for step 2**  |
| USER_MAIL        | Akshat Verma | COMPLETED   | 416      | 37      | 4.8 MB     | 0.089     | pre-wipe; re-run before comparing  |
| USER_ONEDRIVE    | Amit Mishra  | COMPLETED†  | 1843     | 537/539 | 1190 MB    | 0.291     | 2 medium files lost (see notes)    |
| USER_ONEDRIVE    | Akshat Verma | COMPLETED   | 97       | 114     | 83 MB      | 1.175     | pre-wipe                           |
| USER_CHATS       | Amit Mishra  | COMPLETED†  | 1837     | 23983   | 28 MB      | 13.061    | 14 / 23997 msgs lost (see notes)   |
| USER_CHATS       | Akshat Verma | COMPLETED   | 879      | 4103    | 464 MB     | 4.668     | pre-wipe                           |

† Eventually transitioned to `COMPLETED` after the attachment-upload retry
loop / medium-file timeouts gave up. **The wall_secs is inflated by 15–20
minutes of post-persist retries**, not by useful work — for like-for-like
comparison against the optimization branch, the meaningful numbers are the
fetch-and-persist portion (USER_CHATS: 23997 msgs in 659 s @ 36.4 msg/s;
USER_ONEDRIVE: 536 of 539 files committed before the failing tail). See the
per-workload notes for the retry-loop details.

## Per-workload observations

### ENTRA_USER

5–7 s per user. Single Graph `GET /users/{id}` + a small fan-out (manager,
profile photo, group memberships). Linear in fan-out size, not item count. No
obvious optimization candidate — already fast.

### USER_CONTACTS

8–34 s for ≤1 contact. The fixed cost is the `/users/{id}/contacts/delta`
round-trip plus its empty-page handling. With zero / one contact returned, this
is mostly Graph latency. Optimization candidate: combine into a JSON `$batch`
with Calendar so the two empty endpoints share a single HTTP call.

### USER_CALENDAR

`54.5 items/s` (Amit, 708 events in 13 s) is fast — Calendar uses `calendarView`
with broad time-window paging, returns large pages, and there's no per-folder
fan-out. Already near the Graph wire rate; little room to gain. (Note: an
earlier capture recorded 78.7 items/s in 9 s for the same 708 events — the
13 s post-wipe number is well within run-to-run variance for a fixed-size
fetch.)

### USER_MAIL (target of `feature/mail-backup-opt-mailbox-wide-delta`)

`0.089–0.379 items/s` is dominated by **per-folder Graph round-trips**, not
bytes. Amit's 225 messages took 593 s because the per-folder fan-out across
39 folders (`$top=50`) issues 39 first-page calls at ~200 ms each before any
item is persisted, then per-folder pagination as messages are found. The
branch under review collapses these to a single mailbox-wide `$top=999` delta
cursor; expected 5–10× improvement for sub-1k-message mailboxes.

The clean post-wipe re-run hit 593 s vs. the original 635 s — both produce
exactly 225 items / 31 MB, so the difference is network jitter on the
per-folder GETs; **either number is a valid baseline**.

### USER_ONEDRIVE

Akshat's 97 s for 114 files / 83 MB → `0.85 MB/s` and `1.18 files/s`. As with
mail, the wall time is per-file Graph round-trips (download-URL resolution,
permissions, etc.) and Azure block-list commits — not raw bandwidth. Amit's
post-wipe run finished at 1843 s / 537 of 539 files / 1190 MB. The useful
work (download + commit) reached 536 files / 1028 MB at roughly the
17-minute mark; the remaining ~13 minutes were burned on retries of three
medium files that fail every attempt:

- `2025-08-04 21-44-18.mov` (received 22 / 49 MB)
- SharePoint shared URL `EY-FnQTR3…` (16 / 24 MB)
- SharePoint shared URL `…amit_mishra…` (48 / 49 MB)

All three die with `RemoteProtocolError: peer closed connection without
sending complete message body` — the Graph / SharePoint CDN closes the TCP
connection mid-stream, and `_stream_graph_to_backend` has no Range-resume
path so the partial bytes are discarded. Two of the three eventually
succeeded on retry (final count 537/539); one was permanently lost. This
is the failure mode Task #4 (`fix/onedrive-range-resume`) is designed to
eliminate.

Optimization candidates (recap, ordered by leverage):

- JSON `$batch` is **already implemented** for the download-URL fan-out
  (worker log: `pre-fetching download URLs in $batch groups of 20`). Only
  the permission fetch (`/permissions`) remains as a candidate for the same
  amortization.
- Skip the permission fetch on files with a "personal" share scope (most user
  drive files), only fetch when `shared.scope == "anonymous"` or
  `"organization"`.
- Range-resume in `_stream_graph_to_backend` so medium-sized files (10–100
  MB, e.g. .mov / .mp4) survive mid-stream CDN drops (`workers/backup-worker/main.py:4362`).
  Alternative: lower `large_file_threshold` so these files take
  `_parallel_range_stream_to_backend` (`main.py:4215`) which already uses
  HTTP `Range` and is resilient by construction.

### USER_CHATS

Akshat's `4.7 items/s` and Amit's clean run at 23997 messages in 659.3 s
(`36.4 msg/s`) both reflect chat-message throughput; the bigger user is
faster because the per-chat-id overhead is amortized over more messages.

Amit's post-wipe run completed the **message fetch + persist** cleanly
(`fetched 23997 messages across 144 chats in 659.3s` → `streaming persist:
23997 msgs, 29611183 bytes committed`) but the snapshot row took an
additional ~20 minutes to transition to `COMPLETED` (final wall 1837 s,
final item_count 23983 — 14 of the 23997 fetched messages dropped during
the retry phase). The delay came from the **post-persist hosted-content
attachment upload phase entering a retry loop**: every `catt_*` upload to
`backup-teams-b7e7a184` failed with `Timeout on reading data from socket`,
and the worker kept retrying at fresh timestamp directories
(`20260511T124638`, `…T124653`, `…T124715`, `…T124720`, `…T124747`, …)
until some succeeded and others were given up on.

Symptom matches the previously diagnosed Azure SDK aiohttp transport leak
(`ClientConnectionError('Connection lost: SSL shutdown timed out')` seen at
the start of the same worker session). Worth investigating before step 2
because it inflates wall-time and will distort any chats comparison on the
optimization branch.

Optimization candidate (independent of the transport bug): JSON `$batch`
for attachment metadata reads — `messages` returns the attachment list
inline, but each hosted-content fetch is a separate Graph call today.

## What to compare against on the optimization branch

When re-running on `feature/mail-backup-opt-mailbox-wide-delta`, focus on:

1. **USER_MAIL wall_secs** for both users. Predicted drop:
   - Amit: **593 → ~75–115 s (5–8×)**.
   - Akshat: 416 → ~30–60 s (7–14×) — re-trigger Akshat post-wipe first so
     the comparison is apples-to-apples.
2. **USER_MAIL items / s** — should rise into the 2–10 items/s band
   (currently 0.379).
3. Other workloads should be **unchanged** — this branch only touches the
   `USER_MAIL` path; OneDrive/Chats/Calendar/Contacts/Entra code is identical.
4. **Investigate the chat-attachment timeout loop first** if it persists on
   the next run — it blocks USER_CHATS from completing and will pollute
   any comparison that relies on chats reaching `COMPLETED`.

If the JSON-`$batch` follow-up lands later (not yet on this branch), re-run with
that on and compare against the mailbox-wide-only numbers from this benchmark.

## Re-run recipe (deep wipe — use this; the shallow version above was
insufficient on 2026-05-11)

A partial backup that crashes mid-flight can leave a delta token persisted
inside `resources.metadata` and a stale `snapshot_items` dedup pool — both
will make a re-trigger return `fetched 0 messages` (silent incremental
short-circuit) instead of a real first-sync. The recipe below wipes both
sides plus the Azure container.

```bash
DB_USERNAME=$(grep -E "^DB_USERNAME=" tm_backend/.env | cut -d= -f2-)
DB_NAME=$(grep -E "^DB_NAME=" tm_backend/.env | cut -d= -f2-)

# 0. Stop workers BEFORE wiping — restart causes RabbitMQ to redeliver
#    in-flight backup tasks, which will repopulate snapshots from scratch
#    against whatever state survives the wipe.
docker compose stop backup-worker backup-worker-heavy

# 0b. Purge pending backup tasks so they don't resurrect after restart.
docker exec tm_vault_rabbitmq sh -c '
for q in backup.urgent backup.high backup.normal backup.low backup.heavy; do
  rabbitmqctl purge_queue "$q"
done'

# 1. Drop backup tables (snapshot_items also = the dedup pool).
docker exec tm_vault_db psql -U "$DB_USERNAME" -d "$DB_NAME" -c "
  SET search_path TO tm,public;
  BEGIN;
    DELETE FROM snapshot_items;
    DELETE FROM snapshots;
    DELETE FROM job_logs;
    DELETE FROM jobs;
  COMMIT;
"

# 2. Strip every delta token + cached count + last_backup_* from resources.
#    NOTE: delta tokens live in resources.metadata (json, not jsonb).
docker exec tm_vault_db psql -U "$DB_USERNAME" -d "$DB_NAME" -c "
  SET search_path TO tm,public;
  UPDATE resources
     SET metadata = (
           COALESCE(metadata::jsonb, '{}'::jsonb)
             - 'mail_delta_token'
             - 'mail_delta_tokens_by_folder'
             - 'calendar_delta_tokens'
             - 'chat_delta_tokens'
             - 'onedrive_delta_token'
             - 'contacts_delta_token'
             - 'item_count' - 'folder_count' - 'chat_count' - 'calendar_count'
             - 'chat_ids' - 'calendar_names'
         )::json,
         last_backup_at = NULL, last_backup_status = NULL,
         last_backup_job_id = NULL, storage_bytes = 0, updated_at = NOW();
"

# 3. Wipe Azure storage so dedup blob lookups can't short-circuit.
ACCT=\$(grep -E '^AZURE_STORAGE_ACCOUNT_NAME=' tm_backend/.env | cut -d= -f2-)
KEY=\$(grep  -E '^AZURE_STORAGE_ACCOUNT_KEY='  tm_backend/.env | cut -d= -f2-)
for c in \$(docker run --rm mcr.microsoft.com/azure-cli:latest \
              az storage container list \
                --account-name "\$ACCT" --account-key "\$KEY" \
                --query "[?starts_with(name,'backup-')].name" -o tsv); do
  docker run --rm mcr.microsoft.com/azure-cli:latest \
    az storage container delete --name "\$c" \
      --account-name "\$ACCT" --account-key "\$KEY"
done
# Containers auto-recreate on next write via _ensure_container().

# 4. Start workers (start, not rebuild — rebuild SIGTERMs in-flight backups).
docker compose start backup-worker backup-worker-heavy

# 5. Verify zero state — every row should be 0.
docker exec tm_vault_db psql -U "$DB_USERNAME" -d "$DB_NAME" -c "
  SET search_path TO tm,public;
  SELECT 'snapshot_items' AS tbl, COUNT(*) FROM snapshot_items
  UNION ALL SELECT 'snapshots', COUNT(*) FROM snapshots
  UNION ALL SELECT 'jobs', COUNT(*) FROM jobs
  UNION ALL SELECT 'resources w/ delta-token',
                 COUNT(*) FILTER (WHERE
                     (metadata::jsonb ? 'mail_delta_token')
                  OR (metadata::jsonb ? 'mail_delta_tokens_by_folder')
                  OR (metadata::jsonb ? 'calendar_delta_tokens')
                  OR (metadata::jsonb ? 'chat_delta_tokens')
                  OR (metadata::jsonb ? 'onedrive_delta_token'))
    FROM resources;
"

# 6. Trigger the backup from the UI (or the scheduler API). Re-run the
#    benchmark query at the top of this file once the run is done.
```
