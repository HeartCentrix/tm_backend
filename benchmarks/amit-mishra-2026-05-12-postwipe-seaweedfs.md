# Amit Mishra — first-sync benchmark (post-wipe, SeaweedFS)

Single-user run after the full DB + Azure + SeaweedFS wipe at ~00:13 IST
on 2026-05-12. Active backend: **seaweedfs-local** (`http://seaweedfs:8333`,
`transition_state=stable`). All Stage 1+2+3 fixes active:

- Per-chat `chat_delta_tokens` save
- Per-folder `mail_delta_tokens_by_folder` save
- OneDrive `BackupCheckpoint` cadence 50 files / 256 MiB
- Range-resume on `fetch_shared_url_content` (6 attempts)
- `CHAT_HC_MSG_CONCURRENCY=16` cap
- Inline-attachment threshold 1 MiB
- asyncio handler suppresses Azure-aiohttp orphan futures
- Upload transport-reset retry with client recycle
- Supervisor wrapping `consume_queue` so transport cascades don't kill the worker

## Per-resource wall time

| Resource | Status | Started | Ended | Wall (s) | Items | Bytes |
|---|---|---|---|---:|---:|---|
| ENTRA_USER | COMPLETED | 18:53:15 | 18:53:22 | **8** | 9 | 8.5 KB |
| USER_CONTACTS | COMPLETED | 18:53:15 | 18:53:28 | **13** | 0 | 0 |
| USER_CALENDAR | COMPLETED | 18:53:15 | 18:53:28 | **13** | 708 | 845 KB |
| USER_MAIL | COMPLETED | 18:53:15 | 19:03:16 | **601** | 225 | 31 MB |
| USER_ONEDRIVE | COMPLETED | 18:53:15 | 19:15:42 | **1,347** | 540 | 1,481 MB |
| USER_CHATS | COMPLETED | 18:53:23 | 19:16:21 | **1,378** | 8,727 | 1,746 MB |

## Totals

| Metric | Value |
|---|---:|
| First snapshot started | 18:53:15 |
| Last snapshot completed | 19:16:21 |
| **Total wall time (max-of-parallel)** | **1,386 s (23 min 6 s)** |
| Total items captured | 10,209 |
| Total bytes captured | 3,259 MB |

## Notes

- All 6 resource snapshots COMPLETED — zero PARTIAL/FAILED.
- Wall is dominated by USER_CHATS (1378 s) and USER_ONEDRIVE (1347 s)
  running in parallel; lightweight resources (Entra/Calendar/Contacts)
  finish in <15 s and would have been immediately browsable if the UI
  read snapshot-level status.
- Backend: SeaweedFS only — the `nsescanner4e104b` Azure account had
  the 4 backup containers deleted in the same wipe and was not written
  to during this run.
- No worker cycles, no SSL cascades observed in the logs for this run.
- Compared to the 2026-05-11 Run B (Azure backend): comparable wall
  (~22-34 min depending on chat thread count) but cleaner — chats
  here captured 1.7 GB vs ~10 MB on the prior run because the post-
  wipe fresh deltas re-fetched all attachment bytes.
