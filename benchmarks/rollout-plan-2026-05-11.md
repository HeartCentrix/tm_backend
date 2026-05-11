# Mail-backup optimization — rollout plan, revised 2026-05-11

This doc supersedes the earlier mailbox-wide-delta plan. The branch
`feature/mail-backup-opt-mailbox-wide-delta` was deleted because the
premise was wrong on two fronts:

1. **Microsoft Graph does not support `/users/{id}/messages/delta`** at
   the mailbox root. Delta queries on the `messages` resource are only
   defined at the `mailFolders/{id}/messages/delta` scope. The branch's
   single-cursor implementation returned HTTP 400 for every tenant.
2. Even if the endpoint had existed, the predicted 5–8× win was based
   on a faulty bottleneck analysis. For Amit's 225-message mailbox the
   per-folder first-page round-trips total ~1 s out of a 593 s run
   (folder fan-out is already parallelised — see below) — collapsing
   them to one cursor would save <0.2 % of wall time.

## What dev-am already has

The mail handler at `workers/backup-worker/main.py:1098-1410` already
implements every reasonable optimisation for sub-1k mailboxes:

- **Folder parallelism** — `asyncio.Semaphore(USER_MAIL_PARALLEL_FOLDERS=10)`
  drains 10 folders concurrently (`_drain_mail_parallel`).
- **Per-folder delta cursors** — incremental syncs hit only changed
  messages per folder (`mail_delta_tokens_by_folder`).
- **Streaming persist** — each folder's messages bulk-insert as soon as
  that folder's pages drain, so peak memory is O(biggest-folder) instead
  of O(whole-mailbox) and `snapshot.item_count` ticks up live for UI.
- **`$batch` for attachment LIST** — `_usermail_backup_attachments`
  bundles up to 20 `/messages/{id}/attachments` GETs per HTTP call via
  `shared.graph_batch.BatchClient`. For 109 attachments: ~22 s → ~1.2 s.
- **Concurrent attachment downloads** — 8-way semaphore on the actual
  byte-fetch (`USER_MAIL_ATT_CONCURRENCY=8`).

## The only mail change being landed in this round

**`$top=50 → $top=999`** in `_drain_one_folder` for both the delta path
and the fallback non-delta path (`main.py:1209,1217`). 999 is Graph's
maximum for messages.

| Mailbox scale | Effect of the bump |
| ------------- | ------------------ |
| <1k msgs      | No-op — every folder fits in one page either way. |
| 10k msgs      | ~20 s saved on the biggest folder (10 pages → 1 page). |
| 100k msgs     | ~4 min saved on the largest folder (1200 pages → 60). |
| 1M msgs       | ~40 min saved. |

This is a true config tuning — no new functionality, no behaviour change
for small mailboxes, strictly faster for larger ones. Landing on
`dev-am` directly per the no-new-feature rule.

## What's deliberately NOT being done

- **Mailbox-wide single cursor** — Graph doesn't support it; even if it
  did, the saving is in the noise.
- **Per-folder `$batch` for the initial fetch** — would save ~1 s of
  parallel-but-not-batched first-page calls at Amit's scale. With
  `USER_MAIL_PARALLEL_FOLDERS=10` already in place, the marginal win is
  too small for the complexity (would need to special-case the deltaLink
  resume across batch boundaries).
- **JSON `$batch` for attachment metadata** — already implemented.
- **Concurrent per-message pipeline** — `_drain_one_folder` already
  bulk-inserts per folder; per-message Azure uploads are batched in
  `_usermail_backup_attachments` with 8-way concurrency. Going wider
  risks Graph 429s without a measured headroom signal.

## Next: OneDrive medium-file resilience

The post-mail focus is the OneDrive failure mode observed in the
2026-05-11 baseline run:

```
[USER_ONEDRIVE] blob upload failed for 2025-08-04 21-44-18.mov:
peer closed connection without sending complete message body
(received 22949888 bytes, expected 49614452)
```

Same symptom on two SharePoint shared URLs (~16 MB and ~48 MB
respectively). Three files in the 20–50 MB band died mid-stream when
the Graph / SharePoint CDN closed the TCP connection. Two of three
eventually succeeded on retry; one was permanently lost.

`_stream_graph_to_backend` (`main.py:4362`) currently has no
Range-resume — partial bytes are discarded on `RemoteProtocolError`.
The fix has two viable shapes:

- **(a)** Lower `large_file_threshold` so files in the 10–100 MB band
  take the existing `_parallel_range_stream_to_backend`
  (`main.py:4215`), which uses HTTP `Range` headers and is resilient
  by construction. Smallest change.
- **(b)** Add mid-stream Range-resume to `_stream_graph_to_backend`
  itself: track bytes received, on `httpx.RemoteProtocolError` reissue
  the GET with `Range: bytes=N-` and continue from offset N. Hash
  inline against the upper-level sha256 accumulator.

Implementing on `dev-am` next.
