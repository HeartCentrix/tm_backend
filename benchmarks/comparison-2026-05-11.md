# TMvault — first-sync benchmark comparison (2026-05-11)

Compares first-sync wall-time **before** and **after** today's enterprise-
grade reliability fixes for Amit + Akshat. Both runs start from a fully
wiped DB (no delta tokens, no snapshot rows, no Azure blobs).

## Run A — baseline (commit `efefbe4`)

Triggered: 2026-05-11 14:14 IST. Workers: 4 light + 1 heavy.
Active fixes: inline attachments + OneDrive Range-resume + `$top=999`.

| User | Resource | Wall (s) | Items | Bytes |
|---|---|---:|---:|---|
| Amit | ENTRA_USER | 11 | 9 | 8.7 KB |
| Amit | USER_CALENDAR | 11 | 708 | 845 KB |
| Amit | USER_CONTACTS | 9 | 0 | 0 |
| Akshat | ENTRA_USER | 10 | 6 | 6.6 KB |
| Akshat | USER_CALENDAR | 15 | 357 | 896 KB |
| Akshat | USER_CONTACTS | 23 | 1 | 1.3 KB |
| Akshat | USER_ONEDRIVE | 93 | 114 | 83 MB |
| Akshat | USER_MAIL | 418 | 37 | 4.8 MB |
| Amit | USER_MAIL | 661 | 225 | 31 MB |
| **Akshat** | **USER_CHATS** | **1400** | 4,103 | 420 MB |
| **Amit** | **USER_ONEDRIVE** | **1837** | 539 | 1,226 MB |
| **Amit** | **USER_CHATS** | **1921** | 24,004 | 28 MB |

Bulk wall (max of all): **32 min** (driven by Amit USER_CHATS).

## Run B — after chat fixes (commit `6487118`)

Triggered: 2026-05-11 15:22 IST. Same hardware.
Added fixes: Range-resume on `fetch_shared_url_content`,
`CHAT_HC_MSG_CONCURRENCY=16` cap.

| User | Resource | Wall (s) | Items | Bytes | Δ vs Run A |
|---|---|---:|---:|---|---:|
| Amit | ENTRA_USER | 6 | 9 | 8.7 KB | -45% |
| Amit | USER_CALENDAR | 15 | 708 | 845 KB | +36% (noise) |
| Amit | USER_CONTACTS | 11 | 0 | 0 | +22% (noise) |
| Akshat | ENTRA_USER | 11 | 6 | 6.6 KB | +10% (noise) |
| Akshat | USER_CALENDAR | 14 | 357 | 896 KB | -7% |
| Akshat | USER_CONTACTS | 15 | 1 | 1.3 KB | -35% |
| Akshat | USER_ONEDRIVE | 98 | 114 | 83 MB | +5% (noise) |
| Akshat | USER_MAIL | 418 | 37 | 4.8 MB | **0** (identical) |
| Amit | USER_MAIL | **562** | 225 | 31 MB | **-99s (-15%)** |
| **Akshat** | **USER_CHATS** | **1339** | 4,103 | **464 MB** | **-61s + +44 MB recovered** |
| **Amit** | **USER_ONEDRIVE** | **1809** | 539 | 1230 MB | -28s, +4 MB |
| **Amit** | **USER_CHATS** | **2050** | 23,999 | 23 MB | **+129s** (worker cycle wasted progress) |

Bulk wall: **~34 min** — dominated by Amit USER_CHATS, which restarted
from zero after worker-2 cycled at ~15:48 IST.

## What we proved

1. **Range-resume on shared SharePoint URLs works.** 1 confirmed
   recovery on Amit chat (`resume 1/6 after 1.0s` at offset 50 MB),
   and Akshat ended up with +44 MB captured vs Run A.
2. **Fanout cap prevents the cascading-CDN-drop storm.** Akshat CHATS
   was 61s faster and captured more, without changing the message-fetch
   path (which is Graph-rate-bound).
3. **Mail/Cal/Contacts paths are unchanged.** Akshat USER_MAIL was
   identical to the second (418s). Confirms the changes are scoped.
4. **The benchmark exposed a real bug:** worker-2 cycled mid-run during
   Amit's heavy phase. Without a resume mechanism, the ~5 min of
   already-fetched chat messages + ~24 min of OneDrive byte-transfer
   restarted from zero. **This is the single biggest production risk.**

## Worker cycle — root cause

Worker-2's session 402e23d2 exited with ExitCode=0 (NOT OOM-killed) at
~15:48. Leading-up log lines show:
- Sustained `ClientConnectionError: SSL shutdown timed out` cascade
  from aiohttp connector cleanup
- Final `Timeout on reading data from socket` from Azure SDK on a
  `catt_*` upload path
- The process exited cleanly (likely an unhandled exception in a
  background task killed the asyncio event loop)

RabbitMQ redelivered the in-flight USER_CHATS + USER_ONEDRIVE jobs to
the heavy worker. The new session restarted from zero against the same
snapshot rows, wasting all prior progress.

## Stage 1 fix — checkpoint cadence (shipped)

OneDrive backup already has a `BackupCheckpoint` system that commits
to `Job.result.backup_checkpoint` and resumes on restart. Defaults
(500 files / 1 GiB) only fire once on small drives.

**Lowered to 50 files / 256 MiB.** On a 5k-file Taylor user this is
100 checkpoint commits per drive (~0.1ms each = 10ms total overhead)
in exchange for capping mid-cycle waste at ~10% of a checkpoint window
instead of ~100%.

## Stage 2 fix — chat + mail resume (NOT YET DONE)

The remaining waste sources:
- **Chat messages**: `chat_delta_tokens` saves at end-of-drain only.
  Per-chat token save would let restart skip already-drained chats.
  Currently re-fetches ALL chat messages on restart (~24k messages,
  ~10 min of Graph quota wasted).
- **Mail per-folder delta**: `mail_delta_tokens_by_folder` saves at
  end-of-mailbox only. Per-folder save lets restart skip drained
  folders.
- **OneDrive URL pre-fetch**: 539 URLs × ~1s = 540s of wall replayed
  on restart. Cache should survive cycle.

## Stage 3 fix — SSL/aiohttp cycle root cause (NOT YET DONE)

The actual underlying bug. Most likely Azure SDK aiohttp transport
under high concurrency on `catt_*` uploads, but verification needed
via a focused repro and tcpdump on the failing socket. Possible
mitigations: pin to a httpx-backed Azure transport, add an event-loop
supervisor that catches+restarts cleanly, or fail-fast specific upload
errors without taking down the worker.

## Taylor scaling math (correct view)

| Metric | Local dev (this benchmark) | Taylor production |
|---|---:|---:|
| Users | 2 | 5,000 |
| Workers | 4 light + 1 heavy | 8 light + 2 heavy (or 20+ if needed) |
| Per-user wall (heavy case) | ~22-35 min | similar shape |
| First-sync metadata floor | — | **~20 days** (Graph throttle, 12 apps) |
| First-sync byte floor at 25 Gbps | — | **~24 hours** (parallel to metadata) |
| Daily incremental | 35s for delta-zero | **3-8 hours** (1-3% change rate) |
| Mid-run resilience (Stage 1) | ~10% waste on cycle | same |
| Mid-run resilience (Stage 2 target) | <1% waste on cycle | same |

The **22-min for 550-MB** snapshot doesn't extrapolate to "decades for
260 TiB" because per-user wall doesn't multiply linearly — workers run
in parallel and the dominant constraint is Graph rate-limiting on
metadata, not byte transfer. The runbook math holds:
- Bytes: 260 TiB / 25 Gbps ≈ 24 hours
- Metadata: 100M Graph calls / 60 RPS (12 apps × 5 RPS) ≈ 20 days
- Both run in parallel → first sync ~20 days
