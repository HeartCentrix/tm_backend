# Graph API Throttle Hardening — Rollout

**Spec:** `docs/superpowers/specs/2026-04-19-graph-api-throttle-hardening-design.md`
**Plan:** `docs/superpowers/plans/2026-04-19-graph-api-throttle-hardening.md`
**Rollback tag:** `v0.stable-pre-graph-hardening`

## Stages

1. **Staging bake (48 h).** Set `GRAPH_HARDENING_ENABLED=true` in
   staging. Run one nightly incremental + one synthetic soak. Check
   metrics: `graph_429_total` should be near zero; no
   `GraphRetryExhaustedError` in logs.
2. **Canary tenant (prod).** Flip flag on for one internal tenant.
   Watch `graph_429_total` + `graph_cumulative_wait_seconds` for 48 h.
3. **Full prod rollout.** Flip globally.
4. **Cleanup (after 2 weeks stable).** Delete `GRAPH_HARDENING_ENABLED`
   gate + `_get_legacy` / `_iter_pages_legacy` paths. File a follow-up
   task.

## Kill switch

If anything regresses, the flag reverts to legacy without a redeploy:

```bash
# Edit tm_backend/.env — set GRAPH_HARDENING_ENABLED=false
docker compose restart backup-worker backup-worker-heavy discovery-worker
```

Or, even faster, set pacing rates to 0 which disables the token buckets
without dropping the hardened code path:

```bash
GRAPH_STREAM_PACE_REQS_PER_SEC=0
GRAPH_APP_PACE_REQS_PER_SEC=0
```

Full rollback to the tagged stable state:

```bash
git -C tm_backend checkout v0.stable-pre-graph-hardening
git -C tm_vault   checkout v0.stable-pre-graph-hardening
docker compose up -d --build
```

## Tuning knobs

All knobs are listed in `.env.example` under the "Graph API throttle
hardening" block. Recommended starting values are in the spec §7.

Target production configuration for 3–5 k users / 400 TiB:

```
A = 16 app registrations (APP_1_* through APP_16_*)
W = 16 backup-worker replicas
C = 2  (MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER)

GRAPH_APP_PACE_REQS_PER_SEC=2.5      # aggregate load ≤ 40 rps
GRAPH_STREAM_PACE_REQS_PER_SEC=1.0   # 32 streams × 1 = 32 rps sustained
```

Expected wall-clock:

| Phase                     | 3 000 users at 32 rps sustained |
|---------------------------|---------------------------------|
| Initial full backup       | ~39 h (weekend seed)            |
| Daily incremental         | ~23 min                         |

## Observability

- Per-stream log line every ~100 pages:
  `[GraphClient/hardened iter] migrating 429 -> app=app-X`
- Per-app counters via `multi_app_manager.get_stats()` (request count,
  throttled_until).
- Existing log lines (`[USER_MAIL]`, `[USER_CHATS]`, `[USER_ONEDRIVE]`)
  still emit their `fetched N messages` summaries.

## Acceptance criteria (re-check before declaring the rollout done)

- `grep -rn "graph.microsoft.com" tm_backend/workers tm_backend/shared | grep -v GraphClient` returns zero lines.
- `pytest tests/shared/test_graph_ratelimit.py tests/shared/test_graph_batch.py tests/shared/test_multi_app_manager_hardened.py` → all pass.
- Integration tests `tests/integration/test_graph_*.py` pass against the fake-Graph harness.
- Zero failed-snapshot events attributable to throttle in the 14-day canary window.
