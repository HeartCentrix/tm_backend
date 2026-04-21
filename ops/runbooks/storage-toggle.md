# Storage Toggle Runbook

## Prereqs

- Org-admin credentials
- Maintenance window scheduled (1–2h wall clock; actual 5–15 min downtime)
- Paging / Slack channel open
- Monitoring dashboards open:
  - `tmvault_active_backend` (should pill between values during toggle)
  - `tmvault_toggle_phase_duration_seconds`
  - `tmvault_toggle_replica_lag_seconds`

## Preflight (always green before flip)

1. Open **Settings → Storage** in the admin UI.
2. Click **Refresh preflight** (or reload the page). All of these must be ✓:
   - `target_reachable`
   - `db_replica_lag_ok`
   - `no_inflight_transition`
   - `cooldown_elapsed`
   - `secrets_accessible`
3. If any is ✗, **stop**, resolve the root cause, retry preflight. Common fixes:
   - `target_reachable` — VPN tunnel down, S3 endpoint TLS cert expired, wrong creds.
   - `db_replica_lag_ok` — lag > 10s. Wait for catch-up; investigate slow writers.
   - `no_inflight_transition` — another toggle in progress. Abort via the UI or wait.
   - `cooldown_elapsed` — last toggle was less than 30 min ago. Cool down.

## Executing a toggle

1. Click **Switch to [target]**.
2. Enter a short **reason** (min 10 chars) — e.g. `quarterly DR drill`, `onprem cutover week 1`.
3. Type `TMVAULT-ORG` in the confirmation field.
4. Submit. Watch the SSE stream — phases fire in order:
   - `started` → `drain_completed` → `db_promoted` → `dns_flipped` → `workers_restarted` → `smoke_passed` → `completed`
5. Expected wall time: 5–15 min (dominated by drain).

## If it hangs

### `draining` > 20 min

Check inflight jobs:

```sql
SELECT id, type, status, updated_at FROM tm.jobs
WHERE status IN ('RUNNING','QUEUED','PENDING')
ORDER BY updated_at DESC LIMIT 20;
```

If a worker is stuck, hard-mark it:

```sql
UPDATE tm.jobs SET status='CANCELLED'
 WHERE status IN ('RUNNING','QUEUED','PENDING')
   AND updated_at < NOW() - INTERVAL '30 min';
```

### Phase 3 (`db_promote`) fails

Auto-rollback sets `transition_state='stable'` and the event row to `failed`.
The DB may be split-brained if your promote strategy is `patroni`. Check:

```bash
patronictl -c /etc/patroni/config.yml list
```

If needed, re-promote the original primary manually and redo the toggle.

### Phase 7 (`smoke`) fails

The flip already happened — active backend is the target, but smoke revealed a
problem. Options:
- Investigate + fix root cause, then call the smoke endpoint manually.
- Flip back: start a new toggle to the original backend. This is what the
  30-min cooldown is for — in-flight problems get to cool down first.

## Rolling back

Open the UI and run a toggle in reverse. Same procedure, same 5–15 min
window.

## Disaster recovery (target side fully down)

If the DC is dark and you need to force Azure back online while the toggle
worker can't run:

```bash
# 1. Flip DNS to Azure
/opt/tmvault/ops/dns/flip.sh azure

# 2. Promote Azure replica (if it was demoted)
psql $AZURE_DSN -c "SELECT pg_promote()"

# 3. Update system_config
psql $AZURE_DSN -c "
  UPDATE tm.system_config
  SET active_backend_id = (SELECT id FROM tm.storage_backends WHERE name='azure-primary'),
      transition_state = 'stable',
      cooldown_until = NULL
  WHERE id = 1;
"

# 4. Scale Azure workers back up (if using kubectl strategy)
kubectl --context azure -n tmvault scale deploy/backup-worker --replicas=8
```

**RPO: minutes (limited by replica lag).**
**RTO: 15–30 min.**

## Audit

After each toggle, the event row in `tm.storage_toggle_events` is the
complete audit trail. Export the latest for records:

```bash
psql $DSN -c "SELECT row_to_json(e) FROM tm.storage_toggle_events e
              ORDER BY started_at DESC LIMIT 1" \
  > ops/audit/toggle-$(date +%F).json
```

## Known limitations

- Cross-backend server-side copy for Microsoft Graph download URLs is not
  supported when the target is on-prem (Azure `start_copy_from_url` is an
  Azure-only feature). Workers stream through for that hop.
- Azurite doesn't implement legal-hold; WORM parity tests skip the
  legal-hold case there. Real Azure supports it.
- `storage_migration_jobs` (bulk historical migration between backends) is
  Phase 8 deferred work — not available yet.
