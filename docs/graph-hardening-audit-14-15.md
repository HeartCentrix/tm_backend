# Graph Hardening — Task 14 & 15 Audit (2026-04-19)

Plan tasks 14 (SharePoint metadata batching) and 15 (discovery-worker hardening)
scan result after Tasks 11-13 shipped:

## Task 14 — SharePoint site metadata batching
`workers/backup-worker/main.py::backup_sharepoint` and
`_backup_files_parallel` already route all Graph reads through
`graph_client.get_sharepoint_site_drives`, `get_sharepoint_subsites`,
and `get_drive_items_delta` — all of which call `GraphClient._get` /
`_iter_pages` internally. These inherit the hardened policy (pacing +
sticky rotation + 429 + cumulative cap) automatically once
`GRAPH_HARDENING_ENABLED=true`. No sequential per-site `/sites/{id}`
metadata loop exists to batch. No code change required.

## Task 15 — discovery-worker
Scan for "graph.microsoft.com" in workers/discovery-worker/ returns
only a reference inside an audit-service POST URL — zero raw-httpx
calls to Graph. Every Graph read goes through GraphClient._get /
_iter_pages already. No code change required.

## Out-of-scope services (still use raw httpx but not for backup fan-out)
- services/auth-service/main.py — one-shot /me, /organization on login
- services/tenant-service/azure_provisioning.py — per-tenant setup
- services/graph-proxy/main.py — dedicated proxy service with its own
  token loop

These are low-volume control-plane calls, not backup fan-outs. They
don't share the tenant's backup app buckets and aren't the source of
throttle at 3-5 k user scale.
