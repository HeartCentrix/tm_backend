# DB migration commands

Engineer-facing runbook for ad-hoc Postgres migrations in this repo.
Each section documents one migration file under `migrations/` in reverse
chronological order.

## 2026-04-20 — Chat export indexes
Run during low-traffic window. All indexes use `CONCURRENTLY` so no table lock.

    psql "$DATABASE_URL" -f migrations/2026_04_20_chat_export_indexes.sql

Verify:
    psql "$DATABASE_URL" -c "\di+ idx_snapshot_items_resource_type_folder"
