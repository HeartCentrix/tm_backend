-- ====================================================================
-- Railway WIPE — pre-partition validation rerun (2026-05-14)
-- ====================================================================
--
-- Drops + recreates the tm_vault schema so the next backup runs against
-- a clean DB. This is Railway DEMO only (not Taylor Morrison prod).
--
-- Run via:
--   railway connect Postgres < scripts/2026_05_14_wipe_railway_pre_partition_validation.sql
--
-- Or interactively:
--   railway connect Postgres
--   \i scripts/2026_05_14_wipe_railway_pre_partition_validation.sql
--
-- Pairs with:
--   1. RabbitMQ queue purge (separate management UI / CLI)
--   2. SeaweedFS volume wipe (user does separately)
--   3. Service redeploy after wipe (Railway auto-redeploys on push;
--      manual `railway redeploy --service <name>` if needed)
--
-- After wipe runs:
--   * tm_vault schema is empty (no tenants/users/snapshots/items)
--   * `add_column_statements` startup in shared/database.py recreates
--     all tables on first service connect.
-- ====================================================================

\set ON_ERROR_STOP on
\timing on

\echo '── BEFORE wipe — row counts (sanity) ──'
SELECT 'snapshots' AS table, COUNT(*) FROM tm_vault.snapshots
UNION ALL SELECT 'snapshot_items', COUNT(*) FROM tm_vault.snapshot_items
UNION ALL SELECT 'snapshot_partitions', COUNT(*) FROM tm_vault.snapshot_partitions
UNION ALL SELECT 'jobs', COUNT(*) FROM tm_vault.jobs
UNION ALL SELECT 'resources', COUNT(*) FROM tm_vault.resources
UNION ALL SELECT 'chat_threads', COUNT(*) FROM tm_vault.chat_threads
UNION ALL SELECT 'chat_thread_messages', COUNT(*) FROM tm_vault.chat_thread_messages
UNION ALL SELECT 'chat_url_cache', COUNT(*) FROM tm_vault.chat_url_cache
UNION ALL SELECT 'audit_events', COUNT(*) FROM tm_vault.audit_events;

\echo '── DROPPING tm_vault schema ──'
DROP SCHEMA tm_vault CASCADE;

\echo '── RECREATING empty tm_vault schema ──'
CREATE SCHEMA tm_vault;
GRANT ALL ON SCHEMA tm_vault TO postgres;

\echo '── AFTER wipe — schema confirmation ──'
SELECT schema_name FROM information_schema.schemata
WHERE schema_name = 'tm_vault';

\echo '── tables left in tm_vault (should be empty list) ──'
SELECT tablename FROM pg_tables WHERE schemaname = 'tm_vault';

\echo '── DONE. Tables will be recreated on next service startup. ──'
