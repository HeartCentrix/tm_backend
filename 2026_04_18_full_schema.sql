--
-- TM Vault — full schema migration
-- =================================
--
-- Generated from the current local Postgres snapshot with CREATE statements
-- wrapped in IF-NOT-EXISTS / DO-EXCEPTION blocks so this file is SAFE to run
-- against:
--   • a fresh empty database (creates everything)
--   • a database that already has part or all of the schema (no-op where
--     objects exist, creates missing tables/types/constraints/indexes).
--
-- Matches the ORM models in shared/models.py plus the one extra table
-- (access_groups) and one extra column (snapshot_items.search_vector) that
-- the production Railway DB carries.
--
-- How to apply
-- ------------
--   psql "postgresql://user:pwd@host:5432/dbname" -v ON_ERROR_STOP=1 \
--        -f 2026_04_18_full_schema.sql
--
--   Or inside a container:
--     docker exec -i <pg_container> \
--       psql -U postgres -d <dbname> -v ON_ERROR_STOP=1 < 2026_04_18_full_schema.sql
--
-- After applying, just start the backend services — their auto-init on
-- startup is a no-op when every object already exists.
--
-- Safety: runs in a single implicit transaction via psql, so a failure
-- mid-way rolls back cleanly. Re-runs are idempotent.
--

--
-- PostgreSQL database dump
--


-- Dumped from database version 16.13
-- Dumped by pg_dump version 16.13

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: tm_vault; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS tm_vault;


--
-- Name: jobstatus; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.jobstatus AS ENUM (
    'QUEUED',
    'RUNNING',
    'COMPLETED',
    'FAILED',
    'CANCELLED',
    'RETRYING'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


--
-- Name: jobtype; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.jobtype AS ENUM (
    'BACKUP',
    'RESTORE',
    'EXPORT',
    'DISCOVERY',
    'DELETE'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


--
-- Name: resourcestatus; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.resourcestatus AS ENUM (
    'DISCOVERED',
    'ACTIVE',
    'ARCHIVED',
    'SUSPENDED',
    'PENDING_DELETION',
    'INACCESSIBLE'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


--
-- Name: resourcetype; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.resourcetype AS ENUM (
    'MAILBOX',
    'SHARED_MAILBOX',
    'ROOM_MAILBOX',
    'ONEDRIVE',
    'SHAREPOINT_SITE',
    'TEAMS_CHANNEL',
    'TEAMS_CHAT',
    'ENTRA_USER',
    'ENTRA_GROUP',
    'ENTRA_APP',
    'ENTRA_SERVICE_PRINCIPAL',
    'ENTRA_DEVICE',
    'ENTRA_ROLE',
    'ENTRA_ADMIN_UNIT',
    'ENTRA_AUDIT_LOG',
    'INTUNE_MANAGED_DEVICE',
    'AZURE_VM',
    'AZURE_SQL_DB',
    'AZURE_POSTGRESQL',
    'AZURE_POSTGRESQL_SINGLE',
    'RESOURCE_GROUP',
    'DYNAMIC_GROUP',
    'POWER_BI',
    'POWER_APPS',
    'POWER_AUTOMATE',
    'POWER_DLP',
    'COPILOT',
    'PLANNER',
    'TODO',
    'ONENOTE',
    'M365_GROUP',
    'ENTRA_CONDITIONAL_ACCESS',
    'ENTRA_BITLOCKER_KEY',
    'USER_MAIL',
    'USER_ONEDRIVE',
    'USER_CONTACTS',
    'USER_CALENDAR',
    'USER_CHATS',
    'TEAMS_CHAT_EXPORT'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


--
-- Name: snapshotstatus; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.snapshotstatus AS ENUM (
    'IN_PROGRESS',
    'COMPLETED',
    'FAILED',
    'PARTIAL',
    'PENDING_DELETION'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


--
-- Name: snapshottype; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.snapshottype AS ENUM (
    'FULL',
    'INCREMENTAL',
    'PREEMPTIVE',
    'MANUAL'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


--
-- Name: tenantstatus; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.tenantstatus AS ENUM (
    'PENDING',
    'ACTIVE',
    'DISCONNECTED',
    'SUSPENDED',
    'PENDING_DELETION',
    'DISCOVERING',
    'PENDING_DISCOVERY'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


--
-- Name: tenanttype; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.tenanttype AS ENUM (
    'M365',
    'AZURE'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


--
-- Name: userrole; Type: TYPE; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
CREATE TYPE tm_vault.userrole AS ENUM (
    'SUPER_ADMIN',
    'ORG_ADMIN',
    'TENANT_ADMIN',
    'BACKUP_OPERATOR',
    'RESTORE_OPERATOR',
    'CONTENT_VIEWER',
    'USER'
);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: admin_consent_tokens; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.admin_consent_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    tenant_id uuid,
    consent_type character varying NOT NULL,
    access_token_encrypted bytea,
    refresh_token_encrypted bytea,
    token_type character varying DEFAULT 'Bearer'::character varying,
    expires_at timestamp without time zone,
    granted_by character varying,
    consented_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_used_at timestamp without time zone,
    is_active boolean DEFAULT true,
    scope character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: alerts; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.alerts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    org_id uuid,
    type character varying NOT NULL,
    severity character varying DEFAULT 'MEDIUM'::character varying,
    message text NOT NULL,
    resource_id uuid,
    resource_type character varying,
    resource_name character varying,
    triggered_by character varying,
    resolved boolean DEFAULT false,
    resolved_at timestamp without time zone,
    resolved_by uuid,
    resolution_note text,
    details json DEFAULT '{}'::json,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: audit_events; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.audit_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    tenant_id uuid,
    actor_id uuid,
    actor_email character varying,
    actor_type character varying DEFAULT 'SYSTEM'::character varying,
    action character varying NOT NULL,
    resource_id uuid,
    resource_type character varying,
    resource_name character varying,
    outcome character varying DEFAULT 'SUCCESS'::character varying,
    job_id uuid,
    snapshot_id uuid,
    details jsonb DEFAULT '{}'::jsonb,
    occurred_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: discovery_runs; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.discovery_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    scope json DEFAULT '[]'::json,
    status character varying DEFAULT 'RUNNING'::character varying,
    fetched_count integer DEFAULT 0,
    staged_count integer DEFAULT 0,
    inserted_count integer DEFAULT 0,
    updated_count integer DEFAULT 0,
    unchanged_count integer DEFAULT 0,
    stale_marked_count integer DEFAULT 0,
    error_message text,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    finished_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: group_policy_assignments; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.group_policy_assignments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    group_id uuid NOT NULL,
    policy_id uuid NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: job_logs; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.job_logs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    job_id uuid,
    "timestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    level character varying DEFAULT 'INFO'::character varying,
    message text,
    details text
);


--
-- Name: jobs; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    type tm_vault.jobtype DEFAULT 'BACKUP'::tm_vault.jobtype NOT NULL,
    tenant_id uuid,
    resource_id uuid,
    batch_resource_ids uuid[] DEFAULT '{}'::uuid[],
    snapshot_id uuid,
    status tm_vault.jobstatus DEFAULT 'QUEUED'::tm_vault.jobstatus,
    priority integer DEFAULT 5,
    attempts integer DEFAULT 0,
    max_attempts integer DEFAULT 5,
    error_message text,
    progress_pct integer DEFAULT 0,
    items_processed bigint DEFAULT 0,
    bytes_processed bigint DEFAULT 0,
    result json DEFAULT '{}'::json,
    spec json DEFAULT '{}'::json,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp without time zone
);


--
-- Name: organizations; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.organizations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name character varying NOT NULL,
    slug character varying NOT NULL,
    storage_region character varying,
    encryption_mode character varying DEFAULT 'TMVAULT_MANAGED'::character varying,
    storage_quota_bytes bigint DEFAULT '536870912000'::bigint,
    storage_bytes_used bigint DEFAULT 0,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: platform_users; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.platform_users (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    email character varying NOT NULL,
    name character varying NOT NULL,
    external_user_id character varying,
    org_id uuid,
    tenant_id uuid,
    mfa_enabled boolean DEFAULT false,
    last_login_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: report_configs; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.report_configs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    enabled boolean DEFAULT false NOT NULL,
    schedule_type character varying DEFAULT 'daily'::character varying NOT NULL,
    send_empty_report boolean DEFAULT true NOT NULL,
    empty_message character varying DEFAULT 'No updates. No backups occurred.'::character varying,
    send_detailed_report boolean DEFAULT false NOT NULL,
    email_recipients json DEFAULT '[]'::json,
    slack_webhooks json DEFAULT '[]'::json,
    teams_webhooks json DEFAULT '[]'::json,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: report_history; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.report_history (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    report_config_id uuid,
    report_type character varying NOT NULL,
    period_start timestamp without time zone,
    period_end timestamp without time zone,
    generated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    total_backups integer DEFAULT 0,
    successful_backups integer DEFAULT 0,
    failed_backups integer DEFAULT 0,
    success_rate character varying,
    coverage_rate character varying,
    report_data json DEFAULT '{}'::json,
    is_empty boolean DEFAULT false NOT NULL,
    delivery_status json DEFAULT '{}'::json,
    error_message text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: resource_discovery_staging; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.resource_discovery_staging (
    id bigint NOT NULL,
    run_id uuid,
    tenant_id uuid NOT NULL,
    resource_type character varying NOT NULL,
    external_id character varying NOT NULL,
    display_name character varying NOT NULL,
    email character varying,
    metadata jsonb DEFAULT '{}'::jsonb,
    resource_status character varying DEFAULT 'DISCOVERED'::character varying,
    resource_hash character varying,
    azure_subscription_id character varying,
    azure_resource_group character varying,
    azure_region character varying,
    discovered_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: resource_discovery_staging_id_seq; Type: SEQUENCE; Schema: tm_vault; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS tm_vault.resource_discovery_staging_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: resource_discovery_staging_id_seq; Type: SEQUENCE OWNED BY; Schema: tm_vault; Owner: -
--

ALTER SEQUENCE tm_vault.resource_discovery_staging_id_seq OWNED BY tm_vault.resource_discovery_staging.id;


--
-- Name: resource_groups; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.resource_groups (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    name character varying NOT NULL,
    description text,
    group_type character varying DEFAULT 'DYNAMIC'::character varying NOT NULL,
    rules json DEFAULT '[]'::json NOT NULL,
    combinator character varying DEFAULT 'AND'::character varying NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    auto_protect_new boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: resources; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.resources (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    type tm_vault.resourcetype DEFAULT 'ENTRA_USER'::tm_vault.resourcetype NOT NULL,
    external_id character varying NOT NULL,
    display_name character varying NOT NULL,
    email character varying,
    metadata json DEFAULT '{}'::json,
    sla_policy_id uuid,
    status tm_vault.resourcestatus DEFAULT 'DISCOVERED'::tm_vault.resourcestatus,
    last_backup_job_id uuid,
    last_backup_at timestamp without time zone,
    last_backup_status character varying,
    storage_bytes bigint DEFAULT 0,
    discovered_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    archived_at timestamp without time zone,
    deletion_queued_at timestamp without time zone,
    azure_subscription_id character varying,
    azure_resource_group character varying,
    azure_region character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    resource_hash character varying,
    parent_resource_id uuid
);


--
-- Name: sla_exclusions; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.sla_exclusions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    policy_id uuid NOT NULL,
    exclusion_type character varying NOT NULL,
    pattern character varying NOT NULL,
    workload character varying,
    apply_to_historical boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: sla_policies; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.sla_policies (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    service_type character varying DEFAULT 'm365'::character varying,
    name character varying NOT NULL,
    frequency character varying DEFAULT 'DAILY'::character varying,
    backup_days character varying[],
    backup_window_start character varying,
    backup_window_end character varying,
    resource_types character varying[],
    batch_size integer DEFAULT 1000,
    max_concurrent_backups integer DEFAULT 5,
    sla_violation_alert boolean DEFAULT true,
    retention_days integer DEFAULT 2555,
    retention_versions integer DEFAULT 10,
    backup_exchange boolean DEFAULT true,
    backup_exchange_archive boolean DEFAULT false,
    backup_exchange_recoverable boolean DEFAULT false,
    backup_onedrive boolean DEFAULT true,
    backup_sharepoint boolean DEFAULT true,
    backup_teams boolean DEFAULT true,
    backup_teams_chats boolean DEFAULT false,
    backup_entra_id boolean DEFAULT true,
    backup_power_platform boolean DEFAULT false,
    backup_copilot boolean DEFAULT false,
    contacts boolean DEFAULT true,
    calendars boolean DEFAULT true,
    tasks boolean DEFAULT false,
    group_mailbox boolean DEFAULT true,
    planner boolean DEFAULT false,
    backup_azure_vm boolean DEFAULT true,
    backup_azure_sql boolean DEFAULT true,
    backup_azure_postgresql boolean DEFAULT true,
    retention_type character varying DEFAULT 'INDEFINITE'::character varying,
    retention_hot_days integer DEFAULT 7,
    retention_cool_days integer DEFAULT 30,
    retention_archive_days integer,
    legal_hold_enabled boolean DEFAULT false,
    legal_hold_until timestamp without time zone,
    immutability_mode character varying DEFAULT 'None'::character varying,
    retention_mode character varying DEFAULT 'FLAT'::character varying NOT NULL,
    gfs_daily_count integer,
    gfs_weekly_count integer,
    gfs_monthly_count integer,
    gfs_yearly_count integer,
    item_retention_days integer,
    item_retention_basis character varying DEFAULT 'SNAPSHOT'::character varying NOT NULL,
    archived_retention_mode character varying DEFAULT 'SAME'::character varying NOT NULL,
    archived_retention_days integer,
    storage_region character varying,
    encryption_mode character varying DEFAULT 'VAULT_MANAGED'::character varying NOT NULL,
    key_vault_uri character varying,
    key_name character varying,
    key_version character varying,
    auto_apply_to_matching boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT true,
    is_default boolean DEFAULT false,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: snapshot_items; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.snapshot_items (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    snapshot_id uuid,
    tenant_id uuid,
    external_id character varying NOT NULL,
    item_type character varying NOT NULL,
    name character varying NOT NULL,
    folder_path character varying,
    content_hash character varying,
    content_checksum character varying,
    content_size bigint DEFAULT 0,
    blob_path character varying,
    encryption_key_id character varying,
    backup_version integer DEFAULT 1,
    metadata json DEFAULT '{}'::json,
    is_deleted boolean DEFAULT false,
    indexed_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: snapshots; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.snapshots (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid,
    job_id uuid,
    type tm_vault.snapshottype DEFAULT 'FULL'::tm_vault.snapshottype,
    status tm_vault.snapshotstatus DEFAULT 'IN_PROGRESS'::tm_vault.snapshotstatus,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp without time zone,
    duration_secs integer,
    item_count integer DEFAULT 0,
    new_item_count integer DEFAULT 0,
    bytes_added bigint DEFAULT 0,
    bytes_total bigint DEFAULT 0,
    delta_token character varying,
    delta_tokens_json json DEFAULT '{}'::json,
    extra_data json DEFAULT '{}'::json,
    snapshot_label character varying,
    content_checksum character varying,
    blob_path character varying,
    storage_version integer DEFAULT 1,
    azure_restore_point_id character varying,
    azure_operation_id character varying,
    dr_replication_status character varying DEFAULT 'pending'::character varying,
    dr_blob_path character varying,
    dr_replicated_at timestamp without time zone,
    dr_error text,
    dr_replication_attempts integer DEFAULT 0,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: tenants; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.tenants (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    type tm_vault.tenanttype DEFAULT 'M365'::tm_vault.tenanttype,
    display_name character varying NOT NULL,
    external_tenant_id character varying,
    customer_id character varying,
    subscription_id character varying,
    client_id character varying,
    client_secret_ref character varying,
    graph_client_id character varying,
    graph_client_secret_encrypted bytea,
    status tm_vault.tenantstatus DEFAULT 'PENDING'::tm_vault.tenantstatus,
    storage_region character varying,
    last_discovery_at timestamp without time zone,
    graph_delta_tokens json DEFAULT '{}'::json,
    extra_data json DEFAULT '{}'::json,
    dr_region_enabled boolean DEFAULT false,
    dr_region character varying,
    dr_storage_account_name character varying,
    dr_storage_account_key_encrypted bytea,
    dr_last_replicated_at timestamp without time zone,
    azure_refresh_token_encrypted bytea,
    azure_refresh_token_updated_at timestamp without time zone,
    azure_subscriptions_cached json DEFAULT '[]'::json,
    azure_sql_servers_configured json DEFAULT '[]'::json,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    azure_pg_servers_configured json DEFAULT '{}'::json
);


--
-- Name: user_roles; Type: TABLE; Schema: tm_vault; Owner: -
--

CREATE TABLE IF NOT EXISTS tm_vault.user_roles (
    user_id uuid NOT NULL,
    role tm_vault.userrole NOT NULL
);


--
-- Name: resource_discovery_staging id; Type: DEFAULT; Schema: tm_vault; Owner: -
--

ALTER TABLE ONLY tm_vault.resource_discovery_staging ALTER COLUMN id SET DEFAULT nextval('tm_vault.resource_discovery_staging_id_seq'::regclass);


--
-- Name: admin_consent_tokens admin_consent_tokens_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.admin_consent_tokens
    ADD CONSTRAINT admin_consent_tokens_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: alerts alerts_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: audit_events audit_events_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.audit_events
    ADD CONSTRAINT audit_events_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: discovery_runs discovery_runs_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.discovery_runs
    ADD CONSTRAINT discovery_runs_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: group_policy_assignments group_policy_assignments_group_id_policy_id_key; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.group_policy_assignments
    ADD CONSTRAINT group_policy_assignments_group_id_policy_id_key UNIQUE (group_id, policy_id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: group_policy_assignments group_policy_assignments_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.group_policy_assignments
    ADD CONSTRAINT group_policy_assignments_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: job_logs job_logs_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.job_logs
    ADD CONSTRAINT job_logs_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: organizations organizations_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: organizations organizations_slug_key; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.organizations
    ADD CONSTRAINT organizations_slug_key UNIQUE (slug);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: platform_users platform_users_email_key; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.platform_users
    ADD CONSTRAINT platform_users_email_key UNIQUE (email);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: platform_users platform_users_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.platform_users
    ADD CONSTRAINT platform_users_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: report_configs report_configs_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.report_configs
    ADD CONSTRAINT report_configs_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: report_history report_history_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.report_history
    ADD CONSTRAINT report_history_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resource_discovery_staging resource_discovery_staging_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resource_discovery_staging
    ADD CONSTRAINT resource_discovery_staging_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resource_groups resource_groups_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resource_groups
    ADD CONSTRAINT resource_groups_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resources resources_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: sla_exclusions sla_exclusions_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.sla_exclusions
    ADD CONSTRAINT sla_exclusions_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: sla_policies sla_policies_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.sla_policies
    ADD CONSTRAINT sla_policies_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: snapshot_items snapshot_items_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.snapshot_items
    ADD CONSTRAINT snapshot_items_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: snapshots snapshots_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.snapshots
    ADD CONSTRAINT snapshots_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: tenants tenants_external_tenant_id_key; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.tenants
    ADD CONSTRAINT tenants_external_tenant_id_key UNIQUE (external_tenant_id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: tenants tenants_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.tenants
    ADD CONSTRAINT tenants_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: user_roles user_roles_pkey; Type: CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.user_roles
    ADD CONSTRAINT user_roles_pkey PRIMARY KEY (user_id, role);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: idx_admin_consent_active; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_admin_consent_active ON tm_vault.admin_consent_tokens USING btree (is_active);


--
-- Name: idx_admin_consent_org; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_admin_consent_org ON tm_vault.admin_consent_tokens USING btree (org_id);


--
-- Name: idx_admin_consent_tenant; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_admin_consent_tenant ON tm_vault.admin_consent_tokens USING btree (tenant_id);


--
-- Name: idx_admin_consent_type; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_admin_consent_type ON tm_vault.admin_consent_tokens USING btree (consent_type);


--
-- Name: idx_audit_events_action; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_audit_events_action ON tm_vault.audit_events USING btree (action);


--
-- Name: idx_audit_events_occurred; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_audit_events_occurred ON tm_vault.audit_events USING btree (occurred_at DESC);


--
-- Name: idx_audit_events_org; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_audit_events_org ON tm_vault.audit_events USING btree (org_id);


--
-- Name: idx_audit_events_resource; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_audit_events_resource ON tm_vault.audit_events USING btree (resource_id);


--
-- Name: idx_audit_events_tenant; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_audit_events_tenant ON tm_vault.audit_events USING btree (tenant_id);


--
-- Name: idx_discovery_runs_status; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_discovery_runs_status ON tm_vault.discovery_runs USING btree (status);


--
-- Name: idx_discovery_runs_tenant_started; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_discovery_runs_tenant_started ON tm_vault.discovery_runs USING btree (tenant_id, started_at DESC);


--
-- Name: idx_discovery_stage_lookup; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_discovery_stage_lookup ON tm_vault.resource_discovery_staging USING btree (run_id, tenant_id, resource_type, external_id);


--
-- Name: idx_discovery_stage_run; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_discovery_stage_run ON tm_vault.resource_discovery_staging USING btree (run_id);


--
-- Name: idx_group_policy_assignments_group; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_group_policy_assignments_group ON tm_vault.group_policy_assignments USING btree (group_id);


--
-- Name: idx_group_policy_assignments_policy; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_group_policy_assignments_policy ON tm_vault.group_policy_assignments USING btree (policy_id);


--
-- Name: idx_report_configs_org; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_report_configs_org ON tm_vault.report_configs USING btree (org_id);


--
-- Name: idx_report_history_generated; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_report_history_generated ON tm_vault.report_history USING btree (generated_at DESC);


--
-- Name: idx_report_history_org; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_report_history_org ON tm_vault.report_history USING btree (org_id);


--
-- Name: idx_resource_groups_priority; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_resource_groups_priority ON tm_vault.resource_groups USING btree (tenant_id, priority, enabled);


--
-- Name: idx_resource_groups_tenant; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_resource_groups_tenant ON tm_vault.resource_groups USING btree (tenant_id, enabled);


--
-- Name: idx_resources_tenant_status_type; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_resources_tenant_status_type ON tm_vault.resources USING btree (tenant_id, status, type);


--
-- Name: idx_resources_tenant_type_external; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_resources_tenant_type_external ON tm_vault.resources USING btree (tenant_id, type, external_id);


--
-- Name: idx_sla_exclusions_policy; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_sla_exclusions_policy ON tm_vault.sla_exclusions USING btree (policy_id, enabled);


--
-- Name: idx_sla_policies_tenant_service; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_sla_policies_tenant_service ON tm_vault.sla_policies USING btree (tenant_id, service_type);


--
-- Name: idx_snapshot_items_metadata_chat_id; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_snapshot_items_metadata_chat_id ON tm_vault.snapshot_items USING btree (((metadata ->> 'chatId'::text))) WHERE ((item_type)::text = 'TEAMS_CHAT_MESSAGE'::text);


--
-- Name: idx_snapshot_items_tenant_checksum; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_snapshot_items_tenant_checksum ON tm_vault.snapshot_items USING btree (tenant_id, content_checksum);


--
-- Name: ix_resources_parent_id; Type: INDEX; Schema: tm_vault; Owner: -
--

CREATE INDEX IF NOT EXISTS ix_resources_parent_id ON tm_vault.resources USING btree (parent_resource_id);


--
-- Name: admin_consent_tokens admin_consent_tokens_org_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.admin_consent_tokens
    ADD CONSTRAINT admin_consent_tokens_org_id_fkey FOREIGN KEY (org_id) REFERENCES tm_vault.organizations(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: admin_consent_tokens admin_consent_tokens_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.admin_consent_tokens
    ADD CONSTRAINT admin_consent_tokens_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: alerts alerts_org_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.alerts
    ADD CONSTRAINT alerts_org_id_fkey FOREIGN KEY (org_id) REFERENCES tm_vault.organizations(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: alerts alerts_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.alerts
    ADD CONSTRAINT alerts_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: audit_events audit_events_org_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.audit_events
    ADD CONSTRAINT audit_events_org_id_fkey FOREIGN KEY (org_id) REFERENCES tm_vault.organizations(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: audit_events audit_events_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.audit_events
    ADD CONSTRAINT audit_events_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: discovery_runs discovery_runs_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.discovery_runs
    ADD CONSTRAINT discovery_runs_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: group_policy_assignments group_policy_assignments_group_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.group_policy_assignments
    ADD CONSTRAINT group_policy_assignments_group_id_fkey FOREIGN KEY (group_id) REFERENCES tm_vault.resource_groups(id) ON DELETE CASCADE;
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: group_policy_assignments group_policy_assignments_policy_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.group_policy_assignments
    ADD CONSTRAINT group_policy_assignments_policy_id_fkey FOREIGN KEY (policy_id) REFERENCES tm_vault.sla_policies(id) ON DELETE CASCADE;
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: job_logs job_logs_job_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.job_logs
    ADD CONSTRAINT job_logs_job_id_fkey FOREIGN KEY (job_id) REFERENCES tm_vault.jobs(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: jobs jobs_resource_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.jobs
    ADD CONSTRAINT jobs_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES tm_vault.resources(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: jobs jobs_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.jobs
    ADD CONSTRAINT jobs_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: platform_users platform_users_org_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.platform_users
    ADD CONSTRAINT platform_users_org_id_fkey FOREIGN KEY (org_id) REFERENCES tm_vault.organizations(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: platform_users platform_users_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.platform_users
    ADD CONSTRAINT platform_users_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: report_configs report_configs_org_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.report_configs
    ADD CONSTRAINT report_configs_org_id_fkey FOREIGN KEY (org_id) REFERENCES tm_vault.organizations(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: report_history report_history_org_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.report_history
    ADD CONSTRAINT report_history_org_id_fkey FOREIGN KEY (org_id) REFERENCES tm_vault.organizations(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: report_history report_history_report_config_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.report_history
    ADD CONSTRAINT report_history_report_config_id_fkey FOREIGN KEY (report_config_id) REFERENCES tm_vault.report_configs(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resource_discovery_staging resource_discovery_staging_run_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resource_discovery_staging
    ADD CONSTRAINT resource_discovery_staging_run_id_fkey FOREIGN KEY (run_id) REFERENCES tm_vault.discovery_runs(id) ON DELETE CASCADE;
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resource_discovery_staging resource_discovery_staging_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resource_discovery_staging
    ADD CONSTRAINT resource_discovery_staging_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resource_groups resource_groups_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resource_groups
    ADD CONSTRAINT resource_groups_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id) ON DELETE CASCADE;
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resources resources_parent_resource_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resources
    ADD CONSTRAINT resources_parent_resource_id_fkey FOREIGN KEY (parent_resource_id) REFERENCES tm_vault.resources(id) ON DELETE CASCADE;
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resources resources_sla_policy_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resources
    ADD CONSTRAINT resources_sla_policy_id_fkey FOREIGN KEY (sla_policy_id) REFERENCES tm_vault.sla_policies(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: resources resources_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.resources
    ADD CONSTRAINT resources_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: sla_exclusions sla_exclusions_policy_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.sla_exclusions
    ADD CONSTRAINT sla_exclusions_policy_id_fkey FOREIGN KEY (policy_id) REFERENCES tm_vault.sla_policies(id) ON DELETE CASCADE;
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: sla_policies sla_policies_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.sla_policies
    ADD CONSTRAINT sla_policies_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: snapshot_items snapshot_items_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.snapshot_items
    ADD CONSTRAINT snapshot_items_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES tm_vault.snapshots(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: snapshot_items snapshot_items_tenant_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.snapshot_items
    ADD CONSTRAINT snapshot_items_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: snapshots snapshots_job_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.snapshots
    ADD CONSTRAINT snapshots_job_id_fkey FOREIGN KEY (job_id) REFERENCES tm_vault.jobs(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: snapshots snapshots_resource_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.snapshots
    ADD CONSTRAINT snapshots_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES tm_vault.resources(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: tenants tenants_org_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.tenants
    ADD CONSTRAINT tenants_org_id_fkey FOREIGN KEY (org_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- Name: user_roles user_roles_user_id_fkey; Type: FK CONSTRAINT; Schema: tm_vault; Owner: -
--

DO $$ BEGIN
ALTER TABLE ONLY tm_vault.user_roles
    ADD CONSTRAINT user_roles_user_id_fkey FOREIGN KEY (user_id) REFERENCES tm_vault.platform_users(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;


--
-- PostgreSQL database dump complete
--



-- =========================================================================
-- Production extras (present on Railway, auto-created by the full ORM
-- bootstrap path; the raw-DDL bootstrap in shared/database.py skips both,
-- so we add them here so dev environments match prod exactly.)
-- =========================================================================

-- access_groups: role/scope container referenced by user_roles + resource
-- access policies. Not created by the raw-DDL path but defined in models.py
-- (class AccessGroup). See that class for column meanings.
CREATE TABLE IF NOT EXISTS tm_vault.access_groups (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    tenant_id uuid,
    name character varying NOT NULL,
    description character varying,
    scope character varying DEFAULT 'TENANT'::character varying,
    resource_ids uuid[] DEFAULT '{}'::uuid[],
    permissions json DEFAULT '{}'::json,
    member_ids uuid[] DEFAULT '{}'::uuid[],
    active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);
DO $$ BEGIN
    ALTER TABLE ONLY tm_vault.access_groups
        ADD CONSTRAINT access_groups_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE ONLY tm_vault.access_groups
        ADD CONSTRAINT access_groups_org_id_fkey
        FOREIGN KEY (org_id) REFERENCES tm_vault.organizations(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE ONLY tm_vault.access_groups
        ADD CONSTRAINT access_groups_tenant_id_fkey
        FOREIGN KEY (tenant_id) REFERENCES tm_vault.tenants(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
    WHEN duplicate_table THEN NULL;
    WHEN duplicate_column THEN NULL;
    WHEN invalid_table_definition THEN NULL;
    WHEN unique_violation THEN NULL;
END $$;

-- snapshot_items.search_vector: full-text search column populated by the
-- (future) indexer. Plain tsvector; triggers / generated expression live
-- elsewhere so this file just reserves the column.
DO $$ BEGIN
    ALTER TABLE tm_vault.snapshot_items ADD COLUMN search_vector tsvector;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
CREATE INDEX IF NOT EXISTS idx_snapshot_items_search_vector
    ON tm_vault.snapshot_items USING gin (search_vector);

-- ============================================================
-- Teams chat export (v1) — schema additions
-- Required for tm_backend/workers/chat-export-worker and
-- tm_backend/services/job-service/chat_export.py.
-- ============================================================

-- snapshot_items.parent_external_id: links CHAT_ATTACHMENT and
-- CHAT_HOSTED_CONTENT rows back to their parent message so the export
-- pipeline can resolve attachments without table-scanning metadata JSONB.
DO $$ BEGIN
    ALTER TABLE tm_vault.snapshot_items ADD COLUMN parent_external_id VARCHAR;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- jobstatus gains PENDING (queued-but-idempotency-safe) and CANCELLING
-- (transient while worker wraps up a user-cancelled export).
ALTER TYPE tm_vault.jobstatus ADD VALUE IF NOT EXISTS 'PENDING';
ALTER TYPE tm_vault.jobstatus ADD VALUE IF NOT EXISTS 'CANCELLING';

-- Chat-export scope resolution + job-concurrency indexes
CREATE INDEX IF NOT EXISTS idx_snapshot_items_folder_type
    ON tm_vault.snapshot_items (folder_path, item_type);

CREATE INDEX IF NOT EXISTS idx_snapshot_items_parent_ext
    ON tm_vault.snapshot_items (parent_external_id)
    WHERE item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT');

CREATE INDEX IF NOT EXISTS idx_snapshot_items_snapshot_type
    ON tm_vault.snapshot_items (snapshot_id, item_type);

CREATE INDEX IF NOT EXISTS idx_snapshot_items_chat_created
    ON tm_vault.snapshot_items (snapshot_id, folder_path, created_at)
    WHERE item_type IN ('TEAMS_CHAT_MESSAGE','TEAMS_MESSAGE','TEAMS_MESSAGE_REPLY');

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_type_status
    ON tm_vault.jobs (tenant_id, type, status)
    WHERE status IN ('QUEUED','PENDING','RUNNING');
