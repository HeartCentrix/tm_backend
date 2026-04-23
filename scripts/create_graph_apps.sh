#!/usr/bin/env bash
# Create N Microsoft Entra ID (Azure AD) app registrations for TMvault
# multi-app Graph throttle fan-out (APP_1_* .. APP_16_*).
#
# Every app gets the EXACT Graph Application permissions the TMvault
# codebase uses, plus admin consent granted in the tenant. Idempotent:
# re-running skips apps that already exist by display name.
#
# Permission list derived from source via graphify; see "PERMISSIONS"
# array below for the authoritative list pulled from shared/graph_client.py
# + docs/superpowers/specs/2026-04-19-graph-api-throttle-hardening-design.md.
#
# USAGE
#   ./create_graph_apps.sh [-n COUNT] [-p PREFIX] [-o OUT_FILE] [--skip-consent]
#
#   -n  number of apps to create (0–16, default 16)
#   -p  app display-name prefix (default "TMvault-Graph")
#   -o  output .env file path   (default ./.env.apps)
#   --skip-consent  create apps + secrets but do NOT grant admin consent
#                   (use when running as a non-admin or when consent will
#                   be done via Azure Portal admin-consent URL)
#
# PREREQS
#   1. `az` CLI installed (>= 2.55) — brew install azure-cli / apt install azure-cli
#   2. `az login` with a Global Administrator or
#      Privileged Role Administrator (needed for admin consent on
#      high-privilege Graph permissions)
#   3. `jq` installed (usually preinstalled on mac/linux)
#
# OUTPUT
#   A .env.apps file containing APP_1_* .. APP_N_* env var triples,
#   plus a summary of admin-consent URLs if --skip-consent was used.
#
# COST
#   $0 — app registrations are free. Secrets expire in 24 months,
#   rotate them before.
#
set -euo pipefail

# ─── Defaults ────────────────────────────────────────────────────────────
COUNT=16
PREFIX="TMvault-Graph"
OUT_FILE="./.env.apps"
SKIP_CONSENT=false
SECRET_YEARS=2

# Microsoft Graph resource app ID (global constant)
GRAPH_APP_ID="00000003-0000-0000-c000-000000000000"

# ─── Permission matrix (Application permissions only) ────────────────────
# Each entry: "<Permission-Name>:<Role-GUID>". Role GUIDs are Microsoft's
# published constants — they never change. Sourced from:
#   https://learn.microsoft.com/en-us/graph/permissions-reference
#
# Grouped by workload so it's easy to trim in a future edit if an install
# doesn't need (say) Entra / BitLocker.
PERMISSIONS=(
  # ── Mail ──
  "Mail.ReadWrite:e2a3a72e-5f79-4c64-b1b1-878b674786c9"
  "MailboxSettings.Read:40f97065-369a-49f4-947c-6a255697ae91"

  # ── OneDrive / SharePoint ──
  "Files.ReadWrite.All:75359482-378d-4052-8f01-80520e7db3cd"
  "Sites.ReadWrite.All:9492366f-7969-46a4-8d15-ed1a20078fff"
  "Sites.FullControl.All:a82116e5-55eb-4c41-a434-62fe8a61c773"

  # ── Teams chats & channels ──
  "Chat.Read.All:6b7d71aa-70aa-4810-a8d9-5d9fb2830017"
  "Chat.ReadWrite.All:294ce7c9-31ba-490a-ad7d-97a7d075e4ed"
  "ChatMessage.Read.All:b9bb2381-47a4-46cd-aafb-00cb12f68504"
  "ChannelMessage.Read.All:7b2449af-6ccd-4f4d-9f78-e550c193f0d1"
  "Team.ReadBasic.All:2280dda6-0bfd-44ee-a2f4-cb867cfc4c1e"

  # ── Calendar & Contacts ──
  "Calendars.ReadWrite:ef54d2bf-783f-4e0f-bca1-3210c0444d99"
  "Contacts.ReadWrite:6918b873-d17a-4dc1-b314-35f528134491"

  # ── OneNote & To Do ──
  "Notes.Read.All:3aeca27b-ee3a-4c2b-8ded-80376e2134a4"
  "Tasks.ReadWrite.All:44e666d1-d276-445b-a5fc-8815eeb81d55"

  # ── Entra (users, groups, directory, policies) ──
  "User.Read.All:df021288-bdef-4463-88db-98f22de89214"
  "Group.ReadWrite.All:62a82d76-70ea-41e2-9197-370581804d09"
  "GroupMember.Read.All:98830695-27a2-44f7-8c18-0c3ebc9698f6"
  "Directory.Read.All:7ab1d382-f21e-4acd-a863-ba3e13f7da61"
  "Directory.ReadWrite.All:19dbc75e-c2e2-444c-a770-ec69d8559fc7"
  "Policy.Read.All:246dd0d5-5bd0-4def-940b-0421030a5b68"

  # ── Audit / BitLocker / Intune (optional but code references them) ──
  "AuditLog.Read.All:b0afded3-3588-46d8-8b3d-9842eff778da"
  "BitlockerKey.Read.All:57f1cf28-c0c4-4ec3-9a30-19a2eaaf2f6e"
  "DeviceManagementManagedDevices.Read.All:2f51be20-0bb4-4fed-bf7b-db946066c75e"
)

# ─── Arg parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -n) COUNT="$2"; shift 2 ;;
    -p) PREFIX="$2"; shift 2 ;;
    -o) OUT_FILE="$2"; shift 2 ;;
    --skip-consent) SKIP_CONSENT=true; shift ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "Unknown flag: $1" >&2; exit 2 ;;
  esac
done

if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || (( COUNT < 0 || COUNT > 16 )); then
  echo "ERROR: -n must be 0..16, got '$COUNT'" >&2
  exit 2
fi

# ─── Pre-flight ──────────────────────────────────────────────────────────
command -v az >/dev/null 2>&1 || { echo "ERROR: 'az' CLI not found. brew install azure-cli" >&2; exit 3; }
command -v jq >/dev/null 2>&1 || { echo "ERROR: 'jq' not found. brew install jq" >&2; exit 3; }

if ! az account show >/dev/null 2>&1; then
  echo "ERROR: not logged in. Run 'az login' first." >&2
  exit 4
fi

TENANT_ID="$(az account show --query tenantId -o tsv)"
SIGNED_IN_USER="$(az account show --query user.name -o tsv)"
echo "Tenant : $TENANT_ID"
echo "User   : $SIGNED_IN_USER"
echo "Apps   : $COUNT × '$PREFIX-N'"
echo "Out    : $OUT_FILE"
echo "Consent: $([ "$SKIP_CONSENT" = true ] && echo 'SKIPPED — manual consent URL will be printed' || echo 'auto-granted (requires admin)')"
echo

if (( COUNT == 0 )); then
  echo "COUNT=0 — nothing to do. (Useful as a dry-run prereq check.)"
  exit 0
fi

# ─── Build --required-resource-accesses payload once ─────────────────────
# Format matches what `az ad app create` expects.
RESOURCE_ACCESS_JSON=$(
  printf '[{"resourceAppId":"%s","resourceAccess":[' "$GRAPH_APP_ID"
  first=true
  for entry in "${PERMISSIONS[@]}"; do
    id="${entry##*:}"
    if $first; then first=false; else printf ','; fi
    printf '{"id":"%s","type":"Role"}' "$id"
  done
  printf ']}]'
)

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT
RESOURCE_FILE="$TMPDIR/resources.json"
echo "$RESOURCE_ACCESS_JSON" > "$RESOURCE_FILE"

# ─── Output file header ──────────────────────────────────────────────────
: > "$OUT_FILE"
{
  echo "# TMvault Graph app registrations — generated by create_graph_apps.sh"
  echo "# Tenant : $TENANT_ID"
  echo "# Created: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# Apps   : $COUNT"
  echo
  echo "# Copy these into your Railway/prod .env (all services share them)."
  echo
} >> "$OUT_FILE"

# ─── Create / discover each app ─────────────────────────────────────────
declare -a CLIENT_IDS=()
declare -a APP_OBJECT_IDS=()

for i in $(seq 1 "$COUNT"); do
  name="${PREFIX}-${i}"
  echo "── [$i/$COUNT] $name"

  # Idempotency: skip if app with this display name already exists.
  existing_client_id="$(az ad app list --display-name "$name" --query "[0].appId" -o tsv 2>/dev/null || true)"
  existing_object_id="$(az ad app list --display-name "$name" --query "[0].id" -o tsv 2>/dev/null || true)"

  if [[ -n "$existing_client_id" && "$existing_client_id" != "null" ]]; then
    echo "   exists → client_id=$existing_client_id (skipping create; rotating secret)"
    client_id="$existing_client_id"
    object_id="$existing_object_id"
    # Update permissions to current spec — handles schema drift between script runs.
    az ad app update --id "$object_id" --required-resource-accesses "@$RESOURCE_FILE" 1>/dev/null
  else
    create_json="$(az ad app create \
      --display-name "$name" \
      --sign-in-audience AzureADMyOrg \
      --required-resource-accesses "@$RESOURCE_FILE" \
      -o json)"
    client_id="$(echo "$create_json" | jq -r '.appId')"
    object_id="$(echo "$create_json" | jq -r '.id')"
    echo "   created → client_id=$client_id"
  fi

  # Ensure a service principal exists for this app in this tenant (needed
  # for admin consent and app-only token issuance).
  if ! az ad sp show --id "$client_id" >/dev/null 2>&1; then
    az ad sp create --id "$client_id" >/dev/null
    echo "   service principal created"
  fi

  # Create a fresh client secret (2-year lifetime). Previous secrets are
  # left intact — rotate manually if you want to purge old ones.
  secret_json="$(az ad app credential reset \
    --id "$object_id" \
    --append \
    --years "$SECRET_YEARS" \
    --display-name "tmvault-$(date -u +%Y%m%d)" \
    -o json)"
  client_secret="$(echo "$secret_json" | jq -r '.password')"
  echo "   secret issued (expires in $SECRET_YEARS years)"

  # Write env lines
  {
    echo "APP_${i}_CLIENT_ID=$client_id"
    echo "APP_${i}_CLIENT_SECRET=$client_secret"
    echo "APP_${i}_TENANT_ID=$TENANT_ID"
    echo
  } >> "$OUT_FILE"

  CLIENT_IDS+=("$client_id")
  APP_OBJECT_IDS+=("$object_id")
done

# ─── Admin consent ───────────────────────────────────────────────────────
if [[ "$SKIP_CONSENT" = false ]]; then
  echo
  echo "── Granting admin consent on ${#CLIENT_IDS[@]} app(s)"
  failed_consent=()
  for idx in "${!CLIENT_IDS[@]}"; do
    cid="${CLIENT_IDS[$idx]}"
    if az ad app permission admin-consent --id "$cid" 2>"$TMPDIR/consent.err"; then
      echo "   consented: $cid"
    else
      echo "   FAILED:   $cid (see $TMPDIR/consent.err — likely not Global Admin)"
      failed_consent+=("$cid")
    fi
  done

  if (( ${#failed_consent[@]} > 0 )); then
    {
      echo
      echo "# ─── MANUAL CONSENT REQUIRED for ${#failed_consent[@]} app(s) ───"
      echo "# Have a Global Admin open each URL below and click 'Accept':"
      for cid in "${failed_consent[@]}"; do
        echo "#   https://login.microsoftonline.com/${TENANT_ID}/adminconsent?client_id=${cid}"
      done
    } >> "$OUT_FILE"
    echo
    echo "⚠  Consent partially failed — manual URLs appended to $OUT_FILE"
  fi
else
  {
    echo "# ─── MANUAL CONSENT REQUIRED (--skip-consent was set) ───"
    echo "# Have a Global Admin open each URL and click 'Accept':"
    for cid in "${CLIENT_IDS[@]}"; do
      echo "#   https://login.microsoftonline.com/${TENANT_ID}/adminconsent?client_id=${cid}"
    done
  } >> "$OUT_FILE"
  echo
  echo "ℹ  Admin-consent URLs appended to $OUT_FILE"
fi

# ─── Summary ─────────────────────────────────────────────────────────────
cat <<SUMMARY

═══════════════════════════════════════════════════════════════════════
✓ Done. $COUNT app(s) created / reconciled.

  ENV FILE : $OUT_FILE
  TENANT   : $TENANT_ID
  PERMS    : ${#PERMISSIONS[@]} Application permissions per app

Next steps:
  1. Copy $OUT_FILE contents into your Railway variables
     (or merge into tm_backend/.env for local docker-compose).
  2. Verify consent in Azure Portal → Enterprise applications → filter
     by '$PREFIX-' → each should show 'Granted for <tenant>' on all perms.
  3. Restart workers:  docker compose restart backup-worker restore-worker
                       discovery-worker azure-workload-worker
  4. Confirm fan-out:  curl http://audit-service:8012/api/v1/graph-apps/stats
     (should report $COUNT registered apps).

Priority scheduling is ALREADY enabled via GRAPH_PRIORITY_SCHEDULING_ENABLED
  — no additional env needed, every app inherits the priority scheduler.
═══════════════════════════════════════════════════════════════════════
SUMMARY
