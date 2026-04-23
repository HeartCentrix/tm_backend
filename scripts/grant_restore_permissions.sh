#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# grant_restore_permissions.sh
#
# Grant the Microsoft Graph *Application* (app-only) permissions that the
# TMvault restore/backup workers need across every app registration in the
# multi-app rotation, then admin-consent each one.
#
# Why this script exists:
#   Every backend worker (backup-worker, restore-worker, azure-workload-worker)
#   uses client_credentials flow. Only Application-type permissions are in the
#   resulting token — Delegated permissions are invisible. Clicking
#   "Calendars.ReadWrite" in the Entra UI without picking "Application
#   permissions" leaves the worker 403'ing with ErrorAccessDenied even though
#   the UI shows the scope as "Granted".
#
# What this script does:
#   1. Verifies az CLI is installed and you're signed in to the right tenant.
#   2. For each app in APP_IDS:
#        - Reads current permissions (no writes yet).
#        - Adds every required scope as Role (Application) if missing.
#        - Grants admin consent via Graph appRoleAssignments.
#   3. Prints a final matrix of which scopes are now granted on which apps.
#
# Works on macOS's bash 3.2 — uses parallel indexed arrays, no associative
# arrays.
#
# Safety:
#   - Idempotent: re-running is a no-op if everything is already granted.
#   - --dry-run flag skips every write and prints what would change.
#   - --apps and --scopes flags narrow the scope so you can test on one
#     app or one permission first.
# -----------------------------------------------------------------------------
set -eo pipefail

# ---- CONFIG -----------------------------------------------------------------

# The three app registrations that serve the TMvault workers. If you add
# more in shared/config.py GRAPH_APPS, add them here too.
APP_IDS=(
  "7be18314-da9a-4244-a570-daed55e11234"   # APP_1
  "0a763313-2273-4bd4-ba60-96d7bf876e02"   # APP_2
  "17858e25-77ae-4165-a1da-85c40c0ed9f4"   # APP_3
)

# Expected tenant id. We sanity-check `az account show` against this so the
# script can't accidentally grant permissions against the wrong tenant when
# the operator has multiple subscriptions.
EXPECTED_TENANT_ID="0cac6ab1-bbd3-4e27-8325-333c51e4567d"   # QFION Software

# Microsoft Graph's service principal appId. Same for every tenant.
GRAPH_APP_ID="00000003-0000-0000-c000-000000000000"

# Parallel arrays: ROLE_IDS[i] is the GUID, SCOPE_NAMES[i] is the human name.
# These GUIDs are Microsoft-assigned and constant across every tenant (they
# describe the Graph API's own appRoles). Source:
#   https://learn.microsoft.com/en-us/graph/permissions-reference
ROLE_IDS=(
  "ef54d2bf-783f-4e0f-bca1-3210c0444d99"
  "6918b873-d17a-4dc1-b314-35f528134491"
  "e2a3a72e-5f79-4c64-b1b1-878b674786c9"
  "75359482-378d-4052-8f01-80520e7db3cd"
  "9492366f-7969-46a4-8d15-ed1a20078fff"
  "741f803b-c850-494e-b5df-cde7c675a1ca"
  "19dbc75e-c2e2-444c-a770-ec69d8559fc7"
)
SCOPE_NAMES=(
  "Calendars.ReadWrite"
  "Contacts.ReadWrite"
  "Mail.ReadWrite"
  "Files.ReadWrite.All"
  "Sites.ReadWrite.All"
  "User.ReadWrite.All"
  "Directory.ReadWrite.All"
)

# ---- FLAGS ------------------------------------------------------------------
DRY_RUN=0
ONLY_APPS=""
ONLY_SCOPES=""

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --apps)    ONLY_APPS="$2"; shift ;;
    --scopes)  ONLY_SCOPES="$2"; shift ;;
    -h|--help)
      sed -n '2,35p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
  shift
done

say()  { printf '\n\033[1m==>\033[0m %s\n' "$*"; }
ok()   { printf '    \033[32m✓\033[0m %s\n' "$*"; }
miss() { printf '    \033[33m+\033[0m %s\n' "$*"; }
bad()  { printf '    \033[31m✗\033[0m %s\n' "$*"; }

# Return 0 if $1 (whitespace-separated haystack) contains $2 as an exact line/word.
contains() {
  local haystack="$1"
  local needle="$2"
  # Use grep -F -x against a newline-split haystack.
  printf '%s\n' $haystack | grep -Fxq "$needle"
}

# ---- NARROW APPS / SCOPES IF FLAGGED ---------------------------------------

if [ -n "$ONLY_APPS" ]; then
  IFS=',' read -r -a APP_IDS <<< "$ONLY_APPS"
fi

if [ -n "$ONLY_SCOPES" ]; then
  FILTERED_ROLE_IDS=()
  FILTERED_SCOPE_NAMES=()
  IFS=',' read -r -a WANTED <<< "$ONLY_SCOPES"
  i=0
  while [ $i -lt ${#ROLE_IDS[@]} ]; do
    for want in "${WANTED[@]}"; do
      if [ "${SCOPE_NAMES[$i]}" = "$want" ]; then
        FILTERED_ROLE_IDS+=("${ROLE_IDS[$i]}")
        FILTERED_SCOPE_NAMES+=("${SCOPE_NAMES[$i]}")
      fi
    done
    i=$((i + 1))
  done
  ROLE_IDS=("${FILTERED_ROLE_IDS[@]}")
  SCOPE_NAMES=("${FILTERED_SCOPE_NAMES[@]}")
fi

if [ ${#ROLE_IDS[@]} -eq 0 ]; then
  bad "No scopes left after --scopes filter"
  exit 2
fi

# ---- PRE-FLIGHT -------------------------------------------------------------

say "Pre-flight"

if ! command -v az >/dev/null 2>&1; then
  bad "az CLI not found. Install from https://learn.microsoft.com/cli/azure/install-azure-cli"
  exit 1
fi
ok "az CLI present ($(az version --query '\"azure-cli\"' -o tsv 2>/dev/null || echo unknown))"

if ! az account show >/dev/null 2>&1; then
  bad "Not signed in. Run: az login --tenant $EXPECTED_TENANT_ID"
  exit 1
fi

CURRENT_TENANT="$(az account show --query tenantId -o tsv)"
CURRENT_USER="$(az account show --query user.name -o tsv)"
if [ "$CURRENT_TENANT" != "$EXPECTED_TENANT_ID" ]; then
  bad "Signed-in tenant $CURRENT_TENANT ≠ expected $EXPECTED_TENANT_ID"
  echo "    Re-run: az login --tenant $EXPECTED_TENANT_ID"
  exit 1
fi
ok "Signed in as $CURRENT_USER on tenant $CURRENT_TENANT"

# Fetch the Graph SP's object id once; every permission-add / admin-consent
# call needs it.
GRAPH_SP_OBJECT_ID="$(az ad sp show --id "$GRAPH_APP_ID" --query id -o tsv 2>/dev/null)" \
  || { bad "Can't read Microsoft Graph service principal — your account lacks Directory.Read perms"; exit 1; }
ok "Microsoft Graph SP object id: $GRAPH_SP_OBJECT_ID"

if [ $DRY_RUN -eq 1 ]; then say "DRY RUN — no writes will be performed"; fi

# ---- MAIN LOOP --------------------------------------------------------------

PROCESSED=0
for APP_ID in "${APP_IDS[@]}"; do
  say "App $APP_ID"

  APP_NAME="$(az ad app show --id "$APP_ID" --query displayName -o tsv 2>/dev/null || echo '<unknown>')"
  SP_OBJECT_ID="$(az ad sp show --id "$APP_ID" --query id -o tsv 2>/dev/null)" || {
    bad "Service principal not found for appId $APP_ID — does the app exist in this tenant?"
    continue
  }
  ok "Name: $APP_NAME"
  ok "SP object id: $SP_OBJECT_ID"

  # Current Application-role assignments granted against Microsoft Graph.
  CURRENT="$(
    az rest --method GET \
      --uri "https://graph.microsoft.com/v1.0/servicePrincipals/${SP_OBJECT_ID}/appRoleAssignments" \
      --query "value[?resourceId=='${GRAPH_SP_OBJECT_ID}'].appRoleId" -o tsv 2>/dev/null || true
  )"

  NEEDS_PORTAL_SYNC=0
  i=0
  while [ $i -lt ${#ROLE_IDS[@]} ]; do
    ROLE_ID="${ROLE_IDS[$i]}"
    SCOPE_NAME="${SCOPE_NAMES[$i]}"
    i=$((i + 1))

    if contains "$CURRENT" "$ROLE_ID"; then
      ok "$SCOPE_NAME — already granted as Application"
      continue
    fi

    miss "$SCOPE_NAME — missing (will grant)"
    NEEDS_PORTAL_SYNC=1
    if [ $DRY_RUN -eq 1 ]; then continue; fi

    # Graph POST creates the role assignment directly; equivalent to
    # clicking "Grant admin consent" for that single role. Because we're
    # granting on the SP of the app itself, this IS admin consent for
    # app-only scopes — no separate admin-consent step is required.
    BODY="{\"principalId\":\"${SP_OBJECT_ID}\",\"resourceId\":\"${GRAPH_SP_OBJECT_ID}\",\"appRoleId\":\"${ROLE_ID}\"}"
    if az rest --method POST \
      --uri "https://graph.microsoft.com/v1.0/servicePrincipals/${SP_OBJECT_ID}/appRoleAssignments" \
      --headers 'Content-Type=application/json' \
      --body "$BODY" >/dev/null 2>&1; then
      ok "$SCOPE_NAME — granted"
    else
      # capture the error by re-running without --silent to surface cause
      RESP="$(
        az rest --method POST \
          --uri "https://graph.microsoft.com/v1.0/servicePrincipals/${SP_OBJECT_ID}/appRoleAssignments" \
          --headers 'Content-Type=application/json' \
          --body "$BODY" 2>&1 || true
      )"
      bad "$SCOPE_NAME — grant failed: $(echo "$RESP" | head -3)"
    fi
  done

  # Sync the app's requiredResourceAccess manifest so the Entra UI shows
  # the "Application permissions" rows alongside the actual grants.
  # This is cosmetic — appRoleAssignments above are what governs the
  # token's roles claim — but it keeps the portal in sync.
  if [ $NEEDS_PORTAL_SYNC -eq 1 ] && [ $DRY_RUN -eq 0 ]; then
    PERM_ARGS=""
    j=0
    while [ $j -lt ${#ROLE_IDS[@]} ]; do
      PERM_ARGS="${PERM_ARGS} ${ROLE_IDS[$j]}=Role"
      j=$((j + 1))
    done
    if az ad app permission add \
      --id "$APP_ID" --api "$GRAPH_APP_ID" \
      --api-permissions $PERM_ARGS >/dev/null 2>&1; then
      ok "portal manifest synced"
    else
      miss "portal manifest sync had warnings (harmless — behaviour governed by appRoleAssignments)"
    fi
  fi

  PROCESSED=$((PROCESSED + 1))
done

# ---- VERIFICATION MATRIX ----------------------------------------------------

say "Final permission matrix"

# Header row: app column + one column per scope.
printf '%-40s' "App"
i=0
while [ $i -lt ${#SCOPE_NAMES[@]} ]; do
  NAME="${SCOPE_NAMES[$i]}"
  printf '  %-22s' "${NAME:0:22}"
  i=$((i + 1))
done
printf '\n'

for APP_ID in "${APP_IDS[@]}"; do
  SP_OBJECT_ID="$(az ad sp show --id "$APP_ID" --query id -o tsv 2>/dev/null)" || continue
  GRANTED="$(
    az rest --method GET \
      --uri "https://graph.microsoft.com/v1.0/servicePrincipals/${SP_OBJECT_ID}/appRoleAssignments" \
      --query "value[?resourceId=='${GRAPH_SP_OBJECT_ID}'].appRoleId" -o tsv 2>/dev/null || true
  )"
  printf '%-40s' "$APP_ID"
  i=0
  while [ $i -lt ${#ROLE_IDS[@]} ]; do
    if contains "$GRANTED" "${ROLE_IDS[$i]}"; then
      printf '  %-22s' "✓"
    else
      printf '  %-22s' "—"
    fi
    i=$((i + 1))
  done
  printf '\n'
done

say "Done. $PROCESSED app(s) processed."
if [ $DRY_RUN -eq 1 ]; then
  echo "This was a dry run — re-run without --dry-run to apply."
else
  echo "Permissions can take 5–15 minutes to propagate to Exchange Online."
  echo "Test with a fresh restore job once you see no more ErrorAccessDenied in restore-worker logs."
fi
