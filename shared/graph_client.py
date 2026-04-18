"""Microsoft Graph API client for resource discovery"""
import asyncio
import logging
import httpx
from typing import AsyncGenerator, List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import hashlib
import time

from shared.power_bi_client import PowerBIClient

logger = logging.getLogger(__name__)

# Timeout constants — tuned for Graph API and token endpoint behavior
_DEFAULT_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=10.0)
_TOKEN_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)


class GraphClient:
    """Client for Microsoft Graph API calls with multi-app support"""

    GRAPH_URL = "https://graph.microsoft.com/v1.0"
    TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    SCOPES = [
        "https://graph.microsoft.com/.default"
    ]

    def __init__(self, client_id: str, client_secret: str, tenant_id: str, power_bi_refresh_token: Optional[str] = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.power_bi_refresh_token = power_bi_refresh_token
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        # Per-app token cache for multi-app rotation on paginated endpoints.
        # Key is client_id; value is (token, expiry_utc).
        self._multi_token_cache: Dict[str, Tuple[str, datetime]] = {}

    @property
    def app_client_id(self) -> str:
        """Return the app client ID for tracking purposes"""
        return self.client_id

    async def _get_token(self) -> str:
        """Get or refresh access token using client credentials with retry."""
        if self._access_token and self._token_expiry and datetime.utcnow() < self._token_expiry:
            return self._access_token

        last_exc = None
        for attempt in range(1, 4):  # 3 attempts
            try:
                async with httpx.AsyncClient(timeout=_TOKEN_TIMEOUT) as client:
                    resp = await client.post(
                        self.TOKEN_URL.format(tenant_id=self.tenant_id),
                        data={
                            "grant_type": "client_credentials",
                            "client_id": self.client_id,
                            "client_secret": self.client_secret,
                            "scope": "https://graph.microsoft.com/.default",
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    self._access_token = data["access_token"]
                    # Token expires in ~1 hour, refresh 5 min early
                    expires_in = data.get("expires_in", 3600)
                    self._token_expiry = datetime.utcnow() + timedelta(seconds=expires_in - 300)
                    return self._access_token
            except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                last_exc = e
                wait = 2 ** attempt
                print(f"[GraphClient] Token fetch timeout (attempt {attempt}/3), retry in {wait}s: {e}")
                await asyncio.sleep(wait)
        raise RuntimeError(f"Could not acquire token after 3 attempts: {last_exc}")
    
    async def _get(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated GET request with pagination, throttling, and timeout retry.
        Preserves @odata.deltaLink for incremental sync and handles
        single-object responses (e.g. /users/{id}) that have no 'value' array.
        """
        token = await self._get_token()
        all_items = []
        next_url = url
        max_retries = 5
        retry_count = 0
        delta_link = None
        last_data = {}

        while next_url:
            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    # ConsistencyLevel: eventual is only valid with $count queries
                    if params and params.get("$count") == "true":
                        headers = {"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"}
                    else:
                        headers = {"Authorization": f"Bearer {token}"}
                    resp = await client.get(next_url, headers=headers, params=params if not next_url.startswith("http") else None)

                    # Handle 429 throttling
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", "30"))
                        from shared.multi_app_manager import multi_app_manager
                        multi_app_manager.mark_throttled(self.client_id, retry_after)
                        if retry_count < max_retries:
                            retry_count += 1
                            await __import__('asyncio').sleep(retry_after)
                            continue
                        resp.raise_for_status()

                    resp.raise_for_status()
                    data = resp.json()
                    last_data = data
                    retry_count = 0  # Reset on success

                    # Single-object response (e.g. /users/{id}, /users/{id}/drive)
                    # These have no "value" array — return the object directly
                    if "value" not in data and "@odata.nextLink" not in data:
                        return data

                    all_items.extend(data.get("value", []))

                    # Capture delta link for incremental sync
                    if "@odata.deltaLink" in data:
                        delta_link = data["@odata.deltaLink"]

                    next_url = data.get("@odata.nextLink")
                    params = None  # params only on first request

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                if retry_count < max_retries:
                    retry_count += 1
                    wait = min(5 * retry_count, 30)
                    print(f"[GraphClient] Timeout on {next_url} (attempt {retry_count}/{max_retries}), retrying in {wait}s: {e}")
                    await __import__('asyncio').sleep(wait)
                    # Refresh token in case it expired during the wait
                    token = await self._get_token()
                    continue
                raise

        result = {
            "value": all_items,
            "@odata.count": last_data.get("@odata.count", len(all_items)),
        }
        # Preserve delta link so callers can save it for incremental backups
        if delta_link:
            result["@odata.deltaLink"] = delta_link
        return result

    async def _get_token_for_app(self, app) -> str:
        """Fetch a token for a specific AppRegistry and cache it by client_id.

        Enables per-page rotation across multiple app registrations on paginated
        endpoints (each app has an independent Graph quota → ~Nx throttle headroom).
        Falls back to normal _get_token() if caller didn't provide an app override.
        """
        cached = self._multi_token_cache.get(app.client_id)
        if cached and datetime.utcnow() < cached[1]:
            return cached[0]
        async with httpx.AsyncClient(timeout=_TOKEN_TIMEOUT) as client:
            resp = await client.post(
                self.TOKEN_URL.format(tenant_id=app.tenant_id),
                data={
                    "grant_type": "client_credentials",
                    "client_id": app.client_id,
                    "client_secret": app.client_secret,
                    "scope": "https://graph.microsoft.com/.default",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            expires_in = data.get("expires_in", 3600)
            expiry = datetime.utcnow() + timedelta(seconds=expires_in - 300)
            self._multi_token_cache[app.client_id] = (data["access_token"], expiry)
            return data["access_token"]

    async def _iter_pages(
        self, url: str, params: Optional[Dict] = None, rotate_apps: bool = True,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Streaming variant of _get: yields each page as it arrives.

        Overlaps downstream work (upload/DB) with Graph pagination — callers can
        flush data mid-iteration instead of waiting for the full response.
        Preserves 429 Retry-After + timeout retry semantics.

        rotate_apps=True uses multi_app_manager.get_next_app() per request to
        spread quota across app registrations (no-op when only 1 app is configured).
        """
        from shared.multi_app_manager import multi_app_manager

        next_url = url
        max_retries = 5
        retry_count = 0

        while next_url:
            if rotate_apps and multi_app_manager.app_count > 1:
                app = multi_app_manager.get_next_app()
                token = await self._get_token_for_app(app)
                active_client_id = app.client_id
            else:
                token = await self._get_token()
                active_client_id = self.client_id

            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    if params and params.get("$count") == "true":
                        headers = {"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"}
                    else:
                        headers = {"Authorization": f"Bearer {token}"}
                    resp = await client.get(
                        next_url, headers=headers,
                        params=params if not next_url.startswith("http") else None,
                    )

                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", "30"))
                        multi_app_manager.mark_throttled(active_client_id, retry_after)
                        if retry_count < max_retries:
                            retry_count += 1
                            await asyncio.sleep(retry_after)
                            continue
                        resp.raise_for_status()

                    resp.raise_for_status()
                    data = resp.json()
                    retry_count = 0
                    yield data

                    next_url = data.get("@odata.nextLink")
                    params = None  # params only on the first request

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                if retry_count < max_retries:
                    retry_count += 1
                    wait = min(5 * retry_count, 30)
                    print(f"[GraphClient] Timeout on {next_url} (attempt {retry_count}/{max_retries}), retrying in {wait}s: {e}")
                    await asyncio.sleep(wait)
                    continue
                raise

    async def _post(self, url: str, payload: Dict[str, Any], headers: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated POST request"""
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            req_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            if headers:
                req_headers.update(headers)
            resp = await client.post(url, headers=req_headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def _put(self, url: str, content: Any, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated PUT request (for file uploads)"""
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            req_headers = {"Authorization": f"Bearer {token}"}
            if headers:
                req_headers.update(headers)
            else:
                req_headers["Content-Type"] = "application/octet-stream"

            if isinstance(content, str):
                content = content.encode('utf-8')

            resp = await client.put(url, headers=req_headers, content=content)
            resp.raise_for_status()
            return resp.json()

    async def _patch(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make authenticated PATCH request"""
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            req_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            resp = await client.patch(url, headers=req_headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def _delete(self, url: str) -> None:
        """Make authenticated DELETE request"""
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            headers = {"Authorization": f"Bearer {token}"}
            resp = await client.delete(url, headers=headers)
            resp.raise_for_status()
    
    # System mailbox display-name prefixes Microsoft creates and never wants backed up.
    # Matches afi.ai's exclusion list — these are tenant infrastructure, not user data.
    _SYSTEM_MAILBOX_PREFIXES = (
        "DiscoverySearchMailbox",
        "FederatedEmail.",
        "SystemMailbox{",
        "Microsoft Office 365 portal",
        "MicrosoftSupport",
        "MicrosoftCustomerSupport",
        "Spam Quarantine",
    )

    @classmethod
    def _is_system_mailbox(cls, display_name: Optional[str], upn: Optional[str]) -> bool:
        for needle in (display_name or "", upn or ""):
            for prefix in cls._SYSTEM_MAILBOX_PREFIXES:
                if needle.startswith(prefix):
                    return True
        return False

    async def discover_users(self) -> List[Dict[str, Any]]:
        """Fetch all users from Entra ID. Skips Guest users and system mailboxes —
        afi.ai treats these as out-of-scope for backup."""
        result = await self._get(
            f"{self.GRAPH_URL}/users",
            params={
                "$top": "999",
                "$count": "true",
                "$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled,createdDateTime,userType",
            },
        )
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))

        users = []
        skipped_guest = 0
        skipped_system = 0
        for u in all_value:
            user_type = (u.get("userType") or "").lower()
            display_name = u.get("displayName") or u.get("mail") or u.get("userPrincipalName") or "Unknown"
            upn = u.get("userPrincipalName")
            if user_type == "guest":
                skipped_guest += 1
                continue
            if self._is_system_mailbox(display_name, upn):
                skipped_system += 1
                continue
            is_enabled = u.get("accountEnabled", True)
            users.append({
                "external_id": u.get("id"),
                "display_name": display_name,
                "email": u.get("mail") or upn,
                "type": "ENTRA_USER",
                "metadata": {
                    "user_principal_name": upn,
                    "job_title": u.get("jobTitle"),
                    "department": u.get("department"),
                    "account_enabled": is_enabled,
                    "user_type": u.get("userType"),
                    "created_at": u.get("createdDateTime"),
                },
                "_account_enabled": is_enabled,  # For discovery worker to filter
            })
        if skipped_guest or skipped_system:
            print(f"[GraphClient] discover_users: skipped {skipped_guest} guest(s), {skipped_system} system account(s)")
        return users
    
    @staticmethod
    def _classify_group(g: Dict[str, Any]) -> str:
        """Map Entra group flags to a canonical classification.

        Microsoft splits groups across three flags (groupTypes, mailEnabled,
        securityEnabled) which don't form an obvious taxonomy. afi.ai surfaces
        a single 'kind' to the user — we mirror that:
          M365_GROUP            — groupTypes contains 'Unified' (a.k.a. modern group)
          DISTRIBUTION_LIST     — mail-enabled, NOT security, no Unified flag
          MAIL_ENABLED_SECURITY — both mail- and security-enabled
          SECURITY_GROUP        — security-only (not mail-enabled)
        Anything else falls back to UNKNOWN — typically dynamic groups or
        provisioning artifacts. The caller decides whether to back it up."""
        group_types = [t.lower() for t in (g.get("groupTypes") or [])]
        mail_enabled = bool(g.get("mailEnabled"))
        security_enabled = bool(g.get("securityEnabled"))
        if "unified" in group_types:
            return "M365_GROUP"
        if mail_enabled and security_enabled:
            return "MAIL_ENABLED_SECURITY"
        if mail_enabled and not security_enabled:
            return "DISTRIBUTION_LIST"
        if security_enabled and not mail_enabled:
            return "SECURITY_GROUP"
        return "UNKNOWN"

    async def discover_groups(self) -> List[Dict[str, Any]]:
        """Fetch all groups from Entra ID and classify each one.

        Unified (M365) groups are emitted as type=M365_GROUP so a single resource
        row represents the group's mailbox + SharePoint site + (optional) Team —
        matching afi.ai's UX. Distribution Lists and security groups stay as
        ENTRA_GROUP rows but carry a `group_classification` so the UI can label
        them and backup handlers can decide what to fetch.
        """
        result = await self._get(
            f"{self.GRAPH_URL}/groups",
            params={
                "$top": "999",
                "$count": "true",
                # Pull resourceProvisioningOptions so we know if a Unified group
                # has a Team attached (caller can skip Team-scan if absent).
                "$select": "id,displayName,mail,mailEnabled,securityEnabled,groupTypes,description,resourceProvisioningOptions,visibility,createdDateTime",
            },
        )
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))

        groups = []
        counts: Dict[str, int] = {}
        for g in all_value:
            classification = self._classify_group(g)
            counts[classification] = counts.get(classification, 0) + 1
            provisioning = [p.lower() for p in (g.get("resourceProvisioningOptions") or [])]
            metadata = {
                "mail_enabled": g.get("mailEnabled"),
                "security_enabled": g.get("securityEnabled"),
                "group_types": g.get("groupTypes", []),
                "description": g.get("description"),
                "visibility": g.get("visibility"),
                "created_at": g.get("createdDateTime"),
                "group_classification": classification,
                "has_team": "team" in provisioning,
                "resource_provisioning_options": g.get("resourceProvisioningOptions") or [],
            }
            groups.append({
                "external_id": g.get("id"),
                "display_name": g.get("displayName", "Unknown"),
                "email": g.get("mail"),
                # Unified → first-class M365_GROUP row; everything else stays as
                # ENTRA_GROUP and the classification metadata distinguishes them.
                "type": "M365_GROUP" if classification == "M365_GROUP" else "ENTRA_GROUP",
                "metadata": metadata,
            })
        if counts:
            summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
            print(f"[GraphClient] discover_groups: {summary}")
        return groups
    
    # ------------------------------------------------------------------
    # Mailbox discovery — simple & direct:
    #   1. Fetch all users
    #   2. Enrich each with mailboxSettings.userPurpose
    #   3. Build resource records based on userPurpose value
    # ------------------------------------------------------------------

    async def discover_mailboxes(self) -> List[Dict[str, Any]]:
        """
        Discover all mailboxes by enriching users with userPurpose.

        userPurpose → resource type mapping:
          "user"      → MAILBOX
          "shared"    → SHARED_MAILBOX
          "room"      → ROOM_MAILBOX
          "equipment" → ROOM_MAILBOX
          None/other  → skipped (no mailbox)
        """
        # Step 1: Fetch all users (no $filter — get everyone). Add userType so we
        # can drop guests; afi.ai never backs up guest mailboxes (they live in their
        # home tenant).
        users_result = await self._get(
            f"{self.GRAPH_URL}/users",
            params={"$top": "999", "$count": "true",
                    "$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled,createdDateTime,userType"},
        )
        all_users_raw = users_result.get("value", [])
        while "@odata.nextLink" in users_result:
            users_result = await self._get(users_result["@odata.nextLink"])
            all_users_raw.extend(users_result.get("value", []))
        # Drop guests + system mailboxes BEFORE the per-user mailboxSettings round-trip
        # to avoid wasted API calls on tenant infrastructure accounts.
        all_users = []
        skipped_guest = 0
        skipped_system = 0
        for u in all_users_raw:
            if (u.get("userType") or "").lower() == "guest":
                skipped_guest += 1
                continue
            if self._is_system_mailbox(u.get("displayName"), u.get("userPrincipalName")):
                skipped_system += 1
                continue
            all_users.append(u)
        if skipped_guest or skipped_system:
            print(f"[GraphClient] discover_mailboxes: skipped {skipped_guest} guest(s), {skipped_system} system account(s) before enrichment")
        mailboxes = []

        # Step 2: Enrich each user with userPurpose
        semaphore = asyncio.Semaphore(10)

        async def _enrich_one_user(user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            async with semaphore:
                email = user.get("mail")
                if not email:
                    return None

                # Try mailboxSettings.userPurpose first — gives us the precise
                # mailbox type (user / shared / room / equipment). Common cause
                # of failure: app lacks MailboxSettings.Read.All. We fall back
                # to a /messages probe in that case so a missing scope doesn't
                # silently drop every mailbox in the tenant.
                purpose: Optional[str] = None
                try:
                    result = await self._get(
                        f"{self.GRAPH_URL}/users/{user['id']}/mailboxSettings",
                        params={"$select": "userPurpose"},
                    )
                    purpose = result.get("userPurpose") if result else None
                except Exception:
                    purpose = None

                if purpose == "user":
                    rtype = "MAILBOX"
                elif purpose == "shared":
                    rtype = "SHARED_MAILBOX"
                elif purpose in ("room", "equipment"):
                    rtype = "ROOM_MAILBOX"
                else:
                    # Fallback: probe /messages directly. This is the actual
                    # endpoint backup_mailbox uses, so success here proves the
                    # mailbox is backup-able regardless of the userPurpose
                    # field's accessibility. Cost: one extra HEAD-style GET
                    # per user-without-purpose, which is acceptable for the
                    # ~1x/discovery-cycle frequency.
                    try:
                        probe = await self._get(
                            f"{self.GRAPH_URL}/users/{user['id']}/messages",
                            params={"$top": "1", "$select": "id"},
                        )
                        if probe and "value" in probe:
                            # Mailbox reachable. Without userPurpose we can't
                            # distinguish user vs shared vs room — assume MAILBOX
                            # (user) since that's the dominant case. UI can let
                            # users reclassify as needed.
                            rtype = "MAILBOX"
                            purpose = "user (probed)"
                        else:
                            return None
                    except Exception:
                        return None  # truly no mailbox

                print(f"[GraphClient] {email} → userPurpose={purpose} → {rtype}")

                return {
                    "external_id": user.get("id"),
                    "display_name": user.get("displayName", email),
                    "email": email,
                    "type": rtype,
                    "metadata": {
                        "user_principal_name": user.get("userPrincipalName"),
                        "job_title": user.get("jobTitle"),
                        "department": user.get("department"),
                        "account_enabled": user.get("accountEnabled", True),
                        "created_at": user.get("createdDateTime"),
                        "mailbox_purpose": purpose,
                    },
                    "_account_enabled": user.get("accountEnabled", True),  # For discovery worker
                }

        tasks = [_enrich_one_user(u) for u in all_users]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        for r in results:
            if r:
                mailboxes.append(r)

        print(f"[GraphClient] discover_mailboxes: found {len(mailboxes)} mailboxes "
              f"({[m['type'] for m in mailboxes]})")
        return mailboxes

    async def discover_onedrive(self) -> List[Dict[str, Any]]:
        """Discover OneDrive sites for all users in parallel (bounded by Semaphore(10)).

        Previously serial — one GET /users/{id}/drive per user awaited in a loop —
        which turned into ~N × round-trip-latency wall time for tenants with many
        users. Matches the pattern used by discover_mailboxes and discover_teams."""
        users = await self.discover_users()
        semaphore = asyncio.Semaphore(10)

        async def _fetch_drive(u: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            if not u.get("email"):
                return None
            user_id = u["external_id"]
            async with semaphore:
                try:
                    drive_result = await self._get(f"{self.GRAPH_URL}/users/{user_id}/drive")
                except Exception as e:
                    msg = str(e)
                    if "404" in msg or "423" in msg:
                        # Not found / locked — discovery worker will stale-mark later
                        return None
                    print(f"Error discovering OneDrive for user {u.get('email')}: {e}")
                    return None

            if not drive_result or not drive_result.get("id"):
                return None
            return {
                "external_id": drive_result["id"],
                "display_name": drive_result.get("name", f"OneDrive - {u['display_name']}"),
                "email": u["email"],
                "type": "ONEDRIVE",
                "metadata": {
                    "user_id": user_id,
                    "user_email": u["email"],
                    "drive_id": drive_result["id"],
                    "web_url": drive_result.get("webUrl"),
                    "quota": drive_result.get("quota", {}),
                },
                "_account_enabled": u.get("_account_enabled", True),
            }

        results = await asyncio.gather(
            *[_fetch_drive(u) for u in users],
            return_exceptions=True,
        )
        drives: List[Dict[str, Any]] = []
        for r in results:
            if isinstance(r, dict):
                drives.append(r)
        return drives
    
    async def discover_sharepoint(self) -> List[Dict[str, Any]]:
        """Discover SharePoint sites"""
        result = await self._get(
            f"{self.GRAPH_URL}/sites",
            params={"$search": '"contentclass:STS_Site" AND NOT "contentclass:STS_MySite"', "$top": "999"}
        )
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))

        sites = []
        for site in all_value:
            sites.append({
                "external_id": site.get("id", "").replace(",", "/"),
                "display_name": site.get("displayName") or site.get("name", "Unknown Site"),
                "email": None,
                "type": "SHAREPOINT_SITE",
                "metadata": {
                    "web_url": site.get("webUrl"),
                    "site_collection": site.get("siteCollection", {}),
                },
            })
        return sites
    
    async def discover_teams(self) -> List[Dict[str, Any]]:
        """Discover Teams groups (for channels) and all chats (1-on-1 and group)"""
        resources = []

        # 1. Discover Teams groups (for channel backups)
        result = await self._get(
            f"{self.GRAPH_URL}/groups",
            params={"$filter": "resourceProvisioningOptions/Any(x:x eq 'Team')", "$top": "999"}
        )
        all_teams = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_teams.extend(result.get("value", []))

        for g in all_teams:
            resources.append({
                "external_id": g.get("id"),
                "display_name": g.get("displayName", "Unknown Team"),
                "email": g.get("mail"),
                "type": "TEAMS_CHANNEL",
                "metadata": {
                    "description": g.get("description"),
                    "mail_enabled": g.get("mailEnabled"),
                    "visibility": g.get("visibility"),
                },
            })

        # 2. Discover all chats (1-on-1 and group chats)
        # Note: GET /chats (global) does NOT support app-only auth.
        # We use GET /users/{id}/chats per-user, which DOES support app-only with Chat.Read.All.
        try:
            import time

            # Fetch all users first
            users_result = await self._get(
                f"{self.GRAPH_URL}/users",
                params={"$top": "999", "$select": "id,userPrincipalName,displayName"}
            )
            all_users = users_result.get("value", [])
            # Follow pagination
            while users_result.get("@odata.nextLink"):
                users_result = await self._get(users_result["@odata.nextLink"])
                all_users.extend(users_result.get("value", []))

            _chat_semaphore = asyncio.Semaphore(10)  # Max 10 concurrent Graph API calls

            async def _fetch_chat_members(chat_id: str) -> tuple:
                """Fetch members for a single chat (with semaphore)."""
                async with _chat_semaphore:
                    try:
                        members_result = await self._get(
                            f"{self.GRAPH_URL}/chats/{chat_id}/members"
                        )
                        emails = []
                        names = []
                        for m in members_result.get("value", []):
                            email = m.get("email")
                            display_name = m.get("displayName")
                            if email:
                                emails.append(email)
                            if display_name:
                                names.append(display_name)
                        return emails, names
                    except Exception:
                        return [], []

            def _build_chat_resource(chat: Dict) -> Dict:
                """Build a TEAMS_CHAT resource dict from a chat object."""
                chat_id = chat.get("id")
                chat_type = chat.get("chatType", "unknown")
                topic = chat.get("topic")
                return {
                    "chat_id": chat_id,
                    "chat_type": chat_type,
                    "topic": topic,
                    "createdDateTime": chat.get("createdDateTime"),
                    "lastUpdatedDateTime": chat.get("lastUpdatedDateTime"),
                }

            async def _process_user_chats(user: Dict):
                """Fetch and process all chats for a single user."""
                user_id = user.get("id")
                if not user_id:
                    return []

                user_chats_raw = []
                try:
                    async with _chat_semaphore:
                        chats_result = await self._get(
                            f"{self.GRAPH_URL}/users/{user_id}/chats",
                            params={"$top": "999"}
                        )
                    user_chats_raw.extend(chats_result.get("value", []))

                    # Follow pagination
                    while chats_result.get("@odata.nextLink"):
                        async with _chat_semaphore:
                            chats_result = await self._get(chats_result["@odata.nextLink"])
                        user_chats_raw.extend(chats_result.get("value", []))
                except Exception:
                    pass  # Skip users where we can't access chats

                return user_chats_raw

            # Phase 1: Fetch all user chats in parallel (bounded concurrency)
            logger.info("Discovering Teams chats for %d users...", len(all_users))
            start_time = time.time()
            user_chat_tasks = [_process_user_chats(u) for u in all_users]
            user_chat_results = await asyncio.gather(*user_chat_tasks, return_exceptions=True)

            # Collect all unique chats
            all_chats: Dict[str, Dict] = {}  # chat_id -> chat object
            for result in user_chat_results:
                if isinstance(result, Exception):
                    continue
                for chat in result:
                    chat_id = chat.get("id")
                    if chat_id and chat_id not in all_chats:
                        all_chats[chat_id] = chat

            elapsed1 = time.time() - start_time
            logger.info("Found %d unique chats across users in %.1fs", len(all_chats), elapsed1)

            # Phase 2: Fetch members for all chats in parallel (bounded concurrency)
            start_time2 = time.time()
            all_chat_ids = list(all_chats.keys())
            member_tasks = [_fetch_chat_members(cid) for cid in all_chat_ids]
            member_results = await asyncio.gather(*member_tasks, return_exceptions=True)

            chat_members: Dict[str, tuple] = {}
            for i, chat_id in enumerate(all_chat_ids):
                result = member_results[i]
                if isinstance(result, Exception):
                    chat_members[chat_id] = ([], [])
                else:
                    chat_members[chat_id] = result

            elapsed2 = time.time() - start_time2
            logger.info("Fetched members for %d chats in %.1fs", len(all_chats), elapsed2)

            # Phase 3: Build resource dicts (CPU-bound, no network)
            for chat_id, chat in all_chats.items():
                chat_type = chat.get("chatType", "unknown")
                topic = chat.get("topic")
                member_emails, member_names = chat_members.get(chat_id, ([], []))

                # Build display name
                if chat_type == "oneOnOne":
                    if topic:
                        display_name = topic
                    elif member_names:
                        display_name = " | ".join(member_names)
                    else:
                        display_name = f"1-on-1 Chat ({chat_id[:8]})"
                else:
                    if topic:
                        display_name = topic
                    elif member_names:
                        display_name = f"Group: {', '.join(member_names[:3])}"
                        if len(member_names) > 3:
                            display_name += f" +{len(member_names) - 3} more"
                    else:
                        display_name = f"Group Chat ({chat_id[:8]})"

                resources.append({
                    "external_id": chat_id,
                    "display_name": display_name,
                    "email": None,
                    "type": "TEAMS_CHAT",
                    "metadata": {
                        "chatType": chat_type,
                        "topic": topic,
                        "memberCount": len(member_names),
                        "memberEmails": member_emails,
                        "memberNames": member_names,
                        "createdDateTime": chat.get("createdDateTime"),
                        "lastUpdatedDateTime": chat.get("lastUpdatedDateTime"),
                    },
                })

            total_elapsed = time.time() - start_time
            logger.info("Teams chat discovery complete: %d chats in %.1fs", len(all_chats), total_elapsed)

            # Phase 4: emit per-user TEAMS_CHAT_EXPORT shards.
            # One resource per Graph user who has any chats — the backup worker
            # issues a single /users/{id}/chats/getAllMessages/delta call per
            # shard instead of one call per chat (Graph caps $top=50, so a heavy
            # user was previously stuck paying that full-export cost once per
            # chat job). Delta token lives on this row's extra_data.
            for user, user_chats in zip(all_users, user_chat_results):
                if isinstance(user_chats, Exception):
                    continue
                chat_ids = [c.get("id") for c in user_chats if c.get("id")]
                if not chat_ids:
                    continue
                user_id = user.get("id")
                if not user_id:
                    continue
                display = user.get("displayName") or user.get("userPrincipalName") or user_id
                resources.append({
                    "external_id": user_id,
                    "display_name": f"Chat export — {display}",
                    "email": user.get("userPrincipalName"),
                    "type": "TEAMS_CHAT_EXPORT",
                    "metadata": {
                        "userPrincipalName": user.get("userPrincipalName"),
                        "userDisplayName": user.get("displayName"),
                        "chatIds": chat_ids,
                        "chatCount": len(chat_ids),
                    },
                })
            logger.info("Teams chat-export shards emitted: %d users", sum(
                1 for r in resources if r.get("type") == "TEAMS_CHAT_EXPORT"
            ))

        except Exception as e:
            logger.warning(f"Failed to discover Teams chats: {e}")

        return resources

    async def discover_power_platform(self) -> List[Dict[str, Any]]:
        """Discover Power Platform resources via PowerPlatformClient (correct audience).

        Previously this code hand-rolled HTTP against api.bap.microsoft.com using
        a Graph-scoped token, which Microsoft rejects with 401
        InvalidAuthenticationAudience (the BAP admin endpoints require a
        service.powerapps.com-scoped token). PowerPlatformClient gets the right
        token audience, so environments / apps / flows / DLP policies actually
        come back here.

        Power BI is orthogonal and still uses PowerBIClient (different REST
        surface, different scope)."""
        from shared.power_platform_client import PowerPlatformClient
        resources = []

        pp = PowerPlatformClient(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
        )

        # 1. Environments
        try:
            envs_data = await pp.list_environments()
            environments = envs_data.get("value", []) if isinstance(envs_data, dict) else []
        except Exception as e:
            print(f"[discover_power_platform] list_environments failed: {e}")
            environments = []

        for env in environments:
            env_id = env.get("name")
            env_props = env.get("properties", {})
            env_name = env_props.get("displayName", env_id)
            env_type = env_props.get("environmentType", "Unknown")
            env_region = (env_props.get("location", {}) or {}).get("name", "Unknown") \
                if isinstance(env_props.get("location"), dict) else env_props.get("location", "Unknown")
            has_dataverse = (env_props.get("linkedEnvironmentMetadata", {}) or {}).get("CommonDataService") is not None

            # Environment itself as a POWER_APPS row (external_id prefixed "env_"
            # so the Recovery RestoreModal can distinguish environments from apps)
            resources.append({
                "external_id": f"env_{env_id}",
                "display_name": f"{env_name} (Environment)",
                "email": None,
                "type": "POWER_APPS",
                "metadata": {
                    "environment_id": env_id,
                    "environment_type": env_type,
                    "region": env_region,
                    "has_dataverse": has_dataverse,
                    "created_time": env_props.get("createdTime"),
                },
            })

            # 2. Power Apps in this environment
            try:
                apps_data = await pp.list_apps(env_id)
                for app in (apps_data.get("value", []) if isinstance(apps_data, dict) else []):
                    app_props = app.get("properties", {})
                    resources.append({
                        "external_id": f"app_{app.get('id', app.get('name'))}",
                        "display_name": app_props.get("displayName", app.get("name", "Unknown App")),
                        "email": None,
                        "type": "POWER_APPS",
                        "metadata": {
                            "app_id": app.get("name"),
                            "environment_id": env_id,
                            "environment_name": env_name,
                            "app_type": app_props.get("appType"),
                            "created_by": (app_props.get("createdBy", {}) or {}).get("displayName"),
                            "created_time": app_props.get("createdTime"),
                            "modified_time": app_props.get("lastModifiedTime"),
                        },
                    })
            except Exception as e:
                print(f"[discover_power_platform] list_apps failed for env {env_id}: {e}")

            # 3. Power Automate flows in this environment
            try:
                flows_data = await pp.list_flows(env_id)
                for flow in (flows_data.get("value", []) if isinstance(flows_data, dict) else []):
                    flow_props = flow.get("properties", {})
                    resources.append({
                        "external_id": f"flow_{flow.get('id', flow.get('name'))}",
                        "display_name": flow_props.get("displayName", flow.get("name", "Unknown Flow")),
                        "email": None,
                        "type": "POWER_AUTOMATE",
                        "metadata": {
                            "flow_id": flow.get("name"),
                            "environment_id": env_id,
                            "environment_name": env_name,
                            "state": flow_props.get("state"),
                            "created_by": (flow_props.get("createdBy", {}) or {}).get("displayName"),
                            "created_time": flow_props.get("createdTime"),
                            "modified_time": flow_props.get("lastModifiedTime"),
                        },
                    })
            except Exception as e:
                print(f"[discover_power_platform] list_flows failed for env {env_id}: {e}")

        # 4. Tenant-level DLP policies
        try:
            dlp_data = await pp.list_dlp_policies()
            for policy in (dlp_data.get("value", []) if isinstance(dlp_data, dict) else []):
                policy_id = policy.get("name") or policy.get("id")
                policy_props = policy.get("properties", {})
                if not policy_id:
                    continue
                resources.append({
                    "external_id": f"dlp_{policy_id}",
                    "display_name": policy_props.get("displayName", policy_id),
                    "email": None,
                    "type": "POWER_DLP",
                    "metadata": {
                        "policy_id": policy_id,
                        "policy_type": policy_props.get("policyType"),
                        "environment_type": policy_props.get("environmentType"),
                        "created_time": policy_props.get("createdTime"),
                        "modified_time": policy_props.get("lastModifiedTime"),
                    },
                })
        except Exception as e:
            print(f"[discover_power_platform] list_dlp_policies failed: {e}")

        # 5. Discover Power BI workspaces via Power BI REST API
        try:
            power_bi_client = PowerBIClient(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.power_bi_refresh_token,
            )
            workspaces = await power_bi_client.list_workspaces()
            self.power_bi_refresh_token = power_bi_client.refresh_token
            for workspace in workspaces:
                workspace_id = workspace.get("id")
                if not workspace_id:
                    continue
                resources.append({
                    "external_id": f"pbi_ws_{workspace_id}",
                    "display_name": workspace.get("name", "Unknown Power BI Workspace"),
                    "email": None,
                    "type": "POWER_BI",
                    "metadata": {
                        "workspace_id": workspace_id,
                        "workspace_type": workspace.get("type"),
                        "is_on_dedicated_capacity": workspace.get("isOnDedicatedCapacity"),
                        "capacity_id": workspace.get("capacityId"),
                        "description": workspace.get("description"),
                        "state": workspace.get("state"),
                        "default_dataset_storage_format": workspace.get("defaultDatasetStorageFormat"),
                    },
                })
        except Exception as e:
            print(f"Error discovering Power BI workspaces: {e}")

        return resources

    async def discover_planner(self) -> List[Dict[str, Any]]:
        """Discover Planner-capable group containers that actually have plans."""
        resources = []
        groups = await self.discover_groups()
        semaphore = asyncio.Semaphore(8)

        async def _probe_group(group: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            group_id = group.get("external_id")
            if not group_id:
                return None

            metadata = group.get("metadata") or {}
            if not (
                metadata.get("mail_enabled")
                or "Unified" in (metadata.get("group_types") or [])
            ):
                return None

            async with semaphore:
                try:
                    plans = await self.get_planner_plans_for_group(group_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in (403, 404):
                        return None
                    raise
                except Exception:
                    return None

            plan_list = plans.get("value", [])
            if not plan_list:
                return None

            return {
                "external_id": group_id,
                "display_name": f"{group.get('display_name', 'Unknown')} Planner",
                "email": group.get("email"),
                "type": "PLANNER",
                "metadata": {
                    "group_id": group_id,
                    "group_display_name": group.get("display_name"),
                    "group_email": group.get("email"),
                    "plan_count": len(plan_list),
                    "plan_ids": [plan.get("id") for plan in plan_list if plan.get("id")],
                },
            }

        results = await asyncio.gather(
            *[_probe_group(group) for group in groups],
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, dict):
                resources.append(result)
        return resources

    async def discover_todo(self) -> List[Dict[str, Any]]:
        """Discover users whose To Do workload is accessible."""
        resources = []
        users = await self.discover_users()
        semaphore = asyncio.Semaphore(10)

        async def _probe_user(user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            user_id = user.get("external_id")
            if not user_id:
                return None

            async with semaphore:
                try:
                    lists = await self.get_user_todo_lists(user_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in (403, 404):
                        return None
                    raise
                except Exception:
                    return None

            list_items = lists.get("value", [])
            if not list_items:
                return None

            return {
                "external_id": user_id,
                "display_name": f"{user.get('display_name', 'Unknown')} To Do",
                "email": user.get("email"),
                "type": "TODO",
                "metadata": {
                    "user_id": user_id,
                    "user_email": user.get("email"),
                    "list_count": len(list_items),
                    "wellknown_lists": [
                        item.get("wellknownListName")
                        for item in list_items
                        if item.get("wellknownListName")
                    ],
                },
                "_account_enabled": user.get("_account_enabled", True),
            }

        results = await asyncio.gather(
            *[_probe_user(user) for user in users],
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, dict):
                resources.append(result)
        return resources
    
    async def discover_all(self) -> List[Dict[str, Any]]:
        """Run full discovery in PARALLEL and return all resources"""
        all_resources = []

        async def _safe_discover(name, coro):
            """Run a discovery coroutine safely, catching errors."""
            try:
                result = await coro
                return result
            except Exception as e:
                print(f"Error discovering {name}: {e}")
                return []

        # Run ALL discovery endpoints in parallel (Graph API, users, groups, mailboxes,
        # OneDrive, SharePoint, Teams chats/channels, Power Platform)
        tasks = [
            _safe_discover("users", self.discover_users()),
            _safe_discover("groups", self.discover_groups()),
            _safe_discover("mailboxes", self.discover_mailboxes()),
            _safe_discover("onedrive", self.discover_onedrive()),
            _safe_discover("sharepoint", self.discover_sharepoint()),
            _safe_discover("teams", self.discover_teams()),
            _safe_discover("planner", self.discover_planner()),
            _safe_discover("todo", self.discover_todo()),
            _safe_discover("power_platform", self.discover_power_platform()),
            # Phase 2 P2 — security-critical Entra extras
            _safe_discover("conditional_access", self.discover_conditional_access()),
            _safe_discover("bitlocker", self.discover_bitlocker_keys()),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                print(f"Discovery task failed: {result}")
            elif isinstance(result, list):
                all_resources.extend(result)

        # Deduplicate: same external_id AND same type
        seen = set()
        unique = []
        for r in all_resources:
            key = f"{r.get('external_id')}:{r.get('type')}"
            if key and key not in seen:
                seen.add(key)
                unique.append(r)

        return unique

    # ── Conditional Access policies ─────────────────────────────────────────
    # Tenant-singleton resources — small in number, high in security value.
    # afi backs these up so a misconfiguration or tenant-takeover incident can
    # be reverted by re-applying the captured definitions.

    async def discover_conditional_access(self) -> List[Dict[str, Any]]:
        """List all CA policies as discovery rows. Each row's external_id is the
        policy ID; full definition lives in metadata so the backup handler can
        re-dump it without a second round-trip."""
        url = f"{self.GRAPH_URL}/identity/conditionalAccess/policies"
        try:
            result = await self._get(url, params={"$top": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                # Tenant lacks Policy.Read.All or doesn't have Entra ID P1+
                print(f"[GraphClient] CA policies inaccessible (HTTP {e.response.status_code}) — skipping")
                return []
            raise
        all_value = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        rows = []
        for p in all_value:
            rows.append({
                "external_id": p.get("id"),
                "display_name": p.get("displayName") or "(unnamed CA policy)",
                "email": None,
                "type": "ENTRA_CONDITIONAL_ACCESS",
                "metadata": {
                    "state": p.get("state"),
                    "created_at": p.get("createdDateTime"),
                    "modified_at": p.get("modifiedDateTime"),
                    "raw": p,  # full definition cached for backup handler
                },
            })
        return rows

    async def get_conditional_access_policy(self, policy_id: str) -> Optional[Dict[str, Any]]:
        """Re-fetch a single CA policy by ID — used by the backup handler when
        the cached metadata is stale or missing."""
        url = f"{self.GRAPH_URL}/identity/conditionalAccess/policies/{policy_id}"
        try:
            return await self._get(url)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    # ── BitLocker recovery keys ─────────────────────────────────────────────
    # The list endpoint returns key metadata (id, deviceId, createdDateTime)
    # WITHOUT the key value. Reading the key value requires a separate GET to
    # /informationProtection/bitlocker/recoveryKeys/{id}?$select=key — and the
    # caller must have BitlockerKey.Read.All. We capture metadata at discovery
    # and pull the key bytes during backup so a least-privileged discovery
    # token still works.

    async def discover_bitlocker_keys(self) -> List[Dict[str, Any]]:
        """List BitLocker recovery key metadata across the tenant."""
        url = f"{self.GRAPH_URL}/informationProtection/bitlocker/recoveryKeys"
        try:
            result = await self._get(url, params={"$top": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403, 404):
                print(f"[GraphClient] BitLocker keys inaccessible (HTTP {e.response.status_code}) — skipping")
                return []
            raise
        all_value = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        rows = []
        for k in all_value:
            kid = k.get("id")
            device_id = k.get("deviceId")
            volume_type = k.get("volumeType")
            rows.append({
                "external_id": kid,
                "display_name": f"BitLocker key — device {device_id} ({volume_type})" if device_id else (kid or "BitLocker key"),
                "email": None,
                "type": "ENTRA_BITLOCKER_KEY",
                "metadata": {
                    "device_id": device_id,
                    "volume_type": volume_type,
                    "created_at": k.get("createdDateTime"),
                },
            })
        return rows

    async def get_bitlocker_key_value(self, key_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the actual recovery key bytes for a single BitLocker entry.
        Requires BitlockerKey.Read.All — separate from the metadata-only
        BitlockerKey.ReadBasic.All used by the list endpoint."""
        url = f"{self.GRAPH_URL}/informationProtection/bitlocker/recoveryKeys/{key_id}"
        try:
            # $select=key promotes the actual recovery key into the response
            return await self._get(url, params={"$select": "id,createdDateTime,deviceId,volumeType,key"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return None
            raise

    async def get_directory_audit_logs(self, filter_expr: str = None, top: int = 100) -> List[Dict[str, Any]]:
        """
        Get Microsoft Entra directory audit logs.
        Graph API: GET /auditLogs/directoryAudits
        Permission: AuditLog.Read.All
        """
        params = {"$top": min(top, 999)}
        if filter_expr:
            params["$filter"] = filter_expr

        return await self._paginated_get("/auditLogs/directoryAudits", params=params)

    async def get_sign_in_logs(self, filter_expr: str = None, top: int = 100) -> List[Dict[str, Any]]:
        """
        Get sign-in logs.
        Graph API: GET /auditLogs/signIns
        Permission: AuditLog.Read.All
        """
        params = {"$top": min(top, 999)}
        if filter_expr:
            params["$filter"] = filter_expr

        return await self._paginated_get("/auditLogs/signIns", params=params)

    # ==================== Backup-Specific Graph API Methods ====================

    async def get_sharepoint_site_drives(self, site_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get drive items from a SharePoint site using delta API.
        Graph API: GET /sites/{site-id}/drive/root/delta
        site_id format in DB: hostname/site-collection-id/site-id
        Graph API requires: hostname,site-collection-id,site-id
        """
        # Convert slashes to commas for Graph API
        graph_site_id = site_id.replace("/", ",")
        url = f"{self.GRAPH_URL}/sites/{graph_site_id}/drive/root/delta"
        if delta_token:
            url = delta_token

        # No $select or $expand — delta endpoint ignores them
        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_sharepoint_subsites(self, site_id: str) -> Dict[str, Any]:
        """
        Get subsites for a SharePoint site.
        Graph API: GET /sites/{site-id}/sites
        """
        graph_site_id = site_id.replace("/", ",")
        result = await self._get(f"{self.GRAPH_URL}/sites/{graph_site_id}/sites", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_sharepoint_site_lists(self, site_id: str) -> Dict[str, Any]:
        """
        Get SharePoint site lists.
        Graph API: GET /sites/{site-id}/lists
        """
        return await self._get(f"{self.GRAPH_URL}/sites/{site_id}/lists", params={"$top": "999"})

    async def get_sharepoint_site_list_items(self, site_id: str, list_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get items from a SharePoint list using delta API.
        Graph API: GET /sites/{site-id}/lists/{list-id}/items/delta
        """
        url = f"{self.GRAPH_URL}/sites/{site_id}/lists/{list_id}/items/delta"
        if delta_token:
            url = delta_token

        params = {"$expand": "fields", "$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_site_permissions(self, site_id: str) -> Dict[str, Any]:
        """
        Get SharePoint site permissions.
        Graph API: GET /sites/{site-id}/permissions
        """
        return await self._get(f"{self.GRAPH_URL}/sites/{site_id}/permissions")

    async def get_teams_channels(self, team_id: str) -> Dict[str, Any]:
        """
        Get channels in a Teams team.
        Graph API: GET /teams/{team-id}/channels
        """
        result = await self._get(f"{self.GRAPH_URL}/teams/{team_id}/channels", params={"$top": "999"})
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_channel_messages(self, team_id: str, channel_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get messages from a Teams channel using delta API.
        Graph API: GET /teams/{team-id}/channels/{channel-id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/teams/{team_id}/channels/{channel_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_channel_messages_replies(self, team_id: str, channel_id: str, message_id: str) -> Dict[str, Any]:
        """
        Get replies to a Teams channel message.
        Graph API: GET /teams/{team-id}/channels/{channel-id}/messages/{message-id}/replies
        """
        return await self._get(
            f"{self.GRAPH_URL}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies",
            params={"$top": "999"}
        )

    async def get_teams_chats(self, delta_token: str = None) -> Dict[str, Any]:
        """Get all Teams chats accessible to the app.

        /chats/delta is NOT in the v1.0 Graph reference (was previously called here
        but never documented). We now scope by user: /users/{id}/chats. For
        organization-wide chat export the documented approach is
        /users/{id}/chats/getAllMessages.

        Kept API-compatible: callers may still pass delta_token from a previous
        nextLink response and it'll be used verbatim."""
        if delta_token:
            url = delta_token
        else:
            url = f"{self.GRAPH_URL}/chats"
        params = {"$top": "999", "$expand": "members,permission"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    # Whitelisted chatMessage fields for getAllMessages (delta + non-delta).
    # Drops channelIdentity (always null for chat messages — it's a channel-only
    # field), eventDetail (system/control messages — rarely consumed), and
    # policyViolation (moderation flags — almost always null). Cuts payload by
    # ~20-30% on a typical delta page without losing anything a restore needs.
    # Graph carries $select through into nextLink/deltaLink automatically.
    _CHAT_MESSAGE_SELECT = (
        "id,chatId,replyToId,messageType,createdDateTime,lastModifiedDateTime,"
        "deletedDateTime,subject,body,summary,importance,from,attachments,"
        "mentions,reactions,locale,webUrl"
    )

    async def get_all_chat_messages_for_user(self, user_id: str) -> Dict[str, Any]:
        """Export all chat messages a user is part of.

        Graph API: GET /users/{id}/chats/getAllMessages
        Permission: Chat.Read.All (or ChatMessage.Read.All). This is the documented
        replacement for the undocumented /chats/delta used previously."""
        url = f"{self.GRAPH_URL}/users/{user_id}/chats/getAllMessages"
        params = {"$top": "50", "$select": self._CHAT_MESSAGE_SELECT}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_all_chat_messages_for_user_delta(
        self, user_id: str, delta_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Incremental chat export via delta query.

        Graph API: GET /users/{id}/chats/getAllMessages/delta
        Permission: Chat.Read.All (same as the non-delta endpoint).

        First call (no delta_token): full sync, returns every message and an
        @odata.deltaLink. Subsequent calls (delta_token = previous deltaLink):
        only messages added/changed since the last sync.

        Hard limit: delta only covers the last 8 months. If the token is
        expired or too old, Graph returns 410/400 and callers should fall
        back to get_all_chat_messages_for_user() for a full reseed.
        """
        if delta_token:
            # A deltaLink IS the full URL — use it verbatim, no extra params.
            # Graph bakes the original $select into deltaLink/nextLink, so the
            # field projection is preserved across incremental syncs.
            url = delta_token
            params = None
        else:
            url = f"{self.GRAPH_URL}/users/{user_id}/chats/getAllMessages/delta"
            params = {"$top": "50", "$select": self._CHAT_MESSAGE_SELECT}
        # _get already paginates via @odata.nextLink and preserves @odata.deltaLink.
        return await self._get(url, params=params)

    async def iter_all_chat_messages_for_user_delta(
        self, user_id: str, delta_token: Optional[str] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Streaming counterpart to get_all_chat_messages_for_user_delta.

        Yields one page dict at a time ({"value": [...], "@odata.deltaLink"?, ...}),
        letting callers pack/flush incrementally instead of buffering every message
        in RAM before doing any work. Rotates app-registration tokens per page
        when multi_app_manager has more than one app configured.
        """
        if delta_token:
            url = delta_token
            params = None
        else:
            url = f"{self.GRAPH_URL}/users/{user_id}/chats/getAllMessages/delta"
            params = {"$top": "50", "$select": self._CHAT_MESSAGE_SELECT}
        async for page in self._iter_pages(url, params=params):
            yield page

    async def get_chat_messages(self, chat_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get messages from a Teams chat.
        Note: /messages/delta is NOT supported for chat messages (MS Graph limitation).
        Graph API: GET /chats/{chat-id}/messages
        """
        url = f"{self.GRAPH_URL}/chats/{chat_id}/messages"
        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_group_profile(self, group_id: str) -> Dict[str, Any]:
        """
        Get Entra ID group profile.
        Graph API: GET /groups/{id}
        """
        return await self._get(f"{self.GRAPH_URL}/groups/{group_id}")

    async def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """
        Get detailed user profile.
        Graph API: GET /users/{id}
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}")

    async def get_user_manager(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's manager.
        Graph API: GET /users/{id}/manager
        """
        try:
            return await self._get(f"{self.GRAPH_URL}/users/{user_id}/manager")
        except Exception:
            return {}

    async def get_user_direct_reports(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's direct reports.
        Graph API: GET /users/{id}/directReports
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/directReports", params={"$top": "999"})

    async def get_user_group_memberships(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's group memberships.
        Graph API: GET /users/{id}/memberOf
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/memberOf", params={"$top": "999"})

    async def get_group_members(self, group_id: str) -> Dict[str, Any]:
        """
        Get group members.
        Graph API: GET /groups/{id}/members
        """
        return await self._get(f"{self.GRAPH_URL}/groups/{group_id}/members", params={"$top": "999"})

    async def get_group_owners(self, group_id: str) -> Dict[str, Any]:
        """
        Get group owners.
        Graph API: GET /groups/{id}/owners
        """
        return await self._get(f"{self.GRAPH_URL}/groups/{group_id}/owners", params={"$top": "999"})

    async def get_entra_apps(self) -> Dict[str, Any]:
        """
        Get Entra ID application registrations.
        Graph API: GET /applications
        """
        result = await self._get(f"{self.GRAPH_URL}/applications", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_entra_service_principals(self) -> Dict[str, Any]:
        """
        Get service principals.
        Graph API: GET /servicePrincipals
        """
        result = await self._get(f"{self.GRAPH_URL}/servicePrincipals", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_entra_devices(self) -> Dict[str, Any]:
        """
        Get registered devices.
        Graph API: GET /devices
        """
        result = await self._get(f"{self.GRAPH_URL}/devices", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_user_mailbox_settings(self, user_id: str) -> Dict[str, Any]:
        """
        Get user mailbox settings.
        Graph API: GET /users/{id}/mailboxSettings
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/mailboxSettings")

    # ── Entra restore helpers ────────────────────────────────────────────────
    # Each follows PATCH-if-exists, POST-if-missing. Graph mints a new id on
    # POST, so restoring a hard-deleted object produces a new external id; the
    # caller is responsible for keeping resource.external_id in sync if desired.

    _ENTRA_APP_WRITE_FIELDS = (
        "displayName", "description", "signInAudience", "tags", "notes",
        "identifierUris", "api", "web", "spa", "publicClient",
        "requiredResourceAccess", "optionalClaims", "appRoles", "keyCredentials",
        "passwordCredentials",
    )
    _ENTRA_SP_WRITE_FIELDS = (
        "displayName", "description", "accountEnabled", "tags", "notes",
        "servicePrincipalType", "appRoleAssignmentRequired", "loginUrl",
        "logoutUrl", "homepage", "replyUrls", "preferredSingleSignOnMode",
    )
    _ENTRA_DEVICE_WRITE_FIELDS = (
        "displayName", "accountEnabled", "operatingSystem",
        "operatingSystemVersion", "profileType", "isManaged", "isCompliant",
    )
    _ENTRA_CA_WRITE_FIELDS = (
        "displayName", "state", "conditions", "grantControls", "sessionControls",
    )

    @staticmethod
    def _pick_fields(payload: Dict[str, Any], allowed: tuple) -> Dict[str, Any]:
        return {k: v for k, v in (payload or {}).items() if k in allowed and v is not None}

    async def restore_entra_app(self, app_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /applications/{id} if it exists, else POST /applications."""
        clean = self._pick_fields(payload, self._ENTRA_APP_WRITE_FIELDS)
        url = f"{self.GRAPH_URL}/applications/{app_id}"
        try:
            await self._get(url)
            return await self._patch(url, clean)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return await self._post(f"{self.GRAPH_URL}/applications", clean)
            raise

    async def restore_service_principal(self, sp_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /servicePrincipals/{id} if it exists, else POST /servicePrincipals.

        Creation requires an `appId` — if the SP was hard-deleted but the parent
        application still exists, Graph auto-provisions the SP by appId.
        """
        clean = self._pick_fields(payload, self._ENTRA_SP_WRITE_FIELDS)
        url = f"{self.GRAPH_URL}/servicePrincipals/{sp_id}"
        try:
            await self._get(url)
            return await self._patch(url, clean)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                app_id = (payload or {}).get("appId")
                if not app_id:
                    raise ValueError("Cannot recreate service principal without appId in backup payload") from e
                return await self._post(f"{self.GRAPH_URL}/servicePrincipals", {"appId": app_id, **clean})
            raise

    async def restore_entra_device(self, device_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /devices/{id}. Devices can't be created via Graph without MDM
        enrollment — if the device is hard-deleted, we report a skip rather
        than attempting a meaningless POST."""
        clean = self._pick_fields(payload, self._ENTRA_DEVICE_WRITE_FIELDS)
        url = f"{self.GRAPH_URL}/devices/{device_id}"
        try:
            await self._get(url)
            return await self._patch(url, clean)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ValueError(
                    "Device object no longer exists and cannot be re-created via Graph API "
                    "(device records are provisioned by MDM enrollment, not by tenant admins)."
                ) from e
            raise

    async def restore_conditional_access_policy(self, policy_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /identity/conditionalAccess/policies/{id} if it exists, else POST."""
        clean = self._pick_fields(payload, self._ENTRA_CA_WRITE_FIELDS)
        url = f"{self.GRAPH_URL}/identity/conditionalAccess/policies/{policy_id}"
        try:
            await self._get(url)
            return await self._patch(url, clean)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return await self._post(f"{self.GRAPH_URL}/identity/conditionalAccess/policies", clean)
            raise

    async def get_user_contacts(self, user_id: str) -> Dict[str, Any]:
        """
        Get user contacts.
        Graph API: GET /users/{id}/contacts
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/contacts", params={"$top": "999"})

    async def get_calendar_events_delta(self, user_id: str, delta_token: str = None) -> Dict[str, Any]:
        """Get calendar events using the documented delta API.

        Graph v1.0 documents /users/{id}/calendarView/delta (with startDateTime /
        endDateTime bounds); the previously-used /calendar/events/delta is not in
        the v1.0 reference — it may still respond today but isn't guaranteed.

        Window is 10 years back / 1 year forward by default, which covers almost
        every realistic retention need without paginating the full multi-decade
        history of recurring meetings."""
        if delta_token:
            # delta token contains the full next URL including the preserved window
            url = delta_token
            params = {"$top": "999"}
        else:
            url = f"{self.GRAPH_URL}/users/{user_id}/calendarView/delta"
            now = datetime.utcnow()
            start = (now - timedelta(days=365 * 10)).replace(microsecond=0).isoformat() + "Z"
            end = (now + timedelta(days=365)).replace(microsecond=0).isoformat() + "Z"
            params = {
                "$top": "999",
                "startDateTime": start,
                "endDateTime": end,
            }
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_messages_delta(self, user_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get mailbox messages with full pagination.
        NOTE: Graph API does NOT support delta/change tracking on messages with app-only auth.
        Falls back to regular /messages endpoint with $top pagination.
        Graph API: GET /users/{id}/messages
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/messages"
        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    # ── Attachment endpoints ────────────────────────────────────────────────
    # Mailbox messages and calendar events both expose /attachments collections.
    # Three attachment types exist:
    #   #microsoft.graph.fileAttachment       — binary file, content via /$value
    #   #microsoft.graph.itemAttachment       — embedded item (msg/event/contact);
    #                                           expand inline at list time
    #   #microsoft.graph.referenceAttachment  — link only (OneDrive URL etc.) —
    #                                           no content, just metadata
    # afi.ai captures fileAttachments inline as separate blobs; we mirror that.

    async def list_message_attachments(self, user_id: str, message_id: str) -> List[Dict[str, Any]]:
        """List attachments on a single mailbox message. Returns the raw list
        (no $value blobs) — caller fetches binary content separately for
        fileAttachments. Empty list on 404 (message gone) or 403 (no access)."""
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}/attachments"
        try:
            result = await self._get(url, params={"$top": "100"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        # Some tenants paginate even for /attachments — follow nextLink defensively.
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    async def get_message_attachment_content(
        self, user_id: str, message_id: str, attachment_id: str
    ) -> bytes:
        """Download a fileAttachment's binary content via /$value."""
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}/attachments/{attachment_id}/$value"
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    async def list_event_attachments(self, user_id: str, event_id: str) -> List[Dict[str, Any]]:
        """List attachments on a calendar event."""
        url = f"{self.GRAPH_URL}/users/{user_id}/events/{event_id}/attachments"
        try:
            result = await self._get(url, params={"$top": "100"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    async def get_event_attachment_content(
        self, user_id: str, event_id: str, attachment_id: str
    ) -> bytes:
        """Download a calendar event fileAttachment's binary content via /$value."""
        url = f"{self.GRAPH_URL}/users/{user_id}/events/{event_id}/attachments/{attachment_id}/$value"
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    # ── File version endpoints ──────────────────────────────────────────────
    # OneDrive/SharePoint files retain a version history when versioning is
    # enabled (default for SP, opt-in for OD personal). Graph exposes:
    #   GET /drives/{did}/items/{iid}/versions          — list metadata
    #   GET /drives/{did}/items/{iid}/versions/{vid}/content  — binary

    async def list_file_versions(self, drive_id: str, item_id: str) -> List[Dict[str, Any]]:
        """List historical versions of a drive item. Returns newest-first.
        The first entry is the current version (same content as the live file)."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/versions"
        try:
            result = await self._get(url, params={"$top": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    async def get_file_version_content(
        self, drive_id: str, item_id: str, version_id: str
    ) -> bytes:
        """Download the binary content of a specific historical version."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/versions/{version_id}/content"
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    # ── File / item permissions ─────────────────────────────────────────────
    # Graph's `permissions` collection on a drive item lists every grant —
    # direct sharing, SP groups, link-based access, inheritance markers. afi
    # captures these so restored files re-establish the exact same ACL set.

    async def list_file_permissions(self, drive_id: str, item_id: str) -> List[Dict[str, Any]]:
        """List ACL grants on a OneDrive/SharePoint item. Empty list on 404
        (item gone) or 403 (no permissions to read permissions — uncommon)."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/permissions"
        try:
            result = await self._get(url, params={"$top": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    # ── Event creation + attachment (re)attach ──────────────────────────────
    # Used by restore-worker. Event creation returns the new event_id; the
    # attachment endpoint accepts inline base64 (no /$value upload step needed
    # for restore — the event is freshly created so size limits aren't an issue
    # in practice for typical attachments under 3MB).

    async def create_calendar_event(self, user_id: str, event_payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /users/{id}/events — restore a calendar event.
        Strip server-managed fields the caller might still have in the payload."""
        url = f"{self.GRAPH_URL}/users/{user_id}/events"
        # Graph rejects creates that include these read-only / server-set fields.
        readonly = {
            "id", "createdDateTime", "lastModifiedDateTime", "changeKey",
            "iCalUId", "webLink", "onlineMeeting", "transactionId",
            "@odata.etag", "@odata.context",
        }
        clean = {k: v for k, v in event_payload.items() if k not in readonly}
        return await self._post(url, clean)

    async def create_user_contact(self, user_id: str, contact_payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /users/{id}/contacts — restore a personal contact.

        Same read-only shape as events: the server re-mints id/timestamps/etag.
        Nested @odata.type entries inside emailAddresses / phones are kept since
        Graph requires them on create.
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/contacts"
        readonly = {
            "id", "createdDateTime", "lastModifiedDateTime", "changeKey",
            "@odata.etag", "@odata.context",
        }
        clean = {k: v for k, v in contact_payload.items() if k not in readonly}
        return await self._post(url, clean)

    async def attach_file_to_event(
        self, user_id: str, event_id: str, name: str,
        content_bytes: bytes, content_type: Optional[str] = None,
        is_inline: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """POST /users/{id}/events/{eid}/attachments — inline base64 upload."""
        import base64 as _b64
        url = f"{self.GRAPH_URL}/users/{user_id}/events/{event_id}/attachments"
        payload = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": name,
            "contentType": content_type or "application/octet-stream",
            "contentBytes": _b64.b64encode(content_bytes).decode("ascii"),
            "isInline": is_inline,
        }
        try:
            return await self._post(url, payload)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return None
            raise

    # ── Permission RESTORE ──────────────────────────────────────────────────
    # Replay sharing grants captured by list_file_permissions onto a freshly
    # restored file. Two grant shapes:
    #   - User/group invite — POST /items/{id}/invite  with recipients + roles
    #   - Anonymous / org link — POST /items/{id}/createLink  with type + scope
    # Inherited permissions can't be re-created via API (they come from the
    # parent folder), so callers should filter them out before calling.

    async def invite_to_drive_item(
        self, drive_id: str, item_id: str, recipients: List[Dict[str, str]],
        roles: List[str], require_signin: bool = True, send_invitation: bool = False,
        message: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """POST /drives/{did}/items/{iid}/invite — grants users specific roles."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/invite"
        payload = {
            "recipients": recipients,
            "roles": roles,
            "requireSignIn": require_signin,
            "sendInvitation": send_invitation,
        }
        if message:
            payload["message"] = message
        try:
            return await self._post(url, payload)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return None
            raise

    async def create_drive_item_link(
        self, drive_id: str, item_id: str, link_type: str, scope: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """POST /drives/{did}/items/{iid}/createLink — recreates a sharing link.
        link_type: 'view' | 'edit' | 'embed'. scope: 'anonymous' | 'organization' | 'users'."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/createLink"
        payload: Dict[str, Any] = {"type": link_type}
        if scope:
            payload["scope"] = scope
        try:
            return await self._post(url, payload)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return None
            raise

    # ── Mailbox folder tree ─────────────────────────────────────────────────
    # Each message's `parentFolderId` is opaque; to reconstruct "/Inbox/Project X"
    # we must walk the folder tree once per user. afi rebuilds the hierarchy on
    # restore — without the full path we can only restore items to a flat root.

    async def get_mail_folder_tree(
        self, user_id: str, well_known_root: Optional[str] = None,
    ) -> Dict[str, str]:
        """Return a flat map: folder_id → full path like "/Inbox/Subfolder".

        If well_known_root is provided (e.g., 'archive', 'recoverableitemsroot'),
        starts the walk at that special folder instead of the primary mailbox.
        Returns empty dict if the root folder doesn't exist (no archive license,
        no Exchange mailbox, etc.)."""
        if well_known_root:
            root_url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{well_known_root}"
            try:
                root = await self._get(root_url, params={"$select": "id,displayName"})
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 404):
                    return {}
                raise
            roots = [root]
        else:
            try:
                top = await self._get(
                    f"{self.GRAPH_URL}/users/{user_id}/mailFolders",
                    params={"$top": "200", "$select": "id,displayName"},
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 404):
                    return {}
                raise
            roots = top.get("value", []) or []

        tree: Dict[str, str] = {}

        async def walk(folder: Dict[str, Any], parent_path: str) -> None:
            fid = folder.get("id")
            name = folder.get("displayName") or "(unnamed)"
            path = f"{parent_path}/{name}"
            if fid:
                tree[fid] = path
            # Each folder has a childFolderCount field; only descend if > 0 to
            # avoid wasted requests on leaves.
            if folder.get("childFolderCount", 0) <= 0:
                # If we don't know the count (selective $select), still walk once.
                if "childFolderCount" in folder:
                    return
            try:
                child_resp = await self._get(
                    f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{fid}/childFolders",
                    params={"$top": "200", "$select": "id,displayName,childFolderCount"},
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 404):
                    return
                raise
            children = child_resp.get("value", []) or []
            for c in children:
                await walk(c, path)

        # Start each root walk in parallel, then walk children serially within
        # each subtree (folder trees are usually shallow and bounded).
        await asyncio.gather(*[walk(r, "") for r in roots], return_exceptions=False)
        return tree

    async def list_messages_in_folder(
        self, user_id: str, folder_id: str, top: int = 999,
    ) -> List[Dict[str, Any]]:
        """Fetch all messages directly inside a single mail folder. Used for
        pulling Online Archive / Recoverable Items content where the top-level
        /messages endpoint doesn't reach."""
        url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{folder_id}/messages"
        try:
            result = await self._get(url, params={"$top": str(top)})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    async def get_drive_items_delta(self, drive_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get drive items using delta API.
        Works with both user drives and SharePoint drives.
        Graph API: GET /drives/{drive-id}/root/delta

        NOTE: The delta endpoint does NOT support $select. It returns a fixed
        set of properties (id, name, size, file, folder, deleted, eTag,
        lastModifiedDateTime, @microsoft.graph.downloadUrl, etc.).
        """
        # Use /drives/{drive-id}/root/delta — works for any drive type
        url = f"{self.GRAPH_URL}/drives/{drive_id}/root/delta"
        if delta_token:
            url = delta_token

        # No $select or $expand — delta endpoint ignores them and returns empty
        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_user_onedrive_root(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's OneDrive root drive info.
        Graph API: GET /users/{id}/drive
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/drive")

    async def get_download_url(self, drive_id: str, item_id: str,
                               max_attempts: int = 3) -> Tuple[str, int, Optional[str]]:
        """
        Reliably obtain a fresh @microsoft.graph.downloadUrl for a drive item.

        Why this is non-trivial:
          - Delta responses don't always include @microsoft.graph.downloadUrl.
          - When you $select it explicitly, Graph computes it on-the-fly.
          - The URL is short-lived (~1 hour) and must be used promptly.
          - For files just modified, the URL may briefly 404; retry helps.
          - Some file types (whiteboards, notebooks, packages) have NO download URL
            even though they have a 'file' facet — these are cloud-native objects.

        Returns: (download_url, size_bytes, quick_xor_hash_or_none)
        Raises: RuntimeError if no URL can be obtained after retries.
        Raises: RuntimeError("no_download_url") if item is not downloadable at all.
        """
        # For app-only access, do NOT use $select - Graph ignores it or strips downloadUrl
        # for application permission tokens. Get the full item and extract what we need.
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}"
        last_error = None
        for attempt in range(max_attempts):
            try:
                item = await self._get(url)
                download_url = item.get("@microsoft.graph.downloadUrl")
                size = item.get("size", 0)
                qxh = (item.get("file") or {}).get("hashes", {}).get("quickXorHash")
                file_facet = item.get("file")
                if download_url:
                    return download_url, size, qxh
                # DEBUG: log what Graph returned
                keys = list(item.keys()) if isinstance(item, dict) else "not a dict"
                print(f"[GraphClient] get_download_url attempt {attempt+1}/{max_attempts} for "
                      f"item {item_id}: download_url={'present' if download_url else 'MISSING'}, "
                      f"file_facet={'yes' if file_facet else 'no'}, "
                      f"size={size}, keys={keys}")
                if not file_facet:
                    raise RuntimeError(f"Item {item_id} has no 'file' facet — not downloadable")
                last_error = "downloadUrl missing despite $select; retrying"
            except Exception as e:
                last_error = str(e)
                if attempt == 0:
                    print(f"[GraphClient] get_download_url attempt {attempt+1}/{max_attempts} for "
                          f"item {item_id}: EXCEPTION: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(2 ** attempt)
        # Final attempt failed — check if this item type is fundamentally non-downloadable
        raise RuntimeError(f"Could not obtain downloadUrl for item {item_id}: {last_error}")

    async def get_group_mailbox_messages(self, group_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get group mailbox messages using delta API.
        Graph API: GET /groups/{id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/groups/{group_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_group_threads(self, group_id: str) -> Dict[str, Any]:
        """
        Get group conversation threads.
        Graph API: GET /groups/{id}/threads
        """
        result = await self._get(f"{self.GRAPH_URL}/groups/{group_id}/threads", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_group_thread_posts(self, group_id: str, thread_id: str) -> Dict[str, Any]:
        """
        Get posts for a specific group thread.
        Graph API: GET /groups/{id}/threads/{thread-id}/posts
        """
        result = await self._get(
            f"{self.GRAPH_URL}/groups/{group_id}/threads/{thread_id}/posts",
            params={"$top": "999"},
        )
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_planner_tasks(self, user_id: str = None, plan_id: str = None) -> Dict[str, Any]:
        """
        Get Planner tasks.
        Graph API: GET /users/{id}/planner/tasks or /planner/plans/{id}/tasks
        """
        if plan_id:
            url = f"{self.GRAPH_URL}/planner/plans/{plan_id}/tasks"
        elif user_id:
            url = f"{self.GRAPH_URL}/users/{user_id}/planner/tasks"
        else:
            return {"value": []}

        result = await self._get(url, params={"$top": "999"})
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_power_bi_workspaces(self) -> Dict[str, Any]:
        """
        Get Power BI workspaces via Power BI REST API.
        """
        power_bi_client = PowerBIClient(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            refresh_token=self.power_bi_refresh_token,
        )
        workspaces = await power_bi_client.list_workspaces()
        self.power_bi_refresh_token = power_bi_client.refresh_token
        return {"value": workspaces}

    async def get_onenote_notebooks(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's OneNote notebooks.
        Graph API: GET /users/{id}/onenote/notebooks
        Permission: Notes.Read.All
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/onenote/notebooks", params={"$top": "999"})

    async def get_onenote_sections(self, user_id: str, notebook_id: str) -> Dict[str, Any]:
        """
        Get sections in a OneNote notebook.
        Graph API: GET /users/{id}/onenote/notebooks/{nb-id}/sections
        """
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/onenote/notebooks/{notebook_id}/sections",
            params={"$top": "999"}
        )

    async def get_onenote_pages(self, user_id: str, section_id: str) -> Dict[str, Any]:
        """
        Get pages in a OneNote section.
        Graph API: GET /users/{id}/onenote/sections/{section-id}/pages
        """
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/onenote/sections/{section_id}/pages",
            params={"$top": "999"}
        )

    async def get_user_todo_lists(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's To Do task lists.
        Graph API: GET /users/{id}/todo/lists
        Permission: Tasks.Read.All
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/todo/lists", params={"$top": "999"})

    async def get_user_todo_tasks(self, user_id: str, list_id: str) -> Dict[str, Any]:
        """
        Get tasks in a To Do list.
        Graph API: GET /users/{id}/todo/lists/{list-id}/tasks
        """
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/todo/lists/{list_id}/tasks",
            params={"$top": "999"}
        )

    async def get_planner_plans_for_group(self, group_id: str) -> Dict[str, Any]:
        """
        Get Planner plans for a group/team.
        Graph API: GET /groups/{id}/planner/plans
        Permission: Tasks.Read.All
        """
        return await self._get(f"{self.GRAPH_URL}/groups/{group_id}/planner/plans", params={"$top": "999"})

    async def get_planner_task_details(self, task_id: str) -> Dict[str, Any]:
        """Task details: description, checklist, references, previewType.
        Graph API: GET /planner/tasks/{task-id}/details
        Permission: Tasks.Read.All"""
        return await self._get(f"{self.GRAPH_URL}/planner/tasks/{task_id}/details")

    async def get_user_todo_task_checklist(self, user_id: str, list_id: str, task_id: str) -> Dict[str, Any]:
        """Checklist items nested under a To Do task.
        Graph API: GET /users/{id}/todo/lists/{list-id}/tasks/{task-id}/checklistItems"""
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/todo/lists/{list_id}/tasks/{task_id}/checklistItems",
            params={"$top": "999"},
        )

    async def get_user_todo_task_linked_resources(self, user_id: str, list_id: str, task_id: str) -> Dict[str, Any]:
        """Linked resources (attached URLs / apps) on a To Do task.
        Graph API: GET /users/{id}/todo/lists/{list-id}/tasks/{task-id}/linkedResources"""
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/todo/lists/{list_id}/tasks/{task_id}/linkedResources",
            params={"$top": "999"},
        )

    async def _get_bytes(self, url: str) -> bytes:
        """Authenticated GET that returns the raw response body as bytes — for non-JSON
        endpoints like OneNote page content (text/html) or resource $value (binary).
        Follows 302 redirects implicitly via httpx."""
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    async def get_onenote_page_content(self, user_id: str, page_id: str) -> bytes:
        """Get the HTML body of a OneNote page (returns bytes — the endpoint emits text/html).
        Graph API: GET /users/{id}/onenote/pages/{page-id}/content?includeinkML=true"""
        url = f"{self.GRAPH_URL}/users/{user_id}/onenote/pages/{page_id}/content?includeinkML=true"
        return await self._get_bytes(url)

    async def get_onenote_resource(self, url: str) -> bytes:
        """Fetch a OneNote resource (image or attachment) by its fully-qualified Graph URL.
        URL is taken verbatim from the page HTML's data-fullres-src or src attribute."""
        return await self._get_bytes(url)

    async def _paginated_get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Helper for paginated GET requests"""
        return await self._get(f"{self.GRAPH_URL}{path}", params=params)
