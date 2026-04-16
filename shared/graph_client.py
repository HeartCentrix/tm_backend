"""Microsoft Graph API client for resource discovery"""
import asyncio
import logging
import httpx
from typing import List, Optional, Dict, Any, Tuple
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
    
    async def discover_users(self) -> List[Dict[str, Any]]:
        """Fetch all users from Entra ID"""
        result = await self._get(f"{self.GRAPH_URL}/users", params={"$top": "999", "$count": "true"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))

        users = []
        for u in all_value:
            is_enabled = u.get("accountEnabled", True)
            users.append({
                "external_id": u.get("id"),
                "display_name": u.get("displayName", u.get("mail", u.get("userPrincipalName", "Unknown"))),
                "email": u.get("mail") or u.get("userPrincipalName"),
                "type": "ENTRA_USER",
                "metadata": {
                    "user_principal_name": u.get("userPrincipalName"),
                    "job_title": u.get("jobTitle"),
                    "department": u.get("department"),
                    "account_enabled": is_enabled,
                    "created_at": u.get("createdDateTime"),
                },
                "_account_enabled": is_enabled,  # For discovery worker to filter
            })
        return users
    
    async def discover_groups(self) -> List[Dict[str, Any]]:
        """Fetch all groups from Entra ID"""
        result = await self._get(f"{self.GRAPH_URL}/groups", params={"$top": "999", "$count": "true"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))

        groups = []
        for g in all_value:
            groups.append({
                "external_id": g.get("id"),
                "display_name": g.get("displayName", "Unknown"),
                "email": g.get("mail"),
                "type": "ENTRA_GROUP",
                "metadata": {
                    "mail_enabled": g.get("mailEnabled"),
                    "security_enabled": g.get("securityEnabled"),
                    "group_types": g.get("groupTypes", []),
                    "description": g.get("description"),
                },
            })
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
        # Step 1: Fetch all users (no $filter — get everyone)
        users_result = await self._get(
            f"{self.GRAPH_URL}/users",
            params={"$top": "999", "$count": "true",
                    "$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled,createdDateTime"},
        )
        all_users = users_result.get("value", [])
        while "@odata.nextLink" in users_result:
            users_result = await self._get(users_result["@odata.nextLink"])
            all_users.extend(users_result.get("value", []))
        mailboxes = []

        # Step 2: Enrich each user with userPurpose
        semaphore = asyncio.Semaphore(10)

        async def _enrich_one_user(user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            async with semaphore:
                email = user.get("mail")
                if not email:
                    return None

                try:
                    result = await self._get(
                        f"{self.GRAPH_URL}/users/{user['id']}/mailboxSettings",
                        params={"$select": "userPurpose"},
                    )
                    purpose = result.get("userPurpose") if result else None
                except Exception:
                    purpose = None

                # Step 3: Build resource ONLY if we have a known mailbox type
                if purpose == "user":
                    rtype = "MAILBOX"
                elif purpose == "shared":
                    rtype = "SHARED_MAILBOX"
                elif purpose in ("room", "equipment"):
                    rtype = "ROOM_MAILBOX"
                else:
                    return None  # no mailbox → skip

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
        """Discover OneDrive sites for all users"""
        # Get all users with mailbox licenses, then fetch each user's drive
        users = await self.discover_users()
        drives = []
        for u in users:
            if not u.get("email"):
                continue
            try:
                user_id = u["external_id"]
                drive_result = await self._get(f"{self.GRAPH_URL}/users/{user_id}/drive")
                if drive_result and drive_result.get("id"):
                    drives.append({
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
                        "_account_enabled": u.get("_account_enabled", True),  # For discovery worker
                    })
                # If drive not found (404), skip — discovery worker will mark stale resources
            except Exception as e:
                if "404" in str(e) or "423" in str(e):
                    # Resource not found or locked — discovery worker will handle it
                    pass
                else:
                    print(f"Error discovering OneDrive for user {u.get('email')}: {e}")
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

        except Exception as e:
            logger.warning(f"Failed to discover Teams chats: {e}")

        return resources

    async def discover_power_platform(self) -> List[Dict[str, Any]]:
        """Discover Power Platform resources using Power Platform Admin APIs and Power BI APIs"""
        resources = []
        token = await self._get_token()

        # 1. Discover Power Platform Environments using Power Platform Admin API
        try:
            env_url = "https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments"
            headers = {"Authorization": f"Bearer {token}"}
            async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
                resp = await client.get(env_url, headers=headers)
                if resp.status_code == 200:
                    env_data = resp.json()
                    environments = env_data.get("value", [])
                else:
                    print(f"Error fetching Power Platform environments: {resp.status_code} {resp.text}")
                    environments = []

            for env in environments:
                env_id = env.get("name")
                env_props = env.get("properties", {})
                env_name = env_props.get("displayName", env_id)
                env_type = env_props.get("environmentType", "Unknown")
                env_region = env_props.get("location", {}).get("name", "Unknown")
                has_dataverse = env_props.get("linkedEnvironmentMetadata", {}).get("CommonDataService") is not None

                # Add environment as a resource
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

                # 2. Discover Power Apps in each environment
                try:
                    apps_url = f"https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/{env_id}/resources/Microsoft.PowerApps"
                    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
                        apps_resp = await client.get(apps_url, headers=headers)
                        if apps_resp.status_code == 200:
                            apps_data = apps_resp.json()
                            for app in apps_data.get("value", []):
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
                                        "created_by": app_props.get("createdBy", {}).get("displayName"),
                                        "created_time": app_props.get("createdTime"),
                                        "modified_time": app_props.get("lastModifiedTime"),
                                    },
                                })
                except Exception as e:
                    print(f"Error discovering Power Apps for env {env_id}: {e}")

                # 3. Discover Power Automate flows in each environment
                try:
                    flows_url = f"https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/{env_id}/resources/Microsoft.Flow"
                    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
                        flows_resp = await client.get(flows_url, headers=headers)
                        if flows_resp.status_code == 200:
                            flows_data = flows_resp.json()
                            for flow in flows_data.get("value", []):
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
                                        "created_by": flow_props.get("createdBy", {}).get("displayName"),
                                        "created_time": flow_props.get("createdTime"),
                                        "modified_time": flow_props.get("lastModifiedTime"),
                                    },
                                })
                except Exception as e:
                    print(f"Error discovering Power Automate flows for env {env_id}: {e}")

        except Exception as e:
            print(f"Error discovering Power Platform environments: {e}")

        # 4. Discover Power BI workspaces via Power BI REST API
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
        """
        Get all Teams chats accessible to the app using delta API.
        Graph API: GET /chats/delta
        """
        url = f"{self.GRAPH_URL}/chats/delta"
        if delta_token:
            url = delta_token

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

    async def get_user_contacts(self, user_id: str) -> Dict[str, Any]:
        """
        Get user contacts.
        Graph API: GET /users/{id}/contacts
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/contacts", params={"$top": "999"})

    async def get_calendar_events_delta(self, user_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get calendar events using delta API.
        Graph API: GET /users/{id}/calendar/events/delta
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/calendar/events/delta"
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

    async def _paginated_get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Helper for paginated GET requests"""
        return await self._get(f"{self.GRAPH_URL}{path}", params=params)
