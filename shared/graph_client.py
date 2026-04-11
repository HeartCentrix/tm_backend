"""Microsoft Graph API client for resource discovery"""
import httpx
from typing import List, Optional, Dict, Any
from datetime import datetime
import hashlib
import time


class GraphClient:
    """Client for Microsoft Graph API calls with multi-app support"""

    GRAPH_URL = "https://graph.microsoft.com/v1.0"
    TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    SCOPES = [
        "https://graph.microsoft.com/.default"
    ]

    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    @property
    def app_client_id(self) -> str:
        """Return the app client ID for tracking purposes"""
        return self.client_id

    async def _get_token(self) -> str:
        """Get or refresh access token using client credentials"""
        if self._access_token and self._token_expiry and datetime.utcnow() < self._token_expiry:
            return self._access_token
        
        async with httpx.AsyncClient() as client:
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
            self._token_expiry = datetime.utcnow() + __import__('datetime').timedelta(seconds=expires_in - 300)
            return self._access_token
    
    async def _get(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated GET request with pagination and throttling support.
        Preserves @odata.deltaLink for incremental sync and handles
        single-object responses (e.g. /users/{id}) that have no 'value' array.
        """
        token = await self._get_token()
        all_items = []
        next_url = url
        max_retries = 3
        retry_count = 0
        delta_link = None
        last_data = {}

        while next_url:
            async with httpx.AsyncClient(timeout=60.0) as client:
                headers = {"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"}
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
                retry_count = 0  # Reset on success

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
        async with httpx.AsyncClient() as client:
            req_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            if headers:
                req_headers.update(headers)
            resp = await client.post(url, headers=req_headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def _put(self, url: str, content: Any, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated PUT request (for file uploads)"""
        token = await self._get_token()
        async with httpx.AsyncClient() as client:
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
        async with httpx.AsyncClient() as client:
            req_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            resp = await client.patch(url, headers=req_headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def _delete(self, url: str) -> None:
        """Make authenticated DELETE request"""
        token = await self._get_token()
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {token}"}
            resp = await client.delete(url, headers=headers)
            resp.raise_for_status()
    
    async def discover_users(self) -> List[Dict[str, Any]]:
        """Fetch all users from Entra ID"""
        result = await self._get(f"{self.GRAPH_URL}/users", params={"$top": "999", "$count": "true"})
        users = []
        for u in result.get("value", []):
            users.append({
                "external_id": u.get("id"),
                "display_name": u.get("displayName", u.get("mail", u.get("userPrincipalName", "Unknown"))),
                "email": u.get("mail") or u.get("userPrincipalName"),
                "type": "ENTRA_USER",
                "metadata": {
                    "user_principal_name": u.get("userPrincipalName"),
                    "job_title": u.get("jobTitle"),
                    "department": u.get("department"),
                    "account_enabled": u.get("accountEnabled", True),
                    "created_at": u.get("createdDateTime"),
                },
            })
        return users
    
    async def discover_groups(self) -> List[Dict[str, Any]]:
        """Fetch all groups from Entra ID"""
        result = await self._get(f"{self.GRAPH_URL}/groups", params={"$top": "999", "$count": "true"})
        groups = []
        for g in result.get("value", []):
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
    
    async def discover_mailboxes(self) -> List[Dict[str, Any]]:
        """Discover mailboxes by fetching users with mailboxes"""
        # Get all users - those with mailboxLicense are mailboxes
        users = await self.discover_users()
        mailboxes = []
        for u in users:
            if u.get("email"):
                mailboxes.append({
                    "external_id": u["external_id"],
                    "display_name": u["display_name"],
                    "email": u["email"],
                    "type": "MAILBOX",
                    "metadata": u.get("metadata", {}),
                })
        return mailboxes
    
    async def discover_shared_mailboxes(self) -> List[Dict[str, Any]]:
        """Discover shared mailboxes"""
        # Shared mailboxes are users with mailbox but no license
        result = await self._get(
            f"{self.GRAPH_URL}/users",
            params={"$filter": "mailboxSettings is not null", "$top": "999", "$count": "true"}
        )
        shared = []
        for u in result.get("value", []):
            # Filter for shared mailboxes (typically have no password/sign-in)
            if u.get("mail") and not u.get("assignedLicenses"):
                shared.append({
                    "external_id": u.get("id"),
                    "display_name": u.get("displayName", u.get("mail")),
                    "email": u.get("mail"),
                    "type": "SHARED_MAILBOX",
                    "metadata": {"user_principal_name": u.get("userPrincipalName")},
                })
        return shared
    
    async def discover_onedrive(self) -> List[Dict[str, Any]]:
        """Discover OneDrive sites for all users"""
        result = await self._get(
            f"{self.GRAPH_URL}/sites",
            params={"$search": '"contentclass:STS_Site"', "$top": "999"}
        )
        drives = []
        for site in result.get("value", []):
            site_id = site.get("id", "").split(",")[-1] if site.get("id") else None
            if site_id:
                drives.append({
                    "external_id": site_id,
                    "display_name": site.get("name", "Unknown OneDrive"),
                    "email": None,
                    "type": "ONEDRIVE",
                    "metadata": {
                        "site_id": site.get("id"),
                        "web_url": site.get("webUrl"),
                        "site_collection": site.get("siteCollection", {}),
                    },
                })
        return drives
    
    async def discover_sharepoint(self) -> List[Dict[str, Any]]:
        """Discover SharePoint sites"""
        result = await self._get(
            f"{self.GRAPH_URL}/sites",
            params={"$search": '"contentclass:STS_Site" AND NOT "contentclass:STS_MySite"', "$top": "999"}
        )
        sites = []
        for site in result.get("value", []):
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
        """Discover Teams channels and chats"""
        # Get all teams (groups with resourceProvisioningOptions containing "Team")
        result = await self._get(
            f"{self.GRAPH_URL}/groups",
            params={"$filter": "resourceProvisioningOptions/Any(x:x eq 'Team')", "$top": "999"}
        )
        teams = []
        for g in result.get("value", []):
            teams.append({
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
        return teams
    
    async def discover_all(self) -> List[Dict[str, Any]]:
        """Run full discovery and return all resources"""
        all_resources = []
        
        try:
            all_resources.extend(await self.discover_users())
        except Exception as e:
            print(f"Error discovering users: {e}")
        
        try:
            all_resources.extend(await self.discover_groups())
        except Exception as e:
            print(f"Error discovering groups: {e}")
        
        try:
            all_resources.extend(await self.discover_mailboxes())
        except Exception as e:
            print(f"Error discovering mailboxes: {e}")
        
        try:
            all_resources.extend(await self.discover_sharepoint())
        except Exception as e:
            print(f"Error discovering SharePoint: {e}")
        
        try:
            all_resources.extend(await self.discover_teams())
        except Exception as e:
            print(f"Error discovering Teams: {e}")
        
        # Deduplicate by external_id
        seen = set()
        unique = []
        for r in all_resources:
            key = r.get("external_id")
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

        params = {"$expand": "thumbnails,listItem", "$top": "999"}
        result = await self._get(url, params=params)
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
        return await self._get(f"{self.GRAPH_URL}/teams/{team_id}/channels", params={"$top": "999"})

    async def get_channel_messages(self, team_id: str, channel_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get messages from a Teams channel using delta API.
        Graph API: GET /teams/{team-id}/channels/{channel-id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/teams/{team_id}/channels/{channel_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999", "$expand": "replies,hostedContents"}
        result = await self._get(url, params=params)
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
        return result

    async def get_chat_messages(self, chat_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get messages from a Teams chat using delta API.
        Graph API: GET /chats/{chat-id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/chats/{chat_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999"}
        result = await self._get(url, params=params)
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
        return await self._get(f"{self.GRAPH_URL}/applications", params={"$top": "999"})

    async def get_entra_service_principals(self) -> Dict[str, Any]:
        """
        Get service principals.
        Graph API: GET /servicePrincipals
        """
        return await self._get(f"{self.GRAPH_URL}/servicePrincipals", params={"$top": "999"})

    async def get_entra_devices(self) -> Dict[str, Any]:
        """
        Get registered devices.
        Graph API: GET /devices
        """
        return await self._get(f"{self.GRAPH_URL}/devices", params={"$top": "999"})

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
        return result

    async def get_messages_delta(self, user_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get mailbox messages using delta API.
        Graph API: GET /users/{id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999", "$expand": "attachments"}
        result = await self._get(url, params=params)
        return result

    async def get_drive_items_delta(self, user_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get OneDrive drive items using delta API.
        Graph API: GET /users/{id}/drive/root/delta
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/drive/root/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999", "$expand": "thumbnails,listItem"}
        result = await self._get(url, params=params)
        return result

    async def get_user_onedrive_root(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's OneDrive root drive info.
        Graph API: GET /users/{id}/drive
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/drive")

    async def get_sharepoint_site_drives(self, site_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get SharePoint site drive items using delta API.
        Graph API: GET /sites/{site-id}/drive/root/delta
        """
        # Convert slashes to commas for Graph API
        graph_site_id = site_id.replace("/", ",")
        url = f"{self.GRAPH_URL}/sites/{graph_site_id}/drive/root/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999", "$expand": "thumbnails,listItem"}
        result = await self._get(url, params=params)
        return result

    async def get_teams_channels(self, team_id: str) -> Dict[str, Any]:
        """
        Get Teams channels.
        Graph API: GET /teams/{team-id}/channels
        """
        return await self._get(f"{self.GRAPH_URL}/teams/{team_id}/channels", params={"$top": "999"})

    async def get_channel_messages_delta(self, team_id: str, channel_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get Teams channel messages using delta API.
        Graph API: GET /teams/{team-id}/channels/{channel-id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/teams/{team_id}/channels/{channel_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999", "$expand": "replies,hostedContents"}
        result = await self._get(url, params=params)
        return result

    async def get_teams_chats_delta(self, delta_token: str = None) -> Dict[str, Any]:
        """
        Get all Teams chats using delta API.
        Graph API: GET /chats/delta
        """
        url = f"{self.GRAPH_URL}/chats/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999", "$expand": "members"}
        result = await self._get(url, params=params)
        return result

    async def get_chat_messages_delta(self, chat_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get Teams chat messages using delta API.
        Graph API: GET /chats/{chat-id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/chats/{chat_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999"}
        result = await self._get(url, params=params)
        return result

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
        return result

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
        
        return await self._get(url, params={"$top": "999"})

    async def get_power_bi_workspaces(self) -> Dict[str, Any]:
        """
        Get Power BI workspaces.
        Graph API: GET /groups (filtered for Power BI)
        """
        return await self._get(f"{self.GRAPH_URL}/groups", params={
            "$filter": "resourceProvisioningOptions/Any(x:x eq 'PowerBI')",
            "$top": "999"
        })

    async def _paginated_get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Helper for paginated GET requests"""
        return await self._get(f"{self.GRAPH_URL}{path}", params=params)
