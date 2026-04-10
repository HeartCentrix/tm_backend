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
        """Make authenticated GET request with pagination and throttling support"""
        token = await self._get_token()
        all_items = []
        next_url = url
        max_retries = 3
        retry_count = 0

        while next_url:
            async with httpx.AsyncClient() as client:
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
                all_items.extend(data.get("value", []))
                next_url = data.get("@odata.nextLink")
                params = None  # params only on first request
                retry_count = 0  # Reset on success

        return {"value": all_items, "@odata.count": data.get("@odata.count", len(all_items))}
    
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
