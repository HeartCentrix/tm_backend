"""Power Platform Admin API client.

Covers Power Apps, Power Automate (Flows), and Power Platform DLP policies.
Uses app-only client credentials with a Power Platform-scoped token
(audience https://service.powerapps.com — NOT a Graph token).

The app registration needs a Power Platform Administrator role on the tenant
for the admin-scope endpoints used here. See README for setup.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx


logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=10.0)
_TOKEN_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)


class PowerPlatformClient:
    """Client for Power Platform Admin API (admin-scope endpoints)."""

    BAP_BASE = "https://api.bap.microsoft.com"
    FLOW_BASE = "https://api.flow.microsoft.com"
    POWER_APPS_BASE = "https://api.powerapps.com"
    TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    # App-only token audience — different from Graph. BAP/Flow/PowerApps all accept this.
    SCOPE = "https://service.powerapps.com/.default"

    API_VERSION_BAP = "2020-10-01"
    API_VERSION_FLOW = "2016-11-01"
    API_VERSION_POWERAPPS = "2020-07-01"

    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    async def _get_token(self) -> str:
        if self._access_token and self._token_expiry and datetime.utcnow() < self._token_expiry:
            return self._access_token

        url = self.TOKEN_URL.format(tenant_id=self.tenant_id)
        async with httpx.AsyncClient(timeout=_TOKEN_TIMEOUT) as client:
            resp = await client.post(url, data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": self.SCOPE,
            })
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._token_expiry = datetime.utcnow() + timedelta(seconds=data.get("expires_in", 3600) - 300)
            return self._access_token

    async def _get(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Paginated GET. Concatenates @odata.nextLink pages into value[]."""
        token = await self._get_token()
        headers = {"Authorization": f"Bearer {token}"}
        all_value: List[Any] = []
        next_url: Optional[str] = url
        last: Dict[str, Any] = {}
        first = True
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            while next_url:
                resp = await client.get(next_url, headers=headers, params=params if first else None)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict):
                    if "value" in data:
                        all_value.extend(data.get("value", []))
                    last = data
                else:
                    return data
                next_url = data.get("nextLink") or data.get("@odata.nextLink")
                first = False
        if "value" in last:
            last["value"] = all_value
        return last

    # ==================== Power Apps ====================

    async def list_apps(self, environment_id: str) -> Dict[str, Any]:
        """List all canvas/model-driven apps in an environment.
        Admin scope: includes apps across all users."""
        url = (f"{self.BAP_BASE}/providers/Microsoft.BusinessAppPlatform"
               f"/scopes/admin/environments/{environment_id}/resources/Microsoft.PowerApps")
        return await self._get(url, params={"api-version": self.API_VERSION_BAP})

    async def get_app(self, environment_id: str, app_name: str) -> Dict[str, Any]:
        """Get the full definition of a single Power App (includes connection refs, properties)."""
        url = (f"{self.BAP_BASE}/providers/Microsoft.BusinessAppPlatform"
               f"/scopes/admin/environments/{environment_id}/resources/Microsoft.PowerApps/{app_name}")
        return await self._get(url, params={"api-version": self.API_VERSION_BAP, "$expand": "unpublishedAppDefinition"})

    # ==================== Power Automate (Flows) ====================

    async def list_flows(self, environment_id: str) -> Dict[str, Any]:
        """List all flows in an environment (admin scope — all users' flows)."""
        url = (f"{self.BAP_BASE}/providers/Microsoft.BusinessAppPlatform"
               f"/scopes/admin/environments/{environment_id}/resources/Microsoft.Flow")
        return await self._get(url, params={"api-version": self.API_VERSION_BAP})

    async def get_flow(self, environment_id: str, flow_name: str) -> Dict[str, Any]:
        """Get the full definition of a single flow, including trigger, actions,
        and connection references. $expand=definitionSummary returns the full JSON."""
        url = (f"{self.FLOW_BASE}/providers/Microsoft.ProcessSimple"
               f"/scopes/admin/environments/{environment_id}/flows/{flow_name}")
        return await self._get(url, params={
            "api-version": self.API_VERSION_FLOW,
            "$expand": "properties/definitionSummary,properties/connectionReferences",
        })

    async def get_flow_connections(self, environment_id: str, flow_name: str) -> Dict[str, Any]:
        """Connection objects referenced by a flow. Useful for restore planning
        because a flow without its connections re-installs as broken."""
        url = (f"{self.FLOW_BASE}/providers/Microsoft.ProcessSimple"
               f"/scopes/admin/environments/{environment_id}/flows/{flow_name}/connections")
        try:
            return await self._get(url, params={"api-version": self.API_VERSION_FLOW})
        except httpx.HTTPStatusError as e:
            # Some flow types don't expose this endpoint — non-fatal
            if e.response.status_code in (404, 400):
                return {"value": []}
            raise

    # ==================== DLP Policies ====================

    async def list_dlp_policies(self) -> Dict[str, Any]:
        """List all tenant-level DLP policies (admin scope)."""
        url = (f"{self.BAP_BASE}/providers/PowerPlatform.Governance/v2/policies")
        return await self._get(url, params={"api-version": self.API_VERSION_BAP})

    async def get_dlp_policy(self, policy_name: str) -> Dict[str, Any]:
        """Full DLP policy definition: connector groups (business/non-business/blocked),
        environment scope, and any custom connector rules."""
        url = (f"{self.BAP_BASE}/providers/PowerPlatform.Governance/v2/policies/{policy_name}")
        return await self._get(url, params={"api-version": self.API_VERSION_BAP})

    # ==================== Environments ====================

    async def list_environments(self) -> Dict[str, Any]:
        """All environments the app can see under admin scope."""
        url = f"{self.BAP_BASE}/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments"
        return await self._get(url, params={"api-version": self.API_VERSION_BAP})

    # ==================== Package Export (async ops) ====================

    async def _post_and_poll(self, post_url: str, post_body: Dict, post_params: Dict,
                             poll_interval: float = 3.0, timeout_seconds: int = 600) -> Dict[str, Any]:
        """Fire an async operation and poll until complete.

        Power Platform export endpoints follow the 202-Accepted + Location/Operation-Location
        pattern. Returns the final operation payload which typically contains a
        'packageLink' or similar SAS-signed download URL."""
        token = await self._get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            resp = await client.post(post_url, headers=headers, params=post_params, json=post_body)
            # 200 (sync complete) or 202 (async accepted) — both valid
            if resp.status_code == 200:
                return resp.json() if resp.content else {}
            if resp.status_code != 202:
                resp.raise_for_status()

            operation_url = (resp.headers.get("Location")
                             or resp.headers.get("Operation-Location")
                             or resp.headers.get("Azure-AsyncOperation"))
            if not operation_url:
                # Some endpoints inline the result even on 202
                return resp.json() if resp.content else {}

            deadline = datetime.utcnow() + timedelta(seconds=timeout_seconds)
            while datetime.utcnow() < deadline:
                await asyncio.sleep(poll_interval)
                # Refresh token if it expired mid-poll for long ops
                headers["Authorization"] = f"Bearer {await self._get_token()}"
                poll_resp = await client.get(operation_url, headers=headers)
                if poll_resp.status_code == 202:
                    # Still in progress; honor Retry-After if present
                    retry_after = poll_resp.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        poll_interval = min(float(retry_after), 30.0)
                    continue
                poll_resp.raise_for_status()
                data = poll_resp.json() if poll_resp.content else {}
                status = (data.get("status") or data.get("properties", {}).get("status") or "").lower()
                if status in ("succeeded", "completed", "") and data:
                    return data
                if status in ("failed", "cancelled", "canceled"):
                    raise RuntimeError(f"Async op failed: {data.get('error') or data}")
            raise TimeoutError(f"Async op did not complete within {timeout_seconds}s: {post_url}")

    async def _download_sas(self, url: str) -> bytes:
        """Download a pre-signed SAS URL — no auth header; the SAS token is in the query string."""
        async with httpx.AsyncClient(timeout=httpx.Timeout(connect=15.0, read=300.0, write=30.0, pool=10.0),
                                     follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content

    async def export_app_package(self, environment_id: str, app_name: str,
                                 package_display_name: Optional[str] = None) -> Optional[bytes]:
        """Export a canvas Power App as a .zip package (async, polled).

        Returns the package bytes on success, or None if the endpoint indicates
        export isn't supported for this app type (e.g. model-driven). Raises on
        other failures so the caller can mark the snapshot partial."""
        # Step 1: list what's in the package
        list_url = (f"{self.POWER_APPS_BASE}/providers/Microsoft.PowerApps"
                    f"/apps/{app_name}/listPackageResources")
        list_params = {"api-version": self.API_VERSION_POWERAPPS}
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            list_resp = await client.post(list_url,
                                          headers={"Authorization": f"Bearer {token}"},
                                          params=list_params, json={})
            if list_resp.status_code in (404, 400):
                logger.info("Power App %s does not support package export: %s", app_name, list_resp.status_code)
                return None
            list_resp.raise_for_status()
            resources = list_resp.json().get("resources", {})

        # Step 2: kick off the export with all listed resources selected
        export_url = (f"{self.POWER_APPS_BASE}/providers/Microsoft.PowerApps"
                      f"/apps/{app_name}/exportPackage")
        body = {
            "includedResourceIds": list(resources.keys()),
            "details": {
                "displayName": package_display_name or app_name,
                "description": f"tm_vault backup of {app_name}",
                "creator": "tm_vault",
                "sourceEnvironment": environment_id,
            },
            "resources": resources,
        }
        result = await self._post_and_poll(export_url, body, list_params, timeout_seconds=900)
        download_url = (result.get("packageLink", {}) or {}).get("value") or result.get("packageLink")
        if isinstance(download_url, dict):
            download_url = download_url.get("value")
        if not download_url:
            logger.warning("Power App export completed without packageLink: %s", result)
            return None
        return await self._download_sas(download_url)

    # ==================== Package Import (restore path) ====================

    async def _upload_package_to_staging(self, zip_bytes: bytes) -> str:
        """Upload a package ZIP to Microsoft-managed storage and return the reference.

        Power Platform import APIs don't accept binary uploads directly — you must
        first PUT the bytes to a SAS URL returned by generateResourceStorage, then
        reference the returned blob URL in the import call."""
        token = await self._get_token()
        gen_url = f"{self.POWER_APPS_BASE}/providers/Microsoft.PowerApps/generateResourceStorage"
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            resp = await client.post(gen_url,
                                     headers={"Authorization": f"Bearer {token}"},
                                     params={"api-version": self.API_VERSION_POWERAPPS})
            resp.raise_for_status()
            storage = resp.json()
            sas_url = storage.get("sharedAccessSignature")
            if not sas_url:
                raise RuntimeError(f"generateResourceStorage returned no SAS URL: {storage}")
            # Strip any query-string then append /package.zip before the SAS token
            base, _, sas_token = sas_url.partition("?")
            blob_url = f"{base.rstrip('/')}/{uuid.uuid4().hex}/package.zip?{sas_token}"

        async with httpx.AsyncClient(timeout=httpx.Timeout(connect=15.0, read=300.0, write=300.0, pool=10.0)) as client:
            put_resp = await client.put(blob_url, content=zip_bytes,
                                        headers={"x-ms-blob-type": "BlockBlob",
                                                 "Content-Type": "application/zip"})
            put_resp.raise_for_status()
        return blob_url

    async def import_app_package(self, environment_id: str, zip_bytes: bytes,
                                 display_name: Optional[str] = None) -> Dict[str, Any]:
        """Import a previously-exported Power App package into an environment.

        Three-step flow:
          1. generateResourceStorage → SAS URL
          2. PUT the zip bytes to that URL
          3. POST /environments/{env}/importPackage with a reference to the uploaded blob
        Returns the final async-op payload (contains a resources map with new app IDs)."""
        package_link = await self._upload_package_to_staging(zip_bytes)
        import_url = (f"{self.POWER_APPS_BASE}/providers/Microsoft.PowerApps"
                      f"/environments/{environment_id}/importPackage")
        # Query Microsoft for the resources embedded in the uploaded package
        token = await self._get_token()
        list_url = (f"{self.POWER_APPS_BASE}/providers/Microsoft.PowerApps"
                    f"/environments/{environment_id}/listPackageResources")
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            list_resp = await client.post(
                list_url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                params={"api-version": self.API_VERSION_POWERAPPS},
                json={"packageLink": {"value": package_link}},
            )
            list_resp.raise_for_status()
            listed = list_resp.json()
        resources = listed.get("resources", {})
        # Mark every resource for creation (simplest: new IDs in target env)
        for res_id, res in resources.items():
            res.setdefault("suggestedCreationType", "New")
            res.setdefault("selectedCreationType", "New")

        body = {
            "packageLink": {"value": package_link},
            "details": {
                "displayName": display_name or "tm_vault restored app",
                "description": "Restored by tm_vault",
                "creator": "tm_vault",
            },
            "resources": resources,
        }
        return await self._post_and_poll(import_url, body,
                                         {"api-version": self.API_VERSION_POWERAPPS},
                                         timeout_seconds=900)

    async def import_flow_package(self, environment_id: str, zip_bytes: bytes,
                                  display_name: Optional[str] = None) -> Dict[str, Any]:
        """Import a previously-exported Power Automate flow package. Same three-step
        shape as import_app_package but targeting the Flow service."""
        package_link = await self._upload_package_to_staging(zip_bytes)
        list_url = (f"{self.FLOW_BASE}/providers/Microsoft.ProcessSimple"
                    f"/environments/{environment_id}/listPackageResources")
        import_url = (f"{self.FLOW_BASE}/providers/Microsoft.ProcessSimple"
                      f"/environments/{environment_id}/importPackage")
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            list_resp = await client.post(
                list_url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                params={"api-version": self.API_VERSION_FLOW},
                json={"packageLink": {"value": package_link}},
            )
            list_resp.raise_for_status()
            listed = list_resp.json()
        resources = listed.get("resources", {})
        for res_id, res in resources.items():
            res.setdefault("suggestedCreationType", "New")
            res.setdefault("selectedCreationType", "New")

        body = {
            "packageLink": {"value": package_link},
            "details": {
                "displayName": display_name or "tm_vault restored flow",
                "description": "Restored by tm_vault",
                "creator": "tm_vault",
            },
            "resources": resources,
        }
        return await self._post_and_poll(import_url, body,
                                         {"api-version": self.API_VERSION_FLOW},
                                         timeout_seconds=900)

    async def upsert_dlp_policy(self, policy_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Create or replace a DLP policy from a previously-captured definition.

        If the incoming definition has a `name` / `id`, we attempt PUT (update in place).
        If the policy doesn't exist or has no identifier, POST (create new)."""
        token = await self._get_token()
        params = {"api-version": self.API_VERSION_BAP}
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        policy_id = (policy_definition.get("name")
                     or policy_definition.get("id")
                     or policy_definition.get("properties", {}).get("name"))
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            if policy_id:
                put_url = f"{self.BAP_BASE}/providers/PowerPlatform.Governance/v2/policies/{policy_id}"
                resp = await client.put(put_url, headers=headers, params=params, json=policy_definition)
                if resp.status_code in (200, 201, 202):
                    return resp.json() if resp.content else {"policyId": policy_id}
                if resp.status_code != 404:
                    resp.raise_for_status()
            # No identifier or 404 on PUT — fall through to create
            post_url = f"{self.BAP_BASE}/providers/PowerPlatform.Governance/v2/policies"
            resp = await client.post(post_url, headers=headers, params=params, json=policy_definition)
            resp.raise_for_status()
            return resp.json() if resp.content else {}

    async def export_flow_package(self, environment_id: str, flow_name: str,
                                  package_display_name: Optional[str] = None) -> Optional[bytes]:
        """Export a Power Automate flow as a package (ZIP) via the async op pattern."""
        list_url = (f"{self.FLOW_BASE}/providers/Microsoft.ProcessSimple"
                    f"/environments/{environment_id}/flows/{flow_name}/listPackageResources")
        list_params = {"api-version": self.API_VERSION_FLOW}
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            list_resp = await client.post(list_url,
                                          headers={"Authorization": f"Bearer {token}"},
                                          params=list_params, json={})
            if list_resp.status_code in (404, 400):
                logger.info("Flow %s does not support package export: %s", flow_name, list_resp.status_code)
                return None
            list_resp.raise_for_status()
            resources = list_resp.json().get("resources", {})

        export_url = (f"{self.FLOW_BASE}/providers/Microsoft.ProcessSimple"
                      f"/environments/{environment_id}/flows/{flow_name}/exportPackage")
        body = {
            "includedResourceIds": list(resources.keys()),
            "details": {
                "displayName": package_display_name or flow_name,
                "description": f"tm_vault backup of flow {flow_name}",
                "creator": "tm_vault",
                "sourceEnvironment": environment_id,
            },
            "resources": resources,
        }
        result = await self._post_and_poll(export_url, body, list_params, timeout_seconds=900)
        download_url = (result.get("packageLink", {}) or {}).get("value") or result.get("packageLink")
        if isinstance(download_url, dict):
            download_url = download_url.get("value")
        if not download_url:
            logger.warning("Flow export completed without packageLink: %s", result)
            return None
        return await self._download_sas(download_url)
