"""Power BI / Fabric client for discovery, backup, and restore."""

from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta
import logging
from typing import Any, Dict, List, Optional

import httpx

from shared.config import settings
from shared.security import decrypt_secret, encrypt_secret


logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=10.0)
_TOKEN_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)


class PowerBIClient:
    POWER_BI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
    FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
    POWER_BI_DELEGATED_SCOPE = " ".join([
        "https://analysis.windows.net/powerbi/api/Workspace.ReadWrite.All",
        "https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All",
        "https://analysis.windows.net/powerbi/api/Report.ReadWrite.All",
        "https://analysis.windows.net/powerbi/api/Dashboard.ReadWrite.All",
        "https://analysis.windows.net/powerbi/api/Dataflow.ReadWrite.All",
        "https://analysis.windows.net/powerbi/api/Tenant.Read.All",
        "offline_access",
    ])
    FABRIC_DELEGATED_SCOPE = " ".join([
        "https://api.fabric.microsoft.com/Workspace.ReadWrite.All",
        "https://api.fabric.microsoft.com/Item.ReadWrite.All",
        "offline_access",
    ])
    POWER_BI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"
    TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    def __init__(
        self,
        tenant_id: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
    ):
        self.tenant_id = tenant_id or settings.EFFECTIVE_POWER_BI_TENANT_ID
        self.client_id = client_id or settings.EFFECTIVE_POWER_BI_CLIENT_ID
        self.client_secret = client_secret or settings.EFFECTIVE_POWER_BI_CLIENT_SECRET
        self.refresh_token = refresh_token
        self._token_cache: Dict[str, tuple[str, datetime]] = {}

    @property
    def auth_mode(self) -> str:
        return "DELEGATED_SERVICE_USER" if self.refresh_token else "APP_ONLY"

    @staticmethod
    def get_refresh_token_from_tenant(tenant: Any) -> Optional[str]:
        extra_data = getattr(tenant, "extra_data", None) or {}
        encoded = extra_data.get("power_bi_refresh_token_encrypted")
        if not encoded:
            return None
        try:
            ciphertext = base64.urlsafe_b64decode(encoded.encode("utf-8"))
            return decrypt_secret(ciphertext)
        except Exception:
            logger.warning("Failed to decrypt Power BI delegated refresh token for tenant %s", getattr(tenant, "id", "unknown"))
            return None

    @staticmethod
    def encode_refresh_token(refresh_token: str) -> str:
        return base64.urlsafe_b64encode(encrypt_secret(refresh_token)).decode("utf-8")

    @staticmethod
    async def persist_refresh_token(db: Any, tenant: Any, refresh_token: Optional[str]) -> bool:
        if not refresh_token or not tenant:
            return False

        from datetime import timezone
        from sqlalchemy import select
        from shared.models import AdminConsentToken

        encrypted_refresh_token = encrypt_secret(refresh_token)
        encoded_refresh_token = base64.urlsafe_b64encode(encrypted_refresh_token).decode("utf-8")
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        tenant.extra_data = tenant.extra_data or {}
        changed = tenant.extra_data.get("power_bi_refresh_token_encrypted") != encoded_refresh_token
        tenant.extra_data["power_bi_refresh_token_encrypted"] = encoded_refresh_token
        tenant.extra_data["power_bi_auth_mode"] = "DELEGATED_SERVICE_USER"
        tenant.extra_data["power_bi_refresh_token_updated_at"] = now.isoformat()

        stmt = select(AdminConsentToken).where(
            AdminConsentToken.tenant_id == tenant.id,
            AdminConsentToken.consent_type == "POWER_BI",
            AdminConsentToken.is_active == True,
        ).order_by(AdminConsentToken.consented_at.desc()).limit(1)
        token = (await db.execute(stmt)).scalar_one_or_none()
        if token is not None:
            token.refresh_token_encrypted = encrypted_refresh_token
            token.last_used_at = now

        return changed

    async def _get_token(self, scope: str) -> str:
        cached = self._token_cache.get(scope)
        if cached and cached[1] > datetime.utcnow():
            return cached[0]

        last_exc = None
        for attempt in range(1, 4):
            try:
                async with httpx.AsyncClient(timeout=_TOKEN_TIMEOUT) as client:
                    delegated_scope = (
                        self.POWER_BI_DELEGATED_SCOPE
                        if scope == self.POWER_BI_SCOPE
                        else self.FABRIC_DELEGATED_SCOPE
                    )
                    data = {
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                    }
                    if self.refresh_token:
                        data.update({
                            "grant_type": "refresh_token",
                            "refresh_token": self.refresh_token,
                            "scope": delegated_scope,
                        })
                    else:
                        data.update({
                            "grant_type": "client_credentials",
                            "scope": scope,
                        })
                    response = await client.post(
                        self.TOKEN_URL.format(tenant_id=self.tenant_id),
                        data=data,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    token = payload["access_token"]
                    expires_in = payload.get("expires_in", 3600)
                    if payload.get("refresh_token"):
                        self.refresh_token = payload["refresh_token"]
                    self._token_cache[scope] = (
                        token,
                        datetime.utcnow() + timedelta(seconds=expires_in - 300),
                    )
                    return token
            except (httpx.HTTPError, KeyError) as exc:
                last_exc = exc
                await asyncio.sleep(2**attempt)
        raise RuntimeError(f"Could not acquire Power BI/Fabric token: {last_exc}")

    async def _request(
        self,
        method: str,
        url: str,
        *,
        api: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        scope = self.POWER_BI_SCOPE if api == "powerbi" else self.FABRIC_SCOPE
        token = await self._get_token(scope)
        request_headers = {"Authorization": f"Bearer {token}"}
        if headers:
            request_headers.update(headers)

        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            response = await client.request(
                method,
                url,
                params=params,
                json=json,
                headers=request_headers,
            )
            response.raise_for_status()
            return response

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        api: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        response = await self._request(method, url, api=api, params=params, json=json, headers=headers)
        if response.status_code == 204 or not response.content:
            return {}
        return response.json()

    async def _poll_lro(self, location: str, api: str) -> Dict[str, Any]:
        deadline = datetime.utcnow() + timedelta(minutes=3)
        while datetime.utcnow() < deadline:
            response = await self._request("GET", location, api=api)
            if response.status_code == 202:
                await asyncio.sleep(int(response.headers.get("Retry-After", "3")))
                continue

            payload = response.json() if response.content else {}
            status = str(payload.get("status", "")).lower()
            if status in {"running", "inprogress", "notstarted"}:
                await asyncio.sleep(int(response.headers.get("Retry-After", "3")))
                continue
            if "result" in payload and isinstance(payload["result"], dict):
                return payload["result"]
            return payload
        raise TimeoutError(f"Timed out waiting for Power BI/Fabric LRO: {location}")

    async def _request_lro_json(
        self,
        method: str,
        url: str,
        *,
        api: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        response = await self._request(method, url, api=api, params=params, json=json)
        if response.status_code == 202:
            location = response.headers.get("Location")
            if not location:
                raise RuntimeError(f"Power BI/Fabric LRO response missing Location header for {url}")
            return await self._poll_lro(location, api)
        if response.status_code == 204 or not response.content:
            return {}
        return response.json()

    async def list_workspaces(self) -> List[Dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/groups",
            api="powerbi",
            params={"$top": "5000"},
        )
        return payload.get("value", [])

    async def list_modified_workspace_ids(self, modified_since: datetime) -> List[str]:
        payload = await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/admin/workspaces/modified",
            api="powerbi",
            params={"modifiedSince": modified_since.isoformat()},
        )
        return [workspace["id"] for workspace in payload.get("value", []) if workspace.get("id")]

    async def scan_workspaces(
        self,
        workspace_ids: List[str],
        *,
        lineage: bool = True,
        datasource_details: bool = True,
        dataset_schema: bool = False,
        dataset_expressions: bool = False,
        get_artifact_users: bool = True,
    ) -> Dict[str, Any]:
        if not workspace_ids:
            return {"workspaces": []}

        payload = await self._request_json(
            "POST",
            f"{self.POWER_BI_BASE_URL}/admin/workspaces/getInfo",
            api="powerbi",
            params={
                "lineage": str(lineage).lower(),
                "datasourceDetails": str(datasource_details).lower(),
                "datasetSchema": str(dataset_schema).lower(),
                "datasetExpressions": str(dataset_expressions).lower(),
                "getArtifactUsers": str(get_artifact_users).lower(),
            },
            json={"workspaces": workspace_ids},
        )

        scan_id = payload.get("id")
        if not scan_id:
            raise RuntimeError("Power BI workspace scan did not return a scan id")

        deadline = datetime.utcnow() + timedelta(minutes=2)
        while datetime.utcnow() < deadline:
            status_payload = await self._request_json(
                "GET",
                f"{self.POWER_BI_BASE_URL}/admin/workspaces/scanStatus/{scan_id}",
                api="powerbi",
            )
            status = str(status_payload.get("status", "")).lower()
            if status in {"succeeded", "success"}:
                return await self._request_json(
                    "GET",
                    f"{self.POWER_BI_BASE_URL}/admin/workspaces/scanResult/{scan_id}",
                    api="powerbi",
                )
            if status in {"failed", "error"}:
                raise RuntimeError(f"Power BI workspace scan failed: {status_payload}")
            await asyncio.sleep(3)

        raise TimeoutError(f"Timed out waiting for Power BI workspace scan {scan_id}")

    async def list_reports_in_group(self, workspace_id: str) -> List[Dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/groups/{workspace_id}/reports",
            api="powerbi",
        )
        return payload.get("value", [])

    async def list_dashboards_in_group(self, workspace_id: str) -> List[Dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/groups/{workspace_id}/dashboards",
            api="powerbi",
        )
        return payload.get("value", [])

    async def list_tiles_in_group(self, workspace_id: str, dashboard_id: str) -> List[Dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/groups/{workspace_id}/dashboards/{dashboard_id}/tiles",
            api="powerbi",
        )
        return payload.get("value", [])

    async def list_datasets_in_group(self, workspace_id: str) -> List[Dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/groups/{workspace_id}/datasets",
            api="powerbi",
        )
        return payload.get("value", [])

    async def list_dataflows_in_group(self, workspace_id: str) -> List[Dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/groups/{workspace_id}/dataflows",
            api="powerbi",
        )
        return payload.get("value", [])

    async def list_fabric_items(self, workspace_id: str, item_type: Optional[str] = None) -> List[Dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"{self.FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
            api="fabric",
            params={"type": item_type} if item_type else None,
        )
        return payload.get("value", [])

    async def get_dataset_datasources(self, workspace_id: str, dataset_id: str) -> List[Dict[str, Any]]:
        payload = await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/groups/{workspace_id}/datasets/{dataset_id}/datasources",
            api="powerbi",
        )
        return payload.get("value", [])

    async def get_dataset_refresh_schedule(self, workspace_id: str, dataset_id: str) -> Dict[str, Any]:
        return await self._request_json(
            "GET",
            f"{self.POWER_BI_BASE_URL}/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule",
            api="powerbi",
        )

    async def get_item_definition(
        self,
        workspace_id: str,
        item_id: str,
        *,
        format: Optional[str] = None,
    ) -> Dict[str, Any]:
        params = {"format": format} if format else None
        return await self._request_lro_json(
            "POST",
            f"{self.FABRIC_BASE_URL}/workspaces/{workspace_id}/items/{item_id}/getDefinition",
            api="fabric",
            params=params,
        )

    async def create_item(
        self,
        workspace_id: str,
        *,
        display_name: str,
        item_type: str,
        definition: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "displayName": display_name,
            "type": item_type,
        }
        if definition:
            payload["definition"] = definition
        if description:
            payload["description"] = description
        return await self._request_lro_json(
            "POST",
            f"{self.FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
            api="fabric",
            json=payload,
        )

    async def update_item_definition(
        self,
        workspace_id: str,
        item_id: str,
        definition: Dict[str, Any],
        *,
        update_metadata: bool = True,
    ) -> Dict[str, Any]:
        return await self._request_lro_json(
            "POST",
            f"{self.FABRIC_BASE_URL}/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
            api="fabric",
            params={"updateMetadata": str(update_metadata).lower()},
            json={"definition": definition},
        )

    async def rebind_report_in_group(self, workspace_id: str, report_id: str, dataset_id: str) -> None:
        await self._request_json(
            "POST",
            f"{self.POWER_BI_BASE_URL}/groups/{workspace_id}/reports/{report_id}/Rebind",
            api="powerbi",
            json={"datasetId": dataset_id},
        )
