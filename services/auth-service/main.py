"""Auth Service - Handles authentication and user management"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone
from urllib.parse import urlencode
import secrets

import httpx
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, String

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession, async_session_factory
from shared.models import PlatformUser, UserRoleMapping, Organization, UserRole, Tenant, TenantType, TenantStatus
from shared.security import create_access_token, create_refresh_token, decode_token, get_current_user_from_token
from shared.schemas import (
    UserResponse, LoginResponse, RefreshTokenRequest, RefreshTokenResponse,
    MicrosoftAuthUrlResponse, OAuthCallbackRequest,
    DatasourceConsentRequest, DatasourceCallbackResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Auth Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "auth"}


@app.get("/api/v1/auth/microsoft/url", response_model=MicrosoftAuthUrlResponse)
async def get_microsoft_login_url(state: Optional[str] = Query(None)):
    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.MICROSOFT_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": f"{settings.FRONTEND_URL}/auth/callback",
        "response_mode": "query",
        "scope": "openid profile email offline_access User.Read",
        "state": csrf_state,
    }
    auth_url = f"{settings.MICROSOFT_AUTH_URL}?{urlencode(params)}"
    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


@app.get("/api/v1/auth/microsoft/datasource/url", response_model=MicrosoftAuthUrlResponse)
async def get_datasource_url(state: Optional[str] = Query(None)):
    """
    Initiate admin-consent flow for M365 datasource onboarding.
    Redirects to /adminconsent endpoint — no user login required.
    After admin grants consent, redirects to /datasource-callback with ?tenant=...&admin_consent=True.
    """
    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.MICROSOFT_CLIENT_ID,
        "redirect_uri": f"{settings.FRONTEND_URL}/datasource-callback",
        "state": csrf_state,
    }
    # Use admin-consent endpoint — grants app-level (not user-delegated) permissions
    auth_url = f"https://login.microsoftonline.com/organizations/adminconsent?{urlencode(params)}"
    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


@app.get("/api/v1/auth/azure/datasource/url", response_model=MicrosoftAuthUrlResponse)
async def get_azure_datasource_url(state: Optional[str] = Query(None)):
    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.MICROSOFT_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": f"{settings.FRONTEND_URL}/azure-datasource-callback",
        "response_mode": "query",
        "scope": "https://management.azure.com/.default openid",
        "state": csrf_state,
    }
    # CRITICAL: Use /organizations for multi-tenant Azure datasource onboarding.
    # The MICROSOFT_AUTH_URL uses the specific tenant ID which only works for
    # users within that tenant. For external Azure tenants connecting as datasources,
    # we need /organizations to accept any Azure AD tenant.
    auth_url = f"https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize?{urlencode(params)}"
    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


@app.post("/api/v1/auth/callback", response_model=LoginResponse)
async def oauth_callback(callback: OAuthCallbackRequest, db: AsyncSession = Depends(get_db)):
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            settings.MICROSOFT_TOKEN_URL,
            data={
                "client_id": settings.MICROSOFT_CLIENT_ID,
                "client_secret": settings.MICROSOFT_CLIENT_SECRET,
                "code": callback.code,
                "redirect_uri": f"{settings.FRONTEND_URL}/auth/callback",
                "grant_type": "authorization_code",
            },
        )
        token_response.raise_for_status()
        tokens = token_response.json()
        
        graph_response = await client.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        graph_response.raise_for_status()
        profile = graph_response.json()
    
    email = profile.get("mail") or profile.get("userPrincipalName")
    name = profile.get("displayName", email.split("@")[0])
    external_id = profile.get("id")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    
    stmt = select(PlatformUser).where(PlatformUser.email == email)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    
    if user is None:
        org_stmt = select(Organization).limit(1)
        org_result = await db.execute(org_stmt)
        org = org_result.scalar_one_or_none()
        
        if org is None:
            org = Organization(
                id=UUID("00000000-0000-0000-0000-000000000001"),
                name="Taylor Morrison",
                slug="taylor-morrison",
            )
            db.add(org)
            await db.flush()
        
        user = PlatformUser(
            id=uuid4(),
            email=email,
            name=name,
            external_user_id=external_id,
            org_id=org.id,
        )
        db.add(user)
        db.add(UserRoleMapping(user_id=user.id, role=UserRole.USER))
        await db.flush()
    
    user.last_login_at = now
    user.updated_at = now
    await db.flush()
    
    roles_stmt = select(UserRoleMapping).where(UserRoleMapping.user_id == user.id)
    roles_result = await db.execute(roles_stmt)
    user_roles = [r.role.value for r in roles_result.scalars().all()]
    
    token_data = {
        "sub": str(user.id),
        "email": user.email,
        "roles": user_roles,
        "orgId": str(user.org_id) if user.org_id else None,
        "tenantIds": [str(user.tenant_id)] if user.tenant_id else [],
    }
    
    access_token = create_access_token(token_data)
    refresh_token = create_refresh_token(token_data)
    expires_in = settings.JWT_EXPIRATION_HOURS * 3600
    
    return LoginResponse(
        accessToken=access_token,
        refreshToken=refresh_token,
        expiresIn=expires_in,
        user=UserResponse(
            id=str(user.id),
            email=user.email,
            name=user.name,
            roles=user_roles,
            organizationId=str(user.org_id) if user.org_id else "",
            tenantId=str(user.tenant_id) if user.tenant_id else None,
        ),
    )


@app.post("/api/v1/auth/microsoft/datasource/callback", response_model=DatasourceCallbackResponse)
async def datasource_callback(
    callback: DatasourceConsentRequest,
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Handle Microsoft admin-consent callback for M365 datasource onboarding.
    
    Flow:
    1. Validate CSRF state
    2. Verify admin consent was granted
    3. Test client-credentials flow against the newly-consented tenant
    4. Store encrypted Graph API credentials on the Tenant row
    5. Publish discovery.m365 message to RabbitMQ
    """
    # 1. Validate CSRF state
    from shared.config import settings as app_settings
    if app_settings.REDIS_ENABLED:
        try:
            import redis.asyncio as aioredis
            redis_client = aioredis.from_url(
                f"redis://{app_settings.REDIS_HOST}:{app_settings.REDIS_PORT}/{app_settings.REDIS_DB}",
                decode_responses=True,
            )
            expected_state = await redis_client.get(f"oauth_state:{current_user['id']}")
            await redis_client.delete(f"oauth_state:{current_user['id']}")
            await redis_client.aclose()
            if not expected_state or expected_state != callback.state:
                raise HTTPException(400, "Invalid or expired state token")
        except Exception:
            # Redis unavailable — skip CSRF check (dev mode)
            pass

    if not callback.admin_consent:
        raise HTTPException(400, "Admin consent was not granted")

    external_tenant_id = callback.external_tenant_id

    # 2. Test client-credentials flow against the newly-consented tenant
    async with httpx.AsyncClient(timeout=30.0) as client:
        token_url = f"https://login.microsoftonline.com/{external_tenant_id}/oauth2/v2.0/token"
        token_resp = await client.post(token_url, data={
            "client_id": settings.MICROSOFT_CLIENT_ID,
            "client_secret": settings.MICROSOFT_CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        })
        if token_resp.status_code != 200:
            raise HTTPException(400,
                f"Consent verification failed: {token_resp.text}. "
                "Confirm the app has required application permissions and the admin granted consent.")
        app_token = token_resp.json()["access_token"]

        # 3. Fetch organization info using the app token
        org_resp = await client.get(
            "https://graph.microsoft.com/v1.0/organization",
            headers={"Authorization": f"Bearer {app_token}"},
        )
        org_resp.raise_for_status()
        org_data = org_resp.json()
        display_name = (org_data["value"][0]["displayName"]
                        if org_data.get("value") else "M365 Tenant")

    # 4. Upsert tenant with CREDENTIALS STORED (encrypted)
    from shared.security import encrypt_secret
    encrypted_secret = encrypt_secret(settings.MICROSOFT_CLIENT_SECRET)

    stmt = select(Tenant).where(Tenant.external_tenant_id == external_tenant_id)
    tenant = (await db.execute(stmt)).scalar_one_or_none()

    if tenant is None:
        org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None

        # Ensure org exists
        if org_id:
            org_stmt = select(Organization).where(Organization.id == org_id)
            org_result = await db.execute(org_stmt)
            org = org_result.scalar_one_or_none()
            if org is None:
                org = Organization(
                    id=org_id,
                    name="Default Organization",
                    slug="default-org",
                )
                db.add(org)
                await db.flush()

        tenant = Tenant(
            id=uuid4(),
            org_id=org_id,
            type=TenantType.M365,
            display_name=display_name,
            external_tenant_id=external_tenant_id,
            graph_client_id=settings.MICROSOFT_CLIENT_ID,
            graph_client_secret_encrypted=encrypted_secret,
            graph_delta_tokens={},
            status=TenantStatus.ACTIVE,
        )
        db.add(tenant)
    else:
        tenant.graph_client_id = settings.MICROSOFT_CLIENT_ID
        tenant.graph_client_secret_encrypted = encrypted_secret
        tenant.status = TenantStatus.ACTIVE

    await db.flush()
    tenant_id = tenant.id
    await db.commit()

    # 5. PUBLISH DISCOVERY JOB — critical missing step
    # Uses canonical signature: publish(routing_key: str, message: Dict, priority: int = 5)
    # Matching: services/job-service/main.py:200, services/backup-scheduler/main.py:226
    from shared.message_bus import message_bus as msg_bus
    if not msg_bus.connection:
        await msg_bus.connect()

    discovery_status = "queued"
    try:
        discovery_message = {
            "jobId": str(uuid4()),
            "tenantId": str(tenant_id),
            "externalTenantId": external_tenant_id,
            "discoveryScope": ["users", "groups", "mailboxes", "shared_mailboxes",
                               "onedrive", "sharepoint", "teams"],
            "triggeredBy": str(current_user["id"]),
            "triggeredAt": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        }
        print(f"[auth-service] Publishing discovery.m365 for tenant {tenant_id} ({display_name})")
        await msg_bus.publish("discovery.m365", discovery_message, priority=5)
        print(f"[auth-service] Published discovery.m365 successfully for tenant {tenant_id}")
    except Exception as e:
        import logging
        logging.getLogger("auth-service").exception(
            "Failed to publish discovery.m365 for tenant %s", tenant_id
        )
        # Mark tenant so a reconciler job picks it up
        async with async_session_factory() as retry_session:
            retry_tenant = await retry_session.get(Tenant, tenant_id)
            if retry_tenant:
                retry_tenant.status = TenantStatus.PENDING_DISCOVERY
                await retry_session.commit()
        discovery_status = "queue_failed_will_retry"

    return DatasourceCallbackResponse(
        tenantId=str(tenant_id),
        tenantName=display_name,
        discoveryStatus=discovery_status,
    )


@app.post("/api/v1/auth/azure/datasource/callback")
async def azure_datasource_callback(
    callback: OAuthCallbackRequest,
    current_user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    # Exchange code for tokens (Azure ARM scope)
    # CRITICAL: Use /organizations for token exchange since the auth URL used /organizations
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            "https://login.microsoftonline.com/organizations/oauth2/v2.0/token",
            data={
                "client_id": settings.MICROSOFT_CLIENT_ID,
                "client_secret": settings.MICROSOFT_CLIENT_SECRET,
                "code": callback.code,
                "redirect_uri": f"{settings.FRONTEND_URL}/azure-datasource-callback",
                "grant_type": "authorization_code",
            },
        )
        token_response.raise_for_status()
        tokens = token_response.json()

        # Get user info from Graph API (need to use the same token - if it has Graph scopes it will work)
        # If the token is only for ARM, we need to decode the JWT to get user info
        from jose import jwt
        from jose.exceptions import JWTError
        
        # Try to decode the access token to get user info
        display_name = "Azure Tenant"
        external_tenant_id = None
        
        try:
            # Decode JWT without verification to extract claims
            decoded = jwt.decode(tokens.get("access_token", ""), key="", algorithms=["RS256", "HS256"], options={"verify_signature": False, "verify_aud": False})
            display_name = decoded.get("name") or decoded.get("unique_name") or decoded.get("email") or "Azure Tenant"
            external_tenant_id = decoded.get("tid")  # Azure AD tenant ID
            
            if not external_tenant_id:
                # Try calling Azure ARM to get subscription info
                arm_response = await client.get(
                    "https://management.azure.com/subscriptions?api-version=2022-12-01",
                    headers={"Authorization": f"Bearer {tokens['access_token']}"},
                )
                if arm_response.status_code == 200:
                    arm_data = arm_response.json()
                    if arm_data.get("value"):
                        # Use first subscription's tenant ID
                        external_tenant_id = arm_data["value"][0].get("tenantId")
                        display_name = f"Azure - {arm_data['value'][0].get('displayName', 'Subscription')}"
        except JWTError:
            # If JWT decode fails, try ARM API
            try:
                arm_response = await client.get(
                    "https://management.azure.com/subscriptions?api-version=2022-12-01",
                    headers={"Authorization": f"Bearer {tokens['access_token']}"},
                )
                if arm_response.status_code == 200:
                    arm_data = arm_response.json()
                    if arm_data.get("value"):
                        external_tenant_id = arm_data["value"][0].get("tenantId")
                        display_name = f"Azure - {arm_data['value'][0].get('displayName', 'Subscription')}"
            except Exception:
                pass

    # Check if tenant already exists for this org
    org_id = UUID(current_user["org_id"]) if current_user.get("org_id") else None

    # CRITICAL: Query by external_tenant_id WITHOUT type filter.
    # The same Azure AD tenant ID is used for both M365 and Azure datasources.
    # If M365 was onboarded first, a tenant with this external_tenant_id exists as type M365.
    # We should reuse that tenant or upgrade it to BOTH, not create a duplicate.
    if external_tenant_id:
        stmt = select(Tenant).where(Tenant.external_tenant_id == external_tenant_id)
    else:
        stmt = select(Tenant).where(
            Tenant.type.cast(String) == "AZURE",
            Tenant.org_id == org_id,
        )

    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()

    if tenant is None:
        # Ensure org exists
        if org_id:
            org_stmt = select(Organization).where(Organization.id == org_id)
            org_result = await db.execute(org_stmt)
            org = org_result.scalar_one_or_none()
            if org is None:
                org = Organization(
                    id=org_id,
                    name="Taylor Morrison",
                    slug="taylor-morrison",
                )
                db.add(org)
                await db.flush()

        tenant = Tenant(
            id=uuid4(),
            org_id=org_id,
            type=TenantType.AZURE,
            display_name=display_name,
            external_tenant_id=external_tenant_id,
            status=TenantStatus.ACTIVE,
        )
        db.add(tenant)
        await db.commit()
        print(f"[auth-service] Created Azure tenant: {tenant.id} ({display_name}), external_tenant_id={external_tenant_id}")
    else:
        print(f"[auth-service] Azure tenant already exists: {tenant.id} ({tenant.display_name}), type={tenant.type}")
        # If existing tenant is M365 type, upgrade to BOTH (M365 + Azure connected)
        if tenant.type == TenantType.M365:
            tenant.type = TenantType.BOTH
            print(f"[auth-service] Upgraded tenant {tenant.id} from M365 to BOTH")
            await db.commit()

    # Publish discovery.azure message so the discovery worker picks up Azure resources
    from shared.message_bus import message_bus as msg_bus
    if not msg_bus.connection:
        await msg_bus.connect()

    discovery_status = "queued"
    try:
        discovery_message = {
            "jobId": str(uuid4()),
            "tenantId": str(tenant.id),
            "externalTenantId": external_tenant_id or "",
            "discoveryScope": ["azure_vms", "azure_sql", "azure_postgresql"],
            "triggeredBy": str(current_user["id"]) if current_user.get("id") else "system",
            "triggeredAt": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        }
        print(f"[auth-service] Publishing discovery.azure for tenant {tenant.id} ({tenant.display_name})")
        await msg_bus.publish("discovery.azure", discovery_message, priority=5)
        print(f"[auth-service] Published discovery.azure successfully for tenant {tenant.id}")
    except Exception as e:
        import logging
        logging.getLogger("auth-service").exception(
            "Failed to publish discovery.azure for tenant %s", tenant.id
        )
        discovery_status = "queue_failed_will_retry"

    return {"tenantId": str(tenant.id), "tenantName": tenant.display_name, "discoveryStatus": discovery_status}


@app.post("/api/v1/auth/refresh", response_model=RefreshTokenResponse)
async def refresh_token(request: RefreshTokenRequest):
    payload = decode_token(request.refreshToken)
    token_data = {
        "sub": payload.get("sub"),
        "email": payload.get("email"),
        "roles": payload.get("roles", []),
        "orgId": payload.get("orgId"),
        "tenantIds": payload.get("tenantIds", []),
    }
    access_token = create_access_token(token_data)
    new_refresh_token = create_refresh_token(token_data)
    expires_in = settings.JWT_EXPIRATION_HOURS * 3600
    return RefreshTokenResponse(accessToken=access_token, refreshToken=new_refresh_token, expiresIn=expires_in)


@app.post("/api/v1/auth/logout")
async def logout():
    return {"message": "Logged out successfully"}


@app.get("/api/v1/auth/me", response_model=UserResponse)
async def get_me(
    user: dict = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(PlatformUser).where(PlatformUser.id == UUID(user["id"]))
    result = await db.execute(stmt)
    platform_user = result.scalar_one_or_none()
    
    if not platform_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return UserResponse(
        id=str(platform_user.id),
        email=platform_user.email,
        name=platform_user.name,
        roles=user["roles"],
        organizationId=str(platform_user.org_id) if platform_user.org_id else "",
        tenantId=str(platform_user.tenant_id) if platform_user.tenant_id else None,
    )
