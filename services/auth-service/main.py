"""Auth Service - Handles authentication and user management"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone
from urllib.parse import urlencode
import secrets

import httpx
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import PlatformUser, UserRoleMapping, Organization, UserRole
from shared.security import create_access_token, create_refresh_token, decode_token, get_current_user_from_token
from shared.schemas import (
    UserResponse, LoginResponse, RefreshTokenRequest, RefreshTokenResponse,
    MicrosoftAuthUrlResponse, OAuthCallbackRequest
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
        "redirect_uri": "http://localhost:4200/auth/callback",
        "response_mode": "query",
        "scope": "openid profile email offline_access User.Read",
        "state": csrf_state,
    }
    auth_url = f"{settings.MICROSOFT_AUTH_URL}?{urlencode(params)}"
    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


@app.get("/api/v1/auth/microsoft/datasource/url", response_model=MicrosoftAuthUrlResponse)
async def get_datasource_url(state: Optional[str] = Query(None)):
    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.MICROSOFT_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": "http://localhost:4200/datasource-callback",
        "response_mode": "query",
        "scope": "openid profile email offline_access User.Read.All Group.Read.All Mail.Read Files.Read.All Sites.Read.All",
        "state": csrf_state,
    }
    auth_url = f"{settings.MICROSOFT_AUTH_URL}?{urlencode(params)}"
    return MicrosoftAuthUrlResponse(url=auth_url, state=csrf_state)


@app.get("/api/v1/auth/azure/datasource/url", response_model=MicrosoftAuthUrlResponse)
async def get_azure_datasource_url(state: Optional[str] = Query(None)):
    csrf_state = state or secrets.token_urlsafe(32)
    params = {
        "client_id": settings.MICROSOFT_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": "http://localhost:4200/azure-datasource-callback",
        "response_mode": "query",
        "scope": "https://management.azure.com/.default openid",
        "state": csrf_state,
    }
    auth_url = f"{settings.MICROSOFT_AUTH_URL}?{urlencode(params)}"
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
                "redirect_uri": "http://localhost:4200/auth/callback",
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
    
    user.last_login_at = datetime.now(timezone.utc)
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


@app.post("/api/v1/auth/microsoft/datasource/callback")
async def datasource_callback(callback: OAuthCallbackRequest):
    return {"tenantId": str(uuid4()), "tenantName": "New Tenant"}


@app.post("/api/v1/auth/azure/datasource/callback")
async def azure_datasource_callback(callback: OAuthCallbackRequest):
    return {"tenantId": str(uuid4()), "tenantName": "Azure Tenant"}


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
