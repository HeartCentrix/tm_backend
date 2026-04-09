"""Authentication routes"""
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

from app.config import settings
from app.db.database import get_db, AsyncSession
from app.db import models
from app.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
)
from app.schemas import (
    MicrosoftAuthUrlResponse,
    OAuthCallbackRequest,
    LoginResponse,
    UserResponse,
    RefreshTokenRequest,
    RefreshTokenResponse,
)

router = APIRouter()


@router.get("/microsoft/url", response_model=MicrosoftAuthUrlResponse)
async def get_microsoft_login_url(state: Optional[str] = Query(None)):
    """Generate Microsoft OAuth2 login URL"""
    import secrets
    
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


@router.get("/microsoft/datasource/url", response_model=MicrosoftAuthUrlResponse)
async def get_datasource_login_url(state: Optional[str] = Query(None)):
    """Generate Microsoft OAuth2 URL for adding datasource"""
    import secrets
    
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


@router.get("/azure/datasource/url", response_model=MicrosoftAuthUrlResponse)
async def get_azure_datasource_url(state: Optional[str] = Query(None)):
    """Generate Azure OAuth2 URL for adding datasource"""
    import secrets
    
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


@router.post("/callback", response_model=LoginResponse)
async def oauth_callback(callback: OAuthCallbackRequest, db: AsyncSession = Depends(get_db)):
    """Handle OAuth2 callback and issue platform JWT"""
    async with httpx.AsyncClient() as client:
        # Exchange code for tokens
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
        
        # Fetch user profile from Graph API
        graph_response = await client.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        graph_response.raise_for_status()
        profile = graph_response.json()
    
    email = profile.get("mail") or profile.get("userPrincipalName")
    name = profile.get("displayName", email.split("@")[0])
    external_id = profile.get("id")
    
    # Upsert user
    stmt = select(models.PlatformUser).where(models.PlatformUser.email == email)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    
    if user is None:
        # Get or create default organization
        org_stmt = select(models.Organization).limit(1)
        org_result = await db.execute(org_stmt)
        org = org_result.scalar_one_or_none()
        
        if org is None:
            org = models.Organization(
                id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
                name="Taylor Morrison",
                slug="taylor-morrison",
            )
            db.add(org)
            await db.flush()
        
        user = models.PlatformUser(
            id=uuid.uuid4(),
            email=email,
            name=name,
            external_user_id=external_id,
            org_id=org.id,
        )
        db.add(user)
        
        # Assign default role
        role = models.UserRoleMapping(user_id=user.id, role=models.UserRole.USER)
        db.add(role)
        await db.flush()
    
    user.last_login_at = datetime.now(timezone.utc)
    await db.flush()
    
    # Get user roles
    roles_stmt = select(models.UserRoleMapping).where(
        models.UserRoleMapping.user_id == user.id
    )
    roles_result = await db.execute(roles_stmt)
    user_roles = [r.role.value for r in roles_result.scalars().all()]
    
    # Create JWT tokens
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


@router.post("/microsoft/datasource/callback")
async def datasource_callback(
    callback: OAuthCallbackRequest,
    current_user: models.PlatformUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Handle Microsoft OAuth callback for datasource connection"""
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            settings.MICROSOFT_TOKEN_URL,
            data={
                "client_id": settings.MICROSOFT_CLIENT_ID,
                "client_secret": settings.MICROSOFT_CLIENT_SECRET,
                "code": callback.code,
                "redirect_uri": "http://localhost:4200/datasource-callback",
                "grant_type": "authorization_code",
            },
        )
        token_response.raise_for_status()
        tokens = token_response.json()
        
        graph_response = await client.get(
            "https://graph.microsoft.com/v1.0/organization",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        graph_response.raise_for_status()
        org_data = graph_response.json()
    
    external_tenant_id = org_data["value"][0]["id"] if org_data.get("value") else None
    display_name = org_data["value"][0]["displayName"] if org_data.get("value") else "New Tenant"
    
    # Check if tenant exists
    stmt = select(models.Tenant).where(
        models.Tenant.external_tenant_id == external_tenant_id
    )
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    
    if tenant is None:
        tenant = models.Tenant(
            id=uuid.uuid4(),
            org_id=current_user.org_id,
            type=models.TenantType.M365,
            display_name=display_name,
            external_tenant_id=external_tenant_id,
            status=models.TenantStatus.PENDING,
        )
        db.add(tenant)
        await db.flush()
    
    return {"tenantId": str(tenant.id), "tenantName": tenant.display_name}


@router.post("/azure/datasource/callback")
async def azure_datasource_callback(
    callback: OAuthCallbackRequest,
    current_user: models.PlatformUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Handle Azure OAuth callback for datasource connection"""
    # Similar to Microsoft callback but for Azure
    return {"tenantId": str(uuid.uuid4()), "tenantName": "Azure Tenant"}


@router.post("/refresh", response_model=RefreshTokenResponse)
async def refresh_token(request: RefreshTokenRequest):
    """Refresh access token using refresh token"""
    payload = decode_token(request.refreshToken)
    
    user_id = payload.get("sub")
    email = payload.get("email")
    roles = payload.get("roles", [])
    org_id = payload.get("orgId")
    tenant_ids = payload.get("tenantIds", [])
    
    token_data = {
        "sub": user_id,
        "email": email,
        "roles": roles,
        "orgId": org_id,
        "tenantIds": tenant_ids,
    }
    
    access_token = create_access_token(token_data)
    new_refresh_token = create_refresh_token(token_data)
    expires_in = settings.JWT_EXPIRATION_HOURS * 3600
    
    return RefreshTokenResponse(
        accessToken=access_token,
        refreshToken=new_refresh_token,
        expiresIn=expires_in,
    )


@router.post("/logout")
async def logout():
    """Logout - in production, blacklist token in Redis"""
    # Simplified: just return success
    # In production: extract JTI from token and add to Redis blacklist
    return {"message": "Logged out successfully"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: models.PlatformUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get current user profile"""
    roles_stmt = select(models.UserRoleMapping).where(
        models.UserRoleMapping.user_id == current_user.id
    )
    roles_result = await db.execute(roles_stmt)
    user_roles = [r.role.value for r in roles_result.scalars().all()]
    
    return UserResponse(
        id=str(current_user.id),
        email=current_user.email,
        name=current_user.name,
        roles=user_roles,
        organizationId=str(current_user.org_id) if current_user.org_id else "",
        tenantId=str(current_user.tenant_id) if current_user.tenant_id else None,
    )
