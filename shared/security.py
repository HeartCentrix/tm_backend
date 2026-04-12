"""Shared security utilities"""
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

from shared.config import settings

security = HTTPBearer()


# ==================== Secret Encryption ====================

def _get_fernet() -> Fernet:
    """Get Fernet cipher instance from ENCRYPTION_KEY env var."""
    key = settings.ENCRYPTION_KEY
    if not key:
        # Generate a deterministic key from JWT_SECRET as fallback (dev only)
        import hashlib
        import base64
        raw = hashlib.sha256((settings.JWT_SECRET or "dev-fallback-key-never-use-in-prod").encode()).digest()
        key = base64.urlsafe_b64encode(raw).decode()
    # Ensure key is valid Fernet key (base64-encoded 32 bytes)
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception:
        raise RuntimeError("Invalid ENCRYPTION_KEY. Must be a base64-encoded 32-byte key.")


def encrypt_secret(plaintext: str) -> bytes:
    """Encrypt a secret string using Fernet symmetric encryption.
    
    Returns ciphertext as bytes.
    """
    f = _get_fernet()
    return f.encrypt(plaintext.encode('utf-8'))


def decrypt_secret(ciphertext: bytes) -> str:
    """Decrypt a previously encrypted secret string.
    
    Returns the original plaintext string.
    """
    f = _get_fernet()
    return f.decrypt(ciphertext).decode('utf-8')


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(hours=settings.JWT_EXPIRATION_HOURS))
    to_encode.update({"exp": expire, "jti": str(uuid.uuid4())})
    return jwt.encode(to_encode, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=settings.JWT_REFRESH_EXPIRATION_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_current_user_from_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    """Extract user from JWT token (used by API Gateway)"""
    token = credentials.credentials
    payload = decode_token(token)
    
    user_id: str = payload.get("sub")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    return {
        "id": user_id,
        "email": payload.get("email"),
        "roles": payload.get("roles", []),
        "org_id": payload.get("orgId"),
        "tenant_ids": payload.get("tenantIds", []),
    }
