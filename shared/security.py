"""Shared security utilities."""
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

try:
    from fastapi import Depends, HTTPException, status
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
except ModuleNotFoundError:  # pragma: no cover - worker-only runtimes
    Depends = None
    HTTPException = None
    status = None
    HTTPBearer = None
    HTTPAuthorizationCredentials = Any

from shared.config import settings

security = HTTPBearer() if HTTPBearer else None


# ==================== Secret Encryption ====================

def _get_fernet():
    """Get Fernet cipher instance from ENCRYPTION_KEY env var.

    ENCRYPTION_KEY MUST be a separate random secret from JWT_SECRET. Deriving
    one from the other means a single env-var leak (SSRF, leaked .env, log
    capture) decrypts every stored secret. Generate with:
        python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    """
    from cryptography.fernet import Fernet

    key = settings.ENCRYPTION_KEY
    if not key:
        raise RuntimeError(
            "ENCRYPTION_KEY must be set. Provision a separate random 32-byte "
            "Fernet key in every environment (including CI) — do NOT reuse or "
            "derive from JWT_SECRET."
        )
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception:
        raise RuntimeError("Invalid ENCRYPTION_KEY. Must be a base64-encoded 32-byte Fernet key.")


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
    from jose import jwt

    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(hours=settings.JWT_EXPIRATION_HOURS))
    to_encode.update({"exp": expire, "jti": str(uuid.uuid4()), "type": "access"})
    return jwt.encode(to_encode, settings.ACCESS_TOKEN_SECRET, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(data: dict) -> str:
    from jose import jwt

    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=settings.JWT_REFRESH_EXPIRATION_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, settings.REFRESH_TOKEN_SECRET, algorithm=settings.JWT_ALGORITHM)


def _unauthorized(detail: str):
    if HTTPException is None or status is None:
        raise RuntimeError("JWT decoding requires FastAPI runtime support")
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def decode_token(token: str, expected_type: str = "access") -> dict:
    from jose import JWTError, jwt

    if expected_type == "access":
        secret = settings.ACCESS_TOKEN_SECRET
    elif expected_type == "refresh":
        secret = settings.REFRESH_TOKEN_SECRET
    else:
        raise ValueError(f"Unknown token type: {expected_type!r}")

    try:
        payload = jwt.decode(token, secret, algorithms=[settings.JWT_ALGORITHM])
    except JWTError:
        raise _unauthorized("Invalid or expired token")

    if payload.get("type") != expected_type:
        raise _unauthorized("Invalid token type")

    return payload


def _get_user_from_token(token: str) -> dict:
    payload = decode_token(token, expected_type="access")

    user_id: str = payload.get("sub")
    if user_id is None:
        if HTTPException is None:
            raise RuntimeError("Token validation requires FastAPI runtime support")
        raise HTTPException(status_code=401, detail="Invalid token")

    return {
        "id": user_id,
        "email": payload.get("email"),
        "roles": payload.get("roles", []),
        "org_id": payload.get("orgId"),
        "tenant_ids": payload.get("tenantIds", []),
    }


if Depends is not None and security is not None:
    def get_current_user_from_token(
        credentials: HTTPAuthorizationCredentials = Depends(security),
    ) -> dict:
        """Extract user from JWT token (used by API Gateway)."""
        return _get_user_from_token(credentials.credentials)
else:  # pragma: no cover - non-API runtimes should not call this
    def get_current_user_from_token(credentials: Optional[Any] = None) -> dict:
        raise RuntimeError("FastAPI authentication helpers are unavailable in this runtime")
