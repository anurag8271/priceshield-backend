"""
PriceShield v2 — auth.py
JWT creation, verification, and FastAPI dependency injection.
"""
from __future__ import annotations
import time
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from loguru import logger
from pydantic import BaseModel

from cache import CacheManager, get_cache

import os

_JWT_SECRET    = os.environ.get("JWT_SECRET", "priceshield-default-secret-change-in-production")
_JWT_ALGORITHM = "HS256"
_JWT_EXPIRE    = int(os.environ.get("JWT_EXPIRE_MIN", 10080)) * 60  # 7 days in seconds

_bearer = HTTPBearer(auto_error=False)


# ── Token models ─────────────────────────────────────────────

class AuthUser(BaseModel):
    user_id:  str
    phone:    str
    is_guest: bool  = False
    token:    str   = ""


# ── Token creation ───────────────────────────────────────────

def create_access_token(user_id: str, phone: str, is_guest: bool = False) -> str:
    now = int(time.time())
    payload = {
        "sub":      user_id,
        "phone":    phone,
        "iat":      now,
        "exp":      now + _JWT_EXPIRE,
        "is_guest": is_guest,
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm=_JWT_ALGORITHM)


def _decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, _JWT_SECRET, algorithms=[_JWT_ALGORITHM])
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token. Please log in again.",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ── FastAPI dependencies ─────────────────────────────────────

async def require_auth(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    cache: CacheManager = Depends(get_cache),
) -> AuthUser:
    """Dependency: route requires a valid Bearer token."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please log in.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token   = credentials.credentials
    payload = _decode_token(token)
    return AuthUser(
        user_id  = payload["sub"],
        phone    = payload.get("phone", ""),
        is_guest = payload.get("is_guest", False),
        token    = token,
    )


async def optional_auth(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
    cache: CacheManager = Depends(get_cache),
) -> Optional[AuthUser]:
    """Dependency: auth is optional. Returns None for unauthenticated requests."""
    if not credentials:
        return None
    try:
        return await require_auth(credentials, cache)
    except HTTPException:
        return None


async def require_verified_user(user: AuthUser = Depends(require_auth)) -> AuthUser:
    """Dependency: requires a registered (non-guest) account."""
    if user.is_guest:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This feature requires a registered account. Please sign in.",
        )
    return user


async def revoke_token(token: str, cache: CacheManager) -> bool:
    """Remove a session token from cache (logout)."""
    return await cache.delete_session(token)


# ── Rate limiter ─────────────────────────────────────────────

class RateLimit:
    """
    Configurable per-IP rate limit dependency.

    Usage:
        @app.post("/api/auth/otp/send", dependencies=[Depends(RateLimit(3, 60))])
    """
    def __init__(self, max_requests: int = 10, window_seconds: int = 60):
        self.max_req = max_requests
        self.window  = window_seconds

    async def __call__(
        self,
        request: Request,
        cache: CacheManager = Depends(get_cache),
    ) -> None:
        ip       = request.client.host if request.client else "unknown"
        endpoint = request.url.path.replace("/", "_")
        allowed  = await cache.check_rate_limit(ip, endpoint, self.max_req, self.window)
        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Too many requests. Try again in {self.window} seconds.",
                headers={"Retry-After": str(self.window)},
            )
