import base64
import hashlib
import hmac
import os
import secrets
import time
from uuid import UUID

import jwt
from fastapi import HTTPException, Request, status

APP_SECRET = os.getenv("APP_SECRET", "")
SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "28800"))
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "true").lower() == "true"


def ensure_security_config() -> None:
    if len(APP_SECRET) < 32:
        raise RuntimeError("APP_SECRET must contain at least 32 characters")


def hash_password(password: str, salt: bytes | None = None) -> tuple[bytes, bytes]:
    if len(password) < 12:
        raise HTTPException(422, "Password must be at least 12 characters")
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.scrypt(
        password.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32
    )
    return salt, digest


def verify_password(password: str, salt: bytes, expected: bytes) -> bool:
    _, actual = hash_password(password, salt)
    return hmac.compare_digest(actual, expected)


def issue_session(user_id: UUID, tenant_id: UUID) -> str:
    now = int(time.time())
    return jwt.encode(
        {"sub": str(user_id), "tenant_id": str(tenant_id), "iat": now, "exp": now + SESSION_TTL_SECONDS},
        APP_SECRET,
        algorithm="HS256",
    )


def current_identity(request: Request) -> tuple[UUID, UUID]:
    token = request.cookies.get("cdc_session")
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Login required")
    try:
        claims = jwt.decode(token, APP_SECRET, algorithms=["HS256"])
        return UUID(claims["sub"]), UUID(claims["tenant_id"])
    except Exception as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired session") from exc


def csrf_token(user_id: UUID, tenant_id: UUID) -> str:
    raw = f"{user_id}:{tenant_id}".encode()
    return base64.urlsafe_b64encode(hmac.new(APP_SECRET.encode(), raw, hashlib.sha256).digest()).decode()


def require_csrf(request: Request, user_id: UUID, tenant_id: UUID) -> None:
    supplied = request.headers.get("x-csrf-token", "")
    if not hmac.compare_digest(supplied, csrf_token(user_id, tenant_id)):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid CSRF token")
