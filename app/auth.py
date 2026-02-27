from __future__ import annotations

import os
from typing import Optional, Dict, Any
from itsdangerous import URLSafeSerializer, BadSignature
from passlib.context import CryptContext

COOKIE_NAME = "tg_broadcast_session"
COOKIE_MAX_AGE = 60 * 60 * 24 * 30  # 30 days

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _secret() -> str:
    return os.environ.get("SECRET_KEY", "change-me-please-very-secret")


def _serializer() -> URLSafeSerializer:
    return URLSafeSerializer(_secret(), salt="tg-broadcast-session-v2")


def hash_password(password: str) -> str:
    """
    bcrypt has a 72-byte effective input limit.
    We truncate to 72 bytes to avoid runtime errors.
    """
    if password is None:
        password = ""
    pw_bytes = password.encode("utf-8")
    if len(pw_bytes) > 72:
        pw_bytes = pw_bytes[:72]
        password = pw_bytes.decode("utf-8", errors="ignore")
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return pwd_context.verify(password or "", password_hash or "")
    except Exception:
        return False


def build_session(user: Dict[str, Any]) -> str:
    payload = {
        "uid": int(user["id"]),
        "role": user.get("role"),
        "username": user.get("username"),
    }
    return _serializer().dumps(payload)


def read_session(token: str) -> Optional[Dict[str, Any]]:
    if not token:
        return None
    try:
        data = _serializer().loads(token)
        if not isinstance(data, dict) or "uid" not in data:
            return None
        return data
    except BadSignature:
        return None


def get_session_data(request) -> Optional[Dict[str, Any]]:
    token = request.cookies.get(COOKIE_NAME)
    return read_session(token or "")


def set_session_cookie(response, user: Dict[str, Any]) -> None:
    token = build_session(user)
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=False,  # set True if behind HTTPS
        path="/",
    )


def clear_session_cookie(response) -> None:
    response.delete_cookie(COOKIE_NAME, path="/")
