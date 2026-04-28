"""Session cookie auth for internal deployments (DigitalOcean, etc.)."""

from __future__ import annotations

import os
import threading
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from backend.app.services.simple_accounts import verify_credentials

# Per-IP failed-login window (no Redis) — slows password stuffing on small deploys.
_FAILURE_WINDOW_SEC = 300.0
_FAILURE_MAX = 30
_failures: dict[str, list[float]] = {}
_fail_lock = threading.Lock()

router = APIRouter(prefix="/api/auth", tags=["auth"])

SESSION_COOKIE = "figwork_session"
SESSION_MAX_AGE = 14 * 24 * 3600


def auth_enabled() -> bool:
    return os.getenv("FIGWORK_AUTH_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")


def auth_secret() -> str:
    s = os.getenv("FIGWORK_AUTH_SECRET", "").strip()
    if auth_enabled() and len(s) < 32:
        raise RuntimeError(
            "FIGWORK_AUTH_SECRET must be set to at least 32 random bytes when FIGWORK_AUTH_ENABLED=1."
        )
    return s


def _serializer():
    from itsdangerous import URLSafeTimedSerializer

    return URLSafeTimedSerializer(auth_secret(), salt="figwork-auth-session")


def create_session_token(payload: dict[str, Any]) -> str:
    return _serializer().dumps(payload)


def read_session_token(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        return _serializer().loads(raw, max_age=SESSION_MAX_AGE)
    except Exception:
        return None


def session_user(request: Request) -> dict[str, Any] | None:
    raw = request.cookies.get(SESSION_COOKIE)
    data = read_session_token(raw)
    if not data or not isinstance(data, dict):
        return None
    u = data.get("username")
    if not isinstance(u, str) or not u.strip():
        return None
    return {"username": u.strip(), "display_name": str(data.get("display_name") or u).strip()}


class LoginBody(BaseModel):
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=256)


def _client_ip(request: Request) -> str:
    fwd = (request.headers.get("x-forwarded-for") or "").strip()
    if fwd:
        return fwd.split(",")[0].strip()[:64]
    if request.client and request.client.host:
        return request.client.host[:64]
    return "unknown"


def _register_failed_login(ip: str) -> None:
    """Record a failed attempt; raise ``429`` if this IP has exceeded the window."""
    now = time.monotonic()
    with _fail_lock:
        bucket = _failures.setdefault(ip, [])
        while bucket and bucket[0] < now - _FAILURE_WINDOW_SEC:
            bucket.pop(0)
        if len(bucket) >= _FAILURE_MAX:
            raise HTTPException(
                status_code=429,
                detail="Too many failed login attempts from this network; try again later.",
            )
        bucket.append(now)


@router.get("/me")
def me(request: Request) -> dict[str, Any]:
    if not auth_enabled():
        return {"username": "anonymous", "display_name": "Anonymous", "auth_disabled": True}
    user = session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {
        "username": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "auth_disabled": False,
    }


@router.post("/login")
def login(request: Request, response: Response, body: LoginBody) -> dict[str, Any]:
    if not auth_enabled():
        raise HTTPException(status_code=400, detail="Authentication is disabled on this server.")
    acct = verify_credentials(body.username, body.password)
    if not acct:
        _register_failed_login(_client_ip(request))
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = create_session_token(
        {"username": acct["username"], "display_name": acct.get("display_name") or acct["username"]}
    )
    secure = os.getenv("FIGWORK_COOKIE_SECURE", "").strip().lower() in ("1", "true", "yes", "on")
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=secure,
        path="/",
    )
    return {"username": acct["username"], "display_name": acct.get("display_name") or acct["username"]}


@router.post("/logout")
def logout(response: Response) -> dict[str, str]:
    response.delete_cookie(SESSION_COOKIE, path="/")
    return {"status": "ok"}
