"""Require a valid session cookie for all ``/api/*`` routes when auth is on."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from backend.app.api.auth import SESSION_COOKIE, auth_enabled, read_session_token


def _path_ok(path: str) -> bool:
    if path == "/health":
        return True
    if path.startswith("/api/auth/login"):
        return True
    if path.startswith("/api/auth/logout"):
        return True
    if path.startswith("/api/auth/me"):
        return True
    if path.startswith("/docs") or path.startswith("/openapi.json") or path.startswith("/redoc"):
        return True
    return False


class AuthGateMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if not auth_enabled():
            return await call_next(request)
        # CORS preflight must never be blocked — the CORSMiddleware that
        # sits inner in the stack will short-circuit OPTIONS responses.
        if request.method == "OPTIONS":
            return await call_next(request)
        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)
        if _path_ok(path):
            return await call_next(request)
        raw = request.cookies.get(SESSION_COOKIE)
        data = read_session_token(raw)
        if not data or not isinstance(data.get("username"), str) or not str(data["username"]).strip():
            return JSONResponse({"detail": "Not authenticated"}, status_code=401)
        return await call_next(request)
