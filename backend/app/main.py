import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.staticfiles import StaticFiles

from backend.app.api import (
    ai,
    auth,
    boundaries,
    compare,
    geographies,
    metrics,
    recommendations,
    scenarios,
    scores,
    system,
    tiles,
    trust,
    worktrigger,
)
from backend.app.middleware.auth_gate import AuthGateMiddleware
from backend.app.services.telemetry import init_telemetry


def _cors_origins() -> list[str]:
    raw = os.getenv(
        "FIGWORK_ALLOWED_ORIGINS",
        "http://localhost:3000,http://localhost:8000",
    )
    origins = [o.strip() for o in raw.split(",") if o.strip()]
    # Starlette rejects ``allow_credentials=True`` with ``allow_origins=["*"]``.
    # An empty list breaks browser CORS entirely — fall back to local dev.
    if not origins:
        origins = ["http://127.0.0.1:8080", "http://localhost:8080", "http://localhost:3000", "http://localhost:8000"]
    return origins


app = FastAPI(
    title="Figwork Geographic Intelligence Engine API",
    version="0.1.0",
    description="Map-first decision API for geographic market selection.",
    redirect_slashes=False,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# `/api/boundaries/us_places` returns ~218 MB of GeoJSON; gzip is essential
# on a 2 GB Droplet's small upstream pipe.  Geo coordinates compress to
# roughly 12-18% of source, so the wire payload drops to ~30-40 MB and the
# initial map paint goes from 30-60 s to 5-10 s.
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=6)
_hosts = os.getenv("FIGWORK_TRUSTED_HOSTS", "").strip()
if _hosts:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=[h.strip() for h in _hosts.split(",") if h.strip()],
    )
app.add_middleware(AuthGateMiddleware)

init_telemetry(app)

app.include_router(auth.router)
app.include_router(geographies.router, prefix="/api/geographies", tags=["geographies"])
app.include_router(metrics.router, prefix="/api/metrics", tags=["metrics"])
app.include_router(scores.router, prefix="/api/scores", tags=["scores"])
app.include_router(
    recommendations.router,
    prefix="/api/recommendations",
    tags=["recommendations"],
)
app.include_router(scenarios.router, prefix="/api/scenarios", tags=["scenarios"])
app.include_router(compare.router, prefix="/api/compare", tags=["compare"])
app.include_router(trust.router, prefix="/api/trust", tags=["trust"])
app.include_router(tiles.router, prefix="/api/tiles", tags=["tiles"])
app.include_router(system.router, prefix="/api/system", tags=["system"])
app.include_router(ai.router, prefix="/api/ai/research", tags=["ai"])
app.include_router(boundaries.router, prefix="/api/boundaries", tags=["boundaries"])
app.include_router(worktrigger.router, prefix="/api/worktrigger", tags=["worktrigger"])


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
def health_ready() -> dict[str, str]:
    """SQLite (WorkTrigger) reachability — use behind a load balancer."""
    try:
        from backend.app.api import worktrigger as _wt

        _wt._store.list_all_accounts(limit=1)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)[:200]) from exc
    return {"status": "ready"}


@app.on_event("startup")
def _validate_deployment_auth() -> None:
    """Fail fast on bad auth config when internal login is enabled."""
    log = logging.getLogger("uvicorn.error")
    origins = _cors_origins()
    if auth.auth_enabled() and any(o == "*" for o in origins):
        raise RuntimeError(
            "FIGWORK_ALLOWED_ORIGINS cannot contain '*' when FIGWORK_AUTH_ENABLED=1 "
            "(cookies + CORS require explicit origins)."
        )
    if auth.auth_enabled():
        auth.auth_secret()
        from backend.app.services.simple_accounts import list_usernames

        if not list_usernames():
            log.warning(
                "FIGWORK_AUTH_ENABLED=1 but FIGWORK_ACCOUNTS_JSON has no users — login will always fail."
            )


_static_dir = os.getenv("FRONTEND_DIST", "").strip()
if _static_dir and Path(_static_dir).is_dir():
    app.mount("/", StaticFiles(directory=_static_dir, html=True), name="frontend")
