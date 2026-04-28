from __future__ import annotations

import os
import time
from typing import Any

import httpx
from fastapi import FastAPI, Request


def _posthog_capture(event: str, properties: dict[str, Any]) -> None:
    api_key = os.getenv("POSTHOG_API_KEY", "").strip()
    host = os.getenv("POSTHOG_HOST", "https://app.posthog.com").strip().rstrip("/")
    if not api_key:
        return
    try:
        with httpx.Client(timeout=3) as client:
            client.post(
                f"{host}/capture/",
                json={
                    "api_key": api_key,
                    "event": event,
                    "distinct_id": properties.get("distinct_id", "worktrigger-service"),
                    "properties": properties,
                },
            )
    except Exception:
        return


def _init_sentry() -> None:
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn:
        return
    try:
        import sentry_sdk

        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
            environment=os.getenv("APP_ENV", "local"),
        )
    except Exception:
        return


def init_telemetry(app: FastAPI) -> None:
    _init_sentry()

    @app.middleware("http")
    async def telemetry_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        started = time.perf_counter()
        try:
            response = await call_next(request)
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
            _posthog_capture(
                "api_request",
                {
                    "path": request.url.path,
                    "method": request.method,
                    "status_code": response.status_code,
                    "latency_ms": elapsed_ms,
                },
            )
            response.headers["X-Elapsed-Ms"] = str(elapsed_ms)
            return response
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
            _posthog_capture(
                "api_error",
                {
                    "path": request.url.path,
                    "method": request.method,
                    "error": str(exc),
                    "latency_ms": elapsed_ms,
                },
            )
            raise
