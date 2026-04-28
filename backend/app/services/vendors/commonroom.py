"""Common Room adapter for website visitor + job-change signal ingestion.

Common Room supports website visitor tracking, job changes, and signal stacking.
This adapter handles:
- Inbound webhooks from Common Room automations
- Outbound API pulls for account-level intent signals
- Normalization of visitor/job-change events into canonical signals
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import HTTPException


COMMONROOM_API_BASE = "https://api.commonroom.io/community/v1"


def _cr_headers() -> dict[str, str]:
    token = os.getenv("COMMONROOM_API_TOKEN", "").strip()
    if not token:
        raise HTTPException(status_code=503, detail="COMMONROOM_API_TOKEN is required for Common Room integration.")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def normalize_commonroom_webhook(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert a Common Room webhook payload into canonical signal format."""
    activity = payload.get("activity") or payload
    actor = activity.get("actor") or {}
    org = actor.get("organization") or activity.get("organization") or {}

    domain = str(org.get("domain") or org.get("website") or "").strip().lower()
    if not domain and actor.get("email"):
        email = str(actor["email"])
        if "@" in email:
            domain = email.split("@")[1].strip().lower()

    signal_map: dict[str, str] = {
        "page_view": "web_visit",
        "website_visit": "web_visit",
        "job_change": "exec_change",
        "job_posting": "hiring_surge",
        "product_signup": "buyer_intent",
    }
    raw_type = str(activity.get("type") or activity.get("activityType") or "web_visit").strip().lower()
    signal_type = signal_map.get(raw_type, raw_type)

    return {
        "source": "commonroom",
        "signal_type": signal_type,
        "account": {
            "domain": domain,
            "name": str(org.get("name") or "").strip() or None,
            "linkedin_company_id": str(org.get("linkedinId") or "").strip() or None,
        },
        "occurred_at": str(activity.get("occurredAt") or activity.get("timestamp") or datetime.now(timezone.utc).isoformat()),
        "payload": {
            "page_url": activity.get("url") or activity.get("pageUrl"),
            "job_title": activity.get("jobTitle") or actor.get("title"),
            "actor_name": actor.get("name") or actor.get("fullName"),
            "actor_email": actor.get("email"),
            "raw_type": raw_type,
        },
    }


def fetch_commonroom_signals(*, days: int = 7, limit: int = 100) -> list[dict[str, Any]]:
    """Pull recent account-level signals from Common Room API."""
    headers = _cr_headers()
    with httpx.Client(timeout=30) as client:
        resp = client.get(
            f"{COMMONROOM_API_BASE}/activities",
            headers=headers,
            params={"limit": min(500, max(1, limit)), "days": min(90, max(1, days))},
        )
        if resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"Common Room API error: {resp.text}")
        data = resp.json()
    activities = data.get("activities") or data.get("data") or data.get("results") or []
    return [normalize_commonroom_webhook({"activity": a}) for a in activities if isinstance(a, dict)]
