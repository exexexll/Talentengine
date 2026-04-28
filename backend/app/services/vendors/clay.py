"""Clay signal orchestration adapter.

Clay serves as the "glue layer" for aggregating multiple premium data sources
and automating GTM workflows. This adapter handles:
- Inbound webhooks from Clay table automations
- Outbound API calls to pull enriched table rows
- Signal normalization from Clay's schema to our canonical format
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import HTTPException


CLAY_API_BASE = "https://api.clay.com/v1"


def _clay_headers() -> dict[str, str]:
    api_key = os.getenv("CLAY_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=503, detail="CLAY_API_KEY is required for Clay integration.")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


CONTACT_FIELD_NAMES = {
    "contact_name", "contact_full_name", "contact_first_name",
    "contact_email", "contact_title", "contact_linkedin_url",
    "contact_persona_type",
}

ACCOUNT_META_FIELDS = {
    "company_domain", "domain", "website", "company_name", "name",
    "signal_type", "trigger_type", "occurred_at", "timestamp",
    "linkedin_company_id", "crunchbase_uuid", "geography_id", "hq_geo_id", "locations",
    "industry", "employee_count", "funding_stage", "total_funding", "country",
}


def normalize_clay_webhook(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert a Clay webhook/table-row payload into our canonical signal format."""
    row = payload.get("row") or payload
    domain = (
        str(row.get("company_domain") or row.get("domain") or row.get("website") or "")
        .strip().lower()
        .replace("https://", "").replace("http://", "").strip("/")
    )
    company_name = str(row.get("company_name") or row.get("name") or "").strip()
    signal_type = str(row.get("signal_type") or row.get("trigger_type") or "clay_enrichment").strip()
    occurred_at = str(row.get("occurred_at") or row.get("timestamp") or datetime.now(timezone.utc).isoformat())

    normalized = {
        "source": "clay",
        "signal_type": signal_type,
        "account": {
            "domain": domain,
            "name": company_name or None,
            "linkedin_company_id": str(row.get("linkedin_company_id") or "").strip() or None,
            "crunchbase_uuid": str(row.get("crunchbase_uuid") or "").strip() or None,
            "headquarters_geo_id": str(row.get("geography_id") or row.get("hq_geo_id") or "").strip() or None,
            "locations": row.get("locations") or [],
        },
        "occurred_at": occurred_at,
        "payload": {k: v for k, v in row.items() if k not in ACCOUNT_META_FIELDS and k not in CONTACT_FIELD_NAMES},
    }
    return normalized


def extract_clay_contacts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract contact records from a Clay row if contact fields are present.

    Supports single-contact rows (contact_name, contact_email, ...) and
    multi-contact arrays (contacts: [{name, email, title, ...}]).
    """
    row = payload.get("row") or payload
    contacts: list[dict[str, Any]] = []

    if isinstance(row.get("contacts"), list):
        for c in row["contacts"]:
            if not isinstance(c, dict):
                continue
            email = str(c.get("email") or c.get("contact_email") or "").strip()
            name = str(c.get("name") or c.get("full_name") or c.get("contact_name") or "").strip()
            if not email and not name:
                continue
            contacts.append({
                "full_name": name or None,
                "title": str(c.get("title") or c.get("contact_title") or "").strip() or None,
                "email": email or None,
                "linkedin_url": str(c.get("linkedin_url") or c.get("contact_linkedin_url") or "").strip() or None,
                "persona_type": str(c.get("persona_type") or c.get("contact_persona_type") or "").strip() or None,
                "confidence_score": float(c.get("confidence_score", 0.85)),
                "source": "clay",
            })
        return contacts

    email = str(row.get("contact_email") or "").strip()
    name = str(
        row.get("contact_name")
        or row.get("contact_full_name")
        or ""
    ).strip()
    if not name and row.get("contact_first_name"):
        name = str(row.get("contact_first_name", "")).strip()
    if email or name:
        contacts.append({
            "full_name": name or None,
            "title": str(row.get("contact_title") or "").strip() or None,
            "email": email or None,
            "linkedin_url": str(row.get("contact_linkedin_url") or "").strip() or None,
            "persona_type": str(row.get("contact_persona_type") or "").strip() or None,
            "confidence_score": 0.85,
            "source": "clay",
        })

    return contacts


def extract_clay_account_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract account-level enrichment fields from a Clay row for updating the account record."""
    row = payload.get("row") or payload
    fields: dict[str, Any] = {}
    if row.get("industry"):
        fields["industry"] = str(row["industry"]).strip()
    if row.get("employee_count"):
        try:
            fields["employee_count"] = int(row["employee_count"])
        except (ValueError, TypeError):
            pass
    if row.get("funding_stage"):
        fields["funding_stage"] = str(row["funding_stage"]).strip()
    if row.get("total_funding"):
        try:
            fields["total_funding"] = float(row["total_funding"])
        except (ValueError, TypeError):
            pass
    if row.get("country"):
        fields["country"] = str(row["country"]).strip()
    li = str(row.get("linkedin_url") or row.get("company_linkedin_url") or "").strip()
    if li:
        fields["linkedin_url"] = li
    tw = str(row.get("twitter_url") or row.get("company_twitter_url") or "").strip()
    if tw:
        fields["twitter_url"] = tw
    return fields


def fetch_clay_table_rows(table_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
    """Pull rows from a Clay table via API for batch ingestion."""
    headers = _clay_headers()
    with httpx.Client(timeout=30) as client:
        resp = client.get(
            f"{CLAY_API_BASE}/tables/{table_id}/rows",
            headers=headers,
            params={"limit": min(500, max(1, limit))},
        )
        if resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"Clay API error: {resp.text}")
        data = resp.json()
    rows = data.get("rows") or data.get("data") or []
    if isinstance(rows, list):
        return rows
    return []


def fetch_clay_enrichment(domain: str) -> dict[str, Any]:
    """Enrich a single company domain via Clay's enrichment endpoint."""
    headers = _clay_headers()
    with httpx.Client(timeout=20) as client:
        resp = client.post(
            f"{CLAY_API_BASE}/enrich/company",
            headers=headers,
            json={"domain": domain},
        )
        if resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"Clay enrichment failed: {resp.text}")
        return resp.json()
