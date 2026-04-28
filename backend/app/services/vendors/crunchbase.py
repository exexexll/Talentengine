"""Crunchbase adapter for funding/company intelligence enrichment.

Crunchbase offers API access with 600+ endpoints for round-by-round funding data,
firmographics, and predictive company intelligence. This adapter handles:
- Funding round signal detection
- Company profile enrichment (headcount, funding stage, industry)
- Acquisition/expansion context
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from fastapi import HTTPException


CRUNCHBASE_API_BASE = "https://api.crunchbase.com/api/v4"


def _cb_params() -> dict[str, str]:
    key = os.getenv("CRUNCHBASE_API_KEY", "").strip()
    if not key:
        raise HTTPException(status_code=503, detail="CRUNCHBASE_API_KEY is required for Crunchbase integration.")
    return {"user_key": key}


def enrich_company(domain: str) -> dict[str, Any]:
    """Fetch company profile from Crunchbase by domain."""
    params = _cb_params()
    with httpx.Client(timeout=20) as client:
        search_resp = client.get(
            f"{CRUNCHBASE_API_BASE}/autocompletes",
            params={**params, "query": domain, "collection_ids": "organizations", "limit": 1},
        )
        if search_resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"Crunchbase search failed: {search_resp.text}")
        entities = search_resp.json().get("entities") or []
        if not entities:
            return {"found": False, "domain": domain}
        entity = entities[0]
        permalink = entity.get("identifier", {}).get("permalink", "")
        if not permalink:
            return {"found": False, "domain": domain}

        org_resp = client.get(
            f"{CRUNCHBASE_API_BASE}/entities/organizations/{permalink}",
            params={**params, "field_ids": "short_description,num_employees_enum,funding_total,last_funding_type,categories,location_identifiers,founded_on"},
        )
        if org_resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"Crunchbase org fetch failed: {org_resp.text}")
        props = org_resp.json().get("properties", {})

    return {
        "found": True,
        "domain": domain,
        "permalink": permalink,
        "short_description": props.get("short_description"),
        "num_employees_enum": props.get("num_employees_enum"),
        "funding_total_usd": props.get("funding_total", {}).get("value_usd") if isinstance(props.get("funding_total"), dict) else props.get("funding_total"),
        "last_funding_type": props.get("last_funding_type"),
        "categories": [c.get("value") for c in (props.get("categories") or []) if isinstance(c, dict)],
        "location": [loc.get("value") for loc in (props.get("location_identifiers") or []) if isinstance(loc, dict)],
        "founded_on": props.get("founded_on"),
    }


def fetch_recent_funding_rounds(*, limit: int = 50) -> list[dict[str, Any]]:
    """Fetch recent funding rounds from Crunchbase for signal ingestion."""
    params = _cb_params()
    with httpx.Client(timeout=30) as client:
        resp = client.post(
            f"{CRUNCHBASE_API_BASE}/searches/funding_rounds",
            params=params,
            json={
                "field_ids": ["identifier", "funded_organization_identifier", "money_raised", "investment_type", "announced_on"],
                "order": [{"field_id": "announced_on", "sort": "desc"}],
                "limit": min(200, max(1, limit)),
            },
        )
        if resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"Crunchbase funding search failed: {resp.text}")
        entities = resp.json().get("entities") or []

    rounds: list[dict[str, Any]] = []
    for e in entities:
        props = e.get("properties", {})
        org_id = props.get("funded_organization_identifier", {})
        rounds.append({
            "organization_permalink": org_id.get("permalink", ""),
            "organization_name": org_id.get("value", ""),
            "investment_type": props.get("investment_type"),
            "money_raised_usd": props.get("money_raised", {}).get("value_usd") if isinstance(props.get("money_raised"), dict) else None,
            "announced_on": props.get("announced_on"),
        })
    return rounds


def normalize_funding_round(round_data: dict[str, Any], *, domain: str | None = None) -> dict[str, Any]:
    """Convert a Crunchbase funding round into canonical signal format."""
    return {
        "source": "crunchbase",
        "signal_type": "funding_round",
        "account": {
            "domain": domain or f"{round_data.get('organization_permalink', 'unknown')}.com",
            "name": round_data.get("organization_name"),
            "crunchbase_uuid": round_data.get("organization_permalink"),
        },
        "occurred_at": round_data.get("announced_on") or "2026-01-01",
        "payload": {
            "round_type": round_data.get("investment_type"),
            "amount": round_data.get("money_raised_usd"),
        },
    }
