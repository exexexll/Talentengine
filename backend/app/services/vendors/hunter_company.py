"""Hunter.io company enrichment adapter — tech stack, funding, employees.

Hunter's Company Enrichment API returns firmographics, tech stack,
funding rounds, and social profiles from a domain lookup.
"""

from __future__ import annotations

import os
from typing import Any

import httpx


def hunter_company_enrich(domain: str) -> dict[str, Any]:
    """Enrich a company by domain via Hunter.io Company Enrichment API."""
    api_key = os.getenv("HUNTER_API_KEY", "").strip()
    if not api_key:
        return {"found": False, "domain": domain, "source": "hunter"}

    with httpx.Client(timeout=15) as client:
        resp = client.get(
            "https://api.hunter.io/v2/companies/find",
            params={"domain": domain.strip().lower(), "api_key": api_key},
        )

    if resp.status_code >= 300:
        return {"found": False, "domain": domain, "source": "hunter"}

    body = resp.json()
    c = body.get("data") or {}
    if not c.get("name"):
        return {"found": False, "domain": domain, "source": "hunter"}

    metrics = c.get("metrics") or {}
    site = c.get("site") or {}
    phones = site.get("phoneNumbers") or []

    funding_rounds = c.get("fundingRounds") or []
    total_raised = 0
    latest_round_type = ""
    for fr in funding_rounds:
        amt_str = str(fr.get("amount") or "0").replace("$", "").replace(",", "")
        try:
            total_raised += int(float(amt_str))
        except (ValueError, TypeError):
            pass
        if fr.get("type"):
            latest_round_type = fr["type"]

    return {
        "found": True,
        "domain": domain,
        "name": c.get("name"),
        "legal_name": c.get("legalName"),
        "industry": c.get("industryGroup") or c.get("industry") or c.get("sector"),
        "employee_count": metrics.get("employees"),
        "company_type": c.get("companyType"),
        "founded_year": c.get("foundedYear"),
        "description": (c.get("description") or "")[:300],
        "city": (c.get("location") or {}).get("city") if isinstance(c.get("location"), dict) else None,
        "state": (c.get("location") or {}).get("state") if isinstance(c.get("location"), dict) else None,
        "country": (c.get("location") or {}).get("country") if isinstance(c.get("location"), dict) else None,
        "phone": phones[0] if phones else None,
        "tech_stack": c.get("tech") or [],
        "tech_categories": c.get("techCategories") or [],
        "funding_rounds": funding_rounds,
        "total_raised": total_raised if total_raised > 0 else None,
        "latest_funding_type": latest_round_type or None,
        "traffic_rank": metrics.get("trafficRank"),
        "linkedin_url": None,
        "source": "hunter",
    }


def hunter_email_count(domain: str) -> dict[str, Any]:
    """FREE Hunter endpoint — returns how many public emails exist for a domain.

    `email-count` does NOT consume API credits, but Hunter still requires
    authentication (returns 401 without an `api_key`).  Use this for
    browsing/hover UX; call `hunter_domain_search` only on user commit
    (e.g. +SDR intake) to fetch actual contact rows.
    """
    d = (domain or "").strip().lower()
    if not d:
        return {"found": False, "domain": "", "total": 0}

    api_key = os.getenv("HUNTER_API_KEY", "").strip()
    params: dict[str, str] = {"domain": d}
    if api_key:
        params["api_key"] = api_key

    try:
        with httpx.Client(timeout=6) as client:
            resp = client.get(
                "https://api.hunter.io/v2/email-count",
                params=params,
            )
    except httpx.HTTPError:
        return {"found": False, "domain": d, "total": 0}

    if resp.status_code >= 300:
        return {"found": False, "domain": d, "total": 0, "status": resp.status_code}

    data = (resp.json() or {}).get("data") or {}
    total = int(data.get("total") or 0)
    return {
        "found": total > 0,
        "domain": d,
        "total": total,
        "personal": int(data.get("personal_emails") or 0),
        "generic": int(data.get("generic_emails") or 0),
        "pattern": data.get("pattern") or "",
        # Department breakdown so the UI can hint at what roles exist
        # (e.g. 3 engineering, 2 sales) without fetching actual emails.
        "departments": data.get("department") or {},
        "seniority": data.get("seniority") or {},
    }


def hunter_domain_search(domain: str, limit: int = 5) -> dict[str, Any]:
    """Lightweight lookup: how many public emails exist for a domain.

    Uses Hunter's domain-search endpoint.  Returns a small preview list so
    the frontend can confirm "contacts exist" before the user intakes a
    company, without having to run the full enrichment waterfall.
    """
    api_key = os.getenv("HUNTER_API_KEY", "").strip()
    if not api_key:
        return {"found": False, "domain": domain, "total": 0, "contacts": []}

    try:
        with httpx.Client(timeout=8) as client:
            resp = client.get(
                "https://api.hunter.io/v2/domain-search",
                params={
                    "domain": domain.strip().lower(),
                    "api_key": api_key,
                    "limit": max(1, min(25, limit)),
                },
            )
    except httpx.HTTPError:
        return {"found": False, "domain": domain, "total": 0, "contacts": []}

    if resp.status_code >= 300:
        return {"found": False, "domain": domain, "total": 0, "contacts": []}

    data = (resp.json() or {}).get("data") or {}
    emails = data.get("emails") or []
    contacts = []
    for e in emails[:limit]:
        contacts.append({
            "email": e.get("value") or "",
            "first_name": e.get("first_name") or "",
            "last_name": e.get("last_name") or "",
            "full_name": " ".join(x for x in [e.get("first_name"), e.get("last_name")] if x).strip(),
            "title": e.get("position") or "",
            "confidence": e.get("confidence") or 0,
            "seniority": e.get("seniority") or "",
            "department": e.get("department") or "",
        })
    total = int((data.get("meta") or {}).get("results") or len(emails) or 0)
    return {
        "found": bool(contacts),
        "domain": domain,
        "total": total,
        "pattern": data.get("pattern") or "",
        "contacts": contacts,
    }
