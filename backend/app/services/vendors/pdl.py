"""People Data Labs adapter — company search + enrichment + job listings.

PDL has 70M+ companies searchable via Elasticsearch/SQL queries with
location, industry, employee, funding, and tech stack filters.
"""

from __future__ import annotations

import os
from typing import Any

import httpx

PDL_BASE = "https://api.peopledatalabs.com/v5"


def _pdl_key() -> str:
    key = os.getenv("PDL_API_KEY", "").strip()
    if not key:
        raise RuntimeError("PDL_API_KEY not configured.")
    return key


def pdl_company_search(
    location: str,
    *,
    industry: str = "",
    min_employees: int = 1,
    max_employees: int = 100000,
    page: int = 1,
    limit: int = 20,
    scroll_token: str = "",
) -> tuple[list[dict[str, Any]], int, str]:
    """Search PDL for companies by location. Returns (companies, total, next_scroll_token)."""
    loc_clean = location.replace("'", "''")
    loc_parts = [p.strip() for p in loc_clean.split(",")]

    if len(loc_parts) >= 2:
        city = loc_parts[0]
        region = loc_parts[1] if len(loc_parts) > 1 else ""
        loc_conditions = [
            f"location.locality='{city}'",
            f"location.metro LIKE '%{city}%'",
            f"location.locality LIKE '%{city}%'",
        ]
        if region:
            loc_conditions.append(f"(location.region='{region}' AND location.metro IS NOT NULL)")
        where_clauses = [f"({' OR '.join(loc_conditions)})"]
    else:
        where_clauses = [f"(location.locality='{loc_clean}' OR location.region='{loc_clean}' OR location.metro LIKE '%{loc_clean}%')"]

    if industry:
        ind_clean = industry.replace("'", "''").strip().lower()
        where_clauses.append(f"industry LIKE '%{ind_clean}%'")
    if min_employees > 1:
        where_clauses.append(f"employee_count>={min_employees}")
    if max_employees < 100000:
        where_clauses.append(f"employee_count<={max_employees}")

    sql = f"SELECT * FROM company WHERE {' AND '.join(where_clauses)} ORDER BY employee_count DESC"
    params: dict[str, str] = {"sql": sql, "size": str(min(100, max(1, limit)))}
    if scroll_token:
        params["scroll_token"] = scroll_token

    with httpx.Client(timeout=20) as client:
        resp = client.get(
            f"{PDL_BASE}/company/search",
            headers={"x-api-key": _pdl_key()},
            params=params,
        )

    if resp.status_code >= 300:
        print(f"[PDL] Search failed ({resp.status_code}): {resp.text[:200]}")
        return [], 0, ""

    data = resp.json()
    total = data.get("total", 0)
    next_token = data.get("scroll_token", "")
    results: list[dict[str, Any]] = []
    for c in data.get("data", []):
        loc = c.get("location", {}) or {}
        domain = c.get("website") or ""
        if domain:
            domain = domain.replace("https://", "").replace("http://", "").split("/")[0].lower()
        results.append({
            "name": c.get("name", ""),
            "domain": domain,
            "industry": c.get("industry"),
            "employee_count": c.get("employee_count"),
            "city": loc.get("locality"),
            "state": loc.get("region"),
            "country": loc.get("country"),
            "linkedin_url": c.get("linkedin_url"),
            "twitter_url": c.get("twitter_url"),
            "logo_url": c.get("profile_pic_url"),
            "short_description": (c.get("summary") or "")[:200],
            "founded_year": c.get("founded"),
            "funding_stage": c.get("latest_funding_stage"),
            "total_funding": c.get("total_funding_raised"),
            "tags": c.get("tags") or [],
            "employee_growth": c.get("employee_count_by_country"),
            "lat": None,
            "lng": None,
            "source": "pdl",
        })
    return results, total, next_token


def pdl_company_enrich(domain: str) -> dict[str, Any]:
    """Enrich a single company by domain via PDL."""
    with httpx.Client(timeout=15) as client:
        resp = client.get(
            f"{PDL_BASE}/company/enrich",
            headers={"x-api-key": _pdl_key()},
            params={"website": domain.strip().lower()},
        )
    if resp.status_code >= 300:
        return {"found": False, "domain": domain}

    c = resp.json()
    loc = c.get("location", {}) or {}
    return {
        "found": True,
        "domain": domain,
        "name": c.get("name"),
        "industry": c.get("industry"),
        "employee_count": c.get("employee_count"),
        "annual_revenue": None,
        "funding_stage": c.get("latest_funding_stage"),
        "total_funding": c.get("total_funding_raised"),
        "founded_year": c.get("founded"),
        "city": loc.get("locality"),
        "state": loc.get("region"),
        "country": loc.get("country"),
        "linkedin_url": c.get("linkedin_url"),
        "website_url": c.get("website"),
        "short_description": c.get("summary"),
        "tags": c.get("tags") or [],
        "tech_stack": [t for t in (c.get("tech", []) or []) if isinstance(t, str)][:15],
        "source": "pdl",
    }


def pdl_job_listings(domain: str, *, limit: int = 5) -> list[dict[str, Any]]:
    """Get recent job listings for a company via PDL (hiring signals)."""
    sql = f"SELECT * FROM job_listing WHERE company_website='{domain.strip().lower()}' ORDER BY listed_at DESC"
    with httpx.Client(timeout=15) as client:
        resp = client.get(
            f"{PDL_BASE}/job/search",
            headers={"x-api-key": _pdl_key()},
            params={"sql": sql, "size": min(20, limit)},
        )
    if resp.status_code >= 300:
        return []

    data = resp.json()
    jobs: list[dict[str, Any]] = []
    for j in data.get("data", [])[:limit]:
        jobs.append({
            "title": j.get("title", ""),
            "location": j.get("location", {}).get("name", "") if isinstance(j.get("location"), dict) else str(j.get("location", "")),
            "listed_at": j.get("listed_at", ""),
            "company": j.get("company", {}).get("name", "") if isinstance(j.get("company"), dict) else "",
            "url": j.get("url", ""),
            "source": "pdl_jobs",
        })
    return jobs
