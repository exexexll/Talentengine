"""Company discovery per geography — Apollo org search + org enrichment + SEC EDGAR.

Discovers companies in a geographic area, enriches firmographics, and caches results
so the map can show a company list per region with one-click SDR intake.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import HTTPException

APOLLO_API_V1 = "https://api.apollo.io/api/v1"
CACHE_DIR = Path("backend/data/company_discovery_cache")
CACHE_TTL_SECONDS = 86400 * 3


def _apollo_headers() -> dict[str, str]:
    api_key = os.getenv("APOLLO_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=503, detail="APOLLO_API_KEY required for company discovery.")
    return {"Content-Type": "application/json", "Cache-Control": "no-cache", "x-api-key": api_key}


def apollo_org_enrich(domain: str) -> dict[str, Any]:
    """Enrich a single company by domain via Apollo Organization Enrichment API."""
    headers = _apollo_headers()
    with httpx.Client(timeout=20) as client:
        resp = client.get(
            f"{APOLLO_API_V1}/organizations/enrich",
            headers=headers,
            params={"domain": domain.strip().lower()},
        )
    if resp.status_code >= 300:
        return {"found": False, "domain": domain}
    data = resp.json()
    org = data.get("organization") or data
    if not org or not org.get("name"):
        return {"found": False, "domain": domain}
    return {
        "found": True,
        "domain": domain,
        "name": org.get("name"),
        "industry": org.get("industry"),
        "employee_count": org.get("estimated_num_employees"),
        "annual_revenue": org.get("annual_revenue"),
        "funding_stage": org.get("latest_funding_stage"),
        "total_funding": org.get("total_funding"),
        "founded_year": org.get("founded_year"),
        "city": org.get("city"),
        "state": org.get("state"),
        "country": org.get("country"),
        "linkedin_url": org.get("linkedin_url"),
        "twitter_url": org.get("twitter_url")
        or (f"https://twitter.com/{h.lstrip('@')}" if (h := str(org.get("twitter_handle") or "").strip()) else None),
        "website_url": org.get("website_url") or org.get("primary_domain"),
        "short_description": org.get("short_description"),
        "logo_url": org.get("logo_url"),
        "keywords": org.get("keywords") or [],
        "technologies": [t.get("name") for t in (org.get("current_technologies") or []) if t.get("name")][:10],
    }


def apollo_org_search(
    location: str,
    *,
    industry: str = "",
    min_employees: int = 0,
    max_employees: int = 0,
    page: int = 1,
    limit: int = 25,
) -> tuple[list[dict[str, Any]], int]:
    """Search Apollo for companies in a geographic location. Returns (results, total_count)."""
    headers = _apollo_headers()
    params: list[tuple[str, str]] = [
        ("organization_locations[]", location),
        ("page", str(max(1, page))),
        ("per_page", str(min(100, max(1, limit)))),
    ]
    if min_employees > 0 and max_employees > 0:
        params.append(("organization_num_employees_ranges[]", f"{min_employees},{max_employees}"))
    if industry:
        params.append(("q_organization_keyword_tags[]", industry.strip().lower()))

    with httpx.Client(timeout=25) as client:
        resp = client.post(
            f"{APOLLO_API_V1}/mixed_companies/search",
            headers=headers,
            params=params,
        )
    if resp.status_code >= 300:
        return [], 0

    data = resp.json()
    total = int(data.get("pagination", {}).get("total_entries", 0) or 0)
    orgs = data.get("organizations") or data.get("accounts") or []
    results: list[dict[str, Any]] = []
    for o in orgs[:limit]:
        lat = o.get("raw_address", {}).get("latitude") if isinstance(o.get("raw_address"), dict) else None
        lng = o.get("raw_address", {}).get("longitude") if isinstance(o.get("raw_address"), dict) else None
        results.append({
            "name": o.get("name", ""),
            "domain": o.get("primary_domain") or o.get("domain") or "",
            "industry": o.get("industry"),
            "employee_count": o.get("estimated_num_employees"),
            "city": o.get("city"),
            "state": o.get("state"),
            "country": o.get("country"),
            "linkedin_url": o.get("linkedin_url"),
            "logo_url": o.get("logo_url"),
            "short_description": (o.get("short_description") or "")[:200],
            "lat": lat,
            "lng": lng,
            "source": "apollo",
        })
    return results, total


def discover_companies_for_geography(
    geography_id: str,
    geography_name: str,
    *,
    industry: str = "",
    min_employees: int = 10,
    max_employees: int = 10000,
    page: int = 1,
    limit: int = 20,
    force_refresh: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    """Discover companies in a geography. Returns (companies, total_available)."""
    has_filters = bool(industry) or (min_employees > 0 and min_employees != 10) or (max_employees > 0 and max_employees != 10000)
    location = _geo_id_to_location(geography_id, geography_name)
    if not location:
        return [], 0

    # Build cache key that includes filters
    cache_key = f"{geography_id}:{industry}:{min_employees}:{max_employees}" if has_filters else geography_id

    if not force_refresh and page == 1:
        cached = _load_cache(cache_key)
        if cached is not None:
            return cached[:limit], len(cached)

    companies: list[dict[str, Any]] = []
    total = 0
    seen: set[str] = set()

    def _add(results: list[dict[str, Any]]) -> None:
        for c in results:
            key = (c.get("domain") or c.get("name") or "").lower()
            if key and key not in seen:
                seen.add(key)
                companies.append(c)

    # Apollo: primary search (best location matching for counties/cities/states)
    try:
        apollo_results, apollo_total = apollo_org_search(
            location, industry=industry, min_employees=min_employees,
            max_employees=max_employees, page=page, limit=limit,
        )
        total = apollo_total
        _add(apollo_results)
    except Exception as exc:
        print(f"[CompanyDiscovery] Apollo search failed: {exc}")

    # PDL: secondary (fills gaps, better industry filtering for specific queries)
    if len(companies) < limit:
        try:
            from backend.app.services.vendors.pdl import pdl_company_search
            pdl_results, pdl_total, _ = pdl_company_search(
                location, industry=industry, min_employees=max(min_employees, 5),
                max_employees=max_employees, page=page, limit=limit - len(companies),
            )
            total = max(total, pdl_total)
            _add(pdl_results)
        except Exception as exc:
            print(f"[CompanyDiscovery] PDL search failed: {exc}")

    if page == 1:
        _save_cache(cache_key, companies)

    return companies[:limit], max(total, len(companies))




def _geo_id_to_location(geography_id: str, geography_name: str) -> str:
    """Convert a geography ID to an Apollo-compatible location string."""
    import re

    def _clean_place(name: str) -> str:
        return re.sub(r'\s+(city|town|CDP|village|borough|census designated place)$', '', name, flags=re.IGNORECASE).strip()

    if geography_id.startswith("EU-"):
        _EU_CODE_TO_COUNTRY: dict[str, str] = {
            "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "HR": "Croatia",
            "CY": "Cyprus", "CZ": "Czechia", "DK": "Denmark", "EE": "Estonia",
            "FI": "Finland", "FR": "France", "DE": "Germany", "EL": "Greece",
            "HU": "Hungary", "IE": "Ireland", "IT": "Italy", "LV": "Latvia",
            "LT": "Lithuania", "LU": "Luxembourg", "MT": "Malta", "NL": "Netherlands",
            "PL": "Poland", "PT": "Portugal", "RO": "Romania", "SK": "Slovakia",
            "SI": "Slovenia", "ES": "Spain", "SE": "Sweden", "NO": "Norway",
            "CH": "Switzerland", "UK": "United Kingdom", "IS": "Iceland",
            "TR": "Turkey", "RS": "Serbia", "AL": "Albania",
        }
        parts = geography_name.split(",")
        region = parts[0].strip() if parts else geography_name
        country = parts[1].strip() if len(parts) >= 2 else ""
        if not country:
            nuts_code = geography_id[3:5]
            country = _EU_CODE_TO_COUNTRY.get(nuts_code, "")
        return f"{region}, {country}" if country else region
    if geography_id.startswith("AU-"):
        clean = geography_name.replace(", Australia", "").strip()
        clean = clean.split(" - ")[0].strip()
        return f"{clean}, Australia"
    if geography_id.startswith("IN-"):
        clean = geography_name.replace(", India", "").strip()
        clean = clean.replace("District in ", "")
        return f"{clean}, India"
    if len(geography_id) == 2 and geography_id.isdigit():
        return geography_name
    if len(geography_id) == 7 and geography_id.isdigit():
        clean = _clean_place(geography_name)
        parts = clean.split(",")
        if len(parts) >= 2:
            return f"{parts[0].strip()}, {parts[1].strip()}"
        return clean
    if len(geography_id) == 5 and geography_id.isdigit():
        clean = geography_name.replace(" County", "").replace(" Municipio", "").replace(" Parish", "")
        parts = clean.split(",")
        if len(parts) >= 2:
            return f"{parts[0].strip()}, {parts[1].strip()}"
        return clean
    return geography_name


def _serpapi_local_business_search(location: str, limit: int = 10) -> list[dict[str, Any]]:
    """Search for businesses in a location using SerpAPI Google search. Free supplement to Apollo."""
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if not api_key:
        return []
    try:
        from serpapi import GoogleSearch
    except ImportError:
        return []

    results: list[dict[str, Any]] = []
    queries = [
        f"top companies employers in {location}",
        f"largest businesses hiring {location}",
    ]
    for query in queries:
        try:
            search = GoogleSearch({"engine": "google", "q": query, "api_key": api_key, "num": 10})
            data = search.get_dict()
            for item in data.get("organic_results", [])[:limit]:
                title = item.get("title", "")
                link = item.get("link", "")
                snippet = item.get("snippet", "")
                domain = ""
                if link:
                    domain = link.replace("https://", "").replace("http://", "").split("/")[0].lower()
                    if domain.startswith("www."):
                        domain = domain[4:]
                if not title or domain in {"linkedin.com", "indeed.com", "glassdoor.com", "yelp.com",
                                            "wikipedia.org", "google.com", "facebook.com", "bbb.org",
                                            "yellowpages.com", "bloomberg.com", "forbes.com", "crunchbase.com"}:
                    continue
                results.append({
                    "name": title.split(" - ")[0].split(" | ")[0].strip()[:60],
                    "domain": domain,
                    "industry": None,
                    "employee_count": None,
                    "city": None, "state": None, "country": None,
                    "linkedin_url": None, "logo_url": None,
                    "short_description": snippet[:200],
                    "lat": None, "lng": None,
                    "source": "serpapi_web",
                })
        except Exception:
            continue
        if results:
            break
    return results[:limit]


def _cache_key(geography_id: str) -> str:
    return geography_id.replace("/", "_").replace(" ", "_")


def _load_cache(geography_id: str) -> list[dict[str, Any]] | None:
    cf = CACHE_DIR / f"{_cache_key(geography_id)}.json"
    if not cf.exists():
        return None
    try:
        data = json.loads(cf.read_text(encoding="utf-8"))
        ts = data.get("timestamp", 0)
        import time
        if time.time() - ts > CACHE_TTL_SECONDS:
            return None
        return data.get("companies", [])
    except (json.JSONDecodeError, OSError):
        return None


def _save_cache(geography_id: str, companies: list[dict[str, Any]]) -> None:
    import time
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cf = CACHE_DIR / f"{_cache_key(geography_id)}.json"
    cf.write_text(json.dumps({"timestamp": time.time(), "companies": companies}, indent=2), encoding="utf-8")
