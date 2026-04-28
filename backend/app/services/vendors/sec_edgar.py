"""SEC EDGAR adapter for company funding and material-event signals.

SEC EDGAR is free, no API key required, and covers all US public/private companies
that file Form D (funding), 8-K (material events), and 10-K/10-Q (financials).

This replaces Crunchbase for the WorkTrigger pipeline.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import httpx

EDGAR_EFTS = "https://efts.sec.gov/LATEST/search-index"

HEADERS = {
    "User-Agent": "FigworkSDR/1.0 (hanson@figwork.ai)",
    "Accept": "application/json",
}


def _edgar_search(
    query: str = "",
    forms: str = "D,8-K",
    start: str = "",
    end: str = "",
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Search EDGAR full-text search API (EFTS)."""
    now = datetime.now(timezone.utc)
    if not end:
        end = now.strftime("%Y-%m-%d")
    if not start:
        from datetime import timedelta
        start = (now - timedelta(days=90)).strftime("%Y-%m-%d")

    params: dict[str, str] = {}
    if query:
        params["q"] = query
    if forms:
        params["forms"] = forms
    params["dateRange"] = "custom"
    params["startdt"] = start
    params["enddt"] = end

    try:
        with httpx.Client(timeout=15, headers=HEADERS) as client:
            resp = client.get(EDGAR_EFTS, params=params)
            if resp.status_code >= 300:
                print(f"[SEC_EDGAR] HTTP {resp.status_code} for '{query}'")
                return []
            data = resp.json()
    except Exception as exc:
        print(f"[SEC_EDGAR] Search failed for '{query}': {exc}")
        return []

    hits = data.get("hits", {}).get("hits", [])
    results: list[dict[str, Any]] = []
    for hit in hits[:limit]:
        src = hit.get("_source", {})
        display_names = src.get("display_names") or []
        name = display_names[0] if display_names else ""
        cik_raw = (src.get("ciks") or [""])[0]
        adsh = src.get("adsh", "")
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_raw}/{adsh.replace('-', '')}/{adsh}-index.htm" if cik_raw and adsh else ""
        results.append({
            "company_name": name.split("(CIK")[0].strip() if "(CIK" in name else name,
            "form_type": src.get("form", "") or src.get("form_type", ""),
            "filed_at": src.get("file_date", ""),
            "description": name,
            "file_url": filing_url,
            "cik": cik_raw,
            "location": (src.get("biz_locations") or [""])[0],
        })
    return results


def search_company_signals(
    domain: str = "",
    company_name: str = "",
    *,
    days_back: int = 90,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search SEC EDGAR for recent funding (Form D) and material events (8-K) for a company."""
    query = company_name or (domain.split(".")[0] if domain else "")
    if not query:
        return []

    now = datetime.now(timezone.utc)
    from datetime import timedelta
    start = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")

    signals: list[dict[str, Any]] = []

    form_d_results = _edgar_search(query=query, forms="D", start=start, end=end, limit=limit)
    for r in form_d_results:
        signals.append({
            "source": "sec_edgar",
            "signal_type": "funding_round",
            "company_name": r["company_name"],
            "form_type": r["form_type"],
            "filed_at": r["filed_at"],
            "description": f"SEC Form D filing: {r['company_name']}",
            "url": r.get("file_url", ""),
            "confidence": 0.8,
        })

    event_results = _edgar_search(query=query, forms="8-K", start=start, end=end, limit=limit)
    for r in event_results:
        signals.append({
            "source": "sec_edgar",
            "signal_type": "material_event",
            "company_name": r["company_name"],
            "form_type": r["form_type"],
            "filed_at": r["filed_at"],
            "description": f"SEC 8-K filing: {r['company_name']}",
            "url": r.get("file_url", ""),
            "confidence": 0.7,
        })

    return signals[:limit]


def fetch_recent_funding_filings(
    *,
    days_back: int = 30,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Pull recent Form D filings (startup fundraises) from SEC EDGAR. No API key needed."""
    now = datetime.now(timezone.utc)
    from datetime import timedelta
    start = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")

    return _edgar_search(query="", forms="D", start=start, end=end, limit=limit)
