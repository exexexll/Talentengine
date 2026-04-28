"""LinkedIn Sales Navigator adapter for account prioritization and buyer intent.

Sales Navigator provides first-party buyer-intent signals, account insights,
and prioritization. This adapter handles:
- Account-level intent score retrieval
- Buyer persona identification from account pages
- Contact context enrichment
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from fastapi import HTTPException


def _li_headers() -> dict[str, str]:
    token = os.getenv("LINKEDIN_SALES_NAV_TOKEN", "").strip()
    if not token:
        raise HTTPException(
            status_code=503,
            detail="LINKEDIN_SALES_NAV_TOKEN is required for LinkedIn Sales Navigator integration.",
        )
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0",
    }


def search_accounts(query: str, *, limit: int = 10) -> list[dict[str, Any]]:
    """Search Sales Navigator accounts by company name/domain."""
    headers = _li_headers()
    with httpx.Client(timeout=20) as client:
        resp = client.get(
            "https://api.linkedin.com/v2/salesApiAccounts",
            headers=headers,
            params={"q": "search", "query": query, "count": min(50, max(1, limit))},
        )
        if resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"LinkedIn account search failed: {resp.text}")
        data = resp.json()

    results: list[dict[str, Any]] = []
    for elem in data.get("elements", []):
        results.append({
            "linkedin_account_id": elem.get("id"),
            "name": elem.get("companyName") or elem.get("name"),
            "domain": elem.get("website"),
            "industry": elem.get("industry"),
            "employee_count_range": elem.get("employeeCountRange"),
            "buyer_intent_score": elem.get("buyerIntentScore"),
        })
    return results


def get_account_insights(linkedin_account_id: str) -> dict[str, Any]:
    """Fetch account-level insights and buyer intent from Sales Navigator."""
    headers = _li_headers()
    with httpx.Client(timeout=20) as client:
        resp = client.get(
            f"https://api.linkedin.com/v2/salesApiAccounts/{linkedin_account_id}",
            headers=headers,
        )
        if resp.status_code >= 300:
            raise HTTPException(status_code=502, detail=f"LinkedIn account insight failed: {resp.text}")
        data = resp.json()

    return {
        "linkedin_account_id": linkedin_account_id,
        "company_name": data.get("companyName") or data.get("name"),
        "domain": data.get("website"),
        "industry": data.get("industry"),
        "employee_count_range": data.get("employeeCountRange"),
        "buyer_intent_score": data.get("buyerIntentScore"),
        "decision_makers": [
            {
                "name": dm.get("name") or dm.get("fullName"),
                "title": dm.get("title"),
                "linkedin_url": dm.get("publicProfileUrl"),
            }
            for dm in (data.get("decisionMakers") or data.get("recommendedLeads") or [])
            if isinstance(dm, dict)
        ],
        "recent_activities": data.get("recentActivities") or [],
    }


def normalize_linkedin_intent(account_data: dict[str, Any]) -> dict[str, Any]:
    """Convert LinkedIn account insights into canonical signal format."""
    return {
        "source": "linkedin_sales_nav",
        "signal_type": "buyer_intent",
        "account": {
            "domain": account_data.get("domain") or "",
            "name": account_data.get("company_name"),
            "linkedin_company_id": str(account_data.get("linkedin_account_id") or ""),
        },
        "occurred_at": "now",
        "payload": {
            "buyer_intent_score": account_data.get("buyer_intent_score"),
            "employee_count_range": account_data.get("employee_count_range"),
            "decision_maker_count": len(account_data.get("decision_makers") or []),
        },
    }
