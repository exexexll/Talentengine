"""Contact enrichment waterfall: Apollo -> Findymail -> Hunter.

The PRD specifies a three-stage waterfall for contact discovery and verification.
Each provider is tried in order; the first to return a verified email wins.
Results are normalized into canonical contact records.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from fastapi import HTTPException

# Apollo public API: https://docs.apollo.io/reference/people-api-search
APOLLO_API_V1 = "https://api.apollo.io/api/v1"
# Legacy path (some accounts still resolve here); prefer header auth per Apollo guidance.
APOLLO_LEGACY_MIXED_SEARCH = "https://api.apollo.io/v1/mixed_people/search"

APOLLO_PERSON_TITLES = [
    "CEO", "CTO", "COO", "CFO", "VP", "Head of", "Director",
    "Founder", "General Manager", "Managing Director",
]


def _apollo_headers(api_key: str) -> dict[str, str]:
    """Apollo OpenAPI uses apiKey in header name `x-api-key` (not URL/query)."""
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "x-api-key": api_key,
    }


def _apollo_people_match(
    client: httpx.Client,
    headers: dict[str, str],
    person_id: str,
    domain: str,
    *,
    reveal_personal_emails: bool,
) -> dict[str, Any]:
    """POST /people/match — returns work email when reveal_personal_emails=true (uses credits)."""
    params: dict[str, str] = {"id": person_id, "domain": domain}
    if reveal_personal_emails:
        params["reveal_personal_emails"] = "true"
    r = client.post(f"{APOLLO_API_V1}/people/match", headers=headers, params=params, timeout=25.0)
    if r.status_code >= 300:
        return {}
    return r.json()


def _apollo_row_to_contact(p: dict[str, Any]) -> dict[str, Any]:
    name = (p.get("name") or "").strip()
    if not name:
        name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
    estatus = p.get("email_status")
    verified = estatus == "verified"
    return {
        "full_name": name,
        "title": p.get("title"),
        "email": (p.get("email") or "").strip() or None,
        "email_status": "valid" if verified else "unknown",
        "linkedin_url": p.get("linkedin_url"),
        "persona_type": _infer_persona(str(p.get("title") or "")),
        "confidence_score": 0.85 if verified else 0.5,
        "source": "apollo",
    }


def _apollo_search(domain: str, *, limit: int = 5) -> list[dict[str, Any]]:
    """Search Apollo for contacts at a domain, then optionally enrich emails via /people/match.

    Per Apollo docs, People API Search does not return email addresses; enrichment is required
    for emails and consumes credits. Toggle with APOLLO_REVEAL_PERSONAL_EMAILS (default: true).
    Search endpoint requires a master API key.
    """
    api_key = os.getenv("APOLLO_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=503, detail="APOLLO_API_KEY is required for Apollo enrichment.")
    domain_clean = domain.strip().lower()
    per_page = min(100, max(1, limit))
    reveal = os.getenv("APOLLO_REVEAL_PERSONAL_EMAILS", "true").strip().lower() in ("1", "true", "yes")

    params: list[tuple[str, str]] = [
        ("q_organization_domains_list[]", domain_clean),
        ("page", "1"),
        ("per_page", str(per_page)),
    ]
    for t in APOLLO_PERSON_TITLES:
        params.append(("person_titles[]", t))

    headers = _apollo_headers(api_key)
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{APOLLO_API_V1}/mixed_people/api_search",
            headers=headers,
            params=params,
        )
        if resp.status_code < 300:
            data = resp.json()
        else:
            # Fallback: older mixed_people/search JSON body (omit api_key when using x-api-key).
            resp2 = client.post(
                APOLLO_LEGACY_MIXED_SEARCH,
                headers=headers,
                json={
                    "q_organization_domains": domain_clean,
                    "page": 1,
                    "per_page": per_page,
                    "person_titles": APOLLO_PERSON_TITLES,
                },
            )
            if resp2.status_code >= 300:
                raise HTTPException(
                    status_code=502,
                    detail=f"Apollo search failed: {resp.text}; fallback: {resp2.text}",
                )
            data = resp2.json()

        people = data.get("people") or []
        contacts: list[dict[str, Any]] = []
        for p in people[:limit]:
            merged = dict(p)
            pid = p.get("id")
            has_verified = p.get("email_status") == "verified"
            email_present = bool((p.get("email") or "").strip())
            if reveal and pid and not (email_present and has_verified):
                match_payload = _apollo_people_match(
                    client, headers, str(pid), domain_clean, reveal_personal_emails=True
                )
                person_enriched = match_payload.get("person")
                if isinstance(person_enriched, dict):
                    merged = {**p, **person_enriched}
            contacts.append(_apollo_row_to_contact(merged))
        return contacts


def _findymail_verify(email: str) -> dict[str, Any]:
    """Verify/find email via Findymail."""
    api_key = os.getenv("FINDYMAIL_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=503, detail="FINDYMAIL_API_KEY is required for Findymail verification.")
    with httpx.Client(timeout=15) as client:
        resp = client.post(
            "https://app.findymail.com/api/search/mail",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"email": email},
        )
        if resp.status_code >= 300:
            return {"email": email, "status": "unknown", "source": "findymail"}
        data = resp.json()
    return {
        "email": data.get("email") or email,
        "status": "valid" if data.get("status") == "valid" else data.get("status", "unknown"),
        "source": "findymail",
    }


def _findymail_find(full_name: str, domain: str) -> dict[str, Any]:
    """Find email for a person at a domain via Findymail."""
    api_key = os.getenv("FINDYMAIL_API_KEY", "").strip()
    if not api_key:
        return {"email": None, "status": "unavailable", "source": "findymail"}
    with httpx.Client(timeout=15) as client:
        resp = client.post(
            "https://app.findymail.com/api/search/mail",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"name": full_name, "domain": domain},
        )
        if resp.status_code >= 300:
            return {"email": None, "status": "error", "source": "findymail"}
        data = resp.json()
    return {
        "email": data.get("email"),
        "status": "valid" if data.get("email") and data.get("status") == "valid" else "unknown",
        "source": "findymail",
    }


def _hunter_find(full_name: str, domain: str) -> dict[str, Any]:
    """Find email for a person at a domain via Hunter.io (tertiary fallback)."""
    api_key = os.getenv("HUNTER_API_KEY", "").strip()
    if not api_key:
        return {"email": None, "status": "unavailable", "source": "hunter"}
    parts = full_name.strip().split()
    first_name = parts[0] if parts else ""
    last_name = parts[-1] if len(parts) > 1 else ""
    with httpx.Client(timeout=15) as client:
        resp = client.get(
            "https://api.hunter.io/v2/email-finder",
            params={
                "domain": domain,
                "first_name": first_name,
                "last_name": last_name,
                "api_key": api_key,
            },
        )
        if resp.status_code >= 300:
            return {"email": None, "status": "error", "source": "hunter"}
        data = resp.json().get("data", {})
    return {
        "email": data.get("email"),
        "status": "valid" if (data.get("score") or 0) >= 80 else "risky",
        "source": "hunter",
    }


def _infer_persona(title: str) -> str:
    t = title.lower()
    if any(k in t for k in ("ceo", "founder", "owner", "managing director")):
        return "executive_buyer"
    if any(k in t for k in ("cto", "vp engineering", "head of engineering", "tech lead")):
        return "technical_buyer"
    if any(k in t for k in ("coo", "operations", "head of ops")):
        return "operations_buyer"
    if any(k in t for k in ("cfo", "finance", "controller")):
        return "finance_buyer"
    if any(k in t for k in ("marketing", "growth", "demand gen")):
        return "marketing_buyer"
    if any(k in t for k in ("hr", "people", "talent")):
        return "people_buyer"
    return "general"


def apollo_search_contacts_by_title(
    domain: str,
    titles: list[str],
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search Apollo for contacts at a domain filtered to specific job titles."""
    api_key = os.getenv("APOLLO_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=503, detail="APOLLO_API_KEY is required.")
    headers = _apollo_headers(api_key)
    params: list[tuple[str, str]] = [
        ("q_organization_domains_list[]", domain.strip().lower()),
        ("page", "1"),
        ("per_page", str(min(100, max(1, limit)))),
    ]
    for t in titles:
        params.append(("person_titles[]", t.strip()))

    reveal = os.getenv("APOLLO_REVEAL_PERSONAL_EMAILS", "true").strip().lower() in ("1", "true", "yes")
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(f"{APOLLO_API_V1}/mixed_people/api_search", headers=headers, params=params)
        if resp.status_code >= 300:
            return []
        data = resp.json()

        people = data.get("people") or []
        contacts: list[dict[str, Any]] = []
        for p in people[:limit]:
            merged = dict(p)
            pid = p.get("id")
            if reveal and pid and not (p.get("email") and p.get("email_status") == "verified"):
                match_data = _apollo_people_match(client, headers, str(pid), domain.strip().lower(), reveal_personal_emails=True)
                person_enriched = match_data.get("person")
                if isinstance(person_enriched, dict):
                    merged = {**p, **person_enriched}
            row = _apollo_row_to_contact(merged)
            # Drop rows that came back without a usable email — they
            # would clutter the contact list with people we cannot
            # actually message.  The SDR can still add them by hand
            # later; nothing about Apollo's response gives us a way
            # to recover a missing email later, so persisting now
            # would just be noise.
            if not row.get("email") or "@" not in str(row.get("email") or ""):
                continue
            contacts.append(row)
    return contacts


def enrich_contacts_waterfall(
    domain: str,
    *,
    limit: int = 5,
    verify_with_findymail: bool = True,
    fallback_to_hunter: bool = True,
) -> list[dict[str, Any]]:
    """Run the full contact enrichment waterfall: Apollo -> Findymail -> Hunter.

    Returns a list of enriched, deduplicated contact records sorted by confidence.
    """
    contacts = _apollo_search(domain, limit=limit)

    seen_emails: set[str] = set()
    enriched: list[dict[str, Any]] = []

    for c in contacts:
        email = (c.get("email") or "").strip().lower()

        if email and c.get("email_status") == "valid":
            if email not in seen_emails:
                seen_emails.add(email)
                enriched.append(c)
            continue

        if email and verify_with_findymail:
            result = _findymail_verify(email)
            if result["status"] == "valid":
                c["email"] = result["email"]
                c["email_status"] = "valid"
                c["confidence_score"] = max(c.get("confidence_score", 0), 0.88)
                c["source"] = f"apollo+findymail"
                if c["email"].lower() not in seen_emails:
                    seen_emails.add(c["email"].lower())
                    enriched.append(c)
                continue

        if c.get("full_name") and verify_with_findymail:
            fm_result = _findymail_find(c["full_name"], domain)
            if fm_result.get("email") and fm_result["status"] == "valid":
                c["email"] = fm_result["email"]
                c["email_status"] = "valid"
                c["confidence_score"] = 0.82
                c["source"] = "findymail"
                if c["email"].lower() not in seen_emails:
                    seen_emails.add(c["email"].lower())
                    enriched.append(c)
                continue

        if c.get("full_name") and fallback_to_hunter:
            h_result = _hunter_find(c["full_name"], domain)
            if h_result.get("email"):
                c["email"] = h_result["email"]
                c["email_status"] = h_result["status"]
                c["confidence_score"] = 0.65 if h_result["status"] == "valid" else 0.4
                c["source"] = "hunter"
                if c["email"].lower() not in seen_emails:
                    seen_emails.add(c["email"].lower())
                    enriched.append(c)
                continue

        if email and email not in seen_emails:
            seen_emails.add(email)
            enriched.append(c)

    enriched.sort(key=lambda x: float(x.get("confidence_score", 0)), reverse=True)
    return enriched
