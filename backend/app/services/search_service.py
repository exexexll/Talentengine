"""Universal search — one box, many sources.

Pipeline (top to bottom):

  1. Normalize + heuristic classify  (pure, 0 credits, <1 ms)
  2. Cheap LLM clean-up pass         (gpt-5.4-mini, cached, ~$0.0001/query)
  3. Local SQLite fuzzy search       (0 credits, <20 ms)
  4. Vendor fanout by intent         (Apollo / PDL / Hunter, cached 10 min)
  5. Dedupe + rank + group           (pure, <5 ms)

The universal search box is wired to existing SDR actions — opening a
local account swaps the Review Queue onto it, intaking a vendor company
calls the same ``/vendors/companies/intake`` endpoint as the map's
"+ SDR" button, and adding a person as a contact reuses the contact
waterfall.  No duplicate flows, just one more entry point.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Literal

from backend.app.services.llm_config import cheap_model, grounding_preamble
from backend.app.services.signal_taxonomy import classify_signal_type  # noqa: F401 — pipeline parity
from backend.app.services.worktrigger_store import WorkTriggerStore


# ---------------------------------------------------------------------------
# Intent classification
# ---------------------------------------------------------------------------

Intent = Literal[
    "empty",
    "domain",
    "email",
    "person_at_company",
    "person_name",
    "industry",
    "title_search",
    "keyword",
    "company_name",
]

_DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*(?:\.[a-z0-9][a-z0-9-]*)+$", re.I)
_EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
_TWO_WORDS_TITLECASE_RE = re.compile(r"^[A-Z][a-z]+(?:[- ][A-Z][a-z]+)+$")

# Known industry keywords.  Lowercase; substring match wins.
_INDUSTRY_KEYWORDS = frozenset({
    "fintech", "healthcare", "health care", "biotech", "saas", "b2b saas",
    "marketplace", "ecommerce", "e-commerce", "retail", "cpg", "manufacturing",
    "construction", "real estate", "proptech", "edtech", "insurtech",
    "legaltech", "hr tech", "hrtech", "cybersecurity", "security", "crypto",
    "web3", "ai", "ml", "machine learning", "agency", "consulting",
    "logistics", "supply chain", "energy", "clean energy", "climate tech",
    "defense", "govtech", "nonprofit", "non-profit", "media", "gaming",
    "travel", "hospitality", "automotive", "agtech", "foodtech",
})

_KNOWN_TITLES = frozenset({
    "ceo", "cto", "cfo", "coo", "cmo", "cpo", "cio", "cso", "cro", "cdo",
    "founder", "co-founder", "cofounder", "president", "vp", "svp", "evp",
    "head of", "director of", "director", "manager", "lead",
    "engineer", "developer", "designer", "recruiter", "sourcer",
    "sales", "marketing", "growth", "operations",
})


def heuristic_intent(query: str) -> Intent:
    """Zero-cost intent guess.  Returns a canonical tag the LLM can refine."""
    q = (query or "").strip()
    if not q:
        return "empty"
    lower = q.lower()
    if _EMAIL_RE.search(q):
        return "email"
    if _DOMAIN_RE.match(q):
        return "domain"
    # "CFO at Stripe" / "CTOs at fintechs" / "engineers @ databricks"
    if re.search(r"\b(at|@)\s+[A-Za-z0-9]", q):
        return "person_at_company"
    # Single-word industry match e.g. "fintech"
    if any(kw == lower or (kw in lower and len(lower.split()) <= 3) for kw in _INDUSTRY_KEYWORDS):
        return "industry"
    # Title search e.g. "CTO", "VP Engineering"
    if lower.split() and lower.split()[0] in _KNOWN_TITLES:
        return "title_search"
    if _TWO_WORDS_TITLECASE_RE.match(q):
        return "person_name"
    # Multi-word keyword phrase ("ai agent platform")
    if len(lower.split()) >= 2:
        return "keyword"
    return "company_name"


# ---------------------------------------------------------------------------
# LLM query cleanup — optional but cheap
# ---------------------------------------------------------------------------


@dataclass
class NormalizedQuery:
    """Cleaned-up, structured interpretation of a user's raw search query."""
    raw: str
    intent: Intent
    corrected: str = ""          # fixed-typo version of the raw query
    company_hint: str = ""       # "Stripe" from "cfo at strp"
    industry_hints: list[str] = field(default_factory=list)
    title_filters: list[str] = field(default_factory=list)
    seniority: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    llm_used: bool = False

    def effective_query(self) -> str:
        return self.corrected.strip() or self.raw.strip()


# OpenAI structured-output schema.
_NORMALIZE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["corrected", "intent", "industry_hints", "title_filters", "seniority", "keywords", "company_hint"],
    "properties": {
        "corrected": {"type": "string", "description": "Cleaned query — fix typos, expand abbreviations, strip filler. Empty string if no cleanup needed."},
        "company_hint": {"type": "string", "description": "If the user named a specific company (possibly misspelled), the best canonical name. Empty if none."},
        "intent": {
            "type": "string",
            "enum": [
                "domain", "email", "person_at_company", "person_name",
                "industry", "title_search", "keyword", "company_name",
            ],
        },
        "industry_hints": {"type": "array", "items": {"type": "string"}, "description": "Canonical industry terms (e.g. 'fintech', 'healthcare')."},
        "title_filters": {"type": "array", "items": {"type": "string"}, "description": "Normalized job titles (e.g. 'CTO', 'VP Engineering')."},
        "seniority": {"type": "array", "items": {"type": "string", "enum": ["executive", "senior", "mid", "junior"]}},
        "keywords": {"type": "array", "items": {"type": "string"}, "description": "Remaining free-text keywords useful for company search."},
    },
}

_NORMALIZE_PROMPT = (
    "You are a query normalizer for an SDR prospecting tool. "
    "Given a raw search query, extract a structured interpretation. "
    "Fix obvious typos (e.g. 'strp' → 'Stripe'). Expand title abbreviations. "
    "Identify whether the user is looking for a company, an industry, a person, "
    "a title, or a domain. Keep outputs concise and schema-valid.\n\n"
    "Raw query: {query}\n"
    "Heuristic intent guess (may be wrong): {intent}\n\n"
    "Return strict JSON matching the schema."
)


# In-memory LLM cache — 24 hours per raw query.
_NORM_CACHE: dict[str, tuple[float, NormalizedQuery]] = {}
_NORM_CACHE_TTL = 24 * 60 * 60


def _should_skip_llm(query: str, intent: Intent) -> bool:
    """Skip the LLM for queries where heuristics are reliable or too short."""
    if len(query) < 3:
        return True
    # Clean domain or email => no LLM needed
    if intent in ("domain", "email"):
        return True
    return False


def _normalize_with_llm(raw: str, heuristic: Intent) -> NormalizedQuery:
    """Call gpt-5.4-mini (or configured cheap model) with structured outputs."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = cheap_model()
    if not api_key:
        return NormalizedQuery(raw=raw, intent=heuristic)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": f"{grounding_preamble()} Return strict JSON matching the schema. Never invent facts."},
                {"role": "user", "content": _NORMALIZE_PROMPT.format(query=raw, intent=heuristic)},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "normalized_search_query", "schema": _NORMALIZE_SCHEMA, "strict": True},
            },
            temperature=0.0,
            max_completion_tokens=300,
        )
        data = json.loads(resp.choices[0].message.content or "{}")
    except Exception:
        return NormalizedQuery(raw=raw, intent=heuristic)

    return NormalizedQuery(
        raw=raw,
        intent=(data.get("intent") or heuristic),  # type: ignore[arg-type]
        corrected=str(data.get("corrected") or ""),
        company_hint=str(data.get("company_hint") or ""),
        industry_hints=list(data.get("industry_hints") or []),
        title_filters=list(data.get("title_filters") or []),
        seniority=list(data.get("seniority") or []),
        keywords=list(data.get("keywords") or []),
        llm_used=True,
    )


def normalize_query(raw: str) -> NormalizedQuery:
    """Heuristic first, cheap LLM second (cached).  Never raises."""
    q = (raw or "").strip()
    if not q:
        return NormalizedQuery(raw="", intent="empty")

    cache_key = q.lower()
    hit = _NORM_CACHE.get(cache_key)
    if hit and time.time() - hit[0] < _NORM_CACHE_TTL:
        return hit[1]

    intent = heuristic_intent(q)
    if _should_skip_llm(q, intent):
        norm = NormalizedQuery(raw=q, intent=intent)
    else:
        norm = _normalize_with_llm(q, intent)

    _NORM_CACHE[cache_key] = (time.time(), norm)
    if len(_NORM_CACHE) > 2000:
        # Evict oldest 500 when cache grows
        for k in list(_NORM_CACHE.keys())[:500]:
            _NORM_CACHE.pop(k, None)
    return norm


# ---------------------------------------------------------------------------
# Result shapes
# ---------------------------------------------------------------------------


def _local_account_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "local_account",
        "id": row["id"],
        "name": row.get("name") or row.get("domain") or "",
        "domain": row.get("domain") or "",
        "industry": row.get("industry") or "",
        "employee_count": row.get("employee_count"),
        "funding_stage": row.get("funding_stage"),
        "country": row.get("country") or "",
        "linkedin_url": row.get("linkedin_url") or "",
        "twitter_url": row.get("twitter_url") or "",
        "icp_status": row.get("icp_status") or "unknown",
        "signal_score": float(row.get("signal_score") or 0.0),
        "draft_count": int(row.get("draft_count") or 0),
        "contact_count": int(row.get("contact_count") or 0),
        "updated_at": row.get("updated_at"),
    }


def _local_contact_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "local_contact",
        "contact_id": row["contact_id"],
        "full_name": row.get("full_name") or "",
        "title": row.get("title") or "",
        "email": row.get("email") or "",
        "linkedin_url": row.get("linkedin_url") or "",
        "account_id": row["account_id"],
        "account_name": row.get("account_name") or "",
        "account_domain": row.get("account_domain") or "",
        "industry": row.get("industry") or "",
        "confidence_score": float(row.get("confidence_score") or 0.0),
    }


def _company_item(raw: dict[str, Any], source: str) -> dict[str, Any]:
    return {
        "kind": "company",
        "name": raw.get("name") or "",
        "domain": raw.get("domain") or raw.get("website") or "",
        "industry": raw.get("industry") or "",
        "employee_count": raw.get("employee_count") or raw.get("estimated_num_employees"),
        "funding_stage": raw.get("funding_stage") or raw.get("latest_funding_stage"),
        "country": raw.get("country") or "",
        "linkedin_url": raw.get("linkedin_url") or "",
        "logo_url": raw.get("logo_url") or "",
        "short_description": (raw.get("short_description") or raw.get("summary") or "")[:200],
        "source": source,
    }


def _person_item(raw: dict[str, Any], source: str) -> dict[str, Any]:
    org = raw.get("organization") or {}
    return {
        "kind": "person",
        "full_name": raw.get("full_name") or raw.get("name") or "",
        "title": raw.get("title") or "",
        "linkedin_url": raw.get("linkedin_url") or "",
        "company_name": raw.get("company_name") or org.get("name") or "",
        "company_domain": raw.get("company_domain") or org.get("primary_domain") or org.get("domain") or "",
        "source": source,
    }


# ---------------------------------------------------------------------------
# Vendor fanout (company + people) — cached per-query server-side
# ---------------------------------------------------------------------------


_VENDOR_CACHE: dict[str, tuple[float, Any]] = {}
_VENDOR_CACHE_TTL = 10 * 60  # 10 minutes


def _vendor_cache_get(key: str) -> Any:
    hit = _VENDOR_CACHE.get(key)
    if hit and time.time() - hit[0] < _VENDOR_CACHE_TTL:
        return hit[1]
    return None


def _vendor_cache_set(key: str, value: Any) -> None:
    _VENDOR_CACHE[key] = (time.time(), value)
    if len(_VENDOR_CACHE) > 1000:
        for k in list(_VENDOR_CACHE.keys())[:200]:
            _VENDOR_CACHE.pop(k, None)


def _apollo_company_search(
    query: str,
    *,
    industries: list[str] | None = None,
    limit: int = 8,
    page: int = 1,
) -> tuple[list[dict[str, Any]], int]:
    """Apollo `mixed_companies/search`.  Returns ``(hits, total_available)``.

    Apollo charges one credit per search **per page**, independent of
    ``per_page`` (capped at 100 by Apollo).  So for industry-bulk queries
    we always request ``per_page=100`` and let the UI paginate client-side
    with zero extra credits until the user explicitly asks for the next
    100-row batch.
    """
    import httpx
    api_key = os.getenv("APOLLO_API_KEY", "").strip()
    if not api_key:
        return [], 0
    params: list[tuple[str, str]] = [
        ("page", str(max(1, page))),
        ("per_page", str(min(100, max(1, limit)))),
    ]
    if query:
        params.append(("q_organization_name", query))
    for industry in (industries or []):
        clean = industry.strip().lower()
        if clean:
            params.append(("q_organization_keyword_tags[]", clean))
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(
                "https://api.apollo.io/api/v1/mixed_companies/search",
                headers={"Content-Type": "application/json", "Cache-Control": "no-cache", "x-api-key": api_key},
                params=params,
            )
        if resp.status_code >= 300:
            return [], 0
        data = resp.json()
    except httpx.HTTPError:
        return [], 0
    orgs = data.get("organizations") or data.get("accounts") or []
    pagination = data.get("pagination") or {}
    total = int(pagination.get("total_entries") or len(orgs))
    out: list[dict[str, Any]] = []
    for o in orgs[:limit]:
        out.append({
            "name": o.get("name", ""),
            "domain": o.get("primary_domain") or o.get("domain") or "",
            "industry": o.get("industry"),
            "employee_count": o.get("estimated_num_employees"),
            "funding_stage": o.get("latest_funding_stage"),
            "country": o.get("country"),
            "linkedin_url": o.get("linkedin_url"),
            "logo_url": o.get("logo_url"),
            "short_description": (o.get("short_description") or "")[:200],
        })
    return out, total


def _apollo_people_search(
    query: str,
    *,
    titles: list[str],
    company_domain: str = "",
    limit: int = 8,
) -> list[dict[str, Any]]:
    """Apollo `mixed_people/api_search` WITHOUT email reveal (cheapest path)."""
    import httpx
    api_key = os.getenv("APOLLO_API_KEY", "").strip()
    if not api_key:
        return []
    params: list[tuple[str, str]] = [
        ("page", "1"),
        ("per_page", str(min(25, max(1, limit)))),
    ]
    if query and not titles:
        params.append(("q_keywords", query))
    for t in titles[:5]:
        params.append(("person_titles[]", t.strip()))
    if company_domain:
        params.append(("q_organization_domains_list[]", company_domain.strip().lower()))
    try:
        with httpx.Client(timeout=12) as client:
            resp = client.post(
                "https://api.apollo.io/api/v1/mixed_people/api_search",
                headers={"Content-Type": "application/json", "Cache-Control": "no-cache", "x-api-key": api_key},
                params=params,
            )
        if resp.status_code >= 300:
            return []
        data = resp.json()
    except httpx.HTTPError:
        return []

    people = data.get("people") or []
    out: list[dict[str, Any]] = []
    for p in people[:limit]:
        org = p.get("organization") or {}
        out.append({
            "full_name": p.get("name") or f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
            "title": p.get("title") or "",
            "linkedin_url": p.get("linkedin_url") or "",
            "company_name": org.get("name") or "",
            "company_domain": org.get("primary_domain") or org.get("website_url") or "",
        })
    return out


# ---------------------------------------------------------------------------
# SearchService
# ---------------------------------------------------------------------------


class SearchService:
    def __init__(self, store: WorkTriggerStore) -> None:
        self.store = store

    def search(
        self,
        raw_query: str,
        *,
        types: str = "all",
        limit: int = 20,
        apollo_page: int = 1,
        industries: list[str] | None = None,
    ) -> dict[str, Any]:
        """Run the full pipeline.  Returns grouped results + metadata."""
        started = time.time()
        if not raw_query or not raw_query.strip():
            return self._empty_response(raw_query)

        norm = normalize_query(raw_query)
        effective = norm.effective_query()
        credits: dict[str, int] = {"apollo": 0, "pdl": 0, "hunter": 0}
        selected_industries = [x.strip().lower() for x in (industries or []) if x and x.strip()]
        heuristic_industries: list[str] = []
        raw_lower = raw_query.strip().lower()
        # Help short natural-language phrases like "student nonprofit"
        # resolve into provider-friendly industry tags even when the LLM
        # returns sparse hints.
        if "student" in raw_lower:
            heuristic_industries.append("education")
        if "nonprofit" in raw_lower or "non-profit" in raw_lower:
            heuristic_industries.append("nonprofit")
        merged_industry_hints = list(dict.fromkeys([
            *(x.strip().lower() for x in norm.industry_hints if x and x.strip()),
            *heuristic_industries,
            *selected_industries,
        ]))

        # 1) Local — always cheap, always runs
        local_accounts_raw = self.store.fuzzy_search_accounts(effective, limit=max(4, limit // 2))
        local_contacts_raw = self.store.fuzzy_search_contacts(effective, limit=max(3, limit // 3))
        if norm.company_hint and norm.company_hint.lower() != effective.lower():
            # Additional cheap local search on the LLM-corrected name
            extra_accounts = self.store.fuzzy_search_accounts(norm.company_hint, limit=5)
            for row in extra_accounts:
                if all(r["id"] != row["id"] for r in local_accounts_raw):
                    local_accounts_raw.append(row)

        local_accounts = [_local_account_item(r) for r in local_accounts_raw]
        local_contacts = [_local_contact_item(r) for r in local_contacts_raw]

        # 2) Vendor fanout — conditional on intent + user's `types` filter
        companies: list[dict[str, Any]] = []
        people: list[dict[str, Any]] = []
        want_companies = types in {"all", "companies", "industries"}
        want_people = types in {"all", "people"}

        # Decide which vendor paths to fire.  Beyond the raw intent, also
        # fan out to people search whenever the LLM extracted title filters
        # (e.g. "fintechs hiring CTOs" has intent=industry but clearly
        # wants both companies and people), and skip company search for
        # pure person-name queries.
        company_intents = {"company_name", "keyword", "industry", "domain", "title_search"}
        people_intents = {"person_name", "person_at_company", "title_search", "email"}
        has_title = bool(norm.title_filters)

        apollo_total = 0
        if want_companies and norm.intent in company_intents:
            cache_key = f"cmp::{effective.lower()}::{'|'.join(merged_industry_hints)[:120]}::{types}::p{apollo_page}"
            cached = _vendor_cache_get(cache_key)
            if cached is not None:
                companies = cached["companies"]
                apollo_total = cached.get("total", len(companies))  # type: ignore[union-attr]
            else:
                industry_hints = merged_industry_hints[:5]
                # Industry-bulk maxes out Apollo's per_page at 100 (the
                # hard API cap) so we get every company Apollo will give
                # us for one credit.  Client-side pagination slices those
                # 100 into 8-row pages with zero additional cost.  If the
                # user explicitly clicks "Load next 100" the frontend
                # bumps ``apollo_page`` to 2, which spends one more credit.
                is_industry_bulk = norm.intent == "industry" or types == "industries" or bool(industry_hints)
                per_call_limit = 100 if is_industry_bulk else 8
                if norm.intent == "industry" and not norm.company_hint:
                    q_for_apollo = ""
                else:
                    q_for_apollo = norm.company_hint or effective
                hits, total_avail = _apollo_company_search(
                    q_for_apollo,
                    industries=industry_hints,
                    limit=per_call_limit,
                    page=apollo_page,
                )
                if hits:
                    credits["apollo"] += 1
                companies = [_company_item(h, "apollo") for h in hits]
                apollo_total = total_avail
                _vendor_cache_set(cache_key, {"companies": companies, "total": total_avail})  # type: ignore[arg-type]

        fire_people = want_people and (norm.intent in people_intents or has_title)
        if fire_people:
            company_domain_hint = ""
            # "CFO at Stripe" — try to pull domain from any matching local account.
            at_match = re.search(r"\b(?:at|@)\s+([A-Za-z0-9][A-Za-z0-9.\- ]+)$", effective)
            if at_match:
                tail = at_match.group(1).strip().rstrip(".").lower()
                if "." in tail:
                    company_domain_hint = tail
                else:
                    for r in local_accounts_raw:
                        if tail in (r.get("name") or "").lower() or tail in (r.get("domain") or "").lower():
                            company_domain_hint = r.get("domain") or ""
                            break
            cache_key = f"ppl::{effective.lower()}::{'|'.join(norm.title_filters)}::{company_domain_hint}"
            cached = _vendor_cache_get(cache_key)
            if cached is not None:
                people = cached
            else:
                titles = norm.title_filters or []
                if norm.intent == "title_search" and not titles:
                    titles = [effective]
                q_for_apollo = norm.company_hint if norm.intent == "person_at_company" else effective
                hits = _apollo_people_search(
                    q_for_apollo,
                    titles=titles,
                    company_domain=company_domain_hint,
                    limit=8,
                )
                if hits:
                    credits["apollo"] += 1
                people = [_person_item(h, "apollo") for h in hits]
                _vendor_cache_set(cache_key, people)

        # 3) Dedupe vendor results against local accounts (don't show the same
        #    company twice — if it's in local, the vendor row is redundant).
        local_domains = {(a.get("domain") or "").lower() for a in local_accounts if a.get("domain")}
        companies = [c for c in companies if (c.get("domain") or "").lower() not in local_domains]

        # 4) Rank within each group (see docstring at top of file)
        companies = _rank_companies(companies, effective)
        local_accounts = _rank_local_accounts(local_accounts, effective)
        people = _rank_people(people, effective, norm)

        # 5) Assemble response
        is_industry_bulk = norm.intent == "industry" or types == "industries" or bool(merged_industry_hints)
        groups: list[dict[str, Any]] = []
        if local_accounts:
            groups.append({"kind": "local_accounts", "label": "Already in your pipeline", "items": local_accounts[:8]})
        if local_contacts:
            groups.append({"kind": "local_contacts", "label": "Contacts you've saved", "items": local_contacts[:5]})
        if companies:
            # For industry-bulk queries return ALL fetched rows (Apollo
            # gave us up to 100 for 1 credit) so the UI paginates them
            # client-side with first 8 rich / rest compact and zero
            # additional credit cost.
            company_items = companies if is_industry_bulk else companies[:8]
            label = f"Companies in {', '.join(merged_industry_hints[:2])}" if is_industry_bulk and merged_industry_hints else "Companies"
            groups.append({
                "kind": "companies",
                "label": label,
                "items": company_items,
                "rich_count": 8,
                "paginated": is_industry_bulk and len(company_items) > 8,
                "total": len(company_items),
                # Metadata for "Load next 100 (+1 credit)" button.  Apollo
                # will give us up to ``apollo_total`` across all server
                # pages; we've used ``apollo_page`` so far.
                "apollo_total": apollo_total,
                "apollo_page": apollo_page,
                "can_load_more": is_industry_bulk and apollo_total > apollo_page * 100,
            })
        if people:
            groups.append({"kind": "people", "label": "People", "items": people[:8]})

        return {
            "query": raw_query,
            "normalized": {
                "effective": effective,
                "intent": norm.intent,
                "corrected": norm.corrected,
                "company_hint": norm.company_hint,
                "industry_hints": merged_industry_hints,
                "title_filters": norm.title_filters,
                "llm_used": norm.llm_used,
            },
            "groups": groups,
            "credits_spent": credits,
            "took_ms": int((time.time() - started) * 1000),
        }

    def _empty_response(self, raw: str) -> dict[str, Any]:
        return {
            "query": raw,
            "normalized": {"effective": "", "intent": "empty", "corrected": "", "company_hint": "", "industry_hints": [], "title_filters": [], "llm_used": False},
            "groups": [],
            "credits_spent": {"apollo": 0, "pdl": 0, "hunter": 0},
            "took_ms": 0,
        }


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------


def _match_bonus(haystack: str, needle: str) -> int:
    h = (haystack or "").lower()
    n = (needle or "").lower()
    if not h or not n:
        return 0
    if h == n:
        return 30
    if h.startswith(n):
        return 20
    if n in h:
        return 10
    return 0


def _rank_local_accounts(items: list[dict[str, Any]], q: str) -> list[dict[str, Any]]:
    for it in items:
        score = 50  # base bonus — local always outranks remote
        score += _match_bonus(it.get("domain", ""), q)
        score += _match_bonus(it.get("name", ""), q)
        score += min(20, int(it.get("signal_score") or 0) // 5)
        if it.get("draft_count"):
            score += 5
        it["_rank"] = score
    items.sort(key=lambda x: x.get("_rank", 0), reverse=True)
    return items


def _rank_companies(items: list[dict[str, Any]], q: str) -> list[dict[str, Any]]:
    for it in items:
        score = 0
        score += _match_bonus(it.get("domain", ""), q)
        score += _match_bonus(it.get("name", ""), q)
        if it.get("linkedin_url"):
            score += 5
        # Penalize obvious noise
        nm = (it.get("name") or "").lower()
        if any(x in nm for x in ("test", "smoke", "example", "demo", "placeholder")):
            score -= 50
        it["_rank"] = score
    items.sort(key=lambda x: x.get("_rank", 0), reverse=True)
    return items


def _rank_people(items: list[dict[str, Any]], q: str, norm: NormalizedQuery) -> list[dict[str, Any]]:
    want_titles = {t.lower() for t in norm.title_filters}
    for it in items:
        score = 0
        score += _match_bonus(it.get("full_name", ""), q)
        title_lower = (it.get("title") or "").lower()
        if want_titles and any(t in title_lower for t in want_titles):
            score += 15
        if any(t in title_lower for t in ("ceo", "founder", "cto", "cfo", "coo", "vp", "head")):
            score += 8
        if it.get("linkedin_url"):
            score += 3
        it["_rank"] = score
    items.sort(key=lambda x: x.get("_rank", 0), reverse=True)
    return items
