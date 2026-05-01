"""Social signal analysis — LinkedIn/Twitter post scraping + AI signal extraction.

Scrapes recent LinkedIn company posts and Twitter/X posts via SerpAPI Google search,
fetches Apollo job postings and PDL job listings, then uses OpenAI to extract
structured business signals from the combined data.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


# --- Persistent cache (shared across restarts) -----------------------------

_SOCIAL_CACHE_DIR = Path("backend/data/social_signals_cache")
_SOCIAL_CACHE_TTL = 24 * 60 * 60  # 24 hours — social data is slow-moving
_SOCIAL_PROMPT_VERSION = "v2-anti-generic"


def _norm_url_for_cache(u: str) -> str:
    """Stable lowercased URL fragment for cache keys (ignore query strings)."""
    s = (u or "").strip().lower()
    if not s:
        return ""
    return s.split("?", 1)[0].rstrip("/")


def _cache_path(domain: str, linkedin_url: str = "", twitter_url: str = "") -> Path:
    """Disk path for this company's social snapshot.

    Keyed primarily by **domain**.  When ``linkedin_url`` or ``twitter_url``
    are supplied, a short hash suffix is added so enriching the account
    with a real LinkedIn company URL does not keep reusing a stale cache
    that was built from name-only heuristics (a real oversight in v1).
    """
    safe = "".join(c if c.isalnum() else "_" for c in domain.strip().lower())[:80]
    li = _norm_url_for_cache(linkedin_url)
    tw = _norm_url_for_cache(twitter_url)
    if li or tw:
        h = hashlib.sha256(f"{li}|{tw}".encode("utf-8")).hexdigest()[:14]
        return _SOCIAL_CACHE_DIR / f"{safe}_{h}.json"
    return _SOCIAL_CACHE_DIR / f"{safe}.json"


def _cache_load(domain: str, linkedin_url: str = "", twitter_url: str = "") -> dict[str, Any] | None:
    p = _cache_path(domain, linkedin_url, twitter_url)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        ts = float(data.get("_cached_at", 0))
        if data.get("_prompt_version") != _SOCIAL_PROMPT_VERSION:
            return None
        if time.time() - ts < _SOCIAL_CACHE_TTL:
            data["_cache_hit"] = True
            return data
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return None


def _cache_save(
    domain: str,
    payload: dict[str, Any],
    linkedin_url: str = "",
    twitter_url: str = "",
) -> None:
    try:
        _SOCIAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        p = _cache_path(domain, linkedin_url, twitter_url)
        body = dict(payload)
        body["_cached_at"] = time.time()
        body["_prompt_version"] = _SOCIAL_PROMPT_VERSION
        p.write_text(json.dumps(body), encoding="utf-8")
    except OSError:
        pass


def _serpapi_search(query: str, *, num: int = 10, time_range: str = "qdr:m3") -> list[dict[str, Any]]:
    """Run a SerpAPI Google search with time range filter."""
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if not api_key:
        return []
    try:
        from serpapi import GoogleSearch
        search = GoogleSearch({
            "engine": "google",
            "q": query,
            "api_key": api_key,
            "num": num,
            "tbs": time_range,
            "hl": "en",
        })
        data = search.get_dict()
        return data.get("organic_results", [])
    except Exception as exc:
        print(f"[SocialSignals] SerpAPI search failed: {exc}")
        return []


# --- Entity attribution helpers -------------------------------------------
#
# The single biggest source of noise in Twitter/X scraping was the prior
# "search for <company name> on x.com" approach: Google happily returned
# tweets *about* the company (news, complaints, lists, mentions by random
# people, fan accounts, even completely unrelated accounts whose tweets
# happened to contain the company's name as a keyword).  We never want
# those — for sales-signal purposes we only care about tweets *from* the
# company itself.
#
# The fix: resolve a small set of plausible candidate handles for the
# company, then strictly require every returned URL to be a status post
# whose handle segment matches one of those candidates.  When we cannot
# resolve any handle confidently, we return zero posts rather than
# serving misattributed content.

_TWITTER_HOSTS = {
    "x.com", "www.x.com",
    "twitter.com", "www.twitter.com", "mobile.twitter.com",
}

# Handles that show up at /<segment>/ but are NOT user accounts — we must
# never treat them as the "company handle".
_TWITTER_RESERVED = {
    "i", "search", "explore", "home", "notifications", "messages",
    "compose", "intent", "share", "hashtag", "settings", "tos", "privacy",
    "about", "login", "signup", "logout", "help",
}


def _twitter_url_handle(url: str) -> str | None:
    """Return the lowercase handle from a *status post* URL.

    Accepts only canonical status URLs of the form
    ``https://x.com/<handle>/status/<id>`` (and the twitter.com /
    mobile.twitter.com aliases).  Returns ``None`` for anything else —
    including search pages, profile-only links, ``/i/`` URLs, hashtag
    pages, lists pages, etc.  Reserved segments are also rejected.
    """
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        u = urlparse(url)
    except Exception:
        return None
    if (u.netloc or "").lower() not in _TWITTER_HOSTS:
        return None
    parts = [p for p in u.path.split("/") if p]
    if len(parts) < 3:
        return None
    handle = parts[0].lower().lstrip("@")
    if handle in _TWITTER_RESERVED:
        return None
    if parts[1].lower() != "status":
        return None
    # parts[2] should be a numeric tweet id
    if not parts[2].isdigit():
        return None
    return handle


def _candidate_twitter_handles(
    company_name: str = "",
    twitter_url: str = "",
    domain: str = "",
) -> list[str]:
    """Build a ranked list of plausible Twitter handles for a company.

    Order is by confidence — first entry is the most trusted, used to
    label every matched post.  We cap aggressively so the SerpAPI fanout
    stays cheap.
    """
    cands: list[str] = []
    seen: set[str] = set()

    def _add(h: str | None) -> None:
        if not h:
            return
        h = h.strip().lstrip("@").lower()
        # Strip surrounding non-alphanum but allow underscores (valid in
        # Twitter handles).  Twitter handles are 4-15 chars in practice
        # but minimum-length filter would reject "uber"/"ibm"/"ge", so
        # we just require non-empty.
        h = "".join(c for c in h if c.isalnum() or c == "_")
        if not h or h in seen or h in _TWITTER_RESERVED:
            return
        seen.add(h)
        cands.append(h)

    # 1. Definitive: explicit URL passed in (either from CRM/enrichment
    #    or already on the account row).  Highest trust.
    if twitter_url:
        try:
            seg = twitter_url.rstrip("/").split("/")[-1]
            _add(seg)
        except Exception:
            pass

    # 2. Strong: primary domain stem.  e.g. figwork.ai → "figwork";
    #    common.com → "common".  Most companies' Twitter handle matches
    #    their primary domain stem either exactly or with a small suffix.
    if domain:
        stem = domain.lower().split(".")[0]
        if stem and len(stem) >= 2:
            _add(stem)
            _add(f"{stem}hq")
            _add(f"get{stem}")

    # 3. Weak: company name normalised (alphanum only).  Risky for short
    #    names that collide with English words ("Apple", "Slack"), so we
    #    only emit it as a fallback after domain-derived candidates.
    if company_name:
        norm = "".join(c for c in company_name.lower() if c.isalnum())
        if norm:
            _add(norm)

    return cands[:5]


def _linkedin_org_from_url(url: str) -> tuple[str | None, str | None]:
    """Return ``(slug, segment)`` for a LinkedIn org URL.

    ``segment`` is one of ``company``, ``school``, or ``showcase`` — used
    for both attribution checks and ``site:`` search scoping so colleges
    and showcase pages are not mis-handled as ``/company/`` paths.
    """
    if not url:
        return None, None
    try:
        from urllib.parse import urlparse
        u = urlparse(url)
    except Exception:
        return None, None
    if "linkedin.com" not in (u.netloc or "").lower():
        return None, None
    parts = [p for p in u.path.split("/") if p]
    if len(parts) >= 2 and parts[0].lower() in ("company", "school", "showcase"):
        return parts[1].lower(), parts[0].lower()
    return None, None


def _linkedin_url_company_slug(url: str) -> str | None:
    """Extract a LinkedIn org slug from a URL, or None (any org type)."""
    slug, _ = _linkedin_org_from_url(url)
    return slug


def scrape_linkedin_posts(
    company_name: str,
    linkedin_url: str = "",
    *,
    limit: int = 8,
    domain: str = "",
) -> list[dict[str, Any]]:
    """Scrape recent LinkedIn posts attributable to a specific company.

    Strategy:
      1. If we have an org URL (``/company/``, ``/school/``, or ``/showcase/``),
         prefer Serp results under that exact org path — definitively org-owned.
      2. Otherwise expand to ``site:linkedin.com/posts`` plus the company
         name; only keep results whose snippet/title actually contains
         a meaningful match for the company.  This is still permissive
         because LinkedIn's URL space is messier than Twitter's.
    """
    slug, li_seg = _linkedin_org_from_url(linkedin_url) if linkedin_url else (None, None)
    queries: list[str] = []

    if slug and li_seg:
        queries.append(f"site:linkedin.com/{li_seg}/{slug}/posts")
        queries.append(f"site:linkedin.com/feed/update \"{company_name}\"")
    queries.append(f"site:linkedin.com/posts \"{company_name}\"")
    if domain:
        # Many founders/employees mention their company's domain in posts;
        # this is a strong attribution signal for posts about the company.
        queries.append(f"site:linkedin.com/posts \"{domain}\"")

    posts: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    name_lc = (company_name or "").lower()

    for query in queries:
        results = _serpapi_search(query, num=8)
        for item in results:
            url = item.get("link", "")
            if url in seen_urls or "linkedin.com" not in url:
                continue
            own_slug, own_seg = _linkedin_org_from_url(url)
            text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
            if own_slug == slug and own_seg == li_seg and slug and li_seg:
                confidence = "high"
            elif name_lc and name_lc in text:
                confidence = "medium"
            else:
                # No attribution evidence — drop rather than mislabel.
                continue
            seen_urls.add(url)
            posts.append({
                "platform": "linkedin",
                "title": item.get("title", ""),
                "text": item.get("snippet", ""),
                "url": url,
                "date": item.get("date", ""),
                "source": "serpapi_linkedin",
                "entity_confidence": confidence,
                "matched_company_slug": own_slug,
                "matched_linkedin_segment": own_seg,
            })
        if len(posts) >= limit:
            break

    return posts[:limit]


def scrape_twitter_posts(
    company_name: str,
    twitter_url: str = "",
    *,
    limit: int = 6,
    domain: str = "",
) -> list[dict[str, Any]]:
    """Scrape recent Twitter/X posts FROM a specific company account.

    Hard constraints:
      * Every returned post is a status URL (``/<handle>/status/<id>``).
      * Every handle is one of a small set of pre-resolved candidate
        handles for this company — never a generic keyword match.
      * If no handles can be resolved, returns ``[]`` rather than
        producing misattributed content.

    The pre-2026 implementation searched for "<company name>" anywhere on
    x.com/twitter.com, which routinely surfaced tweets *about* the
    company (news commentary, third-party mentions, unrelated accounts
    using the name as a keyword) — that was the noise the user reported.
    """
    candidates = _candidate_twitter_handles(company_name, twitter_url, domain)
    if not candidates:
        return []

    handle_set = set(candidates)
    primary_handle = candidates[0]

    queries: list[str] = []
    # Site-scoped to each candidate handle's status namespace.  Google's
    # `site:` operator matches URL prefixes, so this returns ONLY tweets
    # posted by that account (vs replies/quotes/mentions).
    for h in candidates[:3]:
        queries.append(f"site:x.com/{h}/status")
        queries.append(f"site:twitter.com/{h}/status")

    posts: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for query in queries:
        results = _serpapi_search(query, num=6)
        for item in results:
            url = item.get("link", "")
            if not url or url in seen_urls:
                continue
            handle = _twitter_url_handle(url)
            if not handle or handle not in handle_set:
                # Either not a status URL or not from one of our resolved
                # company handles — drop unconditionally.  This is what
                # eliminates the "wrong entity" problem.
                continue
            seen_urls.add(url)
            posts.append({
                "platform": "twitter",
                "title": item.get("title", ""),
                "text": item.get("snippet", ""),
                "url": url,
                "date": item.get("date", ""),
                "source": "serpapi_twitter",
                "matched_handle": handle,
                # Definitive when the handle came from an explicit URL we
                # were given; "derived" when we inferred it from the
                # domain/name.  The UI can use this to gate display.
                "entity_confidence": "high" if (twitter_url and handle == primary_handle) else "medium",
            })
        if len(posts) >= limit:
            break

    return posts[:limit]


def fetch_apollo_job_postings(domain: str, *, limit: int = 10) -> list[dict[str, Any]]:
    """Fetch active job postings from Apollo for a company domain."""
    api_key = os.getenv("APOLLO_API_KEY", "").strip()
    if not api_key:
        return []

    headers = {"Content-Type": "application/json", "x-api-key": api_key}
    with httpx.Client(timeout=20) as client:
        enrich_resp = client.get(
            "https://api.apollo.io/api/v1/organizations/enrich",
            headers=headers,
            params={"domain": domain.strip().lower()},
        )
        if enrich_resp.status_code >= 300:
            return []
        org = enrich_resp.json().get("organization") or {}
        org_id = org.get("id")
        if not org_id:
            return []

        jobs_resp = client.get(
            f"https://api.apollo.io/api/v1/organizations/{org_id}/job_postings",
            headers=headers,
        )
        if jobs_resp.status_code >= 300:
            return []
        postings = jobs_resp.json().get("organization_job_postings") or []

    return [
        {
            "title": j.get("title", ""),
            "url": j.get("url", ""),
            "posted_at": j.get("posted_at", ""),
            "source": "apollo_jobs",
        }
        for j in postings[:limit]
    ]


def fetch_pdl_job_listings(domain: str, *, limit: int = 5) -> list[dict[str, Any]]:
    """Fetch recent job listings from PDL."""
    try:
        from backend.app.services.vendors.pdl import pdl_job_listings
        return pdl_job_listings(domain, limit=limit)
    except Exception:
        return []


def analyze_signals_with_ai(
    company_name: str,
    linkedin_posts: list[dict[str, Any]],
    twitter_posts: list[dict[str, Any]],
    job_postings: list[dict[str, Any]],
) -> dict[str, Any]:
    """Use OpenAI to extract structured business signals from social data."""
    from backend.app.services.llm_config import grounding_preamble, primary_model

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return _fallback_signal_analysis(linkedin_posts, twitter_posts, job_postings)

    post_texts = []
    for p in linkedin_posts[:6]:
        post_texts.append(f"[LinkedIn] {p.get('title', '')} — {p.get('text', '')}")
    for p in twitter_posts[:4]:
        post_texts.append(f"[Twitter/X] {p.get('title', '')} — {p.get('text', '')}")
    for j in job_postings[:8]:
        post_texts.append(f"[Job Posting] {j.get('title', '')} posted {j.get('posted_at', '')}")

    if not post_texts:
        return {"signals": [], "summary": "No recent social activity or job postings found.", "hiring_intensity": "none", "sentiment": "neutral"}

    combined = "\n".join(post_texts)

    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    model = primary_model()

    prompt = f"""{grounding_preamble()}

Analyze the following recent LinkedIn posts, Twitter posts, and job postings for {company_name}.
Extract structured business signals that indicate:
1. Hiring intensity (aggressive/moderate/minimal/none)
2. Growth signals (expanding, new product, funding, partnership)
3. Pain points or challenges mentioned
4. Key departments hiring (engineering, sales, marketing, ops, etc.)
5. Overall company momentum (accelerating/steady/slowing/restructuring)

Quality bar:
- No broad generic wording. Avoid phrases like "company appears strong" or "shows positive momentum" unless tied to specific evidence.
- Each signal description must reference a concrete observed detail (role title, team, initiative, product, location, date cue, or quoted phrase).
- If evidence is weak, return fewer signals instead of vague signals.
- Outreach angle must name one concrete trigger from the evidence.

Social data:
{combined}

Return strict JSON with this schema:
{{
  "signals": [
    {{"type": "hiring|growth|pain_point|partnership|product_launch|funding|restructuring", "description": "1-2 sentence description", "confidence": 0.0-1.0, "evidence": "which post/job this came from"}}
  ],
  "summary": "2-3 sentence executive summary of what this company is doing right now",
  "hiring_intensity": "aggressive|moderate|minimal|none",
  "hiring_departments": ["engineering", "sales", ...],
  "growth_signals": ["signal1", "signal2", ...],
  "pain_points": ["pain1", "pain2", ...],
  "momentum": "accelerating|steady|slowing|restructuring",
  "outreach_angle": "1 sentence suggesting the best angle for Figwork outreach based on these signals"
}}"""

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_completion_tokens=1500,
            response_format={"type": "json_object"},
        )
        return json.loads(resp.choices[0].message.content or "{}")
    except Exception as exc:
        print(f"[SocialSignals] AI analysis failed: {exc}")
        return _fallback_signal_analysis(linkedin_posts, twitter_posts, job_postings)


def _fallback_signal_analysis(
    linkedin_posts: list[dict[str, Any]],
    twitter_posts: list[dict[str, Any]],
    job_postings: list[dict[str, Any]],
) -> dict[str, Any]:
    """Keyword-based signal extraction used when OpenAI is unavailable.

    Delegates all classification to ``signal_taxonomy.classify_post`` so
    we never duplicate keyword lists across modules.
    """
    from backend.app.services.signal_taxonomy import classify_post

    signals: list[dict[str, Any]] = []
    if job_postings:
        titles = [j.get("title", "") for j in job_postings[:5]]
        signals.append({
            "type": "hiring",
            "description": f"Actively hiring {len(job_postings)} roles including: {', '.join(titles[:3])}",
            "confidence": 0.9,
            "evidence": "job_postings",
        })

    growth_signals: list[str] = []
    pain_points: list[str] = []
    for p in linkedin_posts + twitter_posts:
        text = f"{p.get('title', '')} {p.get('text', '')}"
        cls = classify_post(text)
        if cls.top_category is None:
            continue
        snippet = (p.get("title") or p.get("text") or "").strip()[:100]
        signals.append({
            "type": cls.top_category,
            "description": snippet,
            "confidence": 0.7,
            "evidence": p.get("url", ""),
        })
        if cls.top_category == "growth" and snippet:
            growth_signals.append(snippet)
        if cls.top_category == "pain_point" and snippet:
            pain_points.append(snippet)

    if len(job_postings) > 5:
        intensity = "aggressive"
    elif job_postings:
        intensity = "moderate"
    else:
        intensity = "none"

    return {
        "signals": signals[:10],
        "summary": f"Company has {len(job_postings)} active job postings and {len(linkedin_posts)} recent LinkedIn posts.",
        "hiring_intensity": intensity,
        "hiring_departments": [],
        "growth_signals": growth_signals[:5],
        "pain_points": pain_points[:5],
        "momentum": "steady",
        "outreach_angle": "",
    }


def get_company_social_signals(
    domain: str,
    company_name: str = "",
    linkedin_url: str = "",
    twitter_url: str = "",
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Full social signal pipeline: scrape → analyze → return structured signals.

    Result is cached to disk for 24 hours keyed on the normalized domain so
    that tab switches, account re-opens, and server restarts do not burn
    Apollo / SerpAPI / OpenAI credits. Pass force_refresh=True to bypass.
    """
    domain_key = (domain or "").strip().lower()
    li_key = (linkedin_url or "").strip()
    tw_key = (twitter_url or "").strip()
    if not force_refresh and domain_key:
        cached = _cache_load(domain_key, li_key, tw_key)
        if cached is not None:
            return cached

    name = company_name or domain.split(".")[0]
    # Pass `domain` through so the entity-attribution helpers (which now
    # gate every Twitter/X result on a verified handle and label LinkedIn
    # results with a confidence tier) can derive candidate handles even
    # when the caller does not have explicit social URLs on file.
    linkedin_posts = scrape_linkedin_posts(name, linkedin_url, domain=domain)
    twitter_posts = scrape_twitter_posts(name, twitter_url, domain=domain)
    all_jobs = fetch_apollo_job_postings(domain)

    # Surface the resolved company handle (if any) so downstream UI can
    # label the section "Latest from @<handle>" instead of a generic
    # "Twitter activity", which made misattribution harder to spot.
    resolved_twitter_handles = sorted({
        p.get("matched_handle") for p in twitter_posts if p.get("matched_handle")
    })

    analysis = analyze_signals_with_ai(name, linkedin_posts, twitter_posts, all_jobs)

    payload: dict[str, Any] = {
        "domain": domain,
        "company_name": name,
        "linkedin_posts": linkedin_posts,
        "twitter_posts": twitter_posts,
        "twitter_handles": resolved_twitter_handles,
        "job_postings": all_jobs,
        "job_count": len(all_jobs),
        "linkedin_post_count": len(linkedin_posts),
        "twitter_post_count": len(twitter_posts),
        "hiring_active": len(all_jobs) > 0,
        "social_active": len(linkedin_posts) + len(twitter_posts) > 0,
        "analysis": analysis,
    }
    if domain_key:
        _cache_save(domain_key, payload, li_key, tw_key)
    return payload
