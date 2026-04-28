"""Canonical signal taxonomy — shared across every pipeline.

One module defines what a signal *is*, how to classify an incoming
``signal_type`` string into a canonical category, how to age a signal
(recency decay), and what weight each category carries in the final
signal score.

This replaces three duplicated copies of the same ad-hoc logic that
lived in ``worktrigger_service.py`` (ingested signal classification),
``vendors/social_signals.py`` (fallback post categorization), and
several vendor adapters.  Changing the taxonomy now changes every
pipeline at once instead of requiring synchronized edits.

Categories (SDR PRD §15.2 Signal Score):
    funding, exec_change, hiring, web_intent, buyer_intent, expansion

Additional post-level categories used only for social media analysis:
    pain_point, partnership, product_launch
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


# ---------------------------------------------------------------------------
# Canonical category names.  Keep stable — persisted as `signal_type` in the
# DB, shown in the UI, referenced by weights.
# ---------------------------------------------------------------------------

FUNDING = "funding"
EXEC_CHANGE = "exec_change"
HIRING = "hiring"
WEB_INTENT = "web_intent"
BUYER_INTENT = "buyer_intent"
EXPANSION = "expansion"

# Post-level only (not part of the SDR signal score formula).
PAIN_POINT = "pain_point"
PARTNERSHIP = "partnership"
PRODUCT_LAUNCH = "product_launch"
GROWTH = "growth"  # catch-all for "we're expanding" style posts

SDR_SIGNAL_CATEGORIES: tuple[str, ...] = (
    FUNDING, EXEC_CHANGE, HIRING, WEB_INTENT, BUYER_INTENT, EXPANSION,
)


# ---------------------------------------------------------------------------
# Classification — maps free-text ``signal_type`` strings (from ingestion
# payloads, vendor webhooks, etc.) onto a canonical category.
# Longest match wins, case-insensitive, substring match.
# ---------------------------------------------------------------------------

_TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    FUNDING: ("funding_round", "funding", "fund", "raise", "series_", "seed_round"),
    EXEC_CHANGE: (
        "exec_change", "exec", "leadership", "executive",
        "ceo", "cfo", "cto", "coo",
    ),
    HIRING: (
        "hiring_surge", "hiring", "job_post", "job_opening",
        "job", "requisition", "recruiting",
    ),
    # Keep "web" and "visit" last so longer variants like "web_visit"
    # still win via length-sort; this makes sure any `web_*` or `*visit*`
    # signal type still lands here (matching pre-refactor behavior).
    WEB_INTENT: (
        "web_visit", "web_intent", "website_visit", "pageview",
        "visitor", "web", "visit",
    ),
    BUYER_INTENT: ("buyer_intent", "intent_signal", "buyer", "intent"),
    EXPANSION: (
        "product_launch", "market_entry", "new_office",
        "expansion", "launch",
    ),
}

# Reverse lookup: keyword → category, longest first so that e.g.
# "hiring_surge" wins over "hiring".
_KEYWORD_LOOKUP: list[tuple[str, str]] = sorted(
    ((kw, cat) for cat, kws in _TYPE_KEYWORDS.items() for kw in kws),
    key=lambda x: -len(x[0]),
)


def classify_signal_type(raw_type: str | None) -> str | None:
    """Return the canonical category for a free-text signal_type.

    Returns None if nothing matches — callers should decide whether to
    persist an uncategorized signal or discard it.
    """
    if not raw_type:
        return None
    needle = str(raw_type).strip().lower()
    if not needle:
        return None
    for keyword, category in _KEYWORD_LOOKUP:
        if keyword in needle:
            return category
    return None


# ---------------------------------------------------------------------------
# SDR signal-score aggregation weights (PRD §15.2).
#   SIGNAL_SCORE = 0.25*funding + 0.20*exec_change + 0.20*hiring
#                + 0.20*web_intent + 0.10*buyer_intent + 0.05*expansion
# Weights must sum to 1.0 exactly.
# ---------------------------------------------------------------------------

CATEGORY_WEIGHTS: dict[str, float] = {
    FUNDING: 0.25,
    EXEC_CHANGE: 0.20,
    HIRING: 0.20,
    WEB_INTENT: 0.20,
    BUYER_INTENT: 0.10,
    EXPANSION: 0.05,
}
assert abs(sum(CATEGORY_WEIGHTS.values()) - 1.0) < 1e-9, "CATEGORY_WEIGHTS must sum to 1.0"


# ---------------------------------------------------------------------------
# Recency policy — signals lose value linearly as they age, but never drop
# below a floor so old-but-strong signals still contribute.
# ---------------------------------------------------------------------------

RECENCY_DECAY_WINDOW_DAYS = 120.0
RECENCY_FLOOR = 0.25

DEFAULT_SIGNAL_CONFIDENCE = 0.7


def recency_factor(age_days: float) -> float:
    """Piecewise-linear decay from 1.0 (fresh) to RECENCY_FLOOR at window end."""
    if age_days <= 0:
        return 1.0
    decayed = 1.0 - (age_days / RECENCY_DECAY_WINDOW_DAYS)
    return max(RECENCY_FLOOR, decayed)


# ---------------------------------------------------------------------------
# Post-level categorization for social scraping (LinkedIn / Twitter /
# news snippets).  Free-text; a single post may match multiple categories.
# Kept in sync with the AI analyzer prompt so AI and fallback path produce
# similar-shaped outputs.
# ---------------------------------------------------------------------------

_POST_KEYWORDS: dict[str, frozenset[str]] = {
    HIRING: frozenset({
        "hiring", "we're hiring", "were hiring", "join our team", "join us",
        "open role", "open position", "careers", "apply now", "recruiting",
    }),
    GROWTH: frozenset({
        "launch", "announcing", "introducing", "we raised", "series a",
        "series b", "series c", "funding", "expand", "new office", "milestone",
    }),
    PRODUCT_LAUNCH: frozenset({
        "launch", "launched", "releasing", "new product", "now available",
        "beta", "ga ", "general availability",
    }),
    PARTNERSHIP: frozenset({
        "partnership", "partner with", "partnering", "integration with",
        "collaboration", "joint venture",
    }),
    PAIN_POINT: frozenset({
        "struggle", "challenge", "bottleneck", "backlog", "slow",
        "outage", "customer complaint", "delay",
    }),
    FUNDING: frozenset({
        "raised $", "closed our series", "seed round", "funding round",
        "valuation of", "led by",
    }),
}


@dataclass(frozen=True)
class PostClassification:
    """Result of categorizing a single social post."""
    categories: tuple[str, ...]
    top_category: str | None


def classify_post(text: str) -> PostClassification:
    """Return the categories a post matches, plus the strongest one.

    "Strongest" is the category with the most keyword matches, falling
    back to dictionary order if tied.
    """
    if not text:
        return PostClassification(categories=(), top_category=None)
    lower = text.lower()
    hits: dict[str, int] = {}
    for category, keywords in _POST_KEYWORDS.items():
        count = sum(1 for kw in keywords if kw in lower)
        if count:
            hits[category] = count
    if not hits:
        return PostClassification(categories=(), top_category=None)
    ordered = sorted(hits.items(), key=lambda kv: (-kv[1], list(_POST_KEYWORDS.keys()).index(kv[0])))
    cats = tuple(c for c, _ in ordered)
    return PostClassification(categories=cats, top_category=cats[0])


# ---------------------------------------------------------------------------
# Public helpers for the SDR pipeline
# ---------------------------------------------------------------------------


def blank_category_scores() -> dict[str, float]:
    """Zero-initialized score dict, one slot per SDR category."""
    return {c: 0.0 for c in SDR_SIGNAL_CATEGORIES}


def weighted_signal_score(category_scores: dict[str, float]) -> float:
    """Collapse per-category scores into a single SDR signal score [0, 100]."""
    total = 0.0
    for category, weight in CATEGORY_WEIGHTS.items():
        total += weight * category_scores.get(category, 0.0)
    return max(0.0, min(100.0, total))


def all_sdr_categories() -> Iterable[str]:
    return SDR_SIGNAL_CATEGORIES
