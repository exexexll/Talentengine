"""Centralized LLM configuration — one place to update model names + date.

Every AI-touching service (chat co-pilot, WorkTrigger hypothesis/draft
generation, AI research, query normalizer, social-signal analyzer) pulls
its default model + a grounding preamble from here.  This guarantees:

  1. A single source of truth for "which model powers Figwork today"
     (default: gpt-5.4, see https://developers.openai.com/api/docs/models/gpt-5.4).
  2. Every prompt is stamped with the real current date so the model
     never reasons about "recent news" under the wrong year.
  3. Switching the whole platform to a new model is a one-env-var change
     (``OPENAI_MODEL``) rather than a sweep through six service files.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone


# GPT-5.4 is OpenAI's current frontier model (Aug 2025 knowledge cutoff,
# 1.05M context).  Use gpt-5.4-mini for cheap, high-volume tasks.
DEFAULT_MODEL = "gpt-5.4"
DEFAULT_CHEAP_MODEL = "gpt-5.4-mini"


def primary_model() -> str:
    """Return the default model for high-quality reasoning tasks.

    Overridable via ``OPENAI_MODEL`` env var.  Every consumer of this
    helper picks up a new default the moment the env var changes.
    """
    return os.getenv("OPENAI_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL


def cheap_model() -> str:
    """Model used for fast/high-volume tasks — query normalization,
    fallback classification, structured extraction at bulk."""
    return (
        os.getenv("SEARCH_LLM_MODEL")
        or os.getenv("WORKTRIGGER_OPENAI_MODEL")
        or DEFAULT_CHEAP_MODEL
    ).strip() or DEFAULT_CHEAP_MODEL


def current_date_str() -> str:
    """Today's date in a form the model can quote back to the user."""
    return datetime.now(timezone.utc).strftime("%B %d, %Y")


def current_year() -> int:
    return datetime.now(timezone.utc).year


def grounding_preamble() -> str:
    """Standard system-prompt preamble.  Injects the real date and
    platform context so the model never drifts into thinking it's
    operating in its training-cutoff world."""
    return (
        f"Current date: {current_date_str()}. "
        f"You are operating inside Figwork, an SDR prospecting platform. "
        f"Use the current date when reasoning about 'recent', 'last year', "
        f"or 'this quarter'. "
        f"Style rules for all analysis: be concrete, evidence-led, and specific to "
        f"the provided company/geography context; avoid broad generic wording. "
        f"Do not use vague filler phrases like 'in today's landscape', "
        f"'across industries', 'many companies', or 'likely to'. "
        f"Every key claim must be tied to an explicit observed signal, metric, or source."
    )
