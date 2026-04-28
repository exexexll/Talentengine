from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any

import httpx
from fastapi import HTTPException

from backend.app.models.worktrigger import (
    AccountScoreResponse,
    ReplyClassifyResponse,
    ReviewRequest,
    ScopingBriefResponse,
    WorkHypothesisResponse,
)
from backend.app.services.analysis_engine import score_for_geography
from backend.app.services.llm_config import cheap_model, grounding_preamble
from backend.app.services.signal_taxonomy import (
    DEFAULT_SIGNAL_CONFIDENCE,
    blank_category_scores,
    classify_signal_type,
    recency_factor,
    weighted_signal_score,
)
from backend.app.services.worktrigger_store import WorkTriggerStore


# ---------------------------------------------------------------------------
# Scoring constants (PRD §15 — Scoring Logic).  All weights are named here
# so readers can trace WHY a weight is 0.20 vs 0.15 rather than hunting
# through function bodies.
# ---------------------------------------------------------------------------

# PRD §15.1 — ICP fit sub-weights (must sum to 1.0)
ICP_WEIGHTS: dict[str, float] = {
    "employee_fit": 0.20,
    "industry_fit": 0.15,
    "stage_fit": 0.20,
    "geography_fit": 0.15,
    "projectability_fit": 0.15,
    "persona_buying_fit": 0.15,
}
assert abs(sum(ICP_WEIGHTS.values()) - 1.0) < 1e-9

# PRD §15.3 — Work-fit sub-weights (must sum to 1.0)
WORK_FIT_WEIGHTS: dict[str, float] = {
    "taskability": 0.22,
    "urgency": 0.22,
    "scope_clarity": 0.16,
    "likely_budget": 0.16,
    "talent_supply": 0.12,
    "implementation_feasibility": 0.12,
}
assert abs(sum(WORK_FIT_WEIGHTS.values()) - 1.0) < 1e-9

# PRD §15.4 — Priority score: 35% ICP + 35% Signal + 30% Work fit
PRIORITY_ICP_WEIGHT = 0.35
PRIORITY_SIGNAL_WEIGHT = 0.35
PRIORITY_WORK_WEIGHT = 0.30

# Threshold above which an account is marked `icp_status = "pass"`.
ICP_PASS_THRESHOLD = 60.0

# ICP input thresholds.
EMPLOYEE_SWEET_SPOT_LO = 20   # employees below here = small; diminishing fit
EMPLOYEE_SWEET_SPOT_HI = 1000  # above here = enterprise; diminishing fit
PROJECT_FUNDING_LO = 1_000_000
PROJECT_FUNDING_HI = 50_000_000

# Heuristic funding-stage fit scores (0-100).
_STAGE_FIT: dict[str, float] = {
    "seed": 55.0,
    "series a": 75.0,
    "series b": 90.0,
    "series c": 80.0,
    "public": 50.0,
}
_STAGE_FIT_DEFAULT = 60.0

# US is the primary Figwork market; international accounts score lower by
# default (updated as expansion markets onboard).
_DOMESTIC_COUNTRIES = {"US", "USA", "UNITED STATES"}
GEOGRAPHY_DOMESTIC_FIT = 70.0
GEOGRAPHY_INTERNATIONAL_FIT = 55.0

# Signal-window for the stack snapshot.
SIGNAL_STACK_WINDOW_DAYS = 120


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _score_from_range(value: float | int | None, *, lo: float, hi: float) -> float:
    """Linear map of ``value`` onto [0, 100] over [lo, hi].  Missing → 50."""
    if value is None:
        return 50.0
    if value <= lo:
        return 0.0
    if value >= hi:
        return 100.0
    return ((float(value) - lo) / (hi - lo)) * 100.0


def _parse_iso_utc(iso: str | None) -> datetime | None:
    """Parse an ISO-8601 string as a UTC-aware datetime, or None."""
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(str(iso))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_iso_to_ts(iso: Any) -> float:
    """ISO timestamp → epoch seconds.  Used as a sort key for picking
    the most-recent draft when collapsing duplicates.  Missing/garbage
    timestamps fall back to 0 so they sort last."""
    dt = _parse_iso_utc(iso if isinstance(iso, str) else None)
    return dt.timestamp() if dt else 0.0


def _model_name() -> str:
    return cheap_model()


def _openai_structured_json(
    prompt: str,
    schema_name: str,
    schema: dict[str, Any],
    *,
    store: WorkTriggerStore | None = None,
    task_name: str = "generic",
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=503, detail="OPENAI_API_KEY is required for WorkTrigger generation.")
    token_budget = int(os.getenv("WORKTRIGGER_LLM_TOKEN_BUDGET", "2000"))
    cache_ttl = int(os.getenv("WORKTRIGGER_LLM_CACHE_TTL_SECONDS", "1800"))
    prompt_hash = sha256(prompt.encode("utf-8")).hexdigest()
    cache_key = f"{task_name}:{schema_name}:{prompt_hash}"
    if store is not None:
        cached = store.get_llm_cache(cache_key)
        if cached is not None:
            store.log_llm_run(
                task_name=task_name,
                model_name=_model_name(),
                prompt_hash=prompt_hash,
                token_budget=token_budget,
                evidence=evidence or {},
                response=cached,
                cached_hit=True,
            )
            return cached
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=_model_name(),
            messages=[
                {"role": "system", "content": f"{grounding_preamble()} Return only schema-valid JSON. Never invent unsupported facts."},
                {"role": "user", "content": prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {"name": schema_name, "schema": schema, "strict": True},
            },
            temperature=0.1,
            max_completion_tokens=token_budget,
        )
        text = resp.choices[0].message.content or "{}"
        parsed = json.loads(text)
        usage = getattr(resp, "usage", None)
        if store is not None:
            store.put_llm_cache(cache_key=cache_key, response=parsed, ttl_seconds=cache_ttl)
            store.log_llm_run(
                task_name=task_name,
                model_name=_model_name(),
                prompt_hash=prompt_hash,
                token_budget=token_budget,
                evidence=evidence or {},
                response=parsed,
                cached_hit=False,
                prompt_tokens=getattr(usage, "prompt_tokens", None),
                completion_tokens=getattr(usage, "completion_tokens", None),
            )
        return parsed
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - network/provider variability
        raise HTTPException(status_code=502, detail=f"OpenAI generation failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Pure scoring helpers — no I/O, fully testable in isolation.
# ---------------------------------------------------------------------------


def _compute_icp_fit(
    account: dict[str, Any],
    contacts: list[dict[str, Any]],
) -> tuple[float, dict[str, float]]:
    """PRD §15.1 — ICP fit as a weighted sum of six sub-scores.

    Returns ``(icp_fit, breakdown)`` where ``breakdown`` is each sub-score
    on 0-100 for the tooltip.
    """
    emp_raw = _score_from_range(
        account.get("employee_count"),
        lo=EMPLOYEE_SWEET_SPOT_LO,
        hi=EMPLOYEE_SWEET_SPOT_HI,
    )
    # Triangular: best fit is the midpoint, not either extreme.
    employee_fit = 100.0 - abs(emp_raw - 50.0)

    industry_fit = 75.0 if (account.get("industry") or "").strip() else 50.0

    stage_key = (account.get("funding_stage") or "").strip().lower()
    stage_fit = _STAGE_FIT.get(stage_key, _STAGE_FIT_DEFAULT)

    country_key = (account.get("country") or "").strip().upper()
    geography_fit = GEOGRAPHY_DOMESTIC_FIT if country_key in _DOMESTIC_COUNTRIES else GEOGRAPHY_INTERNATIONAL_FIT

    projectability_fit = _score_from_range(
        account.get("total_funding"),
        lo=PROJECT_FUNDING_LO,
        hi=PROJECT_FUNDING_HI,
    )

    best_persona = max(
        (float(c.get("confidence_score") or 0.0) for c in contacts),
        default=0.4,
    )
    persona_buying_fit = _clamp(best_persona * 100.0)

    breakdown = {
        "employee_fit": employee_fit,
        "industry_fit": industry_fit,
        "stage_fit": stage_fit,
        "geography_fit": geography_fit,
        "projectability_fit": projectability_fit,
        "persona_buying_fit": persona_buying_fit,
    }
    icp_fit = _clamp(sum(ICP_WEIGHTS[k] * v for k, v in breakdown.items()))
    return icp_fit, breakdown


def _aggregate_signal_scores(
    signals: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, float]:
    """Collapse a list of raw signals into per-category 0-100 scores.

    Each signal contributes ``100 * recency * confidence`` to its category;
    we take the max per category so two funding rounds don't double-count.
    Uncategorizable signals are silently skipped.
    """
    per_category = blank_category_scores()
    for s in signals:
        category = classify_signal_type(s.get("signal_type"))
        if category is None or category not in per_category:
            continue
        occurred = _parse_iso_utc(str(s.get("occurred_at"))) or now
        age_days = max(0.0, (now - occurred).total_seconds() / 86400.0)
        conf = float(s.get("confidence_score") or DEFAULT_SIGNAL_CONFIDENCE)
        value = 100.0 * recency_factor(age_days) * conf
        per_category[category] = max(per_category[category], value)
    return per_category


def _compute_geo_priority(geo_rows: list[dict[str, Any]]) -> float:
    """Weighted average of opportunity scores across attributed geographies.

    Falls back to 50 (neutral) when no geography attribution is available
    or every geography lookup fails.
    """
    if not geo_rows:
        return 50.0
    weighted_sum = 0.0
    total_weight = 0.0
    for row in geo_rows:
        gid = row["geography_id"]
        weight = float(row["weight"])
        try:
            score, _features = score_for_geography(gid, "default-opportunity")
        except Exception:
            continue
        weighted_sum += score.score_value * weight
        total_weight += weight
    return (weighted_sum / total_weight) if total_weight > 0 else 50.0


def _compute_work_fit(
    *,
    signal_score: float,
    stage_fit: float,
    geography_fit: float,
    projectability_fit: float,
    geo_priority: float,
    num_signals: int,
) -> float:
    """PRD §15.3 — Work-fit sub-scores.

    Each sub-score is a small blend of already-computed signals:
        taskability            = avg(signal, geo_priority)
        urgency                = 70/30 mix of signal + stage
        scope_clarity          = boolean — do we have ≥2 distinct signals?
        likely_budget          = 60/40 mix of projectability + stage
        talent_supply          = geo priority as-is
        implementation_feas.   = avg(geography_fit, talent_supply)
    """
    taskability = _clamp(0.5 * signal_score + 0.5 * geo_priority)
    urgency = _clamp(0.7 * signal_score + 0.3 * stage_fit)
    scope_clarity = 70.0 if num_signals >= 2 else 45.0
    likely_budget = _clamp(0.6 * projectability_fit + 0.4 * stage_fit)
    talent_supply = _clamp(geo_priority)
    implementation_feasibility = _clamp(0.5 * geography_fit + 0.5 * talent_supply)

    sub_scores = {
        "taskability": taskability,
        "urgency": urgency,
        "scope_clarity": scope_clarity,
        "likely_budget": likely_budget,
        "talent_supply": talent_supply,
        "implementation_feasibility": implementation_feasibility,
    }
    return _clamp(sum(WORK_FIT_WEIGHTS[k] * v for k, v in sub_scores.items()))


class WorkTriggerService:
    def __init__(self, store: WorkTriggerStore) -> None:
        self.store = store

    def build_grounding_package(
        self, *, task_name: str, account: dict[str, Any], extras: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        facts = {
            "account_domain": account.get("domain"),
            "account_name": account.get("name"),
            "industry": account.get("industry"),
            "funding_stage": account.get("funding_stage"),
            "country": account.get("country"),
        }
        return {
            "task_name": task_name,
            "facts": facts,
            "allowed_claims": [
                "Only claims directly supported by provided facts and signals",
                "No unverifiable performance claims",
            ],
            "disallowed_claims": [
                "No fabricated customer names or metrics",
                "No invented product features",
            ],
            "evidence_ids": [f"fact:{k}" for k, v in facts.items() if v],
            "extras": extras or {},
        }

    # ------------------------------------------------------------------
    # Account scoring  (PRD §15)
    # Pipeline:  signals + account → ICP fit → signal score → geo priority
    #                               → work fit → priority score → ICP gate
    # ------------------------------------------------------------------

    def recompute_account_score(self, account_id: str) -> AccountScoreResponse:
        account = self.store.get_account(account_id)
        signals = self.store.list_account_signals(account_id, limit=100)
        contacts = self.store.list_contacts(account_id)

        now = _utc_now()
        icp_fit, icp_inputs = _compute_icp_fit(account, contacts)
        signal_scores = _aggregate_signal_scores(signals, now=now)
        signal_score = weighted_signal_score(signal_scores)

        geo_rows = self._derive_geo_attribution(signals)
        self.store.replace_geo_attribution(account_id=account_id, rows=geo_rows)
        geo_priority = _compute_geo_priority(geo_rows)

        work_fit = _compute_work_fit(
            signal_score=signal_score,
            stage_fit=icp_inputs["stage_fit"],
            geography_fit=icp_inputs["geography_fit"],
            projectability_fit=icp_inputs["projectability_fit"],
            geo_priority=geo_priority,
            num_signals=len(signals),
        )

        priority = _clamp(
            PRIORITY_ICP_WEIGHT * icp_fit
            + PRIORITY_SIGNAL_WEIGHT * signal_score
            + PRIORITY_WORK_WEIGHT * work_fit
        )
        qualified = priority >= ICP_PASS_THRESHOLD
        self.store.update_account_fields(
            account_id,
            {"icp_status": "pass" if qualified else "fail"},
        )

        # Persist for the UI.  `scores` is the flat dict rendered as bars;
        # `explanation` is the nested breakdown shown in the tooltip.
        scores = {
            f"{category}_score": round(value, 2)
            for category, value in signal_scores.items()
        }
        scores["total_signal_score"] = round(signal_score, 2)

        explanation = {
            "icp_fit": round(icp_fit, 2),
            "signal_score": round(signal_score, 2),
            "work_fit": round(work_fit, 2),
            "priority_score": round(priority, 2),
            "inputs": {k: round(v, 2) for k, v in icp_inputs.items()},
        }
        self.store.save_signal_stack(
            account_id=account_id,
            window_start=(now - timedelta(days=SIGNAL_STACK_WINDOW_DAYS)).isoformat(),
            window_end=now.isoformat(),
            scores=scores,
            explanation=explanation,
        )
        return AccountScoreResponse(
            account_id=account_id,
            qualified=qualified,
            icp_fit_score=round(icp_fit, 2),
            signal_score=round(signal_score, 2),
            work_fit_score=round(work_fit, 2),
            priority_score=round(priority, 2),
            geo_attribution=[
                {
                    "geography_id": r["geography_id"],
                    "weight": round(float(r["weight"]), 4),
                    "evidence": r["evidence"],
                    "confidence_score": round(float(r["confidence_score"]), 4),
                }
                for r in geo_rows
            ],
            rationale=[
                f"ICP fit={icp_fit:.1f}",
                f"Signal score={signal_score:.1f}",
                f"Work fit={work_fit:.1f}",
                f"Geo/talent context={geo_priority:.1f}",
            ],
        )

    def _derive_geo_attribution(self, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        weights: dict[str, float] = {}
        confidences: dict[str, float] = {}
        evidences: dict[str, str] = {}
        for s in signals:
            payload = s.get("normalized_payload") or {}
            base_conf = float(s.get("confidence_score", 0.7))
            geo_single = payload.get("geography_id")
            if isinstance(geo_single, str) and geo_single:
                weights[geo_single] = weights.get(geo_single, 0.0) + base_conf
                confidences[geo_single] = max(confidences.get(geo_single, 0.0), base_conf)
                evidences[geo_single] = "signal_geography"
            geo_list = payload.get("geography_ids")
            if isinstance(geo_list, list):
                valid = [str(g) for g in geo_list if isinstance(g, str) and g]
                if valid:
                    share = base_conf / len(valid)
                    for gid in valid:
                        weights[gid] = weights.get(gid, 0.0) + share
                        confidences[gid] = max(confidences.get(gid, 0.0), base_conf)
                        evidences[gid] = "signal_geography_list"
            locs = payload.get("locations")
            if isinstance(locs, list):
                for loc in locs:
                    if not isinstance(loc, dict):
                        continue
                    gid = str(loc.get("geography_id") or "").strip()
                    if not gid:
                        continue
                    share = float(loc.get("weight") or 0.0)
                    if share <= 0:
                        continue
                    add = base_conf * share
                    weights[gid] = weights.get(gid, 0.0) + add
                    confidences[gid] = max(confidences.get(gid, 0.0), min(1.0, base_conf))
                    evidences[gid] = "account_locations"
        if not weights:
            return []
        total = sum(weights.values())
        ranked = sorted(weights.items(), key=lambda x: x[1], reverse=True)[:8]
        rows: list[dict[str, Any]] = []
        for gid, w in ranked:
            rows.append(
                {
                    "geography_id": gid,
                    "weight": (w / total) if total > 0 else 0.0,
                    "evidence": evidences.get(gid, "unknown"),
                    "confidence_score": confidences.get(gid, 0.5),
                }
            )
        return rows

    def generate_work_hypothesis(self, account_id: str) -> WorkHypothesisResponse:
        account = self.store.get_account(account_id)
        stack = self.store.get_latest_signal_stack(account_id)
        if stack is None:
            raise HTTPException(status_code=400, detail="Score the account before generating work hypothesis.")

        name = account.get("name") or account.get("domain", "")
        domain = account.get("domain") or ""
        industry = account.get("industry") or "unknown"
        employees = account.get("employee_count") or "unknown"
        funding = account.get("funding_stage") or "unknown"
        total_funding = account.get("total_funding")
        country = account.get("country") or "unknown"

        signals = self.store.list_account_signals(account_id, limit=10)
        signal_lines = [f"- {s.get('signal_type')} from {s.get('source')} ({s.get('occurred_at', '')[:10]})" for s in signals[:6]]

        # Scoring context
        exp = (stack.get("explanation") or {}) if isinstance(stack.get("explanation"), dict) else {}
        score_context = (
            f"ICP fit: {exp.get('icp_fit', 0):.0f}/100, "
            f"Signal score: {exp.get('signal_score', 0):.0f}/100, "
            f"Work fit: {exp.get('work_fit', 0):.0f}/100, "
            f"Priority: {exp.get('priority_score', 0):.0f}/100"
        )

        # Social signals (LinkedIn posts, Twitter, job postings, AI analysis)
        social_context = ""
        try:
            from backend.app.services.vendors.social_signals import get_company_social_signals

            social = get_company_social_signals(
                domain,
                company_name=name,
                linkedin_url=str(account.get("linkedin_url") or ""),
                twitter_url=str(account.get("twitter_url") or ""),
            )
            analysis = social.get("analysis") or {}

            if analysis.get("summary"):
                social_context += f"\n\nSOCIAL SIGNAL ANALYSIS:\n{analysis['summary']}\n"
            if analysis.get("hiring_intensity"):
                social_context += f"Hiring intensity: {analysis['hiring_intensity']}\n"
            if analysis.get("momentum"):
                social_context += f"Company momentum: {analysis['momentum']}\n"
            hiring_depts = analysis.get("hiring_departments") or []
            if hiring_depts:
                social_context += f"Departments hiring: {', '.join(hiring_depts)}\n"
            growth_sigs = analysis.get("growth_signals") or []
            if growth_sigs:
                social_context += f"Growth signals: {'; '.join(growth_sigs)}\n"
            pain_pts = analysis.get("pain_points") or []
            if pain_pts:
                social_context += f"Pain points detected: {'; '.join(pain_pts)}\n"
            if analysis.get("outreach_angle"):
                social_context += f"Suggested outreach angle: {analysis['outreach_angle']}\n"

            ai_signals = analysis.get("signals") or []
            if ai_signals:
                social_context += "\nDetected business signals:\n"
                for s in ai_signals[:5]:
                    social_context += f"- [{s.get('type')}] {s.get('description')} (confidence: {s.get('confidence', 0):.0%})\n"

            job_postings = social.get("job_postings") or []
            if job_postings:
                social_context += f"\nActive job postings ({len(job_postings)}):\n"
                for j in job_postings[:8]:
                    social_context += f"- {j.get('title', '')}\n"

            li_posts = social.get("linkedin_posts") or []
            if li_posts:
                social_context += f"\nRecent LinkedIn posts ({len(li_posts)}):\n"
                for p in li_posts[:4]:
                    social_context += f"- {p.get('title', '')}: {p.get('text', '')[:100]}\n"

            tw_posts = social.get("twitter_posts") or []
            if tw_posts:
                social_context += f"\nRecent Twitter/X posts ({len(tw_posts)}):\n"
                for p in tw_posts[:3]:
                    social_context += f"- {p.get('title', '')}: {p.get('text', '')[:100]}\n"
        except Exception as exc:
            print(f"[WorkHypothesis] Social signal fetch failed: {exc}")

        prompt = (
            f"You are a senior business analyst at Figwork, a staffing and project-based services platform "
            f"that connects companies with specialized freelancers and contractors.\n\n"
            f"=== COMPANY PROFILE ===\n"
            f"Name: {name}\n"
            f"Domain: {domain}\n"
            f"Industry: {industry}\n"
            f"Employees: {employees}\n"
            f"Funding: {funding}" + (f" (${total_funding:,.0f} raised)" if total_funding else "") + f"\n"
            f"Country: {country}\n"
            f"Scoring: {score_context}\n\n"
            f"=== INGESTED SIGNALS ===\n"
            + ("\n".join(signal_lines) if signal_lines else "No signals yet") + "\n"
            f"{social_context}\n"
            f"=== INSTRUCTIONS ===\n"
            f"Using ALL the data above — company profile, scoring, social signals, job postings, "
            f"LinkedIn/Twitter activity, growth signals, and pain points — generate a highly specific "
            f"work hypothesis for what Figwork could help this company with.\n\n"
            f"Be SPECIFIC to {name}'s actual situation. Reference real signals from the data above. "
            f"Do NOT be generic. If they are hiring aggressively in engineering, say so. "
            f"If they have a pain point around compliance, address it.\n\n"
            f"- probable_problem: 2-3 sentences about their specific operational challenge, referencing signals\n"
            f"- probable_deliverable: A concrete project scope Figwork could staff (1-2 sentences)\n"
            f"- talent_archetype: Specific role titles + specializations needed (not generic)\n"
            f"- rationale: 3-5 bullet points explaining WHY this hypothesis, citing specific evidence\n"
            f"Return strict JSON."
        )
        grounding = self.build_grounding_package(
            task_name="work_hypothesis",
            account=account,
            extras={"signal_stack_id": stack["id"]},
        )
        schema = {
            "type": "object",
            "required": [
                "probable_problem",
                "probable_deliverable",
                "talent_archetype",
                "urgency_score",
                "taskability_score",
                "fit_score",
                "confidence_score",
                "rationale",
            ],
            "properties": {
                "probable_problem": {"type": "string"},
                "probable_deliverable": {"type": "string"},
                "talent_archetype": {"type": "string"},
                "urgency_score": {"type": "number"},
                "taskability_score": {"type": "number"},
                "fit_score": {"type": "number"},
                "confidence_score": {"type": "number"},
                "rationale": {"type": "array", "items": {"type": "string"}},
            },
            "additionalProperties": False,
        }
        obj = _openai_structured_json(
            prompt,
            "work_hypothesis",
            schema,
            store=self.store,
            task_name="work_hypothesis",
            evidence=grounding,
        )
        hid = self.store.save_work_hypothesis(
            account_id=account_id,
            signal_stack_id=stack["id"],
            probable_problem=obj["probable_problem"],
            probable_deliverable=obj["probable_deliverable"],
            talent_archetype=obj["talent_archetype"],
            urgency_score=float(_clamp(float(obj["urgency_score"]))),
            taskability_score=float(_clamp(float(obj["taskability_score"]))),
            fit_score=float(_clamp(float(obj["fit_score"]))),
            confidence_score=float(max(0.0, min(1.0, float(obj["confidence_score"])))),
            rationale=list(obj["rationale"]),
            generated_by_model=_model_name(),
            model_version="worktrigger-v1",
        )
        saved = self.store.get_work_hypothesis(hid)
        return WorkHypothesisResponse(
            work_hypothesis_id=saved["id"],
            account_id=saved["account_id"],
            probable_problem=saved["probable_problem"],
            probable_deliverable=saved["probable_deliverable"],
            talent_archetype=saved["talent_archetype"],
            urgency_score=float(saved["urgency_score"]),
            taskability_score=float(saved["taskability_score"]),
            fit_score=float(saved["fit_score"]),
            confidence_score=float(saved["confidence_score"]),
            rationale=saved["rationale"],
            created_at=datetime.fromisoformat(saved["created_at"]),
        )

    def _pick_target_job(
        self,
        domain: str,
        name: str,
        archetype: str,
        *,
        linkedin_url: str = "",
        twitter_url: str = "",
    ) -> dict[str, str] | None:
        """Pick the most relevant job posting to anchor a job-reference draft.

        Resolution order (cheapest → most expensive):
          1. **Disk-cached social signals** (free) — same cache key as
             ``get_company_social_signals`` (domain + optional LinkedIn /
             Twitter URLs).  If it has any ``job_postings`` we're done.
          2. **Direct Apollo job-postings call** (~2 Apollo credits, no
             SerpAPI / no LLM) — used at intake time when the social-signal
             cache is cold.  We deliberately avoid calling
             ``get_company_social_signals`` here: that path also runs
             SerpAPI scraping + an OpenAI analysis call which would burn
             real money on every intake (and on bulk intake of 1000
             companies it adds up fast).  Apollo job postings are flat
             plan pricing, so this stays cheap.

        Match preference within whichever pool we end up with:
          1. A title containing any keyword from the work-hypothesis
             talent archetype (e.g. archetype="data engineer" → match
             "Data Engineer" / "Senior Data Engineer").
          2. Otherwise the most recently posted job.

        Returns ``{"title", "url", "posted_at"}`` or ``None`` if no
        jobs are available from any source.  ``None`` causes
        ``generate_draft`` to fall back to ``outreach_mode="default"``.
        """
        if not domain:
            return None

        jobs: list[dict[str, Any]] = []

        # Step 1 — disk cache (free).  Must use the same cache key shape as
        # ``get_company_social_signals`` (domain + optional social URLs) so
        # we never read another enrichment's stale ``job_postings`` list.
        try:
            from backend.app.services.vendors.social_signals import _cache_load

            cached = _cache_load(domain, linkedin_url, twitter_url)
            if cached:
                jobs = list(cached.get("job_postings") or [])
        except Exception:
            jobs = []

        # Step 2 — lightweight Apollo-only fetch.  Direct call avoids
        # `get_company_social_signals` which would also scrape SerpAPI
        # (~2 paid searches) and run an LLM analysis pass.  This keeps
        # intake-time draft generation cheap and predictable.
        if not jobs:
            try:
                from backend.app.services.vendors.social_signals import fetch_apollo_job_postings
                jobs = fetch_apollo_job_postings(domain) or []
            except Exception:
                return None

        if not jobs:
            return None

        # Archetype-keyword match first (case-insensitive substring on title).
        # We only match words longer than 2 chars to avoid spurious hits
        # on filler tokens like "of"/"in".
        if archetype:
            archetype_words = {w.strip().lower() for w in archetype.split() if len(w) > 2}
            for j in jobs:
                title_lc = str(j.get("title") or "").lower()
                if archetype_words and any(w in title_lc for w in archetype_words):
                    return {
                        "title": str(j.get("title") or ""),
                        "url": str(j.get("url") or ""),
                        "posted_at": str(j.get("posted_at") or ""),
                    }
        # Fallback: newest job posting (Apollo returns them recency-sorted).
        first = jobs[0]
        return {
            "title": str(first.get("title") or ""),
            "url": str(first.get("url") or ""),
            "posted_at": str(first.get("posted_at") or ""),
        }

    def generate_draft(
        self,
        *,
        account_id: str,
        contact_id: str,
        work_hypothesis_id: str,
        channel: str,
        outreach_mode: str | None = None,
    ) -> str:
        """Generate a single outreach draft for one (account, contact, hypothesis).

        ``outreach_mode`` controls how the prompt is shaped:
          * ``None`` (default) — read the toggle from the account row;
            ``job_listing`` if the account has the per-account toggle on
            and at least one cached open job; otherwise ``default``.
          * ``"default"`` — original problem/deliverable angle.
          * ``"job_listing"`` — pitch fills a specific currently-open
            role.  Falls back to ``"default"`` automatically when no job
            postings are available so the toggle never silently breaks
            draft generation.
        """
        account = self.store.get_account(account_id)
        contact = self.store.get_contact(contact_id)
        hypothesis = self.store.get_work_hypothesis(work_hypothesis_id)

        contact_name = contact.get("full_name") or "there"
        contact_first = contact_name.split()[0] if contact_name != "there" else "there"
        contact_title = contact.get("title") or ""
        company_name = account.get("name") or account.get("domain", "")
        domain = account.get("domain") or ""
        industry = account.get("industry") or ""
        funding = account.get("funding_stage") or ""
        employees = account.get("employee_count") or ""
        problem = hypothesis.get("probable_problem", "")
        deliverable = hypothesis.get("probable_deliverable", "")
        archetype = hypothesis.get("talent_archetype", "")

        # Resolve effective outreach mode (toggle-driven if not forced).
        if outreach_mode is None:
            outreach_mode = "job_listing" if account.get("job_outreach_enabled") else "default"

        target_job: dict[str, str] | None = None
        if outreach_mode == "job_listing":
            target_job = self._pick_target_job(
                domain,
                company_name,
                archetype,
                linkedin_url=str(account.get("linkedin_url") or ""),
                twitter_url=str(account.get("twitter_url") or ""),
            )
            if target_job is None or not target_job.get("title"):
                # No usable jobs cached → cleanly fall back to default
                # angle so the toggle never produces a broken draft.
                outreach_mode = "default"
                target_job = None

        if outreach_mode == "job_listing" and target_job:
            job_title = target_job["title"]
            job_url = target_job.get("url") or ""
            mode_block = (
                f"OUTREACH MODE: job_listing\n"
                f"- {company_name} is publicly hiring for: \"{job_title}\"\n"
                + (f"- Posting URL: {job_url}\n" if job_url else "")
                + f"- Position the message as: Figwork can help fill THIS specific role with vetted, on-demand talent.\n"
                f"- Reference the role title verbatim in the body — it's the proof of personalization.\n"
                f"- Do NOT name internal recruiters or claim insider knowledge of the search.\n"
                f"- The CTA should be a 15-minute call to discuss filling the {job_title} role on a fractional or trial basis.\n"
            )
        else:
            mode_block = (
                "OUTREACH MODE: default\n"
                "- Lead with the inferred problem/deliverable from the work hypothesis.\n"
            )

        prompt = (
            f"Write a personalized cold outreach email from a Figwork sales rep.\n\n"
            f"{mode_block}\n"
            f"RECIPIENT:\n"
            f"- Name: {contact_name}\n"
            f"- Title: {contact_title}\n"
            f"- Company: {company_name}\n"
            f"- Industry: {industry}\n"
            f"- Employees: {employees}\n"
            f"- Funding: {funding}\n\n"
            f"WORK HYPOTHESIS:\n"
            f"- Problem: {problem}\n"
            f"- Deliverable: {deliverable}\n"
            f"- Talent needed: {archetype}\n\n"
            f"RULES:\n"
            f"- Address the recipient as 'Hi {contact_first}'\n"
            f"- Mention ONE specific trigger about {company_name} (the open role if mode=job_listing; otherwise funding, growth, industry trend)\n"
            f"- Propose ONE concrete deliverable, not vague 'let's chat'\n"
            f"- Keep email_body under 150 words\n"
            f"- Keep followup_body under 80 words\n"
            f"- linkedin_dm should be under 50 words\n"
            f"- Sign off as the Figwork team (not '[Your Name]')\n"
            f"- No fake compliments. No 'I came across your profile'. Direct and specific.\n"
            f"- subject_a and subject_b should be different A/B test variants\n"
        )
        grounding = self.build_grounding_package(
            task_name="draft_generation",
            account=account,
            extras={
                "contact_id": contact_id,
                "work_hypothesis_id": work_hypothesis_id,
                "outreach_mode": outreach_mode,
                "target_job_title": (target_job or {}).get("title"),
            },
        )
        schema = {
            "type": "object",
            "required": ["subject_a", "subject_b", "email_body", "followup_body", "linkedin_dm"],
            "properties": {
                "subject_a": {"type": "string"},
                "subject_b": {"type": "string"},
                "email_body": {"type": "string"},
                "followup_body": {"type": "string"},
                "linkedin_dm": {"type": "string"},
            },
            "additionalProperties": False,
        }
        obj = _openai_structured_json(
            prompt,
            "outreach_draft",
            schema,
            store=self.store,
            task_name="draft_generation",
            evidence=grounding,
        )
        return self.store.save_draft(
            account_id=account_id,
            contact_id=contact_id,
            work_hypothesis_id=work_hypothesis_id,
            channel=channel,
            subject_a=obj["subject_a"],
            subject_b=obj["subject_b"],
            email_body=obj["email_body"],
            followup_body=obj["followup_body"],
            linkedin_dm=obj["linkedin_dm"],
            metadata={
                "model": _model_name(),
                "version": "worktrigger-v1",
                "outreach_mode": outreach_mode,
                "target_job_title": (target_job or {}).get("title"),
            },
            outreach_mode=outreach_mode,
            target_job_title=(target_job or {}).get("title"),
            target_job_url=(target_job or {}).get("url"),
        )

    def collapse_duplicate_drafts(self, account_id: str | None = None) -> dict[str, Any]:
        """Archive duplicate active drafts so each (account, contact,
        hypothesis, channel) tuple has exactly ONE non-archived draft.

        Picks the winner by status priority (draft_ready > approved >
        sent > replied) then by most-recent ``updated_at``.  Costs zero
        LLM/vendor credits — purely a database cleanup.

        ``account_id`` scoping is optional; pass it to clean a single
        account, or omit to sweep the whole pipeline.  The same logic
        powers the per-account 'collapse duplicates' button in the UI
        and the global 'fix legacy data' admin endpoint.
        """
        # status priority: lower number = keeps the spot
        priority = {"draft_ready": 0, "approved": 1, "sent": 2, "replied": 3, "snoozed": 4}
        active_statuses = tuple(priority.keys())

        if account_id is not None:
            drafts = self.store.list_drafts_for_account(account_id, statuses=active_statuses)
        else:
            drafts = [
                d for d in self.store.list_drafts(status=None, limit=5000)
                if d.get("status") in priority
            ]

        # Grouping key intentionally OMITS work_hypothesis_id: SDRs care
        # about "one email per contact per channel", regardless of which
        # internal hypothesis spawned it.  Different hypotheses for the
        # same (account, contact) used to leak past the dedupe logic.
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for d in drafts:
            key = (
                str(d["account_id"]),
                str(d["contact_id"]),
                str(d["channel"] or "email"),
            )
            groups.setdefault(key, []).append(d)

        archived: list[str] = []
        kept: list[str] = []
        now = _utc_now().isoformat()
        for group_drafts in groups.values():
            if len(group_drafts) <= 1:
                if group_drafts:
                    kept.append(group_drafts[0]["id"])
                continue
            ranked = sorted(
                group_drafts,
                key=lambda d: (
                    priority.get(str(d.get("status") or ""), 99),
                    -(_parse_iso_to_ts(d.get("updated_at"))),
                ),
            )
            keeper = ranked[0]
            kept.append(keeper["id"])
            for d in ranked[1:]:
                try:
                    self.store.update_draft(d["id"], status="discarded", updated_at=now)
                    archived.append(d["id"])
                except Exception:
                    pass
        return {"archived_count": len(archived), "kept_count": len(kept)}

    def regenerate_drafts_for_account(self, account_id: str) -> dict[str, Any]:
        """Re-generate every active draft for an account using the
        current toggle-driven outreach mode.

        Behavior:
          1. Group all active drafts by (contact_id, work_hypothesis_id, channel).
          2. Generate ONE replacement per group with the new outreach mode.
          3. Archive **every** prior active draft in the group (not just
             the most recent), so the queue truly shows one active draft
             per contact.  This catches legacy duplicates that may have
             accumulated before the dedupe fix landed.
        """
        active_statuses = ("draft_ready", "approved")
        drafts = self.store.list_drafts_for_account(account_id, statuses=active_statuses)
        # Group by (contact, channel) — collapses across multiple hypotheses
        # for the same contact, which used to leak through.
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for d in drafts:
            key = (str(d["contact_id"]), str(d["channel"] or "email"))
            groups.setdefault(key, []).append(d)

        regenerated: list[str] = []
        archived: list[str] = []
        skipped: list[dict[str, str]] = []
        for (contact_id, channel), group_drafts in groups.items():
            # Pick the most-recent draft's hypothesis as the seed for
            # the regenerated draft (it had the latest signal context).
            seed = sorted(
                group_drafts,
                key=lambda d: -_parse_iso_to_ts(d.get("updated_at")),
            )[0]
            hypothesis_id = str(seed["work_hypothesis_id"])
            try:
                new_id = self.generate_draft(
                    account_id=account_id,
                    contact_id=contact_id,
                    work_hypothesis_id=hypothesis_id,
                    channel=channel,
                )
                regenerated.append(new_id)
            except Exception as exc:
                skipped.append({
                    "contact_id": contact_id,
                    "reason": str(exc),
                    "kept_drafts": str(len(group_drafts)),
                })
                continue
            # Archive every prior active draft in this group — including
            # legacy duplicates the previous regenerate logic missed.
            now = _utc_now().isoformat()
            for d in group_drafts:
                try:
                    self.store.update_draft(d["id"], status="discarded", updated_at=now)
                    archived.append(d["id"])
                except Exception:
                    pass
        return {
            "regenerated_count": len(regenerated),
            "regenerated": regenerated,
            "archived_count": len(archived),
            "skipped": skipped,
        }

    def apply_review(self, draft_id: str, review: ReviewRequest) -> None:
        draft = self.store.get_draft(draft_id)
        action_status_map = {
            "approve": "approved",
            "edit_and_approve": "approved",
            "discard": "discarded",
            "snooze": "snoozed",
            "reroute_contact": "draft_ready",
            "reroute_angle": "draft_ready",
        }
        status = action_status_map.get(review.action, "draft_ready")
        updates: dict[str, Any] = {"status": status, "updated_at": _utc_now().isoformat()}
        if review.action == "edit_and_approve":
            if review.edited_subject:
                updates["subject_a"] = review.edited_subject
            if review.edited_body:
                updates["email_body"] = review.edited_body
        self.store.update_draft(draft["id"], **updates)
        self.store.add_review_decision(
            draft_id=draft_id,
            reviewer_user_id=review.reviewer_user_id,
            action=review.action,
            edited_subject=review.edited_subject,
            edited_body=review.edited_body,
            reason_code=review.reason_code,
            notes=review.notes,
        )

    def send_approved_draft(self, draft_id: str) -> str:
        draft = self.store.get_draft(draft_id)
        if draft["status"] != "approved":
            raise HTTPException(status_code=409, detail="Only approved drafts can be sent.")

        channel = (draft.get("channel") or "email").strip().lower()
        if channel == "linkedin":
            self.store.update_draft(draft_id, status="sent", updated_at=_utc_now().isoformat())
            return f"linkedin_manual_{draft_id}"

        api_key = os.getenv("RESEND_API_KEY", "").strip()
        from_email = os.getenv("WORKTRIGGER_FROM_EMAIL", "").strip()
        if not api_key or not from_email:
            raise HTTPException(
                status_code=503,
                detail="RESEND_API_KEY and WORKTRIGGER_FROM_EMAIL are required for sending.",
            )
        contact = self.store.get_contact(draft["contact_id"])
        to_email = (contact.get("email") or "").strip()
        if not to_email:
            raise HTTPException(status_code=400, detail="Contact has no email.")
        if self.store.is_suppressed(to_email):
            raise HTTPException(status_code=409, detail="Recipient is suppressed.")
        consent = self.store.get_consent(email=to_email, channel="email")
        if consent is not None and str(consent.get("status", "")).lower() not in {"granted", "double_opt_in"}:
            raise HTTPException(status_code=409, detail="Recipient consent does not permit sending.")
        account = self.store.get_account(draft["account_id"])
        domain = str(account.get("domain") or "")
        cap = int(os.getenv("WORKTRIGGER_DAILY_SEND_CAP_PER_DOMAIN", "50"))
        sent_today = self.store.count_sent_by_domain_since(domain, (_utc_now() - timedelta(days=1)).isoformat())
        if sent_today >= cap:
            raise HTTPException(
                status_code=429,
                detail=f"Daily send cap exceeded for domain={domain} (cap={cap}).",
            )
        try:
            with httpx.Client(timeout=20) as client:
                resp = client.post(
                    "https://api.resend.com/emails",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={
                        "from": from_email,
                        "to": [to_email],
                        "subject": draft.get("subject_a") or "Quick thought",
                        "text": draft.get("email_body") or "",
                    },
                )
            if resp.status_code >= 300:
                raise HTTPException(status_code=502, detail=f"Resend send failed: {resp.text}")
            payload = resp.json()
            message_id = payload.get("id")
            if not message_id:
                raise HTTPException(status_code=502, detail="Resend response missing message id.")
            self.store.update_draft(draft_id, status="sent", updated_at=_utc_now().isoformat())
            return str(message_id)
        except HTTPException:
            raise
        except Exception as exc:  # pragma: no cover
            raise HTTPException(status_code=502, detail=f"Send failed: {exc}") from exc

    def sync_opportunity_to_hubspot(self, draft_id: str) -> str:
        draft = self.store.get_draft(draft_id)
        token = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN", "").strip()
        if not token:
            raise HTTPException(status_code=503, detail="HUBSPOT_PRIVATE_APP_TOKEN is required for CRM sync.")

        existing_opp = self.store.get_opportunity_by_draft(draft_id)
        if existing_opp and existing_opp.get("crm_id"):
            return str(existing_opp["crm_id"])

        contact = self.store.get_contact(draft["contact_id"])
        account = self.store.get_account(draft["account_id"])
        opportunity_id = self.store.create_or_update_opportunity(
            account_id=draft["account_id"],
            contact_id=draft["contact_id"],
            source_draft_id=draft_id,
            stage="new",
        )
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        try:
            with httpx.Client(timeout=20) as client:
                # Company upsert by domain
                search_company = client.post(
                "https://api.hubapi.com/crm/v3/objects/companies/search",
                headers=headers,
                json={
                    "filterGroups": [{"filters": [{"propertyName": "domain", "operator": "EQ", "value": account["domain"]}]}],
                    "properties": ["domain", "name"],
                    "limit": 1,
                },
            )
                if search_company.status_code >= 300:
                    raise HTTPException(status_code=502, detail=f"HubSpot company search failed: {search_company.text}")
                comp_results = search_company.json().get("results", [])
                if comp_results:
                    company_id = comp_results[0]["id"]
                else:
                    create_company = client.post(
                    "https://api.hubapi.com/crm/v3/objects/companies",
                    headers=headers,
                    json={"properties": {"domain": account["domain"], "name": account.get("name") or account["domain"]}},
                )
                    if create_company.status_code >= 300:
                        raise HTTPException(status_code=502, detail=f"HubSpot company create failed: {create_company.text}")
                    company_id = create_company.json()["id"]
                # Contact upsert by email if available
                crm_contact_id = ""
                email = (contact.get("email") or "").strip()
                if email:
                    search_contact = client.post(
                    "https://api.hubapi.com/crm/v3/objects/contacts/search",
                    headers=headers,
                    json={
                        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
                        "properties": ["email", "firstname", "lastname"],
                        "limit": 1,
                    },
                )
                    if search_contact.status_code >= 300:
                        raise HTTPException(status_code=502, detail=f"HubSpot contact search failed: {search_contact.text}")
                    c_results = search_contact.json().get("results", [])
                    if c_results:
                        crm_contact_id = c_results[0]["id"]
                    else:
                        first, _, last = (contact.get("full_name") or "Unknown Contact").partition(" ")
                        create_contact = client.post(
                        "https://api.hubapi.com/crm/v3/objects/contacts",
                        headers=headers,
                        json={"properties": {"email": email, "firstname": first, "lastname": last}},
                    )
                        if create_contact.status_code >= 300:
                            raise HTTPException(status_code=502, detail=f"HubSpot contact create failed: {create_contact.text}")
                        crm_contact_id = create_contact.json()["id"]
                # Create deal
                create_deal = client.post(
                "https://api.hubapi.com/crm/v3/objects/deals",
                headers=headers,
                json={
                    "properties": {
                        "dealname": f"Figwork - {account.get('name') or account['domain']}",
                        "pipeline": "default",
                        "dealstage": "appointmentscheduled",
                        "figwork_trigger_type": "worktrigger",
                        "figwork_draft_status": draft["status"],
                    }
                },
            )
                if create_deal.status_code >= 300:
                    raise HTTPException(status_code=502, detail=f"HubSpot deal create failed: {create_deal.text}")
                deal_id = create_deal.json()["id"]
                # Associate deal-company
                client.put(
                f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/companies/{company_id}/deal_to_company",
                headers=headers,
            )
                if crm_contact_id:
                    client.put(
                    f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/contacts/{crm_contact_id}/deal_to_contact",
                    headers=headers,
                )
            opp_id = self.store.create_or_update_opportunity(
                account_id=draft["account_id"],
                contact_id=draft["contact_id"],
                source_draft_id=draft_id,
                stage="meeting_booked",
                crm_id=deal_id,
            )
            self.store.log_crm_sync_event(
                account_id=draft["account_id"],
                contact_id=draft["contact_id"],
                opportunity_id=opp_id,
                direction="app_to_hubspot",
                status="success",
                details={"deal_id": deal_id, "draft_id": draft_id},
            )
            return str(deal_id)
        except HTTPException as exc:
            self.store.log_crm_sync_event(
                account_id=draft["account_id"],
                contact_id=draft["contact_id"],
                opportunity_id=opportunity_id,
                direction="app_to_hubspot",
                status="error",
                details={"draft_id": draft_id, "error": exc.detail},
            )
            raise

    def classify_reply(self, draft_id: str, reply_text: str, thread_metadata: dict[str, Any]) -> ReplyClassifyResponse:
        self.store.get_draft(draft_id)
        prompt = (
            "Classify this inbound reply for sales handling.\n"
            f"Draft ID: {draft_id}\n"
            f"Thread metadata: {json.dumps(thread_metadata, default=str)}\n"
            f"Reply: {reply_text}\n"
        )
        schema = {
            "type": "object",
            "required": ["classification", "confidence", "next_action"],
            "properties": {
                "classification": {
                    "type": "string",
                    "enum": ["positive_interest", "objection", "referral", "not_now", "unsubscribe", "irrelevant"],
                },
                "confidence": {"type": "number"},
                "next_action": {"type": "string"},
            },
            "additionalProperties": False,
        }
        obj = _openai_structured_json(
            prompt,
            "reply_classification",
            schema,
            store=self.store,
            task_name="reply_classification",
            evidence={"draft_id": draft_id, "thread_metadata": thread_metadata},
        )
        classification = obj["classification"]
        if classification == "positive_interest":
            draft = self.store.get_draft(draft_id)
            self.store.create_or_update_opportunity(
                account_id=draft["account_id"],
                contact_id=draft["contact_id"],
                source_draft_id=draft_id,
                stage="discovery_done",
                positive_reply_at=_utc_now().isoformat(),
            )
            self.store.update_draft(draft_id, status="replied", updated_at=_utc_now().isoformat())
        if classification == "unsubscribe":
            draft = self.store.get_draft(draft_id)
            contact = self.store.get_contact(draft["contact_id"])
            email = (contact.get("email") or "").strip().lower()
            if email:
                self.store.add_suppression(email=email, reason="unsubscribe", source="reply_classifier")
                self.store.upsert_consent(
                    email=email,
                    channel="email",
                    legal_basis="withdrawn",
                    status="revoked",
                    source="reply_classifier",
                    metadata={"draft_id": draft_id},
                )
        return ReplyClassifyResponse(
            classification=classification,
            confidence=max(0.0, min(1.0, float(obj["confidence"]))),
            next_action=obj["next_action"],
        )

    def create_scoping_brief(self, opportunity_id: str) -> ScopingBriefResponse:
        context_lines = [f"Opportunity ID: {opportunity_id}"]
        try:
            opp = self.store.get_opportunity_by_draft("")
            with self.store._lock, self.store._conn() as conn:
                row = conn.execute("SELECT * FROM wt_opportunities WHERE id = ?", (opportunity_id,)).fetchone()
            if row:
                acct = self.store.get_account(row["account_id"])
                context_lines.append(f"Company: {acct.get('name', '')} ({acct.get('domain', '')})")
                context_lines.append(f"Industry: {acct.get('industry', 'unknown')}")
                context_lines.append(f"Employee count: {acct.get('employee_count', 'unknown')}")
                context_lines.append(f"Funding stage: {acct.get('funding_stage', 'unknown')}")
                try:
                    contact = self.store.get_contact(row["contact_id"])
                    context_lines.append(f"Contact: {contact.get('full_name', '')} — {contact.get('title', '')}")
                except KeyError:
                    pass
                if row.get("source_draft_id"):
                    try:
                        draft = self.store.get_draft(row["source_draft_id"])
                        context_lines.append(f"Draft subject: {draft.get('subject_a', '')}")
                    except KeyError:
                        pass
                hypotheses = self.store.list_work_hypotheses(row["account_id"])
                if hypotheses:
                    h = hypotheses[0]
                    context_lines.append(f"Work hypothesis: {h.get('probable_problem', '')} → {h.get('probable_deliverable', '')}")
        except Exception:
            pass

        prompt = (
            "Generate a concise first-call scoping brief for a services opportunity.\n"
            + "\n".join(context_lines)
            + "\nInclude concrete work packages and discovery questions."
        )
        schema = {
            "type": "object",
            "required": [
                "summary",
                "likely_pain_points",
                "proposed_work_packages",
                "suggested_talent_archetypes",
                "discovery_questions",
            ],
            "properties": {
                "summary": {"type": "string"},
                "likely_pain_points": {"type": "array", "items": {"type": "string"}},
                "proposed_work_packages": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["name", "scope", "duration_weeks"],
                        "properties": {
                            "name": {"type": "string"},
                            "scope": {"type": "string"},
                            "duration_weeks": {"type": "number"},
                        },
                        "additionalProperties": False,
                    },
                },
                "suggested_talent_archetypes": {"type": "array", "items": {"type": "string"}},
                "discovery_questions": {"type": "array", "items": {"type": "string"}},
            },
            "additionalProperties": False,
        }
        obj = _openai_structured_json(
            prompt,
            "scoping_brief",
            schema,
            store=self.store,
            task_name="scoping_brief",
            evidence={"opportunity_id": opportunity_id},
        )
        sid = self.store.save_scoping_brief(
            opportunity_id=opportunity_id,
            summary=obj["summary"],
            likely_pain_points=list(obj["likely_pain_points"]),
            proposed_work_packages=list(obj["proposed_work_packages"]),
            suggested_talent_archetypes=list(obj["suggested_talent_archetypes"]),
            discovery_questions=list(obj["discovery_questions"]),
        )
        saved = self.store.get_scoping_brief(sid)
        return ScopingBriefResponse(
            scoping_brief_id=saved["id"],
            opportunity_id=saved["opportunity_id"],
            summary=saved["summary"],
            likely_pain_points=saved["likely_pain_points"],
            proposed_work_packages=saved["proposed_work_packages"],
            suggested_talent_archetypes=saved["suggested_talent_archetypes"],
            discovery_questions=saved["discovery_questions"],
        )

    def run_job(self, job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        if job_type == "enrich_contacts":
            account_id = str(payload["account_id"])
            contacts = payload.get("contacts", [])
            for item in contacts:
                self.store.upsert_contact(account_id, item)
            return {"contacts_found": len(self.store.list_contacts(account_id))}
        if job_type == "generate_hypothesis":
            account_id = str(payload["account_id"])
            out = self.generate_work_hypothesis(account_id)
            return out.model_dump()
        if job_type == "generate_draft":
            did = self.generate_draft(
                account_id=str(payload["account_id"]),
                contact_id=str(payload["contact_id"]),
                work_hypothesis_id=str(payload["work_hypothesis_id"]),
                channel=str(payload.get("channel", "email")),
            )
            return {"draft_id": did}
        if job_type == "send_draft":
            mid = self.send_approved_draft(str(payload["draft_id"]))
            return {"message_id": mid}
        if job_type == "sync_crm":
            deal = self.sync_opportunity_to_hubspot(str(payload["draft_id"]))
            return {"deal_id": deal}
        if job_type == "classify_reply":
            out = self.classify_reply(
                draft_id=str(payload["draft_id"]),
                reply_text=str(payload["reply_text"]),
                thread_metadata=dict(payload.get("thread_metadata", {})),
            )
            return out.model_dump()
        if job_type == "create_scoping_brief":
            out = self.create_scoping_brief(str(payload["opportunity_id"]))
            return out.model_dump()
        raise ValueError(f"Unsupported job_type={job_type}")

    def crm_reconciliation_report(self, account_id: str | None = None, limit: int = 200) -> dict[str, Any]:
        events = self.store.list_crm_sync_events(account_id=account_id, limit=limit)
        success = sum(1 for e in events if e["status"] == "success")
        errors = sum(1 for e in events if e["status"] != "success")
        return {
            "account_id": account_id,
            "event_count": len(events),
            "success_count": success,
            "error_count": errors,
            "events": events,
            "drift_summary": self.store.crm_drift_summary(),
            "source_of_truth_matrix": {
                "account.domain": "app",
                "account.name": "hubspot_if_present_else_app",
                "contact.email": "hubspot_if_valid_else_app",
                "opportunity.stage": "app",
                "opportunity.crm_id": "hubspot",
                "draft.status": "app",
            },
        }

    def detect_crm_conflicts(
        self,
        *,
        account_id: str,
        crm_company_name: str | None = None,
        crm_domain: str | None = None,
    ) -> list[str]:
        account = self.store.get_account(account_id)
        conflict_ids: list[str] = []
        app_domain = str(account.get("domain") or "").strip().lower()
        app_name = str(account.get("name") or "").strip()
        if crm_domain and crm_domain.strip().lower() != app_domain:
            conflict_ids.append(
                self.store.add_crm_conflict(
                    account_id=account_id,
                    field_name="account.domain",
                    app_value=app_domain or None,
                    crm_value=crm_domain.strip().lower(),
                    policy="app",
                )
            )
        if crm_company_name and app_name and crm_company_name.strip() != app_name:
            conflict_ids.append(
                self.store.add_crm_conflict(
                    account_id=account_id,
                    field_name="account.name",
                    app_value=app_name,
                    crm_value=crm_company_name.strip(),
                    policy="hubspot_if_present_else_app",
                )
            )
        return conflict_ids

    def generate_quote(self, opportunity_id: str) -> dict[str, Any]:
        base_quote = {
            "currency": "USD",
            "line_items": [
                {"name": "Discovery sprint", "weeks": 2, "rate_per_week": 4500},
                {"name": "Execution sprint", "weeks": 4, "rate_per_week": 5200},
            ],
            "assumptions": [
                "Client provides single point of contact",
                "Weekly delivery cadence",
            ],
        }
        qid = self.store.save_quote(opportunity_id=opportunity_id, quote=base_quote, status="draft")
        return self.store.get_quote(qid)

    def build_talent_shortlist(self, opportunity_id: str, geography_id: str | None = None) -> dict[str, Any]:
        candidates = [
            {"role": "Product Marketing Lead", "seniority": "Senior", "availability": "2-3 weeks"},
            {"role": "Demand Gen Specialist", "seniority": "Mid", "availability": "1-2 weeks"},
            {"role": "RevOps Analyst", "seniority": "Senior", "availability": "Immediate"},
        ]
        sid = self.store.save_talent_shortlist(
            opportunity_id=opportunity_id,
            geography_id=geography_id,
            candidates=candidates,
            status="draft",
        )
        return self.store.get_talent_shortlist(sid)

    def update_staffing_workflow(
        self, *, opportunity_id: str, state: str, owner_user_id: str | None, checklist: dict[str, Any]
    ) -> dict[str, Any]:
        self.store.upsert_staffing_workflow(
            opportunity_id=opportunity_id,
            state=state,
            owner_user_id=owner_user_id,
            checklist=checklist,
        )
        workflow = self.store.get_staffing_workflow(opportunity_id)
        if workflow is None:
            raise HTTPException(status_code=500, detail="Failed to persist staffing workflow.")
        return workflow

    def llm_eval_report(self, task_name: str | None = None, limit: int = 200) -> dict[str, Any]:
        runs = self.store.list_llm_runs(task_name=task_name, limit=limit)
        if not runs:
            return {"task_name": task_name, "run_count": 0, "cache_hit_rate": 0.0, "schema_pass_rate": 0.0}
        cache_hits = sum(1 for r in runs if r.get("cached_hit"))
        schema_pass = 0
        for run in runs:
            response = run.get("response")
            schema_pass += 1 if isinstance(response, dict) and len(response) > 0 else 0
        return {
            "task_name": task_name,
            "run_count": len(runs),
            "cache_hit_rate": cache_hits / len(runs),
            "schema_pass_rate": schema_pass / len(runs),
            "runs": runs,
        }
