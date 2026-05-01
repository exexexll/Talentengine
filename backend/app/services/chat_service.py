"""Per-account conversational chat (ChatGPT-style) with tool calling.

Each session is scoped to an account.  The assistant receives a grounded
context block (company profile, signals, hypothesis, geography scores,
cached social signals) plus the full prior conversation, and may call a
`web_search` tool backed by SerpAPI to look up fresh information.

Sessions and messages persist to SQLite via `WorkTriggerStore`, so
conversations survive restarts and the SDR can always pull up prior
threads per account.
"""

from __future__ import annotations

import json
import os
from typing import Any

from backend.app.services.llm_config import (
    grounding_preamble,
    primary_model,
)
from backend.app.services.vendors.social_signals import get_company_social_signals
from backend.app.services.worktrigger_store import WorkTriggerStore

# Bounded: the ~10 most recent user/assistant turns + system + tool msgs.
# GPT-5.4's 1.05M context leaves enormous headroom; this cap is purely
# for latency + cost control.
MAX_HISTORY_MESSAGES = 40
MAX_TOOL_ITERATIONS = 4  # cap so the model can't loop forever on tool calls


# ---------------------------------------------------------------------------
# Tool definitions (OpenAI function-calling format)
# ---------------------------------------------------------------------------

TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": (
                "Search the public web for up-to-date information about the "
                "company, the buyer, a competitor, a technology, or industry "
                "news. Returns the top organic results (title, snippet, URL). "
                "Use this when you need facts the assistant does not already "
                "have in its context, such as recent funding rounds, hiring "
                "announcements, executive changes, or press coverage."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A focused Google search query. Include the company name for precision.",
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "How many results to return (1-10).",
                        "minimum": 1,
                        "maximum": 10,
                    },
                    "time_range": {
                        "type": "string",
                        "description": "Optional recency filter: 'day', 'week', 'month', 'year'. Omit for all time.",
                        "enum": ["day", "week", "month", "year"],
                    },
                },
                "required": ["query"],
            },
        },
    },
]


def _tbs_for(time_range: str | None) -> str:
    return {
        "day": "qdr:d",
        "week": "qdr:w",
        "month": "qdr:m",
        "year": "qdr:y",
    }.get(time_range or "", "")


def tool_web_search(query: str, num_results: int = 6, time_range: str | None = None) -> dict[str, Any]:
    """Run a SerpAPI Google search and return a compact result list."""
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if not api_key:
        return {"ok": False, "error": "SERPAPI_KEY not configured", "query": query, "results": []}

    try:
        from serpapi import GoogleSearch
    except ImportError:
        return {"ok": False, "error": "serpapi package not installed", "query": query, "results": []}

    params: dict[str, Any] = {
        "engine": "google",
        "q": query,
        "api_key": api_key,
        "num": max(1, min(10, int(num_results or 6))),
        "hl": "en",
    }
    tbs = _tbs_for(time_range)
    if tbs:
        params["tbs"] = tbs

    try:
        data = GoogleSearch(params).get_dict()
    except Exception as exc:  # network / rate-limit / quota
        return {"ok": False, "error": f"SerpAPI error: {exc}", "query": query, "results": []}

    out: list[dict[str, Any]] = []
    for item in (data.get("organic_results") or [])[: params["num"]]:
        out.append({
            "title": (item.get("title") or "")[:180],
            "url": item.get("link") or "",
            "snippet": (item.get("snippet") or "")[:300],
            "source": item.get("source") or "",
            "date": item.get("date") or "",
        })
    return {"ok": True, "query": query, "count": len(out), "results": out}


# ---------------------------------------------------------------------------
# Context builder — grounds the model in real account data
# ---------------------------------------------------------------------------


def _fmt_dollars(value: Any) -> str:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return ""
    if n <= 0:
        return ""
    if n >= 1_000_000_000:
        return f"${n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n / 1_000:.0f}K"
    return f"${n:.0f}"


def build_account_context(store: WorkTriggerStore, account_id: str) -> str:
    """Assemble a concise grounding block the model can use.

    Keeps it under ~2k tokens so history still fits comfortably.
    """
    try:
        acct_rows = [a for a in store.list_all_accounts(limit=2000) if str(a.get("id")) == account_id]
    except Exception:
        acct_rows = []
    if not acct_rows:
        return ""
    acct = acct_rows[0]

    domain = str(acct.get("domain") or "")
    name = str(acct.get("name") or domain or "Unknown")
    industry = str(acct.get("industry") or "")
    employees = acct.get("employee_count")
    funding_stage = str(acct.get("funding_stage") or "")
    total_funding = _fmt_dollars(acct.get("total_funding"))
    country = str(acct.get("country") or "")
    icp = str(acct.get("icp_status") or "unknown")
    li = str(acct.get("linkedin_url") or "")

    lines: list[str] = []
    lines.append(f"# Company: {name}")
    if domain:
        lines.append(f"Domain: {domain}")
    descriptors: list[str] = []
    if industry:
        descriptors.append(f"Industry: {industry}")
    if employees:
        descriptors.append(f"Employees: {employees}")
    if funding_stage:
        descriptors.append(f"Funding stage: {funding_stage}")
    if total_funding:
        descriptors.append(f"Total raised: {total_funding}")
    if country:
        descriptors.append(f"Country: {country}")
    descriptors.append(f"ICP status: {icp}")
    if descriptors:
        lines.append(" · ".join(descriptors))
    if li:
        lines.append(f"LinkedIn: {li}")

    # Signal stack
    stack = store.get_latest_signal_stack(account_id)
    if stack:
        lines.append("")
        lines.append("## Signal stack")
        parts: list[str] = []
        for k in ("funding_score", "hiring_score", "exec_change_score", "web_intent_score", "buyer_intent_score", "total_signal_score"):
            v = stack.get(k)
            if v is not None:
                parts.append(f"{k.replace('_score', '').replace('_', ' ')}: {float(v):.0f}")
        if parts:
            lines.append(" · ".join(parts))
        exp = stack.get("explanation") or {}
        priority = exp.get("priority_score") if isinstance(exp, dict) else None
        if priority is not None:
            lines.append(f"Priority score: {float(priority):.1f}")

    # Work hypothesis (most recent)
    try:
        hypotheses = store.list_work_hypotheses(account_id=account_id)  # type: ignore[attr-defined]
    except Exception:
        hypotheses = []
    if hypotheses:
        hyp = hypotheses[0]
        lines.append("")
        lines.append("## Current work hypothesis")
        if hyp.get("probable_problem"):
            lines.append(f"Problem: {hyp['probable_problem']}")
        if hyp.get("probable_deliverable"):
            lines.append(f"Deliverable: {hyp['probable_deliverable']}")
        if hyp.get("talent_archetype"):
            lines.append(f"Talent archetype: {hyp['talent_archetype']}")
        rationale = hyp.get("rationale") or []
        if isinstance(rationale, list) and rationale:
            lines.append("Evidence: " + " | ".join(str(r) for r in rationale[:4]))

    # Contacts (top 5)
    try:
        contacts = store.list_contacts(account_id=account_id)  # type: ignore[attr-defined]
    except Exception:
        contacts = []
    if contacts:
        lines.append("")
        lines.append(f"## Contacts ({len(contacts)})")
        for c in contacts[:5]:
            nm = c.get("full_name") or ""
            title = c.get("title") or ""
            email = c.get("email") or ""
            parts = [p for p in [nm, title, email] if p]
            lines.append(" · ".join(parts))

    # Geo attribution
    geo = store.get_geo_attribution(account_id)
    if geo:
        lines.append("")
        lines.append("## Geography")
        for g in geo[:3]:
            lines.append(f"- {g.get('geography_id')} (weight {float(g.get('weight') or 0):.2f})")

    # Cached social signals (free because already on disk)
    if domain:
        try:
            sig = get_company_social_signals(
                domain,
                company_name=name,
                linkedin_url=str(acct.get("linkedin_url") or ""),
                twitter_url=str(acct.get("twitter_url") or ""),
            )
        except Exception:
            sig = None
        if sig:
            analysis = (sig.get("analysis") or {}) if isinstance(sig, dict) else {}
            if isinstance(analysis, dict):
                summary = str(analysis.get("summary") or "").strip()
                if summary:
                    lines.append("")
                    lines.append("## Social/hiring signal summary")
                    lines.append(summary[:600])
                outreach = str(analysis.get("outreach_angle") or "").strip()
                if outreach:
                    lines.append(f"Suggested angle: {outreach}")
                jobs = sig.get("job_postings") or []
                if isinstance(jobs, list) and jobs:
                    top_jobs = [j.get("title") for j in jobs[:5] if isinstance(j, dict) and j.get("title")]
                    if top_jobs:
                        lines.append("Active roles: " + " · ".join(top_jobs))

    return "\n".join(lines).strip()


SYSTEM_TEMPLATE = (
    "{preamble}\n\n"
    "You are Figwork's SDR co-pilot.  You help the human SDR strategize, "
    "research, and write outbound to a specific prospect company.  Be concise, "
    "concrete, and action-oriented — no fluff.  Ground every claim in the "
    "context block or in web_search results; if you do not know, say so and "
    "(when helpful) call web_search.  Prefer project-scoped, fractional "
    "engagement angles over recruiter-style pitches.  Avoid broad generic "
    "phrases (e.g. 'in today's market', 'many companies', 'drive growth', "
    "'optimize operations') unless tied to a concrete fact in context. "
    "Use Markdown for lists "
    "and bold for emphasis.\n\n"
    "--- ACCOUNT CONTEXT ---\n{context}\n--- END CONTEXT ---"
)


# ---------------------------------------------------------------------------
# Chat service
# ---------------------------------------------------------------------------


class ChatService:
    def __init__(self, store: WorkTriggerStore) -> None:
        self.store = store

    # --- Sessions --------------------------------------------------------

    def list_sessions(self, account_id: str) -> list[dict[str, Any]]:
        return self.store.list_chat_sessions(account_id)

    def create_session(self, account_id: str, title: str = "") -> dict[str, Any]:
        return self.store.create_chat_session(account_id, title=title or "New conversation", model=primary_model())

    def rename_session(self, session_id: str, title: str) -> None:
        self.store.rename_chat_session(session_id, title)

    def delete_session(self, session_id: str) -> None:
        self.store.delete_chat_session(session_id)

    def list_messages(self, session_id: str) -> list[dict[str, Any]]:
        return self.store.list_chat_messages(session_id, include_system=False)

    # --- Turn handling ---------------------------------------------------

    def send_message(self, session_id: str, user_message: str) -> dict[str, Any]:
        """Append a user message, run the assistant turn (with tool-calls),
        persist the results, and return the final assistant reply."""
        session = self.store.get_chat_session(session_id)
        account_id = session["account_id"]

        # Persist user message first so the UI can optimistically render it.
        self.store.append_chat_message(session_id, role="user", content=user_message)

        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        # Sessions pin the model they were created with so an ongoing
        # conversation stays on a consistent model, but new sessions
        # always pick up the platform default (primary_model()).
        model = session.get("model") or primary_model()

        if not api_key:
            fallback = (
                "Chat requires an OPENAI_API_KEY to be configured on the backend."
                " I saved your message, but I can't respond without the API key."
            )
            return self.store.append_chat_message(session_id, role="assistant", content=fallback)

        # Build a fresh context each turn so updated signals/hypotheses are
        # reflected.  Cheap — pulls from local SQLite only.
        context_block = build_account_context(self.store, account_id) or "No account context available."
        system_msg = {
            "role": "system",
            "content": SYSTEM_TEMPLATE.format(
                preamble=grounding_preamble(),
                context=context_block,
            ),
        }

        # Reconstruct OpenAI-format history from persisted messages.
        history_rows = self.store.list_chat_messages(session_id, include_system=False)[-MAX_HISTORY_MESSAGES:]
        openai_messages: list[dict[str, Any]] = [system_msg]
        for m in history_rows:
            role = m["role"]
            if role == "assistant" and m.get("tool_calls"):
                openai_messages.append({
                    "role": "assistant",
                    "content": m.get("content") or "",
                    "tool_calls": m["tool_calls"],
                })
            elif role == "tool":
                openai_messages.append({
                    "role": "tool",
                    "content": m["content"],
                    "tool_call_id": m.get("tool_call_id") or "",
                })
            else:
                openai_messages.append({"role": role, "content": m["content"]})

        # Tool-calling loop
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        final_assistant: dict[str, Any] | None = None
        for _ in range(MAX_TOOL_ITERATIONS + 1):
            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=openai_messages,
                    tools=TOOLS,
                    tool_choice="auto",
                    temperature=0.4,
                    max_completion_tokens=1200,
                )
            except Exception as exc:
                err_msg = f"LLM error: {exc}"
                return self.store.append_chat_message(session_id, role="assistant", content=err_msg)

            choice = resp.choices[0].message
            tool_calls = getattr(choice, "tool_calls", None) or []

            if tool_calls:
                # Persist the assistant's tool-call stub so conversation history
                # reflects it, then execute each tool and append tool results.
                serialized_calls = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments or "{}",
                        },
                    }
                    for tc in tool_calls
                ]
                self.store.append_chat_message(
                    session_id,
                    role="assistant",
                    content=choice.content or "",
                    tool_calls=serialized_calls,
                )
                openai_messages.append({
                    "role": "assistant",
                    "content": choice.content or "",
                    "tool_calls": serialized_calls,
                })

                for tc in tool_calls:
                    name = tc.function.name
                    try:
                        args = json.loads(tc.function.arguments or "{}")
                    except (ValueError, TypeError):
                        args = {}
                    if name == "web_search":
                        out = tool_web_search(
                            query=str(args.get("query") or ""),
                            num_results=int(args.get("num_results") or 6),
                            time_range=args.get("time_range"),
                        )
                    else:
                        out = {"ok": False, "error": f"Unknown tool: {name}"}
                    tool_content = json.dumps(out)[:6000]
                    self.store.append_chat_message(
                        session_id,
                        role="tool",
                        content=tool_content,
                        tool_call_id=tc.id,
                        tool_name=name,
                    )
                    openai_messages.append({
                        "role": "tool",
                        "content": tool_content,
                        "tool_call_id": tc.id,
                    })
                continue

            # No tool calls — final answer
            final_assistant = self.store.append_chat_message(
                session_id,
                role="assistant",
                content=choice.content or "",
            )
            break

        if final_assistant is None:
            final_assistant = self.store.append_chat_message(
                session_id,
                role="assistant",
                content="(Reached tool-call iteration limit without producing a final response.)",
            )

        # Auto-title the session based on the first exchange if still default
        if (session.get("title") or "").strip().lower() in {"new conversation", "", "untitled"}:
            try:
                seed = user_message.strip().split("\n")[0][:60]
                if seed:
                    self.store.rename_chat_session(session_id, seed)
            except Exception:
                pass

        return final_assistant
