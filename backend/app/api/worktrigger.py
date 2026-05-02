from __future__ import annotations

import os
from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Body, HTTPException

from backend.app.models.worktrigger import (
    AccountScoreResponse,
    ContactEnrichRequest,
    ContactEnrichResponse,
    DraftGenerateRequest,
    DraftGenerateResponse,
    EmailTemplateCreateRequest,
    EmailTemplateResponse,
    EmailTemplateUpdateRequest,
    JobEnqueueRequest,
    JobRecordResponse,
    ReplyClassifyRequest,
    ReplyClassifyResponse,
    ReviewRequest,
    ScopingBriefResponse,
    SendResponse,
    SignalIngestRequest,
    SignalIngestResponse,
    WorkHypothesisResponse,
)
from backend.app.services.vendors.clay import extract_clay_account_fields, extract_clay_contacts, fetch_clay_table_rows, normalize_clay_webhook
from backend.app.services.vendors.commonroom import fetch_commonroom_signals, normalize_commonroom_webhook
from backend.app.services.vendors.contact_waterfall import apollo_search_contacts_by_title, enrich_contacts_waterfall
from backend.app.services.vendors.crunchbase import enrich_company as cb_enrich_company, fetch_recent_funding_rounds, normalize_funding_round
from backend.app.services.vendors.linkedin import get_account_insights, normalize_linkedin_intent, search_accounts as li_search_accounts
from backend.app.services.vendors.sec_edgar import fetch_recent_funding_filings, search_company_signals
from backend.app.services.vendors.company_discovery import apollo_org_enrich, discover_companies_for_geography
from backend.app.services.vendors.social_signals import get_company_social_signals
from backend.app.services.worktrigger_service import WorkTriggerService
from backend.app.services.worktrigger_store import WorkTriggerStore


router = APIRouter()

_store = WorkTriggerStore(os.getenv("WORKTRIGGER_DB_PATH", "backend/data/worktrigger.sqlite3"))
_service = WorkTriggerService(_store)

from backend.app.services.chat_service import ChatService
_chat = ChatService(_store)

from backend.app.services.search_service import SearchService
_search = SearchService(_store)


@router.get("/search")
def universal_search(
    q: str = "",
    types: str = "all",
    limit: int = 20,
    apollo_page: int = 1,
    industries: str = "",
) -> dict[str, Any]:
    """Universal search across local accounts, contacts, and vendors.

    Returns grouped results (local_accounts, local_contacts, companies,
    people) plus metadata (intent, llm_used, credits_spent, took_ms).
    Cheap: local stages are always free; vendor stages cache for 10 min
    per normalized query.  ``apollo_page`` lets the UI request the next
    100-row batch of industry-bulk results — each bump costs 1 more
    Apollo credit; the frontend prompts the user before doing so.
    """
    return _search.search(
        q,
        types=types,
        limit=max(1, min(50, limit)),
        apollo_page=max(1, min(10, apollo_page)),
        industries=[x.strip() for x in industries.split(",") if x.strip()],
    )


@router.post("/signals/ingest", response_model=SignalIngestResponse)
def ingest_signal(request: SignalIngestRequest) -> SignalIngestResponse:
    idem_key = request.idempotency_key
    if idem_key:
        record = _store.get_idempotency("signals/ingest", idem_key)
        if record is not None:
            return SignalIngestResponse(**record.response_json)
    resolved_account_id = None
    if request.account.linkedin_company_id:
        resolved_account_id = _store.resolve_account_by_identity("linkedin_company_id", request.account.linkedin_company_id)
    if not resolved_account_id and request.account.crunchbase_uuid:
        resolved_account_id = _store.resolve_account_by_identity("crunchbase_uuid", request.account.crunchbase_uuid)
    if not resolved_account_id and request.account.hubspot_company_id:
        resolved_account_id = _store.resolve_account_by_identity("hubspot_company_id", request.account.hubspot_company_id)

    if resolved_account_id:
        existing = _store.get_account(resolved_account_id)
        existing_domain = str(existing.get("domain") or "").strip().lower()
        incoming_domain = request.account.domain.strip().lower()
        if incoming_domain.startswith("http://"):
            incoming_domain = incoming_domain[7:]
        if incoming_domain.startswith("https://"):
            incoming_domain = incoming_domain[8:]
        incoming_domain = incoming_domain.strip("/ ")
        if existing_domain and incoming_domain and existing_domain != incoming_domain:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Identity collision: external ID resolves to account "
                    f"domain={existing_domain} but ingest domain={incoming_domain}. "
                    f"Resolve the identity conflict before ingesting."
                ),
            )
        account_id = resolved_account_id
        _store.update_account_fields(account_id, {"name": request.account.name})
    else:
        account_id, _created = _store.upsert_account(
            domain=request.account.domain,
            name=request.account.name,
        )
    try:
        if request.account.linkedin_company_id:
            _store.upsert_identity(
                account_id=account_id,
                identity_type="linkedin_company_id",
                identity_value=request.account.linkedin_company_id,
                confidence_score=1.0,
                source=request.source,
            )
        if request.account.crunchbase_uuid:
            _store.upsert_identity(
                account_id=account_id,
                identity_type="crunchbase_uuid",
                identity_value=request.account.crunchbase_uuid,
                confidence_score=1.0,
                source=request.source,
            )
        if request.account.hubspot_company_id:
            _store.upsert_identity(
                account_id=account_id,
                identity_type="hubspot_company_id",
                identity_value=request.account.hubspot_company_id,
                confidence_score=1.0,
                source=request.source,
            )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    normalized_payload = {
        "signal_type": request.signal_type,
        "geography_id": request.account.headquarters_geo_id or request.payload.get("geography_id"),
        "locations": request.account.locations,
        **request.payload,
    }
    signal_id, is_new = _store.add_signal(
        account_id=account_id,
        signal_type=request.signal_type,
        source=request.source,
        occurred_at=request.occurred_at.isoformat(),
        raw_payload=request.payload,
        normalized_payload=normalized_payload,
        confidence_score=0.8,
    )
    response = SignalIngestResponse(
        signal_id=signal_id,
        account_id=account_id,
        status="accepted" if is_new else "duplicate",
    )
    if idem_key:
        _store.put_idempotency("signals/ingest", idem_key, response.model_dump())
    return response


@router.post("/accounts/{account_id}/score", response_model=AccountScoreResponse)
def recompute_account_score(account_id: str) -> AccountScoreResponse:
    try:
        return _service.recompute_account_score(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/accounts/{account_id}/work-hypothesis", response_model=WorkHypothesisResponse)
def create_work_hypothesis(account_id: str) -> WorkHypothesisResponse:
    try:
        return _service.generate_work_hypothesis(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/accounts/{account_id}/contacts/enrich", response_model=ContactEnrichResponse)
def enrich_contacts(account_id: str, request: ContactEnrichRequest) -> ContactEnrichResponse:
    try:
        _store.get_account(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    for item in request.contacts:
        _store.upsert_contact(account_id, item)
    contacts = _store.list_contacts(account_id)
    best_contact = contacts[0]["id"] if contacts else None
    return ContactEnrichResponse(contacts_found=len(contacts), best_contact_id=best_contact)


@router.post("/drafts/generate", response_model=DraftGenerateResponse)
def generate_draft(request: DraftGenerateRequest) -> DraftGenerateResponse:
    try:
        draft_id = _service.generate_draft(
            account_id=request.account_id,
            contact_id=request.contact_id,
            work_hypothesis_id=request.work_hypothesis_id,
            channel=request.channel,
            template_id=request.template_id,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return DraftGenerateResponse(draft_id=draft_id, status="draft_ready")


@router.get("/templates/email", response_model=list[EmailTemplateResponse])
def list_email_templates(limit: int = 200) -> list[EmailTemplateResponse]:
    rows = _store.list_email_templates(limit=limit)
    return [EmailTemplateResponse(**row) for row in rows]


@router.post("/templates/email", response_model=EmailTemplateResponse)
def create_email_template(request: EmailTemplateCreateRequest) -> EmailTemplateResponse:
    template_id = _store.create_email_template(
        name=request.name,
        subject_a=request.subject_a,
        subject_b=request.subject_b,
        email_body=request.email_body,
        followup_body=request.followup_body,
        linkedin_dm=request.linkedin_dm,
    )
    return EmailTemplateResponse(**_store.get_email_template(template_id))


@router.delete("/templates/email/{template_id}")
def delete_email_template(template_id: str) -> dict[str, str]:
    try:
        _store.delete_email_template(template_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"template_id": template_id, "status": "deleted"}


@router.put("/templates/email/{template_id}", response_model=EmailTemplateResponse)
def update_email_template(template_id: str, request: EmailTemplateUpdateRequest) -> EmailTemplateResponse:
    try:
        _store.update_email_template(
            template_id,
            name=request.name,
            subject_a=request.subject_a,
            subject_b=request.subject_b,
            email_body=request.email_body,
            followup_body=request.followup_body,
            linkedin_dm=request.linkedin_dm,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return EmailTemplateResponse(**_store.get_email_template(template_id))


@router.get("/drafts/{draft_id}")
def get_draft(draft_id: str) -> dict[str, Any]:
    """Return a single draft row.  Used by the Inbox view to lazy-load
    the full email body once the user expands a card — keeps the queue
    payload small while still allowing inline preview on demand."""
    try:
        return _store.get_draft(draft_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/drafts/{draft_id}/review")
def review_draft(draft_id: str, request: ReviewRequest) -> dict[str, Any]:
    try:
        _service.apply_review(draft_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"draft_id": draft_id, "status": "ok"}


@router.post("/drafts/{draft_id}/send", response_model=SendResponse)
def send_draft(draft_id: str, idempotency_key: str | None = None) -> SendResponse:
    if idempotency_key:
        record = _store.get_idempotency("drafts/send", idempotency_key)
        if record is not None:
            return SendResponse(**record.response_json)
    try:
        message_id = _service.send_approved_draft(draft_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    response = SendResponse(message_id=message_id, status="sent")
    if idempotency_key:
        _store.put_idempotency("drafts/send", idempotency_key, response.model_dump())
    return response


@router.post("/crm/sync/opportunity")
def sync_crm(draft_id: str) -> dict[str, str]:
    try:
        deal_id = _service.sync_opportunity_to_hubspot(draft_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"draft_id": draft_id, "deal_id": deal_id, "status": "synced"}


@router.post("/replies/classify", response_model=ReplyClassifyResponse)
def classify_reply(request: ReplyClassifyRequest) -> ReplyClassifyResponse:
    try:
        return _service.classify_reply(
            draft_id=request.draft_id,
            reply_text=request.reply_text,
            thread_metadata=request.thread_metadata,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/opportunities/{opportunity_id}/scoping-brief", response_model=ScopingBriefResponse)
def create_scoping_brief(opportunity_id: str) -> ScopingBriefResponse:
    try:
        return _service.create_scoping_brief(opportunity_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/status")
def status() -> dict[str, Any]:
    return {
        "service": "worktrigger",
        "server_time": datetime.utcnow().isoformat(),
        "db_path": os.getenv("WORKTRIGGER_DB_PATH", "backend/data/worktrigger.sqlite3"),
        "requires_openai": True,
        "requires_resend_for_send": True,
        "requires_hubspot_for_crm_sync": True,
    }


@router.get("/queue")
def review_queue(status: str = "draft_ready", limit: int = 100) -> list[dict[str, Any]]:
    drafts = _store.list_drafts(status=status if status != "all" else None, limit=max(1, min(500, limit)))
    out: list[dict[str, Any]] = []
    for d in drafts:
        try:
            account = _store.get_account(d["account_id"])
        except KeyError:
            account = {"name": "Unknown", "domain": ""}
        try:
            contact = _store.get_contact(d["contact_id"])
        except KeyError:
            contact = {"full_name": "Unknown", "title": ""}
        latest_stack = _store.get_latest_signal_stack(d["account_id"])
        out.append(
            {
                "draft_id": d["id"],
                "status": d["status"],
                "account_id": d["account_id"],
                "account_name": account.get("name"),
                "domain": account.get("domain"),
                "contact_id": d["contact_id"],
                "contact_name": contact.get("full_name"),
                "contact_title": contact.get("title"),
                "contact_email": contact.get("email", ""),
                "signal_score": (latest_stack or {}).get("total_signal_score", 0.0),
                "subject_a": d.get("subject_a"),
                "updated_at": d.get("updated_at"),
                "outreach_mode": d.get("outreach_mode"),
                "target_job_title": d.get("target_job_title"),
                "target_job_url": d.get("target_job_url"),
                "template_id": ((d.get("generation_metadata") or {}).get("template_id")),
            }
        )
    return out


@router.get("/accounts/{account_id}/detail")
def account_detail(account_id: str) -> dict[str, Any]:
    try:
        account = _store.get_account(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {
        "account": account,
        "geo_attribution": _store.get_geo_attribution(account_id),
        "identity_events": _store.list_identity_events(account_id, limit=200),
        "signals": _store.list_account_signals(account_id, limit=100),
        "signal_stack": _store.get_latest_signal_stack(account_id),
        "contacts": _store.list_contacts(account_id),
        "work_hypotheses": _store.list_work_hypotheses(account_id, limit=20),
        "drafts": _enrich_drafts_with_contact(account_id),
    }


@router.post("/jobs/enqueue", response_model=JobRecordResponse)
def enqueue_job(request: JobEnqueueRequest) -> JobRecordResponse:
    job_id, created = _store.enqueue_job(
        job_type=request.job_type,
        payload=request.payload,
        idempotency_key=request.idempotency_key,
        max_attempts=request.max_attempts,
    )
    row = _store.get_job(job_id)
    return JobRecordResponse(
        job_id=job_id,
        job_type=row["job_type"],
        status=row["status"],
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        run_after=datetime.fromisoformat(str(row["run_after"])),
        payload=row["payload"],
        last_error=row.get("last_error"),
    )


@router.post("/jobs/claim", response_model=JobRecordResponse | None)
def claim_job(job_types: list[str] | None = None) -> JobRecordResponse | None:
    row = _store.claim_next_job(allowed_types=job_types)
    if row is None:
        return None
    return JobRecordResponse(
        job_id=row["id"],
        job_type=row["job_type"],
        status="in_progress",
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        run_after=datetime.fromisoformat(str(row["run_after"])),
        payload=row["payload"],
        last_error=row.get("last_error"),
    )


@router.post("/jobs/{job_id}/complete")
def complete_job(job_id: str) -> dict[str, str]:
    try:
        _store.complete_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"job_id": job_id, "status": "completed"}


@router.post("/jobs/{job_id}/fail")
def fail_job(job_id: str, error_message: str) -> dict[str, str]:
    try:
        _store.fail_job(job_id, error_message)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"job_id": job_id, "status": "failed"}


@router.get("/jobs/dead-letter")
def dead_letters(limit: int = 100) -> list[dict[str, Any]]:
    return _store.list_dead_letters(limit=limit)


@router.post("/jobs/dead-letter/{dead_letter_id}/requeue")
def requeue_dead_letter(dead_letter_id: str, max_attempts: int = 5) -> dict[str, Any]:
    try:
        job_id = _store.requeue_dead_letter(dead_letter_id, max_attempts=max_attempts)
        job = _store.get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"dead_letter_id": dead_letter_id, "job_id": job_id, "status": job["status"]}


@router.post("/worker/run-once")
def worker_run_once(job_types: list[str] | None = None) -> dict[str, Any]:
    job = _store.claim_next_job(allowed_types=job_types)
    if job is None:
        return {"status": "idle"}
    try:
        result = _service.run_job(job["job_type"], job["payload"])
        _store.complete_job(job["id"])
        return {"status": "completed", "job_id": job["id"], "result": result}
    except Exception as exc:
        _store.fail_job(job["id"], str(exc))
        return {"status": "failed", "job_id": job["id"], "error": str(exc)}


@router.post("/compliance/suppress")
def suppress_email(email: str, reason: str = "manual", source: str = "operator") -> dict[str, str]:
    _store.add_suppression(email=email, reason=reason, source=source)
    return {"email": email.strip().lower(), "status": "suppressed"}


@router.get("/compliance/suppressions")
def list_suppressions(limit: int = 200) -> list[dict[str, Any]]:
    return _store.list_suppressions(limit=limit)


@router.get("/crm/reconcile")
def crm_reconcile(account_id: str | None = None, limit: int = 200) -> dict[str, Any]:
    return _service.crm_reconciliation_report(account_id=account_id, limit=limit)


@router.get("/accounts/all")
def list_all_accounts(limit: int = 500) -> list[dict[str, Any]]:
    """Return all accounts with geo attribution, signal stack, and entity counts."""
    rows = _store.list_all_accounts(limit=limit)
    out: list[dict[str, Any]] = []
    for acct in rows:
        geo = _store.get_geo_attribution(acct["id"])
        stack = _store.get_latest_signal_stack(acct["id"])
        counts = _store.account_counts(acct["id"])
        acct["geo_attribution"] = geo
        acct["signal_score"] = float(stack["total_signal_score"]) if stack else 0.0
        acct["priority_score"] = float((stack.get("explanation") or {}).get("priority_score", 0)) if stack else 0.0
        acct["primary_geo_id"] = geo[0]["geography_id"] if geo else None
        acct.update(counts)
        out.append(acct)
    return out


@router.post("/accounts/{account_id}/backfill-contacts-from-hunter")
def backfill_contacts_from_hunter(account_id: str) -> dict[str, Any]:
    """Cheap-credit retry for accounts that came out of intake with 0
    contacts but where Hunter's free email-count knows emails exist.

    Skips Apollo entirely (the prior intake already burned credits
    there).  Calls Hunter's `email-count` first (free) to verify there
    really are emails — if the count is 0 we don't even spend the
    Hunter `domain-search` credit.  When the user has been seeing the
    "2 emails available" hover preview but the pipeline is empty, this
    is the one-button fix.
    """
    try:
        account = _store.get_account(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    domain = str(account.get("domain") or "").strip().lower()
    if not domain:
        raise HTTPException(status_code=400, detail="Account has no domain.")
    existing = _store.list_contacts(account_id)
    existing_emails = {(c.get("email") or "").lower() for c in existing if c.get("email")}

    from backend.app.services.vendors.hunter_company import (
        hunter_domain_search, hunter_email_count,
    )
    from backend.app.services.vendors.contact_waterfall import _infer_persona

    count_data = hunter_email_count(domain)
    total = int(count_data.get("total") or 0)
    if total == 0:
        return {"added": 0, "hunter_total": 0, "reason": "Hunter has no emails on file for this domain."}

    domain_search = hunter_domain_search(domain, limit=5)
    found = domain_search.get("contacts") or []
    added = 0
    for hc in found:
        email = (hc.get("email") or "").strip().lower()
        if not email or "@" not in email or email in existing_emails:
            continue
        confidence = float(hc.get("confidence") or 0) / 100.0
        _store.upsert_contact(account_id, {
            "full_name": hc.get("full_name") or email.split("@")[0],
            "title": hc.get("title") or "",
            "email": hc.get("email"),
            "email_status": "valid" if confidence >= 0.8 else "risky",
            "linkedin_url": "",
            "persona_type": _infer_persona(hc.get("title") or ""),
            "confidence_score": confidence or 0.5,
            "source": "hunter_domain_search",
        })
        existing_emails.add(email)
        added += 1
    return {
        "added": added,
        "hunter_total": total,
        "domain": domain,
        "fetched": len(found),
    }


@router.post("/accounts/enable-job-outreach-all")
def enable_job_outreach_all() -> dict[str, Any]:
    """One-shot: turn on Job-Listing Outreach for every account that
    was created before the platform default flipped to ON.  Free; no
    LLM or vendor calls.  Existing accounts that the user explicitly
    turned OFF will be flipped back ON — by design, since the user
    asked for the platform default to be ON."""
    n = _store.enable_job_outreach_for_all()
    return {"updated": n}


@router.post("/contacts/purge-emailless")
def purge_emailless_contacts() -> dict[str, Any]:
    """Delete every auto-generated contact that has no usable email.

    Manual contacts (source='manual') are preserved — they were added
    deliberately by an SDR who plans to find the email later.  This
    endpoint exists to clean up legacy data produced by the previous
    intake logic, which used to persist Apollo people whose email
    reveal failed and Hunter "Main Contact" phone-only placeholders.
    Free — no vendor calls.
    """
    return _store.purge_emailless_auto_contacts()


@router.post("/drafts/collapse-duplicates")
def collapse_duplicate_drafts_endpoint(account_id: str | None = None) -> dict[str, Any]:
    """Archive duplicate active drafts so each contact has exactly ONE.

    Free, no LLM/vendor credits used.  Pass ``?account_id=…`` to scope
    to a single account, or omit for a global sweep across the whole
    pipeline.  The "winning" draft per (account, contact, hypothesis,
    channel) tuple is kept; everything else moves to ``discarded``.
    """
    if account_id is not None:
        try:
            _store.get_account(account_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _service.collapse_duplicate_drafts(account_id=account_id)


@router.post("/accounts/{account_id}/job-outreach")
def set_job_outreach_mode(
    account_id: str,
    body: dict[str, Any] = Body(...),
) -> dict[str, Any]:
    """Toggle the per-account 'job-listing outreach' mode.

    When enabled, future ``generate_draft`` calls fold the company's
    currently-open job postings into the prompt and produce email copy
    that pitches Figwork as fill for one of the listed roles.

    Body: ``{ "enabled": bool, "regenerate": bool }``.  When
    ``regenerate`` is true, every active draft on the account is
    re-generated with the new outreach mode and the old drafts are
    archived as ``discarded``.
    """
    enabled = bool(body.get("enabled"))
    regenerate = bool(body.get("regenerate"))
    try:
        _store.get_account(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    _store.set_job_outreach_enabled(account_id, enabled)

    result: dict[str, Any] = {"account_id": account_id, "enabled": enabled, "regenerated_count": 0}
    if regenerate:
        try:
            outcome = _service.regenerate_drafts_for_account(account_id)
            result.update(outcome)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Regenerate failed: {exc}")
    return result


@router.delete("/accounts/{account_id}")
def delete_account_endpoint(account_id: str) -> dict[str, Any]:
    """Hard-delete an account. Cascade removes contacts, signals, drafts,
    hypotheses, opportunities, and scoping briefs."""
    try:
        _store.delete_account(account_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete failed: {exc}")
    return {"status": "deleted", "account_id": account_id}


@router.post("/accounts/bulk-delete")
def bulk_delete_accounts_endpoint(account_ids: list[str] = Body(...)) -> dict[str, Any]:
    """Bulk-delete a list of accounts."""
    deleted = _store.bulk_delete_accounts(account_ids)
    return {"status": "ok", "deleted": deleted, "requested": len(account_ids)}


@router.get("/accounts/find-test")
def find_test_accounts_endpoint() -> dict[str, Any]:
    """List accounts whose name/domain matches test/smoke/demo patterns."""
    matches = _store.find_test_accounts()
    return {"count": len(matches), "accounts": matches}


@router.post("/drafts/purge")
def purge_drafts_by_status_endpoint(status: str = "discarded") -> dict[str, Any]:
    """Hard-delete all drafts at a given status (typically 'discarded')."""
    if status not in {"discarded", "snoozed"}:
        raise HTTPException(status_code=400, detail="Only 'discarded' or 'snoozed' drafts may be purged")
    deleted = _store.purge_drafts_by_status(status)
    return {"status": "ok", "deleted": deleted, "target_status": status}


# --- Chat (ChatGPT-style per-account assistant with SerpAPI tool) ----------


@router.get("/accounts/{account_id}/chat/sessions")
def list_chat_sessions(account_id: str) -> list[dict[str, Any]]:
    return _chat.list_sessions(account_id)


@router.post("/accounts/{account_id}/chat/sessions")
def create_chat_session(account_id: str, title: str = "") -> dict[str, Any]:
    # Verify account exists so we return 404 instead of FK violation
    try:
        _store.get_account(account_id)  # type: ignore[attr-defined]
    except Exception:
        pass
    return _chat.create_session(account_id, title=title)


@router.get("/chat/sessions/{session_id}/messages")
def list_chat_messages(session_id: str) -> dict[str, Any]:
    try:
        session = _store.get_chat_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"session": session, "messages": _chat.list_messages(session_id)}


@router.post("/chat/sessions/{session_id}/messages")
def send_chat_message(session_id: str, body: dict[str, Any] = Body(...)) -> dict[str, Any]:
    try:
        _store.get_chat_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    content = str(body.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="content required")
    reply = _chat.send_message(session_id, content)
    return {
        "reply": reply,
        "messages": _chat.list_messages(session_id),
    }


@router.patch("/chat/sessions/{session_id}")
def rename_chat_session(session_id: str, body: dict[str, Any] = Body(...)) -> dict[str, Any]:
    try:
        _chat.rename_session(session_id, str(body.get("title") or ""))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"status": "ok", "session_id": session_id}


@router.delete("/chat/sessions/{session_id}")
def delete_chat_session(session_id: str) -> dict[str, Any]:
    _chat.delete_session(session_id)
    return {"status": "deleted", "session_id": session_id}


@router.post("/accounts/purge-test")
def purge_test_accounts_endpoint() -> dict[str, Any]:
    """Delete every account matching test/smoke/demo patterns in one call."""
    matches = _store.find_test_accounts()
    ids = [m["id"] for m in matches]
    deleted = _store.bulk_delete_accounts(ids)
    return {"status": "ok", "deleted": deleted, "matched": len(matches), "accounts": matches}


@router.get("/opportunities")
def list_opportunities(stage: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    return _store.list_opportunities(stage=stage, limit=limit)


@router.get("/analytics/summary")
def analytics_summary() -> dict[str, Any]:
    return _store.analytics_summary()


@router.get("/worker/heartbeats")
def worker_heartbeats() -> list[dict[str, Any]]:
    return _store.list_worker_heartbeats()


@router.post("/crm/conflicts/detect")
def detect_crm_conflicts(
    account_id: str,
    crm_company_name: str | None = None,
    crm_domain: str | None = None,
) -> dict[str, Any]:
    try:
        conflict_ids = _service.detect_crm_conflicts(
            account_id=account_id,
            crm_company_name=crm_company_name,
            crm_domain=crm_domain,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"account_id": account_id, "conflict_ids": conflict_ids, "conflict_count": len(conflict_ids)}


@router.get("/crm/conflicts")
def list_crm_conflicts(status: str = "open", limit: int = 200) -> list[dict[str, Any]]:
    return _store.list_crm_conflicts(status=status, limit=limit)


@router.post("/crm/conflicts/{conflict_id}/resolve")
def resolve_crm_conflict(conflict_id: str, resolved_by: str, resolved_value: str) -> dict[str, Any]:
    _store.resolve_crm_conflict(conflict_id, resolved_by=resolved_by, resolved_value=resolved_value)
    return {"conflict_id": conflict_id, "status": "resolved"}


@router.post("/compliance/consent")
def upsert_consent(
    email: str,
    channel: str = "email",
    legal_basis: str = "legitimate_interest",
    status: str = "granted",
    source: str = "operator",
    metadata: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    _store.upsert_consent(
        email=email,
        channel=channel,
        legal_basis=legal_basis,
        status=status,
        source=source,
        metadata=metadata or {},
    )
    return {"email": email.strip().lower(), "channel": channel, "status": status}


@router.get("/compliance/consent")
def get_consent(email: str, channel: str = "email") -> dict[str, Any]:
    row = _store.get_consent(email=email, channel=channel)
    if row is None:
        raise HTTPException(status_code=404, detail="Consent record not found.")
    return row


@router.post("/compliance/delete")
def request_deletion(
    requested_by: str,
    reason: str = "privacy_request",
    email: str | None = None,
    account_id: str | None = None,
) -> dict[str, Any]:
    request_id = _store.request_deletion(email=email, account_id=account_id, reason=reason, requested_by=requested_by)
    return {"deletion_request_id": request_id, "status": "requested"}


@router.post("/compliance/delete/{request_id}/complete")
def complete_deletion(request_id: str) -> dict[str, Any]:
    try:
        _store.complete_deletion(request_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"deletion_request_id": request_id, "status": "completed"}


@router.post("/compliance/retention/policy")
def upsert_retention_policy(entity_type: str, retention_days: int, enabled: bool = True) -> dict[str, Any]:
    _store.upsert_retention_policy(entity_type=entity_type, retention_days=retention_days, enabled=enabled)
    return {"entity_type": entity_type, "retention_days": retention_days, "enabled": enabled}


@router.post("/compliance/retention/apply")
def apply_retention() -> dict[str, Any]:
    return {"purged": _store.apply_retention()}


@router.get("/llm/evals")
def llm_evals(task_name: str | None = None, limit: int = 200) -> dict[str, Any]:
    return _service.llm_eval_report(task_name=task_name, limit=limit)


@router.get("/llm/runs")
def llm_runs(task_name: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    return _store.list_llm_runs(task_name=task_name, limit=limit)


@router.post("/feedback/events")
def add_feedback_event(
    event_type: str,
    account_id: str | None = None,
    draft_id: str | None = None,
    value_num: float | None = None,
    value_text: str | None = None,
    metadata: dict[str, Any] | None = Body(default=None),
) -> dict[str, str]:
    _store.add_feedback_event(
        event_type=event_type,
        account_id=account_id,
        draft_id=draft_id,
        value_num=value_num,
        value_text=value_text,
        metadata=metadata or {},
    )
    return {"status": "accepted"}


@router.get("/feedback/events")
def list_feedback_events(event_type: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    return _store.list_feedback_events(event_type=event_type, limit=limit)


@router.post("/execution/quotes")
def create_quote(opportunity_id: str) -> dict[str, Any]:
    return _service.generate_quote(opportunity_id)


@router.post("/execution/shortlists")
def create_shortlist(opportunity_id: str, geography_id: str | None = None) -> dict[str, Any]:
    return _service.build_talent_shortlist(opportunity_id, geography_id=geography_id)


@router.post("/execution/staffing")
def upsert_staffing_workflow(
    opportunity_id: str,
    state: str,
    owner_user_id: str | None = None,
    checklist: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    return _service.update_staffing_workflow(
        opportunity_id=opportunity_id,
        state=state,
        owner_user_id=owner_user_id,
        checklist=checklist or {},
    )


# ---------------------------------------------------------------------------
# Vendor integration endpoints
# ---------------------------------------------------------------------------

def _enrich_drafts_with_contact(account_id: str) -> list[dict[str, Any]]:
    """Add contact_name and contact_email to each draft for display."""
    drafts = [d for d in _store.list_drafts(status=None, limit=500) if d["account_id"] == account_id]
    for d in drafts:
        try:
            contact = _store.get_contact(d["contact_id"])
            d["contact_name"] = contact.get("full_name", "")
            d["contact_email"] = contact.get("email", "")
        except KeyError:
            d["contact_name"] = ""
            d["contact_email"] = ""
    return drafts


def _auto_ingest(normalized: dict[str, Any]) -> dict[str, Any]:
    """Helper: ingest a normalized signal payload and auto-score the account."""
    from backend.app.models.worktrigger import SignalIngestRequest

    req = SignalIngestRequest(**normalized)
    resp = ingest_signal(req)
    try:
        _service.recompute_account_score(resp.account_id)
    except Exception:
        pass
    return {"signal_id": resp.signal_id, "account_id": resp.account_id, "status": resp.status}


def _generate_signals_from_enrichment(
    account_id: str,
    domain: str,
    enrichment: dict[str, Any],
    occurred_at: str,
) -> None:
    """Convert Apollo org enrichment data into real signals the scoring engine can use."""
    from backend.app.models.worktrigger import SignalIngestRequest

    account_ref = {"domain": domain, "name": enrichment.get("name") or domain}

    funding_stage = (enrichment.get("funding_stage") or "").strip().lower()
    total_funding = enrichment.get("total_funding")
    if funding_stage and funding_stage not in ("", "unknown", "undisclosed"):
        try:
            ingest_signal(SignalIngestRequest(
                source="apollo_enrichment", signal_type="funding_round",
                account=account_ref, occurred_at=occurred_at,
                payload={"funding_stage": funding_stage, "total_funding": total_funding},
            ))
        except Exception:
            pass

    employee_count = enrichment.get("employee_count")
    if employee_count and int(employee_count) > 50:
        try:
            ingest_signal(SignalIngestRequest(
                source="apollo_enrichment", signal_type="hiring_surge",
                account=account_ref, occurred_at=occurred_at,
                payload={"employee_count": employee_count, "industry": enrichment.get("industry")},
            ))
        except Exception:
            pass

    annual_revenue = enrichment.get("annual_revenue")
    if annual_revenue and float(annual_revenue) > 10_000_000:
        try:
            ingest_signal(SignalIngestRequest(
                source="apollo_enrichment", signal_type="expansion_event",
                account=account_ref, occurred_at=occurred_at,
                payload={"annual_revenue": annual_revenue, "employee_count": employee_count},
            ))
        except Exception:
            pass

    keywords = enrichment.get("keywords") or []
    if keywords:
        try:
            ingest_signal(SignalIngestRequest(
                source="apollo_enrichment", signal_type="web_intent",
                account=account_ref, occurred_at=occurred_at,
                payload={"keywords": keywords[:20], "short_description": enrichment.get("short_description", "")[:300]},
            ))
        except Exception:
            pass


@router.post("/vendors/clay/webhook")
def clay_webhook(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Receive a Clay table-row webhook, ingest signal, enrich contacts, and update account fields."""
    normalized = normalize_clay_webhook(payload)
    result = _auto_ingest(normalized)
    account_id = result.get("account_id", "")
    contacts_added = 0
    fields_updated = False

    if account_id:
        contacts = extract_clay_contacts(payload)
        for c in contacts:
            _store.upsert_contact(account_id, c)
            contacts_added += 1

        acct_fields = extract_clay_account_fields(payload)
        if acct_fields:
            _store.update_account_fields(account_id, acct_fields)
            fields_updated = True

    result["contacts_added"] = contacts_added
    result["account_fields_updated"] = fields_updated
    return result


@router.post("/vendors/clay/pull")
def clay_pull(table_id: str, limit: int = 100) -> dict[str, Any]:
    """Pull rows from a Clay table: ingest signals, enrich contacts, update account fields."""
    rows = fetch_clay_table_rows(table_id, limit=limit)
    results: list[dict[str, Any]] = []
    for row in rows:
        try:
            normalized = normalize_clay_webhook(row)
            r = _auto_ingest(normalized)
            aid = r.get("account_id", "")
            if aid:
                for c in extract_clay_contacts(row):
                    _store.upsert_contact(aid, c)
                acct_fields = extract_clay_account_fields(row)
                if acct_fields:
                    _store.update_account_fields(aid, acct_fields)
            results.append(r)
        except Exception as exc:
            results.append({"error": str(exc), "row_domain": row.get("company_domain") or row.get("domain")})
    return {"ingested": len([r for r in results if "signal_id" in r]), "errors": len([r for r in results if "error" in r]), "results": results}


@router.post("/vendors/commonroom/webhook")
def commonroom_webhook(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Receive a Common Room activity webhook and ingest as signal."""
    normalized = normalize_commonroom_webhook(payload)
    return _auto_ingest(normalized)


@router.post("/vendors/commonroom/pull")
def commonroom_pull(days: int = 7, limit: int = 100) -> dict[str, Any]:
    """Pull recent Common Room signals and batch-ingest."""
    signals = fetch_commonroom_signals(days=days, limit=limit)
    results: list[dict[str, Any]] = []
    for sig in signals:
        try:
            results.append(_auto_ingest(sig))
        except Exception as exc:
            results.append({"error": str(exc)})
    return {"ingested": len([r for r in results if "signal_id" in r]), "errors": len([r for r in results if "error" in r]), "results": results}


@router.post("/vendors/crunchbase/pull-funding")
def crunchbase_pull_funding(limit: int = 50) -> dict[str, Any]:
    """Pull recent Crunchbase funding rounds and ingest as signals."""
    rounds = fetch_recent_funding_rounds(limit=limit)
    results: list[dict[str, Any]] = []
    for rd in rounds:
        try:
            normalized = normalize_funding_round(rd)
            results.append(_auto_ingest(normalized))
        except Exception as exc:
            results.append({"error": str(exc), "organization": rd.get("organization_name")})
    return {"ingested": len([r for r in results if "signal_id" in r]), "errors": len([r for r in results if "error" in r]), "results": results}


@router.post("/vendors/crunchbase/enrich")
def crunchbase_enrich(domain: str) -> dict[str, Any]:
    """Enrich a company profile from Crunchbase and update account fields."""
    data = cb_enrich_company(domain)
    if not data.get("found"):
        return {"domain": domain, "found": False}
    account_id = _store.resolve_account_by_identity("domain", domain)
    if account_id:
        updates: dict[str, Any] = {}
        if data.get("num_employees_enum"):
            emp_map = {"c_0001_0010": 5, "c_0011_0050": 30, "c_0051_0100": 75, "c_0101_0250": 175, "c_0251_0500": 375, "c_0501_1000": 750, "c_1001_5000": 3000, "c_5001_10000": 7500}
            updates["employee_count"] = emp_map.get(data["num_employees_enum"], None)
        if data.get("last_funding_type"):
            updates["funding_stage"] = data["last_funding_type"]
        if data.get("funding_total_usd"):
            updates["total_funding"] = data["funding_total_usd"]
        if data.get("categories"):
            updates["industry"] = data["categories"][0] if data["categories"] else None
        if data.get("permalink"):
            updates["crunchbase_id"] = data["permalink"]
        if updates:
            _store.update_account_fields(account_id, updates)
    return {"domain": domain, "found": True, "account_id": account_id, "data": data}


@router.post("/vendors/sec-edgar/search")
def sec_edgar_search(domain: str = "", company_name: str = "", days_back: int = 90, limit: int = 10) -> dict[str, Any]:
    """Search SEC EDGAR for company funding (Form D) and material events (8-K). Free, no API key."""
    signals = search_company_signals(domain=domain, company_name=company_name, days_back=days_back, limit=limit)
    return {"domain": domain, "company_name": company_name, "signals_found": len(signals), "signals": signals}


@router.post("/vendors/sec-edgar/pull-funding")
def sec_edgar_pull_funding(days_back: int = 30, limit: int = 50) -> dict[str, Any]:
    """Pull recent SEC Form D filings (startup fundraises). Free, no API key needed."""
    filings = fetch_recent_funding_filings(days_back=days_back, limit=limit)
    results: list[dict[str, Any]] = []
    for f in filings:
        name = f.get("company_name", "")
        if not name:
            continue
        domain_guess = name.lower().replace(" ", "").replace(",", "")[:20] + ".com"
        try:
            result = _auto_ingest({
                "source": "sec_edgar",
                "signal_type": "funding_round",
                "account": {"domain": domain_guess, "name": name},
                "occurred_at": f.get("filed_at") or datetime.now().isoformat(),
                "payload": f,
            })
            results.append(result)
        except Exception as exc:
            results.append({"error": str(exc), "company_name": name})
    return {"filings_found": len(filings), "ingested": len([r for r in results if "signal_id" in r]), "results": results}


@router.get("/vendors/companies/discover")
def discover_companies(
    geography_id: str,
    geography_name: str = "",
    industry: str = "",
    min_employees: int = 0,
    max_employees: int = 0,
    page: int = 1,
    limit: int = 20,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Discover companies in a geographic area using Apollo org search + SEC EDGAR."""
    if not geography_name:
        names = {}
        try:
            from backend.app.services.metrics_engine import _build_geo_name_cache
            names = _build_geo_name_cache([geography_id])
        except Exception:
            pass
        geography_name = names.get(geography_id, geography_id)
    companies, total = discover_companies_for_geography(
        geography_id, geography_name,
        industry=industry, min_employees=min_employees, max_employees=max_employees,
        page=page, limit=limit, force_refresh=force_refresh,
    )
    return {
        "geography_id": geography_id, "geography_name": geography_name,
        "companies": companies, "count": len(companies), "total": total, "page": page,
    }


@router.post("/vendors/companies/enrich")
def enrich_company_waterfall(domain: str) -> dict[str, Any]:
    """Enrich a company by domain. Apollo for firmographics, Hunter for tech stack.
    
    Cost-optimized: no PDL enrichment calls (PDL used only for search).
    """
    merged: dict[str, Any] = {"found": False, "domain": domain, "sources_used": []}

    # Apollo: primary enrichment (firmographics, funding, revenue, description)
    try:
        apollo_data = apollo_org_enrich(domain)
        if apollo_data.get("found"):
            merged.update({k: v for k, v in apollo_data.items() if v is not None and v != ""})
            merged["sources_used"].append("apollo")
    except Exception:
        pass

    # Hunter: tech stack + funding rounds only (free tier, no PDL credits burned)
    try:
        from backend.app.services.vendors.hunter_company import hunter_company_enrich
        hunter_data = hunter_company_enrich(domain)
        if hunter_data.get("found"):
            if hunter_data.get("tech_stack") and not merged.get("tech_stack"):
                merged["tech_stack"] = hunter_data["tech_stack"]
            if hunter_data.get("tech_categories") and not merged.get("tech_categories"):
                merged["tech_categories"] = hunter_data["tech_categories"]
            if hunter_data.get("funding_rounds") and not merged.get("funding_rounds"):
                merged["funding_rounds"] = hunter_data["funding_rounds"]
            if hunter_data.get("total_raised") and not merged.get("total_funding"):
                merged["total_funding"] = hunter_data["total_raised"]
            if hunter_data.get("latest_funding_type") and not merged.get("funding_stage"):
                merged["funding_stage"] = hunter_data["latest_funding_type"]
            if hunter_data.get("description") and not merged.get("short_description"):
                merged["short_description"] = hunter_data["description"]
            for k in ("industry", "employee_count", "city", "country"):
                if hunter_data.get(k) and not merged.get(k):
                    merged[k] = hunter_data[k]
            merged["sources_used"].append("hunter")
    except Exception:
        pass

    merged["found"] = bool(merged.get("name"))

    if merged["found"]:
        account_id = _store.resolve_account_by_identity("domain", domain)
        if account_id:
            updates: dict[str, Any] = {}
            for field in (
                "name", "employee_count", "funding_stage", "total_funding",
                "industry", "country", "linkedin_url", "twitter_url",
            ):
                if merged.get(field):
                    updates[field] = merged[field]
            if updates:
                _store.update_account_fields(account_id, updates)
            merged["account_id"] = account_id

    return merged


@router.get("/vendors/companies/social-signals")
def company_social_signals(
    domain: str,
    company_name: str = "",
    linkedin_url: str = "",
    twitter_url: str = "",
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Get LinkedIn/Twitter post signals and job postings for a company.

    Cached on disk for 24h per normalized domain.  Pass `?force_refresh=true`
    to bypass the cache (burns Apollo + SerpAPI + OpenAI credits).
    """
    return get_company_social_signals(
        domain,
        company_name=company_name,
        linkedin_url=linkedin_url,
        twitter_url=twitter_url,
        force_refresh=force_refresh,
    )


# Cheap in-memory TTL cache so hovering over the same company repeatedly
# doesn't burn API credits.  Keyed by domain.
_CONTACT_PREVIEW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CONTACT_COUNT_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CONTACT_PREVIEW_TTL = 30 * 60  # 30 minutes


@router.get("/vendors/companies/contacts-count")
def company_contacts_count(domain: str) -> dict[str, Any]:
    """FREE contact check — does this company have any public emails?

    Uses Hunter's `email-count` endpoint, which does NOT consume credits.
    Returns only the total count + department breakdown, not actual emails.
    Designed for hover UX where we want to show "N contacts available"
    without burning credits just to browse.
    """
    import time
    from backend.app.services.vendors.hunter_company import hunter_email_count

    d = (domain or "").strip().lower()
    if not d:
        return {"found": False, "domain": "", "total": 0}

    now = time.time()
    hit = _CONTACT_COUNT_CACHE.get(d)
    if hit and now - hit[0] < _CONTACT_PREVIEW_TTL:
        return hit[1]

    try:
        data = hunter_email_count(d)
    except Exception:
        data = {"found": False, "domain": d, "total": 0}

    _CONTACT_COUNT_CACHE[d] = (now, data)
    if len(_CONTACT_COUNT_CACHE) > 5000:
        for k in list(_CONTACT_COUNT_CACHE.keys())[:1000]:
            _CONTACT_COUNT_CACHE.pop(k, None)
    return data


@router.get("/vendors/companies/contacts-preview")
def company_contacts_preview(domain: str, limit: int = 5) -> dict[str, Any]:
    """Quickly check whether a company has public emails on file.

    Returns `{ found, total, contacts: [{ full_name, title, email, confidence }]}`.
    Designed for hover tooltips — aggressively cached in memory, never throws.
    """
    import time
    from backend.app.services.vendors.hunter_company import hunter_domain_search

    d = (domain or "").strip().lower()
    if not d:
        return {"found": False, "domain": "", "total": 0, "contacts": []}

    now = time.time()
    hit = _CONTACT_PREVIEW_CACHE.get(d)
    if hit and now - hit[0] < _CONTACT_PREVIEW_TTL:
        return hit[1]

    try:
        data = hunter_domain_search(d, limit=limit)
    except Exception:
        data = {"found": False, "domain": d, "total": 0, "contacts": []}

    _CONTACT_PREVIEW_CACHE[d] = (now, data)
    # Bound cache size so it can't grow without limit across a long session.
    if len(_CONTACT_PREVIEW_CACHE) > 2000:
        for k in list(_CONTACT_PREVIEW_CACHE.keys())[:500]:
            _CONTACT_PREVIEW_CACHE.pop(k, None)
    return data


# Short-TTL idempotency cache for the intake pipeline.  Calling intake
# N times on the same domain inside this window collapses to a single
# full run; the rest get the cached result instantly so we don't spin
# up Apollo/Hunter/OpenAI six times when a user rapid-clicks the +SDR
# button.  TTL is deliberately short — longer than one network round-
# trip (so accidental double-clicks dedupe) but short enough that
# "re-intake after editing the company record" still works.
import threading as _threading

_INTAKE_IDEMPOTENCY: dict[str, tuple[float, dict[str, Any]]] = {}
_INTAKE_IDEMPOTENCY_TTL = 10.0
# Per-domain in-flight locks.  Serializes TRULY concurrent requests
# (which the timestamp-only cache can't catch because they all miss the
# cache at the same moment) so the pipeline runs exactly once and the
# other N-1 requests block briefly, then read the cached result.
_INTAKE_LOCKS: dict[str, _threading.Lock] = {}
_INTAKE_LOCKS_GUARD = _threading.Lock()


def _intake_idem_key(domain: str) -> str:
    from backend.app.services.worktrigger_store import _norm_domain
    return f"intake::{_norm_domain(domain)}"


def _intake_lock_for(idem_key: str) -> _threading.Lock:
    with _INTAKE_LOCKS_GUARD:
        lock = _INTAKE_LOCKS.get(idem_key)
        if lock is None:
            lock = _threading.Lock()
            _INTAKE_LOCKS[idem_key] = lock
        return lock


@router.post("/vendors/companies/intake")
def intake_company_to_sdr(
    domain: str = "",
    company_name: str = "",
    geography_id: str = "",
) -> dict[str, Any]:
    """One-click: ingest a company into SDR pipeline with auto-enrichment, scoring, and hypothesis."""
    import re
    import time
    if not domain and company_name:
        domain = re.sub(r'[^a-z0-9]', '', company_name.lower())[:30] + ".com"
    if not domain:
        raise HTTPException(status_code=400, detail="Domain or company name required.")
    if len(domain) < 3:
        domain = domain + ".com"
    name = company_name or domain.split(".")[0].title()

    # Idempotency guard: if the same domain was intaken in the last
    # 10 seconds, return the cached response instead of re-running.
    idem_key = _intake_idem_key(domain)
    now_ts = time.time()
    hit = _INTAKE_IDEMPOTENCY.get(idem_key)
    if hit and now_ts - hit[0] < _INTAKE_IDEMPOTENCY_TTL:
        cached = dict(hit[1])
        cached["deduped"] = True
        return cached

    # Per-domain lock: if a concurrent call is already running the
    # pipeline for this domain, wait for it and re-use its result.
    lock = _intake_lock_for(idem_key)
    acquired = lock.acquire(timeout=60)
    if not acquired:
        raise HTTPException(status_code=503, detail="Intake timed out waiting for prior call.")
    try:
        # Re-check the cache inside the lock: the first request may have
        # just finished populating it while we were blocked.
        hit = _INTAKE_IDEMPOTENCY.get(idem_key)
        if hit and time.time() - hit[0] < _INTAKE_IDEMPOTENCY_TTL:
            cached = dict(hit[1])
            cached["deduped"] = True
            return cached
        return _run_intake_pipeline(domain=domain, name=name, geography_id=geography_id, idem_key=idem_key, now_ts=now_ts)
    finally:
        lock.release()


def _run_intake_pipeline(
    *,
    domain: str,
    name: str,
    geography_id: str,
    idem_key: str,
    now_ts: float,
) -> dict[str, Any]:
    """Actual pipeline work.  Called while holding the per-domain lock so
    concurrent requests serialize instead of running 6 copies of Apollo
    enrich + hypothesis + draft generation.  Extracted into its own
    function purely for readability — previously this block was inline
    inside `intake_company_to_sdr` with a 60-line try/except ladder."""
    import time
    results: dict[str, Any] = {"domain": domain, "steps": []}
    now_iso = datetime.now().isoformat()

    try:
        ingest_result = _auto_ingest({
            "source": "company_discovery",
            "signal_type": "buyer_intent",
            "account": {"domain": domain, "name": name},
            "occurred_at": now_iso,
            "payload": {"geography_id": geography_id, "source": "map_click"},
        })
        account_id = ingest_result.get("account_id", "")
        results["account_id"] = account_id
        results["signal_id"] = ingest_result.get("signal_id", "")
        results["steps"].append({"step": "ingest", "status": "ok"})
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Ingest failed: {exc}")

    enrichment: dict[str, Any] = {}
    try:
        enrichment = apollo_org_enrich(domain)
        if enrichment.get("found"):
            updates: dict[str, Any] = {}
            for field in (
                "name", "employee_count", "funding_stage", "total_funding",
                "industry", "country", "linkedin_url", "twitter_url",
            ):
                if enrichment.get(field):
                    updates[field] = enrichment[field]
            if updates:
                _store.update_account_fields(account_id, updates)

            _generate_signals_from_enrichment(account_id, domain, enrichment, now_iso)
            results["steps"].append({"step": "enrich", "status": "ok", "fields": list(updates.keys())})
        else:
            results["steps"].append({"step": "enrich", "status": "skipped", "reason": "not found in Apollo"})
    except Exception as exc:
        results["steps"].append({"step": "enrich", "status": "error", "error": str(exc)})

    try:
        _service.recompute_account_score(account_id)
        results["steps"].append({"step": "score", "status": "ok"})
    except Exception as exc:
        results["steps"].append({"step": "score", "status": "error", "error": str(exc)})

    try:
        _service.generate_work_hypothesis(account_id)
        results["steps"].append({"step": "hypothesis", "status": "ok"})
    except Exception as exc:
        results["steps"].append({"step": "hypothesis", "status": "error", "error": str(exc)})

    try:
        contacts = _store.list_contacts(account_id)
        if not contacts:
            try:
                waterfall_contacts = enrich_contacts_waterfall(domain, limit=5)
                for c in waterfall_contacts:
                    _store.upsert_contact(account_id, c)
            except Exception:
                pass

        contacts = _store.list_contacts(account_id)
        if not contacts:
            try:
                from backend.app.services.vendors.contact_waterfall import apollo_search_contacts_by_title
                title_contacts = apollo_search_contacts_by_title(domain, ["CEO", "CTO", "COO", "VP", "Director", "Founder", "Head of"], limit=5)
                for c in title_contacts:
                    _store.upsert_contact(account_id, c)
            except Exception:
                pass

        # Step 3: Hunter domain-search — pulls REAL contact rows (name +
        # title + email + confidence) from Hunter's index.  Apollo's
        # people DB skews enterprise; Hunter has much better coverage
        # for SMBs (clinics, agencies, local pros) where Apollo returns
        # zero.  This fixes the bug where the +SDR hover preview shows
        # "2 emails available" but the pipeline arrives with 0 contacts.
        #
        # Cost gate: only fires when (a) prior Apollo steps found 0
        # AND (b) the FREE Hunter email-count cache already confirms
        # emails actually exist.  Avoids wasting a Hunter request on
        # domains we know are empty.
        contacts = _store.list_contacts(account_id)
        if not contacts:
            try:
                from backend.app.services.vendors.hunter_company import (
                    hunter_domain_search, hunter_email_count,
                )
                count_data = hunter_email_count(domain)
                if count_data.get("found") and (count_data.get("total") or 0) > 0:
                    domain_search = hunter_domain_search(domain, limit=5)
                    found_contacts = domain_search.get("contacts") or []
                    from backend.app.services.vendors.contact_waterfall import _infer_persona
                    for hc in found_contacts:
                        if not (hc.get("email") and "@" in str(hc["email"])):
                            continue
                        confidence = float(hc.get("confidence") or 0) / 100.0
                        _store.upsert_contact(account_id, {
                            "full_name": hc.get("full_name") or hc.get("email", "").split("@")[0],
                            "title": hc.get("title") or "",
                            "email": hc["email"],
                            "email_status": "valid" if confidence >= 0.8 else "risky",
                            "linkedin_url": "",
                            "persona_type": _infer_persona(hc.get("title") or ""),
                            "confidence_score": confidence or 0.5,
                            "source": "hunter_domain_search",
                        })
            except Exception:
                pass

        # Intentionally NO last-resort step that creates a phoneless or
        # emailless "Main Contact" placeholder.  Persisting an unmessage-
        # able contact card creates more confusion than value: the SDR
        # cannot send to it, draft generation cannot run on it, and the
        # account looks "covered" when in reality it is not.  When all
        # real-email paths above fail, leave the account with 0 contacts
        # so the UI can correctly prompt the SDR to add one manually.

        contacts = _store.list_contacts(account_id)
        results["contacts_count"] = len(contacts)
        results["steps"].append({"step": "contacts", "status": "ok", "count": len(contacts)})
    except Exception as exc:
        results["steps"].append({"step": "contacts", "status": "error", "error": str(exc)})

    try:
        contacts = _store.list_contacts(account_id)
        hypotheses = _store.list_work_hypotheses(account_id)
        best_contact = next((c for c in contacts if c.get("email") and "@" in str(c["email"])), None)
        if best_contact and hypotheses:
            # Default new intakes (talent-map + Universal Search + bulk batch
            # all funnel through here) to job-listing reference mode so the
            # very first draft anchors on a currently-open role at the
            # company.  `_pick_target_job` will pull from cache, then fall
            # back to a direct Apollo job-postings call (cheap, no
            # SerpAPI/LLM).  If Apollo returns zero jobs for the domain,
            # `generate_draft` itself transparently downgrades the mode
            # back to "default" — so this never produces a broken draft.
            draft_id = _service.generate_draft(
                account_id=account_id,
                contact_id=best_contact["id"],
                work_hypothesis_id=hypotheses[0]["id"],
                channel="email",
                outreach_mode="job_listing",
            )
            # Surface which mode actually landed (job_listing vs default
            # fallback) and which role got referenced — useful both for
            # logs and for the frontend to show the "Targeting: <role>"
            # pill on freshly-intaken accounts without an extra round-trip.
            try:
                final_draft = _store.get_draft(draft_id) if draft_id else None
            except Exception:
                final_draft = None
            results["steps"].append({
                "step": "draft",
                "status": "ok",
                "draft_id": draft_id,
                "outreach_mode": (final_draft or {}).get("outreach_mode"),
                "target_job_title": (final_draft or {}).get("target_job_title"),
            })
        else:
            reason = "no contact with email" if not best_contact else "no hypothesis"
            results["steps"].append({"step": "draft", "status": "skipped", "reason": reason})
    except Exception as exc:
        results["steps"].append({"step": "draft", "status": "error", "error": str(exc)})

    # Store under idempotency cache so rapid re-clicks collapse.
    # Timestamp uses the CURRENT time (not the pre-lock capture) so
    # the 10-second TTL starts when the pipeline actually completed.
    # Otherwise a slow 15-second pipeline would already be past TTL
    # before any follow-up click could benefit from the cache.
    _INTAKE_IDEMPOTENCY[idem_key] = (time.time(), results)
    # Bound the cache so it can't grow forever in long-lived processes.
    if len(_INTAKE_IDEMPOTENCY) > 500:
        oldest = sorted(_INTAKE_IDEMPOTENCY.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _INTAKE_IDEMPOTENCY.pop(k, None)

    return results


# Async batch intake state.  Each batch_id maps to a snapshot the
# frontend polls every second.  Lives in process memory only — batches
# are ephemeral by design (a multi-thousand intake is a single SDR
# session).  Bounded so a runaway client can't exhaust memory.
_BATCH_STATE: dict[str, dict[str, Any]] = {}
_BATCH_STATE_LOCK = _threading.Lock()
_BATCH_MAX_KEPT = 64
_BATCH_MAX_ITEMS = 1000          # hard cap per request — prevents OOMs
_BATCH_MAX_CONCURRENCY = 4       # parallel pipelines; Apollo + Hunter rate-limits friendly


def _normalize_intake_item(item: dict[str, Any]) -> tuple[str, str, str]:
    """Apply the same domain heuristics the single-intake endpoint uses,
    so the batch validates each item before kicking off any work."""
    import re as _re
    domain = str(item.get("domain") or "").strip()
    name = str(item.get("company_name") or "").strip()
    geo = str(item.get("geography_id") or "")
    if not domain and name:
        domain = _re.sub(r"[^a-z0-9]", "", name.lower())[:30] + ".com"
    if domain and len(domain) < 3:
        domain += ".com"
    return domain, name, geo


def _process_batch_item(domain: str, name: str, geo: str) -> dict[str, Any]:
    """Run a single intake.  Returns a result dict shaped like the
    legacy synchronous response so the existing UI parser keeps working."""
    if not domain and not name:
        return {"company_name": name, "domain": domain, "status": "skipped", "reason": "no domain or name"}
    existing = _store.resolve_account_by_identity("domain", domain)
    if existing:
        return {
            "company_name": name, "domain": domain, "status": "skipped",
            "reason": "already in pipeline", "account_id": existing,
        }
    try:
        r = intake_company_to_sdr(domain=domain, company_name=name, geography_id=geo)
        ok_steps = len([s for s in r.get("steps", []) if s.get("status") == "ok"])
        return {
            "company_name": name, "domain": domain, "status": "ok",
            "account_id": r.get("account_id"), "steps_ok": ok_steps,
        }
    except Exception as exc:
        return {"company_name": name, "domain": domain, "status": "error", "error": str(exc)[:200]}


def _run_batch_worker(batch_id: str, items: list[tuple[str, str, str]]) -> None:
    """Background worker: drains items concurrently, updates batch state
    after each one so the polling endpoint can stream progress."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time as _time

    started = _time.time()

    def _set(updates: dict[str, Any]) -> None:
        with _BATCH_STATE_LOCK:
            state = _BATCH_STATE.get(batch_id)
            if state is None:
                return
            state.update(updates)

    def _append(field: str, val: Any) -> None:
        with _BATCH_STATE_LOCK:
            state = _BATCH_STATE.get(batch_id)
            if state is None:
                return
            state.setdefault(field, []).append(val)
            state["last_updated"] = _utc_now_iso()

    def _do_one(idx: int, domain: str, name: str, geo: str) -> dict[str, Any]:
        result = _process_batch_item(domain, name, geo)
        _append("results", result)
        with _BATCH_STATE_LOCK:
            state = _BATCH_STATE.get(batch_id)
            if state is not None:
                bucket = result.get("status", "error")
                state[bucket] = int(state.get(bucket, 0)) + 1
                state["completed"] = int(state.get("completed", 0)) + 1
                state["last_updated"] = _utc_now_iso()
        return result

    _set({"status": "running"})
    try:
        with ThreadPoolExecutor(max_workers=_BATCH_MAX_CONCURRENCY) as pool:
            futures = [
                pool.submit(_do_one, i, dom, nm, geo)
                for i, (dom, nm, geo) in enumerate(items)
            ]
            for _ in as_completed(futures):
                pass
    finally:
        _set({
            "status": "complete",
            "elapsed_seconds": round(_time.time() - started, 1),
            "finished_at": _utc_now_iso(),
        })


def _utc_now_iso() -> str:
    return datetime.now().isoformat()


@router.post("/vendors/companies/intake-batch")
def intake_batch(payload: list[dict[str, Any]] = Body(...)) -> dict[str, Any]:
    """Async batch intake — returns immediately with a batch_id; the
    frontend polls ``/intake-batch/{batch_id}`` for progress.

    Replaces the prior synchronous loop, which (a) capped silently at
    50 items and (b) blocked one HTTP request for 8–17 minutes on a
    50-item batch, causing proxy timeouts that lost everything past
    the first finished item.

    Each item: ``{"domain": "...", "company_name": "...", "geography_id": "..."}``.
    Empty/duplicate items are recorded as skipped without burning a
    pipeline run.  Concurrency is bounded so we don't trip Apollo or
    Hunter rate limits.
    """
    if len(payload) > _BATCH_MAX_ITEMS:
        raise HTTPException(
            status_code=413,
            detail=f"Batch too large ({len(payload)} > {_BATCH_MAX_ITEMS}). Split into smaller batches.",
        )

    # Pre-validate + dedupe at submit time so the user gets immediate
    # feedback for "no domain"/"duplicate" before we kick off work.
    seen_domains: set[str] = set()
    items: list[tuple[str, str, str]] = []
    pre_results: list[dict[str, Any]] = []
    pre_skipped = 0
    for item in payload:
        domain, name, geo = _normalize_intake_item(item)
        if not domain and not name:
            pre_results.append({"company_name": name, "domain": domain, "status": "skipped", "reason": "no domain or name"})
            pre_skipped += 1
            continue
        dedup_key = domain.lower()
        if dedup_key in seen_domains:
            pre_results.append({"company_name": name, "domain": domain, "status": "skipped", "reason": "duplicate"})
            pre_skipped += 1
            continue
        seen_domains.add(dedup_key)
        items.append((domain, name, geo))

    batch_id = f"batch_{uuid4().hex[:12]}"
    started_at = _utc_now_iso()

    with _BATCH_STATE_LOCK:
        # Garbage-collect oldest batches if we're about to exceed cap.
        if len(_BATCH_STATE) >= _BATCH_MAX_KEPT:
            for k in sorted(_BATCH_STATE, key=lambda b: _BATCH_STATE[b].get("started_at", ""))[:8]:
                _BATCH_STATE.pop(k, None)
        _BATCH_STATE[batch_id] = {
            "batch_id": batch_id,
            "status": "queued",
            "total_submitted": len(payload),
            "total_runnable": len(items),
            "completed": pre_skipped,           # pre-skips count as already done
            "skipped": pre_skipped,
            "ok": 0,
            "error": 0,
            "results": list(pre_results),       # seed with pre-validation rows
            "started_at": started_at,
            "last_updated": started_at,
        }

    # Background thread runs the actual pipeline.  Daemon so a process
    # exit doesn't wait on it; the in-memory state is ephemeral anyway.
    if items:
        worker = _threading.Thread(
            target=_run_batch_worker,
            args=(batch_id, items),
            daemon=True,
            name=f"intake_batch_{batch_id[:8]}",
        )
        worker.start()
    else:
        # Nothing to do — mark complete immediately.
        with _BATCH_STATE_LOCK:
            state = _BATCH_STATE[batch_id]
            state["status"] = "complete"
            state["finished_at"] = _utc_now_iso()
            state["elapsed_seconds"] = 0.0

    with _BATCH_STATE_LOCK:
        return dict(_BATCH_STATE[batch_id])


@router.get("/vendors/companies/intake-batch/{batch_id}")
def intake_batch_status(batch_id: str) -> dict[str, Any]:
    """Polling endpoint for an async batch.  Frontend hits this every
    ~1-2 seconds while the batch is running to show per-item progress."""
    with _BATCH_STATE_LOCK:
        state = _BATCH_STATE.get(batch_id)
        if state is None:
            raise HTTPException(status_code=404, detail=f"Unknown batch_id={batch_id}")
        return dict(state)


@router.delete("/vendors/companies/intake-batch/{batch_id}")
def forget_intake_batch(batch_id: str) -> dict[str, Any]:
    """Drop a finished batch from memory once the frontend acknowledges
    it.  Optional — the GC also evicts the oldest batches when the
    cap is reached — but useful when a power user is firing many
    consecutive batches in one session."""
    with _BATCH_STATE_LOCK:
        existed = _BATCH_STATE.pop(batch_id, None) is not None
    return {"batch_id": batch_id, "removed": existed}


@router.post("/vendors/linkedin/search")
def linkedin_search(query: str, limit: int = 10) -> list[dict[str, Any]]:
    """Search LinkedIn Sales Navigator for accounts."""
    return li_search_accounts(query, limit=limit)


@router.post("/vendors/linkedin/enrich")
def linkedin_enrich(linkedin_account_id: str) -> dict[str, Any]:
    """Fetch account insights from LinkedIn Sales Navigator and ingest as signal."""
    insights = get_account_insights(linkedin_account_id)
    normalized = normalize_linkedin_intent(insights)
    result = _auto_ingest(normalized)
    return {**result, "insights": insights}


@router.post("/vendors/contacts/enrich-waterfall")
def contacts_enrich_waterfall(
    account_id: str,
    limit: int = 5,
    verify_with_findymail: bool = True,
    fallback_to_hunter: bool = True,
) -> dict[str, Any]:
    """Run Apollo -> Findymail -> Hunter waterfall for an account and store contacts."""
    try:
        account = _store.get_account(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    domain = str(account.get("domain") or "").strip()
    if not domain:
        raise HTTPException(status_code=400, detail="Account has no domain for enrichment.")
    contacts = enrich_contacts_waterfall(
        domain,
        limit=limit,
        verify_with_findymail=verify_with_findymail,
        fallback_to_hunter=fallback_to_hunter,
    )
    for c in contacts:
        _store.upsert_contact(account_id, c)
    all_contacts = _store.list_contacts(account_id)
    return {
        "account_id": account_id,
        "domain": domain,
        "contacts_enriched": len(contacts),
        "contacts_total": len(all_contacts),
        "best_contact_id": all_contacts[0]["id"] if all_contacts else None,
    }


@router.post("/vendors/contacts/search-by-title")
def search_contacts_by_title(
    account_id: str,
    titles: str = "CEO,CTO,VP,Director",
    limit: int = 10,
) -> dict[str, Any]:
    """Search Apollo for contacts at an account's domain filtered to specific job titles."""
    try:
        account = _store.get_account(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    domain = str(account.get("domain") or "").strip()
    if not domain:
        raise HTTPException(status_code=400, detail="Account has no domain.")
    title_list = [t.strip() for t in titles.split(",") if t.strip()]
    contacts = apollo_search_contacts_by_title(domain, title_list, limit=limit)
    for c in contacts:
        _store.upsert_contact(account_id, c)
    all_contacts = _store.list_contacts(account_id)
    return {
        "account_id": account_id,
        "searched_titles": title_list,
        "new_contacts": len(contacts),
        "contacts_total": len(all_contacts),
        "contacts": all_contacts,
    }


@router.post("/accounts/{account_id}/contacts/add")
def add_manual_contact(
    account_id: str,
    full_name: str = "",
    email: str = "",
    title: str = "",
) -> dict[str, Any]:
    """Manually add a contact to an account."""
    try:
        _store.get_account(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not email and not full_name:
        raise HTTPException(status_code=400, detail="Provide at least email or full_name.")
    contact = {
        "full_name": full_name or email.split("@")[0],
        "email": email,
        "title": title,
        "email_status": "unknown",
        "source": "manual",
        "confidence_score": 0.6,
    }
    _store.upsert_contact(account_id, contact)
    return {"status": "added", "contact": contact}


@router.delete("/accounts/{account_id}/contacts/{contact_id}")
def delete_contact(account_id: str, contact_id: str) -> dict[str, str]:
    """Delete a contact from an account."""
    try:
        _store.get_account(account_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _store.delete_contact(contact_id)
    return {"status": "deleted", "contact_id": contact_id}


@router.get("/vendors/status")
def vendor_status() -> dict[str, Any]:
    """Report which vendor API keys are configured."""
    import os
    keys = {
        "clay": "CLAY_API_KEY",
        "commonroom": "COMMONROOM_API_TOKEN",
        "crunchbase": "CRUNCHBASE_API_KEY",
        "linkedin_sales_nav": "LINKEDIN_SALES_NAV_TOKEN",
        "apollo": "APOLLO_API_KEY",
        "findymail": "FINDYMAIL_API_KEY",
        "hunter": "HUNTER_API_KEY",
        "openai": "OPENAI_API_KEY",
        "resend": "RESEND_API_KEY",
        "hubspot": "HUBSPOT_PRIVATE_APP_TOKEN",
    }
    result: dict[str, Any] = {
        name: {"configured": bool(os.getenv(env_var, "").strip()), "env_var": env_var}
        for name, env_var in keys.items()
    }
    result["sec_edgar"] = {"configured": True, "env_var": "(free, no key needed)"}
    return result
