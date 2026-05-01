from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl


DraftStatus = Literal[
    "created",
    "draft_ready",
    "under_review",
    "approved",
    "sent",
    "replied",
    "discarded",
    "snoozed",
]

ReviewAction = Literal[
    "approve",
    "unapprove",
    "edit_and_approve",
    "discard",
    "snooze",
    "reroute_contact",
    "reroute_angle",
]


class AccountRecord(BaseModel):
    id: str
    domain: str
    name: str | None = None
    linkedin_url: HttpUrl | None = None
    crunchbase_id: str | None = None
    industry: str | None = None
    employee_count: int | None = None
    funding_stage: str | None = None
    total_funding: float | None = None
    country: str | None = None
    icp_status: Literal["unknown", "pass", "fail"] = "unknown"
    created_at: datetime
    updated_at: datetime


class ContactRecord(BaseModel):
    id: str
    account_id: str
    full_name: str | None = None
    title: str | None = None
    linkedin_url: HttpUrl | None = None
    email: str | None = None
    email_status: Literal["unknown", "valid", "invalid", "risky"] = "unknown"
    persona_type: str | None = None
    confidence_score: float = Field(default=0.0, ge=0.0, le=1.0)
    source: str | None = None
    created_at: datetime
    updated_at: datetime


class SignalIngestAccount(BaseModel):
    domain: str = Field(min_length=3)
    name: str | None = None
    linkedin_company_id: str | None = None
    crunchbase_uuid: str | None = None
    hubspot_company_id: str | None = None
    headquarters_geo_id: str | None = None
    locations: list[dict[str, Any]] = Field(default_factory=list)


class SignalIngestRequest(BaseModel):
    source: str = Field(min_length=2)
    signal_type: str = Field(min_length=2)
    account: SignalIngestAccount
    occurred_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class SignalIngestResponse(BaseModel):
    signal_id: str
    account_id: str
    status: Literal["accepted", "duplicate"]


class AccountScoreResponse(BaseModel):
    account_id: str
    qualified: bool
    icp_fit_score: float = Field(ge=0.0, le=100.0)
    signal_score: float = Field(ge=0.0, le=100.0)
    work_fit_score: float = Field(ge=0.0, le=100.0)
    priority_score: float = Field(ge=0.0, le=100.0)
    geo_attribution: list[dict[str, Any]] = Field(default_factory=list)
    rationale: list[str]


class WorkHypothesisResponse(BaseModel):
    work_hypothesis_id: str
    account_id: str
    probable_problem: str
    probable_deliverable: str
    talent_archetype: str
    urgency_score: float = Field(ge=0.0, le=100.0)
    taskability_score: float = Field(ge=0.0, le=100.0)
    fit_score: float = Field(ge=0.0, le=100.0)
    confidence_score: float = Field(ge=0.0, le=1.0)
    rationale: list[str]
    created_at: datetime


class ContactEnrichRequest(BaseModel):
    contacts: list[dict[str, Any]] = Field(default_factory=list)


class ContactEnrichResponse(BaseModel):
    contacts_found: int
    best_contact_id: str | None = None


class DraftGenerateRequest(BaseModel):
    account_id: str
    contact_id: str
    work_hypothesis_id: str
    channel: Literal["email", "linkedin"] = "email"
    template_id: str | None = None


class DraftGenerateResponse(BaseModel):
    draft_id: str
    status: DraftStatus


class EmailTemplateCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    subject_a: str = Field(min_length=1, max_length=240)
    subject_b: str = Field(default="", max_length=240)
    email_body: str = Field(min_length=1, max_length=8000)
    followup_body: str = Field(default="", max_length=4000)
    linkedin_dm: str = Field(default="", max_length=2000)


class EmailTemplateUpdateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    subject_a: str = Field(min_length=1, max_length=240)
    subject_b: str = Field(default="", max_length=240)
    email_body: str = Field(min_length=1, max_length=8000)
    followup_body: str = Field(default="", max_length=4000)
    linkedin_dm: str = Field(default="", max_length=2000)


class EmailTemplateResponse(BaseModel):
    id: str
    name: str
    subject_a: str
    subject_b: str
    email_body: str
    followup_body: str
    linkedin_dm: str
    created_at: datetime
    updated_at: datetime


class ReviewRequest(BaseModel):
    action: ReviewAction
    reviewer_user_id: str
    edited_subject: str | None = None
    edited_body: str | None = None
    reason_code: str | None = None
    notes: str | None = None


class SendResponse(BaseModel):
    message_id: str
    status: Literal["sent"]


class ReplyClassifyRequest(BaseModel):
    draft_id: str
    reply_text: str = Field(min_length=1)
    thread_metadata: dict[str, Any] = Field(default_factory=dict)


class ReplyClassifyResponse(BaseModel):
    classification: Literal[
        "positive_interest",
        "objection",
        "referral",
        "not_now",
        "unsubscribe",
        "irrelevant",
    ]
    confidence: float = Field(ge=0.0, le=1.0)
    next_action: str


class ScopingBriefResponse(BaseModel):
    scoping_brief_id: str
    opportunity_id: str
    summary: str
    likely_pain_points: list[str]
    proposed_work_packages: list[dict[str, Any]]
    suggested_talent_archetypes: list[str]
    discovery_questions: list[str]


class JobEnqueueRequest(BaseModel):
    job_type: Literal[
        "enrich_contacts",
        "generate_hypothesis",
        "generate_draft",
        "send_draft",
        "sync_crm",
        "classify_reply",
        "create_scoping_brief",
    ]
    payload: dict[str, Any]
    idempotency_key: str
    max_attempts: int = Field(default=5, ge=1, le=20)


class JobRecordResponse(BaseModel):
    job_id: str
    job_type: str
    status: Literal["queued", "in_progress", "completed", "failed", "dead_letter"]
    attempts: int
    max_attempts: int
    run_after: datetime
    payload: dict[str, Any]
    last_error: str | None = None
