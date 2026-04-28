from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _norm_domain(domain: str) -> str:
    out = domain.strip().lower()
    if out.startswith("http://"):
        out = out[7:]
    if out.startswith("https://"):
        out = out[8:]
    return out.strip("/ ")


@dataclass(frozen=True)
class IdempotencyRecord:
    key: str
    endpoint: str
    response_json: dict[str, Any]


class WorkTriggerStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._bootstrap()

    @staticmethod
    def _migrate_add_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        """Idempotent ``ALTER TABLE ... ADD COLUMN`` for existing databases.

        SQLite does not support ``ADD COLUMN IF NOT EXISTS``, so we
        introspect the schema first and skip the migration when the
        column is already present.  This lets us roll forward new
        per-account fields without dropping the DB on every release.
        """
        try:
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        except sqlite3.Error:
            return
        if not cols or column in cols:
            return
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        except sqlite3.OperationalError:
            # Concurrent migration on another connection — safe to ignore.
            pass

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _bootstrap(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            # Default the column to ON for fresh databases.  Existing
            # databases that were initialized with DEFAULT 0 are
            # patched below via the data migration.
            self._migrate_add_column(conn, "wt_accounts", "job_outreach_enabled", "INTEGER NOT NULL DEFAULT 1")
            self._migrate_add_column(conn, "wt_accounts", "job_outreach_updated_at", "TEXT")
            self._migrate_add_column(conn, "wt_accounts", "twitter_url", "TEXT")
            self._migrate_add_column(conn, "wt_outreach_drafts", "target_job_title", "TEXT")
            self._migrate_add_column(conn, "wt_outreach_drafts", "target_job_url", "TEXT")
            self._migrate_add_column(conn, "wt_outreach_drafts", "outreach_mode", "TEXT")
            # Data migration: any account that has NEVER been touched
            # by the user (job_outreach_updated_at IS NULL) is flipped
            # to ON.  Accounts where the SDR explicitly toggled — even
            # to OFF — keep their setting because their timestamp is
            # populated, so we never overwrite an explicit choice.
            try:
                conn.execute(
                    "UPDATE wt_accounts SET job_outreach_enabled = 1 "
                    "WHERE job_outreach_updated_at IS NULL AND job_outreach_enabled = 0"
                )
            except sqlite3.OperationalError:
                pass
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS wt_accounts (
                    id TEXT PRIMARY KEY,
                    domain TEXT NOT NULL UNIQUE,
                    name TEXT,
                    linkedin_url TEXT,
                    twitter_url TEXT,
                    crunchbase_id TEXT,
                    industry TEXT,
                    employee_count INTEGER,
                    funding_stage TEXT,
                    total_funding REAL,
                    country TEXT,
                    icp_status TEXT NOT NULL DEFAULT 'unknown',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_contacts (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    full_name TEXT,
                    title TEXT,
                    linkedin_url TEXT,
                    email TEXT,
                    email_status TEXT NOT NULL DEFAULT 'unknown',
                    persona_type TEXT,
                    confidence_score REAL NOT NULL DEFAULT 0.0,
                    source TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_contacts_account_id ON wt_contacts(account_id);
                CREATE INDEX IF NOT EXISTS idx_wt_contacts_email ON wt_contacts(email);

                CREATE TABLE IF NOT EXISTS wt_signals (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    signal_type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    raw_payload_json TEXT NOT NULL,
                    normalized_payload_json TEXT NOT NULL,
                    confidence_score REAL NOT NULL,
                    dedupe_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_signals_account_occurred ON wt_signals(account_id, occurred_at DESC);

                CREATE TABLE IF NOT EXISTS wt_signal_stacks (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    stack_window_start TEXT NOT NULL,
                    stack_window_end TEXT NOT NULL,
                    funding_score REAL NOT NULL DEFAULT 0,
                    buyer_intent_score REAL NOT NULL DEFAULT 0,
                    hiring_score REAL NOT NULL DEFAULT 0,
                    web_intent_score REAL NOT NULL DEFAULT 0,
                    exec_change_score REAL NOT NULL DEFAULT 0,
                    total_signal_score REAL NOT NULL,
                    explanation_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_work_hypotheses (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    signal_stack_id TEXT NOT NULL REFERENCES wt_signal_stacks(id) ON DELETE CASCADE,
                    probable_problem TEXT NOT NULL,
                    probable_deliverable TEXT NOT NULL,
                    talent_archetype TEXT NOT NULL,
                    urgency_score REAL NOT NULL,
                    taskability_score REAL NOT NULL,
                    fit_score REAL NOT NULL,
                    confidence_score REAL NOT NULL,
                    rationale_json TEXT NOT NULL,
                    generated_by_model TEXT NOT NULL,
                    model_version TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_outreach_drafts (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    contact_id TEXT NOT NULL REFERENCES wt_contacts(id) ON DELETE CASCADE,
                    work_hypothesis_id TEXT NOT NULL REFERENCES wt_work_hypotheses(id) ON DELETE CASCADE,
                    channel TEXT NOT NULL,
                    subject_a TEXT,
                    subject_b TEXT,
                    email_body TEXT,
                    followup_body TEXT,
                    linkedin_dm TEXT,
                    status TEXT NOT NULL DEFAULT 'draft_ready',
                    generation_metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_drafts_status ON wt_outreach_drafts(status);

                CREATE TABLE IF NOT EXISTS wt_review_decisions (
                    id TEXT PRIMARY KEY,
                    draft_id TEXT NOT NULL REFERENCES wt_outreach_drafts(id) ON DELETE CASCADE,
                    reviewer_user_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    edited_body TEXT,
                    edited_subject TEXT,
                    reason_code TEXT,
                    notes TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_opportunities (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    contact_id TEXT NOT NULL REFERENCES wt_contacts(id) ON DELETE CASCADE,
                    source_draft_id TEXT REFERENCES wt_outreach_drafts(id),
                    crm_id TEXT,
                    stage TEXT NOT NULL DEFAULT 'new',
                    positive_reply_at TEXT,
                    owner_user_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_scoping_briefs (
                    id TEXT PRIMARY KEY,
                    opportunity_id TEXT NOT NULL REFERENCES wt_opportunities(id) ON DELETE CASCADE,
                    summary TEXT NOT NULL,
                    likely_pain_points_json TEXT NOT NULL,
                    proposed_work_packages_json TEXT NOT NULL,
                    suggested_talent_archetypes_json TEXT NOT NULL,
                    discovery_questions_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_idempotency (
                    id TEXT PRIMARY KEY,
                    endpoint TEXT NOT NULL,
                    key TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(endpoint, key)
                );

                CREATE TABLE IF NOT EXISTS wt_account_identity (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    identity_type TEXT NOT NULL,
                    identity_value TEXT NOT NULL,
                    confidence_score REAL NOT NULL DEFAULT 1.0,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(identity_type, identity_value)
                );
                CREATE INDEX IF NOT EXISTS idx_wt_account_identity_account ON wt_account_identity(account_id);

                CREATE TABLE IF NOT EXISTS wt_account_geo_attribution (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    geography_id TEXT NOT NULL,
                    weight REAL NOT NULL,
                    evidence TEXT NOT NULL,
                    confidence_score REAL NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_account_geo_attribution_account ON wt_account_geo_attribution(account_id);

                CREATE TABLE IF NOT EXISTS wt_jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 5,
                    run_after TEXT NOT NULL,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_jobs_status_run_after ON wt_jobs(status, run_after);

                CREATE TABLE IF NOT EXISTS wt_dead_letters (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    failed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_suppressions (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    reason TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_crm_sync_events (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    contact_id TEXT,
                    opportunity_id TEXT,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_crm_sync_events_account ON wt_crm_sync_events(account_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS wt_worker_heartbeats (
                    worker_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_result_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_identity_events (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    identity_type TEXT NOT NULL,
                    identity_value TEXT NOT NULL,
                    source TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_identity_events_account ON wt_identity_events(account_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS wt_crm_conflicts (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    contact_id TEXT,
                    opportunity_id TEXT,
                    field_name TEXT NOT NULL,
                    app_value TEXT,
                    crm_value TEXT,
                    policy TEXT NOT NULL,
                    resolution_status TEXT NOT NULL DEFAULT 'open',
                    resolved_by TEXT,
                    resolved_value TEXT,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_wt_crm_conflicts_open ON wt_crm_conflicts(resolution_status, created_at DESC);

                CREATE TABLE IF NOT EXISTS wt_consent_records (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    legal_basis TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(email, channel)
                );

                CREATE TABLE IF NOT EXISTS wt_deletion_requests (
                    id TEXT PRIMARY KEY,
                    email TEXT,
                    account_id TEXT,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'requested',
                    requested_by TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS wt_retention_policies (
                    entity_type TEXT PRIMARY KEY,
                    retention_days INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_llm_runs (
                    id TEXT PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    prompt_hash TEXT NOT NULL,
                    token_budget INTEGER NOT NULL,
                    prompt_tokens INTEGER,
                    completion_tokens INTEGER,
                    cached_hit INTEGER NOT NULL DEFAULT 0,
                    evidence_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_llm_runs_task ON wt_llm_runs(task_name, created_at DESC);

                CREATE TABLE IF NOT EXISTS wt_llm_cache (
                    cache_key TEXT PRIMARY KEY,
                    response_json TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_feedback_events (
                    id TEXT PRIMARY KEY,
                    account_id TEXT,
                    draft_id TEXT,
                    event_type TEXT NOT NULL,
                    value_num REAL,
                    value_text TEXT,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_quotes (
                    id TEXT PRIMARY KEY,
                    opportunity_id TEXT NOT NULL,
                    quote_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_talent_shortlists (
                    id TEXT PRIMARY KEY,
                    opportunity_id TEXT NOT NULL,
                    geography_id TEXT,
                    candidates_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wt_staffing_workflows (
                    id TEXT PRIMARY KEY,
                    opportunity_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    owner_user_id TEXT,
                    checklist_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                -- Per-account conversational chat (ChatGPT-style) with
                -- tool-use support (SerpAPI web_search).  Sessions persist
                -- forever; messages store the full transcript for context.
                CREATE TABLE IF NOT EXISTS wt_chat_sessions (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES wt_accounts(id) ON DELETE CASCADE,
                    title TEXT NOT NULL DEFAULT 'New conversation',
                    model TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_chat_sessions_account ON wt_chat_sessions(account_id, updated_at DESC);

                CREATE TABLE IF NOT EXISTS wt_chat_messages (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES wt_chat_sessions(id) ON DELETE CASCADE,
                    role TEXT NOT NULL,             -- system | user | assistant | tool
                    content TEXT NOT NULL,
                    tool_calls_json TEXT,           -- when assistant calls tools
                    tool_call_id TEXT,              -- when role='tool'
                    tool_name TEXT,                 -- when role='tool'
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_wt_chat_messages_session ON wt_chat_messages(session_id, created_at);
                """
            )

    def get_idempotency(self, endpoint: str, key: str) -> IdempotencyRecord | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT endpoint, key, response_json FROM wt_idempotency WHERE endpoint = ? AND key = ?",
                (endpoint, key),
            ).fetchone()
            if row is None:
                return None
            return IdempotencyRecord(
                key=row["key"],
                endpoint=row["endpoint"],
                response_json=json.loads(row["response_json"]),
            )

    def put_idempotency(self, endpoint: str, key: str, response_json: dict[str, Any]) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO wt_idempotency (id, endpoint, key, response_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (f"idem_{uuid4().hex}", endpoint, key, json.dumps(response_json), _utc_now()),
            )

    def upsert_account(self, *, domain: str, name: str | None) -> tuple[str, bool]:
        normalized_domain = _norm_domain(domain)
        now = _utc_now()
        with self._lock, self._conn() as conn:
            row = conn.execute("SELECT id FROM wt_accounts WHERE domain = ?", (normalized_domain,)).fetchone()
            if row:
                conn.execute(
                    "UPDATE wt_accounts SET name = COALESCE(?, name), updated_at = ? WHERE id = ?",
                    (name, now, row["id"]),
                )
                return row["id"], False
            account_id = f"acct_{uuid4().hex}"
            # New accounts default to job_outreach_enabled = 1.  This
            # makes job-listing outreach the platform default for fresh
            # intakes; the toggle in the rail still lets the SDR flip
            # any individual account back to the generic problem-and-
            # deliverable angle.  Existing accounts in the DB keep
            # whatever value they were inserted with.
            conn.execute(
                """
                INSERT INTO wt_accounts (
                    id, domain, name, created_at, updated_at, job_outreach_enabled
                ) VALUES (?, ?, ?, ?, ?, 1)
                """,
                (account_id, normalized_domain, name, now, now),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO wt_account_identity (
                    id, account_id, identity_type, identity_value, confidence_score, source, created_at
                ) VALUES (?, ?, 'domain', ?, 1.0, 'ingest', ?)
                """,
                (f"aid_{uuid4().hex}", account_id, normalized_domain, now),
            )
            return account_id, True

    def upsert_identity(
        self,
        *,
        account_id: str,
        identity_type: str,
        identity_value: str,
        confidence_score: float,
        source: str,
    ) -> None:
        ivalue = identity_value.strip().lower()
        if not ivalue:
            return
        with self._lock, self._conn() as conn:
            existing = conn.execute(
                """
                SELECT account_id FROM wt_account_identity
                WHERE identity_type = ? AND identity_value = ?
                """,
                (identity_type, ivalue),
            ).fetchone()
            if existing is not None and existing["account_id"] != account_id:
                raise ValueError(
                    f"Identity collision for {identity_type}:{ivalue}; bound to another account."
                )
            conn.execute(
                """
                INSERT OR REPLACE INTO wt_account_identity (
                    id, account_id, identity_type, identity_value, confidence_score, source, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"aid_{uuid4().hex}",
                    account_id,
                    identity_type,
                    ivalue,
                    max(0.0, min(1.0, confidence_score)),
                    source,
                    _utc_now(),
                ),
            )
            conn.execute(
                """
                INSERT INTO wt_identity_events (
                    id, account_id, event_type, identity_type, identity_value, source, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"ide_{uuid4().hex}",
                    account_id,
                    "bind_identity",
                    identity_type,
                    ivalue,
                    source,
                    json.dumps({"confidence_score": max(0.0, min(1.0, confidence_score))}),
                    _utc_now(),
                ),
            )

    def resolve_account_by_identity(self, identity_type: str, identity_value: str) -> str | None:
        ivalue = identity_value.strip().lower()
        if not ivalue:
            return None
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT account_id FROM wt_account_identity
                WHERE identity_type = ? AND identity_value = ?
                """,
                (identity_type, ivalue),
            ).fetchone()
            if row is None:
                return None
            return str(row["account_id"])

    def update_account_fields(self, account_id: str, fields: dict[str, Any]) -> None:
        allowed = {
            "name", "linkedin_url", "twitter_url", "crunchbase_id", "industry", "employee_count",
            "funding_stage", "total_funding", "country", "icp_status",
            "job_outreach_enabled", "job_outreach_updated_at",
        }
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return
        updates["updated_at"] = _utc_now()
        assignments = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [account_id]
        with self._lock, self._conn() as conn:
            conn.execute(f"UPDATE wt_accounts SET {assignments} WHERE id = ?", values)

    def enable_job_outreach_for_all(self) -> int:
        """Flip ``job_outreach_enabled = 1`` on every account that was
        created before the platform default flipped to ON.

        Free, idempotent, no LLM/vendor calls.  Returns how many rows
        were updated so the UI can show a toast.
        """
        with self._lock, self._conn() as conn:
            cur = conn.execute(
                "UPDATE wt_accounts SET job_outreach_enabled = 1, "
                "job_outreach_updated_at = ?, updated_at = ? "
                "WHERE COALESCE(job_outreach_enabled, 0) = 0",
                (_utc_now(), _utc_now()),
            )
            return cur.rowcount or 0

    def set_job_outreach_enabled(self, account_id: str, enabled: bool) -> None:
        """Toggle the per-account 'regenerate drafts targeting open roles' mode.

        When enabled, future ``generate_draft`` calls fold the company's
        currently-open job postings into the prompt and produce email
        copy that pitches Figwork as fill for one of the listed roles.
        Persists a timestamp so the UI can show 'enabled 3 minutes ago'.
        """
        self.update_account_fields(account_id, {
            "job_outreach_enabled": 1 if enabled else 0,
            "job_outreach_updated_at": _utc_now(),
        })

    def get_account(self, account_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, domain, name, linkedin_url, twitter_url, crunchbase_id, industry, employee_count,
                       funding_stage, total_funding, country, icp_status, created_at, updated_at,
                       job_outreach_enabled, job_outreach_updated_at
                FROM wt_accounts
                WHERE id = ?
                """,
                (account_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown account_id={account_id}")
        return dict(row)

    def add_signal(
        self,
        *,
        account_id: str,
        signal_type: str,
        source: str,
        occurred_at: str,
        raw_payload: dict[str, Any],
        normalized_payload: dict[str, Any],
        confidence_score: float,
    ) -> tuple[str, bool]:
        dedupe_base = f"{account_id}|{signal_type}|{source}|{occurred_at}|{json.dumps(normalized_payload, sort_keys=True)}"
        dedupe_hash = sha256(dedupe_base.encode("utf-8")).hexdigest()
        now = _utc_now()
        with self._lock, self._conn() as conn:
            existing = conn.execute(
                "SELECT id FROM wt_signals WHERE dedupe_hash = ?",
                (dedupe_hash,),
            ).fetchone()
            if existing:
                return existing["id"], False
            signal_id = f"sig_{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO wt_signals (
                    id, account_id, signal_type, source, occurred_at, raw_payload_json,
                    normalized_payload_json, confidence_score, dedupe_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    account_id,
                    signal_type,
                    source,
                    occurred_at,
                    json.dumps(raw_payload),
                    json.dumps(normalized_payload),
                    confidence_score,
                    dedupe_hash,
                    now,
                ),
            )
            return signal_id, True

    def list_account_signals(self, account_id: str, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, signal_type, source, occurred_at, normalized_payload_json, confidence_score
                FROM wt_signals
                WHERE account_id = ?
                ORDER BY occurred_at DESC
                LIMIT ?
                """,
                (account_id, limit),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r["id"],
                    "signal_type": r["signal_type"],
                    "source": r["source"],
                    "occurred_at": r["occurred_at"],
                    "normalized_payload": json.loads(r["normalized_payload_json"]),
                    "confidence_score": float(r["confidence_score"]),
                }
            )
        return out

    def replace_geo_attribution(
        self,
        *,
        account_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        with self._lock, self._conn() as conn:
            conn.execute("DELETE FROM wt_account_geo_attribution WHERE account_id = ?", (account_id,))
            now = _utc_now()
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO wt_account_geo_attribution (
                        id, account_id, geography_id, weight, evidence, confidence_score, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"geoa_{uuid4().hex}",
                        account_id,
                        str(row["geography_id"]),
                        float(row["weight"]),
                        str(row.get("evidence", "unknown")),
                        float(row.get("confidence_score", 0.5)),
                        now,
                    ),
                )

    def get_geo_attribution(self, account_id: str) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT geography_id, weight, evidence, confidence_score, created_at
                FROM wt_account_geo_attribution
                WHERE account_id = ?
                ORDER BY weight DESC, confidence_score DESC
                """,
                (account_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def save_signal_stack(
        self,
        *,
        account_id: str,
        window_start: str,
        window_end: str,
        scores: dict[str, float],
        explanation: dict[str, Any],
    ) -> str:
        stack_id = f"stk_{uuid4().hex}"
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_signal_stacks (
                    id, account_id, stack_window_start, stack_window_end,
                    funding_score, buyer_intent_score, hiring_score, web_intent_score,
                    exec_change_score, total_signal_score, explanation_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stack_id,
                    account_id,
                    window_start,
                    window_end,
                    scores.get("funding_score", 0.0),
                    scores.get("buyer_intent_score", 0.0),
                    scores.get("hiring_score", 0.0),
                    scores.get("web_intent_score", 0.0),
                    scores.get("exec_change_score", 0.0),
                    scores.get("total_signal_score", 0.0),
                    json.dumps(explanation),
                    _utc_now(),
                ),
            )
        return stack_id

    def get_latest_signal_stack(self, account_id: str) -> dict[str, Any] | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, funding_score, buyer_intent_score, hiring_score, web_intent_score,
                       exec_change_score, total_signal_score, explanation_json, created_at
                FROM wt_signal_stacks
                WHERE account_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "funding_score": float(row["funding_score"]),
            "buyer_intent_score": float(row["buyer_intent_score"]),
            "hiring_score": float(row["hiring_score"]),
            "web_intent_score": float(row["web_intent_score"]),
            "exec_change_score": float(row["exec_change_score"]),
            "total_signal_score": float(row["total_signal_score"]),
            "explanation": json.loads(row["explanation_json"]),
            "created_at": row["created_at"],
        }

    def upsert_contact(self, account_id: str, contact: dict[str, Any]) -> str:
        email = (contact.get("email") or "").strip().lower()
        now = _utc_now()
        with self._lock, self._conn() as conn:
            row = None
            if email:
                row = conn.execute(
                    "SELECT id FROM wt_contacts WHERE account_id = ? AND email = ?",
                    (account_id, email),
                ).fetchone()
            if row is not None:
                conn.execute(
                    """
                    UPDATE wt_contacts
                    SET full_name = COALESCE(?, full_name),
                        title = COALESCE(?, title),
                        persona_type = COALESCE(?, persona_type),
                        confidence_score = ?,
                        source = COALESCE(?, source),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        contact.get("full_name"),
                        contact.get("title"),
                        contact.get("persona_type"),
                        float(contact.get("confidence_score", 0.0)),
                        contact.get("source"),
                        now,
                        row["id"],
                    ),
                )
                return row["id"]
            contact_id = f"ct_{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO wt_contacts (
                    id, account_id, full_name, title, linkedin_url, email, email_status,
                    persona_type, confidence_score, source, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    contact_id,
                    account_id,
                    contact.get("full_name"),
                    contact.get("title"),
                    contact.get("linkedin_url"),
                    email or None,
                    contact.get("email_status", "unknown"),
                    contact.get("persona_type"),
                    float(contact.get("confidence_score", 0.0)),
                    contact.get("source"),
                    now,
                    now,
                ),
            )
            return contact_id

    def list_contacts(self, account_id: str) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, full_name, title, linkedin_url, email, email_status, persona_type, confidence_score, source
                FROM wt_contacts
                WHERE account_id = ?
                ORDER BY confidence_score DESC, updated_at DESC
                """,
                (account_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def purge_drafts_by_status(self, status: str) -> int:
        """Hard-delete all drafts at a given status (e.g. 'discarded')."""
        with self._lock, self._conn() as conn:
            cur = conn.execute("DELETE FROM wt_outreach_drafts WHERE status = ?", (status,))
        return cur.rowcount or 0

    # --- Universal search helpers ----------------------------------------

    def fuzzy_search_accounts(self, query: str, *, limit: int = 12) -> list[dict[str, Any]]:
        """LIKE-based fuzzy search across name + domain + industry + country.

        Case-insensitive substring match.  Cheap; runs against the local
        SQLite index so the universal search modal can return instant
        results without hitting any vendor API.
        """
        q = (query or "").strip().lower()
        if not q:
            return []
        needle = f"%{q}%"
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    a.id, a.name, a.domain, a.linkedin_url, a.twitter_url, a.industry, a.country,
                    a.employee_count, a.funding_stage, a.total_funding,
                    a.icp_status, a.updated_at,
                    (SELECT total_signal_score FROM wt_signal_stacks s
                      WHERE s.account_id = a.id
                      ORDER BY s.created_at DESC LIMIT 1) AS signal_score,
                    (SELECT COUNT(*) FROM wt_outreach_drafts d WHERE d.account_id = a.id) AS draft_count,
                    (SELECT COUNT(*) FROM wt_contacts c WHERE c.account_id = a.id) AS contact_count
                FROM wt_accounts a
                WHERE lower(a.name) LIKE ?
                   OR lower(a.domain) LIKE ?
                   OR lower(COALESCE(a.industry, '')) LIKE ?
                   OR lower(COALESCE(a.country, '')) LIKE ?
                ORDER BY
                    CASE WHEN lower(a.name) = ? THEN 0
                         WHEN lower(a.domain) = ? THEN 0
                         WHEN lower(a.name) LIKE ? THEN 1
                         WHEN lower(a.domain) LIKE ? THEN 1
                         ELSE 2 END,
                    a.updated_at DESC
                LIMIT ?
                """,
                (needle, needle, needle, needle, q, q, f"{q}%", f"{q}%", limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def fuzzy_search_contacts(self, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        """LIKE-based fuzzy search across contact name + email + title.

        Returns contacts joined to their parent account so the UI can show
        "Jane Doe · CTO at Stripe" in one row.
        """
        q = (query or "").strip().lower()
        if not q:
            return []
        needle = f"%{q}%"
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    c.id AS contact_id, c.full_name, c.title, c.email,
                    c.linkedin_url, c.persona_type, c.confidence_score,
                    a.id AS account_id, a.name AS account_name, a.domain AS account_domain,
                    a.industry
                FROM wt_contacts c
                JOIN wt_accounts a ON a.id = c.account_id
                WHERE lower(COALESCE(c.full_name, '')) LIKE ?
                   OR lower(COALESCE(c.email, '')) LIKE ?
                   OR lower(COALESCE(c.title, '')) LIKE ?
                ORDER BY
                    CASE WHEN lower(c.full_name) = ? THEN 0
                         WHEN lower(c.email) = ? THEN 0
                         ELSE 1 END,
                    c.confidence_score DESC
                LIMIT ?
                """,
                (needle, needle, needle, q, q, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_account(self, account_id: str) -> None:
        """Hard-delete an account. FK cascades clean up contacts, signals, drafts,
        hypotheses, opportunities, scoping briefs, etc. automatically."""
        with self._lock, self._conn() as conn:
            conn.execute("DELETE FROM wt_accounts WHERE id = ?", (account_id,))

    def bulk_delete_accounts(self, account_ids: list[str]) -> int:
        """Bulk-delete accounts. Returns how many rows were removed."""
        if not account_ids:
            return 0
        deleted = 0
        with self._lock, self._conn() as conn:
            for aid in account_ids:
                cur = conn.execute("DELETE FROM wt_accounts WHERE id = ?", (aid,))
                deleted += cur.rowcount or 0
        return deleted

    def find_test_accounts(self) -> list[dict[str, Any]]:
        """Find accounts whose name or domain matches common test/smoke fixtures.

        Pattern is conservative: matches literal keywords like "test", "smoke",
        "example", "demo", "clay smoke", "a01", "d2cr", etc.  Accounts with
        production-looking names are not flagged.
        """
        import re
        pattern = re.compile(
            r"\b("
            r"test|smoke|example|demo|placeholder|fixture|"
            r"clay\s*(smoke|test|only|v\d)|"
            r"edge\s*\d+|a0\d\b|d2cr|"
            r"cr[-\s]?(smoke|test)|"
            r"scan[-\s]?test|final[-\s]?pipe|"
            r"score[-\s]?example|pipeline[-\s]?test|"
            r"detail[-\s]?(smoke|example|test)|"
            r"audit[-\s]?(full|clay|test)|"
            r"fin[-\s]?(clay|test)|"
            r"exec[-\s]?demo|phase[-\s]?next|"
            r"unknown[-\s]?co\d*|skyrocketventures"
            r")\b",
            re.IGNORECASE,
        )
        matches: list[dict[str, Any]] = []
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                "SELECT id, name, domain FROM wt_accounts"
            ).fetchall()
        for r in rows:
            name = str(r["name"] or "")
            dom = str(r["domain"] or "")
            if pattern.search(f"{name} {dom}"):
                matches.append({"id": r["id"], "name": name, "domain": dom})
        return matches

    def purge_emailless_auto_contacts(self) -> dict[str, Any]:
        """Delete contacts that came from automated paths (Apollo, Hunter,
        vendor enrichment) but have no usable email.

        These rows pollute the pipeline because they look like real
        contacts but cannot be messaged.  Manual contacts (``source =
        'manual'``) are preserved — the SDR may have added them
        intentionally to track someone they will research later.
        """
        protected_sources = ("manual",)
        placeholders = ",".join("?" for _ in protected_sources)
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, account_id, full_name, source
                FROM wt_contacts
                WHERE (email IS NULL OR email = '' OR email NOT LIKE '%@%')
                  AND (source IS NULL OR source NOT IN ({placeholders}))
                """,
                protected_sources,
            ).fetchall()
            ids = [r["id"] for r in rows]
            if ids:
                conn.executemany(
                    "DELETE FROM wt_contacts WHERE id = ?",
                    [(i,) for i in ids],
                )
        return {"deleted": len(ids), "preserved_sources": list(protected_sources)}

    def delete_contact(self, contact_id: str) -> None:
        with self._lock, self._conn() as conn:
            conn.execute("DELETE FROM wt_contacts WHERE id = ?", (contact_id,))

    def get_contact(self, contact_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, account_id, full_name, title, linkedin_url, email, email_status,
                       persona_type, confidence_score, source
                FROM wt_contacts
                WHERE id = ?
                """,
                (contact_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown contact_id={contact_id}")
        return dict(row)

    def count_sent_by_domain_since(self, domain: str, since_iso: str) -> int:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM wt_outreach_drafts d
                JOIN wt_accounts a ON a.id = d.account_id
                WHERE d.status = 'sent' AND a.domain = ? AND d.updated_at >= ?
                """,
                (domain, since_iso),
            ).fetchone()
            return int(row["c"]) if row else 0

    def save_work_hypothesis(
        self,
        *,
        account_id: str,
        signal_stack_id: str,
        probable_problem: str,
        probable_deliverable: str,
        talent_archetype: str,
        urgency_score: float,
        taskability_score: float,
        fit_score: float,
        confidence_score: float,
        rationale: list[str],
        generated_by_model: str,
        model_version: str,
    ) -> str:
        hypothesis_id = f"wh_{uuid4().hex}"
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_work_hypotheses (
                    id, account_id, signal_stack_id, probable_problem, probable_deliverable, talent_archetype,
                    urgency_score, taskability_score, fit_score, confidence_score, rationale_json,
                    generated_by_model, model_version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hypothesis_id,
                    account_id,
                    signal_stack_id,
                    probable_problem,
                    probable_deliverable,
                    talent_archetype,
                    urgency_score,
                    taskability_score,
                    fit_score,
                    confidence_score,
                    json.dumps(rationale),
                    generated_by_model,
                    model_version,
                    _utc_now(),
                ),
            )
        return hypothesis_id

    def get_work_hypothesis(self, hypothesis_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, account_id, probable_problem, probable_deliverable, talent_archetype,
                       urgency_score, taskability_score, fit_score, confidence_score,
                       rationale_json, created_at
                FROM wt_work_hypotheses
                WHERE id = ?
                """,
                (hypothesis_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown work_hypothesis_id={hypothesis_id}")
        payload = dict(row)
        payload["rationale"] = json.loads(payload.pop("rationale_json"))
        return payload

    def list_work_hypotheses(self, account_id: str, limit: int = 10) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, account_id, probable_problem, probable_deliverable, talent_archetype,
                       urgency_score, taskability_score, fit_score, confidence_score, rationale_json, created_at
                FROM wt_work_hypotheses
                WHERE account_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (account_id, limit),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["rationale"] = json.loads(payload.pop("rationale_json"))
            out.append(payload)
        return out

    def save_draft(
        self,
        *,
        account_id: str,
        contact_id: str,
        work_hypothesis_id: str,
        channel: str,
        subject_a: str,
        subject_b: str,
        email_body: str,
        followup_body: str,
        linkedin_dm: str,
        metadata: dict[str, Any],
        outreach_mode: str = "default",
        target_job_title: str | None = None,
        target_job_url: str | None = None,
    ) -> str:
        draft_id = f"dr_{uuid4().hex}"
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_outreach_drafts (
                    id, account_id, contact_id, work_hypothesis_id, channel, subject_a, subject_b,
                    email_body, followup_body, linkedin_dm, status,
                    target_job_title, target_job_url, outreach_mode,
                    generation_metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft_ready', ?, ?, ?, ?, ?, ?)
                """,
                (
                    draft_id,
                    account_id,
                    contact_id,
                    work_hypothesis_id,
                    channel,
                    subject_a,
                    subject_b,
                    email_body,
                    followup_body,
                    linkedin_dm,
                    target_job_title,
                    target_job_url,
                    outreach_mode,
                    json.dumps(metadata),
                    now,
                    now,
                ),
            )
        return draft_id

    def get_draft(self, draft_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, account_id, contact_id, work_hypothesis_id, channel, subject_a, subject_b, email_body,
                       followup_body, linkedin_dm, status, generation_metadata_json, created_at, updated_at
                FROM wt_outreach_drafts
                WHERE id = ?
                """,
                (draft_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown draft_id={draft_id}")
        payload = dict(row)
        payload["generation_metadata"] = json.loads(payload.pop("generation_metadata_json"))
        return payload

    def list_drafts(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        cols = (
            "id, account_id, contact_id, work_hypothesis_id, channel, subject_a, subject_b, "
            "email_body, followup_body, linkedin_dm, status, "
            "target_job_title, target_job_url, outreach_mode, "
            "generation_metadata_json, created_at, updated_at"
        )
        with self._lock, self._conn() as conn:
            if status:
                rows = conn.execute(
                    f"SELECT {cols} FROM wt_outreach_drafts WHERE status = ? ORDER BY updated_at DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"SELECT {cols} FROM wt_outreach_drafts ORDER BY updated_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["generation_metadata"] = json.loads(payload.pop("generation_metadata_json"))
            out.append(payload)
        return out

    def update_draft(self, draft_id: str, **updates: Any) -> None:
        allowed = {
            "status", "subject_a", "subject_b", "email_body", "followup_body",
            "linkedin_dm", "target_job_title", "target_job_url", "outreach_mode",
            "updated_at",
        }
        bad_keys = set(updates) - allowed
        if bad_keys:
            raise ValueError(f"Unsupported draft update fields: {sorted(bad_keys)}")
        if not updates:
            return
        assignments = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [draft_id]
        with self._lock, self._conn() as conn:
            conn.execute(f"UPDATE wt_outreach_drafts SET {assignments} WHERE id = ?", values)

    def list_drafts_for_account(
        self,
        account_id: str,
        *,
        statuses: tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """Return all drafts on an account, optionally filtered by status.

        Used by the bulk-regenerate flow to find which drafts to recompute
        when job-outreach mode is toggled on for an account.  Returns the
        most recent first per (contact_id, work_hypothesis_id) so we can
        regenerate just the latest active draft per contact.
        """
        params: list[Any] = [account_id]
        clauses = ["account_id = ?"]
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(statuses)
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, account_id, contact_id, work_hypothesis_id, channel, status,
                       subject_a, subject_b, email_body, followup_body, linkedin_dm,
                       target_job_title, target_job_url, outreach_mode,
                       generation_metadata_json, created_at, updated_at
                FROM wt_outreach_drafts
                WHERE {' AND '.join(clauses)}
                ORDER BY updated_at DESC
                """,
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    def add_review_decision(
        self,
        *,
        draft_id: str,
        reviewer_user_id: str,
        action: str,
        edited_subject: str | None,
        edited_body: str | None,
        reason_code: str | None,
        notes: str | None,
    ) -> str:
        review_id = f"rv_{uuid4().hex}"
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_review_decisions (
                    id, draft_id, reviewer_user_id, action, edited_body, edited_subject, reason_code, notes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (review_id, draft_id, reviewer_user_id, action, edited_body, edited_subject, reason_code, notes, _utc_now()),
            )
        return review_id

    def create_or_update_opportunity(
        self,
        *,
        account_id: str,
        contact_id: str,
        source_draft_id: str,
        stage: str,
        positive_reply_at: str | None = None,
        crm_id: str | None = None,
    ) -> str:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT id FROM wt_opportunities WHERE source_draft_id = ?",
                (source_draft_id,),
            ).fetchone()
            now = _utc_now()
            if row:
                if crm_id:
                    conn.execute(
                        """
                        UPDATE wt_opportunities
                        SET stage = ?, positive_reply_at = COALESCE(?, positive_reply_at),
                            crm_id = COALESCE(?, crm_id), updated_at = ?
                        WHERE id = ?
                        """,
                        (stage, positive_reply_at, crm_id, now, row["id"]),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE wt_opportunities
                        SET stage = ?, positive_reply_at = COALESCE(?, positive_reply_at), updated_at = ?
                        WHERE id = ?
                        """,
                        (stage, positive_reply_at, now, row["id"]),
                    )
                return row["id"]
            opportunity_id = f"opp_{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO wt_opportunities (
                    id, account_id, contact_id, source_draft_id, crm_id, stage,
                    positive_reply_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (opportunity_id, account_id, contact_id, source_draft_id, crm_id, stage,
                 positive_reply_at, now, now),
            )
        return opportunity_id

    def get_opportunity_by_draft(self, source_draft_id: str) -> dict[str, Any] | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM wt_opportunities WHERE source_draft_id = ?",
                (source_draft_id,),
            ).fetchone()
            return dict(row) if row else None

    def save_scoping_brief(
        self,
        *,
        opportunity_id: str,
        summary: str,
        likely_pain_points: list[str],
        proposed_work_packages: list[dict[str, Any]],
        suggested_talent_archetypes: list[str],
        discovery_questions: list[str],
    ) -> str:
        brief_id = f"sb_{uuid4().hex}"
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_scoping_briefs (
                    id, opportunity_id, summary, likely_pain_points_json, proposed_work_packages_json,
                    suggested_talent_archetypes_json, discovery_questions_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    brief_id,
                    opportunity_id,
                    summary,
                    json.dumps(likely_pain_points),
                    json.dumps(proposed_work_packages),
                    json.dumps(suggested_talent_archetypes),
                    json.dumps(discovery_questions),
                    _utc_now(),
                ),
            )
        return brief_id

    def get_scoping_brief(self, brief_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, opportunity_id, summary, likely_pain_points_json, proposed_work_packages_json,
                       suggested_talent_archetypes_json, discovery_questions_json, created_at
                FROM wt_scoping_briefs
                WHERE id = ?
                """,
                (brief_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown scoping_brief_id={brief_id}")
        return {
            "id": row["id"],
            "opportunity_id": row["opportunity_id"],
            "summary": row["summary"],
            "likely_pain_points": json.loads(row["likely_pain_points_json"]),
            "proposed_work_packages": json.loads(row["proposed_work_packages_json"]),
            "suggested_talent_archetypes": json.loads(row["suggested_talent_archetypes_json"]),
            "discovery_questions": json.loads(row["discovery_questions_json"]),
            "created_at": row["created_at"],
        }

    def list_opportunities(self, stage: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            if stage:
                rows = conn.execute(
                    """
                    SELECT o.id, o.account_id, o.contact_id, o.source_draft_id, o.crm_id, o.stage,
                           o.positive_reply_at, o.owner_user_id, o.created_at, o.updated_at
                    FROM wt_opportunities o
                    WHERE o.stage = ?
                    ORDER BY o.updated_at DESC
                    LIMIT ?
                    """,
                    (stage, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT o.id, o.account_id, o.contact_id, o.source_draft_id, o.crm_id, o.stage,
                           o.positive_reply_at, o.owner_user_id, o.created_at, o.updated_at
                    FROM wt_opportunities o
                    ORDER BY o.updated_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            opp = dict(r)
            try:
                acct = self.get_account(opp["account_id"])
                opp["account_name"] = acct.get("name")
                opp["domain"] = acct.get("domain")
            except KeyError:
                opp["account_name"] = None
                opp["domain"] = None
            try:
                ct = self.get_contact(opp["contact_id"])
                opp["contact_name"] = ct.get("full_name")
                opp["contact_title"] = ct.get("title")
            except KeyError:
                opp["contact_name"] = None
                opp["contact_title"] = None
            out.append(opp)
        return out

    def list_all_accounts(self, limit: int = 500) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, domain, name, linkedin_url, twitter_url, industry, employee_count, funding_stage, total_funding,
                       country, icp_status, created_at, updated_at,
                       job_outreach_enabled, job_outreach_updated_at
                FROM wt_accounts
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, min(2000, limit)),),
            ).fetchall()
        return [dict(r) for r in rows]

    def account_counts(self, account_id: str) -> dict[str, int]:
        with self._lock, self._conn() as conn:
            sc = conn.execute("SELECT COUNT(*) AS c FROM wt_signals WHERE account_id = ?", (account_id,)).fetchone()
            cc = conn.execute("SELECT COUNT(*) AS c FROM wt_contacts WHERE account_id = ?", (account_id,)).fetchone()
            dc = conn.execute("SELECT COUNT(*) AS c FROM wt_outreach_drafts WHERE account_id = ?", (account_id,)).fetchone()
            hc = conn.execute("SELECT COUNT(*) AS c FROM wt_work_hypotheses WHERE account_id = ?", (account_id,)).fetchone()
        return {
            "signal_count": int(sc["c"]) if sc else 0,
            "contact_count": int(cc["c"]) if cc else 0,
            "draft_count": int(dc["c"]) if dc else 0,
            "hypothesis_count": int(hc["c"]) if hc else 0,
        }

    def enqueue_job(
        self,
        *,
        job_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        max_attempts: int,
    ) -> tuple[str, bool]:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT id FROM wt_jobs WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row is not None:
                return str(row["id"]), False
            job_id = f"job_{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO wt_jobs (
                    id, job_type, status, payload_json, idempotency_key,
                    attempts, max_attempts, run_after, created_at, updated_at
                ) VALUES (?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    job_type,
                    json.dumps(payload),
                    idempotency_key,
                    max_attempts,
                    now,
                    now,
                    now,
                ),
            )
            return job_id, True

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, job_type, status, payload_json, idempotency_key, attempts, max_attempts,
                       run_after, last_error, created_at, updated_at
                FROM wt_jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown job_id={job_id}")
        payload = dict(row)
        payload["payload"] = json.loads(payload.pop("payload_json"))
        return payload

    def claim_next_job(self, allowed_types: list[str] | None = None) -> dict[str, Any] | None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            if allowed_types:
                placeholders = ",".join("?" for _ in allowed_types)
                row = conn.execute(
                    f"""
                    SELECT id, job_type, status, payload_json, attempts, max_attempts, run_after, last_error
                    FROM wt_jobs
                    WHERE status = 'queued' AND run_after <= ? AND job_type IN ({placeholders})
                    ORDER BY created_at ASC
                    LIMIT 1
                    """,
                    (now, *allowed_types),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT id, job_type, status, payload_json, attempts, max_attempts, run_after, last_error
                    FROM wt_jobs
                    WHERE status = 'queued' AND run_after <= ?
                    ORDER BY created_at ASC
                    LIMIT 1
                    """,
                    (now,),
                ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE wt_jobs
                SET status = 'in_progress', attempts = attempts + 1, updated_at = ?
                WHERE id = ?
                """,
                (now, row["id"]),
            )
            claimed = dict(row)
            claimed["attempts"] = int(claimed["attempts"]) + 1
            claimed["payload"] = json.loads(claimed.pop("payload_json"))
            return claimed

    def complete_job(self, job_id: str) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                "UPDATE wt_jobs SET status = 'completed', updated_at = ? WHERE id = ?",
                (_utc_now(), job_id),
            )

    def fail_job(self, job_id: str, error_message: str) -> None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT id, job_type, payload_json, attempts, max_attempts FROM wt_jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown job_id={job_id}")
            attempts = int(row["attempts"])
            max_attempts = int(row["max_attempts"])
            now = datetime.now(timezone.utc)
            if attempts >= max_attempts:
                conn.execute(
                    """
                    UPDATE wt_jobs
                    SET status = 'dead_letter', last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (error_message[:2000], now.isoformat(), job_id),
                )
                conn.execute(
                    """
                    INSERT INTO wt_dead_letters (
                        id, job_id, job_type, payload_json, error_message, failed_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"dlq_{uuid4().hex}",
                        row["id"],
                        row["job_type"],
                        row["payload_json"],
                        error_message[:2000],
                        now.isoformat(),
                    ),
                )
            else:
                backoff_seconds = min(900, 2 ** attempts)
                run_after = (now + timedelta(seconds=backoff_seconds)).isoformat()
                conn.execute(
                    """
                    UPDATE wt_jobs
                    SET status = 'queued', run_after = ?, last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (run_after, error_message[:2000], now.isoformat(), job_id),
                )

    def list_dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, job_id, job_type, payload_json, error_message, failed_at
                FROM wt_dead_letters
                ORDER BY failed_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "id": row["id"],
                    "job_id": row["job_id"],
                    "job_type": row["job_type"],
                    "payload": json.loads(row["payload_json"]),
                    "error_message": row["error_message"],
                    "failed_at": row["failed_at"],
                }
            )
        return out

    def requeue_dead_letter(self, dead_letter_id: str, *, max_attempts: int = 5) -> str:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, job_type, payload_json
                FROM wt_dead_letters
                WHERE id = ?
                """,
                (dead_letter_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown dead_letter_id={dead_letter_id}")
            now = _utc_now()
            new_job_id = f"job_{uuid4().hex}"
            idem = f"requeue:{dead_letter_id}:{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO wt_jobs (
                    id, job_type, status, payload_json, idempotency_key,
                    attempts, max_attempts, run_after, created_at, updated_at
                ) VALUES (?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?)
                """,
                (
                    new_job_id,
                    str(row["job_type"]),
                    str(row["payload_json"]),
                    idem,
                    max(1, min(20, max_attempts)),
                    now,
                    now,
                    now,
                ),
            )
            conn.execute("DELETE FROM wt_dead_letters WHERE id = ?", (dead_letter_id,))
            return new_job_id

    def add_suppression(self, *, email: str, reason: str, source: str) -> None:
        normalized = email.strip().lower()
        if not normalized:
            raise ValueError("email must be non-empty")
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO wt_suppressions (id, email, reason, source, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (f"sup_{uuid4().hex}", normalized, reason, source, _utc_now()),
            )

    def is_suppressed(self, email: str) -> bool:
        normalized = email.strip().lower()
        if not normalized:
            return False
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM wt_suppressions WHERE email = ?",
                (normalized,),
            ).fetchone()
            return row is not None

    def list_suppressions(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT email, reason, source, created_at
                FROM wt_suppressions
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def log_crm_sync_event(
        self,
        *,
        account_id: str,
        contact_id: str | None,
        opportunity_id: str | None,
        direction: str,
        status: str,
        details: dict[str, Any],
    ) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_crm_sync_events (
                    id, account_id, contact_id, opportunity_id, direction, status, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"crm_{uuid4().hex}",
                    account_id,
                    contact_id,
                    opportunity_id,
                    direction,
                    status,
                    json.dumps(details),
                    _utc_now(),
                ),
            )

    def list_crm_sync_events(self, account_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            if account_id:
                rows = conn.execute(
                    """
                    SELECT account_id, contact_id, opportunity_id, direction, status, details_json, created_at
                    FROM wt_crm_sync_events
                    WHERE account_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (account_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT account_id, contact_id, opportunity_id, direction, status, details_json, created_at
                    FROM wt_crm_sync_events
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["details"] = json.loads(payload.pop("details_json"))
            out.append(payload)
        return out

    def crm_drift_summary(self) -> dict[str, int]:
        with self._lock, self._conn() as conn:
            total_opp = conn.execute("SELECT COUNT(*) AS c FROM wt_opportunities").fetchone()
            synced_opp = conn.execute(
                """
                SELECT COUNT(DISTINCT opportunity_id) AS c
                FROM wt_crm_sync_events
                WHERE status = 'success' AND opportunity_id IS NOT NULL
                """
            ).fetchone()
        total = int(total_opp["c"]) if total_opp else 0
        synced = int(synced_opp["c"]) if synced_opp else 0
        return {
            "total_opportunities": total,
            "synced_opportunities": synced,
            "unsynced_opportunities": max(0, total - synced),
        }

    def analytics_summary(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(days=7)).isoformat()
        with self._lock, self._conn() as conn:
            draft_rows = conn.execute(
                "SELECT status, COUNT(*) AS c FROM wt_outreach_drafts GROUP BY status"
            ).fetchall()
            opp_rows = conn.execute(
                "SELECT stage, COUNT(*) AS c FROM wt_opportunities GROUP BY stage"
            ).fetchall()
            signal_count = conn.execute("SELECT COUNT(*) AS c FROM wt_signals").fetchone()
            avg_signal = conn.execute("SELECT AVG(total_signal_score) AS a FROM wt_signal_stacks").fetchone()
            drafts_created_7d = conn.execute(
                "SELECT COUNT(*) AS c FROM wt_outreach_drafts WHERE created_at >= ?",
                (cutoff,),
            ).fetchone()
            sends_7d = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM wt_outreach_drafts
                WHERE status IN ('sent', 'replied') AND updated_at >= ?
                """,
                (cutoff,),
            ).fetchone()
            decision_rows = conn.execute(
                "SELECT action FROM wt_review_decisions ORDER BY created_at DESC LIMIT 2000"
            ).fetchall()
            draft_times = conn.execute(
                """
                SELECT status, created_at, updated_at
                FROM wt_outreach_drafts
                WHERE status IN ('approved', 'sent', 'replied')
                ORDER BY updated_at DESC
                LIMIT 5000
                """
            ).fetchall()

        approval_times: list[float] = []
        send_times: list[float] = []
        for row in draft_times:
            created = _parse_iso(str(row["created_at"]))
            updated = _parse_iso(str(row["updated_at"]))
            if created is None or updated is None or updated <= created:
                continue
            hours = (updated - created).total_seconds() / 3600.0
            approval_times.append(hours)
            if str(row["status"]) in {"sent", "replied"}:
                send_times.append(hours)
        approval_times.sort()
        send_times.sort()
        reviewed_total = len(decision_rows)
        approved_total = sum(1 for r in decision_rows if str(r["action"]) in {"approve", "edit_and_approve"})

        def _median(values: list[float]) -> float:
            if not values:
                return 0.0
            mid = len(values) // 2
            if len(values) % 2 == 1:
                return values[mid]
            return (values[mid - 1] + values[mid]) / 2.0

        return {
            "signals_total": int(signal_count["c"]) if signal_count else 0,
            "avg_signal_score": float(avg_signal["a"]) if avg_signal and avg_signal["a"] is not None else 0.0,
            "draft_status_counts": {str(r["status"]): int(r["c"]) for r in draft_rows},
            "opportunity_stage_counts": {str(r["stage"]): int(r["c"]) for r in opp_rows},
            "throughput_7d": {
                "drafts_created": int(drafts_created_7d["c"]) if drafts_created_7d else 0,
                "drafts_sent": int(sends_7d["c"]) if sends_7d else 0,
            },
            "quality": {
                "reviewed_total": reviewed_total,
                "approved_total": approved_total,
                "approval_rate": (approved_total / reviewed_total) if reviewed_total > 0 else 0.0,
            },
            "speed_hours": {
                "median_create_to_approve_or_better": round(_median(approval_times), 2),
                "median_create_to_sent_or_replied": round(_median(send_times), 2),
            },
            "crm_drift": self.crm_drift_summary(),
        }

    def upsert_worker_heartbeat(
        self,
        *,
        worker_id: str,
        status: str,
        last_result: dict[str, Any],
    ) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO wt_worker_heartbeats (worker_id, status, last_seen_at, last_result_json)
                VALUES (?, ?, ?, ?)
                """,
                (worker_id, status, _utc_now(), json.dumps(last_result)),
            )

    def list_worker_heartbeats(self) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT worker_id, status, last_seen_at, last_result_json
                FROM wt_worker_heartbeats
                ORDER BY last_seen_at DESC
                """
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "worker_id": r["worker_id"],
                    "status": r["status"],
                    "last_seen_at": r["last_seen_at"],
                    "last_result": json.loads(r["last_result_json"]),
                }
            )
        return out

    def list_identity_events(self, account_id: str, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT event_type, identity_type, identity_value, source, details_json, created_at
                FROM wt_identity_events
                WHERE account_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (account_id, limit),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["details"] = json.loads(payload.pop("details_json"))
            out.append(payload)
        return out

    def add_crm_conflict(
        self,
        *,
        account_id: str,
        field_name: str,
        app_value: str | None,
        crm_value: str | None,
        policy: str,
        contact_id: str | None = None,
        opportunity_id: str | None = None,
    ) -> str:
        conflict_id = f"crmc_{uuid4().hex}"
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_crm_conflicts (
                    id, account_id, contact_id, opportunity_id, field_name, app_value, crm_value, policy,
                    resolution_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
                """,
                (
                    conflict_id,
                    account_id,
                    contact_id,
                    opportunity_id,
                    field_name,
                    app_value,
                    crm_value,
                    policy,
                    _utc_now(),
                ),
            )
        return conflict_id

    def resolve_crm_conflict(self, conflict_id: str, *, resolved_by: str, resolved_value: str) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                UPDATE wt_crm_conflicts
                SET resolution_status = 'resolved', resolved_by = ?, resolved_value = ?, resolved_at = ?
                WHERE id = ?
                """,
                (resolved_by, resolved_value, _utc_now(), conflict_id),
            )

    def list_crm_conflicts(self, status: str = "open", limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, account_id, contact_id, opportunity_id, field_name, app_value, crm_value, policy,
                       resolution_status, resolved_by, resolved_value, created_at, resolved_at
                FROM wt_crm_conflicts
                WHERE resolution_status = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (status, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def upsert_consent(
        self,
        *,
        email: str,
        channel: str,
        legal_basis: str,
        status: str,
        source: str,
        metadata: dict[str, Any],
    ) -> None:
        normalized = email.strip().lower()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO wt_consent_records (
                    id, email, channel, legal_basis, status, source, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"cons_{uuid4().hex}",
                    normalized,
                    channel,
                    legal_basis,
                    status,
                    source,
                    json.dumps(metadata),
                    _utc_now(),
                ),
            )

    def get_consent(self, *, email: str, channel: str) -> dict[str, Any] | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT email, channel, legal_basis, status, source, metadata_json, created_at
                FROM wt_consent_records
                WHERE email = ? AND channel = ?
                """,
                (email.strip().lower(), channel),
            ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["metadata"] = json.loads(payload.pop("metadata_json"))
        return payload

    def request_deletion(self, *, email: str | None, account_id: str | None, reason: str, requested_by: str) -> str:
        request_id = f"del_{uuid4().hex}"
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_deletion_requests (
                    id, email, account_id, reason, status, requested_by, requested_at
                ) VALUES (?, ?, ?, ?, 'requested', ?, ?)
                """,
                (request_id, email.strip().lower() if email else None, account_id, reason, requested_by, _utc_now()),
            )
        return request_id

    def complete_deletion(self, request_id: str) -> None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT email, account_id FROM wt_deletion_requests WHERE id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown deletion_request_id={request_id}")
            email = (row["email"] or "").strip().lower()
            account_id = row["account_id"]
            if email:
                conn.execute("DELETE FROM wt_contacts WHERE email = ?", (email,))
                conn.execute("DELETE FROM wt_suppressions WHERE email = ?", (email,))
                conn.execute("DELETE FROM wt_consent_records WHERE email = ?", (email,))
            if account_id:
                conn.execute("DELETE FROM wt_accounts WHERE id = ?", (account_id,))
            conn.execute(
                "UPDATE wt_deletion_requests SET status = 'completed', completed_at = ? WHERE id = ?",
                (_utc_now(), request_id),
            )

    def upsert_retention_policy(self, *, entity_type: str, retention_days: int, enabled: bool = True) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO wt_retention_policies (entity_type, retention_days, enabled, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (entity_type, max(1, retention_days), 1 if enabled else 0, _utc_now()),
            )

    # --- Chat sessions + messages ----------------------------------------

    def create_chat_session(self, account_id: str, title: str = "New conversation", model: str = "") -> dict[str, Any]:
        sid = f"chat_{uuid4().hex}"
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                "INSERT INTO wt_chat_sessions (id, account_id, title, model, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (sid, account_id, title.strip() or "New conversation", model or "", now, now),
            )
        return {"id": sid, "account_id": account_id, "title": title, "model": model, "created_at": now, "updated_at": now}

    def list_chat_sessions(self, account_id: str) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT s.id, s.account_id, s.title, s.model, s.created_at, s.updated_at,
                       (SELECT COUNT(*) FROM wt_chat_messages m WHERE m.session_id = s.id AND m.role IN ('user','assistant')) AS message_count
                FROM wt_chat_sessions s
                WHERE s.account_id = ?
                ORDER BY s.updated_at DESC
                """,
                (account_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_chat_session(self, session_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT id, account_id, title, model, created_at, updated_at FROM wt_chat_sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown chat session {session_id}")
            return dict(row)

    def rename_chat_session(self, session_id: str, title: str) -> None:
        with self._lock, self._conn() as conn:
            cur = conn.execute(
                "UPDATE wt_chat_sessions SET title = ?, updated_at = ? WHERE id = ?",
                (title.strip() or "Untitled", _utc_now(), session_id),
            )
            if cur.rowcount == 0:
                raise KeyError(f"Unknown chat session {session_id}")

    def delete_chat_session(self, session_id: str) -> None:
        with self._lock, self._conn() as conn:
            conn.execute("DELETE FROM wt_chat_sessions WHERE id = ?", (session_id,))

    def append_chat_message(
        self,
        session_id: str,
        *,
        role: str,
        content: str,
        tool_calls: list[dict[str, Any]] | None = None,
        tool_call_id: str | None = None,
        tool_name: str | None = None,
    ) -> dict[str, Any]:
        mid = f"msg_{uuid4().hex}"
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_chat_messages (id, session_id, role, content, tool_calls_json, tool_call_id, tool_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mid,
                    session_id,
                    role,
                    content,
                    json.dumps(tool_calls) if tool_calls else None,
                    tool_call_id,
                    tool_name,
                    now,
                ),
            )
            conn.execute(
                "UPDATE wt_chat_sessions SET updated_at = ? WHERE id = ?",
                (now, session_id),
            )
        return {"id": mid, "role": role, "content": content, "created_at": now, "tool_calls": tool_calls, "tool_call_id": tool_call_id, "tool_name": tool_name}

    def list_chat_messages(self, session_id: str, *, include_system: bool = False) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, session_id, role, content, tool_calls_json, tool_call_id, tool_name, created_at
                FROM wt_chat_messages
                WHERE session_id = ?
                ORDER BY created_at, id
                """,
                (session_id,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            if not include_system and r["role"] == "system":
                continue
            m: dict[str, Any] = {
                "id": r["id"],
                "role": r["role"],
                "content": r["content"],
                "created_at": r["created_at"],
            }
            if r["tool_calls_json"]:
                try:
                    m["tool_calls"] = json.loads(r["tool_calls_json"])
                except (ValueError, TypeError):
                    m["tool_calls"] = None
            if r["tool_call_id"]:
                m["tool_call_id"] = r["tool_call_id"]
            if r["tool_name"]:
                m["tool_name"] = r["tool_name"]
            out.append(m)
        return out

    def apply_retention(self) -> dict[str, int]:
        purged: dict[str, int] = {}
        with self._lock, self._conn() as conn:
            policies = conn.execute(
                "SELECT entity_type, retention_days FROM wt_retention_policies WHERE enabled = 1"
            ).fetchall()
            now = datetime.now(timezone.utc)
            table_map = {
                "signals": ("wt_signals", "created_at"),
                "crm_sync_events": ("wt_crm_sync_events", "created_at"),
                "feedback_events": ("wt_feedback_events", "created_at"),
                "llm_runs": ("wt_llm_runs", "created_at"),
            }
            for p in policies:
                et = str(p["entity_type"])
                if et not in table_map:
                    continue
                table, ts_col = table_map[et]
                cutoff = (now - timedelta(days=int(p["retention_days"]))).isoformat()
                before = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
                conn.execute(f"DELETE FROM {table} WHERE {ts_col} < ?", (cutoff,))
                after = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
                purged[et] = max(0, int(before["c"]) - int(after["c"]))
        return purged

    def get_llm_cache(self, cache_key: str) -> dict[str, Any] | None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT response_json
                FROM wt_llm_cache
                WHERE cache_key = ? AND expires_at > ?
                """,
                (cache_key, now),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["response_json"])

    def put_llm_cache(self, *, cache_key: str, response: dict[str, Any], ttl_seconds: int) -> None:
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=max(1, ttl_seconds))).isoformat()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO wt_llm_cache (cache_key, response_json, expires_at, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (cache_key, json.dumps(response), expires_at, _utc_now()),
            )

    def log_llm_run(
        self,
        *,
        task_name: str,
        model_name: str,
        prompt_hash: str,
        token_budget: int,
        evidence: dict[str, Any],
        response: dict[str, Any],
        cached_hit: bool = False,
        prompt_tokens: int | None = None,
        completion_tokens: int | None = None,
    ) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_llm_runs (
                    id, task_name, model_name, prompt_hash, token_budget, prompt_tokens, completion_tokens,
                    cached_hit, evidence_json, response_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"llm_{uuid4().hex}",
                    task_name,
                    model_name,
                    prompt_hash,
                    int(token_budget),
                    prompt_tokens,
                    completion_tokens,
                    1 if cached_hit else 0,
                    json.dumps(evidence),
                    json.dumps(response),
                    _utc_now(),
                ),
            )

    def list_llm_runs(self, task_name: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            if task_name:
                rows = conn.execute(
                    """
                    SELECT task_name, model_name, prompt_hash, token_budget, prompt_tokens, completion_tokens,
                           cached_hit, evidence_json, response_json, created_at
                    FROM wt_llm_runs
                    WHERE task_name = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (task_name, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT task_name, model_name, prompt_hash, token_budget, prompt_tokens, completion_tokens,
                           cached_hit, evidence_json, response_json, created_at
                    FROM wt_llm_runs
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["cached_hit"] = bool(payload["cached_hit"])
            payload["evidence"] = json.loads(payload.pop("evidence_json"))
            payload["response"] = json.loads(payload.pop("response_json"))
            out.append(payload)
        return out

    def add_feedback_event(
        self,
        *,
        event_type: str,
        account_id: str | None = None,
        draft_id: str | None = None,
        value_num: float | None = None,
        value_text: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_feedback_events (
                    id, account_id, draft_id, event_type, value_num, value_text, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"fb_{uuid4().hex}",
                    account_id,
                    draft_id,
                    event_type,
                    value_num,
                    value_text,
                    json.dumps(metadata or {}),
                    _utc_now(),
                ),
            )

    def list_feedback_events(self, event_type: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._conn() as conn:
            if event_type:
                rows = conn.execute(
                    """
                    SELECT account_id, draft_id, event_type, value_num, value_text, metadata_json, created_at
                    FROM wt_feedback_events
                    WHERE event_type = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (event_type, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT account_id, draft_id, event_type, value_num, value_text, metadata_json, created_at
                    FROM wt_feedback_events
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["metadata"] = json.loads(payload.pop("metadata_json"))
            out.append(payload)
        return out

    def save_quote(self, *, opportunity_id: str, quote: dict[str, Any], status: str = "draft") -> str:
        quote_id = f"qt_{uuid4().hex}"
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_quotes (id, opportunity_id, quote_json, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (quote_id, opportunity_id, json.dumps(quote), status, now, now),
            )
        return quote_id

    def get_quote(self, quote_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, opportunity_id, quote_json, status, created_at, updated_at
                FROM wt_quotes
                WHERE id = ?
                """,
                (quote_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown quote_id={quote_id}")
        payload = dict(row)
        payload["quote"] = json.loads(payload.pop("quote_json"))
        return payload

    def save_talent_shortlist(
        self,
        *,
        opportunity_id: str,
        candidates: list[dict[str, Any]],
        geography_id: str | None = None,
        status: str = "draft",
    ) -> str:
        shortlist_id = f"ts_{uuid4().hex}"
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO wt_talent_shortlists (id, opportunity_id, geography_id, candidates_json, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (shortlist_id, opportunity_id, geography_id, json.dumps(candidates), status, now, now),
            )
        return shortlist_id

    def get_talent_shortlist(self, shortlist_id: str) -> dict[str, Any]:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, opportunity_id, geography_id, candidates_json, status, created_at, updated_at
                FROM wt_talent_shortlists
                WHERE id = ?
                """,
                (shortlist_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown shortlist_id={shortlist_id}")
        payload = dict(row)
        payload["candidates"] = json.loads(payload.pop("candidates_json"))
        return payload

    def upsert_staffing_workflow(
        self,
        *,
        opportunity_id: str,
        state: str,
        owner_user_id: str | None,
        checklist: dict[str, Any],
    ) -> str:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            existing = conn.execute(
                "SELECT id FROM wt_staffing_workflows WHERE opportunity_id = ?",
                (opportunity_id,),
            ).fetchone()
            if existing is not None:
                conn.execute(
                    """
                    UPDATE wt_staffing_workflows
                    SET state = ?, owner_user_id = ?, checklist_json = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (state, owner_user_id, json.dumps(checklist), now, existing["id"]),
                )
                return str(existing["id"])
            workflow_id = f"sw_{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO wt_staffing_workflows (id, opportunity_id, state, owner_user_id, checklist_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (workflow_id, opportunity_id, state, owner_user_id, json.dumps(checklist), now, now),
            )
            return workflow_id

    def get_staffing_workflow(self, opportunity_id: str) -> dict[str, Any] | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, opportunity_id, state, owner_user_id, checklist_json, created_at, updated_at
                FROM wt_staffing_workflows
                WHERE opportunity_id = ?
                LIMIT 1
                """,
                (opportunity_id,),
            ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["checklist"] = json.loads(payload.pop("checklist_json"))
        return payload
