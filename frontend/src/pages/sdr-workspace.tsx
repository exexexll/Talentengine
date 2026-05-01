import React from "react";
import { marked } from "marked";
import { UniversalSearch } from "../components/UniversalSearch";
import "./sdr-workspace.css";

// Configure marked once: GitHub-flavored line breaks, no sanitization
// (we trust our own model output; links and headings must render).
marked.setOptions({ breaks: true, gfm: true });

function renderMarkdown(src: string): string {
  try {
    return marked.parse(src || "", { async: false }) as string;
  } catch {
    return src || "";
  }
}

// Short-TTL in-memory cache + in-flight dedupe for GET endpoints so rapid
// re-mounts (StrictMode, tab-switching) don't re-hit the backend.
const _sdrCache = new Map<string, { data: unknown; ts: number }>();
const _sdrInFlight = new Map<string, Promise<unknown>>();
async function sdrFetch<T>(url: string, ttlMs = 20_000): Promise<T> {
  const hit = _sdrCache.get(url);
  if (hit && Date.now() - hit.ts < ttlMs) return hit.data as T;
  const pending = _sdrInFlight.get(url);
  if (pending) return pending as Promise<T>;
  const p = (async () => {
    try {
      const res = await fetch(url, { credentials: "include" });
      if (!res.ok) throw new Error(`${res.status}`);
      const data = (await res.json()) as T;
      _sdrCache.set(url, { data, ts: Date.now() });
      return data;
    } finally {
      _sdrInFlight.delete(url);
    }
  })();
  _sdrInFlight.set(url, p);
  return p;
}
function invalidateSdrCache(prefix: string): void {
  for (const key of Array.from(_sdrCache.keys())) {
    if (key.includes(prefix)) _sdrCache.delete(key);
  }
  for (const key of Array.from(_sdrInFlight.keys())) {
    if (key.includes(prefix)) _sdrInFlight.delete(key);
  }
}

function toTs(value: unknown): number {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

type SdrTab = "inbox" | "queue" | "ingest" | "analytics" | "templates";
type AnalyticsSubTab = "pipeline" | "opportunities" | "accounts" | "operations";

type QueueRow = {
  draft_id: string;
  status: string;
  account_id: string;
  account_name?: string;
  domain?: string;
  contact_id?: string;
  contact_name?: string;
  contact_title?: string;
  contact_email?: string;
  signal_score: number;
  subject_a?: string;
  updated_at?: string;
  outreach_mode?: string | null;
  target_job_title?: string | null;
  target_job_url?: string | null;
  template_id?: string | null;
};

type AccountDetail = {
  account: Record<string, unknown>;
  identity_events: Array<Record<string, unknown>>;
  geo_attribution: Array<Record<string, unknown>>;
  signals: Array<Record<string, unknown>>;
  signal_stack: Record<string, unknown> | null;
  contacts: Array<Record<string, unknown>>;
  work_hypotheses: Array<Record<string, unknown>>;
  drafts: Array<Record<string, unknown>>;
};

type GeoScore = {
  geography_id: string;
  score_value: number;
  confidence: number;
};

type EmailTemplate = {
  id: string;
  name: string;
  subject_a: string;
  subject_b: string;
  email_body: string;
  followup_body: string;
  linkedin_dm: string;
  created_at: string;
  updated_at: string;
};

type SdrWorkspaceProps = {
  onBack: () => void;
  onOpenMap: () => void;
  /** Full-page analytics (``/worktrigger/analytics``) — optional for embedded shells. */
  onOpenWtAnalytics?: () => void;
};

function initials(name: string): string {
  return name.split(/\s+/).map(w => w[0] || "").join("").toUpperCase().slice(0, 2);
}

function ScoreBar({ value, max = 100 }: { value: number; max?: number }) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  const color = pct >= 70 ? "var(--sdr-success)" : pct >= 40 ? "var(--sdr-warning)" : "var(--sdr-danger)";
  return (
    <div className="sdr-geo-bar-track">
      <div className="sdr-geo-bar-fill" style={{ width: `${pct}%`, background: color }} />
    </div>
  );
}

export function SdrWorkspace({ onBack, onOpenMap, onOpenWtAnalytics }: SdrWorkspaceProps): JSX.Element {
  // Default to the keyboard-driven inbox.  The legacy "Review Queue"
  // (split list + detail pane) is still available under "Detailed" for
  // deep editing of a single account.
  const [tab, setTab] = React.useState<SdrTab>("inbox");
  const [rows, setRows] = React.useState<QueueRow[]>([]);
  const [statusFilter, setStatusFilter] = React.useState("active");
  const [search, setSearch] = React.useState("");
  const [sortBy, setSortBy] = React.useState<"date" | "signal">("date");
  const [loading, setLoading] = React.useState(false);
  const [selectedAccountId, setSelectedAccountId] = React.useState<string | null>(null);
  const [detail, setDetail] = React.useState<AccountDetail | null>(null);
  const [detailLoading, setDetailLoading] = React.useState(false);
  const [geoScores, setGeoScores] = React.useState<Record<string, GeoScore>>({});
  const [geoNames, setGeoNames] = React.useState<Record<string, string>>({});
  const [toast, setToast] = React.useState("");
  const [editModal, setEditModal] = React.useState<{ draftId: string; subject: string; body: string } | null>(null);
  const [analytics, setAnalytics] = React.useState<Record<string, unknown> | null>(null);
  const [heartbeats, setHeartbeats] = React.useState<Array<Record<string, unknown>>>([]);
  const [selectedForBulk, setSelectedForBulk] = React.useState<Set<string>>(new Set());
  const [bulkDeleting, setBulkDeleting] = React.useState(false);
  const [searchOpen, setSearchOpen] = React.useState(false);
  const [emailTemplates, setEmailTemplates] = React.useState<EmailTemplate[]>([]);
  const reviewerId = "sdr_operator_1";

  // Global Cmd-K / Ctrl-K shortcut for the universal search modal.
  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setSearchOpen(prev => !prev);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const toggleBulkSelect = (accountId: string) => {
    setSelectedForBulk(prev => {
      const n = new Set(prev);
      if (n.has(accountId)) n.delete(accountId); else n.add(accountId);
      return n;
    });
  };
  const clearBulkSelect = () => setSelectedForBulk(new Set());

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(""), 3500);
  };

  React.useEffect(() => {
    sdrFetch<Record<string, string>>("/api/geographies/names", 30 * 60 * 1000)
      .then(setGeoNames)
      .catch(() => {});
  }, []);

  const loadEmailTemplates = React.useCallback(async (force = false) => {
    try {
      if (force) invalidateSdrCache("/api/worktrigger/templates/email");
      const rows = await sdrFetch<EmailTemplate[]>("/api/worktrigger/templates/email", 60_000);
      setEmailTemplates(Array.isArray(rows) ? rows : []);
    } catch {
      setEmailTemplates([]);
    }
  }, []);

  React.useEffect(() => {
    void loadEmailTemplates();
  }, [loadEmailTemplates]);

  const [undraftedAccounts, setUndraftedAccounts] = React.useState<Array<Record<string, unknown>>>([]);
  const latestQueueRequestRef = React.useRef(0);

  const loadQueue = React.useCallback(async (force = false) => {
    const requestId = latestQueueRequestRef.current + 1;
    latestQueueRequestRef.current = requestId;
    const isStale = () => latestQueueRequestRef.current !== requestId;
    setLoading(true);
    try {
      if (force) {
        invalidateSdrCache("/api/worktrigger/queue");
        invalidateSdrCache("/api/worktrigger/accounts/all");
      }
      const [queueData, accts] = await Promise.all([
        sdrFetch<QueueRow[]>(`/api/worktrigger/queue?status=all&limit=500`),
        sdrFetch<Array<Record<string, unknown>>>(`/api/worktrigger/accounts/all?limit=500`),
      ]);
      if (isStale()) return;
      setRows(queueData);
      const acctIdsWithDrafts = new Set(queueData.map((d: QueueRow) => d.account_id));
      setUndraftedAccounts(accts.filter((a: Record<string, unknown>) => !acctIdsWithDrafts.has(a.id as string)));
    } catch (e) {
      if (!isStale()) flash(`Queue load failed: ${e instanceof Error ? e.message : "network error"}`);
    } finally {
      if (!isStale()) setLoading(false);
    }
  }, []);

  React.useEffect(() => { void loadQueue(); }, [loadQueue]);

  // Tracks the *latest* account the user asked to load.  Every async
  // step inside `loadDetail` checks this against its own captured
  // accountId BEFORE writing to React state — if the user has clicked
  // a different account in the meantime, the stale resolve is silently
  // dropped.  Without this guard the auto-hypothesis generation step
  // (a fresh LLM call that can take 5-10s) routinely won the race
  // against later clicks and overwrote the visible account's `detail`
  // with the first-clicked account's data.  That was the "Casper
  // College has overridden all of them" symptom.
  const latestDetailRequestRef = React.useRef<string | null>(null);
  const inflightDetailAbortRef = React.useRef<AbortController | null>(null);

  const loadDetail = React.useCallback(async (accountId: string) => {
    // Cancel any in-flight detail fetch for the prior account.  This
    // both saves bandwidth and ensures the prior fetch's `.then` chain
    // never runs to completion and overwrites state.
    inflightDetailAbortRef.current?.abort();
    const controller = new AbortController();
    inflightDetailAbortRef.current = controller;
    latestDetailRequestRef.current = accountId;

    setSelectedAccountId(accountId);
    setDetailLoading(true);
    // Optimistically clear stale detail so the UI cannot show another
    // account's hypothesis/contacts during the load — a blank state is
    // far less misleading than a misattributed one.
    setDetail(null);
    setGeoScores({});

    const isStale = () => latestDetailRequestRef.current !== accountId;

    try {
      const res = await fetch(
        `/api/worktrigger/accounts/${encodeURIComponent(accountId)}/detail`,
        { signal: controller.signal },
      );
      if (!res.ok) throw new Error(`${res.status}`);
      const d = (await res.json()) as AccountDetail;
      if (isStale()) return;
      setDetail(d);

      const geoIds = (d.geo_attribution || []).map(g => String(g.geography_id)).filter(Boolean);
      const newScores: Record<string, GeoScore> = {};
      await Promise.all(geoIds.slice(0, 5).map(async gid => {
        try {
          const r = await fetch(`/api/scores/${encodeURIComponent(gid)}`, { signal: controller.signal });
          if (r.ok) { newScores[gid] = await r.json(); }
        } catch { /* skip — abort or transient */ }
      }));
      if (isStale()) return;
      setGeoScores(newScores);

      if (d.work_hypotheses.length === 0) {
        try {
          // Fire-and-await the hypothesis generation, but check
          // `isStale()` after BOTH steps so a slow (~5-10s) LLM call
          // can never paint over a different account the user has
          // since selected.  Also propagate the abort signal so the
          // POST itself is cancelled when the user navigates away.
          await fetch(
            `/api/worktrigger/accounts/${encodeURIComponent(accountId)}/work-hypothesis`,
            { method: "POST", signal: controller.signal },
          );
          if (isStale()) return;
          const refreshed = await fetch(
            `/api/worktrigger/accounts/${encodeURIComponent(accountId)}/detail`,
            { signal: controller.signal },
          );
          if (isStale()) return;
          if (refreshed.ok) {
            const rd = await refreshed.json();
            if (isStale()) return;
            setDetail(rd as AccountDetail);
          }
        } catch { /* hypothesis generation is best-effort */ }
      }
    } catch (e) {
      // Aborts manifest as DOMException("AbortError") — that's expected
      // when the user clicked another account.  Only blank out detail
      // for genuine failures on the *current* request.
      const isAbort = e instanceof DOMException && e.name === "AbortError";
      if (!isAbort && !isStale()) setDetail(null);
    } finally {
      // Only clear the spinner if we're still the active request.
      if (!isStale()) setDetailLoading(false);
    }
  }, []);

  const loadAnalytics = React.useCallback(async () => {
    try {
      const [s, h] = await Promise.all([
        sdrFetch<Record<string, unknown>>("/api/worktrigger/analytics/summary", 30_000).catch(() => null),
        sdrFetch<Array<Record<string, unknown>>>("/api/worktrigger/worker/heartbeats", 30_000).catch(() => null),
      ]);
      if (s) setAnalytics(s);
      if (h) setHeartbeats(h);
    } catch { /* empty */ }
  }, []);

  React.useEffect(() => { if (tab === "analytics") void loadAnalytics(); }, [tab, loadAnalytics]);

  const actionLabels: Record<string, string> = {
    approve: "approved", edit_and_approve: "approved (edited)", discard: "discarded",
    unapprove: "moved back to draft", snooze: "snoozed", reroute_contact: "rerouted (contact)", reroute_angle: "rerouted (angle)",
  };

  const act = async (draftId: string, action: string, extra?: Record<string, unknown>) => {
    try {
      const res = await fetch(`/api/worktrigger/drafts/${encodeURIComponent(draftId)}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, reviewer_user_id: reviewerId, ...extra }),
      });
      if (!res.ok) { flash(`Review failed: ${await res.text()}`); return; }
      flash(`Draft ${actionLabels[action] ?? action}`);
      await loadQueue(true);
      if (selectedAccountId) void loadDetail(selectedAccountId);
    } catch (e) {
      flash(`Review error: ${e instanceof Error ? e.message : "unknown"}`);
    }
  };

  const generateHypothesis = async () => {
    if (!selectedAccountId) return;
    flash("Generating hypothesis...");
    const res = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(selectedAccountId)}/work-hypothesis`, { method: "POST" });
    if (!res.ok) { flash("Hypothesis failed"); return; }
    flash("Hypothesis generated");
    void loadDetail(selectedAccountId);
  };

  const generateDraft = async (contactId?: string, templateId?: string) => {
    if (!selectedAccountId || !detail) return;
    const h = detail.work_hypotheses[0];
    if (!h) { flash("Generate a hypothesis first"); return; }
    const cid = contactId || detail.contacts[0]?.id;
    if (!cid) { flash("Add a contact first"); return; }
    flash("Generating draft...");
    const res = await fetch("/api/worktrigger/drafts/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        account_id: selectedAccountId,
        contact_id: String(cid),
        work_hypothesis_id: String(h.id),
        channel: "email",
        ...(templateId ? { template_id: templateId } : {}),
      }),
    });
    if (!res.ok) { flash("Draft generation failed"); return; }
    flash("Draft generated");
    invalidateSdrCache(`/api/worktrigger/accounts/${selectedAccountId}/detail`);
    invalidateSdrCache("/api/worktrigger/drafts/");
    await loadQueue(true);
    void loadDetail(selectedAccountId);
  };

  const sendDraft = async (draftId: string) => {
    const res = await fetch(`/api/worktrigger/drafts/${encodeURIComponent(draftId)}/send`, { method: "POST" });
    if (!res.ok) { flash("Send failed: " + (await res.text())); return; }
    flash("Sent!");
    await loadQueue(true);
    if (selectedAccountId) void loadDetail(selectedAccountId);
  };

  const syncCrm = async (draftId: string) => {
    try {
      const res = await fetch(`/api/worktrigger/crm/sync/opportunity?draft_id=${encodeURIComponent(draftId)}`, { method: "POST" });
      if (!res.ok) { flash("CRM sync failed: " + (await res.text())); return; }
      flash("CRM synced");
      await loadQueue(true);
      if (selectedAccountId) void loadDetail(selectedAccountId);
    } catch (e) {
      flash(`CRM sync error: ${e instanceof Error ? e.message : "unknown"}`);
    }
  };

  const filtered = rows.filter(r => {
    if (statusFilter === "active" && (r.status === "discarded" || r.status === "snoozed")) return false;
    if (search) {
      const q = search.toLowerCase();
      if (!(r.account_name || "").toLowerCase().includes(q) && !(r.domain || "").toLowerCase().includes(q) && !(r.contact_name || "").toLowerCase().includes(q)) return false;
    }
    return true;
  }).sort((a, b) => {
    if (sortBy === "signal") return (b.signal_score || 0) - (a.signal_score || 0);
    return toTs(b.updated_at) - toTs(a.updated_at);
  });

  return (
    <div className="sdr-root">
      {/* Top bar */}
      <div className="sdr-topbar">
        <span className="sdr-topbar-logo">Figwork</span>
        <div className="sdr-topbar-divider" />
        <button className="sdr-topbar-link" onClick={onOpenMap}>Talent Map</button>
        <button className="sdr-topbar-link" data-active="true">SDR Workspace</button>
        {onOpenWtAnalytics ? (
          <button type="button" className="sdr-topbar-link" onClick={onOpenWtAnalytics}>
            Full analytics
          </button>
        ) : null}
        <button className="sdr-topbar-link" onClick={onBack}>Back</button>
        <button
          className="sdr-topbar-search"
          onClick={() => setSearchOpen(true)}
          title="Search (⌘K)"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.35-4.35" />
          </svg>
          <span>Search companies, people…</span>
          <kbd>⌘K</kbd>
        </button>
      </div>

      {/* Tabs */}
      <div className="sdr-tabs">
        <button className="sdr-tab" data-active={tab === "inbox"} onClick={() => setTab("inbox")}>Inbox</button>
        <button className="sdr-tab" data-active={tab === "queue"} onClick={() => setTab("queue")}>Detailed</button>
        <button className="sdr-tab" data-active={tab === "templates"} onClick={() => setTab("templates")}>Templates</button>
        <button className="sdr-tab" data-active={tab === "analytics"} onClick={() => setTab("analytics")}>Analytics</button>
        <button className="sdr-tab" data-active={tab === "ingest"} onClick={() => setTab("ingest")}>Ingest</button>
      </div>

      {/* Main */}
      <div className="sdr-main">
        {tab === "inbox" ? (
          <OutreachInbox
            rows={rows}
            onAct={async (draftId, action, extra) => act(draftId, action, extra)}
            onSend={async (draftId) => sendDraft(draftId)}
            onGenerateDraft={async (accountId, contactId, templateId) => {
              try {
                const dres = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(accountId)}/detail`);
                if (!dres.ok) { flash("Could not load account detail for draft generation"); return; }
                const d = (await dres.json()) as AccountDetail;
                const h = d.work_hypotheses?.[0];
                if (!h?.id) { flash("Generate a hypothesis first (Detailed tab)"); return; }
                const cid = contactId || d.contacts?.[0]?.id;
                if (!cid) { flash("No contact with email available for draft generation"); return; }
                const res = await fetch("/api/worktrigger/drafts/generate", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    account_id: accountId,
                    contact_id: String(cid),
                    work_hypothesis_id: String(h.id),
                    channel: "email",
                    ...(templateId ? { template_id: templateId } : {}),
                  }),
                });
                if (!res.ok) { flash("Draft generation failed"); return; }
                flash("Draft generated");
                invalidateSdrCache(`/api/worktrigger/accounts/${accountId}/detail`);
                invalidateSdrCache("/api/worktrigger/drafts/");
                await loadQueue(true);
              } catch (e) {
                flash(`Draft generation error: ${e instanceof Error ? e.message : "network error"}`);
              }
            }}
            onRefresh={() => loadQueue(true)}
            onOpenDetailed={(accountId) => {
              setTab("queue");
              void loadDetail(accountId);
            }}
            geoNames={geoNames}
            emailTemplates={emailTemplates}
            flash={flash}
            loading={loading}
          />
        ) : null}
        {tab === "queue" ? (
          <div className="sdr-split">
            {/* Left: Queue list */}
            <div className="sdr-list">
              <div className="sdr-list-toolbar">
                <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)} style={{ fontSize: 11 }}>
                  <option value="active">Active</option>
                  <option value="all">All</option>
                  <option value="draft_ready">Draft</option>
                  <option value="approved">Approved</option>
                  <option value="sent">Sent</option>
                  <option value="replied">Replied</option>
                  <option value="discarded">Discarded</option>
                  <option value="snoozed">Snoozed</option>
                </select>
                <select value={sortBy} onChange={e => setSortBy(e.target.value as "date" | "signal")} style={{ fontSize: 11, width: 70 }}>
                  <option value="date">Newest</option>
                  <option value="signal">Signal</option>
                </select>
                <input placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)} style={{ minWidth: 80 }} />
                <button className="sdr-btn sdr-btn-sm" onClick={() => void loadQueue(true)}>Refresh</button>
              </div>

              {/* Bulk action bar — appears when any cards are selected */}
              {selectedForBulk.size > 0 ? (
                <div className="sdr-bulk-bar">
                  <span className="sdr-bulk-count">{selectedForBulk.size} selected</span>
                  <button
                    className="sdr-btn sdr-btn-danger sdr-btn-sm"
                    disabled={bulkDeleting}
                    onClick={async () => {
                      const ids = Array.from(selectedForBulk);
                      if (!confirm(`Permanently delete ${ids.length} compan${ids.length === 1 ? "y" : "ies"} and all their drafts, contacts, and signals? This cannot be undone.`)) return;
                      setBulkDeleting(true);
                      try {
                        const res = await fetch("/api/worktrigger/accounts/bulk-delete", {
                          method: "POST",
                          headers: { "Content-Type": "application/json" },
                          body: JSON.stringify(ids),
                        });
                        if (res.ok) {
                          const data = await res.json();
                          flash(`Deleted ${data.deleted} compan${data.deleted === 1 ? "y" : "ies"}`);
                          clearBulkSelect();
                          if (ids.includes(selectedAccountId || "")) {
                            setSelectedAccountId(null);
                            setDetail(null);
                          }
                          await loadQueue(true);
                        } else {
                          flash("Delete failed");
                        }
                      } catch (e) {
                        flash(`Delete error: ${e instanceof Error ? e.message : "network"}`);
                      } finally {
                        setBulkDeleting(false);
                      }
                    }}
                  >
                    {bulkDeleting ? "Deleting…" : `Delete ${selectedForBulk.size}`}
                  </button>
                  <button className="sdr-btn sdr-btn-sm" onClick={clearBulkSelect}>Clear</button>
                </div>
              ) : null}

              {/* Select all visible — subtle toolbar row shown above the list */}
              {!loading && (filtered.length > 0 || undraftedAccounts.length > 0) ? (
                <div className="sdr-list-selectall">
                  {(() => {
                    const visibleAccountIds = new Set<string>();
                    for (const r of filtered) {
                      if (r.account_id) visibleAccountIds.add(r.account_id);
                    }
                    if (statusFilter === "active") {
                      for (const a of undraftedAccounts) {
                        const id = String(a.id || "");
                        if (id && !Array.from(visibleAccountIds).some(x => x === id)) visibleAccountIds.add(id);
                      }
                    }
                    const allSelected = visibleAccountIds.size > 0 && Array.from(visibleAccountIds).every(id => selectedForBulk.has(id));
                    return (
                      <>
                        <input
                          type="checkbox"
                          checked={allSelected}
                          onChange={() => {
                            setSelectedForBulk(prev => {
                              if (allSelected) {
                                const n = new Set(prev);
                                for (const id of visibleAccountIds) n.delete(id);
                                return n;
                              }
                              const n = new Set(prev);
                              for (const id of visibleAccountIds) n.add(id);
                              return n;
                            });
                          }}
                        />
                        <span>{allSelected ? "Deselect" : "Select"} all {visibleAccountIds.size} visible</span>
                      </>
                    );
                  })()}
                </div>
              ) : null}
              {loading ? (
                Array.from({ length: 6 }).map((_, i) => (
                  <div key={`sdrsk-${i}`} className="sdr-skeleton-card">
                    <div className="sdr-skel-bar" style={{ width: "45%", height: 9 }} />
                    <div className="sdr-skel-bar" style={{ width: "75%", height: 12 }} />
                    <div className="sdr-skel-bar" style={{ width: "55%", height: 8 }} />
                  </div>
                ))
              ) : null}
              {!loading && filtered.length === 0 && undraftedAccounts.length === 0 ? <div className="sdr-empty">No companies found</div> : null}
              {/* Unified list: undrafted accounts + drafts merged, sorted together */}
              {(() => {
                type QueueItem = {
                  type: string; id: string; name: string; domain: string; industry: string;
                  signal: number; status: string; contactName: string; contactTitle: string;
                  contactEmail: string; subject: string; draftId: string; accountId: string;
                  extraDrafts: number;
                };
                const items: QueueItem[] = [];
                const acctIdsFromDrafts = new Set<string>();

                // Dedupe drafts by account_id so each company appears as
                // exactly ONE card.  The "winning" draft for the card is
                // the one most relevant to the SDR right now, picked by
                // status priority (draft_ready > approved > sent > replied
                // > snoozed > discarded), then by most-recent updated_at.
                // The detail pane still surfaces every contact's latest
                // active draft when the card is opened.
                const STATUS_PRIORITY: Record<string, number> = {
                  draft_ready: 0, approved: 1, sent: 2, replied: 3, snoozed: 4, discarded: 5,
                };
                const byAccount = new Map<string, QueueRow>();
                for (const row of filtered) {
                  const acct = row.account_id || row.draft_id;
                  const existing = byAccount.get(acct);
                  if (!existing) { byAccount.set(acct, row); continue; }
                  const a = STATUS_PRIORITY[existing.status] ?? 99;
                  const b = STATUS_PRIORITY[row.status] ?? 99;
                  if (b < a) { byAccount.set(acct, row); continue; }
                  if (b === a && toTs(row.updated_at) > toTs(existing.updated_at)) {
                    byAccount.set(acct, row);
                  }
                }
                const draftCountByAccount = new Map<string, number>();
                for (const row of filtered) {
                  const k = row.account_id || row.draft_id;
                  draftCountByAccount.set(k, (draftCountByAccount.get(k) || 0) + 1);
                }
                for (const row of byAccount.values()) {
                  acctIdsFromDrafts.add(row.account_id);
                  const totalDrafts = draftCountByAccount.get(row.account_id || row.draft_id) || 1;
                  items.push({
                    type: "draft", id: row.draft_id, name: row.account_name || row.domain || "",
                    domain: row.domain || "", industry: "", signal: row.signal_score, status: row.status,
                    contactName: row.contact_name || "", contactTitle: row.contact_title || "",
                    contactEmail: row.contact_email || "", subject: row.subject_a || "",
                    draftId: row.draft_id, accountId: row.account_id || "",
                    extraDrafts: totalDrafts - 1,
                  } as QueueItem);
                }
                if (statusFilter === "active") {
                  for (const a of undraftedAccounts) {
                    if (!acctIdsFromDrafts.has(String(a.id))) {
                      items.push({ type: "account", id: String(a.id), name: String(a.name || a.domain || ""), domain: String(a.domain || ""), industry: String(a.industry || ""), signal: Number(a.signal_score || 0), status: "new", contactName: "", contactTitle: "", contactEmail: "", subject: "", draftId: "", accountId: String(a.id), extraDrafts: 0 });
                    }
                  }
                }

                items.sort((a, b) => sortBy === "signal" ? b.signal - a.signal : 0);

                return items.map(item => {
                  const accountId = item.accountId || item.id;
                  const hasDraft = Boolean(item.draftId);
                  const statusLabel = hasDraft
                    ? (STATUS_CONFIG[item.status] || STATUS_CONFIG.draft_ready).label
                    : "Pipeline";
                  const statusColor = hasDraft
                    ? (STATUS_CONFIG[item.status] || STATUS_CONFIG.draft_ready).color
                    : "#6b7280";
                  const statusBg = hasDraft
                    ? (STATUS_CONFIG[item.status] || STATUS_CONFIG.draft_ready).bg
                    : "#f3f4f6";

                  const isChecked = selectedForBulk.has(accountId);
                  return (
                    <div
                      key={item.id}
                      className={`sdr-card ${isChecked ? "sdr-card-bulk-selected" : ""}`}
                      data-selected={selectedAccountId === accountId}
                      onClick={() => void loadDetail(accountId)}
                      style={item.status === "discarded" || item.status === "snoozed" ? { opacity: 0.5 } : undefined}
                    >
                      <div className="sdr-card-head-row">
                        <input
                          type="checkbox"
                          className="sdr-card-check"
                          checked={isChecked}
                          onClick={e => e.stopPropagation()}
                          onChange={() => toggleBulkSelect(accountId)}
                          title="Select for bulk delete"
                        />
                        <span className="sdr-card-status" style={{ color: statusColor, background: statusBg }}>{statusLabel}</span>
                        <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: "auto" }}>{item.signal > 0 ? `Signal ${item.signal.toFixed(0)}` : ""}</span>
                      </div>
                      <div style={{ fontWeight: 700, fontSize: 13, color: "#111827" }}>{item.name}</div>
                      <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 2 }}>
                        {item.domain}{item.industry ? ` · ${item.industry}` : ""}
                      </div>
                      {item.contactName ? (
                        <div style={{ fontSize: 11, color: "#2563eb" }}>
                          {item.contactName}{item.contactEmail ? ` · ${item.contactEmail}` : ""}
                          {item.extraDrafts > 0 ? (
                            <span
                              style={{ marginLeft: 6, fontSize: 9, color: "#9ca3af", fontWeight: 500 }}
                              title={`${item.extraDrafts} additional draft${item.extraDrafts === 1 ? "" : "s"} on this account (open the card to see all contacts)`}
                            >
                              · +{item.extraDrafts} more
                            </span>
                          ) : null}
                        </div>
                      ) : null}
                      {item.subject ? (
                        <div style={{ fontSize: 11, color: "#374151", fontStyle: "italic", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", marginTop: 2 }}>{item.subject}</div>
                      ) : null}
                      {hasDraft ? (
                        <div className="sdr-card-actions">
                          {item.status === "draft_ready" ? (
                            <>
                              <button className="sdr-btn sdr-btn-primary sdr-btn-sm" onClick={e => { e.stopPropagation(); void act(item.draftId, "approve"); }}>Approve</button>
                              <button className="sdr-btn sdr-btn-danger sdr-btn-sm" onClick={e => { e.stopPropagation(); void act(item.draftId, "discard"); }}>Discard</button>
                            </>
                          ) : null}
                          {item.status === "approved" ? (
                            <>
                              <button className="sdr-btn sdr-btn-success sdr-btn-sm" onClick={e => { e.stopPropagation(); void sendDraft(item.draftId); }}>Send</button>
                              <button className="sdr-btn sdr-btn-sm" onClick={e => { e.stopPropagation(); void act(item.draftId, "unapprove"); }}>Move to Draft</button>
                            </>
                          ) : null}
                          {item.status === "sent" ? (
                            <span style={{ fontSize: 11, color: "#059669", fontWeight: 600 }}>Sent ✓</span>
                          ) : null}
                          {item.status === "discarded" || item.status === "snoozed" ? (
                            <button className="sdr-btn sdr-btn-sm" onClick={e => { e.stopPropagation(); void act(item.draftId, "approve"); }}>Restore</button>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  );
                });
              })()}
            </div>

            {/* Right: Detail pane */}
            <div className="sdr-detail">
              {!selectedAccountId ? <div className="sdr-empty">Select an account from the queue</div> : null}
              {selectedAccountId && detailLoading ? <div className="sdr-empty">Loading account...</div> : null}
              {selectedAccountId && !detailLoading && !detail ? <div className="sdr-empty">Failed to load account</div> : null}
              {detail ? (
                <AccountDetailPane
                  detail={detail}
                  geoScores={geoScores}
                  geoNames={geoNames}
                  onGenerateDraft={(cid, templateId) => void generateDraft(cid, templateId)}
                  onRefreshDetail={() => { if (selectedAccountId) void loadDetail(selectedAccountId); }}
                  emailTemplates={emailTemplates}
                  onAccountDeleted={async () => {
                    flash("Account deleted");
                    setSelectedAccountId(null);
                    setDetail(null);
                    setSelectedForBulk(prev => {
                      const n = new Set(prev);
                      if (selectedAccountId) n.delete(selectedAccountId);
                      return n;
                    });
                    await loadQueue(true);
                  }}
                />
              ) : null}
            </div>
          </div>
        ) : null}

        {tab === "ingest" ? <IngestPanel onDone={() => { flash("Signal ingested"); setTab("queue"); void loadQueue(); }} /> : null}

        {tab === "templates" ? (
          <EmailTemplatesPanel
            templates={emailTemplates}
            onTemplatesChanged={() => { void loadEmailTemplates(true); }}
            flash={flash}
          />
        ) : null}

        {tab === "analytics" ? <AnalyticsPanel analytics={analytics} heartbeats={heartbeats} onRefresh={loadAnalytics} /> : null}
      </div>

      {/* Edit modal */}
      {editModal ? (
        <div className="sdr-modal-overlay" onClick={() => setEditModal(null)}>
          <div className="sdr-modal" onClick={e => e.stopPropagation()}>
            <h3>Edit and Approve Draft</h3>
            <div className="sdr-field">
              <label>Subject</label>
              <input value={editModal.subject} onChange={e => setEditModal({ ...editModal, subject: e.target.value })} />
            </div>
            <div className="sdr-field" style={{ marginTop: 10 }}>
              <label>Body</label>
              <textarea value={editModal.body} onChange={e => setEditModal({ ...editModal, body: e.target.value })} rows={8} />
            </div>
            <div className="sdr-modal-actions">
              <button className="sdr-btn sdr-btn-primary" onClick={async () => {
                await act(editModal.draftId, "edit_and_approve", { edited_subject: editModal.subject, edited_body: editModal.body });
                setEditModal(null);
              }}>Save and Approve</button>
              <button className="sdr-btn" onClick={() => setEditModal(null)}>Cancel</button>
            </div>
          </div>
        </div>
      ) : null}

      {/* Toast */}
      {toast ? <div className="sdr-toast">{toast}</div> : null}

      {/* Universal search (⌘K) — wired to the same actions as the map */}
      <UniversalSearch
        open={searchOpen}
        onClose={() => setSearchOpen(false)}
        onOpenAccount={(accountId) => {
          setTab("queue");
          void loadDetail(accountId);
        }}
        onIntakeComplete={() => { void loadQueue(true); }}
        flash={flash}
      />
    </div>
  );
}


function EditableText({ value, onChange, multiline }: { value: string; onChange: (v: string) => void; multiline?: boolean }) {
  const [editing, setEditing] = React.useState(false);
  if (editing) {
    const shared = { value, onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => onChange(e.target.value), onBlur: () => setEditing(false), autoFocus: true, style: { width: "100%", padding: "4px 6px", fontSize: 13, lineHeight: "1.5", border: "1px solid var(--sdr-primary)", borderRadius: 3, background: "#fff", outline: "none" } as React.CSSProperties };
    return multiline ? <textarea {...shared} rows={3} /> : <input {...shared} />;
  }
  return <span onClick={() => setEditing(true)} style={{ cursor: "text", borderBottom: "1px dashed #cbd5e1" }}>{value || "(click to edit)"}</span>;
}

function HypothesisCard({ hypo }: { hypo: Record<string, unknown> }) {
  const [problem, setProblem] = React.useState(String(hypo.probable_problem || ""));
  const [deliverable, setDeliverable] = React.useState(String(hypo.probable_deliverable || ""));
  const [archetype, setArchetype] = React.useState(String(hypo.talent_archetype || ""));

  return (
    <div style={{ display: "grid", gap: 10 }}>
      <div>
        <div className="sdr-hypo-label">Probable Problem</div>
        <EditableText value={problem} onChange={setProblem} multiline />
      </div>
      <div>
        <div className="sdr-hypo-label">Probable Deliverable</div>
        <EditableText value={deliverable} onChange={setDeliverable} multiline />
      </div>
      <div>
        <div className="sdr-hypo-label">Talent Archetype</div>
        <EditableText value={archetype} onChange={setArchetype} />
      </div>
      <div className="sdr-hypo-scores">
        <span className="sdr-hypo-score-pill">Urgency {Number(hypo.urgency_score || 0).toFixed(0)}</span>
        <span className="sdr-hypo-score-pill">Taskability {Number(hypo.taskability_score || 0).toFixed(0)}</span>
        <span className="sdr-hypo-score-pill">Fit {Number(hypo.fit_score || 0).toFixed(0)}</span>
        <span className="sdr-hypo-score-pill">Confidence {(Number(hypo.confidence_score || 0) * 100).toFixed(0)}%</span>
      </div>
      {Array.isArray(hypo.rationale) && (hypo.rationale as string[]).length > 0 ? (
        <div style={{ marginTop: 4 }}>
          <div className="sdr-hypo-label">Evidence</div>
          <ul style={{ margin: 0, paddingLeft: 16, fontSize: 12, color: "#374151", lineHeight: 1.5 }}>
            {(hypo.rationale as string[]).map((r, i) => <li key={i}>{r}</li>)}
          </ul>
        </div>
      ) : null}
    </div>
  );
}


function DraftCard({ draft: d, onRefresh }: { draft: Record<string, unknown>; onRefresh: () => void }) {
  const [subject, setSubject] = React.useState(String(d.subject_a || ""));
  const [body, setBody] = React.useState(String(d.email_body || ""));
  const [showFollowup, setShowFollowup] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [dirty, setDirty] = React.useState(false);

  const saveDraft = async () => {
    setSaving(true);
    try {
      await fetch(`/api/worktrigger/drafts/${encodeURIComponent(String(d.id))}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "edit_and_approve", reviewer_user_id: "sdr_operator_1", edited_subject: subject, edited_body: body }),
      });
      setDirty(false);
      onRefresh();
    } catch { /* silent */ }
    finally { setSaving(false); }
  };

  const contactName = String(d.contact_name || "");
  const contactEmail = String(d.contact_email || "");

  return (
    <div>
      {/* Header row */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span className={`sdr-card-badge sdr-badge-${d.status || "draft_ready"}`}>{String(d.status || "").replace(/_/g, " ")}</span>
          {contactName || contactEmail ? (
            <span style={{ fontSize: 12, color: "#6b7280" }}>to {contactName}{contactEmail ? ` (${contactEmail})` : ""}</span>
          ) : null}
        </div>
        {dirty ? <button className="sdr-btn sdr-btn-primary sdr-btn-sm" style={{ padding: "4px 12px" }} disabled={saving} onClick={() => void saveDraft()}>{saving ? "Saving..." : "Save & Approve"}</button> : null}
      </div>
      {/* Subject */}
      <input
        value={subject}
        onChange={e => { setSubject(e.target.value); setDirty(true); }}
        style={{ width: "100%", padding: "8px 10px", fontSize: 14, fontWeight: 600, border: "1px solid #d1d5db", borderRadius: 6, marginBottom: 8, fontFamily: "inherit", color: "#111827" }}
        placeholder="Subject line"
      />
      {/* Body */}
      <textarea
        value={body}
        onChange={e => { setBody(e.target.value); setDirty(true); }}
        rows={8}
        style={{ width: "100%", padding: "10px", fontSize: 13, lineHeight: 1.7, border: "1px solid #d1d5db", borderRadius: 6, fontFamily: "inherit", color: "#374151", resize: "vertical" }}
      />
      {/* Follow-up toggle */}
      {d.followup_body ? (
        <div style={{ marginTop: 6 }}>
          <button
            style={{ background: "none", border: "none", color: "#2563eb", fontSize: 12, cursor: "pointer", padding: 0 }}
            onClick={() => setShowFollowup(!showFollowup)}
          >{showFollowup ? "Hide follow-up ▲" : "Show follow-up ▼"}</button>
          {showFollowup ? (
            <div style={{ marginTop: 6, padding: 10, background: "#f9fafb", borderRadius: 6, border: "1px solid #e5e7eb", fontSize: 13, lineHeight: 1.7, color: "#374151", whiteSpace: "pre-wrap" }}>
              {String(d.followup_body)}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}


const STATUS_CONFIG: Record<string, { label: string; color: string; bg: string }> = {
  draft_ready: { label: "Draft", color: "#6b7280", bg: "#f3f4f6" },
  approved: { label: "Ready to Send", color: "#2563eb", bg: "#eff6ff" },
  sent: { label: "Sent", color: "#059669", bg: "#ecfdf5" },
  replied: { label: "Replied", color: "#7c3aed", bg: "#f5f3ff" },
  discarded: { label: "Discarded", color: "#dc2626", bg: "#fef2f2" },
  snoozed: { label: "Snoozed", color: "#d97706", bg: "#fffbeb" },
};

// ─────────────────────────────────────────────────────────────────────
// OutreachInbox — high-volume triage view.  Designed for SDRs working
// thousands of emails per day:
//   • Card stack (one card per active draft) with the email body inline
//     so the SDR doesn't have to click into a detail pane to triage.
//   • Keyboard-first: J/K to move, A approve, D discard, S send, E edit,
//     X bulk-select, Shift-A approve all selected, Shift-S send all
//     approved, / focus search, ? show shortcut help.
//   • Right rail (collapsible via I) shows account context for the
//     focused card without leaving the page.
//   • Filter by status + min signal score + targeted-only + search,
//     all in a single sticky toolbar.
// ─────────────────────────────────────────────────────────────────────

type InboxStatusFilter = "all_active" | "draft_ready" | "approved" | "sent" | "replied";

function OutreachInbox({
  rows, onAct, onSend, onGenerateDraft, onRefresh, onOpenDetailed, geoNames, emailTemplates, flash, loading,
}: {
  rows: QueueRow[];
  onAct: (draftId: string, action: string, extra?: Record<string, unknown>) => Promise<void>;
  onSend: (draftId: string) => Promise<void>;
  onGenerateDraft: (accountId: string, contactId?: string, templateId?: string) => Promise<void>;
  onRefresh: () => void;
  onOpenDetailed: (accountId: string) => void;
  geoNames: Record<string, string>;
  emailTemplates: EmailTemplate[];
  flash: (msg: string) => void;
  loading: boolean;
}) {
  void geoNames;
  const [statusFilter, setStatusFilter] = React.useState<InboxStatusFilter>("draft_ready");
  const [search, setSearch] = React.useState("");
  const [minSignal, setMinSignal] = React.useState(0);
  const [targetedOnly, setTargetedOnly] = React.useState(false);
  // "signal" = highest signal score first (default — surfaces hottest
  // accounts to triage now).  "recent" = most recently created/edited
  // first (useful when you regenerate or import a batch and want to
  // see the new drafts at the top).
  const [sortBy, setSortBy] = React.useState<"signal" | "recent">("signal");
  const [activeIdx, setActiveIdx] = React.useState(0);
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [contextOpen, setContextOpen] = React.useState(true);
  const [showHelp, setShowHelp] = React.useState(false);
  const [bodyExpanded, setBodyExpanded] = React.useState<Set<string>>(new Set());
  const [bulkBusy, setBulkBusy] = React.useState(false);
  const searchRef = React.useRef<HTMLInputElement>(null);
  const listRef = React.useRef<HTMLDivElement>(null);

  // Filter, dedupe, and sort once per render — quick even at thousands of rows.
  //
  // Each card represents ONE email going to ONE person, so cards are
  // keyed by (account, contact, channel).  Multiple drafts targeting
  // the same recipient (which can happen when an SDR clicks "+ Draft"
  // multiple times on the same contact in the Detailed view) collapse
  // into a single card showing the most recent active draft — same
  // semantics as the Detailed view's contact list, so the two views
  // stay consistent.
  //
  // After dedup, cards are sorted by:
  //   1. signal score (hottest first)
  //   2. account_id (stable secondary key) — so multiple recipients at
  //      the same company appear in adjacent rows, which lets the
  //      header tag highlight a same-account group at a glance.
  const filtered = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    const matches = rows.filter(r => {
      const status = r.status || "";
      if (statusFilter === "all_active") {
        if (status === "discarded" || status === "snoozed") return false;
      } else if (status !== statusFilter) {
        return false;
      }
      if (minSignal > 0 && (r.signal_score || 0) < minSignal) return false;
      if (targetedOnly && !r.target_job_title) return false;
      if (!q) return true;
      return (
        (r.account_name || "").toLowerCase().includes(q)
        || (r.domain || "").toLowerCase().includes(q)
        || (r.contact_name || "").toLowerCase().includes(q)
        || (r.subject_a || "").toLowerCase().includes(q)
      );
    });

    // Dedupe: keep latest active draft per (account, contact).
    const STATUS_PRIORITY: Record<string, number> = {
      draft_ready: 0, approved: 1, sent: 2, replied: 3, snoozed: 4, discarded: 5,
    };
    const winner = new Map<string, QueueRow>();
    for (const r of matches) {
      const key = `${r.account_id}::${r.contact_id || ""}`;
      const prev = winner.get(key);
      if (!prev) { winner.set(key, r); continue; }
      const a = STATUS_PRIORITY[prev.status] ?? 99;
      const b = STATUS_PRIORITY[r.status] ?? 99;
      if (b < a || (b === a && toTs(r.updated_at) > toTs(prev.updated_at))) {
        winner.set(key, r);
      }
    }

    return Array.from(winner.values()).sort((a, b) => {
      if (sortBy === "recent") {
        // updated_at descending; ties fall through to signal so that
        // simultaneously-created drafts still surface the hottest one
        // first.  Adjacent same-account grouping isn't applied for
        // recent-sort because the user explicitly asked for time order.
        const ta = toTs(a.updated_at);
        const tb = toTs(b.updated_at);
        if (tb !== ta) return tb - ta;
        return (b.signal_score || 0) - (a.signal_score || 0);
      }
      // sortBy === "signal" (default)
      const ds = (b.signal_score || 0) - (a.signal_score || 0);
      if (ds !== 0) return ds;
      // tiebreak: same-account rows together (so multiple recipients
      // at the same company appear adjacent, which the visual
      // grouping treatment then collapses into one logical block).
      return (a.account_id || "").localeCompare(b.account_id || "");
    });
  }, [rows, statusFilter, search, minSignal, targetedOnly, sortBy]);

  React.useEffect(() => {
    if (activeIdx >= filtered.length) setActiveIdx(Math.max(0, filtered.length - 1));
  }, [filtered.length, activeIdx]);

  React.useEffect(() => {
    // Strip selections that are no longer in the visible list.
    setSelected(prev => {
      const visible = new Set(filtered.map(r => r.draft_id));
      const next = new Set<string>();
      for (const id of prev) if (visible.has(id)) next.add(id);
      return next;
    });
  }, [filtered]);

  // Scroll active card into view when navigating with keys.
  React.useEffect(() => {
    const el = listRef.current?.querySelector<HTMLElement>(`[data-idx="${activeIdx}"]`);
    el?.scrollIntoView({ block: "nearest" });
  }, [activeIdx]);

  const focused = filtered[activeIdx];

  const toggleSelect = React.useCallback((draftId: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(draftId)) next.delete(draftId); else next.add(draftId);
      return next;
    });
  }, []);

  const selectAllVisible = () => {
    setSelected(new Set(filtered.map(r => r.draft_id)));
  };

  const bulkApprove = async () => {
    const targets = filtered.filter(r => selected.has(r.draft_id) && r.status === "draft_ready");
    if (!targets.length) return;
    if (!confirm(`Approve ${targets.length} draft${targets.length === 1 ? "" : "s"}?`)) return;
    setBulkBusy(true);
    try {
      for (const r of targets) await onAct(r.draft_id, "approve");
      flash(`Approved ${targets.length}`);
      onRefresh();
    } finally { setBulkBusy(false); }
  };

  const bulkDiscard = async () => {
    const targets = filtered.filter(r => selected.has(r.draft_id));
    if (!targets.length) return;
    if (!confirm(`Discard ${targets.length} draft${targets.length === 1 ? "" : "s"}?`)) return;
    setBulkBusy(true);
    try {
      for (const r of targets) await onAct(r.draft_id, "discard");
      flash(`Discarded ${targets.length}`);
      setSelected(new Set());
      onRefresh();
    } finally { setBulkBusy(false); }
  };

  const bulkSend = async () => {
    const targets = filtered.filter(r => selected.has(r.draft_id) && r.status === "approved");
    if (!targets.length) {
      flash("Bulk send only fires on Approved drafts. Approve first, then send.");
      return;
    }
    if (!confirm(`Send ${targets.length} approved draft${targets.length === 1 ? "" : "s"} now?`)) return;
    setBulkBusy(true);
    try {
      for (const r of targets) await onSend(r.draft_id);
      flash(`Sent ${targets.length}`);
      onRefresh();
    } finally { setBulkBusy(false); }
  };

  // Global keyboard handler — only active when the inbox tab is mounted.
  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      // Don't hijack typing inside text inputs / textareas.
      const tgt = e.target as HTMLElement | null;
      const inField = tgt && (tgt.tagName === "INPUT" || tgt.tagName === "TEXTAREA" || tgt.isContentEditable);
      if (e.key === "/") {
        e.preventDefault();
        searchRef.current?.focus();
        return;
      }
      if (inField) return;
      if (e.key === "?") { setShowHelp(s => !s); return; }
      if (e.key === "Escape") { setShowHelp(false); return; }
      if (e.key === "j" || e.key === "ArrowDown") { e.preventDefault(); setActiveIdx(i => Math.min(filtered.length - 1, i + 1)); return; }
      if (e.key === "k" || e.key === "ArrowUp") { e.preventDefault(); setActiveIdx(i => Math.max(0, i - 1)); return; }
      if (!focused) return;
      const did = focused.draft_id;
      if (e.key === "a" && !e.shiftKey) { e.preventDefault(); void onAct(did, "approve"); return; }
      if (e.key === "d" && !e.shiftKey) { e.preventDefault(); void onAct(did, "discard"); return; }
      if (e.key === "z" && !e.shiftKey) { e.preventDefault(); void onAct(did, "snooze"); return; }
      if (e.key === "s" && !e.shiftKey) {
        if (focused.status !== "approved") { flash("Approve before sending (a)"); return; }
        e.preventDefault();
        void onSend(did);
        return;
      }
      if (e.key === "e" && !e.shiftKey) {
        e.preventDefault();
        setBodyExpanded(prev => { const n = new Set(prev); if (n.has(did)) n.delete(did); else n.add(did); return n; });
        return;
      }
      if (e.key === "x" && !e.shiftKey) { e.preventDefault(); toggleSelect(did); return; }
      if (e.key === "i" && !e.shiftKey) { e.preventDefault(); setContextOpen(o => !o); return; }
      if (e.key === "A" && e.shiftKey) { e.preventDefault(); void bulkApprove(); return; }
      if (e.key === "D" && e.shiftKey) { e.preventDefault(); void bulkDiscard(); return; }
      if (e.key === "S" && e.shiftKey) { e.preventDefault(); void bulkSend(); return; }
      if (e.key === "X" && e.shiftKey) { e.preventDefault(); selectAllVisible(); return; }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focused?.draft_id, filtered.length, selected.size]);

  const counts = React.useMemo(() => {
    const c = { draft_ready: 0, approved: 0, sent: 0, replied: 0, all_active: 0 };
    for (const r of rows) {
      if (r.status === "discarded" || r.status === "snoozed") continue;
      c.all_active += 1;
      if (r.status === "draft_ready") c.draft_ready += 1;
      else if (r.status === "approved") c.approved += 1;
      else if (r.status === "sent") c.sent += 1;
      else if (r.status === "replied") c.replied += 1;
    }
    return c;
  }, [rows]);

  return (
    <div className="ob-root" data-context-open={contextOpen}>
      <div className="ob-toolbar">
        <div className="ob-segmented">
          {([
            ["draft_ready", "Draft", counts.draft_ready],
            ["approved", "Approved", counts.approved],
            ["sent", "Sent", counts.sent],
            ["replied", "Replied", counts.replied],
            ["all_active", "All", counts.all_active],
          ] as const).map(([k, label, n]) => (
            <button
              key={k}
              className={`ob-seg-btn ${statusFilter === k ? "ob-seg-btn-active" : ""}`}
              onClick={() => { setStatusFilter(k); setActiveIdx(0); }}
            >
              {label}<span className="ob-seg-count">{n}</span>
            </button>
          ))}
        </div>
        <input
          ref={searchRef}
          className="ob-search"
          placeholder="Search company, contact, subject…  (/)"
          value={search}
          onChange={e => { setSearch(e.target.value); setActiveIdx(0); }}
        />
        <select
          className="ob-select"
          value={minSignal}
          onChange={e => setMinSignal(Number(e.target.value))}
          title="Filter by minimum signal score"
        >
          <option value={0}>Any signal</option>
          <option value={20}>Signal ≥ 20</option>
          <option value={40}>Signal ≥ 40</option>
          <option value={60}>Signal ≥ 60</option>
          <option value={80}>Signal ≥ 80</option>
        </select>
        <select
          className="ob-select"
          value={sortBy}
          onChange={e => { setSortBy(e.target.value as "signal" | "recent"); setActiveIdx(0); }}
          title="How to order the cards"
        >
          <option value="signal">Sort: Signal</option>
          <option value="recent">Sort: Recently added/edited</option>
        </select>
        <label className={`ob-chip ${targetedOnly ? "ob-chip-active" : ""}`} title="Show only drafts targeting an open role">
          <input type="checkbox" checked={targetedOnly} onChange={e => setTargetedOnly(e.target.checked)} />
          <span>Targeted</span>
        </label>
        <div className="ob-toolbar-spacer" />
        {/* Bulk action bar appears only when there are MULTIPLE rows
            selected.  A single selected row already has Approve/Discard/
            Send buttons on the card itself, so showing the same actions
            in the toolbar would be redundant noise.  Selecting two or
            more rows is the user's clear signal that they want batch
            operations. */}
        {selected.size >= 2 ? (
          <div className="ob-bulk-bar">
            <span className="ob-bulk-count">{selected.size} selected</span>
            <button className="ob-btn ob-btn-primary" disabled={bulkBusy} onClick={() => void bulkApprove()} title="Approve all selected (Shift+A)">Approve all</button>
            <button className="ob-btn ob-btn-success" disabled={bulkBusy} onClick={() => void bulkSend()} title="Send all selected approved (Shift+S)">Send all</button>
            <button className="ob-btn ob-btn-danger" disabled={bulkBusy} onClick={() => void bulkDiscard()} title="Discard all selected (Shift+D)">Discard all</button>
            <button className="ob-btn-icon" onClick={() => setSelected(new Set())} title="Clear selection (Esc)">✕</button>
          </div>
        ) : selected.size === 1 ? (
          // Subtle "1 row selected" hint with a clear-selection button,
          // but no bulk action buttons — the user can act on it via the
          // row's own buttons or the keyboard.  Keeps the toolbar quiet.
          <button
            className="ob-bulk-mini"
            onClick={() => setSelected(new Set())}
            title="Clear selection (Esc)"
          >
            1 selected · clear
          </button>
        ) : (
          <button className="ob-btn-icon" title="Keyboard shortcuts (?)" onClick={() => setShowHelp(s => !s)}>?</button>
        )}
        <button className="ob-btn-icon" title="Toggle context rail (i)" onClick={() => setContextOpen(o => !o)}>{contextOpen ? "⇥" : "⇤"}</button>
      </div>

      <div className="ob-main">
        <div className="ob-list" ref={listRef}>
          {loading && filtered.length === 0 ? (
            Array.from({ length: 5 }).map((_, i) => (
              <div key={`obs-${i}`} className="ob-card ob-card-skel">
                <div className="sdr-skel-bar" style={{ width: "30%", height: 11 }} />
                <div className="sdr-skel-bar" style={{ width: "55%", height: 9, marginTop: 6 }} />
                <div className="sdr-skel-bar" style={{ width: "100%", height: 8, marginTop: 12 }} />
                <div className="sdr-skel-bar" style={{ width: "85%", height: 8, marginTop: 4 }} />
              </div>
            ))
          ) : null}
          {!loading && filtered.length === 0 ? (
            <div className="ob-empty">
              <div className="ob-empty-title">Inbox zero.</div>
              <div className="ob-empty-sub">Nothing to triage in this view.  Try the <button className="ob-link" onClick={() => setStatusFilter("all_active")}>All</button> filter, or ingest a new signal.</div>
            </div>
          ) : null}
          {filtered.map((row, i) => {
            const prev = i > 0 ? filtered[i - 1] : null;
            const next = i + 1 < filtered.length ? filtered[i + 1] : null;
            // "Group" a card with the previous one when they share an
            // account_id — flag both first and continuation rows so we
            // can render a connecting visual treatment.
            const sameAsPrev = prev != null && prev.account_id === row.account_id && row.account_id;
            const sameAsNext = next != null && next.account_id === row.account_id && row.account_id;
            return (
              <OutreachCard
                key={row.draft_id}
                row={row}
                index={i}
                focused={i === activeIdx}
                selected={selected.has(row.draft_id)}
                expanded={bodyExpanded.has(row.draft_id)}
                groupContinuation={!!sameAsPrev}
                groupHasMore={!!sameAsNext}
                onClick={() => setActiveIdx(i)}
                onToggleSelect={() => toggleSelect(row.draft_id)}
                onToggleExpand={() => setBodyExpanded(prev => { const n = new Set(prev); if (n.has(row.draft_id)) n.delete(row.draft_id); else n.add(row.draft_id); return n; })}
                onAct={(action) => void onAct(row.draft_id, action)}
                onSend={() => void onSend(row.draft_id)}
                onOpenDetailed={() => onOpenDetailed(row.account_id)}
              />
            );
          })}
        </div>

        {contextOpen && focused ? (
          <OutreachContextRail
            row={focused}
            onClose={() => setContextOpen(false)}
            onOpenDetailed={() => onOpenDetailed(focused.account_id)}
            onRefreshQueue={onRefresh}
            onGenerateDraft={onGenerateDraft}
            emailTemplates={emailTemplates}
          />
        ) : null}
      </div>

      <div className="ob-footer">
        <span><kbd>j</kbd>/<kbd>k</kbd> nav</span>
        <span><kbd>a</kbd> approve</span>
        <span><kbd>d</kbd> discard</span>
        <span><kbd>s</kbd> send</span>
        <span><kbd>e</kbd> expand</span>
        <span><kbd>x</kbd> select</span>
        <span><kbd>i</kbd> context</span>
        <span><kbd>/</kbd> search</span>
        <span><kbd>?</kbd> help</span>
      </div>

      {showHelp ? (
        <div className="ob-help-overlay" onClick={() => setShowHelp(false)}>
          <div className="ob-help" onClick={e => e.stopPropagation()}>
            <div className="ob-help-title">Keyboard shortcuts</div>
            <div className="ob-help-grid">
              {([
                ["j / ↓", "Next draft"], ["k / ↑", "Previous draft"],
                ["a", "Approve focused"], ["d", "Discard focused"],
                ["s", "Send focused (must be approved)"], ["z", "Snooze focused"],
                ["e", "Expand / collapse body"], ["x", "Toggle select"],
                ["⇧A", "Approve all selected"], ["⇧S", "Send all selected approved"],
                ["⇧D", "Discard all selected"], ["⇧X", "Select all visible"],
                ["i", "Toggle context rail"], ["/", "Focus search"],
                ["?", "Toggle this help"], ["Esc", "Close overlays"],
              ] as const).map(([k, label]) => (
                <React.Fragment key={k}><kbd>{k}</kbd><span>{label}</span></React.Fragment>
              ))}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function OutreachCard({
  row, index, focused, selected, expanded, groupContinuation, groupHasMore,
  onClick, onToggleSelect, onToggleExpand, onAct, onSend, onOpenDetailed,
}: {
  row: QueueRow;
  index: number;
  focused: boolean;
  selected: boolean;
  expanded: boolean;
  groupContinuation: boolean;
  groupHasMore: boolean;
  onClick: () => void;
  onToggleSelect: () => void;
  onToggleExpand: () => void;
  onAct: (action: string) => void;
  onSend: () => void;
  onOpenDetailed: () => void;
}) {
  const status = row.status || "draft_ready";
  const cfg = STATUS_CONFIG[status] || STATUS_CONFIG.draft_ready;
  const signal = Number(row.signal_score || 0);
  const heat = signal >= 60 ? "hot" : signal >= 30 ? "warm" : "cold";

  return (
    <div
      className={`ob-card ${focused ? "ob-card-focused" : ""} ${selected ? "ob-card-selected" : ""} ${groupContinuation ? "ob-card-cont" : ""} ${groupHasMore ? "ob-card-grouped-top" : ""}`}
      data-idx={index}
      onClick={onClick}
    >
      <div className="ob-card-rail" data-heat={heat} title={`Signal ${signal.toFixed(0)}`} />

      <input
        type="checkbox"
        className="ob-card-check"
        checked={selected}
        onClick={e => e.stopPropagation()}
        onChange={onToggleSelect}
        title="Select (x)"
      />

      <div className="ob-card-content">
        <div className="ob-card-row1">
          {groupContinuation ? (
            // Continuation row: the company name is implied by the
            // card above, so we show a quiet ↳ marker and skip the
            // bold company label.  Keeps a long stack of recipients
            // at the same account from looking like clutter.
            <span className="ob-card-cont-marker" title={`Another recipient at ${row.account_name || row.domain || ""}`}>
              ↳ same company
            </span>
          ) : (
            <>
              <span className="ob-card-company" title={row.domain}>{row.account_name || row.domain || "Unknown"}</span>
              <span className="ob-card-domain">{row.domain}</span>
            </>
          )}
          {row.target_job_title ? (
            <span className="ob-card-target" title={`Targeting: ${row.target_job_title}`}>
              👥 {String(row.target_job_title).slice(0, 32)}{String(row.target_job_title).length > 32 ? "…" : ""}
            </span>
          ) : null}
          <span className={`ob-card-status ob-card-status-${status}`} style={{ background: cfg.bg, color: cfg.color }}>{cfg.label}</span>
          <span className={`ob-card-signal ob-card-signal-${heat}`}>{signal.toFixed(0)}</span>
        </div>

        <div className="ob-card-row2">
          {row.contact_name ? (
            <span className="ob-card-contact"><strong>{row.contact_name}</strong>{row.contact_title ? ` · ${row.contact_title}` : ""}{row.contact_email ? ` · ${row.contact_email}` : ""}</span>
          ) : (
            <span className="ob-card-contact ob-card-contact-empty">No contact</span>
          )}
        </div>

        {row.subject_a ? (
          <div className="ob-card-subject" title={row.subject_a}>{row.subject_a}</div>
        ) : null}

        {expanded ? (
          <ExpandedDraftBody draftId={row.draft_id} />
        ) : null}
      </div>

      <div className="ob-card-actions" onClick={e => e.stopPropagation()}>
        {status === "draft_ready" ? (
          <>
            <button className="ob-btn ob-btn-primary ob-btn-sm" onClick={() => onAct("approve")} title="a">Approve</button>
            <button className="ob-btn ob-btn-danger ob-btn-sm" onClick={() => onAct("discard")} title="d">Discard</button>
          </>
        ) : null}
        {status === "approved" ? (
          <>
            <button className="ob-btn ob-btn-success ob-btn-sm" onClick={onSend} title="s">Send →</button>
            <button className="ob-btn ob-btn-sm" onClick={() => onAct("unapprove")} title="Move back to draft">Draft ←</button>
          </>
        ) : null}
        {status === "sent" ? <span className="ob-card-sent">Sent ✓</span> : null}
        {status === "replied" ? <span className="ob-card-replied">Replied ✦</span> : null}
        <button className="ob-btn-icon-sm" onClick={onToggleExpand} title="Expand body (e)">{expanded ? "−" : "+"}</button>
        <button className="ob-btn-icon-sm" onClick={onOpenDetailed} title="Open in Detailed view">↗</button>
      </div>
    </div>
  );
}

function ExpandedDraftBody({ draftId }: { draftId: string }) {
  // Lazy-load the full body via the queue endpoint once expanded.  The
  // queue payload only carries the subject — body lives on the draft
  // record, fetched on demand to keep the inbox payload small.
  const [body, setBody] = React.useState<string | null>(null);
  React.useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`/api/worktrigger/drafts/${encodeURIComponent(draftId)}`);
        if (!res.ok) { setBody("(failed to load draft body)"); return; }
        const data = await res.json();
        if (!cancelled) setBody(String(data.email_body || "(empty body)"));
      } catch {
        if (!cancelled) setBody("(network error)");
      }
    })();
    return () => { cancelled = true; };
  }, [draftId]);

  return (
    <div className="ob-card-body">
      {body == null ? <div className="ob-card-body-loading">Loading body…</div> : null}
      {body != null ? body : null}
    </div>
  );
}

type DraftFull = {
  id: string;
  subject_a?: string;
  subject_b?: string;
  email_body?: string;
  followup_body?: string;
  linkedin_dm?: string;
  outreach_mode?: string | null;
  target_job_title?: string | null;
  target_job_url?: string | null;
};

function OutreachContextRail({ row, onClose, onOpenDetailed, onRefreshQueue, onGenerateDraft, emailTemplates }: {
  row: QueueRow;
  onClose: () => void;
  onOpenDetailed: () => void;
  /** Triggered after the user toggles Job-Listing Outreach.  Forces
   *  the parent inbox to re-fetch the queue so any regenerated draft's
   *  ``target_job_title`` shows up as a purple pill on its card. */
  onRefreshQueue: () => void;
  onGenerateDraft: (accountId: string, contactId?: string, templateId?: string) => Promise<void>;
  emailTemplates: EmailTemplate[];
}) {
  // Lightweight account snapshot + the actual email that will go out.
  // Both are cached for 60s so j/k navigation is essentially free.
  const [detail, setDetail] = React.useState<AccountDetail | null>(null);
  const [draft, setDraft] = React.useState<DraftFull | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [showFollowup, setShowFollowup] = React.useState(false);

  // Inline edit state — scoped per draft_id.  When the user types into
  // the subject/body fields we track a "dirty" copy locally; pressing
  // Save calls /drafts/{id}/review with action=edit_and_approve, which
  // persists the edits AND flips status to "approved" in one shot
  // (the high-volume happy path: the SDR is editing because they're
  // about to send).  Discard reverts to the server copy.
  const [editingDraftId, setEditingDraftId] = React.useState<string>("");
  const [editSubject, setEditSubject] = React.useState("");
  const [editBody, setEditBody] = React.useState("");
  const [saving, setSaving] = React.useState(false);
  const [saveErr, setSaveErr] = React.useState("");
  const [selectedTemplateId, setSelectedTemplateId] = React.useState("");
  const [draftGenerating, setDraftGenerating] = React.useState(false);

  // Keep the dropdown aligned with the currently focused draft's source
  // template so switching cards does not leak prior card selections.
  React.useEffect(() => {
    setSelectedTemplateId(String(row.template_id || ""));
  }, [row.draft_id, row.template_id]);

  React.useEffect(() => {
    if (!row.account_id) return;
    let cancelled = false;
    setLoading(true);
    setSaveErr("");
    (async () => {
      try {
        const [accountData, draftData] = await Promise.all([
          sdrFetch<AccountDetail>(`/api/worktrigger/accounts/${encodeURIComponent(row.account_id)}/detail`, 60_000),
          sdrFetch<DraftFull>(`/api/worktrigger/drafts/${encodeURIComponent(row.draft_id)}`, 60_000).catch(() => null as unknown as DraftFull),
        ]);
        if (!cancelled) {
          setDetail(accountData);
          setDraft(draftData || null);
          // Reset edit buffer to the freshly-loaded draft.  This
          // intentionally drops any unsaved edits when the user
          // navigates to a different card — same behavior as Gmail's
          // reading pane: drafts auto-snap back unless explicitly saved.
          setEditingDraftId(row.draft_id);
          setEditSubject(draftData?.subject_a || row.subject_a || "");
          setEditBody(draftData?.email_body || "");
        }
      } catch { /* silent */ }
      finally { if (!cancelled) setLoading(false); }
    })();
    return () => { cancelled = true; };
  }, [row.account_id, row.draft_id, row.subject_a]);

  const acct = detail?.account || {};
  const stack = (detail?.signal_stack as Record<string, unknown> | null) || null;
  const exp = (stack?.explanation as Record<string, unknown> | undefined) || {};
  const hypo = detail?.work_hypotheses?.[0] as Record<string, unknown> | undefined;

  const serverSubject = draft?.subject_a || row.subject_a || "";
  const serverBody = draft?.email_body || "";
  const followup = draft?.followup_body || "";
  const recipient = row.contact_email || row.contact_name || "";
  const status = row.status || "draft_ready";
  const editable = status === "draft_ready" || status === "approved";
  const dirty = editable && editingDraftId === row.draft_id && (
    editSubject !== serverSubject || editBody !== serverBody
  );

  const saveEdits = async (): Promise<void> => {
    if (!dirty || saving) return;
    setSaving(true);
    setSaveErr("");
    try {
      const res = await fetch(`/api/worktrigger/drafts/${encodeURIComponent(row.draft_id)}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "edit_and_approve",
          reviewer_user_id: "sdr_operator_1",
          edited_subject: editSubject,
          edited_body: editBody,
        }),
      });
      if (!res.ok) { setSaveErr(`Save failed: ${await res.text()}`); return; }
      // Bust the per-draft + queue caches so the inbox card and any
      // future j/k revisit reflect the new content + approved status.
      invalidateSdrCache(`/api/worktrigger/drafts/${row.draft_id}`);
      invalidateSdrCache("/api/worktrigger/queue");
      // Refetch the draft so the rail picks up the canonical server copy.
      try {
        const fresh = await sdrFetch<DraftFull>(`/api/worktrigger/drafts/${encodeURIComponent(row.draft_id)}`, 60_000);
        setDraft(fresh);
      } catch { /* silent */ }
    } catch (e) {
      setSaveErr(`Save error: ${e instanceof Error ? e.message : "network"}`);
    } finally { setSaving(false); }
  };

  const discardEdits = (): void => {
    setEditSubject(serverSubject);
    setEditBody(serverBody);
    setSaveErr("");
  };

  // Cmd/Ctrl-S inside the textarea → save.  Stops propagation so the
  // browser save dialog never opens.  Only active when the rail's edit
  // buffer is dirty AND the user is focused inside one of our fields.
  const onFieldKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "s") {
      e.preventDefault();
      e.stopPropagation();
      void saveEdits();
    }
  };

  // Refetch state after the toggle flips so:
  //   • The rail's account snapshot picks up `job_outreach_enabled`.
  //   • The focused draft body refreshes (regenerated drafts have a
  //     new email body referencing the targeted role).
  //   • The parent inbox re-pulls the queue so its cards reflect the
  //     new ``target_job_title`` — that's what makes the purple
  //     "Targeting" pill appear on the corresponding card.
  const refetchAfterToggle = React.useCallback(() => {
    invalidateSdrCache(`/api/worktrigger/accounts/${row.account_id}/detail`);
    invalidateSdrCache("/api/worktrigger/queue");
    invalidateSdrCache(`/api/worktrigger/drafts/${row.draft_id}`);
    sdrFetch<AccountDetail>(`/api/worktrigger/accounts/${encodeURIComponent(row.account_id)}/detail`, 60_000)
      .then(d => setDetail(d))
      .catch(() => {});
    sdrFetch<DraftFull>(`/api/worktrigger/drafts/${encodeURIComponent(row.draft_id)}`, 60_000)
      .then(d => setDraft(d))
      .catch(() => {});
    onRefreshQueue();
  }, [row.account_id, row.draft_id, onRefreshQueue]);

  return (
    <aside className="ob-rail">
      <div className="ob-rail-header">
        <div style={{ minWidth: 0 }}>
          <div className="ob-rail-name">{String(acct.name || row.account_name || row.domain || "Account")}</div>
          <div className="ob-rail-domain">{String(acct.domain || row.domain || "")}</div>
        </div>
        <div style={{ display: "flex", gap: 4 }}>
          <button className="ob-btn-icon-sm" onClick={onOpenDetailed} title="Open Detailed view">↗</button>
          <button className="ob-btn-icon-sm" onClick={onClose} title="Close (i)">✕</button>
        </div>
      </div>

      {/* Job-Listing Outreach toggle.  Defaults to ON (the more
          useful mode for high-volume outreach) and is wired to the
          same endpoint the Detailed view uses, so the setting stays
          consistent across the two views. */}
      {row.account_id ? (
        <div className="ob-rail-toggle-row">
          <JobOutreachToggle
            accountId={row.account_id}
            initialEnabled={acct.job_outreach_enabled !== undefined ? !!acct.job_outreach_enabled : true}
            onChange={refetchAfterToggle}
          />
        </div>
      ) : null}

      {/* Email preview & editor — by default exactly one email is
          generated per contact, and this is what will be sent.  When
          the draft is in an editable status (draft_ready / approved),
          the subject and body become inline-editable inputs and a
          Save (& Approve) button appears once the buffer is dirty.
          Disabled / read-only for sent / replied / discarded. */}
      <div className="ob-rail-section">
        <div className="ob-rail-label-row">
          <span className="ob-rail-label">{editable ? "Email to send · editable" : "Email to send"}</span>
          {row.target_job_title ? (
            <span className="ob-rail-target-pill" title={`Targeting: ${row.target_job_title}`}>
              👥 {String(row.target_job_title).slice(0, 28)}{String(row.target_job_title).length > 28 ? "…" : ""}
            </span>
          ) : null}
        </div>
        {row.account_id ? (
          <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 8 }}>
            <select
              className="ob-select"
              value={selectedTemplateId}
              onChange={(e) => setSelectedTemplateId(e.target.value)}
              title="Optional template for new draft"
              style={{ flex: 1 }}
            >
              <option value="">Default (AI generated)</option>
              {emailTemplates.map((tpl) => (
                <option key={tpl.id} value={tpl.id}>{tpl.name}</option>
              ))}
            </select>
            <button
              className="ob-btn ob-btn-sm"
              disabled={draftGenerating || !row.contact_id}
              onClick={async () => {
                if (!row.account_id) return;
                setDraftGenerating(true);
                try {
                  await onGenerateDraft(row.account_id, row.contact_id, selectedTemplateId || undefined);
                  onRefreshQueue();
                } finally {
                  setDraftGenerating(false);
                }
              }}
              title={row.contact_id ? "Generate a new draft for this contact" : "No contact on this row"}
            >
              {draftGenerating ? "Generating…" : "+ Draft"}
            </button>
          </div>
        ) : null}
        {recipient ? <div className="ob-rail-email-to">To · <strong>{recipient}</strong></div> : null}
        {loading && !draft ? (
          <div className="ob-rail-skel">Loading email…</div>
        ) : editable ? (
          <>
            <input
              className="ob-rail-email-subject-input"
              value={editSubject}
              placeholder="(no subject)"
              onChange={e => setEditSubject(e.target.value)}
              onKeyDown={onFieldKeyDown}
              spellCheck
            />
            <textarea
              className="ob-rail-email-body-input"
              value={editBody}
              placeholder="No body yet — generate a draft."
              onChange={e => setEditBody(e.target.value)}
              onKeyDown={onFieldKeyDown}
              spellCheck
              rows={14}
            />
            <div className="ob-rail-edit-actions">
              {dirty ? (
                <>
                  <button
                    className="ob-btn ob-btn-primary ob-btn-sm"
                    onClick={() => void saveEdits()}
                    disabled={saving}
                    title="Save edits and mark approved (⌘/Ctrl-S)"
                  >
                    {saving ? "Saving…" : "Save & Approve"}
                  </button>
                  <button
                    className="ob-btn ob-btn-sm"
                    onClick={discardEdits}
                    disabled={saving}
                    title="Revert unsaved changes"
                  >
                    Discard changes
                  </button>
                  <span className="ob-rail-dirty-dot" title="Unsaved changes" />
                </>
              ) : (
                <span className="ob-rail-edit-hint">
                  {status === "approved" ? "Approved · edit to revise" : "Edit inline · ⌘S to save"}
                </span>
              )}
              {saveErr ? <span className="ob-rail-edit-error">{saveErr}</span> : null}
            </div>
          </>
        ) : (
          <>
            <div className="ob-rail-email-subject" title={serverSubject}>
              {serverSubject || <em style={{ color: "#9ca3af" }}>(no subject)</em>}
            </div>
            <div className="ob-rail-email-body">
              {serverBody || <em style={{ color: "#9ca3af" }}>No body.</em>}
            </div>
          </>
        )}
        {followup ? (
          <div style={{ marginTop: 8 }}>
            <button
              className="ob-rail-followup-toggle"
              onClick={() => setShowFollowup(s => !s)}
            >
              {showFollowup ? "Hide" : "Show"} follow-up {showFollowup ? "▴" : "▾"}
            </button>
            {showFollowup ? (
              <div className="ob-rail-email-body" style={{ marginTop: 6, background: "#fafbfc" }}>
                {followup}
              </div>
            ) : null}
          </div>
        ) : null}
      </div>

      {detail ? (
        <>
          <div className="ob-rail-section">
            <div className="ob-rail-label">Scoring</div>
            <div className="ob-rail-scores">
              {(["icp_fit", "signal_score", "work_fit", "priority_score"] as const).map(k => (
                <div key={k} className="ob-rail-score">
                  <span className="ob-rail-score-num">{Number(exp[k] || 0).toFixed(0)}</span>
                  <span className="ob-rail-score-label">{k.replace(/_/g, " ")}</span>
                </div>
              ))}
            </div>
          </div>
          <div className="ob-rail-section">
            <div className="ob-rail-label">Firmographics</div>
            <div className="ob-rail-kv">
              {acct.industry ? <><span>Industry</span><span>{String(acct.industry)}</span></> : null}
              {acct.employee_count != null ? <><span>Employees</span><span>{String(acct.employee_count)}</span></> : null}
              {acct.funding_stage ? <><span>Funding</span><span>{String(acct.funding_stage)}</span></> : null}
              {acct.country ? <><span>Country</span><span>{String(acct.country)}</span></> : null}
            </div>
          </div>
          {hypo ? (
            <div className="ob-rail-section">
              <div className="ob-rail-label">Hypothesis</div>
              <div className="ob-rail-hypo">
                <div><strong>Problem.</strong> {String(hypo.probable_problem || "—")}</div>
                <div><strong>Deliverable.</strong> {String(hypo.probable_deliverable || "—")}</div>
                <div><strong>Talent.</strong> {String(hypo.talent_archetype || "—")}</div>
              </div>
            </div>
          ) : null}
          {detail.signals && detail.signals.length > 0 ? (
            <div className="ob-rail-section">
              <div className="ob-rail-label">Recent signals</div>
              <div className="ob-rail-signals">
                {detail.signals.slice(0, 5).map((s, i) => (
                  <div key={i} className="ob-rail-signal">
                    <span className="ob-rail-signal-type">{String(s.signal_type || "")}</span>
                    <span className="ob-rail-signal-source">{String(s.source || "")}</span>
                    <span className="ob-rail-signal-date">{String(s.occurred_at || "").slice(0, 10)}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </>
      ) : null}
    </aside>
  );
}


function ContactWithDrafts({ contact: c, drafts, onGenerateDraft, onRefresh, accountId, emailTemplates }: {
  contact: Record<string, unknown>;
  drafts: Array<Record<string, unknown>>;
  onGenerateDraft: (templateId?: string) => void;
  onRefresh: () => void;
  accountId: string;
  emailTemplates: EmailTemplate[];
}) {
  // Hide discarded + snoozed drafts from the inline view to avoid the
  // user staring at four near-identical email bodies stacked on top of
  // each other (especially right after a job-listing regenerate, which
  // intentionally archives the prior draft).  Discarded drafts are
  // still tracked server-side and surface in the Pipeline board's
  // "Discarded" column for forensic restore.
  const archivedStatuses = React.useMemo(() => new Set(["discarded", "snoozed"]), []);
  const visibleDrafts = React.useMemo(
    () => drafts.filter(d => !archivedStatuses.has(String(d.status || ""))),
    [drafts, archivedStatuses],
  );
  // Newest first, then keep ONLY the most recent active draft per
  // contact.  A contact never has more than one editable email box on
  // screen — older versions are still in the database and can be
  // browsed via the Pipeline board if the user genuinely needs them.
  const latestDraft = React.useMemo(() => {
    if (visibleDrafts.length === 0) return null;
    const sorted = [...visibleDrafts].sort(
      (a, b) => toTs(b.updated_at || b.created_at) - toTs(a.updated_at || a.created_at),
    );
    return sorted[0];
  }, [visibleDrafts]);
  const archivedCount = drafts.length - visibleDrafts.length;

  const [open, setOpen] = React.useState(latestDraft != null);
  const [deleting, setDeleting] = React.useState(false);
  const [selectedTemplateId, setSelectedTemplateId] = React.useState("");

  // If a fresh draft arrives via regenerate, auto-open the section so
  // the user immediately sees the new version.
  React.useEffect(() => {
    if (latestDraft && !open) setOpen(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [latestDraft?.id]);

  const deleteContact = async () => {
    if (!confirm(`Remove ${c.full_name || "this contact"}?`)) return;
    setDeleting(true);
    try {
      await fetch(`/api/worktrigger/accounts/${encodeURIComponent(accountId)}/contacts/${encodeURIComponent(String(c.id))}`, { method: "DELETE" });
      onRefresh();
    } catch { /* silent */ }
    finally { setDeleting(false); }
  };

  const hasEmail = Boolean(c.email && String(c.email).includes("@"));

  return (
    <div style={{ borderBottom: "1px solid #e5e7eb" }}>
      {/* Contact header row */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "10px 0", cursor: "pointer" }} onClick={() => setOpen(!open)}>
        <div className="sdr-contact-avatar">{initials(String(c.full_name || "?"))}</div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontWeight: 600, fontSize: 13, color: "#111827" }}>{String(c.full_name || "Unknown")}</div>
          <div style={{ fontSize: 11, color: "#6b7280" }}>
            {String(c.title || "")}
            {hasEmail ? <span> · {String(c.email)}</span> : <span style={{ color: "#dc2626" }}> · no email</span>}
          </div>
        </div>
        <span style={{ fontSize: 11, fontWeight: 600, color: "#2563eb" }}>{(Number(c.confidence_score || 0) * 100).toFixed(0)}%</span>
        {latestDraft ? (
          <span
            style={{ fontSize: 10, color: "#9ca3af" }}
            title={archivedCount > 0
              ? `1 active draft · ${archivedCount} archived (visible in Pipeline → Discarded)`
              : "1 active draft"}
          >
            1 draft{archivedCount > 0 ? ` · +${archivedCount} archived` : ""}
          </span>
        ) : null}
        {hasEmail ? (
          <>
            <select
              value={selectedTemplateId}
              onClick={(e) => e.stopPropagation()}
              onChange={(e) => setSelectedTemplateId(e.target.value)}
              style={{ fontSize: 11, border: "1px solid #d1d5db", borderRadius: 4, padding: "2px 6px", maxWidth: 180 }}
              title="Optional saved template"
            >
              <option value="">Default (AI generated)</option>
              {emailTemplates.map((tpl) => (
                <option key={tpl.id} value={tpl.id}>{tpl.name}</option>
              ))}
            </select>
            <button
              className="sdr-btn sdr-btn-sm"
              style={{ padding: "3px 10px", fontSize: 11 }}
              onClick={e => { e.stopPropagation(); onGenerateDraft(selectedTemplateId || undefined); }}
            >
              + Draft
            </button>
          </>
        ) : null}
        <button style={{ background: "none", border: "none", color: "#9ca3af", cursor: "pointer", fontSize: 12, padding: "2px 4px" }} onClick={e => { e.stopPropagation(); void deleteContact(); }} disabled={deleting} title="Remove contact">✕</button>
        <span style={{ fontSize: 10, color: "#9ca3af" }}>{open ? "▲" : "▼"}</span>
      </div>

      {/* Collapsible — only the most recent active draft is shown.
          Older / discarded versions live in the Pipeline board so this
          view stays focused on "what should I send right now?". */}
      {open && latestDraft ? (
        <div style={{ paddingLeft: 40, paddingBottom: 12 }}>
          <ContactDraftInline
            key={String(latestDraft.id)}
            draft={latestDraft}
            statusConfig={STATUS_CONFIG[String(latestDraft.status)] || STATUS_CONFIG.draft_ready}
            onRefresh={onRefresh}
          />
        </div>
      ) : null}
      {open && !latestDraft ? (
        <div style={{ paddingLeft: 40, paddingBottom: 12, fontSize: 12, color: "#9ca3af" }}>
          No active draft. Click "+ Draft" to generate an email for {String(c.full_name || "this contact")}.
        </div>
      ) : null}
    </div>
  );
}

function ContactDraftInline({ draft: d, statusConfig: st, onRefresh }: {
  draft: Record<string, unknown>;
  statusConfig: { label: string; color: string; bg: string };
  onRefresh: () => void;
}) {
  const [subject, setSubject] = React.useState(String(d.subject_a || ""));
  const [body, setBody] = React.useState(String(d.email_body || ""));
  const [showFollowup, setShowFollowup] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [dirty, setDirty] = React.useState(false);
  const [sending, setSending] = React.useState(false);

  const saveDraft = async () => {
    setSaving(true);
    try {
      await fetch(`/api/worktrigger/drafts/${encodeURIComponent(String(d.id))}/review`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "edit_and_approve", reviewer_user_id: "sdr_operator_1", edited_subject: subject, edited_body: body }),
      });
      setDirty(false); onRefresh();
    } catch { /* silent */ }
    finally { setSaving(false); }
  };

  const sendDraft = async () => {
    setSending(true);
    try {
      const res = await fetch(`/api/worktrigger/drafts/${encodeURIComponent(String(d.id))}/send`, { method: "POST" });
      if (!res.ok) { alert("Send failed: " + (await res.text())); }
      onRefresh();
    } catch { /* silent */ }
    finally { setSending(false); }
  };

  const moveBackToDraft = async () => {
    try {
      await fetch(`/api/worktrigger/drafts/${encodeURIComponent(String(d.id))}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "unapprove", reviewer_user_id: "sdr_operator_1" }),
      });
      onRefresh();
    } catch { /* silent */ }
  };

  return (
    <div style={{ border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, background: "#fafbfc" }}>
      {/* Status + actions row */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8, gap: 8, flexWrap: "wrap" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
          <span style={{ fontSize: 11, fontWeight: 600, color: st.color, background: st.bg, padding: "2px 8px", borderRadius: 10 }}>{st.label}</span>
          {d.outreach_mode === "job_listing" && d.target_job_title ? (
            <span
              className="sdr-job-target-pill"
              title={d.target_job_url ? `Targeting open role: ${d.target_job_title} (${d.target_job_url})` : `Targeting open role: ${d.target_job_title}`}
            >
              <span aria-hidden>👥</span> Targeting: {String(d.target_job_title).slice(0, 40)}{String(d.target_job_title).length > 40 ? "…" : ""}
            </span>
          ) : null}
        </div>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          {d.status === "approved" ? (
            <>
              <button className="sdr-btn sdr-btn-success sdr-btn-sm" style={{ padding: "3px 10px" }} disabled={sending} onClick={() => void sendDraft()}>
                {sending ? "Sending..." : "Send Now"}
              </button>
              <button className="sdr-btn sdr-btn-sm" style={{ padding: "3px 10px" }} onClick={() => void moveBackToDraft()}>
                Move to Draft
              </button>
            </>
          ) : null}
          {dirty ? (
            <button className="sdr-btn sdr-btn-primary sdr-btn-sm" style={{ padding: "3px 10px" }} disabled={saving} onClick={() => void saveDraft()}>
              {saving ? "..." : "Save & Approve"}
            </button>
          ) : null}
        </div>
      </div>
      {/* Subject */}
      <input value={subject} onChange={e => { setSubject(e.target.value); setDirty(true); }}
        style={{ width: "100%", padding: "6px 8px", fontSize: 13, fontWeight: 600, border: "1px solid #d1d5db", borderRadius: 4, marginBottom: 6, fontFamily: "inherit", color: "#111827" }}
        placeholder="Subject" />
      {/* Body */}
      <textarea value={body} onChange={e => { setBody(e.target.value); setDirty(true); }}
        rows={6} style={{ width: "100%", padding: "6px 8px", fontSize: 12, lineHeight: 1.6, border: "1px solid #d1d5db", borderRadius: 4, fontFamily: "inherit", color: "#374151", resize: "vertical" }} />
      {/* Follow-up toggle */}
      {d.followup_body ? (
        <div style={{ marginTop: 4 }}>
          <button style={{ background: "none", border: "none", color: "#2563eb", fontSize: 11, cursor: "pointer", padding: 0 }} onClick={() => setShowFollowup(!showFollowup)}>
            {showFollowup ? "Hide follow-up ▲" : "Show follow-up ▼"}
          </button>
          {showFollowup ? (
            <div style={{ marginTop: 4, padding: 8, background: "#f9fafb", borderRadius: 4, border: "1px solid #e5e7eb", fontSize: 12, lineHeight: 1.6, color: "#374151", whiteSpace: "pre-wrap" }}>
              {String(d.followup_body)}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}


// =====================================================================
// ChatPanel — per-account ChatGPT-style assistant with SerpAPI tool use.
// Sessions persist to SQLite; each account has its own thread history.
// =====================================================================

type ChatSession = {
  id: string;
  account_id: string;
  title: string;
  created_at: string;
  updated_at: string;
  message_count?: number;
};

type ChatMessage = {
  id: string;
  role: "user" | "assistant" | "tool";
  content: string;
  created_at: string;
  tool_calls?: Array<{ id: string; function: { name: string; arguments: string } }>;
  tool_name?: string;
  tool_call_id?: string;
};

function parseToolArgs(raw: string): Record<string, unknown> {
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

type ToolResultPayload = {
  ok?: boolean;
  error?: string;
  query?: string;
  count?: number;
  results?: Array<{ title?: string; url?: string; snippet?: string; source?: string; date?: string }>;
};

function parseToolResult(msg: ChatMessage | null): ToolResultPayload | null {
  if (!msg) return null;
  try { return JSON.parse(msg.content || "{}") as ToolResultPayload; }
  catch { return null; }
}

/**
 * One chip per tool call.  Shows the intent ("Searched for …"), a result
 * count when available, and — when the result payload has actual
 * results — an expandable list of sources.  Replaces the previous
 * two-element design that rendered a pending bubble AND a separate
 * result chip for the same call.
 */
function ToolUseRow({ block }: { block: { id: string; calls: Array<{ call_id: string; name: string; args: Record<string, unknown>; result: ChatMessage | null }> } }) {
  const [expanded, setExpanded] = React.useState<Record<string, boolean>>({});
  return (
    <div className="sdr-chat-tool-group">
      {block.calls.map(call => {
        const parsed = parseToolResult(call.result);
        const queryFromArgs = typeof call.args.query === "string" ? (call.args.query as string) : "";
        const query = (parsed?.query || queryFromArgs || "").trim();
        const isPending = !call.result;
        const errored = parsed && parsed.ok === false;
        const count = parsed?.count ?? (parsed?.results?.length ?? 0);
        const hasSources = !isPending && !errored && (parsed?.results?.length ?? 0) > 0;
        const open = !!expanded[call.call_id];

        const label = call.name === "web_search" ? "Web search" : call.name;
        return (
          <div key={call.call_id} className="sdr-chat-tool-row">
            <button
              type="button"
              className={`sdr-chat-tool-chip ${isPending ? "sdr-chat-tool-pending" : ""} ${errored ? "sdr-chat-tool-errored" : ""}`}
              onClick={() => hasSources && setExpanded(s => ({ ...s, [call.call_id]: !s[call.call_id] }))}
              title={query || undefined}
              disabled={!hasSources}
            >
              <span className="sdr-chat-tool-icon" aria-hidden>
                {isPending ? (
                  <span className="sdr-chat-tool-spinner" />
                ) : (
                  /* Magnifier icon */
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" /></svg>
                )}
              </span>
              <span className="sdr-chat-tool-label">{label}</span>
              {query ? (
                <span className="sdr-chat-tool-query" title={query}>
                  <span className="sdr-chat-tool-query-text">{query}</span>
                </span>
              ) : null}
              {!isPending && !errored ? (
                <span className="sdr-chat-tool-count">{count} source{count === 1 ? "" : "s"}</span>
              ) : null}
              {errored ? <span className="sdr-chat-tool-count sdr-chat-tool-count-err">error</span> : null}
              {hasSources ? (
                <span className="sdr-chat-tool-caret" aria-hidden>{open ? "▾" : "▸"}</span>
              ) : null}
            </button>
            {open && hasSources ? (
              <div className="sdr-chat-tool-sources">
                {(parsed?.results || []).slice(0, 8).map((r, idx) => (
                  <a key={idx} href={r.url || "#"} target="_blank" rel="noopener noreferrer" className="sdr-chat-tool-source">
                    <span className="sdr-chat-tool-source-num">{idx + 1}</span>
                    <span className="sdr-chat-tool-source-body">
                      <span className="sdr-chat-tool-source-title">{(r.title || r.url || "").slice(0, 120)}</span>
                      {r.url ? <span className="sdr-chat-tool-source-url">{safeHost(r.url)}</span> : null}
                    </span>
                  </a>
                ))}
              </div>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}

function safeHost(url: string): string {
  try { return new URL(url).hostname.replace(/^www\./, ""); }
  catch { return url; }
}

function ChatPanel({ accountId, accountName }: { accountId: string; accountName: string }) {
  const [sessions, setSessions] = React.useState<ChatSession[]>([]);
  const [activeId, setActiveId] = React.useState<string | null>(null);
  const [messages, setMessages] = React.useState<ChatMessage[]>([]);
  const [input, setInput] = React.useState("");
  const [sending, setSending] = React.useState(false);
  const [loadingMsgs, setLoadingMsgs] = React.useState(false);
  const [renaming, setRenaming] = React.useState<{ id: string; title: string } | null>(null);
  const messagesScrollRef = React.useRef<HTMLDivElement>(null);
  const textareaRef = React.useRef<HTMLTextAreaElement>(null);

  // Auto-grow the textarea to fit content up to a max height, then scroll.
  // Recompute on every input change so the surrounding layout stays stable.
  const autoGrowTextarea = React.useCallback(() => {
    const el = textareaRef.current;
    if (!el) return;
    const MAX_PX = 160; // ~6 lines at 14px/1.5
    el.style.height = "auto";
    const next = Math.min(el.scrollHeight, MAX_PX);
    el.style.height = `${next}px`;
    el.style.overflowY = el.scrollHeight > MAX_PX ? "auto" : "hidden";
  }, []);

  React.useEffect(() => { autoGrowTextarea(); }, [input, autoGrowTextarea]);

  const loadSessions = React.useCallback(async () => {
    try {
      const res = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(accountId)}/chat/sessions`);
      if (!res.ok) return;
      const data = (await res.json()) as ChatSession[];
      setSessions(data);
      if (data.length > 0 && !activeId) setActiveId(data[0].id);
      if (data.length === 0) { setActiveId(null); setMessages([]); }
    } catch { /* silent */ }
  }, [accountId, activeId]);

  React.useEffect(() => { void loadSessions(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [accountId]);

  const loadMessages = React.useCallback(async (sessionId: string) => {
    setLoadingMsgs(true);
    try {
      const res = await fetch(`/api/worktrigger/chat/sessions/${encodeURIComponent(sessionId)}/messages`);
      if (res.ok) {
        const data = await res.json() as { messages: ChatMessage[] };
        setMessages(data.messages || []);
      }
    } finally { setLoadingMsgs(false); }
  }, []);

  React.useEffect(() => {
    if (activeId) void loadMessages(activeId);
    else setMessages([]);
  }, [activeId, loadMessages]);

  // Scroll ONLY the inner messages container to the bottom on new messages.
  // Using scrollIntoView would also scroll outer panes (.sdr-detail), which
  // pushes the chat input row below the viewport.
  React.useEffect(() => {
    const el = messagesScrollRef.current;
    if (!el) return;
    // Defer to next frame so layout is settled.
    requestAnimationFrame(() => {
      el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
    });
  }, [messages.length, sending]);

  const newSession = async () => {
    const res = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(accountId)}/chat/sessions`, { method: "POST" });
    if (res.ok) {
      const s = await res.json() as ChatSession;
      setSessions(prev => [s, ...prev]);
      setActiveId(s.id);
      setMessages([]);
    }
  };

  const deleteSession = async (id: string) => {
    if (!confirm("Delete this conversation? This cannot be undone.")) return;
    const res = await fetch(`/api/worktrigger/chat/sessions/${encodeURIComponent(id)}`, { method: "DELETE" });
    if (res.ok) {
      setSessions(prev => prev.filter(s => s.id !== id));
      if (activeId === id) {
        const remaining = sessions.filter(s => s.id !== id);
        setActiveId(remaining[0]?.id || null);
      }
    }
  };

  const renameSession = async (id: string, title: string) => {
    const res = await fetch(`/api/worktrigger/chat/sessions/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title }),
    });
    if (res.ok) {
      setSessions(prev => prev.map(s => s.id === id ? { ...s, title } : s));
    }
  };

  const send = async () => {
    const trimmed = input.trim();
    if (!trimmed || sending) return;

    let sid = activeId;
    if (!sid) {
      // Auto-create a session if none exists yet
      const r = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(accountId)}/chat/sessions`, { method: "POST" });
      if (!r.ok) return;
      const s = await r.json() as ChatSession;
      sid = s.id;
      setSessions(prev => [s, ...prev]);
      setActiveId(s.id);
    }

    setInput("");
    setSending(true);
    // Optimistic user-message render
    const optimistic: ChatMessage = {
      id: `tmp_${Date.now()}`, role: "user", content: trimmed, created_at: new Date().toISOString(),
    };
    setMessages(prev => [...prev, optimistic]);

    try {
      const res = await fetch(`/api/worktrigger/chat/sessions/${encodeURIComponent(sid)}/messages`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ content: trimmed }),
      });
      if (res.ok) {
        const data = await res.json() as { messages: ChatMessage[] };
        setMessages(data.messages || []);
        void loadSessions(); // update updated_at + title
      } else {
        const errText = await res.text();
        setMessages(prev => [...prev, {
          id: `err_${Date.now()}`, role: "assistant", content: `Chat failed: ${errText}`,
          created_at: new Date().toISOString(),
        }]);
      }
    } catch (e) {
      setMessages(prev => [...prev, {
        id: `err_${Date.now()}`, role: "assistant",
        content: `Chat error: ${e instanceof Error ? e.message : "network"}`,
        created_at: new Date().toISOString(),
      }]);
    } finally {
      setSending(false);
    }
  };

  const onInputKey = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      void send();
    }
  };

  // Collapse each "assistant stub + its tool results" pair into a single
  // ToolUseBlock entry.  The persisted stub message has empty content +
  // tool_calls; without this grouping the UI would render both a
  // "Searching the web…" pending bubble AND the "web_search · N results"
  // chip for the same call, which looks like a glitch once the results
  // land.  We also fold follow-on tool rows into the same block so one
  // turn with multiple tool calls renders as one visual unit.
  type ToolUseBlock = {
    role: "tool-use";
    id: string;
    calls: Array<{ call_id: string; name: string; args: Record<string, unknown>; result: ChatMessage | null }>;
  };
  type RenderedItem = ChatMessage | ToolUseBlock;

  const rendered = React.useMemo<RenderedItem[]>(() => {
    const items: RenderedItem[] = [];
    for (let i = 0; i < messages.length; i += 1) {
      const m = messages[i];
      const isAssistantStub =
        m.role === "assistant"
        && !m.content?.trim()
        && Array.isArray(m.tool_calls)
        && m.tool_calls.length > 0;

      if (isAssistantStub) {
        const callDescriptors = (m.tool_calls || []).map(tc => ({
          call_id: tc.id,
          name: tc.function.name,
          args: parseToolArgs(tc.function.arguments),
          result: null as ChatMessage | null,
        }));
        // Consume any immediately-following tool messages and attach
        // them to the matching call by tool_call_id.
        while (i + 1 < messages.length && messages[i + 1].role === "tool") {
          const t = messages[i + 1];
          const target = callDescriptors.find(c => c.call_id === t.tool_call_id);
          if (target) target.result = t;
          else callDescriptors.push({
            call_id: t.tool_call_id || `orphan_${t.id}`,
            name: t.tool_name || "tool",
            args: {},
            result: t,
          });
          i += 1;
        }
        items.push({ role: "tool-use", id: `tu_${m.id}`, calls: callDescriptors });
        continue;
      }

      if (m.role === "tool") {
        // Orphan tool row (no preceding assistant stub persisted).  Show
        // it inline as a degenerate single-call block.
        items.push({
          role: "tool-use",
          id: `tu_orphan_${m.id}`,
          calls: [{
            call_id: m.tool_call_id || m.id,
            name: m.tool_name || "tool",
            args: {},
            result: m,
          }],
        });
        continue;
      }

      items.push(m);
    }
    return items;
  }, [messages]);

  return (
    <div className="sdr-chat">
      {/* Sessions sidebar */}
      <div className="sdr-chat-sidebar">
        <button className="sdr-chat-new-btn" onClick={() => void newSession()}>+ New conversation</button>
        <div className="sdr-chat-sessions">
          {sessions.length === 0 ? (
            <div className="sdr-chat-empty-sessions">No conversations yet</div>
          ) : null}
          {sessions.map(s => (
            <div
              key={s.id}
              className={`sdr-chat-session ${s.id === activeId ? "sdr-chat-session-active" : ""}`}
              onClick={() => setActiveId(s.id)}
            >
              {renaming?.id === s.id ? (
                <input
                  autoFocus
                  value={renaming.title}
                  onChange={e => setRenaming({ id: s.id, title: e.target.value })}
                  onBlur={() => { if (renaming) { void renameSession(s.id, renaming.title); setRenaming(null); } }}
                  onKeyDown={e => {
                    if (e.key === "Enter") { if (renaming) void renameSession(s.id, renaming.title); setRenaming(null); }
                    if (e.key === "Escape") setRenaming(null);
                  }}
                  onClick={e => e.stopPropagation()}
                  className="sdr-chat-session-rename"
                />
              ) : (
                <>
                  <div className="sdr-chat-session-title" title={s.title}>{s.title || "Untitled"}</div>
                  <div className="sdr-chat-session-meta">
                    {s.message_count ? `${s.message_count} msg` : "empty"} · {new Date(s.updated_at).toLocaleDateString()}
                  </div>
                </>
              )}
              <div className="sdr-chat-session-actions">
                <button title="Rename" onClick={e => { e.stopPropagation(); setRenaming({ id: s.id, title: s.title }); }}>✎</button>
                <button title="Delete" onClick={e => { e.stopPropagation(); void deleteSession(s.id); }}>✕</button>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Thread */}
      <div className="sdr-chat-thread">
        <div ref={messagesScrollRef} className="sdr-chat-messages" role="log" aria-live="polite">
          <div className="sdr-chat-column">
            {!activeId && messages.length === 0 ? (
              <div className="sdr-chat-greeting">
                <div className="sdr-chat-greeting-title">Chat about {accountName}</div>
                <div className="sdr-chat-greeting-sub">
                  Ask the co-pilot anything about this account. It has full context — signals, hypothesis, contacts — and can search the web for fresh info.
                </div>
                <div className="sdr-chat-suggests">
                  {[
                    "What's the best outbound angle here?",
                    "Summarize recent news and hiring signals.",
                    "Who's the most likely buyer and why?",
                    "Draft a 4-sentence opener to the CEO.",
                  ].map(s => (
                    <button key={s} className="sdr-chat-suggest" onClick={() => setInput(s)}>
                      {s}
                    </button>
                  ))}
                </div>
              </div>
            ) : null}
            {loadingMsgs ? <div className="sdr-chat-loading">Loading…</div> : null}
            {rendered.map((item, i) => {
              if ("role" in item && item.role === "tool-use") {
                return <ToolUseRow key={item.id} block={item} />;
              }
              const msg = item as ChatMessage;
              if (msg.role === "user") {
                return (
                  <div key={msg.id || i} className="sdr-chat-msg sdr-chat-msg-user">
                    <div className="sdr-chat-bubble">{msg.content}</div>
                  </div>
                );
              }
              return (
                <div key={msg.id || i} className="sdr-chat-msg sdr-chat-msg-assistant">
                  <div
                    className="sdr-chat-bubble sdr-chat-bubble-md"
                    dangerouslySetInnerHTML={{ __html: renderMarkdown(msg.content || "") }}
                  />
                </div>
              );
            })}
            {sending ? (
              <div className="sdr-chat-msg sdr-chat-msg-assistant">
                <div className="sdr-chat-bubble sdr-chat-bubble-thinking">
                  <span className="sdr-chat-typing"><span /><span /><span /></span>
                </div>
              </div>
            ) : null}
          </div>
        </div>
        <div className="sdr-chat-input-row">
          <div className="sdr-chat-input-wrap">
            <textarea
              ref={textareaRef}
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={onInputKey}
              placeholder={`Ask about ${accountName}…`}
              rows={1}
              disabled={sending}
            />
            <button
              className="sdr-chat-send"
              disabled={sending || !input.trim()}
              onClick={() => void send()}
              title="Send (⌘↩ or Ctrl↩)"
              aria-label="Send"
            >
              {sending ? (
                <span className="sdr-chat-typing" style={{ padding: 0 }}><span /><span /><span /></span>
              ) : (
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden>
                  <path d="M4 12l16-8-6 18-3-7-7-3z" stroke="currentColor" strokeWidth="2" strokeLinejoin="round" />
                </svg>
              )}
            </button>
          </div>
          <div className="sdr-chat-hint">⌘ or Ctrl + Enter to send</div>
        </div>
      </div>
    </div>
  );
}


function JobOutreachToggle({ accountId, initialEnabled, onChange }: {
  accountId: string;
  initialEnabled: boolean;
  onChange: () => void;
}) {
  const [enabled, setEnabled] = React.useState(initialEnabled);
  const [busy, setBusy] = React.useState(false);

  React.useEffect(() => { setEnabled(initialEnabled); }, [initialEnabled]);

  const flip = async () => {
    if (busy) return;
    const next = !enabled;

    // Ask whether to also regenerate existing active drafts.  Skipping
    // regeneration is useful when the user just wants to set the mode
    // and let it apply only to NEW drafts they create later.
    let regenerate = false;
    if (next) {
      regenerate = confirm(
        "Turn on Job-Listing Outreach for this company?\n\n" +
        "Future drafts will pitch Figwork as a way to fill the company's currently-open roles.\n\n" +
        "Click OK to also regenerate every active draft on this account right now (uses LLM credits)." +
        "\nClick Cancel to apply only to NEW drafts you create from here on."
      );
    }
    // For "off" we only confirm if there might be regeneration impact.
    setBusy(true);
    try {
      const res = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(accountId)}/job-outreach`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: next, regenerate }),
      });
      if (res.ok) {
        setEnabled(next);
        // Trigger detail refresh so any regenerated drafts show up.
        onChange();
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <button
      type="button"
      className={`sdr-job-toggle ${enabled ? "sdr-job-toggle-on" : ""} ${busy ? "sdr-job-toggle-busy" : ""}`}
      onClick={() => void flip()}
      title={
        enabled
          ? "Job-Listing Outreach is ON. Drafts will pitch Figwork as fill for open roles. Click to turn off."
          : "Turn on Job-Listing Outreach: drafts get personalized to a specific open role at this company."
      }
      disabled={busy}
    >
      <span className="sdr-job-toggle-track">
        <span className="sdr-job-toggle-knob" />
      </span>
      <span className="sdr-job-toggle-label">
        Job-Listing Outreach
        <span className="sdr-job-toggle-state">{busy ? "…" : enabled ? "ON" : "OFF"}</span>
      </span>
    </button>
  );
}

function CollapsibleChatSection({ accountId, accountName }: { accountId: string; accountName: string }) {
  // Persist open/close per-account so switching contexts remembers your choice.
  const storageKey = `sdr_chat_open_${accountId}`;
  const [open, setOpen] = React.useState<boolean>(() => {
    try { return localStorage.getItem(storageKey) !== "0"; } catch { return true; }
  });
  React.useEffect(() => {
    try { localStorage.setItem(storageKey, open ? "1" : "0"); } catch { /* ignore */ }
  }, [open, storageKey]);
  return (
    <div className="sdr-section">
      <div className="sdr-section-head" style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }} onClick={() => setOpen(!open)}>
        <span>AI Co-pilot</span>
        <span style={{ fontSize: 10, color: "#9ca3af", fontWeight: 500, textTransform: "none", letterSpacing: 0 }}>
          Chat with context + web search
        </span>
        <span style={{ marginLeft: "auto", fontSize: 11 }}>{open ? "▾" : "▸"}</span>
      </div>
      {open ? (
        <div className="sdr-section-body" style={{ padding: 0 }}>
          <ChatPanel accountId={accountId} accountName={accountName} />
        </div>
      ) : null}
    </div>
  );
}

function AccountDetailPane({
  detail,
  geoScores,
  geoNames,
  onGenerateDraft,
  onRefreshDetail,
  emailTemplates,
  onAccountDeleted,
}: {
  detail: AccountDetail;
  geoScores: Record<string, GeoScore>;
  geoNames: Record<string, string>;
  onGenerateDraft: (contactId?: string, templateId?: string) => void;
  onRefreshDetail: () => void;
  emailTemplates: EmailTemplate[];
  onAccountDeleted: () => void;
}) {
  const acct = detail.account;
  const stack = detail.signal_stack as Record<string, unknown> | null;
  const hypo = detail.work_hypotheses[0] as Record<string, unknown> | undefined;

  const [addEmail, setAddEmail] = React.useState("");
  const [addName, setAddName] = React.useState("");
  const [addTitle, setAddTitle] = React.useState("");
  const [socialSignals, setSocialSignals] = React.useState<Record<string, unknown> | null>(null);
  const [socialLoading, setSocialLoading] = React.useState(false);
  const [socialRefreshing, setSocialRefreshing] = React.useState(false);

  const acctDomain = String(acct.domain || "").trim();
  const acctName = String(acct.name || "");

  // Same race-guard pattern as `loadDetail` in the parent.  When the
  // user switches accounts quickly, the previous social-signals fetch
  // (which can take several seconds when the cache is cold and SerpAPI
  // + OpenAI run) must not be allowed to call `setSocialSignals` and
  // overwrite the new account's data.  We track the most-recent
  // requested domain in a ref and check before every state write.
  const latestSocialDomainRef = React.useRef<string>("");
  const loadSocialSignals = React.useCallback(async (force = false) => {
    if (!acctDomain) return;
    latestSocialDomainRef.current = acctDomain;
    const li = String(acct.linkedin_url || "").trim();
    const tw = String((acct as Record<string, unknown>).twitter_url || "").trim();
    const qs = new URLSearchParams({
      domain: acctDomain,
      company_name: acctName,
      ...(li ? { linkedin_url: li } : {}),
      ...(tw ? { twitter_url: tw } : {}),
      ...(force ? { force_refresh: "true" } : {}),
    });
    const url = `/api/worktrigger/vendors/companies/social-signals?${qs}`;
    try {
      if (force) {
        invalidateSdrCache("/api/worktrigger/vendors/companies/social-signals");
        setSocialRefreshing(true);
      } else {
        setSocialLoading(true);
      }
      const data = await sdrFetch<Record<string, unknown>>(url, 6 * 60 * 60 * 1000);
      // Drop the result silently if the user has moved to another
      // account while this fetch was in flight.  Verifies BOTH the ref
      // (stale-check) AND the response payload's own domain field
      // matches our current account — defense-in-depth in case the
      // shared client-side fetch cache ever returned a wrong-key hit.
      const responseDomain = String((data as Record<string, unknown>).domain || "").trim().toLowerCase();
      const expectedDomain = acctDomain.trim().toLowerCase();
      if (latestSocialDomainRef.current !== acctDomain) return;
      if (responseDomain && expectedDomain && responseDomain !== expectedDomain) return;
      setSocialSignals(data);
    } catch { /* silent */ }
    finally {
      // Spinner only belongs to the current request.
      if (latestSocialDomainRef.current === acctDomain) {
        setSocialLoading(false);
        setSocialRefreshing(false);
      }
    }
  }, [
    acctDomain,
    acctName,
    String(acct.linkedin_url || ""),
    String((acct as Record<string, unknown>).twitter_url || ""),
  ]);

  // Clear the previous account's social signals immediately on domain
  // change so a stale render frame can never show another company's
  // data while the new fetch is in flight.
  React.useEffect(() => {
    setSocialSignals(null);
    if (!acctDomain) return;
    void loadSocialSignals(false);
  }, [acctDomain, loadSocialSignals]);
  const [searchTitles, setSearchTitles] = React.useState("");
  const [contactSearching, setContactSearching] = React.useState(false);
  const [contactMsg, setContactMsg] = React.useState("");

  const addManualContact = async () => {
    if (!addEmail && !addName) return;
    const qs = new URLSearchParams({ full_name: addName, email: addEmail, title: addTitle });
    await fetch(`/api/worktrigger/accounts/${encodeURIComponent(String(acct.id))}/contacts/add?${qs}`, { method: "POST" });
    setAddEmail(""); setAddName(""); setAddTitle("");
    setContactMsg("Contact added");
    onRefreshDetail();
    setTimeout(() => setContactMsg(""), 2000);
  };

  const searchByTitle = async () => {
    if (!searchTitles.trim()) return;
    setContactSearching(true);
    try {
      const qs = new URLSearchParams({ account_id: String(acct.id), titles: searchTitles, limit: "10" });
      const res = await fetch(`/api/worktrigger/vendors/contacts/search-by-title?${qs}`, { method: "POST" });
      if (res.ok) {
        const data = await res.json();
        setContactMsg(`Found ${data.new_contacts} new contacts`);
      } else {
        setContactMsg("Search failed");
      }
      onRefreshDetail();
    } catch { setContactMsg("Search error"); }
    finally { setContactSearching(false); setTimeout(() => setContactMsg(""), 3000); }
  };

  return (
    <>
      <div className="sdr-detail-header">
        <div>
          <div className="sdr-detail-name">{String(acct.name || acct.domain || "Unknown")}</div>
          <div className="sdr-detail-domain">{String(acct.domain || "")}</div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", marginLeft: "auto", flexWrap: "wrap" }}>
          <JobOutreachToggle
            accountId={String(acct.id)}
            initialEnabled={!!acct.job_outreach_enabled}
            onChange={onRefreshDetail}
          />
          <button
            className="sdr-btn sdr-btn-danger sdr-btn-sm"
            title="Permanently delete this account and all its drafts, contacts, signals, and hypotheses"
            onClick={async () => {
              const name = String(acct.name || acct.domain || "this account");
              if (!confirm(`Permanently delete ${name}?\n\nThis removes the account, all contacts, drafts, signals, and hypotheses. Cannot be undone.`)) return;
              const res = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(String(acct.id))}`, { method: "DELETE" });
              if (res.ok) onAccountDeleted();
            }}
          >
            Delete company
          </button>
        </div>
      </div>

      {/* Account KPIs */}
      <div className="sdr-section">
        <div className="sdr-section-head">Account Overview</div>
        <div className="sdr-section-body">
          <div className="sdr-kv-grid">
            <div className="sdr-kv-item"><span className="sdr-kv-label">Industry</span><span className="sdr-kv-value">{String(acct.industry || "—")}</span></div>
            <div className="sdr-kv-item"><span className="sdr-kv-label">Employees</span><span className="sdr-kv-value">{acct.employee_count != null ? String(acct.employee_count) : "—"}</span></div>
            <div className="sdr-kv-item"><span className="sdr-kv-label">Funding Stage</span><span className="sdr-kv-value">{String(acct.funding_stage || "—")}</span></div>
            <div className="sdr-kv-item"><span className="sdr-kv-label">Total Funding</span><span className="sdr-kv-value">{acct.total_funding ? `$${Number(acct.total_funding).toLocaleString()}` : "—"}</span></div>
            <div className="sdr-kv-item"><span className="sdr-kv-label">Country</span><span className="sdr-kv-value">{String(acct.country || "—")}</span></div>
            <div className="sdr-kv-item"><span className="sdr-kv-label">ICP Status</span><span className="sdr-kv-value" style={{ color: acct.icp_status === "pass" ? "var(--sdr-success)" : acct.icp_status === "fail" ? "var(--sdr-danger)" : "inherit" }}>{String(acct.icp_status || "unknown")}</span></div>
            {acct.linkedin_url ? <div className="sdr-kv-item"><span className="sdr-kv-label">LinkedIn</span><a href={String(acct.linkedin_url)} target="_blank" rel="noopener noreferrer" style={{ fontSize: 12, color: "#2563eb" }}>Profile</a></div> : null}
          </div>
        </div>
      </div>

      {/* AI Chat — per-account ChatGPT-style copilot with SerpAPI web search */}
      <CollapsibleChatSection accountId={String(acct.id)} accountName={String(acct.name || acct.domain || "this account")} />

      {/* Signal stack + scores */}
      <div className="sdr-section">
        <div className="sdr-section-head">Scoring &amp; Signal Stack</div>
        <div className="sdr-section-body">
          {/* Priority scores */}
          {stack && (stack as Record<string, unknown>).explanation ? (() => {
            const exp = (stack as Record<string, unknown>).explanation as Record<string, number>;
            return (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 8, marginBottom: 14, paddingBottom: 12, borderBottom: "1px solid #e5e7eb" }}>
                {(["icp_fit", "signal_score", "work_fit", "priority_score"] as const).map(k => (
                  <div key={k} style={{ textAlign: "center", padding: "6px 0" }}>
                    <div style={{ fontSize: 9, textTransform: "uppercase", color: "#9ca3af", fontWeight: 600, letterSpacing: ".5px", marginBottom: 4 }}>{k.replace(/_/g, " ")}</div>
                    <div style={{ fontSize: 20, fontWeight: 700, color: k === "priority_score" ? "#2563eb" : "#111827" }}>{Number(exp[k] || 0).toFixed(1)}</div>
                  </div>
                ))}
              </div>
            );
          })() : null}
          {/* Signal bars */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px 16px" }}>
            {(["funding_score", "hiring_score", "exec_change_score", "web_intent_score", "buyer_intent_score", "total_signal_score"] as const).map(k => {
              const val = Number((stack as Record<string, unknown>)?.[k] ?? 0);
              return (
                <div key={k} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: 11, color: "#6b7280", minWidth: 80, flexShrink: 0 }}>{k.replace(/_score/, "").replace(/_/g, " ")}</span>
                  <ScoreBar value={val} />
                  <span style={{ fontSize: 12, fontWeight: 600, minWidth: 30, textAlign: "right" }}>{val.toFixed(0)}</span>
                </div>
              );
            })}
          </div>
          {/* ICP breakdown */}
          {stack && (stack as Record<string, unknown>).explanation ? (() => {
            const inputs = ((stack as Record<string, unknown>).explanation as Record<string, unknown>)?.inputs as Record<string, number> | undefined;
            if (!inputs) return null;
            return (
              <div style={{ marginTop: 10, paddingTop: 8, borderTop: "1px solid #f3f4f6" }}>
                <div style={{ fontSize: 10, fontWeight: 600, color: "#9ca3af", textTransform: "uppercase", letterSpacing: ".4px", marginBottom: 4 }}>ICP Breakdown</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "4px 12px", fontSize: 11 }}>
                  {Object.entries(inputs).map(([k, v]) => (
                    <div key={k} style={{ display: "flex", justifyContent: "space-between" }}>
                      <span style={{ color: "#6b7280" }}>{k.replace(/_/g, " ")}</span>
                      <span style={{ fontWeight: 600 }}>{Number(v).toFixed(0)}</span>
                    </div>
                  ))}
                </div>
              </div>
            );
          })() : null}
        </div>
      </div>

      {/* Social & Hiring Signals — primary actionable section */}
      <div className="sdr-section">
        <div className="sdr-section-head" style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span>Social &amp; Hiring Signals</span>
          {socialSignals && typeof (socialSignals._cached_at as number | undefined) === "number" ? (
            <span style={{ fontSize: 9, fontWeight: 500, color: "#9ca3af", textTransform: "none", letterSpacing: 0 }}>
              cached {timeAgo(new Date((socialSignals._cached_at as number) * 1000).toISOString())}
            </span>
          ) : null}
          <button
            className="sdr-btn sdr-btn-sm"
            style={{ marginLeft: "auto", padding: "2px 10px", fontSize: 10 }}
            disabled={socialLoading || socialRefreshing}
            onClick={() => void loadSocialSignals(true)}
            title="Re-run Apollo jobs + SerpAPI LinkedIn/Twitter scrape + OpenAI analysis (costs credits)"
          >
            {socialRefreshing ? "Refreshing…" : "Refresh"}
          </button>
        </div>
        <div className="sdr-section-body">
          {socialLoading ? <div style={{ fontSize: 12, color: "#9ca3af" }}>Loading signals...</div> : null}
          {socialSignals && !socialLoading ? (() => {
            const analysis = (socialSignals.analysis || {}) as Record<string, unknown>;
            const signals = (analysis.signals || []) as Array<Record<string, unknown>>;
            const jobPostings = (socialSignals.job_postings || []) as Array<Record<string, string>>;
            const liPosts = (socialSignals.linkedin_posts || []) as Array<Record<string, string>>;
            const twPosts = (socialSignals.twitter_posts || []) as Array<Record<string, string>>;
            const hiringDepts = (analysis.hiring_departments || []) as string[];
            const growthSigs = (analysis.growth_signals || []) as string[];
            const painPts = (analysis.pain_points || []) as string[];

            return (
              <div style={{ display: "grid", gap: 10 }}>
                {/* AI Summary */}
                {analysis.summary ? (
                  <div style={{ padding: "8px 10px", background: "#f0f9ff", borderRadius: 6, border: "1px solid #bae6fd" }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: "#0369a1", marginBottom: 4 }}>AI Signal Summary</div>
                    <div style={{ fontSize: 12, color: "#1e3a5f", lineHeight: 1.5 }}>{String(analysis.summary)}</div>
                    {analysis.outreach_angle ? <div style={{ fontSize: 11, color: "#0369a1", marginTop: 4, fontStyle: "italic" }}>Outreach angle: {String(analysis.outreach_angle)}</div> : null}
                  </div>
                ) : null}

                {/* Signal badges */}
                <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                  {analysis.hiring_intensity && analysis.hiring_intensity !== "none" ? <span style={{ fontSize: 10, fontWeight: 600, padding: "2px 8px", borderRadius: 10, background: analysis.hiring_intensity === "aggressive" ? "#dcfce7" : "#fef3c7", color: analysis.hiring_intensity === "aggressive" ? "#166534" : "#92400e" }}>Hiring: {String(analysis.hiring_intensity)}</span> : null}
                  {analysis.momentum ? <span style={{ fontSize: 10, fontWeight: 600, padding: "2px 8px", borderRadius: 10, background: "#ede9fe", color: "#5b21b6" }}>Momentum: {String(analysis.momentum)}</span> : null}
                  {hiringDepts.length > 0 ? <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 10, background: "#f3f4f6", color: "#374151" }}>Depts: {hiringDepts.join(", ")}</span> : null}
                </div>

                {/* Extracted signals */}
                {signals.length > 0 ? (
                  <div>
                    <div style={{ fontSize: 11, fontWeight: 600, color: "#374151", marginBottom: 4 }}>Detected Signals ({signals.length})</div>
                    {signals.slice(0, 6).map((s, i) => (
                      <div key={i} style={{ fontSize: 12, padding: "4px 0", borderBottom: "1px solid #f3f4f6", display: "flex", gap: 6, alignItems: "flex-start" }}>
                        <span style={{ fontSize: 10, fontWeight: 600, padding: "1px 6px", borderRadius: 8, background: s.type === "hiring" ? "#dcfce7" : s.type === "growth" ? "#dbeafe" : s.type === "pain_point" ? "#fee2e2" : "#f3f4f6", color: s.type === "hiring" ? "#166534" : s.type === "growth" ? "#1e40af" : s.type === "pain_point" ? "#991b1b" : "#374151", whiteSpace: "nowrap", flexShrink: 0 }}>{String(s.type)}</span>
                        <span style={{ color: "#374151" }}>{String(s.description)}</span>
                        {s.confidence ? <span style={{ fontSize: 9, color: "#9ca3af", marginLeft: 4 }}>{(Number(s.confidence) * 100).toFixed(0)}%</span> : null}
                      </div>
                    ))}
                  </div>
                ) : null}

                {/* Growth signals + pain points */}
                {growthSigs.length > 0 ? <div style={{ fontSize: 12 }}><span style={{ fontWeight: 600, color: "#059669" }}>Growth:</span> {growthSigs.join("; ")}</div> : null}
                {painPts.length > 0 ? <div style={{ fontSize: 12 }}><span style={{ fontWeight: 600, color: "#dc2626" }}>Pain points:</span> {painPts.join("; ")}</div> : null}

                {/* Job postings */}
                {jobPostings.length > 0 ? (
                  <div>
                    <div style={{ fontSize: 11, fontWeight: 600, color: "#059669", marginBottom: 4 }}>Active Jobs ({jobPostings.length})</div>
                    {jobPostings.slice(0, 5).map((j, i) => (
                      <div key={i} style={{ fontSize: 12, padding: "3px 0", borderBottom: "1px solid #f3f4f6" }}>
                        <a href={j.url} target="_blank" rel="noopener noreferrer" style={{ color: "#2563eb", textDecoration: "none" }}>{j.title}</a>
                        {j.posted_at ? <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: 6 }}>{j.posted_at.slice(0, 10)}</span> : null}
                      </div>
                    ))}
                  </div>
                ) : null}

                {/* LinkedIn posts */}
                {liPosts.length > 0 ? (
                  <div>
                    <div style={{ fontSize: 11, fontWeight: 600, color: "#2563eb", marginBottom: 4 }}>LinkedIn Posts ({liPosts.length})</div>
                    {liPosts.slice(0, 4).map((p, i) => (
                      <div key={i} style={{ fontSize: 12, padding: "3px 0", borderBottom: "1px solid #f3f4f6" }}>
                        <a href={p.url} target="_blank" rel="noopener noreferrer" style={{ color: "#111827", textDecoration: "none" }}>{(p.title || "").slice(0, 70)}{(p.title || "").length > 70 ? "..." : ""}</a>
                        {p.text ? <div style={{ fontSize: 11, color: "#6b7280", marginTop: 1 }}>{p.text.slice(0, 120)}{p.text.length > 120 ? "..." : ""}</div> : null}
                        {p.date ? <div style={{ fontSize: 10, color: "#9ca3af" }}>{p.date}</div> : null}
                      </div>
                    ))}
                  </div>
                ) : null}

                {/* Twitter posts — every row is now verified to be FROM
                    the company's own account (handle attribution).  We
                    label the section header with the resolved handle so
                    misattribution is visible at a glance, and tag each
                    row with its own matched_handle when multiple
                    candidate handles produced hits. */}
                {twPosts.length > 0 ? (() => {
                  const handles = ((socialSignals.twitter_handles || []) as string[]).filter(Boolean);
                  const handleLabel = handles.length === 1
                    ? `@${handles[0]}`
                    : handles.length > 1
                      ? handles.map((h) => `@${h}`).join(" / ")
                      : "Twitter/X";
                  return (
                  <div>
                    <div style={{ fontSize: 11, fontWeight: 600, color: "#111827", marginBottom: 4, display: "flex", alignItems: "center", gap: 6 }}>
                      <span>Twitter/X — {handleLabel} ({twPosts.length})</span>
                    </div>
                    {twPosts.slice(0, 3).map((p, i) => (
                      <div key={i} style={{ fontSize: 12, padding: "3px 0", borderBottom: "1px solid #f3f4f6" }}>
                        <a href={p.url} target="_blank" rel="noopener noreferrer" style={{ color: "#111827", textDecoration: "none" }}>{(p.title || "").slice(0, 70)}{(p.title || "").length > 70 ? "..." : ""}</a>
                        {p.matched_handle && handles.length > 1 ? (
                          <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: 6 }}>@{p.matched_handle}</span>
                        ) : null}
                      </div>
                    ))}
                  </div>
                  );
                })() : null}

                {!socialSignals.social_active && !socialSignals.hiring_active ? (
                  <div style={{ fontSize: 12, color: "#9ca3af" }}>No recent social activity found</div>
                ) : null}
              </div>
            );
          })() : null}
          {!socialSignals && !socialLoading ? <div style={{ fontSize: 12, color: "#9ca3af" }}>No domain available for signal lookup</div> : null}
        </div>
      </div>

      {/* Work hypothesis */}
      <div className="sdr-section">
        <div className="sdr-section-head">
          Work Hypothesis
          {!hypo ? <span style={{ fontSize: 11, color: "var(--sdr-text-muted)", marginLeft: 8 }}>(none yet)</span> : null}
        </div>
        <div className="sdr-section-body">
          {hypo ? (
            <HypothesisCard hypo={hypo} />
          ) : (
            <div style={{ fontSize: 12, color: "#9ca3af", padding: 8 }}>
              Generating hypothesis from signals...
            </div>
          )}
        </div>
      </div>

      {/* Contacts + Drafts (contact-centric) */}
      <div className="sdr-section">
        <div className="sdr-section-head">Contacts &amp; Outreach ({detail.contacts.length})</div>
        <div className="sdr-section-body" style={{ display: "grid", gap: 0 }}>
          {detail.contacts.map((c, i) => {
            const cid = String(c.id);
            const contactDrafts = detail.drafts.filter((d: Record<string, unknown>) => String(d.contact_id) === cid);
            return <ContactWithDrafts key={cid || i} contact={c} drafts={contactDrafts} onGenerateDraft={(templateId) => onGenerateDraft(cid, templateId)} onRefresh={onRefreshDetail} accountId={String(acct.id)} emailTemplates={emailTemplates} />;
          })}
          {detail.contacts.length === 0 ? <div style={{ fontSize: 12, color: "var(--sdr-text-muted)", padding: 8 }}>No contacts yet. Add one below or search by role.</div> : null}

          {/* Manual add */}
          <div style={{ marginTop: 10, padding: "10px 0", borderTop: "1px solid #e5e7eb", display: "grid", gap: 6 }}>
            <div style={{ fontSize: 11, fontWeight: 600, color: "#9ca3af" }}>Add Contact</div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              <input placeholder="Email" value={addEmail} onChange={e => setAddEmail(e.target.value)} style={{ flex: 1, minWidth: 120, padding: "4px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }} />
              <input placeholder="Name" value={addName} onChange={e => setAddName(e.target.value)} style={{ flex: 1, minWidth: 80, padding: "4px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }} />
              <input placeholder="Title" value={addTitle} onChange={e => setAddTitle(e.target.value)} style={{ width: 80, padding: "4px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }} />
              <button className="sdr-btn sdr-btn-sm" onClick={() => void addManualContact()}>Add</button>
            </div>
            <div style={{ display: "flex", gap: 6, marginTop: 4 }}>
              <input placeholder="Search by role: CEO, CTO, VP..." value={searchTitles} onChange={e => setSearchTitles(e.target.value)} onKeyDown={e => { if (e.key === "Enter") void searchByTitle(); }} style={{ flex: 1, padding: "4px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }} />
              <button className="sdr-btn sdr-btn-primary sdr-btn-sm" disabled={contactSearching} onClick={() => void searchByTitle()}>{contactSearching ? "..." : "Find"}</button>
            </div>
            {contactMsg ? <div style={{ fontSize: 11, color: "#059669" }}>{contactMsg}</div> : null}
          </div>
        </div>
      </div>

      {/* Signal Timeline + Geo (secondary, at bottom) */}
      {detail.signals.length > 0 || detail.geo_attribution.length > 0 ? (
        <div className="sdr-section">
          <div className="sdr-section-head">Data Sources</div>
          <div className="sdr-section-body" style={{ display: "grid", gap: 8 }}>
            {detail.geo_attribution.length > 0 ? (
              <div>
                <div style={{ fontSize: 11, fontWeight: 600, color: "#6b7280", marginBottom: 4 }}>Geography ({detail.geo_attribution.length})</div>
                {detail.geo_attribution.map((g, i) => {
                  const gid = String(g.geography_id);
                  const gName = geoNames[gid] || gid;
                  return <div key={i} style={{ fontSize: 12, padding: "2px 0" }}>{gName} — {(Number(g.weight || 0) * 100).toFixed(0)}%</div>;
                })}
              </div>
            ) : null}
            {detail.signals.length > 0 ? (
              <div>
                <div style={{ fontSize: 11, fontWeight: 600, color: "#6b7280", marginBottom: 4 }}>Signals ({detail.signals.length})</div>
                {detail.signals.slice(0, 8).map((s, i) => (
                  <div key={i} style={{ fontSize: 12, padding: "2px 0", display: "flex", gap: 8 }}>
                    <span style={{ fontWeight: 600, color: "#2563eb", minWidth: 90 }}>{String(s.signal_type)}</span>
                    <span style={{ color: "#6b7280" }}>{String(s.source)}</span>
                    <span style={{ color: "#9ca3af", marginLeft: "auto" }}>{String(s.occurred_at || "").slice(0, 10)}</span>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </>
  );
}


function IngestPanel({ onDone }: { onDone: () => void }) {
  const [domain, setDomain] = React.useState("");
  const [name, setName] = React.useState("");
  const [signalType, setSignalType] = React.useState("funding_round");
  const [source, setSource] = React.useState("manual");
  const [payload, setPayload] = React.useState("{}");
  const [geoId, setGeoId] = React.useState("");
  const [submitting, setSubmitting] = React.useState(false);
  const [result, setResult] = React.useState<string | null>(null);

  const submit = async () => {
    if (!domain.trim()) return;
    setSubmitting(true);
    setResult(null);
    try {
      let parsed: Record<string, unknown> = {};
      try { parsed = JSON.parse(payload); } catch { /* empty */ }
      const res = await fetch("/api/worktrigger/signals/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source,
          signal_type: signalType,
          account: {
            domain: domain.trim(),
            name: name.trim() || null,
            headquarters_geo_id: geoId.trim() || null,
          },
          occurred_at: new Date().toISOString(),
          payload: parsed,
        }),
      });
      const body = await res.json();
      if (!res.ok) throw new Error(JSON.stringify(body));
      setResult(`Ingested: signal=${body.signal_id} account=${body.account_id} (${body.status})`);

      await fetch(`/api/worktrigger/accounts/${encodeURIComponent(body.account_id)}/score`, { method: "POST" });

      onDone();
    } catch (e) {
      setResult(`Error: ${e instanceof Error ? e.message : "Unknown"}`);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={{ padding: 20, overflow: "auto" }}>
      <h2 style={{ marginTop: 0, fontSize: 18, fontWeight: 700 }}>Ingest New Signal</h2>
      <p style={{ fontSize: 13, color: "var(--sdr-text-muted)", marginBottom: 16 }}>Add an account and signal manually. The account will be auto-scored after ingestion.</p>
      <div className="sdr-ingest-form">
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <div className="sdr-field"><label>Domain *</label><input value={domain} onChange={e => setDomain(e.target.value)} placeholder="acme.com" /></div>
          <div className="sdr-field"><label>Company Name</label><input value={name} onChange={e => setName(e.target.value)} placeholder="Acme Inc" /></div>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <div className="sdr-field">
            <label>Signal Type</label>
            <select value={signalType} onChange={e => setSignalType(e.target.value)}>
              <option value="funding_round">Funding Round</option>
              <option value="exec_change">Exec Change</option>
              <option value="hiring_surge">Hiring Surge</option>
              <option value="web_visit">Web Visit</option>
              <option value="buyer_intent">Buyer Intent</option>
              <option value="expansion">Expansion</option>
            </select>
          </div>
          <div className="sdr-field">
            <label>Source</label>
            <select value={source} onChange={e => setSource(e.target.value)}>
              <option value="manual">Manual</option>
              <option value="crunchbase">Crunchbase</option>
              <option value="commonroom">Common Room</option>
              <option value="clay">Clay</option>
              <option value="linkedin">LinkedIn</option>
            </select>
          </div>
        </div>
        <div className="sdr-field"><label>Geography ID (optional)</label><input value={geoId} onChange={e => setGeoId(e.target.value)} placeholder="e.g. 06037 (LA County)" /></div>
        <div className="sdr-field"><label>Signal Payload (JSON)</label><textarea value={payload} onChange={e => setPayload(e.target.value)} /></div>
        <button className="sdr-btn sdr-btn-primary" style={{ justifySelf: "start", padding: "8px 20px" }} disabled={submitting} onClick={() => void submit()}>
          {submitting ? "Ingesting..." : "Ingest Signal + Score"}
        </button>
        {result ? <div style={{ fontSize: 13, padding: 10, background: "#f0fdf4", borderRadius: 6, border: "1px solid #bbf7d0" }}>{result}</div> : null}
      </div>
    </div>
  );
}


function EmailTemplatesPanel({
  templates,
  onTemplatesChanged,
  flash,
}: {
  templates: EmailTemplate[];
  onTemplatesChanged: () => void;
  flash: (msg: string) => void;
}) {
  const [tplName, setTplName] = React.useState("");
  const [tplSubjectA, setTplSubjectA] = React.useState("");
  const [tplSubjectB, setTplSubjectB] = React.useState("");
  const [tplBody, setTplBody] = React.useState("");
  const [tplFollowup, setTplFollowup] = React.useState("");
  const [tplLinkedinDm, setTplLinkedinDm] = React.useState("");
  const [editingTemplateId, setEditingTemplateId] = React.useState<string | null>(null);
  const [tplSaving, setTplSaving] = React.useState(false);
  const [tplDeletingId, setTplDeletingId] = React.useState<string | null>(null);

  const resetTemplateForm = React.useCallback(() => {
    setTplName("");
    setTplSubjectA("");
    setTplSubjectB("");
    setTplBody("");
    setTplFollowup("");
    setTplLinkedinDm("");
    setEditingTemplateId(null);
  }, []);

  const loadTemplateToForm = (tpl: EmailTemplate) => {
    setTplName(String(tpl.name || ""));
    setTplSubjectA(String(tpl.subject_a || ""));
    setTplSubjectB(String(tpl.subject_b || ""));
    setTplBody(String(tpl.email_body || ""));
    setTplFollowup(String(tpl.followup_body || ""));
    setTplLinkedinDm(String(tpl.linkedin_dm || ""));
    setEditingTemplateId(tpl.id);
  };

  const saveTemplate = async () => {
    if (!tplName.trim() || !tplSubjectA.trim() || !tplBody.trim()) {
      flash("Template name, subject A, and email body are required.");
      return;
    }
    setTplSaving(true);
    try {
      const isEditing = Boolean(editingTemplateId);
      const url = isEditing
        ? `/api/worktrigger/templates/email/${encodeURIComponent(String(editingTemplateId))}`
        : "/api/worktrigger/templates/email";
      const res = await fetch(url, {
        method: isEditing ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: tplName,
          subject_a: tplSubjectA,
          subject_b: tplSubjectB,
          email_body: tplBody,
          followup_body: tplFollowup,
          linkedin_dm: tplLinkedinDm,
        }),
      });
      if (!res.ok) {
        flash("Template save failed");
        return;
      }
      resetTemplateForm();
      flash(isEditing ? "Template updated" : "Template saved");
      onTemplatesChanged();
    } catch (e) {
      flash(`Template save failed: ${e instanceof Error ? e.message : "network error"}`);
    } finally {
      setTplSaving(false);
    }
  };

  const deleteTemplate = async (templateId: string) => {
    if (!confirm("Delete this email template?")) return;
    setTplDeletingId(templateId);
    try {
      const res = await fetch(`/api/worktrigger/templates/email/${encodeURIComponent(templateId)}`, { method: "DELETE" });
      if (!res.ok) {
        flash("Template delete failed");
        return;
      }
      if (editingTemplateId === templateId) resetTemplateForm();
      flash("Template deleted");
      onTemplatesChanged();
    } catch (e) {
      flash(`Template delete failed: ${e instanceof Error ? e.message : "network error"}`);
    } finally {
      setTplDeletingId(null);
    }
  };

  return (
    <div className="sdr-panel">
      <div className="sdr-card" style={{ marginBottom: 12 }}>
        <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 8 }}>Saved Email Templates ({templates.length})</div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          {templates.map((tpl) => (
            <span
              key={tpl.id}
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 8,
                border: "1px solid #dbe3e8",
                borderRadius: 999,
                padding: "4px 10px",
                fontSize: 11,
                background: "#f8fafc",
              }}
            >
              {tpl.name}
              <button
                type="button"
                onClick={() => loadTemplateToForm(tpl)}
                style={{ border: "none", background: "none", color: "#2563eb", cursor: "pointer", padding: 0, fontSize: 11 }}
                title="View/edit template"
              >
                Edit
              </button>
              <button
                type="button"
                onClick={() => void deleteTemplate(tpl.id)}
                disabled={tplDeletingId === tpl.id}
                style={{ border: "none", background: "none", color: "#9ca3af", cursor: "pointer", padding: 0 }}
                title="Delete template"
              >
                {tplDeletingId === tpl.id ? "…" : "✕"}
              </button>
            </span>
          ))}
          {templates.length === 0 ? <span style={{ fontSize: 12, color: "#9ca3af" }}>No templates yet.</span> : null}
        </div>
      </div>

      <div className="sdr-card">
        <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 8 }}>
          {editingTemplateId ? "View / Edit Template" : "Create Template"}
        </div>
        <div style={{ display: "grid", gap: 8 }}>
          <input
            placeholder="Template name"
            value={tplName}
            onChange={(e) => setTplName(e.target.value)}
            style={{ padding: "6px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }}
          />
          <input
            placeholder="Subject A seed (AI will personalize it)"
            value={tplSubjectA}
            onChange={(e) => setTplSubjectA(e.target.value)}
            style={{ padding: "6px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }}
          />
          <input
            placeholder="Subject B (optional)"
            value={tplSubjectB}
            onChange={(e) => setTplSubjectB(e.target.value)}
            style={{ padding: "6px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }}
          />
          <textarea
            placeholder="Email body seed (AI will customize by recipient + company signals)"
            value={tplBody}
            onChange={(e) => setTplBody(e.target.value)}
            rows={5}
            style={{ padding: "6px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4, resize: "vertical" }}
          />
          <input
            placeholder="Follow-up body (optional)"
            value={tplFollowup}
            onChange={(e) => setTplFollowup(e.target.value)}
            style={{ padding: "6px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }}
          />
          <input
            placeholder="LinkedIn DM / connect-note body (optional)"
            value={tplLinkedinDm}
            onChange={(e) => setTplLinkedinDm(e.target.value)}
            style={{ padding: "6px 8px", fontSize: 12, border: "1px solid #d1d5db", borderRadius: 4 }}
          />
          <div>
            <button className="sdr-btn sdr-btn-primary sdr-btn-sm" disabled={tplSaving} onClick={() => void saveTemplate()}>
              {tplSaving ? "Saving..." : (editingTemplateId ? "Update template" : "Save template")}
            </button>
            {editingTemplateId ? (
              <button
                className="sdr-btn sdr-btn-sm"
                style={{ marginLeft: 8 }}
                disabled={tplSaving}
                onClick={resetTemplateForm}
              >
                Cancel edit
              </button>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}


const TEST_PATTERN = /\b(test|smoke|example|demo|placeholder|fixture|clay[- ]?(smoke|test|only|v\d)|edge[- ]?\d+|a0\d|d2cr|cr[- ]?(smoke|test)|scan[- ]?test|final[- ]?pipe|score[- ]?example|pipeline[- ]?test|detail[- ]?(smoke|example|test)|audit[- ]?(full|clay|test)|fin[- ]?(clay|test)|exec[- ]?demo|phase[- ]?next|unknown[- ]?co\d*|skyrocketventures)\b/i;

function isTestAccount(a: { name?: unknown; domain?: unknown; account_name?: unknown } | null | undefined): boolean {
  if (!a) return false;
  const name = String(a.name || a.account_name || "");
  const dom = String(a.domain || "");
  return TEST_PATTERN.test(`${name} ${dom}`);
}

function AnalyticsPanel({ analytics, heartbeats, onRefresh }: { analytics: Record<string, unknown> | null; heartbeats: Array<Record<string, unknown>>; onRefresh: () => void }) {
  const [subTab, setSubTab] = React.useState<AnalyticsSubTab>("pipeline");
  const [allDrafts, setAllDrafts] = React.useState<QueueRow[]>([]);
  const [opps, setOpps] = React.useState<Array<Record<string, unknown>>>([]);
  const [accounts, setAccounts] = React.useState<Array<Record<string, unknown>>>([]);
  const [vendors, setVendors] = React.useState<Record<string, Record<string, unknown>>>({});
  const [dlq, setDlq] = React.useState<Array<Record<string, unknown>>>([]);
  const [conflicts, setConflicts] = React.useState<Array<Record<string, unknown>>>([]);
  const [suppressEmail, setSuppressEmail] = React.useState("");
  const [hideTest, setHideTest] = React.useState(true);
  const [toast, setToast] = React.useState("");

  const flash = (m: string) => { setToast(m); setTimeout(() => setToast(""), 3000); };

  const loadBoardData = React.useCallback(async () => {
    const [dRes, oRes, aRes, vRes, dlqRes, cRes] = await Promise.all([
      fetch("/api/worktrigger/queue?status=all&limit=500"),
      fetch("/api/worktrigger/opportunities?limit=500"),
      fetch("/api/worktrigger/accounts/all?limit=500"),
      fetch("/api/worktrigger/vendors/status"),
      fetch("/api/worktrigger/jobs/dead-letter?limit=100"),
      fetch("/api/worktrigger/crm/conflicts?status=open&limit=100"),
    ]);
    if (dRes.ok) setAllDrafts(await dRes.json());
    if (oRes.ok) setOpps(await oRes.json());
    if (aRes.ok) setAccounts(await aRes.json());
    if (vRes.ok) setVendors(await vRes.json());
    if (dlqRes.ok) setDlq(await dlqRes.json());
    if (cRes.ok) setConflicts(await cRes.json());
  }, []);

  React.useEffect(() => { void loadBoardData(); }, [loadBoardData]);

  const visibleDrafts = React.useMemo(
    () => (hideTest ? allDrafts.filter(d => !isTestAccount(d)) : allDrafts),
    [allDrafts, hideTest],
  );
  const visibleOpps = React.useMemo(
    () => (hideTest ? opps.filter(o => !isTestAccount(o)) : opps),
    [opps, hideTest],
  );
  const visibleAccounts = React.useMemo(
    () => (hideTest ? accounts.filter(a => !isTestAccount(a)) : accounts),
    [accounts, hideTest],
  );
  const hiddenCount = (allDrafts.length - visibleDrafts.length)
    + (opps.length - visibleOpps.length)
    + (accounts.length - visibleAccounts.length);

  const draftCounts = (analytics?.draft_status_counts as Record<string, number> | undefined) || {};
  const throughput = (analytics?.throughput_7d as Record<string, unknown> | undefined) || {};
  const quality = (analytics?.quality as Record<string, unknown> | undefined) || {};
  const speed = (analytics?.speed_hours as Record<string, unknown> | undefined) || {};
  const drift = (analytics?.crm_drift as Record<string, unknown> | undefined) || {};

  // Consolidated funnel: 5 stages with primary counts and a secondary metric
  // underneath so the whole analytics header fits in one compact strip.
  const funnel = [
    { key: "signals", label: "Signals", count: Number(analytics?.signals_total || 0), sub: `avg ${Number(analytics?.avg_signal_score || 0).toFixed(1)}` },
    { key: "drafts", label: "Drafts", count: Number(draftCounts.draft_ready || 0) + Number(draftCounts.approved || 0) + Number(draftCounts.sent || 0) + Number(draftCounts.replied || 0), sub: `+${Number(throughput.drafts_created || 0)} / 7d` },
    { key: "approved", label: "Approved", count: Number(draftCounts.approved || 0) + Number(draftCounts.sent || 0) + Number(draftCounts.replied || 0), sub: `${(Number(quality.approval_rate || 0) * 100).toFixed(0)}% rate` },
    { key: "sent", label: "Sent", count: Number(draftCounts.sent || 0) + Number(draftCounts.replied || 0), sub: `${Number(speed.median_create_to_sent_or_replied || 0).toFixed(1)}h median` },
    { key: "replied", label: "Replied", count: Number(draftCounts.replied || 0), sub: "positive intent" },
    { key: "opps", label: "Opps", count: visibleOpps.length, sub: Number(drift.unsynced_opportunities || 0) > 0 ? `${Number(drift.unsynced_opportunities || 0)} unsynced` : "synced" },
  ];

  const purgeTestData = async () => {
    if (!confirm("Permanently delete ALL test/smoke/example accounts and their drafts, contacts, signals? This cannot be undone.")) return;
    const res = await fetch("/api/worktrigger/accounts/purge-test", { method: "POST" });
    if (res.ok) {
      const data = await res.json();
      flash(`Purged ${data.deleted} test accounts`);
      void onRefresh();
      void loadBoardData();
    } else {
      flash("Purge failed");
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" }}>
      {/* Unified funnel header — replaces the old KPI bar + funnel strip */}
      <div className="sdr-funnel-v2">
        {funnel.map((s, i) => {
          const prev = funnel[i - 1];
          const conv = prev && prev.count > 0 ? Math.round((s.count / prev.count) * 100) : null;
          return (
            <React.Fragment key={s.key}>
              {i > 0 ? (
                <div className="sdr-funnel-v2-arrow">
                  <span>→</span>
                  {conv !== null ? <span className="sdr-funnel-v2-conv">{conv}%</span> : null}
                </div>
              ) : null}
              <div className="sdr-funnel-v2-step" data-active={i < funnel.findIndex(f => f.count === 0 && f !== funnel[0])}>
                <div className="sdr-funnel-v2-count">{s.count.toLocaleString()}</div>
                <div className="sdr-funnel-v2-label">{s.label}</div>
                <div className="sdr-funnel-v2-sub">{s.sub}</div>
              </div>
            </React.Fragment>
          );
        })}
        <div className="sdr-funnel-v2-actions">
          <label className="sdr-funnel-v2-toggle" title="Hide accounts/drafts whose name or domain matches common test fixtures (clay, smoke, example, etc.)">
            <input type="checkbox" checked={hideTest} onChange={e => setHideTest(e.target.checked)} />
            <span>Hide test data{hiddenCount > 0 && hideTest ? ` (${hiddenCount})` : ""}</span>
          </label>
          <button className="sdr-btn sdr-btn-sm sdr-btn-danger" onClick={() => void purgeTestData()} title="Permanently delete all accounts matching test patterns">
            Purge test
          </button>
          <button
            className="sdr-btn sdr-btn-sm"
            title="Archive duplicate active drafts so each contact has exactly one. No LLM/vendor credits used."
            onClick={async () => {
              if (!confirm("Collapse duplicate active drafts across the entire pipeline?\n\nFor each (account, contact, channel) tuple, keeps ONLY the most recent active draft and archives the rest. No credits used.")) return;
              const res = await fetch("/api/worktrigger/drafts/collapse-duplicates", { method: "POST" });
              if (res.ok) {
                const data = await res.json();
                flash(`Archived ${data.archived_count} duplicate draft${data.archived_count === 1 ? "" : "s"} · kept ${data.kept_count}`);
                void onRefresh();
                void loadBoardData();
              } else {
                flash("Cleanup failed");
              }
            }}
          >
            Dedupe drafts
          </button>
          <button className="sdr-btn sdr-btn-sm" onClick={() => { void onRefresh(); void loadBoardData(); }}>Refresh</button>
        </div>
      </div>

      {/* Sub tabs */}
      <div className="sdr-sub-tabs">
        {(["pipeline", "opportunities", "accounts", "operations"] as const).map(t => {
          const label = t === "pipeline" ? "Pipeline" : t === "opportunities" ? "Opportunities" : t === "accounts" ? "Accounts" : "Operations";
          const countLookup: Record<typeof t, number> = {
            pipeline: visibleDrafts.length,
            opportunities: visibleOpps.length,
            accounts: visibleAccounts.length,
            operations: dlq.length + conflicts.length,
          } as Record<typeof t, number>;
          return (
            <button key={t} className="sdr-sub-tab" data-active={subTab === t} onClick={() => setSubTab(t)}>
              {label} <span className="sdr-sub-tab-count">{countLookup[t]}</span>
            </button>
          );
        })}
      </div>

      {/* Board content */}
      <div style={{ flex: 1, overflow: "auto", display: "flex", flexDirection: "column" }}>
        {subTab === "pipeline" ? <PipelineBoard drafts={visibleDrafts} onRefresh={() => { void onRefresh(); void loadBoardData(); }} flash={flash} /> : null}
        {subTab === "opportunities" ? <OpportunitiesBoard opps={visibleOpps} /> : null}
        {subTab === "accounts" ? <AccountsBoard accounts={visibleAccounts} onRefresh={() => { void loadBoardData(); }} flash={flash} /> : null}
        {subTab === "operations" ? (
          <OperationsPanel
            vendors={vendors}
            dlq={dlq}
            conflicts={conflicts}
            heartbeats={heartbeats}
            suppressEmail={suppressEmail}
            onSuppressEmailChange={setSuppressEmail}
            onSuppress={async () => {
              if (!suppressEmail.trim()) return;
              const res = await fetch(`/api/worktrigger/compliance/suppress?email=${encodeURIComponent(suppressEmail.trim())}&reason=manual&source=operator`, { method: "POST" });
              if (res.ok) { flash("Suppressed"); setSuppressEmail(""); }
            }}
            onRequeue={async (dlqId: string) => {
              const res = await fetch(`/api/worktrigger/jobs/dead-letter/${encodeURIComponent(dlqId)}/requeue?max_attempts=3`, { method: "POST" });
              if (res.ok) { flash("Requeued"); void loadBoardData(); }
            }}
            onResolveConflict={async (cId: string) => {
              const res = await fetch(`/api/worktrigger/crm/conflicts/${encodeURIComponent(cId)}/resolve?resolved_by=operator&resolved_value=app`, { method: "POST" });
              if (res.ok) { flash("Resolved"); void loadBoardData(); }
            }}
          />
        ) : null}
      </div>
      {toast ? <div className="sdr-toast">{toast}</div> : null}
    </div>
  );
}


function timeAgo(iso: string): string {
  if (!iso) return "";
  const ms = Date.now() - new Date(iso).getTime();
  const m = Math.floor(ms / 60000);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}


function PipelineBoard({ drafts, onRefresh, flash }: { drafts: QueueRow[]; onRefresh: () => void; flash: (m: string) => void }) {
  const [search, setSearch] = React.useState("");
  const [minSignal, setMinSignal] = React.useState(0);
  const [showArchived, setShowArchived] = React.useState(false);

  const ACTIVE_COLS = [
    { key: "draft_ready", label: "Draft Ready", tone: "warning" as const },
    { key: "approved", label: "Approved", tone: "info" as const },
    { key: "sent", label: "Sent", tone: "success" as const },
    { key: "replied", label: "Replied", tone: "accent" as const },
  ];
  const ARCHIVE_COLS = [
    { key: "discarded", label: "Discarded", tone: "danger" as const },
    { key: "snoozed", label: "Snoozed", tone: "muted" as const },
  ];

  const filtered = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    return drafts.filter(d => {
      if (minSignal > 0 && (d.signal_score || 0) < minSignal) return false;
      if (!q) return true;
      return (
        (d.account_name || "").toLowerCase().includes(q)
        || (d.domain || "").toLowerCase().includes(q)
        || (d.contact_name || "").toLowerCase().includes(q)
        || (d.subject_a || "").toLowerCase().includes(q)
      );
    });
  }, [drafts, search, minSignal]);

  const group = (key: string): QueueRow[] => filtered.filter(d => (d.status || "draft_ready") === key);

  const archivedCount = ARCHIVE_COLS.reduce((sum, c) => sum + group(c.key).length, 0);

  const quickAct = async (draftId: string, action: string) => {
    await fetch(`/api/worktrigger/drafts/${encodeURIComponent(draftId)}/review`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, reviewer_user_id: "sdr_operator_1" }),
    });
    onRefresh();
  };

  const purgeDiscarded = async () => {
    const n = group("discarded").length;
    if (n === 0 || !confirm(`Permanently delete ${n} discarded draft${n === 1 ? "" : "s"}?`)) return;
    const res = await fetch("/api/worktrigger/drafts/purge?status=discarded", { method: "POST" });
    if (res.ok) { flash(`Purged ${n} discarded drafts`); onRefresh(); }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}>
      <div className="sdr-board-toolbar">
        <input
          className="sdr-board-search"
          placeholder="Search account, domain, contact, subject…"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
        <select value={minSignal} onChange={e => setMinSignal(Number(e.target.value))} className="sdr-board-select">
          <option value={0}>Any signal</option>
          <option value={20}>Signal ≥ 20</option>
          <option value={40}>Signal ≥ 40</option>
          <option value={60}>Signal ≥ 60</option>
          <option value={80}>Signal ≥ 80</option>
        </select>
        <label className="sdr-board-chip" data-active={showArchived}>
          <input type="checkbox" checked={showArchived} onChange={e => setShowArchived(e.target.checked)} />
          Archived {archivedCount > 0 ? <span className="sdr-board-chip-count">{archivedCount}</span> : null}
        </label>
        {showArchived && group("discarded").length > 0 ? (
          <button className="sdr-btn sdr-btn-sm sdr-btn-danger" onClick={() => void purgeDiscarded()}>
            Purge discarded
          </button>
        ) : null}
        <div className="sdr-board-toolbar-spacer" />
        <span style={{ fontSize: 11, color: "var(--sdr-text-muted)" }}>{filtered.length} of {drafts.length}</span>
      </div>

      <div className="sdr-kanban">
        {(showArchived ? [...ACTIVE_COLS, ...ARCHIVE_COLS] : ACTIVE_COLS).map(col => {
          const items = group(col.key);
          return (
            <div className="sdr-kanban-col" key={col.key} data-tone={col.tone}>
              <div className="sdr-kanban-col-head">
                <span>{col.label}</span>
                <span className="sdr-kanban-count">{items.length}</span>
              </div>
              <div className="sdr-kanban-body">
                {items.length === 0 ? <div className="sdr-kanban-empty">Empty</div> : null}
                {items.map(d => (
                  <div className="sdr-kanban-card-v2" key={d.draft_id}>
                    <div className="sdr-kanban-card-title">{d.account_name || d.domain || d.account_id}</div>
                    {d.contact_name ? (
                      <div className="sdr-kanban-card-meta">{d.contact_name}{d.contact_title ? ` · ${d.contact_title}` : ""}</div>
                    ) : null}
                    {d.subject_a ? (
                      <div className="sdr-kanban-card-subject" title={d.subject_a}>{d.subject_a}</div>
                    ) : null}
                    <div className="sdr-kanban-card-footer">
                      <span className={`sdr-signal-chip sdr-signal-chip-${(d.signal_score || 0) >= 60 ? "hot" : (d.signal_score || 0) >= 30 ? "warm" : "cold"}`}>
                        {Number(d.signal_score || 0).toFixed(0)}
                      </span>
                      <span className="sdr-kanban-card-time">{timeAgo(d.updated_at || "")}</span>
                    </div>
                    <div className="sdr-kanban-card-actions">
                      {col.key === "draft_ready" ? (
                        <>
                          <button className="sdr-btn sdr-btn-primary sdr-btn-sm" onClick={() => void quickAct(d.draft_id, "approve")}>Approve</button>
                          <button className="sdr-btn sdr-btn-sm" onClick={() => void quickAct(d.draft_id, "snooze")}>Snooze</button>
                          <button className="sdr-btn sdr-btn-danger sdr-btn-sm" onClick={() => void quickAct(d.draft_id, "discard")}>Discard</button>
                        </>
                      ) : null}
                      {col.key === "approved" ? (
                        <>
                          <button className="sdr-btn sdr-btn-success sdr-btn-sm" onClick={async () => {
                            await fetch(`/api/worktrigger/drafts/${encodeURIComponent(d.draft_id)}/send`, { method: "POST" });
                            onRefresh();
                          }}>Send now</button>
                          <button className="sdr-btn sdr-btn-sm" onClick={() => void quickAct(d.draft_id, "unapprove")}>Move to Draft</button>
                        </>
                      ) : null}
                      {col.key === "sent" ? (
                        <button className="sdr-btn sdr-btn-sm" onClick={async () => {
                          await fetch(`/api/worktrigger/crm/sync/opportunity?draft_id=${encodeURIComponent(d.draft_id)}`, { method: "POST" });
                          onRefresh();
                        }}>Sync CRM</button>
                      ) : null}
                      {col.key === "discarded" || col.key === "snoozed" ? (
                        <button className="sdr-btn sdr-btn-sm" onClick={() => void quickAct(d.draft_id, "approve")}>Restore</button>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}


function OpportunitiesBoard({ opps }: { opps: Array<Record<string, unknown>> }) {
  const STAGES = [
    { key: "new", label: "New", tone: "info" as const },
    { key: "meeting_booked", label: "Meeting Booked", tone: "info" as const },
    { key: "discovery_done", label: "Discovery", tone: "warning" as const },
    { key: "scoped", label: "Scoped", tone: "accent" as const },
    { key: "shortlist_prepared", label: "Shortlist", tone: "accent" as const },
    { key: "won", label: "Won", tone: "success" as const },
    { key: "lost", label: "Lost", tone: "danger" as const },
  ];
  const [showEmpty, setShowEmpty] = React.useState(false);
  const grouped = new Map<string, Array<Record<string, unknown>>>();
  for (const s of STAGES) grouped.set(s.key, []);
  for (const o of opps) {
    const k = String(o.stage || "new");
    (grouped.get(k) || grouped.get("new") || []).push(o);
  }
  const visibleStages = showEmpty ? STAGES : STAGES.filter(s => (grouped.get(s.key) || []).length > 0 || s.key === "new");
  const total = opps.length;
  const won = (grouped.get("won") || []).length;
  const lost = (grouped.get("lost") || []).length;
  const winRate = total > 0 ? Math.round((won / Math.max(1, won + lost)) * 100) : 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}>
      <div className="sdr-board-toolbar">
        <div className="sdr-opps-summary">
          <span><strong>{total}</strong> total</span>
          <span><strong>{won}</strong> won</span>
          <span><strong>{lost}</strong> lost</span>
          <span><strong>{winRate}%</strong> win rate</span>
        </div>
        <div className="sdr-board-toolbar-spacer" />
        <label className="sdr-board-chip" data-active={showEmpty}>
          <input type="checkbox" checked={showEmpty} onChange={e => setShowEmpty(e.target.checked)} />
          Show empty stages
        </label>
      </div>
      {total === 0 ? (
        <div className="sdr-empty">
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>No opportunities yet</div>
            <div style={{ fontSize: 12 }}>Opportunities appear when a sent draft gets a positive reply or is promoted from the Pipeline.</div>
          </div>
        </div>
      ) : (
        <div className="sdr-kanban">
          {visibleStages.map(s => {
            const items = grouped.get(s.key) || [];
            return (
              <div className="sdr-kanban-col" key={s.key} data-tone={s.tone}>
                <div className="sdr-kanban-col-head">
                  <span>{s.label}</span>
                  <span className="sdr-kanban-count">{items.length}</span>
                </div>
                <div className="sdr-kanban-body">
                  {items.length === 0 ? <div className="sdr-kanban-empty">Empty</div> : null}
                  {items.map((o, i) => (
                    <div className="sdr-kanban-card-v2" key={String(o.id || i)}>
                      <div className="sdr-kanban-card-title">{String(o.account_name || o.domain || o.account_id || "Unknown")}</div>
                      {o.contact_name ? (
                        <div className="sdr-kanban-card-meta">{String(o.contact_name)}{o.contact_title ? ` · ${o.contact_title}` : ""}</div>
                      ) : null}
                      <div className="sdr-kanban-card-footer">
                        {o.crm_id ? <span className="sdr-crm-chip" title="Synced to HubSpot">HS ↗</span> : <span className="sdr-crm-chip sdr-crm-chip-pending">Unsynced</span>}
                        <span className="sdr-kanban-card-time">{timeAgo(String(o.updated_at || o.created_at || ""))}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}


type AccountRow = Record<string, unknown>;

function AccountsBoard({ accounts, onRefresh, flash }: { accounts: AccountRow[]; onRefresh: () => void; flash: (m: string) => void }) {
  const [search, setSearch] = React.useState("");
  const [sortBy, setSortBy] = React.useState<"signal" | "priority" | "contacts" | "recent">("signal");
  const [icpFilter, setIcpFilter] = React.useState<"all" | "pass" | "fail" | "unknown">("all");
  const [hasContacts, setHasContacts] = React.useState(false);
  const [selected, setSelected] = React.useState<Set<string>>(new Set());

  const filtered = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    const out = accounts.filter(a => {
      if (icpFilter !== "all" && String(a.icp_status || "unknown") !== icpFilter) return false;
      if (hasContacts && Number(a.contact_count || 0) <= 0) return false;
      if (!q) return true;
      return (
        String(a.name || "").toLowerCase().includes(q)
        || String(a.domain || "").toLowerCase().includes(q)
        || String(a.industry || "").toLowerCase().includes(q)
      );
    });
    const keyFn: Record<typeof sortBy, (a: AccountRow) => number> = {
      signal: (a) => Number(a.signal_score || 0),
      priority: (a) => Number(a.priority_score || a.signal_score || 0),
      contacts: (a) => Number(a.contact_count || 0),
      recent: (a) => new Date(String(a.updated_at || a.created_at || 0)).getTime(),
    };
    out.sort((a, b) => keyFn[sortBy](b) - keyFn[sortBy](a));
    return out;
  }, [accounts, search, sortBy, icpFilter, hasContacts]);

  const toggle = (id: string) => setSelected(prev => {
    const n = new Set(prev);
    if (n.has(id)) n.delete(id); else n.add(id);
    return n;
  });
  const clearSel = () => setSelected(new Set());

  const deleteSelected = async () => {
    const ids = Array.from(selected);
    if (ids.length === 0) return;
    if (!confirm(`Permanently delete ${ids.length} account${ids.length === 1 ? "" : "s"}?`)) return;
    const res = await fetch("/api/worktrigger/accounts/bulk-delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(ids),
    });
    if (res.ok) {
      const data = await res.json();
      flash(`Deleted ${data.deleted} account${data.deleted === 1 ? "" : "s"}`);
      clearSel();
      onRefresh();
    }
  };

  if (accounts.length === 0) {
    return <div className="sdr-empty">No accounts yet. Ingest signals or intake from the map to get started.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}>
      <div className="sdr-board-toolbar">
        <input
          className="sdr-board-search"
          placeholder="Search name, domain, industry…"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
        <select value={sortBy} onChange={e => setSortBy(e.target.value as typeof sortBy)} className="sdr-board-select">
          <option value="signal">Sort: Signal</option>
          <option value="priority">Sort: Priority</option>
          <option value="contacts">Sort: Contacts</option>
          <option value="recent">Sort: Recent</option>
        </select>
        <select value={icpFilter} onChange={e => setIcpFilter(e.target.value as typeof icpFilter)} className="sdr-board-select">
          <option value="all">All ICP</option>
          <option value="pass">ICP: Pass</option>
          <option value="fail">ICP: Fail</option>
          <option value="unknown">ICP: Unknown</option>
        </select>
        <label className="sdr-board-chip" data-active={hasContacts}>
          <input type="checkbox" checked={hasContacts} onChange={e => setHasContacts(e.target.checked)} />
          Has contacts
        </label>
        <div className="sdr-board-toolbar-spacer" />
        {selected.size > 0 ? (
          <>
            <span style={{ fontSize: 12, fontWeight: 600 }}>{selected.size} selected</span>
            <button className="sdr-btn sdr-btn-sm sdr-btn-danger" onClick={() => void deleteSelected()}>Delete</button>
            <button className="sdr-btn sdr-btn-sm" onClick={clearSel}>Clear</button>
          </>
        ) : (
          <span style={{ fontSize: 11, color: "var(--sdr-text-muted)" }}>{filtered.length} of {accounts.length}</span>
        )}
      </div>
      <div className="sdr-accounts-grid-v2">
        {filtered.map((a, i) => {
          const id = String(a.id || i);
          const isSel = selected.has(id);
          const icp = String(a.icp_status || "unknown");
          const icpTone = icp === "pass" ? "success" : icp === "fail" ? "danger" : "muted";
          return (
            <div className={`sdr-acct-card ${isSel ? "sdr-acct-card-selected" : ""}`} key={id}>
              <div className="sdr-acct-card-head">
                <input type="checkbox" className="sdr-acct-check" checked={isSel} onChange={() => toggle(id)} />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div className="sdr-acct-name" title={String(a.name || a.domain || "")}>
                    {String(a.name || a.domain || "Unknown")}
                  </div>
                  <div className="sdr-acct-domain">{String(a.domain || "")}</div>
                </div>
                <span className={`sdr-icp-pill sdr-icp-pill-${icpTone}`}>{icp}</span>
              </div>
              <div className="sdr-acct-signal-row">
                <div className="sdr-acct-signal-track">
                  <div
                    className="sdr-acct-signal-fill"
                    style={{ width: `${Math.min(100, Number(a.signal_score || 0))}%` }}
                    data-heat={Number(a.signal_score || 0) >= 60 ? "hot" : Number(a.signal_score || 0) >= 30 ? "warm" : "cold"}
                  />
                </div>
                <span className="sdr-acct-signal-val">{Number(a.signal_score || 0).toFixed(0)}</span>
              </div>
              <div className="sdr-acct-stats">
                <span>{Number(a.signal_count || 0)} signals</span>
                <span>{Number(a.contact_count || 0)} contacts</span>
                <span>{Number(a.draft_count || 0)} drafts</span>
                {Number(a.hypothesis_count || 0) > 0 ? <span>{Number(a.hypothesis_count)} hypo</span> : null}
              </div>
              {a.industry ? <div className="sdr-acct-industry">{String(a.industry)}</div> : null}
            </div>
          );
        })}
        {filtered.length === 0 ? <div className="sdr-empty" style={{ gridColumn: "1 / -1", padding: 40 }}>No accounts match the current filters.</div> : null}
      </div>
    </div>
  );
}


function OperationsPanel({
  vendors,
  dlq,
  conflicts,
  heartbeats,
  suppressEmail,
  onSuppressEmailChange,
  onSuppress,
  onRequeue,
  onResolveConflict,
}: {
  vendors: Record<string, Record<string, unknown>>;
  dlq: Array<Record<string, unknown>>;
  conflicts: Array<Record<string, unknown>>;
  heartbeats: Array<Record<string, unknown>>;
  suppressEmail: string;
  onSuppressEmailChange: (v: string) => void;
  onSuppress: () => void;
  onRequeue: (dlqId: string) => void;
  onResolveConflict: (cId: string) => void;
}) {
  return (
    <div className="sdr-ops-grid">
      {/* Vendor status */}
      <div className="sdr-section">
        <div className="sdr-section-head">Vendor Integrations</div>
        <div className="sdr-section-body">
          {Object.entries(vendors).map(([name, info]) => (
            <div className="sdr-vendor-row" key={name}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div className={`sdr-vendor-dot ${info.configured ? "sdr-vendor-dot-on" : "sdr-vendor-dot-off"}`} />
                <span style={{ fontWeight: 500 }}>{name}</span>
              </div>
              <span style={{ color: "var(--sdr-text-muted)", fontSize: 10 }}>{String(info.env_var || "")}</span>
            </div>
          ))}
          {Object.keys(vendors).length === 0 ? <div style={{ fontSize: 12, color: "var(--sdr-text-muted)" }}>Loading...</div> : null}
        </div>
      </div>

      {/* Dead letter queue */}
      <div className="sdr-section">
        <div className="sdr-section-head">Dead Letter Queue ({dlq.length})</div>
        <div className="sdr-section-body">
          {dlq.slice(0, 20).map((d, i) => (
            <div className="sdr-dlq-row" key={i}>
              <div style={{ flex: 1 }}>
                <div style={{ fontWeight: 600 }}>{String(d.job_type)}</div>
                <div style={{ color: "var(--sdr-danger)", fontSize: 10 }}>{String(d.error_message || "").slice(0, 80)}</div>
              </div>
              <button className="sdr-btn sdr-btn-sm" onClick={() => onRequeue(String(d.id))}>Requeue</button>
            </div>
          ))}
          {dlq.length === 0 ? <div style={{ fontSize: 12, color: "var(--sdr-text-muted)" }}>No dead letters</div> : null}
        </div>
      </div>

      {/* CRM conflicts */}
      <div className="sdr-section">
        <div className="sdr-section-head">CRM Conflicts ({conflicts.length})</div>
        <div className="sdr-section-body">
          {conflicts.slice(0, 20).map((c, i) => (
            <div className="sdr-conflict-row" key={i}>
              <div style={{ flex: 1 }}>
                <div style={{ fontWeight: 600 }}>{String(c.field_name)}</div>
                <div style={{ fontSize: 10, color: "var(--sdr-text-muted)" }}>App: {String(c.app_value || "null")} | CRM: {String(c.crm_value || "null")}</div>
              </div>
              <span className="sdr-card-badge sdr-badge-draft_ready">{String(c.policy)}</span>
              <button className="sdr-btn sdr-btn-sm" onClick={() => onResolveConflict(String(c.id))}>Resolve</button>
            </div>
          ))}
          {conflicts.length === 0 ? <div style={{ fontSize: 12, color: "var(--sdr-text-muted)" }}>No open conflicts</div> : null}
        </div>
      </div>

      {/* Worker heartbeats */}
      <div className="sdr-section">
        <div className="sdr-section-head">Worker Heartbeats</div>
        <div className="sdr-section-body">
          {heartbeats.map((hb, i) => (
            <div key={i} style={{ display: "flex", gap: 12, padding: "6px 0", borderBottom: "1px solid #f3f4f6", fontSize: 12, alignItems: "center" }}>
              <span style={{ fontWeight: 600, minWidth: 120 }}>{String(hb.worker_id)}</span>
              <span className={`sdr-card-badge sdr-badge-${hb.status === "idle" ? "snoozed" : hb.status === "running" ? "approved" : "discarded"}`}>{String(hb.status)}</span>
              <span style={{ color: "var(--sdr-text-muted)", marginLeft: "auto", fontSize: 10 }}>{String(hb.last_seen_at || "").slice(0, 19)}</span>
            </div>
          ))}
          {heartbeats.length === 0 ? <div style={{ fontSize: 12, color: "var(--sdr-text-muted)" }}>No heartbeats</div> : null}
        </div>
      </div>

      {/* Compliance quick actions */}
      <div className="sdr-section" style={{ gridColumn: "1 / -1" }}>
        <div className="sdr-section-head">Compliance Quick Actions</div>
        <div className="sdr-section-body">
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <input
              value={suppressEmail}
              onChange={e => onSuppressEmailChange(e.target.value)}
              placeholder="email@example.com"
              style={{ font: "inherit", fontSize: 12, padding: "6px 10px", border: "1px solid var(--sdr-border)", borderRadius: 6, flex: 1, maxWidth: 300 }}
            />
            <button className="sdr-btn sdr-btn-danger sdr-btn-sm" onClick={() => void onSuppress()}>Suppress Email</button>
          </div>
        </div>
      </div>
    </div>
  );
}
