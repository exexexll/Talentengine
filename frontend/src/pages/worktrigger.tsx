import React from "react";

type QueueRow = {
  draft_id: string;
  status: string;
  account_id: string;
  account_name?: string;
  domain?: string;
  contact_name?: string;
  contact_title?: string;
  signal_score: number;
  subject_a?: string;
  updated_at?: string;
};

type AccountDetail = {
  account: Record<string, unknown>;
  geo_attribution: Array<Record<string, unknown>>;
  signals: Array<Record<string, unknown>>;
  signal_stack: Record<string, unknown> | null;
  contacts: Array<Record<string, unknown>>;
  work_hypotheses: Array<Record<string, unknown>>;
  drafts: Array<Record<string, unknown>>;
};

type WorkTriggerPageProps = {
  onBack: () => void;
  onOpenAnalytics: () => void;
};

export function WorkTriggerPage({ onBack, onOpenAnalytics }: WorkTriggerPageProps): JSX.Element {
  const [rows, setRows] = React.useState<QueueRow[]>([]);
  const [status, setStatus] = React.useState("draft_ready");
  const [loading, setLoading] = React.useState(true);
  const [selectedAccountId, setSelectedAccountId] = React.useState<string | null>(null);
  const [detail, setDetail] = React.useState<AccountDetail | null>(null);
  const [detailLoading, setDetailLoading] = React.useState(false);
  const [reviewerId, setReviewerId] = React.useState("sdr_operator_1");
  const [editSubject, setEditSubject] = React.useState("");
  const [editBody, setEditBody] = React.useState("");
  const [editingDraftId, setEditingDraftId] = React.useState<string | null>(null);
  const [opMessage, setOpMessage] = React.useState<string>("");
  const [analytics, setAnalytics] = React.useState<Record<string, unknown> | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const loadQueue = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/worktrigger/queue?status=${encodeURIComponent(status)}&limit=200`);
      if (!res.ok) throw new Error(`Queue fetch failed: ${res.status}`);
      const payload = (await res.json()) as QueueRow[];
      setRows(payload);
      if (!selectedAccountId && payload.length > 0) {
        setSelectedAccountId(payload[0].account_id);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Queue fetch failed");
    } finally {
      setLoading(false);
    }
  }, [status, selectedAccountId]);

  React.useEffect(() => {
    void loadQueue();
  }, [loadQueue]);

  React.useEffect(() => {
    if (!selectedAccountId) return;
    let cancelled = false;
    void (async () => {
      setDetailLoading(true);
      try {
        const res = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(selectedAccountId)}/detail`);
        if (!res.ok) throw new Error(`Detail fetch failed: ${res.status}`);
        const payload = (await res.json()) as AccountDetail;
        if (!cancelled) setDetail(payload);
      } catch {
        if (!cancelled) setDetail(null);
      } finally {
        if (!cancelled) setDetailLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedAccountId]);

  const actReview = async (draftId: string, action: string) => {
    const res = await fetch(`/api/worktrigger/drafts/${encodeURIComponent(draftId)}/review`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, reviewer_user_id: reviewerId }),
    });
    if (!res.ok) {
      const msg = await res.text();
      throw new Error(msg || `Review action failed (${res.status})`);
    }
    await loadQueue();
  };

  const reloadDetail = React.useCallback(async () => {
    if (!selectedAccountId) return;
    const res = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(selectedAccountId)}/detail`);
    if (!res.ok) throw new Error(`Detail fetch failed: ${res.status}`);
    setDetail((await res.json()) as AccountDetail);
  }, [selectedAccountId]);

  const loadAnalytics = React.useCallback(async () => {
    const res = await fetch("/api/worktrigger/analytics/summary");
    if (!res.ok) throw new Error(`Analytics fetch failed: ${res.status}`);
    setAnalytics((await res.json()) as Record<string, unknown>);
  }, []);

  React.useEffect(() => {
    void loadAnalytics().catch(() => setAnalytics(null));
  }, [loadAnalytics]);

  const triggerHypothesis = async () => {
    if (!selectedAccountId) return;
    const res = await fetch(`/api/worktrigger/accounts/${encodeURIComponent(selectedAccountId)}/work-hypothesis`, { method: "POST" });
    if (!res.ok) throw new Error(await res.text());
    setOpMessage("Work hypothesis generated.");
    await reloadDetail();
  };

  const triggerDraftGeneration = async () => {
    if (!selectedAccountId || !detail) return;
    const contact = detail.contacts[0];
    const hypothesis = detail.work_hypotheses[0];
    if (!contact || !hypothesis) throw new Error("Need at least one contact and one hypothesis.");
    const res = await fetch("/api/worktrigger/drafts/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        account_id: selectedAccountId,
        contact_id: String(contact.id),
        work_hypothesis_id: String(hypothesis.id),
        channel: "email",
      }),
    });
    if (!res.ok) throw new Error(await res.text());
    setOpMessage("Draft generated.");
    await Promise.all([loadQueue(), reloadDetail()]);
  };

  const sendDraft = async (draftId: string) => {
    const res = await fetch(`/api/worktrigger/drafts/${encodeURIComponent(draftId)}/send`, { method: "POST" });
    if (!res.ok) throw new Error(await res.text());
    setOpMessage(`Draft sent (${draftId}).`);
    await Promise.all([loadQueue(), reloadDetail()]);
  };

  const syncDraftToCrm = async (draftId: string) => {
    const res = await fetch(`/api/worktrigger/crm/sync/opportunity?draft_id=${encodeURIComponent(draftId)}`, {
      method: "POST",
    });
    if (!res.ok) throw new Error(await res.text());
    setOpMessage(`CRM sync complete (${draftId}).`);
    await loadAnalytics();
  };

  return (
    <div style={{ display: "grid", gridTemplateRows: "auto 1fr", height: "100vh", fontFamily: "system-ui, sans-serif" }}>
      <header style={{ display: "flex", gap: 12, alignItems: "center", padding: 12, borderBottom: "1px solid #ddd" }}>
        <button type="button" onClick={onBack}>← Back</button>
        <strong>WorkTrigger SDR Queue</strong>
        <button type="button" onClick={onOpenAnalytics}>Analytics</button>
        <select value={status} onChange={(e) => setStatus(e.target.value)}>
          <option value="draft_ready">draft_ready</option>
          <option value="approved">approved</option>
          <option value="sent">sent</option>
          <option value="all">all</option>
        </select>
        <input
          value={reviewerId}
          onChange={(e) => setReviewerId(e.target.value)}
          placeholder="reviewer id"
          style={{ minWidth: 180 }}
        />
        <button type="button" onClick={() => void loadQueue()}>Refresh</button>
        {opMessage ? <span style={{ color: "#146c2e", fontSize: 12 }}>{opMessage}</span> : null}
      </header>
      <div style={{ display: "grid", gridTemplateColumns: "42% 58%", minHeight: 0 }}>
        <section style={{ overflow: "auto", borderRight: "1px solid #ddd", padding: 12 }}>
          {error ? <div style={{ color: "#b00020", marginBottom: 8 }}>{error}</div> : null}
          {loading ? <div>Loading queue...</div> : null}
          {!loading && rows.length === 0 ? <div>No drafts for this status.</div> : null}
          {rows.map((row) => (
            <div
              key={row.draft_id}
              style={{
                border: "1px solid #ddd",
                borderRadius: 8,
                padding: 10,
                marginBottom: 10,
                background: selectedAccountId === row.account_id ? "#f6fbff" : "#fff",
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                <strong>{row.account_name || row.domain || row.account_id}</strong>
                <span>{row.status}</span>
              </div>
              <div style={{ fontSize: 12, opacity: 0.8 }}>{row.contact_name} {row.contact_title ? `• ${row.contact_title}` : ""}</div>
              <div style={{ fontSize: 12, marginTop: 6 }}>Signal score: {Number(row.signal_score || 0).toFixed(1)}</div>
              <div style={{ fontSize: 13, marginTop: 6 }}>{row.subject_a || "(No subject)"}</div>
              <div style={{ display: "flex", gap: 6, marginTop: 8, flexWrap: "wrap" }}>
                <button type="button" onClick={() => setSelectedAccountId(row.account_id)}>Detail</button>
                {row.status === "draft_ready" ? (
                  <>
                    <button type="button" onClick={() => void actReview(row.draft_id, "approve")}>Approve</button>
                    <button
                      type="button"
                      onClick={() => {
                        setEditingDraftId(row.draft_id);
                        setEditSubject(row.subject_a || "");
                        setEditBody("");
                      }}
                    >
                      Edit + Approve
                    </button>
                    <button type="button" onClick={() => void actReview(row.draft_id, "discard")}>Discard</button>
                    <button type="button" onClick={() => void actReview(row.draft_id, "snooze")}>Snooze</button>
                  </>
                ) : null}
                {row.status === "approved" ? (
                  <button type="button" onClick={() => void sendDraft(row.draft_id)}>Send</button>
                ) : null}
                {row.status === "sent" ? (
                  <button type="button" onClick={() => void syncDraftToCrm(row.draft_id)}>Sync CRM</button>
                ) : null}
              </div>
            </div>
          ))}
        </section>
        <section style={{ overflow: "auto", padding: 12 }}>
          {!selectedAccountId ? <div>Select an account from queue.</div> : null}
          {selectedAccountId && detailLoading ? <div>Loading detail...</div> : null}
          {selectedAccountId && !detailLoading && !detail ? <div>Failed to load account detail.</div> : null}
          {detail ? (
            <div style={{ display: "grid", gap: 12 }}>
              <h3 style={{ margin: 0 }}>{String(detail.account.name || detail.account.domain || selectedAccountId)}</h3>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <button type="button" onClick={() => void triggerHypothesis()}>Generate Hypothesis</button>
                <button type="button" onClick={() => void triggerDraftGeneration()}>Generate Draft</button>
              </div>
              <div><strong>Signal stack:</strong> {detail.signal_stack ? JSON.stringify(detail.signal_stack) : "none"}</div>
              <div>
                <strong>Geo attribution</strong>
                <pre style={{ whiteSpace: "pre-wrap", background: "#f7f7f7", padding: 8, borderRadius: 6 }}>
                  {JSON.stringify(detail.geo_attribution, null, 2)}
                </pre>
              </div>
              <div>
                <strong>Latest hypothesis</strong>
                <pre style={{ whiteSpace: "pre-wrap", background: "#f7f7f7", padding: 8, borderRadius: 6 }}>
                  {JSON.stringify(detail.work_hypotheses[0] || null, null, 2)}
                </pre>
              </div>
              <div>
                <strong>Contacts</strong>
                <pre style={{ whiteSpace: "pre-wrap", background: "#f7f7f7", padding: 8, borderRadius: 6 }}>
                  {JSON.stringify(detail.contacts, null, 2)}
                </pre>
              </div>
              <div>
                <strong>Analytics summary</strong>
                <pre style={{ whiteSpace: "pre-wrap", background: "#f7f7f7", padding: 8, borderRadius: 6 }}>
                  {JSON.stringify(analytics, null, 2)}
                </pre>
              </div>
            </div>
          ) : null}
        </section>
      </div>
      {editingDraftId ? (
        <div style={{
          position: "fixed", inset: 0, background: "rgba(0,0,0,0.35)",
          display: "grid", placeItems: "center",
        }}>
          <div style={{ background: "#fff", borderRadius: 8, padding: 16, width: "min(860px, 92vw)" }}>
            <h3 style={{ marginTop: 0 }}>Edit + Approve draft</h3>
            <div style={{ display: "grid", gap: 8 }}>
              <input value={editSubject} onChange={(e) => setEditSubject(e.target.value)} placeholder="Edited subject" />
              <textarea value={editBody} onChange={(e) => setEditBody(e.target.value)} rows={8} placeholder="Edited body" />
            </div>
            <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
              <button
                type="button"
                onClick={async () => {
                  try {
                    const res = await fetch(`/api/worktrigger/drafts/${encodeURIComponent(editingDraftId)}/review`, {
                      method: "POST",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({
                        action: "edit_and_approve",
                        reviewer_user_id: reviewerId,
                        edited_subject: editSubject,
                        edited_body: editBody,
                      }),
                    });
                    if (!res.ok) throw new Error(await res.text());
                    setEditingDraftId(null);
                    setOpMessage(`Draft ${editingDraftId} edited + approved.`);
                    await Promise.all([loadQueue(), reloadDetail()]);
                  } catch (e) {
                    setError(e instanceof Error ? e.message : "Edit + approve failed");
                  }
                }}
              >
                Save and Approve
              </button>
              <button type="button" onClick={() => setEditingDraftId(null)}>Cancel</button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
