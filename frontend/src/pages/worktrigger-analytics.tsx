import React from "react";

type WorkTriggerAnalyticsPageProps = {
  onBack: () => void;
};

type HeartbeatRow = {
  worker_id: string;
  status: string;
  last_seen_at: string;
  last_result: Record<string, unknown>;
};

function StatCard(props: { label: string; value: string | number }): JSX.Element {
  return (
    <div style={{ border: "1px solid #ddd", borderRadius: 8, padding: 12, background: "#fff" }}>
      <div style={{ fontSize: 12, opacity: 0.7 }}>{props.label}</div>
      <div style={{ fontSize: 22, fontWeight: 600 }}>{props.value}</div>
    </div>
  );
}

export function WorkTriggerAnalyticsPage({ onBack }: WorkTriggerAnalyticsPageProps): JSX.Element {
  const [summary, setSummary] = React.useState<Record<string, unknown> | null>(null);
  const [reconcile, setReconcile] = React.useState<Record<string, unknown> | null>(null);
  const [heartbeats, setHeartbeats] = React.useState<HeartbeatRow[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  const loadAll = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [s, r, h] = await Promise.all([
        fetch("/api/worktrigger/analytics/summary"),
        fetch("/api/worktrigger/crm/reconcile?limit=50"),
        fetch("/api/worktrigger/worker/heartbeats"),
      ]);
      if (!s.ok) throw new Error(`Summary failed: ${s.status}`);
      if (!r.ok) throw new Error(`Reconcile failed: ${r.status}`);
      if (!h.ok) throw new Error(`Heartbeats failed: ${h.status}`);
      setSummary((await s.json()) as Record<string, unknown>);
      setReconcile((await r.json()) as Record<string, unknown>);
      setHeartbeats((await h.json()) as HeartbeatRow[]);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Analytics loading failed");
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    void loadAll();
  }, [loadAll]);

  const throughput = (summary?.throughput_7d as Record<string, unknown> | undefined) || {};
  const quality = (summary?.quality as Record<string, unknown> | undefined) || {};
  const speed = (summary?.speed_hours as Record<string, unknown> | undefined) || {};
  const drift = (summary?.crm_drift as Record<string, unknown> | undefined) || {};

  return (
    <div style={{ display: "grid", gridTemplateRows: "auto 1fr", height: "100vh", fontFamily: "system-ui, sans-serif" }}>
      <header style={{ display: "flex", gap: 12, alignItems: "center", padding: 12, borderBottom: "1px solid #ddd" }}>
        <button type="button" onClick={onBack}>← Back</button>
        <strong>WorkTrigger Analytics</strong>
        <button type="button" onClick={() => void loadAll()}>Refresh</button>
      </header>
      <main style={{ overflow: "auto", padding: 12, display: "grid", gap: 12 }}>
        {error ? <div style={{ color: "#b00020" }}>{error}</div> : null}
        {loading ? <div>Loading analytics...</div> : null}
        {!loading && summary ? (
          <>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(140px, 1fr))", gap: 10 }}>
              <StatCard label="Signals total" value={Number(summary.signals_total || 0)} />
              <StatCard label="Avg signal score" value={Number(summary.avg_signal_score || 0).toFixed(2)} />
              <StatCard label="Drafts created (7d)" value={Number(throughput.drafts_created || 0)} />
              <StatCard label="Drafts sent (7d)" value={Number(throughput.drafts_sent || 0)} />
              <StatCard label="Approval rate" value={`${(Number(quality.approval_rate || 0) * 100).toFixed(1)}%`} />
              <StatCard label="Median hrs to approve" value={Number(speed.median_create_to_approve_or_better || 0).toFixed(2)} />
              <StatCard label="Median hrs to send" value={Number(speed.median_create_to_sent_or_replied || 0).toFixed(2)} />
              <StatCard label="Unsynced opps" value={Number(drift.unsynced_opportunities || 0)} />
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
              <section style={{ border: "1px solid #ddd", borderRadius: 8, padding: 10, background: "#fff" }}>
                <h3 style={{ marginTop: 0 }}>Summary Payload</h3>
                <pre style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(summary, null, 2)}</pre>
              </section>
              <section style={{ border: "1px solid #ddd", borderRadius: 8, padding: 10, background: "#fff" }}>
                <h3 style={{ marginTop: 0 }}>CRM Reconciliation</h3>
                <pre style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(reconcile, null, 2)}</pre>
              </section>
            </div>
            <section style={{ border: "1px solid #ddd", borderRadius: 8, padding: 10, background: "#fff" }}>
              <h3 style={{ marginTop: 0 }}>Worker Heartbeats</h3>
              {heartbeats.length === 0 ? <div>No worker heartbeats yet.</div> : null}
              {heartbeats.map((hb) => (
                <div key={hb.worker_id} style={{ borderTop: "1px solid #eee", paddingTop: 8, marginTop: 8 }}>
                  <div>
                    <strong>{hb.worker_id}</strong> - {hb.status} - {hb.last_seen_at}
                  </div>
                  <pre style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(hb.last_result, null, 2)}</pre>
                </div>
              ))}
            </section>
          </>
        ) : null}
      </main>
    </div>
  );
}
