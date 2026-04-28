import React from "react";

import "./rankings.css";

type RankedRow = {
  rank: number;
  geography_id: string;
  score_value: number;
  confidence: number;
};

type ScenarioOption = { id: string; name: string };

const RANKED_LIMIT = 10000;

type RankingsPageProps = {
  onBack: () => void;
};

export function RankingsPage({ onBack }: RankingsPageProps): JSX.Element {
  const [scenarioId, setScenarioId] = React.useState("default-opportunity");
  const [scenarioOptions, setScenarioOptions] = React.useState<ScenarioOption[]>([
    { id: "default-opportunity", name: "Default Opportunity" },
  ]);
  const [rows, setRows] = React.useState<RankedRow[]>([]);
  const [geoNames, setGeoNames] = React.useState<Record<string, string>>({});
  const [filter, setFilter] = React.useState("");
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    void (async () => {
      try {
        const res = await fetch("/api/scenarios");
        if (res.ok) {
          const payload = (await res.json()) as Array<{ scenario_id: string; name: string }>;
          if (payload.length > 0) {
            setScenarioOptions(payload.map((r) => ({ id: r.scenario_id, name: r.name })));
          }
        }
      } catch {
        /* keep default */
      }
    })();
  }, []);

  React.useEffect(() => {
    void (async () => {
      setLoading(true);
      setError(null);
      try {
        const [rankedRes, namesRes] = await Promise.all([
          fetch(
            `/api/scores/_ranked?scenario_id=${encodeURIComponent(scenarioId)}&limit=${RANKED_LIMIT}`,
          ),
          fetch("/api/geographies/names"),
        ]);
        if (!rankedRes.ok) {
          throw new Error(`Ranked scores failed: ${rankedRes.status}`);
        }
        const rankedPayload = (await rankedRes.json()) as RankedRow[];
        setRows(rankedPayload);
        if (namesRes.ok) {
          setGeoNames(await namesRes.json());
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to load rankings");
        setRows([]);
      } finally {
        setLoading(false);
      }
    })();
  }, [scenarioId]);

  const q = filter.trim().toLowerCase();
  const visible = React.useMemo(() => {
    if (!q) return rows;
    return rows.filter((r) => {
      const name = (geoNames[r.geography_id] ?? "").toLowerCase();
      return r.geography_id.toLowerCase().includes(q) || name.includes(q);
    });
  }, [rows, geoNames, q]);

  return (
    <div className="rankings-page">
      <header className="rankings-header">
        <div className="rankings-header-left">
          <button type="button" className="rankings-back" onClick={onBack}>
            ← Map
          </button>
          <h1 className="rankings-title">Opportunity score rankings</h1>
        </div>
        <div className="rankings-header-right">
          <label className="rankings-scenario-label">
            Scenario
            <select
              className="rankings-select"
              value={scenarioId}
              onChange={(e) => setScenarioId(e.target.value)}
            >
              {scenarioOptions.map((opt) => (
                <option key={opt.id} value={opt.id}>
                  {opt.name}
                </option>
              ))}
            </select>
          </label>
        </div>
      </header>

      <div className="rankings-toolbar">
        <input
          type="search"
          className="rankings-filter"
          placeholder="Filter by name or geography ID…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
        <span className="rankings-count">
          {loading ? "Loading…" : `${visible.length.toLocaleString()} geographies shown`}
        </span>
      </div>

      {error ? <div className="rankings-error">{error}</div> : null}

      <div className="rankings-table-wrap">
        <table className="rankings-table">
          <thead>
            <tr>
              <th className="col-rank">Rank</th>
              <th className="col-name">Geography</th>
              <th className="col-id">ID</th>
              <th className="col-score">Opportunity score</th>
              <th className="col-conf">Confidence</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={5} className="rankings-loading">
                  Loading rankings…
                </td>
              </tr>
            ) : visible.length === 0 ? (
              <tr>
                <td colSpan={5} className="rankings-empty">
                  No rows match your filter.
                </td>
              </tr>
            ) : (
              visible.map((r) => (
                <tr key={r.geography_id}>
                  <td className="col-rank mono">{r.rank}</td>
                  <td className="col-name">{geoNames[r.geography_id] ?? "—"}</td>
                  <td className="col-id mono">{r.geography_id}</td>
                  <td className="col-score mono">{r.score_value.toFixed(2)}</td>
                  <td className="col-conf mono">{r.confidence.toFixed(2)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
