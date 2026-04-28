import React from "react";

type CompareRow = {
  geographyId: string;
  score: number;
  recommendation: string;
};

type ComparePanelProps = {
  rows: CompareRow[];
};

export function ComparePanel({ rows }: ComparePanelProps): JSX.Element {
  return (
    <div className="compare-panel">
      <h3>Compare</h3>
      {rows.length === 0 ? <div className="compare-empty">No comparison rows loaded.</div> : null}
      {rows.map((row) => (
        <div className="compare-row" key={row.geographyId}>
          <span>{row.geographyId}</span>
          <span>{row.score.toFixed(1)}</span>
          <span>{row.recommendation}</span>
        </div>
      ))}
    </div>
  );
}
