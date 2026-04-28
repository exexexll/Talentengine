import React from "react";

type ScenarioControlsProps = {
  scenarioId: string;
  options: Array<{ id: string; name: string }>;
  onScenarioChange: (scenarioId: string) => void;
};

export function ScenarioControls({
  scenarioId,
  options,
  onScenarioChange,
}: ScenarioControlsProps): JSX.Element {
  return (
    <div className="scenario-controls">
      <label htmlFor="scenario">Scenario</label>
      <select
        id="scenario"
        value={scenarioId}
        onChange={(event) => onScenarioChange(event.target.value)}
      >
        {options.map((option) => (
          <option key={option.id} value={option.id}>
            {option.name}
          </option>
        ))}
      </select>
    </div>
  );
}
