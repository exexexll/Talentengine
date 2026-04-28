import React from "react";

type LayerPickerProps = {
  activeLayers: string[];
  onToggle: (layer: string) => void;
};

const LAYERS = [
  "Opportunity Score",
  "Talent Supply",
  "Local Core Industry",
  "Demand Gap",
  "Migration",
  "Affordability",
  "Broadband",
];

export function LayerPicker({ activeLayers, onToggle }: LayerPickerProps): JSX.Element {
  return (
    <div className="layer-picker">
      {LAYERS.map((layer) => (
        <button
          key={layer}
          className={activeLayers.includes(layer) ? "layer active" : "layer"}
          onClick={() => onToggle(layer)}
          type="button"
        >
          {layer}
        </button>
      ))}
    </div>
  );
}
