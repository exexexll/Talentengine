"""
Export scored GeoJSON files from metric artifacts for Tippecanoe consumption.

Reads the latest artifact bundle and merges score data into GeoJSON boundary
files, producing one file per geometry type (state, county, metro, etc.).

Usage:
  python -m infra.tiles.build_geojson
  python -m infra.tiles.build_geojson --boundaries infra/tiles/boundaries
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_score_map() -> dict[str, dict[str, float]]:
    """Load the latest phase4 scores keyed by geography_id."""
    from backend.app.services.artifact_store import load_latest_artifact_bundle

    bundle = load_latest_artifact_bundle("phase4")
    score_map: dict[str, dict[str, float]] = {}
    for row in bundle["metrics"]:
        geo_id = row["geography_id"]
        score_map[geo_id] = {
            "score": float(row.get("score_value", 0)),
            "confidence": float(row.get("confidence", 0)),
        }
    return score_map


def _load_metric_features() -> dict[str, dict[str, float]]:
    """Load the latest all-phase metrics as normalized features per geography."""
    from backend.app.services.artifact_store import load_latest_artifact_bundle

    bundle = load_latest_artifact_bundle("all")
    feature_map: dict[str, dict[str, float]] = {}
    for row in bundle["metrics"]:
        geo_id = row["geography_id"]
        if geo_id not in feature_map:
            feature_map[geo_id] = {}
        feature_map[geo_id][row["metric_name"]] = float(row["raw_value"])
    return feature_map


def enrich_geojson(
    geojson_path: Path,
    id_property: str,
    score_map: dict[str, dict[str, float]],
    feature_map: dict[str, dict[str, float]],
) -> dict[str, Any]:
    with geojson_path.open("r", encoding="utf-8") as f:
        geojson = json.load(f)

    for feature in geojson.get("features", []):
        props = feature.get("properties") or {}
        geo_id = str(props.get(id_property, feature.get("id", "")))

        scores = score_map.get(geo_id, {})
        metrics = feature_map.get(geo_id, {})

        props["figwork_score"] = scores.get("score", 0)
        props["figwork_confidence"] = scores.get("confidence", 0)
        for metric_name, value in metrics.items():
            props[f"m_{metric_name}"] = value

        feature["properties"] = props

    return geojson


GEOMETRY_CONFIG = [
    {
        "layer_id": "opportunity_state",
        "filename": "state.geojson",
        "id_property": "STATE",
    },
    {
        "layer_id": "county_supply_demand",
        "filename": "county.geojson",
        "id_property": "GEOID",
    },
    {
        "layer_id": "metro_industry_bubbles",
        "filename": "metro.geojson",
        "id_property": "CBSAFP",
    },
    {
        "layer_id": "zip_overlay",
        "filename": "zcta.geojson",
        "id_property": "ZCTA5CE20",
    },
    {
        "layer_id": "tract_overlay",
        "filename": "tract.geojson",
        "id_property": "GEOID",
    },
]


def build_scored_geojson(
    boundaries_dir: Path,
    output_dir: Path,
) -> list[Path]:
    score_map = _load_score_map()
    feature_map = _load_metric_features()
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []

    for config in GEOMETRY_CONFIG:
        boundary_path = boundaries_dir / config["filename"]
        if not boundary_path.exists():
            print(f"SKIP {config['layer_id']}: boundary file {boundary_path} not found")
            print(f"  -> Download from Census TIGER/Line or natural earth and place at {boundary_path}")
            continue

        enriched = enrich_geojson(
            geojson_path=boundary_path,
            id_property=config["id_property"],
            score_map=score_map,
            feature_map=feature_map,
        )
        out_path = output_dir / f"{config['layer_id']}.geojson"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(enriched, f)
        outputs.append(out_path)
        n_features = len(enriched.get("features", []))
        print(f"  {config['layer_id']}: {n_features} features -> {out_path}")

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Build scored GeoJSON for tile generation")
    parser.add_argument(
        "--boundaries",
        default="infra/tiles/boundaries",
        help="Directory containing raw boundary GeoJSON files",
    )
    parser.add_argument(
        "--output",
        default="infra/tiles/staging",
        help="Directory for scored GeoJSON output",
    )
    args = parser.parse_args()

    outputs = build_scored_geojson(
        boundaries_dir=Path(args.boundaries),
        output_dir=Path(args.output),
    )
    print(f"\nBuilt {len(outputs)} scored GeoJSON files")


if __name__ == "__main__":
    main()
