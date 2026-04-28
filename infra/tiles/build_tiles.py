"""
Vector tile build pipeline using Tippecanoe + PMTiles.

Reads scored GeoJSON from the tile staging directory and produces
PMTiles archives for each zoom-adaptive layer defined in layer_manifest.yaml.

Prerequisites:
  - tippecanoe (https://github.com/felt/tippecanoe) on PATH
  - GeoJSON boundary files staged in infra/tiles/staging/

Usage:
  python -m infra.tiles.build_tiles
  python -m infra.tiles.build_tiles --manifest infra/tiles/layer_manifest.yaml

Typical workflow:
  1. Run `python -m infra.tiles.build_geojson` to export scored GeoJSON
  2. Run `python -m infra.tiles.build_tiles` to convert to PMTiles
  3. Serve via the FastAPI /tiles endpoint or a static file server
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def _load_manifest(manifest_path: Path) -> list[dict]:
    try:
        import yaml
    except ModuleNotFoundError:
        with manifest_path.open("r") as f:
            content = f.read()
        import re
        layers = []
        for block in re.split(r"\n  - ", content):
            if "id:" not in block:
                continue
            entry: dict[str, str | int] = {}
            for line in block.strip().splitlines():
                line = line.strip().lstrip("- ")
                if ":" in line:
                    key, _, value = line.partition(":")
                    value = value.strip()
                    try:
                        entry[key.strip()] = int(value)
                    except ValueError:
                        entry[key.strip()] = value
            if "id" in entry:
                layers.append(entry)
        return layers

    with manifest_path.open("r") as f:
        doc = yaml.safe_load(f)
    return doc.get("layers", [])


def build_pmtiles(
    manifest_path: Path,
    staging_dir: Path,
    output_dir: Path,
) -> list[Path]:
    if not shutil.which("tippecanoe"):
        print("ERROR: tippecanoe not found on PATH. Install from https://github.com/felt/tippecanoe")
        sys.exit(1)

    layers = _load_manifest(manifest_path)
    if not layers:
        print("WARNING: No layers found in manifest")
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []

    for layer in layers:
        layer_id = layer["id"]
        geometry = layer.get("geometry", "unknown")
        min_zoom = layer.get("min_zoom", 0)
        max_zoom = layer.get("max_zoom", 14)

        geojson_path = staging_dir / f"{layer_id}.geojson"
        if not geojson_path.exists():
            geojson_path = staging_dir / f"{geometry}.geojson"
        if not geojson_path.exists():
            print(f"SKIP {layer_id}: no GeoJSON at {geojson_path}")
            continue

        output_path = output_dir / f"{layer_id}.pmtiles"
        cmd = [
            "tippecanoe",
            "-o", str(output_path),
            f"--minimum-zoom={min_zoom}",
            f"--maximum-zoom={max_zoom}",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            f"--layer={layer_id}",
            "--force",
            str(geojson_path),
        ]
        print(f"Building {layer_id}: zoom {min_zoom}-{max_zoom}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"ERROR building {layer_id}: {result.stderr}")
            continue
        outputs.append(output_path)
        print(f"  -> {output_path} ({output_path.stat().st_size:,} bytes)")

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Build PMTiles from scored GeoJSON")
    parser.add_argument(
        "--manifest",
        default="infra/tiles/layer_manifest.yaml",
        help="Path to layer manifest YAML",
    )
    parser.add_argument(
        "--staging",
        default="infra/tiles/staging",
        help="Directory containing GeoJSON input files",
    )
    parser.add_argument(
        "--output",
        default="infra/tiles/output",
        help="Directory for PMTiles output",
    )
    args = parser.parse_args()

    outputs = build_pmtiles(
        manifest_path=Path(args.manifest),
        staging_dir=Path(args.staging),
        output_dir=Path(args.output),
    )
    print(f"\nBuilt {len(outputs)} PMTiles archives")


if __name__ == "__main__":
    main()
