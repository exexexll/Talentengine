import json
import os
from pathlib import Path

from data_pipeline.ingestion.sources import ALL_CONNECTORS


def _catalog_path() -> Path:
    configured = os.getenv(
        "FIGWORK_SOURCE_RECORDS_FILE",
        "data_pipeline/source_snapshots/local_source_records.json",
    )
    return Path(configured)


def run() -> list[str]:
    errors: list[str] = []
    path = _catalog_path()
    if not path.exists():
        return [f"source catalog not found: {path}"]

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"invalid JSON in source catalog {path}: {exc}"]

    expected_sources = {connector_cls.source_name for connector_cls in ALL_CONNECTORS}
    catalog_sources = set(payload.keys())

    missing = sorted(expected_sources.difference(catalog_sources))
    unexpected = sorted(catalog_sources.difference(expected_sources))

    if missing:
        errors.append(f"missing source records for: {missing}")
    if unexpected:
        errors.append(f"unexpected source entries not used by connectors: {unexpected}")

    for source_name in sorted(expected_sources.intersection(catalog_sources)):
        rows = payload[source_name]
        if not isinstance(rows, list):
            errors.append(f"source {source_name} must map to a list of records")
            continue
        if len(rows) == 0:
            errors.append(f"source {source_name} has zero records")

    return errors
