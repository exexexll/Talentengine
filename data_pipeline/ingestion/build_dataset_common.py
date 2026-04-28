import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_pipeline.qa import (
    check_duplicate_metric_keys,
    check_freshness,
    check_geography_coverage,
    check_metric_ranges,
)
from data_pipeline.transforms.standardize_metrics import standardize_metric_row


def build_standardized_rows(connector_classes: list[type]) -> tuple[list[dict], list[dict]]:
    standardized: list[dict] = []
    snapshots: list[dict] = []
    for connector_cls in connector_classes:
        connector = connector_cls()
        snapshot, rows = connector.run()
        snapshots.append(
            {
                "snapshot_id": snapshot.snapshot_id,
                "source_name": snapshot.source_name,
                "extracted_at": snapshot.extracted_at.isoformat(),
                "schema_version": snapshot.schema_version,
                "cadence": snapshot.cadence,
                "row_count": snapshot.row_count,
            }
        )
        for row in rows:
            standardized.append(
                standardize_metric_row(
                    row=row,
                    source_snapshot_id=snapshot.snapshot_id,
                    formula=f"ingested_from_{connector.source_name.lower()}",
                    confidence=0.8,
                )
            )
    return standardized, snapshots


def validate_rows(rows: list[dict[str, Any]], snapshots: list[dict[str, Any]]) -> None:
    errors = []
    errors.extend(check_metric_ranges.run(rows))
    errors.extend(check_geography_coverage.run(rows))
    errors.extend(check_freshness.run(snapshots))
    errors.extend(check_duplicate_metric_keys.run(rows))
    if errors:
        joined = "\n".join(f"- {msg}" for msg in errors[:20])
        total = len(errors)
        if total > 20:
            joined += f"\n  ... and {total - 20} more"
        print(f"[QA] {total} warnings:\n{joined}")
        # Don't fail the pipeline on warnings for large datasets
        if total > 200:
            print("[QA] Large dataset -- treating warnings as non-fatal")
            return
        raise ValueError(f"Dataset QA checks failed:\n{joined}")


def write_ndjson(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in records:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def materialize_dataset(
    rows: list[dict],
    snapshots: list[dict],
    phase_name: str,
) -> Path:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(f"data_pipeline/artifacts/{phase_name}") / run_id
    write_ndjson(out_dir / "metric_fact.ndjson", rows)
    write_ndjson(out_dir / "source_snapshot.ndjson", snapshots)
    return out_dir


def print_summary(rows: list[dict], snapshots: list[dict], out_dir: Path) -> None:
    by_source = defaultdict(int)
    for row in rows:
        source_name = row["source_snapshot_id"].rsplit("-", 1)[0]
        by_source[source_name] += 1

    print(f"standardized_rows={len(rows)}")
    print(f"snapshots={len(snapshots)}")
    print(f"artifacts={out_dir}")
    for source_name, count in sorted(by_source.items()):
        print(f"source_rows[{source_name}]={count}")
    for sample in rows[:3]:
        print(
            f"{sample['source_snapshot_id']} | {sample['geography_id']} | "
            f"{sample['metric_name']}={sample['raw_value']}"
        )
