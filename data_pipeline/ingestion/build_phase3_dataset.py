from data_pipeline.ingestion.build_dataset_common import (
    build_standardized_rows,
    materialize_dataset,
    print_summary,
    validate_rows,
)
from data_pipeline.ingestion.sources import PHASE3_EXPANSION_CONNECTORS


def run() -> None:
    rows, snapshots = build_standardized_rows(PHASE3_EXPANSION_CONNECTORS)
    validate_rows(rows, snapshots)
    out_dir = materialize_dataset(rows, snapshots, phase_name="phase3")
    print_summary(rows, snapshots, out_dir)


if __name__ == "__main__":
    run()
