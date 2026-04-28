from data_pipeline.ingestion.build_dataset_common import (
    build_standardized_rows,
    materialize_dataset,
    print_summary,
    validate_rows,
)
from data_pipeline.ingestion.sources import ALL_CONNECTORS


def run() -> None:
    connector_classes = ALL_CONNECTORS
    rows, snapshots = build_standardized_rows(connector_classes)
    validate_rows(rows, snapshots)
    out_dir = materialize_dataset(rows, snapshots, phase_name="all")
    print_summary(rows, snapshots, out_dir)


if __name__ == "__main__":
    run()
