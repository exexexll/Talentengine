from data_pipeline.ingestion.sources import PHASE2_CORE_CONNECTORS


def run() -> None:
    connectors = [connector_cls() for connector_cls in PHASE2_CORE_CONNECTORS]
    for connector in connectors:
        snapshot, records = connector.run()
        print(f"{snapshot.source_name} snapshot={snapshot.snapshot_id} rows={len(records)}")


if __name__ == "__main__":
    run()
