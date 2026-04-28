from data_pipeline.ingestion.base import SourceConnector


class RUCARUCCConnector(SourceConnector):
    source_name = "USDA_RUCA_RUCC"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "rurality_index", "metro_linkage_index"}

    def transform(self, records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in records:
            out.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "rurality_index",
                        "raw_value": float(row["rurality_index"]),
                        "units": "index_0_1",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "metro_linkage_index",
                        "raw_value": float(row["metro_linkage_index"]),
                        "units": "index_0_1",
                        "source": self.source_name,
                    },
                ]
            )
        return out
