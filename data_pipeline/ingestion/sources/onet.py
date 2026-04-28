from data_pipeline.ingestion.base import SourceConnector


class ONETConnector(SourceConnector):
    source_name = "ONET"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "adjacent_skill_pool_index", "remote_compatibility_index"}

    def transform(self, records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in records:
            out.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "adjacent_skill_pool_index",
                        "raw_value": float(row["adjacent_skill_pool_index"]),
                        "units": "index_0_1",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "remote_compatibility_index",
                        "raw_value": float(row["remote_compatibility_index"]),
                        "units": "index_0_1",
                        "source": self.source_name,
                    },
                ]
            )
        return out
