from data_pipeline.ingestion.base import SourceConnector


class IPEDSConnector(SourceConnector):
    source_name = "NCES_IPEDS"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "relevant_completions", "institutions_reporting"}

    def transform(self, records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in records:
            out.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "relevant_completions",
                        "raw_value": float(row["relevant_completions"]),
                        "units": "graduates",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "institutions_reporting",
                        "raw_value": float(row["institutions_reporting"]),
                        "units": "count",
                        "source": self.source_name,
                    },
                ]
            )
        return out
