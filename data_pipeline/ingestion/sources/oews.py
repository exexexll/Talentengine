from data_pipeline.ingestion.base import SourceConnector


class OEWSConnector(SourceConnector):
    source_name = "BLS_OEWS"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "employment", "median_wage"}

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            transformed.append(
                {
                    "geography_id": row["geography_id"],
                    "period": row["period"],
                    "metric_name": "target_occupation_employment",
                    "raw_value": float(row["employment"]),
                    "units": "workers",
                    "source": self.source_name,
                }
            )
            transformed.append(
                {
                    "geography_id": row["geography_id"],
                    "period": row["period"],
                    "metric_name": "occupation_median_wage",
                    "raw_value": float(row["median_wage"]),
                    "units": "usd_per_hour",
                    "source": self.source_name,
                }
            )
        return transformed
