from data_pipeline.ingestion.base import SourceConnector


class QCEWConnector(SourceConnector):
    source_name = "BLS_QCEW"
    cadence = "quarterly"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "employment", "annual_wages", "establishments"}

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            transformed.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "industry_employment",
                        "raw_value": float(row["employment"]),
                        "units": "workers",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "industry_annual_wages",
                        "raw_value": float(row["annual_wages"]),
                        "units": "usd",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "industry_establishments",
                        "raw_value": float(row["establishments"]),
                        "units": "count",
                        "source": self.source_name,
                    },
                ]
            )
        return transformed
