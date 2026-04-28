from data_pipeline.ingestion.base import SourceConnector


class FCCBroadbandConnector(SourceConnector):
    source_name = "FCC_BROADBAND"
    cadence = "daily"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "served_household_ratio", "high_speed_ratio"}

    def transform(self, records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in records:
            out.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "served_household_ratio",
                        "raw_value": float(row["served_household_ratio"]),
                        "units": "ratio",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "high_speed_ratio",
                        "raw_value": float(row["high_speed_ratio"]),
                        "units": "ratio",
                        "source": self.source_name,
                    },
                ]
            )
        return out
