from data_pipeline.ingestion.base import SourceConnector


class CollegeScorecardConnector(SourceConnector):
    source_name = "COLLEGE_SCORECARD"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "median_earnings_4yr", "completion_rate"}

    def transform(self, records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in records:
            out.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "median_earnings_4yr",
                        "raw_value": float(row["median_earnings_4yr"]),
                        "units": "usd",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "completion_rate",
                        "raw_value": float(row["completion_rate"]),
                        "units": "ratio",
                        "source": self.source_name,
                    },
                ]
            )
        return out
