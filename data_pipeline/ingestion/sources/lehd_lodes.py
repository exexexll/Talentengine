from data_pipeline.ingestion.base import SourceConnector


class LEHDLODESConnector(SourceConnector):
    source_name = "LEHD_LODES"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "workplace_jobs", "residence_workers", "inflow_ratio"}

    def transform(self, records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in records:
            out.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "workplace_jobs",
                        "raw_value": float(row["workplace_jobs"]),
                        "units": "workers",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "residence_workers",
                        "raw_value": float(row["residence_workers"]),
                        "units": "workers",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "commute_inflow_ratio",
                        "raw_value": float(row["inflow_ratio"]),
                        "units": "ratio",
                        "source": self.source_name,
                    },
                ]
            )
        return out
