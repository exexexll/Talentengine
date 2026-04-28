from data_pipeline.ingestion.base import SourceConnector


class IRSMigrationConnector(SourceConnector):
    source_name = "IRS_MIGRATION"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "net_migrants", "inbound_returns", "outbound_returns"}

    def transform(self, records: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in records:
            out.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "net_migrants",
                        "raw_value": float(row["net_migrants"]),
                        "units": "persons",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "inbound_returns",
                        "raw_value": float(row["inbound_returns"]),
                        "units": "tax_returns",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "outbound_returns",
                        "raw_value": float(row["outbound_returns"]),
                        "units": "tax_returns",
                        "source": self.source_name,
                    },
                ]
            )
        return out
