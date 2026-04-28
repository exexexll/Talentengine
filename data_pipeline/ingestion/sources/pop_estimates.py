from data_pipeline.ingestion.base import SourceConnector


class PopulationEstimatesConnector(SourceConnector):
    source_name = "CENSUS_POP_ESTIMATES"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "population", "population_growth_rate"}

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            transformed.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "population",
                        "raw_value": float(row["population"]),
                        "units": "persons",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "population_growth_rate",
                        "raw_value": float(row["population_growth_rate"]),
                        "units": "ratio",
                        "source": self.source_name,
                    },
                ]
            )
        return transformed
