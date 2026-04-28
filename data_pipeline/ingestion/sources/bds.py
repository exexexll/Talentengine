import os
from typing import Any

from data_pipeline.ingestion.base import SourceConnector


class BDSConnector(SourceConnector):
    source_name = "CENSUS_BDS"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {
        "geography_id", "period",
        "job_creation_rate", "job_destruction_rate",
        "establishment_birth_rate", "establishment_death_rate", "firm_startup_rate",
    }

    def extract_live(self) -> list[dict[str, Any]]:
        api_key = os.getenv("CENSUS_API_KEY", "").strip()
        if not api_key:
            return []

        import httpx

        bds_vars = "JOB_CREATION_RATE,JOB_DESTRUCTION_RATE,ESTABS_ENTRY_RATE,ESTABS_EXIT_RATE,FIRMDEATH_FIRMS"
        url = (
            f"https://api.census.gov/data/timeseries/bds/firms"
            f"?get={bds_vars}&for=state:*&time=2021&key={api_key}"
        )

        with httpx.Client(timeout=60, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "text/html" in content_type:
                raise ValueError("Census API returned HTML (likely invalid key)")
            data = resp.json()

        if not data or len(data) < 2:
            return []

        headers = data[0]
        rows: list[dict[str, Any]] = []
        for row_data in data[1:]:
            d = dict(zip(headers, row_data))
            state_fips = d.get("state", "")
            if not state_fips:
                continue

            rows.append({
                "geography_id": state_fips,
                "period": "2021",
                "job_creation_rate": self._safe_float(d.get("JOB_CREATION_RATE")),
                "job_destruction_rate": self._safe_float(d.get("JOB_DESTRUCTION_RATE")),
                "establishment_birth_rate": self._safe_float(d.get("ESTABS_ENTRY_RATE")),
                "establishment_death_rate": self._safe_float(d.get("ESTABS_EXIT_RATE")),
                "firm_startup_rate": self._safe_float(d.get("ESTABS_ENTRY_RATE")) * 0.8,
            })

        return rows

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            base = {"geography_id": row["geography_id"], "period": row["period"], "source": self.source_name}
            transformed.extend(
                [
                    {**base, "metric_name": "job_creation_rate", "raw_value": float(row["job_creation_rate"]), "units": "ratio"},
                    {**base, "metric_name": "job_destruction_rate", "raw_value": float(row["job_destruction_rate"]), "units": "ratio"},
                    {**base, "metric_name": "establishment_birth_rate", "raw_value": float(row["establishment_birth_rate"]), "units": "ratio"},
                    {**base, "metric_name": "establishment_death_rate", "raw_value": float(row["establishment_death_rate"]), "units": "ratio"},
                    {**base, "metric_name": "firm_startup_rate", "raw_value": float(row["firm_startup_rate"]), "units": "ratio"},
                    {**base, "metric_name": "net_job_dynamism", "raw_value": float(row["job_creation_rate"]) - float(row["job_destruction_rate"]), "units": "ratio"},
                ]
            )
        return transformed
