import os
from typing import Any

from data_pipeline.ingestion.base import SourceConnector


class BEAGDPConnector(SourceConnector):
    source_name = "BEA_GDP"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "gdp_current_dollars", "gdp_growth_rate"}

    def extract_live(self) -> list[dict[str, Any]]:
        api_key = os.getenv("BEA_API_KEY", "").strip()
        if not api_key:
            return []

        import httpx

        url = (
            "https://apps.bea.gov/api/data/"
            "?method=GETDATA&datasetname=Regional&TableName=SAGDP1&LineCode=3"
            f"&GeoFIPS=STATE&Year=LAST5&ResultFormat=json&UserID={api_key}"
        )

        with httpx.Client(timeout=60, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            payload = resp.json()

        data_rows = (
            payload.get("BEAAPI", {}).get("Results", {}).get("Data", [])
        )
        if not data_rows:
            return []

        by_geo: dict[str, dict[str, float]] = {}
        for d in data_rows:
            geo_fips = d.get("GeoFips", "")
            if len(geo_fips) != 5 or not geo_fips.endswith("000"):
                continue
            state_fips = geo_fips[:2]
            if state_fips == "00":
                continue

            year = d.get("TimePeriod", "")
            raw_val = str(d.get("DataValue", "")).replace(",", "").strip()
            gdp_millions = self._safe_float(raw_val)
            if gdp_millions <= 0:
                continue

            by_geo.setdefault(state_fips, {})[year] = gdp_millions * 1_000_000

        rows: list[dict[str, Any]] = []
        for state_fips, year_data in by_geo.items():
            sorted_years = sorted(year_data.keys(), reverse=True)
            if not sorted_years:
                continue
            latest_year = sorted_years[0]
            latest_gdp = year_data[latest_year]

            growth_rate = 0.02
            if len(sorted_years) >= 2:
                prev_gdp = year_data[sorted_years[1]]
                if prev_gdp > 0:
                    growth_rate = (latest_gdp - prev_gdp) / prev_gdp

            rows.append({
                "geography_id": state_fips,
                "period": latest_year,
                "gdp_current_dollars": latest_gdp,
                "gdp_growth_rate": round(growth_rate, 4),
            })

        return rows

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            transformed.extend(
                [
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "gdp_current_dollars",
                        "raw_value": float(row["gdp_current_dollars"]),
                        "units": "usd",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": row["geography_id"],
                        "period": row["period"],
                        "metric_name": "gdp_growth_rate",
                        "raw_value": float(row["gdp_growth_rate"]),
                        "units": "ratio",
                        "source": self.source_name,
                    },
                ]
            )
        return transformed
