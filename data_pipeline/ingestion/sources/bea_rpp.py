import os
from typing import Any

from data_pipeline.ingestion.base import SourceConnector


class BEARPPConnector(SourceConnector):
    source_name = "BEA_RPP"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "regional_price_parity"}

    def extract_live(self) -> list[dict[str, Any]]:
        api_key = os.getenv("BEA_API_KEY", "").strip()
        if not api_key:
            return []

        import httpx

        url = (
            "https://apps.bea.gov/api/data/"
            "?method=GETDATA&datasetname=Regional&TableName=SARPP&LineCode=1"
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

        by_geo: dict[str, dict[str, Any]] = {}
        for d in data_rows:
            geo_fips = d.get("GeoFips", "")
            if len(geo_fips) != 5 or not geo_fips.endswith("000"):
                continue
            state_fips = geo_fips[:2]
            if state_fips == "00":
                continue

            year = d.get("TimePeriod", "")
            raw_val = str(d.get("DataValue", "")).replace(",", "").strip()
            rpp = self._safe_float(raw_val)
            if rpp <= 0:
                continue

            if state_fips not in by_geo or year > by_geo[state_fips]["period"]:
                by_geo[state_fips] = {
                    "geography_id": state_fips,
                    "period": year,
                    "regional_price_parity": rpp,
                }

        return list(by_geo.values())

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            transformed.append(
                {
                    "geography_id": row["geography_id"],
                    "period": row["period"],
                    "metric_name": "regional_price_parity",
                    "raw_value": float(row["regional_price_parity"]),
                    "units": "index_us_100",
                    "source": self.source_name,
                }
            )
        return transformed
