import os
from typing import Any

from data_pipeline.ingestion.base import SourceConnector


class LAUSConnector(SourceConnector):
    source_name = "BLS_LAUS"
    cadence = "monthly"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "labor_force", "unemployed"}

    def extract_live(self) -> list[dict[str, Any]]:
        """Fetch state-level LAUS data from BLS API v2."""
        api_key = os.getenv("BLS_API_KEY", "").strip()
        if not api_key:
            return []

        import httpx

        state_fips = [str(i).zfill(2) for i in range(1, 57) if i not in (3, 7, 14, 43, 52)]

        series_labor = [f"LASST{f}0000000000006" for f in state_fips]
        series_unemp = [f"LASST{f}0000000000004" for f in state_fips]
        all_series = series_labor + series_unemp

        rows_by_state: dict[str, dict[str, float]] = {}

        with httpx.Client(timeout=60, follow_redirects=True) as client:
            for chunk_start in range(0, len(all_series), 50):
                chunk = all_series[chunk_start:chunk_start + 50]
                payload = {
                    "seriesid": chunk,
                    "startyear": "2024",
                    "endyear": "2025",
                    "registrationkey": api_key,
                }
                resp = client.post(
                    "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                    json=payload,
                )
                resp.raise_for_status()
                result = resp.json()

                for series in result.get("Results", {}).get("series", []):
                    sid = series.get("seriesID", "")
                    data_points = series.get("data", [])
                    if not data_points:
                        continue

                    latest = data_points[0]
                    value = self._safe_float(latest.get("value"))
                    period = f"{latest.get('year', '2024')}"

                    if sid.startswith("LASST") and "06" in sid[5:7]:
                        state_fips_code = sid[5:7]
                    else:
                        state_fips_code = sid[5:7]

                    entry = rows_by_state.setdefault(state_fips_code, {"period": period})
                    if "0000000006" in sid:
                        entry["labor_force"] = value
                    elif "0000000004" in sid:
                        entry["unemployed"] = value

        rows: list[dict[str, Any]] = []
        for fips, data in rows_by_state.items():
            if "labor_force" in data and "unemployed" in data:
                rows.append({
                    "geography_id": fips,
                    "period": data["period"],
                    "labor_force": data["labor_force"],
                    "unemployed": data["unemployed"],
                })

        print(f"[LAUS] {len(rows)} states with live BLS data")
        return rows

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            labor_force = float(row["labor_force"])
            unemployed = float(row["unemployed"])
            transformed.append(
                {
                    "geography_id": row["geography_id"],
                    "period": row["period"],
                    "metric_name": "labor_force",
                    "raw_value": labor_force,
                    "units": "persons",
                    "source": self.source_name,
                }
            )
            transformed.append(
                {
                    "geography_id": row["geography_id"],
                    "period": row["period"],
                    "metric_name": "unemployment_rate",
                    "raw_value": unemployed / labor_force if labor_force else 0.0,
                    "units": "ratio",
                    "source": self.source_name,
                }
            )
        return transformed
