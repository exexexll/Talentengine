import os
from typing import Any

from data_pipeline.ingestion.base import SourceConnector


class CBPZBPConnector(SourceConnector):
    source_name = "CENSUS_CBP_ZBP"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {"geography_id", "period", "establishments", "employment", "annual_payroll"}

    def _fetch_cbp(self, client: Any, api_key: str, geo_clause: str, naics: str = "00") -> list[dict[str, str]]:
        url = (
            f"https://api.census.gov/data/2022/cbp"
            f"?get=ESTAB,EMP,PAYANN,NAICS2017&{geo_clause}&NAICS2017={naics}&key={api_key}"
        )
        resp = client.get(url)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        if "text/html" in ct:
            raise ValueError("Census CBP API returned HTML (likely invalid key)")
        data = resp.json()
        if not data or len(data) < 2:
            return []
        headers = data[0]
        return [dict(zip(headers, row)) for row in data[1:]]

    def extract_live(self) -> list[dict[str, Any]]:
        api_key = os.getenv("CENSUS_API_KEY", "").strip()
        if not api_key:
            return []

        import httpx

        rows: list[dict[str, Any]] = []

        with httpx.Client(timeout=120, follow_redirects=True) as client:
            # --- States (all industries total) ---
            print("[CBP] Fetching state-level data...")
            for d in self._fetch_cbp(client, api_key, "for=state:*", naics="00"):
                fips = d.get("state", "")
                if not fips:
                    continue
                rows.append({
                    "geography_id": fips,
                    "period": "2022",
                    "naics_code": "00",
                    "establishments": self._safe_float(d.get("ESTAB")),
                    "employment": self._safe_float(d.get("EMP")),
                    "annual_payroll": self._safe_float(d.get("PAYANN")) * 1_000,
                })
            print(f"[CBP] {len(rows)} states loaded")

            # --- Counties (all industries total) ---
            print("[CBP] Fetching county-level data...")
            county_count = 0
            for d in self._fetch_cbp(client, api_key, "for=county:*&in=state:*", naics="00"):
                state = d.get("state", "")
                county = d.get("county", "")
                if not state or not county:
                    continue
                fips = f"{state}{county}"
                emp = self._safe_float(d.get("EMP"))
                if emp <= 0:
                    continue
                rows.append({
                    "geography_id": fips,
                    "period": "2022",
                    "naics_code": "00",
                    "establishments": self._safe_float(d.get("ESTAB")),
                    "employment": emp,
                    "annual_payroll": self._safe_float(d.get("PAYANN")) * 1_000,
                })
                county_count += 1
            print(f"[CBP] {county_count} counties loaded")

            # --- Tech industry overlay (NAICS 5415) for states ---
            print("[CBP] Fetching NAICS 5415 (tech) state overlay...")
            tech_count = 0
            for d in self._fetch_cbp(client, api_key, "for=state:*", naics="5415"):
                fips = d.get("state", "")
                if not fips:
                    continue
                rows.append({
                    "geography_id": fips,
                    "period": "2022",
                    "naics_code": "5415",
                    "establishments": self._safe_float(d.get("ESTAB")),
                    "employment": self._safe_float(d.get("EMP")),
                    "annual_payroll": self._safe_float(d.get("PAYANN")) * 1_000,
                })
                tech_count += 1
            print(f"[CBP] {tech_count} state tech overlays loaded")

        print(f"[CBP] Total: {len(rows)} rows")
        return rows

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            geo_id = row["geography_id"]
            period = row["period"]
            naics = row.get("naics_code", "00")
            prefix = "" if naics == "00" else "industry_"

            transformed.extend(
                [
                    {
                        "geography_id": geo_id,
                        "period": period,
                        "metric_name": f"{prefix}business_establishments" if prefix else "business_establishments",
                        "raw_value": float(row["establishments"]),
                        "units": "count",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": geo_id,
                        "period": period,
                        "metric_name": f"{prefix}employment" if prefix else "business_employment",
                        "raw_value": float(row["employment"]),
                        "units": "workers",
                        "source": self.source_name,
                    },
                    {
                        "geography_id": geo_id,
                        "period": period,
                        "metric_name": f"{prefix}annual_payroll" if prefix else "business_annual_payroll",
                        "raw_value": float(row["annual_payroll"]),
                        "units": "usd",
                        "source": self.source_name,
                    },
                ]
            )
        return transformed
