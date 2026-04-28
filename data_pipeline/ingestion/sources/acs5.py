import os
from typing import Any

from data_pipeline.ingestion.base import SourceConnector


class ACS5Connector(SourceConnector):
    source_name = "CENSUS_ACS5"
    cadence = "annual"
    schema_version = "v1"
    _live_required_fields = {
        "geography_id", "period", "population", "internet_access_rate",
        "work_from_home_rate", "educational_attainment_bachelors_plus",
        "commute_mean_minutes", "housing_cost_burden_ratio", "median_household_income",
    }

    _CENSUS_VARS = {
        "B01003_001E": "total_pop",
        "B28002_004E": "broadband_hh",
        "B28002_001E": "total_hh",
        "B08301_021E": "wfh_workers",
        "B08301_001E": "total_workers",
        "B15003_022E": "bachelors",
        "B15003_023E": "masters",
        "B15003_024E": "professional",
        "B15003_025E": "doctorate",
        "B15003_001E": "edu_total",
        "B08135_001E": "agg_travel_time",
        "B25071_001E": "median_rent_pct",
        "B19013_001E": "median_hh_income",
        "B23025_002E": "in_labor_force",
        "B23025_005E": "unemployed",
    }

    def _fetch_census(self, client: Any, api_key: str, geo_clause: str) -> list[dict[str, str]]:
        var_list = ",".join(self._CENSUS_VARS.keys())
        url = (
            f"https://api.census.gov/data/2023/acs/acs5"
            f"?get=NAME,{var_list}&{geo_clause}&key={api_key}"
        )
        resp = client.get(url)
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        if "text/html" in ct:
            raise ValueError("Census API returned HTML (likely invalid key)")
        data = resp.json()
        if not data or len(data) < 2:
            return []
        headers = data[0]
        return [dict(zip(headers, row)) for row in data[1:]]

    def _parse_row(self, d: dict[str, str], geography_id: str) -> dict[str, Any] | None:
        pop = self._safe_float(d.get("B01003_001E"))
        if pop <= 0:
            return None

        bb_hh = self._safe_float(d.get("B28002_004E"))
        tot_hh = self._safe_float(d.get("B28002_001E"))
        wfh = self._safe_float(d.get("B08301_021E"))
        tot_workers = self._safe_float(d.get("B08301_001E"))
        ba = self._safe_float(d.get("B15003_022E"))
        ma = self._safe_float(d.get("B15003_023E"))
        prof = self._safe_float(d.get("B15003_024E"))
        phd = self._safe_float(d.get("B15003_025E"))
        edu_tot = self._safe_float(d.get("B15003_001E"))
        agg_travel = self._safe_float(d.get("B08135_001E"))
        rent_pct = self._safe_float(d.get("B25071_001E"))
        income = self._safe_float(d.get("B19013_001E"))
        labor_force = self._safe_float(d.get("B23025_002E"))
        unemployed = self._safe_float(d.get("B23025_005E"))

        internet_rate = bb_hh / tot_hh if tot_hh > 0 else 0.0
        wfh_rate = wfh / tot_workers if tot_workers > 0 else 0.0
        ba_plus = (ba + ma + prof + phd) / edu_tot if edu_tot > 0 else 0.0
        commuters = tot_workers - wfh
        commute_min = agg_travel / commuters if commuters > 0 else 0.0
        housing_burden = rent_pct / 100.0 if rent_pct > 0 else 0.0
        unemp_rate = unemployed / labor_force if labor_force > 0 else 0.0

        return {
            "geography_id": geography_id,
            "period": "2023",
            "population": pop,
            "internet_access_rate": round(internet_rate, 4),
            "work_from_home_rate": round(wfh_rate, 4),
            "educational_attainment_bachelors_plus": round(ba_plus, 4),
            "commute_mean_minutes": round(commute_min, 1),
            "housing_cost_burden_ratio": round(housing_burden, 4),
            "median_household_income": income,
            "labor_force": labor_force,
            "unemployment_rate": round(unemp_rate, 4),
        }

    def extract_live(self) -> list[dict[str, Any]]:
        api_key = os.getenv("CENSUS_API_KEY", "").strip()
        if not api_key:
            return []

        import httpx

        rows: list[dict[str, Any]] = []

        with httpx.Client(timeout=120, follow_redirects=True) as client:
            # --- States ---
            print("[ACS5] Fetching state-level data...")
            for d in self._fetch_census(client, api_key, "for=state:*"):
                fips = d.get("state", "")
                if not fips or fips == "72":
                    continue
                parsed = self._parse_row(d, fips)
                if parsed:
                    rows.append(parsed)
            print(f"[ACS5] {len(rows)} states loaded")

            # --- Counties (all ~3,200) ---
            print("[ACS5] Fetching county-level data...")
            county_count = 0
            for d in self._fetch_census(client, api_key, "for=county:*&in=state:*"):
                state = d.get("state", "")
                county = d.get("county", "")
                if not state or not county or state == "72":
                    continue
                fips = f"{state}{county}"
                parsed = self._parse_row(d, fips)
                if parsed:
                    rows.append(parsed)
                    county_count += 1
            print(f"[ACS5] {county_count} counties loaded")

            # --- Places/Cities (all incorporated places) ---
            print("[ACS5] Fetching place/city-level data...")
            place_count = 0
            for d in self._fetch_census(client, api_key, "for=place:*&in=state:*"):
                state = d.get("state", "")
                place = d.get("place", "")
                if not state or not place or state == "72":
                    continue
                pop = self._safe_float(d.get("B01003_001E"))
                if pop < 10000:
                    continue
                fips = f"{state}{place}"
                parsed = self._parse_row(d, fips)
                if parsed:
                    parsed["place_name"] = d.get("NAME", "")
                    rows.append(parsed)
                    place_count += 1
            print(f"[ACS5] {place_count} places (pop >= 10k) loaded")

        print(f"[ACS5] Total: {len(rows)} geographies")
        return rows

    def transform(self, records: list[dict]) -> list[dict]:
        transformed: list[dict] = []
        for row in records:
            base = {"geography_id": row["geography_id"], "period": row["period"], "source": self.source_name}
            transformed.extend(
                [
                    {**base, "metric_name": "population", "raw_value": float(row["population"]), "units": "persons"},
                    {**base, "metric_name": "internet_access_rate", "raw_value": float(row["internet_access_rate"]), "units": "ratio"},
                    {**base, "metric_name": "work_from_home_rate", "raw_value": float(row["work_from_home_rate"]), "units": "ratio"},
                    {**base, "metric_name": "educational_attainment_bachelors_plus", "raw_value": float(row["educational_attainment_bachelors_plus"]), "units": "ratio"},
                    {**base, "metric_name": "commute_mean_minutes", "raw_value": float(row["commute_mean_minutes"]), "units": "minutes"},
                    {**base, "metric_name": "housing_cost_burden_ratio", "raw_value": float(row["housing_cost_burden_ratio"]), "units": "ratio"},
                    {**base, "metric_name": "median_household_income", "raw_value": float(row["median_household_income"]), "units": "usd"},
                ]
            )
            if "labor_force" in row and row["labor_force"]:
                transformed.append({**base, "metric_name": "labor_force", "raw_value": float(row["labor_force"]), "units": "persons"})
            if "unemployment_rate" in row:
                transformed.append({**base, "metric_name": "unemployment_rate", "raw_value": float(row["unemployment_rate"]), "units": "ratio"})
        return transformed
