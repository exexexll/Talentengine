"""Eurostat connector for EU NUTS-2 regional labour market data.

Uses the Eurostat JSON API to fetch unemployment, population, employment,
GDP, and education data at the NUTS-2 level for all EU/EEA member states.
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error
from typing import Any

from data_pipeline.ingestion.base import SourceConnector

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

EU_COUNTRY_CODES = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "EL", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
    "NO", "IS", "CH", "UK",
]


def _fetch_eurostat(dataset: str, params: dict[str, str]) -> dict[str, Any]:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{EUROSTAT_BASE}/{dataset}?{qs}&lang=en&format=JSON"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def _extract_values(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse Eurostat JSON-stat response into list of {geo, time, value}.

    Handles arbitrary number of dimensions by computing flat index from
    the ordered dimension IDs and sizes.
    """
    dim_ids = data.get("id", [])
    sizes = data.get("size", [])
    dims = data.get("dimension", {})
    values = data.get("value", {})

    if not dim_ids or not sizes or not values:
        return []

    dim_indices: dict[str, dict[str, int]] = {}
    for dim_id in dim_ids:
        cat = dims.get(dim_id, {}).get("category", {}).get("index", {})
        dim_indices[dim_id] = cat

    geo_idx_pos = dim_ids.index("geo") if "geo" in dim_ids else -1
    time_idx_pos = dim_ids.index("time") if "time" in dim_ids else -1
    if geo_idx_pos < 0 or time_idx_pos < 0:
        return []

    geo_keys = sorted(dim_indices["geo"].keys(), key=lambda x: dim_indices["geo"][x])
    time_keys = sorted(dim_indices["time"].keys(), key=lambda x: dim_indices["time"][x])

    strides = [1] * len(sizes)
    for i in range(len(sizes) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]

    results: list[dict[str, Any]] = []
    for geo in geo_keys:
        for time_val in time_keys:
            flat = 0
            for d_pos, dim_id in enumerate(dim_ids):
                if dim_id == "geo":
                    flat += dim_indices["geo"][geo] * strides[d_pos]
                elif dim_id == "time":
                    flat += dim_indices["time"][time_val] * strides[d_pos]
                # Other dims fixed at index 0 (first value of sex=T, age=total, etc.)

            if str(flat) in values:
                results.append({
                    "geo": geo,
                    "time": time_val,
                    "value": values[str(flat)],
                })

    return results


class EurostatConnector(SourceConnector):
    source_name = "EUROSTAT_EU"
    cadence = "annual"
    schema_version = "v1"

    def extract_live(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        rows.extend(self._fetch_unemployment())
        rows.extend(self._fetch_population())
        rows.extend(self._fetch_gdp())
        rows.extend(self._fetch_education())
        rows.extend(self._fetch_employment())

        print(f"[EUROSTAT] Total: {len(rows)} rows")
        return rows

    def _fetch_unemployment(self) -> list[dict[str, Any]]:
        """NUTS-2 unemployment rate from lfst_r_lfu3rt."""
        rows: list[dict[str, Any]] = []
        try:
            data = _fetch_eurostat("lfst_r_lfu3rt", {
                "sex": "T",
                "age": "Y15-74",
                "sinceTimePeriod": "2022",
                "unit": "PC",
            })
            for entry in _extract_values(data):
                geo = entry["geo"]
                if len(geo) < 3 or len(geo) > 5:
                    continue
                geo_id = f"EU-{geo}"
                rows.append({
                    "geography_id": geo_id,
                    "period": entry["time"],
                    "metric_name": "unemployment_rate",
                    "raw_value": round(entry["value"] / 100.0, 4),
                    "units": "ratio",
                    "source": self.source_name,
                })
            print(f"[EUROSTAT] Unemployment: {len(rows)} NUTS rows")
        except Exception as exc:
            print(f"[EUROSTAT] Unemployment fetch failed: {exc}")
        return rows

    def _fetch_population(self) -> list[dict[str, Any]]:
        """NUTS-2 population from demo_r_d2jan."""
        rows: list[dict[str, Any]] = []
        try:
            data = _fetch_eurostat("demo_r_d2jan", {
                "sex": "T",
                "age": "TOTAL",
                "sinceTimePeriod": "2022",
                "unit": "NR",
            })
            for entry in _extract_values(data):
                geo = entry["geo"]
                if len(geo) < 2 or len(geo) > 5:
                    continue
                geo_id = f"EU-{geo}"
                rows.append({
                    "geography_id": geo_id,
                    "period": entry["time"],
                    "metric_name": "population",
                    "raw_value": float(entry["value"]),
                    "units": "persons",
                    "source": self.source_name,
                })
            print(f"[EUROSTAT] Population: {len(rows)} rows")
        except Exception as exc:
            print(f"[EUROSTAT] Population fetch failed: {exc}")
        return rows

    def _fetch_gdp(self) -> list[dict[str, Any]]:
        """NUTS-2 GDP per capita from nama_10r_2gdp."""
        rows: list[dict[str, Any]] = []
        try:
            data = _fetch_eurostat("nama_10r_2gdp", {
                "unit": "EUR_HAB",
                "sinceTimePeriod": "2021",
            })
            for entry in _extract_values(data):
                geo = entry["geo"]
                if len(geo) < 3 or len(geo) > 5:
                    continue
                geo_id = f"EU-{geo}"
                rows.append({
                    "geography_id": geo_id,
                    "period": entry["time"],
                    "metric_name": "gdp_per_capita",
                    "raw_value": float(entry["value"]),
                    "units": "eur",
                    "source": self.source_name,
                })
            print(f"[EUROSTAT] GDP: {len(rows)} rows")
        except Exception as exc:
            print(f"[EUROSTAT] GDP fetch failed: {exc}")
        return rows

    def _fetch_education(self) -> list[dict[str, Any]]:
        """NUTS-2 tertiary education share from edat_lfse_04."""
        rows: list[dict[str, Any]] = []
        try:
            data = _fetch_eurostat("edat_lfse_04", {
                "sex": "T",
                "age": "Y25-64",
                "isced11": "ED5-8",
                "sinceTimePeriod": "2022",
                "unit": "PC",
            })
            for entry in _extract_values(data):
                geo = entry["geo"]
                if len(geo) < 3 or len(geo) > 5:
                    continue
                geo_id = f"EU-{geo}"
                rows.append({
                    "geography_id": geo_id,
                    "period": entry["time"],
                    "metric_name": "educational_attainment_bachelors_plus",
                    "raw_value": round(entry["value"] / 100.0, 4),
                    "units": "ratio",
                    "source": self.source_name,
                })
            print(f"[EUROSTAT] Education: {len(rows)} rows")
        except Exception as exc:
            print(f"[EUROSTAT] Education fetch failed: {exc}")
        return rows

    def _fetch_employment(self) -> list[dict[str, Any]]:
        """NUTS-2 employment from lfst_r_lfe2en2."""
        rows: list[dict[str, Any]] = []
        try:
            data = _fetch_eurostat("lfst_r_lfe2en2", {
                "sex": "T",
                "age": "Y15-64",
                "sinceTimePeriod": "2022",
                "unit": "THS",
                "nace_r2": "TOTAL",
            })
            for entry in _extract_values(data):
                geo = entry["geo"]
                if len(geo) < 3 or len(geo) > 5:
                    continue
                geo_id = f"EU-{geo}"
                rows.append({
                    "geography_id": geo_id,
                    "period": entry["time"],
                    "metric_name": "business_employment",
                    "raw_value": float(entry["value"]) * 1000,
                    "units": "persons",
                    "source": self.source_name,
                })
            print(f"[EUROSTAT] Employment: {len(rows)} rows")
        except Exception as exc:
            print(f"[EUROSTAT] Employment fetch failed: {exc}")
        return rows

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return records
