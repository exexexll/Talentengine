import json
import os
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any


@dataclass
class SourceSnapshot:
    snapshot_id: str
    source_name: str
    extracted_at: datetime
    schema_version: str
    cadence: str
    row_count: int = 0


class SourceConnector:
    source_name: str = "unknown"
    cadence: str = "unknown"
    schema_version: str = "v1"
    _live_required_fields: set[str] = {"geography_id", "period"}

    def extract(self) -> list[dict[str, Any]]:
        mode = self._source_mode()
        if mode == "live":
            # Prefer connector-specific live extraction when implemented.
            # Fall back to generic URL/file endpoint mode for connectors that
            # rely on FIGWORK_LIVE_<SOURCE>_URL contracts.
            try:
                live_rows = self.extract_live()
                if live_rows:
                    return live_rows
            except Exception as exc:
                print(f"[LIVE] {self.source_name}: extract_live failed, trying endpoint mode: {exc}")
            return self.extract_from_live_endpoint(self._live_required_fields)
        elif mode == "hybrid":
            return self._extract_hybrid()
        return self.extract_from_catalog()

    def extract_live(self) -> list[dict[str, Any]]:
        """Override in subclasses for source-specific live API calls.
        Returns raw rows in catalog schema. Empty list = no live implementation."""
        return []

    def _extract_hybrid(self) -> list[dict[str, Any]]:
        """Hybrid: fetch live, merge with catalog. Live rows win by geography_id."""
        catalog_rows: list[dict[str, Any]] = []
        try:
            catalog_rows = self.extract_from_catalog()
        except (FileNotFoundError, KeyError):
            pass

        live_rows: list[dict[str, Any]] = []
        try:
            live_rows = self.extract_live()
        except Exception as exc:
            print(f"[HYBRID] {self.source_name}: live extraction failed: {exc}")

        if not live_rows:
            print(f"[HYBRID] {self.source_name}: no live data, catalog only ({len(catalog_rows)} rows)")
            return catalog_rows

        live_geos = {row["geography_id"] for row in live_rows}
        catalog_gap = [r for r in catalog_rows if r["geography_id"] not in live_geos]
        merged = live_rows + catalog_gap
        print(
            f"[HYBRID] {self.source_name}: "
            f"{len(live_rows)} live + {len(catalog_gap)} catalog gap = {len(merged)} total"
        )
        return merged

    def _safe_float(self, val: Any, default: float = 0.0) -> float:
        if val is None:
            return default
        try:
            f = float(val)
            if f < -100_000:
                return default
            return f
        except (ValueError, TypeError):
            return default

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return records

    def _source_catalog_file(self) -> Path:
        configured = os.getenv(
            "FIGWORK_SOURCE_RECORDS_FILE",
            "data_pipeline/source_snapshots/local_source_records.json",
        )
        return Path(configured)

    def _source_mode(self) -> str:
        return os.getenv("FIGWORK_SOURCE_MODE", "catalog").strip().lower()

    def _source_code(self) -> str:
        return self.source_name.replace("-", "_").replace("/", "_").upper()

    def _live_url_env(self) -> str:
        return f"FIGWORK_LIVE_{self._source_code()}_URL"

    def _live_token_env(self) -> str:
        return f"FIGWORK_LIVE_{self._source_code()}_TOKEN"

    def _live_timeout_seconds(self) -> float:
        return float(os.getenv("FIGWORK_LIVE_TIMEOUT_SECONDS", "60"))

    def _allow_catalog_fallback(self) -> bool:
        return os.getenv("FIGWORK_LIVE_ALLOW_CATALOG_FALLBACK", "0").strip() in {"1", "true", "yes"}

    def live_configuration(self) -> dict[str, Any]:
        url_env = self._live_url_env()
        token_env = self._live_token_env()
        return {
            "source_name": self.source_name,
            "url_env": url_env,
            "url_configured": bool(os.getenv(url_env)),
            "token_env": token_env,
            "token_configured": bool(os.getenv(token_env)),
        }

    def extract_from_catalog(self) -> list[dict[str, Any]]:
        catalog_path = self._source_catalog_file()
        if not catalog_path.exists():
            raise FileNotFoundError(
                f"Source catalog missing: {catalog_path}. "
                "Set FIGWORK_SOURCE_RECORDS_FILE or create the default catalog file."
            )
        with catalog_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if self.source_name not in payload:
            strict = os.getenv("FIGWORK_SOURCE_CATALOG_STRICT", "0").strip().lower() in {"1", "true", "yes"}
            if strict:
                raise KeyError(
                    f"{self.source_name} missing in source catalog {catalog_path}. "
                    "Add source records before running ingestion."
                )
            print(
                f"[CATALOG] {self.source_name} missing in {catalog_path}; "
                "continuing with empty record set (set FIGWORK_SOURCE_CATALOG_STRICT=1 to fail)."
            )
            return []
        rows = payload[self.source_name]
        if not isinstance(rows, list):
            raise ValueError(f"{self.source_name} catalog entry must be a list of records.")
        return rows

    def extract_from_live_endpoint(self, required_fields: set[str]) -> list[dict[str, Any]]:
        source_url = os.getenv(self._live_url_env(), "").strip()
        if not source_url:
            if self._allow_catalog_fallback():
                return self.extract_from_catalog()
            raise ValueError(
                f"Live mode requires {self._live_url_env()} for source {self.source_name}. "
                "Provide an API/file URL or enable FIGWORK_LIVE_ALLOW_CATALOG_FALLBACK=1."
            )

        raw_rows: list[dict[str, Any]] = []
        if source_url.startswith("http://") or source_url.startswith("https://"):
            try:
                import httpx
            except ModuleNotFoundError as exc:
                raise ModuleNotFoundError(
                    "httpx is required for HTTP live source mode. "
                    "Install dependencies with `pip install -e .`."
                ) from exc
            headers: dict[str, str] = {}
            token = os.getenv(self._live_token_env(), "").strip()
            if token:
                headers["Authorization"] = f"Bearer {token}"
            with httpx.Client(timeout=self._live_timeout_seconds()) as client:
                response = client.get(source_url, headers=headers)
                response.raise_for_status()
                content_type = response.headers.get("content-type", "").lower()
                text = response.text
                if "application/json" in content_type or source_url.endswith(".json"):
                    payload = response.json()
                    raw_rows = self._coerce_rows(payload)
                elif "text/csv" in content_type or source_url.endswith(".csv"):
                    raw_rows = self._rows_from_csv_text(text)
                elif source_url.endswith(".ndjson"):
                    raw_rows = self._rows_from_ndjson_text(text)
                else:
                    raise ValueError(
                        f"Unsupported live response format for {self.source_name}: {content_type}"
                    )
        else:
            path = Path(source_url)
            if not path.exists():
                raise FileNotFoundError(
                    f"Configured live source path not found for {self.source_name}: {path}"
                )
            text = path.read_text(encoding="utf-8")
            if path.suffix.lower() == ".json":
                raw_rows = self._coerce_rows(json.loads(text))
            elif path.suffix.lower() == ".csv":
                raw_rows = self._rows_from_csv_text(text)
            elif path.suffix.lower() == ".ndjson":
                raw_rows = self._rows_from_ndjson_text(text)
            else:
                raise ValueError(
                    f"Unsupported live file extension for {self.source_name}: {path.suffix}"
                )

        if not isinstance(raw_rows, list):
            raise ValueError(f"{self.source_name} live endpoint did not produce row list")
        for idx, row in enumerate(raw_rows):
            if not isinstance(row, dict):
                raise ValueError(f"{self.source_name} live row {idx} is not an object")
            missing = required_fields.difference(row.keys())
            if missing:
                raise ValueError(
                    f"{self.source_name} live row {idx} missing fields: {sorted(missing)}"
                )
        return raw_rows

    def _coerce_rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("rows", "data", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return value
        raise ValueError(f"{self.source_name} live payload must contain list rows")

    def _rows_from_csv_text(self, text: str) -> list[dict[str, Any]]:
        reader = csv.DictReader(StringIO(text))
        return [dict(row) for row in reader]

    def _rows_from_ndjson_text(self, text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
        return rows

    def validate(self, records: list[dict[str, Any]]) -> None:
        required = {"geography_id", "period", "metric_name", "raw_value", "units", "source"}
        for idx, row in enumerate(records):
            missing = required.difference(row.keys())
            if missing:
                raise ValueError(
                    f"{self.source_name} row {idx} missing keys: {sorted(missing)}"
                )

    def snapshot(self, row_count: int) -> SourceSnapshot:
        timestamp = datetime.now(timezone.utc)
        snapshot_id = f"{self.source_name}-{timestamp.strftime('%Y%m%dT%H%M%SZ')}"
        return SourceSnapshot(
            snapshot_id=snapshot_id,
            source_name=self.source_name,
            extracted_at=timestamp,
            schema_version=self.schema_version,
            cadence=self.cadence,
            row_count=row_count,
        )

    def run(self) -> tuple[SourceSnapshot, list[dict[str, Any]]]:
        raw = self.extract()
        transformed = self.transform(raw)
        self.validate(transformed)
        return self.snapshot(row_count=len(transformed)), transformed
