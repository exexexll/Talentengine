from fastapi import APIRouter
from fastapi import HTTPException
import os

from backend.app.services.artifact_store import cleanup_old_runs, load_latest_artifact_bundle
from backend.app.services.artifact_store import refresh_cache as refresh_artifact_cache
from backend.app.services.cache import api_cache
from backend.app.services.metrics_engine import (
    _infer_geography_type,
    US_CITY_DIRECT_METRICS,
    all_metrics_grouped_by_geography,
    refresh_metric_cache,
)
from data_pipeline.ingestion.sources import ALL_CONNECTORS

router = APIRouter()

_MODELED_PROXY_SOURCES = {"ABS_AUSTRALIA", "INDIA_WORLDBANK"}
_US_DEMAND_CORE_METRICS = [
    "industry_employment",
    "business_establishments",
    "business_employment",
    "job_creation_rate",
    "target_occupation_employment",
    "relevant_completions",
]


@router.post("/refresh-caches")
def refresh_caches() -> dict[str, str]:
    api_cache.clear()
    refresh_artifact_cache()
    refresh_metric_cache()
    return {"status": "ok"}


@router.get("/status")
def status() -> dict[str, object]:
    all_bundle = load_latest_artifact_bundle("all")
    phase4_bundle = load_latest_artifact_bundle("phase4")
    live_sources = [connector().live_configuration() for connector in ALL_CONNECTORS]
    missing_live_urls = [entry["source_name"] for entry in live_sources if not entry["url_configured"]]
    grouped = all_metrics_grouped_by_geography()

    def _coverage_for_geo_type(geo_type: str) -> dict[str, float]:
        ids = [gid for gid in grouped if _infer_geography_type(gid) == geo_type]
        if not ids:
            return {}
        out: dict[str, float] = {}
        for metric in _US_DEMAND_CORE_METRICS:
            present = 0
            for gid in ids:
                names = {m.metric_name for m in grouped[gid]}
                if metric in names:
                    present += 1
            out[metric] = round(present / len(ids), 4)
        return out

    def _us_city_direct_metric_coverage() -> dict[str, float]:
        ids = [gid for gid in grouped if _infer_geography_type(gid) == "place" and gid.isdigit()]
        if not ids:
            return {}
        out: dict[str, float] = {}
        for metric in sorted(US_CITY_DIRECT_METRICS):
            present = 0
            for gid in ids:
                names = {m.metric_name for m in grouped[gid]}
                if metric in names:
                    present += 1
            out[metric] = round(present / len(ids), 4)
        return out

    return {
        "cache_backend": api_cache.backend_name,
        "cache_entries": api_cache.entry_count,
        "source_mode": os.getenv("FIGWORK_SOURCE_MODE", "catalog"),
        "source_catalog_file": os.getenv(
            "FIGWORK_SOURCE_RECORDS_FILE",
            "data_pipeline/source_snapshots/local_source_records.json",
        ),
        "api_key_status": {
            "CENSUS_API_KEY": bool(os.getenv("CENSUS_API_KEY")),
            "BLS_API_KEY": bool(os.getenv("BLS_API_KEY")),
            "BEA_API_KEY": bool(os.getenv("BEA_API_KEY")),
            "ONET_USERNAME": bool(os.getenv("ONET_USERNAME")),
            "ONET_PASSWORD": bool(os.getenv("ONET_PASSWORD")),
            "COLLEGE_SCORECARD_API_KEY": bool(os.getenv("COLLEGE_SCORECARD_API_KEY")),
        },
        "live_source_status": {
            "configured_sources": len(live_sources) - len(missing_live_urls),
            "total_sources": len(live_sources),
            "missing_url_sources": missing_live_urls,
            "sources": live_sources,
        },
        "dataset_audit": {
            "modeled_proxy_sources": sorted(_MODELED_PROXY_SOURCES),
            "us_county_demand_metric_coverage": _coverage_for_geo_type("county"),
            "us_place_demand_metric_coverage": _coverage_for_geo_type("place"),
            "us_city_direct_metric_coverage": _us_city_direct_metric_coverage(),
        },
        "artifacts": {
            "all": {
                "run_name": all_bundle["run_name"],
                "metric_rows": len(all_bundle["metrics"]),
                "snapshot_rows": len(all_bundle["snapshots"]),
                "geographies": len(all_bundle["metrics_by_geography"]),
            },
            "phase4": {
                "run_name": phase4_bundle["run_name"],
                "score_rows": len(phase4_bundle["metrics"]),
                "recommendation_rows": len(phase4_bundle["snapshots"]),
            },
        },
    }


@router.post("/cleanup-artifacts")
def cleanup_artifacts(keep_last_n: int = 20) -> dict[str, object]:
    try:
        deleted = cleanup_old_runs(keep_last_n=keep_last_n)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok", "deleted_runs": deleted, "keep_last_n": keep_last_n}
