"""
Prefect orchestration flows for the Figwork data pipeline.

Cadence mapping (from PRD section 20):
  daily   - FCC broadband, scenario recomputes, cache refresh
  monthly - LAUS, IRS migration monitoring
  quarterly - QCEW
  annual  - OEWS, CBP/ZBP, BEA RPP, BEA GDP, Pop Estimates, IPEDS,
            College Scorecard, O*NET, RUCA/RUCC, LEHD/LODES

Usage:
  # Run locally
  python -m data_pipeline.orchestration.flows

  # Register with Prefect server
  prefect deployment build data_pipeline/orchestration/flows.py:nightly_full_refresh \
      --name nightly-full --cron "0 3 * * *"
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

try:
    from prefect import flow, task, get_run_logger
    from prefect.tasks import task_input_hash
    from datetime import timedelta

    PREFECT_AVAILABLE = True
except ModuleNotFoundError:
    PREFECT_AVAILABLE = False

    def flow(*args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        def decorator(fn: Any) -> Any:
            return fn
        if args and callable(args[0]):
            return args[0]
        return decorator

    def task(*args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        def decorator(fn: Any) -> Any:
            return fn
        if args and callable(args[0]):
            return args[0]
        return decorator

    task_input_hash = None  # type: ignore[assignment]
    timedelta = None  # type: ignore[assignment,misc]


def _log(msg: str) -> None:
    if PREFECT_AVAILABLE:
        try:
            get_run_logger().info(msg)
            return
        except Exception:
            pass
    logger.info(msg)


@task(name="ingest_all_sources", retries=2, retry_delay_seconds=30)
def ingest_all_sources() -> str:
    from data_pipeline.ingestion.build_all_dataset import run as run_build
    _log("Starting full source ingestion (all connectors)")
    run_build()
    _log("Full source ingestion complete")
    return datetime.now(timezone.utc).isoformat()


@task(name="ingest_phase2_sources", retries=2, retry_delay_seconds=30)
def ingest_phase2_sources() -> str:
    from data_pipeline.ingestion.build_phase2_dataset import run as run_build
    _log("Starting phase-2 source ingestion")
    run_build()
    _log("Phase-2 source ingestion complete")
    return datetime.now(timezone.utc).isoformat()


@task(name="ingest_phase3_sources", retries=2, retry_delay_seconds=30)
def ingest_phase3_sources() -> str:
    from data_pipeline.ingestion.build_phase3_dataset import run as run_build
    _log("Starting phase-3 source ingestion")
    run_build()
    _log("Phase-3 source ingestion complete")
    return datetime.now(timezone.utc).isoformat()


@task(name="compute_scores", retries=1, retry_delay_seconds=10)
def compute_scores(depends_on: str | None = None) -> str:
    from data_pipeline.scoring.build_score_fact import run as run_scores
    _log("Starting score + recommendation computation")
    run_scores()
    _log("Score computation complete")
    return datetime.now(timezone.utc).isoformat()


@task(name="run_qa_checks", retries=1, retry_delay_seconds=5)
def run_qa_checks(depends_on: str | None = None) -> str:
    from data_pipeline.qa.release_acceptance import run as run_qa
    _log("Starting release acceptance QA")
    run_qa()
    _log("Release acceptance QA passed")
    return datetime.now(timezone.utc).isoformat()


@task(name="refresh_api_caches")
def refresh_api_caches(depends_on: str | None = None) -> str:
    api_url = os.getenv("FIGWORK_API_URL", "http://localhost:8000")
    try:
        import httpx
        with httpx.Client(timeout=30) as client:
            response = client.post(f"{api_url}/api/system/refresh-caches")
            response.raise_for_status()
        _log(f"API cache refresh succeeded: {response.json()}")
    except Exception as exc:
        _log(f"API cache refresh skipped (API may not be running): {exc}")
    return datetime.now(timezone.utc).isoformat()


@task(name="cleanup_old_artifacts")
def cleanup_old_artifacts(keep_last_n: int = 20) -> str:
    from backend.app.services.artifact_store import cleanup_old_runs
    deleted = cleanup_old_runs(keep_last_n=keep_last_n)
    _log(f"Artifact cleanup complete: {deleted}")
    return datetime.now(timezone.utc).isoformat()


@flow(name="nightly_full_refresh", log_prints=True)
def nightly_full_refresh() -> None:
    """Full pipeline: ingest all sources -> score -> QA -> refresh caches -> cleanup."""
    _log("=== Nightly full refresh started ===")
    ingest_ts = ingest_all_sources()
    score_ts = compute_scores(depends_on=ingest_ts)
    qa_ts = run_qa_checks(depends_on=score_ts)
    refresh_api_caches(depends_on=qa_ts)
    cleanup_old_artifacts(keep_last_n=30)
    _log("=== Nightly full refresh complete ===")


@flow(name="daily_fast_refresh", log_prints=True)
def daily_fast_refresh() -> None:
    """Lightweight daily: recompute scores + refresh caches (no re-ingestion)."""
    _log("=== Daily fast refresh started ===")
    score_ts = compute_scores()
    refresh_api_caches(depends_on=score_ts)
    _log("=== Daily fast refresh complete ===")


@flow(name="monthly_laus_refresh", log_prints=True)
def monthly_laus_refresh() -> None:
    """Monthly cadence: re-ingest all (picks up fresh LAUS) + rescore."""
    _log("=== Monthly LAUS refresh started ===")
    ingest_ts = ingest_all_sources()
    score_ts = compute_scores(depends_on=ingest_ts)
    run_qa_checks(depends_on=score_ts)
    refresh_api_caches(depends_on=score_ts)
    _log("=== Monthly LAUS refresh complete ===")


@flow(name="quarterly_qcew_refresh", log_prints=True)
def quarterly_qcew_refresh() -> None:
    """Quarterly cadence: full re-ingestion + scoring for QCEW update window."""
    _log("=== Quarterly QCEW refresh started ===")
    ingest_ts = ingest_all_sources()
    score_ts = compute_scores(depends_on=ingest_ts)
    qa_ts = run_qa_checks(depends_on=score_ts)
    refresh_api_caches(depends_on=qa_ts)
    cleanup_old_artifacts(keep_last_n=20)
    _log("=== Quarterly QCEW refresh complete ===")


@flow(name="annual_structural_refresh", log_prints=True)
def annual_structural_refresh() -> None:
    """Annual cadence: full pipeline with aggressive cleanup."""
    _log("=== Annual structural refresh started ===")
    ingest_ts = ingest_all_sources()
    score_ts = compute_scores(depends_on=ingest_ts)
    qa_ts = run_qa_checks(depends_on=score_ts)
    refresh_api_caches(depends_on=qa_ts)
    cleanup_old_artifacts(keep_last_n=10)
    _log("=== Annual structural refresh complete ===")


if __name__ == "__main__":
    nightly_full_refresh()
