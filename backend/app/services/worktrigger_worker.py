from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

from backend.app.services.worktrigger_service import WorkTriggerService
from backend.app.services.worktrigger_store import WorkTriggerStore


def run_worker_loop(
    *,
    poll_interval_seconds: float = 1.0,
    max_idle_polls: int | None = None,
    allowed_types: list[str] | None = None,
) -> dict[str, Any]:
    store = WorkTriggerStore(os.getenv("WORKTRIGGER_DB_PATH", "backend/data/worktrigger.sqlite3"))
    service = WorkTriggerService(store)
    worker_id = os.getenv("WORKTRIGGER_WORKER_ID", "worktrigger-worker-1")
    processed = 0
    failed = 0
    idle_polls = 0

    while True:
        job = store.claim_next_job(allowed_types=allowed_types)
        if job is None:
            idle_polls += 1
            store.upsert_worker_heartbeat(
                worker_id=worker_id,
                status="idle",
                last_result={"processed": processed, "failed": failed, "ts": datetime.now(timezone.utc).isoformat()},
            )
            if max_idle_polls is not None and idle_polls >= max_idle_polls:
                break
            time.sleep(poll_interval_seconds)
            continue
        idle_polls = 0
        try:
            service.run_job(job["job_type"], job["payload"])
            store.complete_job(job["id"])
            processed += 1
            store.upsert_worker_heartbeat(
                worker_id=worker_id,
                status="running",
                last_result={"last_job_id": job["id"], "processed": processed, "failed": failed},
            )
        except Exception as exc:  # pragma: no cover - runtime variability
            store.fail_job(job["id"], str(exc))
            failed += 1
            store.upsert_worker_heartbeat(
                worker_id=worker_id,
                status="error",
                last_result={"last_job_id": job["id"], "error": str(exc), "processed": processed, "failed": failed},
            )
    return {"processed": processed, "failed": failed, "idle_polls": idle_polls}


if __name__ == "__main__":
    summary = run_worker_loop(
        poll_interval_seconds=float(os.getenv("WORKTRIGGER_WORKER_POLL_SECONDS", "1.0")),
        max_idle_polls=int(os.getenv("WORKTRIGGER_WORKER_MAX_IDLE_POLLS", "60")),
        allowed_types=None,
    )
    print(summary)
