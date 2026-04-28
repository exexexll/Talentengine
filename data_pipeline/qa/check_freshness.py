from datetime import datetime, timezone
from typing import Any


MAX_AGE_DAYS_BY_CADENCE = {
    "daily": 3,
    "monthly": 45,
    "quarterly": 130,
    "annual": 500,
}


def run(snapshots: list[dict[str, Any]]) -> list[str]:
    now = datetime.now(timezone.utc)
    errors: list[str] = []
    for snapshot in snapshots:
        cadence = snapshot.get("cadence", "annual")
        extracted = datetime.fromisoformat(snapshot["extracted_at"])
        age_days = (now - extracted).days
        max_age = MAX_AGE_DAYS_BY_CADENCE.get(cadence, 500)
        if age_days > max_age:
            errors.append(
                f"{snapshot['source_name']} snapshot stale: {age_days}d > {max_age}d"
            )
    return errors
