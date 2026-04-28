from typing import Any


def run(rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (
            row["source_snapshot_id"],
            row["geography_id"],
            row["period"],
            row["metric_name"],
        )
        if key in seen:
            errors.append(
                "duplicate metric key "
                f"(source_snapshot_id={key[0]}, geography_id={key[1]}, period={key[2]}, metric={key[3]})"
            )
        seen.add(key)
    return errors
