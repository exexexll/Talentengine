from datetime import date


def confidence_from_freshness(last_refresh: date, max_age_days: int = 365) -> float:
    age_days = (date.today() - last_refresh).days
    if age_days <= 0:
        return 1.0
    penalty = min(age_days / max_age_days, 1.0)
    return max(0.1, 1.0 - penalty)


def apply_quality_penalties(
    base_confidence: float,
    is_imputed_from_coarser_geo: bool = False,
    has_known_source_noise: bool = False,
) -> float:
    confidence = base_confidence
    if is_imputed_from_coarser_geo:
        confidence -= 0.2
    if has_known_source_noise:
        confidence -= 0.1
    return min(max(confidence, 0.05), 1.0)
