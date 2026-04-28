import tempfile
import unittest
from pathlib import Path

try:
    from backend.app.models.schemas import ScenarioWeights
    from backend.app.services import artifact_store
except ModuleNotFoundError:  # pragma: no cover - local env may miss optional deps
    ScenarioWeights = None  # type: ignore[assignment]
    artifact_store = None  # type: ignore[assignment]


class ScenarioWeightsValidationTests(unittest.TestCase):
    @unittest.skipIf(ScenarioWeights is None, "pydantic/backend dependencies are not installed")
    def test_weight_sum_valid(self) -> None:
        weights = ScenarioWeights(
            business_demand=0.3,
            talent_supply=0.2,
            market_gap=0.2,
            cost_efficiency=0.1,
            execution_feasibility=0.2,
        )
        self.assertAlmostEqual(
            weights.business_demand
            + weights.talent_supply
            + weights.market_gap
            + weights.cost_efficiency
            + weights.execution_feasibility,
            1.0,
        )

    @unittest.skipIf(ScenarioWeights is None, "pydantic/backend dependencies are not installed")
    def test_weight_sum_invalid(self) -> None:
        with self.assertRaises(ValueError):
            ScenarioWeights(
                business_demand=0.4,
                talent_supply=0.2,
                market_gap=0.2,
                cost_efficiency=0.1,
                execution_feasibility=0.2,
            )


class ArtifactStoreCompatibilityTests(unittest.TestCase):
    @unittest.skipIf(artifact_store is None, "backend dependencies are not installed")
    def test_phase4_fallback_without_snapshot_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            phase4 = root / "phase4" / "run1"
            phase4.mkdir(parents=True)
            (phase4 / "score_fact.ndjson").write_text(
                '{"geography_id":"06","score_value":42.0}\n',
                encoding="utf-8",
            )
            (phase4 / "recommendation_fact.ndjson").write_text(
                '{"geography_id":"06","recommendation_label":"Monitor"}\n',
                encoding="utf-8",
            )
            original_root = artifact_store.ARTIFACT_ROOT
            try:
                artifact_store.ARTIFACT_ROOT = root
                artifact_store.refresh_cache()
                bundle = artifact_store.load_latest_artifact_bundle("phase4")
                self.assertEqual(len(bundle["metrics"]), 1)
                self.assertEqual(bundle["snapshots_by_id"], {})
            finally:
                artifact_store.ARTIFACT_ROOT = original_root
                artifact_store.refresh_cache()


if __name__ == "__main__":
    unittest.main()
