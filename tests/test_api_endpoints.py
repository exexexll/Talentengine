import unittest
from uuid import uuid4

try:
    from fastapi.testclient import TestClient

    from backend.app.main import app
    from data_pipeline.ingestion.build_all_dataset import run as run_all_dataset
    from data_pipeline.scoring.build_score_fact import run as run_score_fact
except ModuleNotFoundError:  # pragma: no cover - optional local dependency
    TestClient = None  # type: ignore[assignment]
    app = None  # type: ignore[assignment]
    run_all_dataset = None  # type: ignore[assignment]
    run_score_fact = None  # type: ignore[assignment]


@unittest.skipIf(TestClient is None or app is None, "fastapi/httpx dependencies are not installed")
class ApiEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        run_all_dataset()
        run_score_fact()
        cls.client = TestClient(app)

    def test_health(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)

    def test_core_endpoints(self) -> None:
        urls = [
            "/api/geographies",
            "/api/geographies/search?q=06",
            "/api/geographies/06/profile",
            "/api/geographies/06/profile/tabs",
            "/api/metrics/06",
            "/api/scores/06",
            "/api/scores/_ranked?limit=3",
            "/api/scores/_features_bulk",
            "/api/scores/_delta?scenario_id=default-opportunity&baseline_scenario_id=default-opportunity&limit=3",
            "/api/recommendations/06",
            "/api/recommendations/06/explain",
            "/api/recommendations/distribution",
            "/api/trust/coverage",
            "/api/trust/geography/06",
            "/api/scenarios",
            "/api/system/status",
            "/api/tiles/manifest",
            "/api/compare/csv?geography_ids=06,06037",
        ]
        for url in urls:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200, msg=f"{url} -> {response.text}")

    def test_profile_tabs_structure(self) -> None:
        response = self.client.get("/api/geographies/06/profile/tabs")
        self.assertEqual(response.status_code, 200, msg=response.text)
        payload = response.json()
        for tab in ("overview", "talent", "industries", "education", "movement", "access", "recommendation"):
            self.assertIn(tab, payload, msg=f"missing tab: {tab}")
        self.assertIn("opportunity_score", payload["overview"])
        self.assertIn("talent_density", payload["talent"])
        self.assertIn("industry_specialization_lq", payload["industries"])
        self.assertIn("demand_supply_gap", payload["recommendation"])

    def test_features_bulk_structure(self) -> None:
        response = self.client.get("/api/scores/_features_bulk")
        self.assertEqual(response.status_code, 200, msg=response.text)
        payload = response.json()
        self.assertIsInstance(payload, dict)
        for geo_id, features in payload.items():
            self.assertIn("business_demand", features)
            self.assertIn("industry_fit", features)
            self.assertIn("talent_conversion", features)
            self.assertIn("demand_capture", features)
            break

    def test_scenario_simulation(self) -> None:
        response = self.client.post(
            "/api/scenarios/simulate",
            json={
                "weights": {
                    "business_demand": 0.25,
                    "talent_supply": 0.2,
                    "market_gap": 0.2,
                    "cost_efficiency": 0.15,
                    "execution_feasibility": 0.2,
                },
                "limit": 3,
                "geography_ids": ["06", "06037", "16980"],
            },
        )
        self.assertEqual(response.status_code, 200, msg=response.text)

    def test_scenario_simulation_invalid_weights(self) -> None:
        response = self.client.post(
            "/api/scenarios/simulate",
            json={
                "weights": {
                    "business_demand": 0.5,
                    "talent_supply": 0.2,
                    "market_gap": 0.2,
                    "cost_efficiency": 0.15,
                    "execution_feasibility": 0.2,
                },
                "limit": 3,
            },
        )
        self.assertEqual(response.status_code, 422)

    def test_compare_validation(self) -> None:
        response = self.client.post(
            "/api/compare",
            json={"geography_ids": ["06", "06"], "scenario_id": "default-opportunity"},
        )
        self.assertEqual(response.status_code, 400)

    def test_refresh_caches(self) -> None:
        response = self.client.post("/api/system/refresh-caches")
        self.assertEqual(response.status_code, 200)

    def test_system_status(self) -> None:
        response = self.client.get("/api/system/status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("api_key_status", payload)
        self.assertIn("cache_backend", payload)
        self.assertIn("artifacts", payload)

    def test_cleanup_artifacts_validation(self) -> None:
        bad = self.client.post("/api/system/cleanup-artifacts?keep_last_n=0")
        self.assertEqual(bad.status_code, 400)
        good = self.client.post("/api/system/cleanup-artifacts?keep_last_n=50")
        self.assertEqual(good.status_code, 200)

    def test_scenario_clone(self) -> None:
        suffix = uuid4().hex[:8]
        response = self.client.post(
            "/api/scenarios/default-opportunity/clone",
            json={
                "target_scenario_id": f"clone-{suffix}",
                "target_name": f"Clone {suffix}",
            },
        )
        self.assertEqual(response.status_code, 200, msg=response.text)

    def test_worktrigger_ingest_and_score(self) -> None:
        ingest_payload = {
            "source": "crunchbase",
            "signal_type": "funding_round",
            "account": {"domain": "acme-example.com", "name": "Acme Example"},
            "occurred_at": "2026-04-03T12:00:00Z",
            "payload": {"round_type": "Series B", "amount": 25000000, "geography_id": "06037"},
            "idempotency_key": "test-ingest-1",
        }
        ingest = self.client.post("/api/worktrigger/signals/ingest", json=ingest_payload)
        self.assertEqual(ingest.status_code, 200, msg=ingest.text)
        body = ingest.json()
        self.assertIn("account_id", body)
        account_id = body["account_id"]

        ingest_dup = self.client.post("/api/worktrigger/signals/ingest", json=ingest_payload)
        self.assertEqual(ingest_dup.status_code, 200, msg=ingest_dup.text)
        self.assertEqual(ingest_dup.json()["signal_id"], body["signal_id"])

        score = self.client.post(f"/api/worktrigger/accounts/{account_id}/score")
        self.assertEqual(score.status_code, 200, msg=score.text)
        scored = score.json()
        self.assertIn("priority_score", scored)
        self.assertIn("qualified", scored)

    def test_worktrigger_contacts_enrich(self) -> None:
        ingest = self.client.post(
            "/api/worktrigger/signals/ingest",
            json={
                "source": "commonroom",
                "signal_type": "web_visit",
                "account": {"domain": "contoso-example.com", "name": "Contoso Example"},
                "occurred_at": "2026-04-03T13:00:00Z",
                "payload": {"path": "/pricing"},
            },
        )
        self.assertEqual(ingest.status_code, 200, msg=ingest.text)
        account_id = ingest.json()["account_id"]
        enrich = self.client.post(
            f"/api/worktrigger/accounts/{account_id}/contacts/enrich",
            json={
                "contacts": [
                    {
                        "full_name": "Jane Doe",
                        "title": "COO",
                        "email": "jane@example.com",
                        "persona_type": "operations_buyer",
                        "confidence_score": 0.91,
                        "source": "apollo",
                    }
                ]
            },
        )
        self.assertEqual(enrich.status_code, 200, msg=enrich.text)
        out = enrich.json()
        self.assertGreaterEqual(out["contacts_found"], 1)
        self.assertTrue(out["best_contact_id"])


if __name__ == "__main__":
    unittest.main()
