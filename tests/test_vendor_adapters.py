import unittest

try:
    from fastapi.testclient import TestClient
    from backend.app.main import app
except ModuleNotFoundError:
    TestClient = None  # type: ignore[assignment]
    app = None  # type: ignore[assignment]


@unittest.skipIf(TestClient is None or app is None, "fastapi/httpx dependencies are not installed")
class VendorAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def test_vendor_status_endpoint(self) -> None:
        resp = self.client.get("/api/worktrigger/vendors/status")
        self.assertEqual(resp.status_code, 200, msg=resp.text)
        body = resp.json()
        for vendor in ("clay", "commonroom", "crunchbase", "linkedin_sales_nav", "apollo", "findymail", "hunter", "openai", "resend", "hubspot"):
            self.assertIn(vendor, body, msg=f"Missing vendor: {vendor}")
            self.assertIn("configured", body[vendor])
            self.assertIn("env_var", body[vendor])

    def test_clay_webhook_normalizes(self) -> None:
        resp = self.client.post("/api/worktrigger/vendors/clay/webhook", json={
            "row": {
                "company_domain": "clay-test-co.com",
                "company_name": "Clay Test Co",
                "signal_type": "funding_round",
                "amount": 10000000,
            }
        })
        self.assertEqual(resp.status_code, 200, msg=resp.text)
        body = resp.json()
        self.assertIn("signal_id", body)
        self.assertIn("account_id", body)

    def test_commonroom_webhook_normalizes(self) -> None:
        resp = self.client.post("/api/worktrigger/vendors/commonroom/webhook", json={
            "activity": {
                "type": "page_view",
                "occurredAt": "2026-04-03T10:00:00Z",
                "url": "/pricing",
                "organization": {
                    "domain": "commonroom-test.com",
                    "name": "CR Test Co",
                },
                "actor": {"name": "Test User", "email": "test@commonroom-test.com"},
            }
        })
        self.assertEqual(resp.status_code, 200, msg=resp.text)
        body = resp.json()
        self.assertIn("signal_id", body)

    def test_contacts_enrich_waterfall_needs_account(self) -> None:
        resp = self.client.post("/api/worktrigger/vendors/contacts/enrich-waterfall?account_id=nonexistent_acct_123")
        self.assertEqual(resp.status_code, 404, msg=resp.text)

    def test_contacts_enrich_waterfall_for_existing_account(self) -> None:
        ingest = self.client.post("/api/worktrigger/signals/ingest", json={
            "source": "manual",
            "signal_type": "web_visit",
            "account": {"domain": "waterfall-test.com", "name": "Waterfall Test"},
            "occurred_at": "2026-04-03T12:00:00Z",
            "payload": {},
        })
        self.assertEqual(ingest.status_code, 200, msg=ingest.text)
        account_id = ingest.json()["account_id"]
        resp = self.client.post(
            f"/api/worktrigger/vendors/contacts/enrich-waterfall?account_id={account_id}&limit=3"
        )
        self.assertIn(resp.status_code, (200, 503), msg=resp.text)


if __name__ == "__main__":
    unittest.main()
