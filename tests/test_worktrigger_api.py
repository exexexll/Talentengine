import unittest

try:
    from fastapi.testclient import TestClient

    from backend.app.main import app
except ModuleNotFoundError:  # pragma: no cover
    TestClient = None  # type: ignore[assignment]
    app = None  # type: ignore[assignment]


@unittest.skipIf(TestClient is None or app is None, "fastapi/httpx dependencies are not installed")
class WorkTriggerApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def test_ingest_idempotent(self) -> None:
        payload = {
            "source": "crunchbase",
            "signal_type": "funding_round",
            "account": {
                "domain": "idempotent-example.com",
                "name": "Idempotent Example",
                "linkedin_company_id": "lnk_12345",
                "crunchbase_uuid": "cb_12345",
                "headquarters_geo_id": "06037",
                "locations": [{"geography_id": "06037", "weight": 0.7}, {"geography_id": "06075", "weight": 0.3}],
            },
            "occurred_at": "2026-04-03T12:00:00Z",
            "payload": {"round_type": "Series B", "amount": 25000000},
            "idempotency_key": "idem-ingest-123",
        }
        first = self.client.post("/api/worktrigger/signals/ingest", json=payload)
        self.assertEqual(first.status_code, 200, msg=first.text)
        second = self.client.post("/api/worktrigger/signals/ingest", json=payload)
        self.assertEqual(second.status_code, 200, msg=second.text)
        self.assertEqual(first.json()["signal_id"], second.json()["signal_id"])

    def test_score_and_contact_enrich(self) -> None:
        ingest = self.client.post(
            "/api/worktrigger/signals/ingest",
            json={
                "source": "commonroom",
                "signal_type": "web_visit",
                "account": {"domain": "score-example.com", "name": "Score Example"},
                "occurred_at": "2026-04-03T13:00:00Z",
                "payload": {"path": "/pricing"},
            },
        )
        self.assertEqual(ingest.status_code, 200, msg=ingest.text)
        account_id = ingest.json()["account_id"]
        score = self.client.post(f"/api/worktrigger/accounts/{account_id}/score")
        self.assertEqual(score.status_code, 200, msg=score.text)
        self.assertIn("priority_score", score.json())
        self.assertIn("geo_attribution", score.json())

        enrich = self.client.post(
            f"/api/worktrigger/accounts/{account_id}/contacts/enrich",
            json={
                "contacts": [
                    {
                        "full_name": "Jane Doe",
                        "title": "COO",
                        "email": "jane@score-example.com",
                        "persona_type": "operations_buyer",
                        "confidence_score": 0.91,
                        "source": "apollo",
                    }
                ]
            },
        )
        self.assertEqual(enrich.status_code, 200, msg=enrich.text)
        self.assertGreaterEqual(enrich.json()["contacts_found"], 1)

    def test_job_queue_contracts(self) -> None:
        enqueue = self.client.post(
            "/api/worktrigger/jobs/enqueue",
            json={
                "job_type": "enrich_contacts",
                "payload": {"account_id": "acct_test"},
                "idempotency_key": "job-idem-1",
                "max_attempts": 2,
            },
        )
        self.assertEqual(enqueue.status_code, 200, msg=enqueue.text)
        job = enqueue.json()
        self.assertIn(job["status"], {"queued", "in_progress"})
        jid = job["job_id"]

        claim = self.client.post("/api/worktrigger/jobs/claim")
        self.assertEqual(claim.status_code, 200, msg=claim.text)
        claimed = claim.json()
        if claimed is not None:
            fail = self.client.post(
                f"/api/worktrigger/jobs/{claimed['job_id']}/fail?error_message=test-error"
            )
            self.assertEqual(fail.status_code, 200, msg=fail.text)

        dead = self.client.get("/api/worktrigger/jobs/dead-letter?limit=10")
        self.assertEqual(dead.status_code, 200, msg=dead.text)
        rows = dead.json()
        if rows:
            requeue = self.client.post(f"/api/worktrigger/jobs/dead-letter/{rows[0]['id']}/requeue?max_attempts=3")
            self.assertEqual(requeue.status_code, 200, msg=requeue.text)
            self.assertEqual(requeue.json()["status"], "queued")

    def test_account_detail_endpoint(self) -> None:
        ingest = self.client.post(
            "/api/worktrigger/signals/ingest",
            json={
                "source": "crunchbase",
                "signal_type": "funding_round",
                "account": {"domain": "detail-example.com", "name": "Detail Example", "headquarters_geo_id": "06037"},
                "occurred_at": "2026-04-03T13:00:00Z",
                "payload": {"amount": 5000000},
            },
        )
        self.assertEqual(ingest.status_code, 200, msg=ingest.text)
        account_id = ingest.json()["account_id"]
        _ = self.client.post(f"/api/worktrigger/accounts/{account_id}/score")
        detail = self.client.get(f"/api/worktrigger/accounts/{account_id}/detail")
        self.assertEqual(detail.status_code, 200, msg=detail.text)
        body = detail.json()
        self.assertIn("account", body)
        self.assertIn("signals", body)
        self.assertIn("geo_attribution", body)

    def test_compliance_and_reconciliation_endpoints(self) -> None:
        suppress = self.client.post(
            "/api/worktrigger/compliance/suppress?email=blocked@example.com&reason=test&source=unittest"
        )
        self.assertEqual(suppress.status_code, 200, msg=suppress.text)
        listing = self.client.get("/api/worktrigger/compliance/suppressions?limit=10")
        self.assertEqual(listing.status_code, 200, msg=listing.text)
        emails = [row["email"] for row in listing.json()]
        self.assertIn("blocked@example.com", emails)

        reconcile = self.client.get("/api/worktrigger/crm/reconcile?limit=10")
        self.assertEqual(reconcile.status_code, 200, msg=reconcile.text)
        self.assertIn("event_count", reconcile.json())

        analytics = self.client.get("/api/worktrigger/analytics/summary")
        self.assertEqual(analytics.status_code, 200, msg=analytics.text)
        self.assertIn("signals_total", analytics.json())
        self.assertIn("throughput_7d", analytics.json())
        self.assertIn("quality", analytics.json())
        self.assertIn("speed_hours", analytics.json())

        run_once = self.client.post("/api/worktrigger/worker/run-once")
        self.assertEqual(run_once.status_code, 200, msg=run_once.text)
        self.assertIn("status", run_once.json())

        heartbeats = self.client.get("/api/worktrigger/worker/heartbeats")
        self.assertEqual(heartbeats.status_code, 200, msg=heartbeats.text)

        consent_put = self.client.post(
            "/api/worktrigger/compliance/consent?email=blocked@example.com&channel=email&legal_basis=consent&status=granted&source=unittest"
        )
        self.assertEqual(consent_put.status_code, 200, msg=consent_put.text)
        consent_get = self.client.get("/api/worktrigger/compliance/consent?email=blocked@example.com&channel=email")
        self.assertEqual(consent_get.status_code, 200, msg=consent_get.text)

        delete_req = self.client.post(
            "/api/worktrigger/compliance/delete?requested_by=unittest&reason=privacy&email=temp-delete@example.com"
        )
        self.assertEqual(delete_req.status_code, 200, msg=delete_req.text)
        req_id = delete_req.json()["deletion_request_id"]
        delete_done = self.client.post(f"/api/worktrigger/compliance/delete/{req_id}/complete")
        self.assertEqual(delete_done.status_code, 200, msg=delete_done.text)

        retention_policy = self.client.post(
            "/api/worktrigger/compliance/retention/policy?entity_type=feedback_events&retention_days=30&enabled=true"
        )
        self.assertEqual(retention_policy.status_code, 200, msg=retention_policy.text)
        retention_apply = self.client.post("/api/worktrigger/compliance/retention/apply")
        self.assertEqual(retention_apply.status_code, 200, msg=retention_apply.text)

        ingest = self.client.post(
            "/api/worktrigger/signals/ingest",
            json={
                "source": "commonroom",
                "signal_type": "web_visit",
                "account": {"domain": "conflict-example.com", "name": "Conflict Example"},
                "occurred_at": "2026-04-03T13:00:00Z",
                "payload": {"path": "/pricing"},
            },
        )
        self.assertEqual(ingest.status_code, 200, msg=ingest.text)
        account_id = ingest.json()["account_id"]
        conflict_detect = self.client.post(
            f"/api/worktrigger/crm/conflicts/detect?account_id={account_id}&crm_company_name=Different Name"
        )
        self.assertEqual(conflict_detect.status_code, 200, msg=conflict_detect.text)

        conflict_list = self.client.get("/api/worktrigger/crm/conflicts?status=open&limit=10")
        self.assertEqual(conflict_list.status_code, 200, msg=conflict_list.text)
        rows = conflict_list.json()
        if rows:
            conflict_resolve = self.client.post(
                f"/api/worktrigger/crm/conflicts/{rows[0]['id']}/resolve?resolved_by=unittest&resolved_value=app"
            )
            self.assertEqual(conflict_resolve.status_code, 200, msg=conflict_resolve.text)

        llm_eval = self.client.get("/api/worktrigger/llm/evals?limit=10")
        self.assertEqual(llm_eval.status_code, 200, msg=llm_eval.text)
        self.assertIn("run_count", llm_eval.json())

        quote = self.client.post("/api/worktrigger/execution/quotes?opportunity_id=opp_demo")
        self.assertEqual(quote.status_code, 200, msg=quote.text)
        shortlist = self.client.post("/api/worktrigger/execution/shortlists?opportunity_id=opp_demo&geography_id=06037")
        self.assertEqual(shortlist.status_code, 200, msg=shortlist.text)
        staffing = self.client.post(
            "/api/worktrigger/execution/staffing?opportunity_id=opp_demo&state=scoped&owner_user_id=sdr1",
            json={"kickoff": True},
        )
        self.assertEqual(staffing.status_code, 200, msg=staffing.text)


if __name__ == "__main__":
    unittest.main()
