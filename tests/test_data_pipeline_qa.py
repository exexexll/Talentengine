import unittest

from data_pipeline.qa import check_duplicate_metric_keys


class DuplicateMetricKeyTests(unittest.TestCase):
    def test_duplicate_metric_key_detected(self) -> None:
        rows = [
            {
                "source_snapshot_id": "BLS_OEWS-1",
                "geography_id": "06",
                "period": "2024",
                "metric_name": "target_occupation_employment",
            },
            {
                "source_snapshot_id": "BLS_OEWS-1",
                "geography_id": "06",
                "period": "2024",
                "metric_name": "target_occupation_employment",
            },
        ]
        errors = check_duplicate_metric_keys.run(rows)
        self.assertEqual(len(errors), 1)

    def test_unique_metric_keys_pass(self) -> None:
        rows = [
            {
                "source_snapshot_id": "BLS_OEWS-1",
                "geography_id": "06",
                "period": "2024",
                "metric_name": "target_occupation_employment",
            },
            {
                "source_snapshot_id": "BLS_OEWS-1",
                "geography_id": "06",
                "period": "2024",
                "metric_name": "occupation_median_wage",
            },
        ]
        errors = check_duplicate_metric_keys.run(rows)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
