import unittest
from datetime import datetime, timezone

from scripts.verify_grafana_queries import (
    EXPECTED_LEVEL_COUNTS,
    FIXED_KEYWORD,
    AcceptanceError,
    build_documents,
    extract_log_total,
    extract_series_total,
    extract_terms_counts,
    nearest_rank,
)


class GrafanaEvidenceTest(unittest.TestCase):
    def test_build_documents_matches_fixed_contract(self) -> None:
        documents = build_documents(
            "uc002-test", datetime(2026, 8, 5, 4, 0, tzinfo=timezone.utc)
        )

        self.assertEqual(16, len(documents))
        self.assertEqual(
            {"api-service", "worker-service"},
            {document["service.name"] for document in documents},
        )
        self.assertEqual(
            set(EXPECTED_LEVEL_COUNTS),
            {document["log.level"] for document in documents},
        )
        self.assertEqual(
            4, sum(FIXED_KEYWORD in document["message"] for document in documents)
        )
        self.assertEqual(16, len({document["event_id"] for document in documents}))
        self.assertTrue(all(len(document) == 14 for document in documents))

    def test_extract_log_total_accepts_empty_and_populated_frames(self) -> None:
        populated = {
            "results": {
                "A": {
                    "status": 200,
                    "frames": [
                        {"schema": {"meta": {"custom": {"total": 16}}}}
                    ],
                }
            }
        }
        empty = {"results": {"A": {"status": 200, "frames": []}}}

        self.assertEqual(16, extract_log_total(populated, "A"))
        self.assertEqual(0, extract_log_total(empty, "A"))

    def test_extract_terms_and_time_series_counts(self) -> None:
        response = {
            "results": {
                "LEVELS": {
                    "status": 200,
                    "frames": [
                        {
                            "schema": {
                                "fields": [
                                    {"name": "log.level"},
                                    {"name": "Count"},
                                ]
                            },
                            "data": {"values": [["INFO", "ERROR"], [12, 4]]},
                        }
                    ],
                },
                "ERROR": {
                    "status": 200,
                    "frames": [
                        {
                            "schema": {
                                "fields": [
                                    {"name": "Time", "type": "time"},
                                    {"name": "Value", "type": "number"},
                                ]
                            },
                            "data": {"values": [[1, 2, 3], [1, None, 3]]},
                        }
                    ],
                },
            }
        }

        self.assertEqual(
            {"INFO": 12, "ERROR": 4},
            extract_terms_counts(response, "LEVELS"),
        )
        self.assertEqual(4, extract_series_total(response, "ERROR"))

    def test_rejects_inconsistent_frame_columns(self) -> None:
        response = {
            "results": {
                "A": {
                    "status": 200,
                    "frames": [
                        {
                            "schema": {"fields": [{"name": "log.level"}]},
                            "data": {"values": [["INFO"], [1]]},
                        }
                    ],
                }
            }
        }

        with self.assertRaises(AcceptanceError):
            extract_terms_counts(response, "A")

    def test_nearest_rank_percentiles(self) -> None:
        samples = [0.9, 0.1, 0.5, 0.2, 0.8]

        self.assertEqual(0.5, nearest_rank(samples, 0.5))
        self.assertEqual(0.9, nearest_rank(samples, 0.95))


if __name__ == "__main__":
    unittest.main()
