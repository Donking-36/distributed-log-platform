"""生产器逐行 JSON 校验器的单元测试。"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.validate_log_producer_output import validate_output


def event(sequence: int) -> dict[str, object]:
    """构造与 log-producer 输出一致的最小测试事件。"""
    return {
        "@timestamp": "2026-08-05T08:00:00Z",
        "event.sequence": sequence,
        "log.level": "INFO",
        "message": f"log event {sequence}",
        "service.name": "api-service",
        "test_run_id": "perf-test",
    }


class ValidateOutputTest(unittest.TestCase):
    """同时覆盖完整输出和容器日志轮转后的连续后缀。"""

    def validate(self, sequences: list[int], first_sequence: int) -> list[str]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.log"
            path.write_text(
                "".join(json.dumps(event(value)) + "\n" for value in sequences),
                encoding="utf-8",
            )
            return validate_output(
                path,
                expected_service="api-service",
                expected_run_id="perf-test",
                expected_count=len(sequences),
                first_sequence=first_sequence,
            )

    def test_accepts_complete_output(self) -> None:
        self.assertEqual(self.validate([1, 2, 3], 1), [])

    def test_accepts_rotated_suffix(self) -> None:
        self.assertEqual(self.validate([59999, 60000], 59999), [])

    def test_rejects_gap_in_rotated_suffix(self) -> None:
        errors = self.validate([59999, 60001], 59999)
        self.assertIn("第 2 行序号为 60001，期望 60000", errors)


if __name__ == "__main__":
    unittest.main()
