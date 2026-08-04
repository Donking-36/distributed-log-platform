#!/usr/bin/env python3
"""validate_filebeat_kafka_output 的标准库单元测试。"""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from scripts.validate_filebeat_kafka_output import main


class ValidateFilebeatKafkaOutputTest(unittest.TestCase):
    RUN_ID = "filebeat-e2e-20260803"
    VERSION = "9.4.4"
    NAMESPACE = "stage3-logs"
    PODS = {
        "api-service": ("api-acceptance-abc", "api-pod-uid"),
        "worker-service": ("worker-acceptance-def", "worker-pod-uid"),
    }

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _source_line(self, service: str, sequence: int) -> str:
        return json.dumps(
            {
                "@timestamp": "2026-08-03T08:00:00Z",
                "event.sequence": sequence,
                "log.level": "INFO",
                "message": f"{service} event {sequence}",
                "service.name": service,
                "test_run_id": self.RUN_ID,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def _outer(self, service: str, source_line: str, *, marker: str = "stable") -> str:
        pod_name, pod_uid = self.PODS[service]
        return json.dumps(
            {
                "agent": {"type": "filebeat", "version": self.VERSION},
                "container": {"id": f"container-{service}"},
                "input": {"type": "filestream"},
                "kubernetes": {
                    "container": {"name": "log-producer"},
                    "labels": {"service": service},
                    "namespace": self.NAMESPACE,
                    "pod": {"name": pod_name, "uid": pod_uid},
                },
                "log": {
                    "offset": 0,
                    "file": {
                        "path": f"/var/log/containers/{pod_name}_{self.NAMESPACE}_log-producer-deadbeef.log"
                    }
                },
                "message": source_line,
                "stream": "stdout",
                "test_marker": marker,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def _capture_line(
        self,
        service: str,
        sequence: int,
        offset: int,
        *,
        key: str | None = None,
        marker: str = "stable",
    ) -> str:
        _, pod_uid = self.PODS[service]
        source_line = self._source_line(service, sequence)
        outer = self._outer(service, source_line, marker=marker)
        return f"Partition:0\tOffset:{offset}\t{key or pod_uid}\t{outer}"

    def _write_fixture(
        self,
        captures: dict[str, list[str]],
        *,
        count: int = 2,
    ) -> list[str]:
        arguments: list[str] = ["routes"]
        for service in ("api-service", "worker-service"):
            source_path = self.root / f"{service}.source.log"
            source_path.write_text(
                "\n".join(self._source_line(service, sequence) for sequence in range(1, count + 1))
                + "\n",
                encoding="utf-8",
            )
            pod_name, pod_uid = self.PODS[service]
            arguments.extend(("--source", f"{service}={source_path}"))
            arguments.extend(("--pod", f"{service}={pod_name},{pod_uid}"))

        for index, (topic, lines) in enumerate(captures.items()):
            capture_path = self.root / f"capture-{index}.log"
            capture_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            arguments.extend(("--capture", f"{topic}={capture_path}"))

        arguments.extend(
            (
                "--run-id",
                self.RUN_ID,
                "--count",
                str(count),
                "--namespace",
                self.NAMESPACE,
                "--filebeat-version",
                self.VERSION,
            )
        )
        return arguments

    def _run(self, arguments: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = main(arguments)
        return status, stdout.getvalue(), stderr.getvalue()

    def _valid_captures(self) -> dict[str, list[str]]:
        return {
            "logs.api-service": [
                self._capture_line("api-service", 1, 10),
                self._capture_line("api-service", 2, 11),
            ],
            "logs.worker-service": [
                self._capture_line("worker-service", 1, 20),
                self._capture_line("worker-service", 2, 21),
            ],
            "logs.unclassified": [],
            "logs.dlq": [],
        }

    def test_success_allows_and_reports_identical_physical_duplicate(self) -> None:
        captures = self._valid_captures()
        captures["logs.api-service"].append(self._capture_line("api-service", 1, 12))

        status, stdout, stderr = self._run(self._write_fixture(captures))

        self.assertEqual(0, status)
        self.assertIn("4 条逻辑事件完整", stdout)
        self.assertIn("重复记录 1 条", stdout)
        self.assertIn("物理重复：api-service 序号 1", stderr)

    def test_returns_two_when_only_capture_is_incomplete(self) -> None:
        captures = self._valid_captures()
        captures["logs.worker-service"].pop()

        status, _, stderr = self._run(self._write_fixture(captures))

        self.assertEqual(2, status)
        self.assertIn("消息未收齐", stderr)
        self.assertIn("worker-service 缺少 2", stderr)

    def test_rejects_event_routed_to_wrong_topic(self) -> None:
        captures = self._valid_captures()
        misplaced = captures["logs.api-service"].pop(0)
        captures["logs.worker-service"].append(misplaced)

        status, _, stderr = self._run(self._write_fixture(captures))

        self.assertEqual(1, status)
        self.assertIn("路由错误", stderr)
        self.assertIn("必须进入 logs.api-service", stderr)

    def test_rejects_key_different_from_actual_pod_uid(self) -> None:
        captures = self._valid_captures()
        captures["logs.api-service"][0] = self._capture_line(
            "api-service", 1, 10, key="wrong-pod-uid"
        )

        status, _, stderr = self._run(self._write_fixture(captures))

        self.assertEqual(1, status)
        self.assertIn("Kafka key 应为实际 Pod UID", stderr)

    def test_rejects_missing_or_invalid_source_log_offset(self) -> None:
        mutations = {
            "missing": lambda outer: outer["log"].pop("offset"),
            "negative": lambda outer: outer["log"].__setitem__("offset", -1),
            "fractional": lambda outer: outer["log"].__setitem__("offset", 1.5),
            "boolean": lambda outer: outer["log"].__setitem__("offset", True),
        }

        for name, mutate in mutations.items():
            with self.subTest(name=name):
                captures = self._valid_captures()
                parts = captures["logs.api-service"][0].split("\t", 3)
                outer = json.loads(parts[3])
                mutate(outer)
                parts[3] = json.dumps(outer, separators=(",", ":"))
                captures["logs.api-service"][0] = "\t".join(parts)

                status, _, stderr = self._run(self._write_fixture(captures))

                self.assertEqual(1, status)
                self.assertIn("log.offset 必须是非负整数", stderr)

    def test_rejects_conflicting_duplicate(self) -> None:
        captures = self._valid_captures()
        captures["logs.api-service"].append(
            self._capture_line("api-service", 1, 12, marker="changed")
        )

        status, _, stderr = self._run(self._write_fixture(captures))

        self.assertEqual(1, status)
        self.assertIn("冲突重复", stderr)

    def test_fallback_accepts_stable_path_fingerprint_record(self) -> None:
        source_path = self.root / "fallback.source.log"
        source_raw = self._source_line("fallback-service", 1)
        source_path.write_text(source_raw + "\n", encoding="utf-8")
        expected_path = (
            "/var/log/containers/filebeat-fallback_stage3-logs_log-producer-deadbeef.log"
        )
        # 该常量按 Filebeat 的 `|field|value|` 指纹输入独立计算，防止遗漏尾部分隔符。
        expected_key = "442aabd9a140dfff3448acc23a926088baa9ef974c6a90280c02e464c6b2e57a"
        outer = json.dumps(
            {
                "agent": {"type": "filebeat", "version": self.VERSION},
                "fields": {"routing_reason": "kubernetes_metadata_missing"},
                "input": {"type": "filestream"},
                "log": {"file": {"path": expected_path}},
                "message": source_raw,
                "stream": "stdout",
            },
            separators=(",", ":"),
        )
        capture_path = self.root / "fallback.capture.log"
        capture_path.write_text(
            f"Partition:0\tOffset:7\t{expected_key}\t{outer}\n", encoding="utf-8"
        )

        status, stdout, stderr = self._run(
            [
                "fallback",
                "--capture",
                str(capture_path),
                "--source",
                str(source_path),
                "--run-id",
                self.RUN_ID,
                "--expected-path",
                expected_path,
                "--filebeat-version",
                self.VERSION,
            ]
        )

        self.assertEqual(0, status, stderr)
        self.assertIn("兜底验证通过", stdout)

    def test_fallback_rejects_fabricated_pod_uid(self) -> None:
        source_path = self.root / "fallback-invalid.source.log"
        source_raw = self._source_line("fallback-service", 1)
        source_path.write_text(source_raw + "\n", encoding="utf-8")
        expected_path = "/var/log/containers/fallback_stage3-logs_log-producer-id.log"
        expected_key = "c77a7b482349d19a7d991934cce24f64f8f4bba4796b34482705c1f78769ba32"
        outer = json.dumps(
            {
                "agent": {"type": "filebeat", "version": self.VERSION},
                "fields": {"routing_reason": "kubernetes_metadata_missing"},
                "input": {"type": "filestream"},
                "kubernetes": {"pod": {"uid": "fabricated"}},
                "log": {"file": {"path": expected_path}},
                "message": source_raw,
                "stream": "stdout",
            },
            separators=(",", ":"),
        )
        capture_path = self.root / "fallback-invalid.capture.log"
        capture_path.write_text(
            f"Partition:0\tOffset:9\t{expected_key}\t{outer}\n", encoding="utf-8"
        )

        status, _, stderr = self._run(
            [
                "fallback",
                "--capture",
                str(capture_path),
                "--source",
                str(source_path),
                "--run-id",
                self.RUN_ID,
                "--expected-path",
                expected_path,
                "--filebeat-version",
                self.VERSION,
            ]
        )

        self.assertEqual(1, status)
        self.assertIn("不应伪造 Kubernetes Pod UID", stderr)

    def _replay_arguments(self, captures: dict[str, list[str]]) -> list[str]:
        arguments = ["replay", "--run-id", self.RUN_ID]
        for index, (topic, lines) in enumerate(captures.items()):
            capture_path = self.root / f"replay-capture-{index}.log"
            capture_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
            arguments.extend(("--capture", f"{topic}={capture_path}"))
        return arguments

    def test_replay_accepts_window_without_old_run_id(self) -> None:
        captures = {topic: [] for topic in self._valid_captures()}

        status, stdout, stderr = self._run(self._replay_arguments(captures))

        self.assertEqual(0, status, stderr)
        self.assertIn("无回放验证通过", stdout)

    def test_replay_rejects_old_run_id_in_recovery_window(self) -> None:
        captures = self._valid_captures()

        status, _, stderr = self._run(self._replay_arguments(captures))

        self.assertEqual(1, status)
        self.assertIn("回放错误", stderr)
        self.assertIn(self.RUN_ID, stderr)

    def test_replay_rejects_malformed_capture_instead_of_reporting_absence(self) -> None:
        captures = {topic: [] for topic in self._valid_captures()}
        captures["logs.api-service"] = ["malformed-kafka-console-line"]

        status, _, stderr = self._run(self._replay_arguments(captures))

        self.assertEqual(1, status)
        self.assertIn("契约错误", stderr)
        self.assertIn("不符合", stderr)


if __name__ == "__main__":
    unittest.main()
