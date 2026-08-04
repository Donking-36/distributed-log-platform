#!/usr/bin/env python3
"""严格核对 Filebeat 写入 Kafka 的端到端事件契约。"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


EXPECTED_TOPICS = {
    "api-service": "logs.api-service",
    "worker-service": "logs.worker-service",
}
EXPECTED_SERVICES = tuple(EXPECTED_TOPICS)
EXPECTED_CAPTURE_TOPICS = frozenset(
    (*EXPECTED_TOPICS.values(), "logs.unclassified", "logs.dlq")
)
_MISSING = object()


class ContractError(ValueError):
    """表示命令参数或输入证据本身不符合验证契约。"""


class ContractArgumentParser(argparse.ArgumentParser):
    """把参数错误归入契约错误，避免与“消息未收齐”的退出码 2 混淆。"""

    def error(self, message: str) -> None:
        raise ContractError(message)


@dataclass(frozen=True)
class PodIdentity:
    name: str
    uid: str


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: str
    outer_raw: str
    outer: dict[str, Any]
    message_raw: str
    message: dict[str, Any]


@dataclass
class ValidationResult:
    errors: list[str]
    missing: list[tuple[str, int]]
    duplicate_counts: dict[tuple[str, int], int]
    physical_records: int


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须是正整数") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("必须是正整数")
    return parsed


def _build_parser() -> ContractArgumentParser:
    parser = ContractArgumentParser(
        description="核对 Filebeat 采集的 api/worker 日志与 Kafka 物理记录",
    )
    parser.add_argument(
        "--capture",
        action="append",
        required=True,
        metavar="TOPIC=PATH",
        help="Kafka console formatter 输出，可重复指定",
    )
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        metavar="SERVICE=PATH",
        help="log-producer 原始标准输出，每个服务指定一次",
    )
    parser.add_argument(
        "--pod",
        action="append",
        required=True,
        metavar="SERVICE=NAME,UID",
        help="验收 Pod 的实际名称和 UID，每个服务指定一次",
    )
    parser.add_argument("--run-id", required=True, help="本次验收唯一 test_run_id")
    parser.add_argument("--count", required=True, type=_positive_int, help="每个服务的预期事件数")
    parser.add_argument("--namespace", required=True, help="预期 Kubernetes 命名空间")
    parser.add_argument("--filebeat-version", required=True, help="预期 Filebeat 版本")
    return parser


def _build_fallback_parser() -> ContractArgumentParser:
    parser = ContractArgumentParser(
        description="核对 Kubernetes 元数据缺失日志的未分类兜底契约",
    )
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-path", required=True)
    parser.add_argument("--filebeat-version", required=True)
    return parser


def _build_replay_parser() -> ContractArgumentParser:
    parser = ContractArgumentParser(
        description="确认指定 Kafka 位点窗口没有回放旧验收批次",
    )
    parser.add_argument(
        "--capture",
        action="append",
        required=True,
        metavar="TOPIC=PATH",
        help="Kafka console formatter 输出，每个项目主题指定一次",
    )
    parser.add_argument("--run-id", required=True, help="不得在恢复窗口再次出现的旧批次")
    return parser


def _split_assignment(value: str, label: str) -> tuple[str, str]:
    key, separator, raw_value = value.partition("=")
    if not separator or not key or not raw_value:
        raise ContractError(f"{label} 必须使用非空的 NAME=VALUE 格式：{value!r}")
    return key, raw_value


def _parse_capture_arguments(values: Sequence[str]) -> dict[str, list[Path]]:
    captures: dict[str, list[Path]] = {}
    for value in values:
        topic, path = _split_assignment(value, "--capture")
        captures.setdefault(topic, []).append(Path(path))
    actual_topics = set(captures)
    if actual_topics != EXPECTED_CAPTURE_TOPICS:
        missing = sorted(EXPECTED_CAPTURE_TOPICS - actual_topics)
        extra = sorted(actual_topics - EXPECTED_CAPTURE_TOPICS)
        raise ContractError(
            "--capture 主题集合不正确："
            f"缺少 {missing or '无'}；存在未知主题 {extra or '无'}"
        )
    return captures


def _parse_source_arguments(values: Sequence[str]) -> dict[str, Path]:
    sources: dict[str, Path] = {}
    for value in values:
        service, path = _split_assignment(value, "--source")
        if service in sources:
            raise ContractError(f"--source 重复指定服务：{service}")
        sources[service] = Path(path)
    _require_exact_services("--source", sources)
    return sources


def _parse_pod_arguments(values: Sequence[str]) -> dict[str, PodIdentity]:
    pods: dict[str, PodIdentity] = {}
    for value in values:
        service, identity = _split_assignment(value, "--pod")
        name, separator, uid = identity.partition(",")
        if not separator or not name or not uid:
            raise ContractError(f"--pod 必须使用 SERVICE=NAME,UID 格式：{value!r}")
        if service in pods:
            raise ContractError(f"--pod 重复指定服务：{service}")
        pods[service] = PodIdentity(name=name, uid=uid)
    _require_exact_services("--pod", pods)
    return pods


def _require_exact_services(label: str, mapping: dict[str, Any]) -> None:
    actual = set(mapping)
    expected = set(EXPECTED_SERVICES)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        details: list[str] = []
        if missing:
            details.append(f"缺少 {', '.join(missing)}")
        if extra:
            details.append(f"存在未知服务 {', '.join(extra)}")
        raise ContractError(f"{label} 服务集合不正确：{'；'.join(details)}")


def _read_lines(path: Path) -> list[str]:
    try:
        # splitlines 只移除行终止符，保留消息正文中的空格，供逐字比较。
        return path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ContractError(f"无法读取 UTF-8 文件 {path}：{exc}") from exc


def _load_sources(
    paths: dict[str, Path], run_id: str, count: int
) -> tuple[dict[tuple[str, int], str], list[str]]:
    sources: dict[tuple[str, int], str] = {}
    errors: list[str] = []

    for service in EXPECTED_SERVICES:
        for line_number, raw_line in enumerate(_read_lines(paths[service]), start=1):
            if not raw_line:
                continue
            location = f"源日志 {paths[service]}:{line_number}"
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                errors.append(f"{location} 不是合法 JSON：{exc.msg}")
                continue
            if not isinstance(event, dict):
                errors.append(f"{location} 必须是 JSON 对象")
                continue
            if event.get("test_run_id") != run_id:
                continue
            if event.get("service.name") != service:
                errors.append(
                    f"{location} 的 service.name 应为 {service!r}，实际为 {event.get('service.name')!r}"
                )
                continue
            sequence = event.get("event.sequence")
            if isinstance(sequence, bool) or not isinstance(sequence, int):
                errors.append(f"{location} 的 event.sequence 必须是整数")
                continue
            if sequence < 1 or sequence > count:
                errors.append(f"{location} 的 event.sequence={sequence} 超出 1..{count}")
                continue
            identity = (service, sequence)
            if identity in sources:
                errors.append(f"{location} 与前一条源日志重复使用 {service} 序号 {sequence}")
                continue
            sources[identity] = raw_line

    for service in EXPECTED_SERVICES:
        actual = {sequence for current_service, sequence in sources if current_service == service}
        missing = sorted(set(range(1, count + 1)) - actual)
        if missing:
            errors.append(f"{service} 源日志序号不完整，缺少：{_format_sequences(missing)}")
    return sources, errors


def _parse_console_line(topic: str, raw_line: str, location: str) -> KafkaRecord | None:
    parts = raw_line.split("\t", 3)
    if len(parts) != 4:
        raise ContractError(
            f"{location} 不符合 Partition:N<TAB>Offset:N<TAB>key<TAB>outer-json 格式"
        )
    partition_text, offset_text, key, outer_raw = parts
    partition = _parse_prefixed_integer(partition_text, "Partition:", location)
    offset = _parse_prefixed_integer(offset_text, "Offset:", location)
    try:
        outer = json.loads(outer_raw)
    except json.JSONDecodeError as exc:
        raise ContractError(f"{location} 的 outer-json 非法：{exc.msg}") from exc
    if not isinstance(outer, dict):
        raise ContractError(f"{location} 的 outer-json 必须是 JSON 对象")

    message_raw = outer.get("message")
    if not isinstance(message_raw, str):
        return None
    try:
        message = json.loads(message_raw)
    except json.JSONDecodeError as exc:
        # 不属于本次 run-id 的历史消息不进入契约判断。
        if "test_run_id" in message_raw:
            raise ContractError(f"{location} 的 message 疑似业务事件但 JSON 非法：{exc.msg}") from exc
        return None
    if not isinstance(message, dict):
        return None
    return KafkaRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        outer_raw=outer_raw,
        outer=outer,
        message_raw=message_raw,
        message=message,
    )


def _parse_prefixed_integer(value: str, prefix: str, location: str) -> int:
    if not value.startswith(prefix):
        raise ContractError(f"{location} 缺少 {prefix} 前缀")
    raw_number = value[len(prefix) :]
    try:
        parsed = int(raw_number)
    except ValueError as exc:
        raise ContractError(f"{location} 的 {prefix[:-1]} 不是整数：{raw_number!r}") from exc
    if parsed < 0:
        raise ContractError(f"{location} 的 {prefix[:-1]} 不能为负数")
    return parsed


def _get_path(value: dict[str, Any], dotted_path: str) -> Any:
    current: Any = value
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _validate_record(
    record: KafkaRecord,
    sources: dict[tuple[str, int], str],
    pods: dict[str, PodIdentity],
    run_id: str,
    count: int,
    namespace: str,
    filebeat_version: str,
    errors: list[str],
) -> tuple[str, int] | None:
    if record.message.get("test_run_id") != run_id:
        return None

    service = record.message.get("service.name")
    sequence = record.message.get("event.sequence")
    location = f"{record.topic} 分区 {record.partition} 位点 {record.offset}"
    if service not in EXPECTED_TOPICS:
        errors.append(f"{location} 的 service.name 未知：{service!r}")
        return None
    if isinstance(sequence, bool) or not isinstance(sequence, int):
        errors.append(f"{location} 的 event.sequence 必须是整数")
        return None

    identity = (service, sequence)
    expected_topic = EXPECTED_TOPICS[service]
    if record.topic != expected_topic:
        errors.append(f"{location} 路由错误：{service} 必须进入 {expected_topic}")
    if sequence < 1 or sequence > count:
        errors.append(f"{location} 的 {service} 序号 {sequence} 超出 1..{count}")

    expected_source = sources.get(identity)
    if expected_source is None:
        errors.append(f"{location} 找不到 {service} 序号 {sequence} 的对应源日志")
    elif record.message_raw != expected_source:
        errors.append(f"{location} 的 message 与 {service} 序号 {sequence} 源日志不逐字一致")

    pod = pods[service]
    if record.key != pod.uid:
        errors.append(f"{location} 的 Kafka key 应为实际 Pod UID {pod.uid!r}，实际为 {record.key!r}")

    expected_fields = {
        "kubernetes.namespace": namespace,
        "kubernetes.container.name": "log-producer",
        "kubernetes.pod.name": pod.name,
        "kubernetes.pod.uid": pod.uid,
        "kubernetes.labels.service": service,
        "input.type": "filestream",
        "stream": "stdout",
        "agent.type": "filebeat",
        "agent.version": filebeat_version,
    }
    for path, expected in expected_fields.items():
        actual = _get_path(record.outer, path)
        if actual != expected:
            displayed = "<缺失>" if actual is _MISSING else repr(actual)
            errors.append(f"{location} 的 {path} 应为 {expected!r}，实际为 {displayed}")

    container_id = _get_path(record.outer, "container.id")
    if not isinstance(container_id, str) or not container_id:
        errors.append(f"{location} 缺少非空的 container.id")
    log_path = _get_path(record.outer, "log.file.path")
    expected_path_fragment = f"/{pod.name}_{namespace}_log-producer-"
    if (
        not isinstance(log_path, str)
        or expected_path_fragment not in log_path
        or not log_path.endswith(".log")
    ):
        errors.append(
            f"{location} 的 log.file.path 未指向实际验收 Pod：{log_path!r}"
        )
    # log.offset 是源日志位置并参与 event_id；不能用 Kafka record offset 代替。
    log_offset = _get_path(record.outer, "log.offset")
    if (
        isinstance(log_offset, bool)
        or not isinstance(log_offset, int)
        or log_offset < 0
    ):
        errors.append(f"{location} 的 log.offset 必须是非负整数，实际为 {log_offset!r}")
    return identity


def validate(
    captures: dict[str, list[Path]],
    source_paths: dict[str, Path],
    pods: dict[str, PodIdentity],
    run_id: str,
    count: int,
    namespace: str,
    filebeat_version: str,
) -> ValidationResult:
    sources, errors = _load_sources(source_paths, run_id, count)
    first_records: dict[tuple[str, int], KafkaRecord] = {}
    duplicate_counts: dict[tuple[str, int], int] = {}
    observed: set[tuple[str, int]] = set()
    physical_records = 0

    for topic, paths in captures.items():
        for path in paths:
            for line_number, raw_line in enumerate(_read_lines(path), start=1):
                if not raw_line:
                    continue
                location = f"Kafka 抓取 {path}:{line_number}"
                try:
                    record = _parse_console_line(topic, raw_line, location)
                except ContractError as exc:
                    errors.append(str(exc))
                    continue
                if record is None or record.message.get("test_run_id") != run_id:
                    continue
                physical_records += 1
                identity = _validate_record(
                    record,
                    sources,
                    pods,
                    run_id,
                    count,
                    namespace,
                    filebeat_version,
                    errors,
                )
                if identity is None:
                    continue
                observed.add(identity)
                previous = first_records.get(identity)
                if previous is None:
                    first_records[identity] = record
                    continue
                # Kafka 位点允许不同；主题、分区、key 和 value 必须逐字相同才是可接受的物理重复。
                previous_content = (
                    previous.topic,
                    previous.partition,
                    previous.key,
                    previous.outer_raw,
                )
                current_content = (record.topic, record.partition, record.key, record.outer_raw)
                if current_content == previous_content:
                    duplicate_counts[identity] = duplicate_counts.get(identity, 0) + 1
                else:
                    errors.append(
                        f"{location} 出现冲突重复：{identity[0]} 序号 {identity[1]} 与首次物理记录内容不一致"
                    )

    expected = {
        (service, sequence)
        for service in EXPECTED_SERVICES
        for sequence in range(1, count + 1)
    }
    for service in EXPECTED_SERVICES:
        partitions = {
            record.partition
            for identity, record in first_records.items()
            if identity[0] == service
        }
        if len(partitions) > 1:
            errors.append(
                f"{service} 的同一 Pod UID 被散列到多个分区：{sorted(partitions)}"
            )
    missing = sorted(expected - observed, key=lambda item: (EXPECTED_SERVICES.index(item[0]), item[1]))
    return ValidationResult(
        errors=errors,
        missing=missing,
        duplicate_counts=duplicate_counts,
        physical_records=physical_records,
    )


def _format_sequences(sequences: Sequence[int]) -> str:
    return ",".join(str(sequence) for sequence in sequences)


def _print_result(result: ValidationResult, count: int) -> int:
    for identity, duplicate_count in sorted(result.duplicate_counts.items()):
        print(
            f"物理重复：{identity[0]} 序号 {identity[1]} 额外出现 {duplicate_count} 条内容一致的记录",
            file=sys.stderr,
        )

    if result.errors:
        for error in result.errors:
            print(f"契约错误：{error}", file=sys.stderr)
        print(f"验证失败：共发现 {len(result.errors)} 项契约错误", file=sys.stderr)
        return 1

    if result.missing:
        grouped: dict[str, list[int]] = {service: [] for service in EXPECTED_SERVICES}
        for service, sequence in result.missing:
            grouped[service].append(sequence)
        details = "；".join(
            f"{service} 缺少 {_format_sequences(sequences)}"
            for service, sequences in grouped.items()
            if sequences
        )
        print(f"消息未收齐：{details}", file=sys.stderr)
        return 2

    logical_count = len(EXPECTED_SERVICES) * count
    duplicate_total = sum(result.duplicate_counts.values())
    print(
        f"验证通过：{logical_count} 条逻辑事件完整，读取 {result.physical_records} 条物理记录，"
        f"其中内容一致的重复记录 {duplicate_total} 条"
    )
    return 0


def _fallback_main(argv: Sequence[str]) -> int:
    try:
        args = _build_fallback_parser().parse_args(argv)
        source_lines = [line for line in _read_lines(args.source) if line]
        if len(source_lines) != 1:
            raise ContractError(f"兜底源日志必须精确包含 1 行，实际 {len(source_lines)} 行")
        source_raw = source_lines[0]
        source_event = json.loads(source_raw)
        if not isinstance(source_event, dict) or source_event.get("test_run_id") != args.run_id:
            raise ContractError("兜底源日志不是本次 run-id 的 JSON 对象")
    except (ContractError, json.JSONDecodeError) as exc:
        print(f"契约错误：{exc}", file=sys.stderr)
        return 1

    errors: list[str] = []
    matching_records: list[KafkaRecord] = []
    # Filebeat fingerprint 会在最后一个字段值后保留分隔符。
    expected_key = hashlib.sha256(
        f"|log.file.path|{args.expected_path}|".encode("utf-8")
    ).hexdigest()
    for line_number, raw_line in enumerate(_read_lines(args.capture), start=1):
        if not raw_line:
            continue
        location = f"Kafka 抓取 {args.capture}:{line_number}"
        try:
            record = _parse_console_line("logs.unclassified", raw_line, location)
        except ContractError as exc:
            errors.append(str(exc))
            continue
        if record is None or record.message.get("test_run_id") != args.run_id:
            continue
        matching_records.append(record)

        expected_fields = {
            "fields.routing_reason": "kubernetes_metadata_missing",
            "input.type": "filestream",
            "stream": "stdout",
            "agent.type": "filebeat",
            "agent.version": args.filebeat_version,
            "log.file.path": args.expected_path,
        }
        if record.key != expected_key:
            errors.append(
                f"{location} 的兜底 Kafka key 应为 {expected_key!r}，实际为 {record.key!r}"
            )
        if record.message_raw != source_raw:
            errors.append(f"{location} 的 message 与兜底源日志不逐字一致")
        for path, expected in expected_fields.items():
            actual = _get_path(record.outer, path)
            if actual != expected:
                displayed = "<缺失>" if actual is _MISSING else repr(actual)
                errors.append(f"{location} 的 {path} 应为 {expected!r}，实际为 {displayed}")
        pod_uid = _get_path(record.outer, "kubernetes.pod.uid")
        if pod_uid is not _MISSING:
            errors.append(f"{location} 不应伪造 Kubernetes Pod UID：{pod_uid!r}")

    if matching_records:
        first = matching_records[0]
        first_content = (first.partition, first.key, first.outer_raw)
        for record in matching_records[1:]:
            if (record.partition, record.key, record.outer_raw) != first_content:
                errors.append("兜底事件出现内容不一致的物理重复")

    if errors:
        for error in errors:
            print(f"契约错误：{error}", file=sys.stderr)
        return 1
    if not matching_records:
        print("消息未收齐：尚未看到本次未分类兜底事件", file=sys.stderr)
        return 2

    duplicate_count = len(matching_records) - 1
    print(
        "兜底验证通过：1 条逻辑事件进入 logs.unclassified，"
        f"稳定指纹 key 匹配，内容一致的重复记录 {duplicate_count} 条"
    )
    return 0


def _replay_main(argv: Sequence[str]) -> int:
    try:
        args = _build_replay_parser().parse_args(argv)
        if not args.run_id:
            raise ContractError("--run-id 不能为空")
        captures = _parse_capture_arguments(args.capture)
    except ContractError as exc:
        print(f"契约错误：{exc}", file=sys.stderr)
        return 1

    errors: list[str] = []
    replayed: list[KafkaRecord] = []
    for topic, paths in captures.items():
        for path in paths:
            for line_number, raw_line in enumerate(_read_lines(path), start=1):
                if not raw_line:
                    continue
                location = f"Kafka 抓取 {path}:{line_number}"
                try:
                    record = _parse_console_line(topic, raw_line, location)
                except ContractError as exc:
                    errors.append(str(exc))
                    continue
                if record is not None and record.message.get("test_run_id") == args.run_id:
                    replayed.append(record)

    if errors:
        for error in errors:
            print(f"契约错误：{error}", file=sys.stderr)
        return 1
    if replayed:
        for record in replayed:
            print(
                "回放错误："
                f"旧批次 {args.run_id!r} 再次出现在 {record.topic} "
                f"分区 {record.partition} 位点 {record.offset}",
                file=sys.stderr,
            )
        return 1

    print(f"无回放验证通过：恢复窗口未再次出现旧批次 {args.run_id}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "fallback":
        return _fallback_main(arguments[1:])
    if arguments and arguments[0] == "replay":
        return _replay_main(arguments[1:])
    if arguments and arguments[0] == "routes":
        arguments = arguments[1:]
    try:
        args = _build_parser().parse_args(arguments)
        if not args.run_id or not args.namespace or not args.filebeat_version:
            raise ContractError("--run-id、--namespace 和 --filebeat-version 不能为空")
        captures = _parse_capture_arguments(args.capture)
        source_paths = _parse_source_arguments(args.source)
        pods = _parse_pod_arguments(args.pod)
        result = validate(
            captures=captures,
            source_paths=source_paths,
            pods=pods,
            run_id=args.run_id,
            count=args.count,
            namespace=args.namespace,
            filebeat_version=args.filebeat_version,
        )
    except ContractError as exc:
        print(f"契约错误：{exc}", file=sys.stderr)
        return 1
    return _print_result(result, args.count)


if __name__ == "__main__":
    raise SystemExit(main())
