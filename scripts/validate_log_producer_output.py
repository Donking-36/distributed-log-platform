#!/usr/bin/env python3
"""校验一次 log-producer 固定批次的逐行 JSON 输出。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def positive_count(value: str) -> int:
    """把数量参数转换为严格正整数。"""
    count = int(value)
    if count <= 0:
        raise argparse.ArgumentTypeError("count 必须大于零")
    return count


def positive_sequence(value: str) -> int:
    """把起始序号转换为严格正整数。"""
    sequence = int(value)
    if sequence <= 0:
        raise argparse.ArgumentTypeError("first-sequence 必须大于零")
    return sequence


def parse_args() -> argparse.Namespace:
    """读取调用方声明的期望批次契约。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--service", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--count", required=True, type=positive_count)
    parser.add_argument("--first-sequence", type=positive_sequence, default=1)
    return parser.parse_args()


def validate_event(
    event: Any,
    *,
    line_number: int,
    expected_sequence: int,
    expected_service: str,
    expected_run_id: str,
) -> list[str]:
    """校验单条事件中由日志源负责的字段和值。"""
    if not isinstance(event, dict):
        return [f"第 {line_number} 行顶层值不是 JSON 对象"]

    errors: list[str] = []
    sequence = event.get("event.sequence")
    if type(sequence) is not int:
        errors.append(f"第 {line_number} 行 event.sequence 不是整数")
        return errors

    if sequence != expected_sequence:
        errors.append(
            f"第 {line_number} 行序号为 {sequence}，期望 {expected_sequence}"
        )
    if event.get("service.name") != expected_service:
        errors.append(
            f"第 {line_number} 行 service.name 不等于 {expected_service}"
        )
    if event.get("test_run_id") != expected_run_id:
        errors.append(
            f"第 {line_number} 行 test_run_id 不等于本次批次"
        )
    if event.get("log.level") != "INFO":
        errors.append(f"第 {line_number} 行 log.level 不等于 INFO")
    if event.get("message") != f"log event {sequence}":
        errors.append(f"第 {line_number} 行 message 与序号不一致")
    if not isinstance(event.get("@timestamp"), str) or not event["@timestamp"]:
        errors.append(f"第 {line_number} 行缺少非空 @timestamp")

    return errors


def validate_output(
    path: Path,
    *,
    expected_service: str,
    expected_run_id: str,
    expected_count: int,
    first_sequence: int = 1,
) -> list[str]:
    """解析完整输出，并返回所有可操作的契约错误。"""
    lines = path.read_text(encoding="utf-8").splitlines()
    errors: list[str] = []
    if len(lines) != expected_count:
        errors.append(f"实际 {len(lines)} 行，期望 {expected_count} 行")

    for line_number, line in enumerate(lines, start=1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            errors.append(
                f"第 {line_number} 行不是合法 JSON：{error.msg}"
            )
            continue
        errors.extend(
            validate_event(
                event,
                line_number=line_number,
                expected_sequence=first_sequence + line_number - 1,
                expected_service=expected_service,
                expected_run_id=expected_run_id,
            )
        )

    return errors


def main() -> int:
    """执行命令行校验并输出适合保存为验收证据的摘要。"""
    args = parse_args()
    errors = validate_output(
        args.input,
        expected_service=args.service,
        expected_run_id=args.run_id,
        expected_count=args.count,
        first_sequence=args.first_sequence,
    )
    if errors:
        for error in errors[:20]:
            print(error, file=sys.stderr)
        if len(errors) > 20:
            print(f"另有 {len(errors) - 20} 个错误", file=sys.stderr)
        return 1

    print(
        f"{args.service}：{args.count} 条合法 JSON，"
        f"test_run_id={args.run_id}，序号从 {args.first_sequence} 连续"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
