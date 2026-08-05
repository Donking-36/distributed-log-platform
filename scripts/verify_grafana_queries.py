"""验证 Grafana 固定数据集查询、性能和 Pod 重建后的声明式恢复。"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


KUBECTL = os.environ.get("KUBECTL", "kubectl")
CONTEXT = os.environ.get("KUBE_CONTEXT", "stage3-logs")
NAMESPACE = os.environ.get("KUBE_NAMESPACE", "stage3-logs")
OVERLAY = os.environ.get(
    "KUSTOMIZE_GRAFANA_OVERLAY", "deploy/kubernetes/overlays/local-grafana"
)
RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,47}$")
FIXED_KEYWORD = "uc002-fixed-error"
EXPECTED_LEVEL_COUNTS = {"DEBUG": 4, "INFO": 4, "WARN": 4, "ERROR": 4}


class AcceptanceError(RuntimeError):
    """表示 UC-002 验收断言或外部命令失败。"""


def run(
    args: list[str], *, input_bytes: bytes | None = None, check: bool = True
) -> subprocess.CompletedProcess[bytes]:
    completed = subprocess.run(
        args, input=input_bytes, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if check and completed.returncode:
        output = (completed.stderr or completed.stdout).decode(
            "utf-8", errors="replace"
        )
        raise AcceptanceError(f"命令失败：{' '.join(args)}：{output.strip()}")
    return completed


def kube(*args: str, input_bytes: bytes | None = None) -> bytes:
    return run(
        [KUBECTL, f"--context={CONTEXT}", *args], input_bytes=input_bytes
    ).stdout


def object_from(raw: bytes, source: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise AcceptanceError(f"{source} 未返回合法 JSON：{error}") from error
    if not isinstance(value, dict):
        raise AcceptanceError(f"{source} 返回的 JSON 顶层不是对象")
    return value


def iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def build_documents(run_id: str, base_time: datetime) -> list[dict[str, Any]]:
    """生成两个服务、四个级别和两个时间窗口的 16 条完整契约文档。"""

    documents = []
    sequence = 0
    waves = (("old", -30), ("recent", -5))
    for wave, minutes in waves:
        for service in ("api-service", "worker-service"):
            for level in ("DEBUG", "INFO", "WARN", "ERROR"):
                sequence += 1
                message = f"uc002 fixture {service} {level.lower()} {wave}"
                if level == "ERROR":
                    message = f"{FIXED_KEYWORD} {service} {wave}"
                event_id = "sha256:" + hashlib.sha256(
                    f"{run_id}:{sequence}".encode()
                ).hexdigest()
                documents.append(
                    {
                        "@timestamp": iso8601(
                            base_time + timedelta(minutes=minutes)
                        ),
                        "event.sequence": sequence,
                        "event_id": event_id,
                        "message": message,
                        "log.level": level,
                        "service.name": service,
                        "test_run_id": run_id,
                        "kubernetes.namespace": NAMESPACE,
                        "kubernetes.pod.name": f"uc002-{service}-{wave}",
                        "kubernetes.pod.uid": f"uc002-pod-{service}-{wave}",
                        "container.id": f"uc002-container-{service}-{wave}",
                        "log.file.path": f"/var/log/containers/uc002-{service}-{wave}.log",
                        "log.offset": 800_000 + sequence,
                        "ingested_at": iso8601(base_time),
                    }
                )
    return documents


def bulk_body(index: str, documents: list[dict[str, Any]]) -> bytes:
    lines = []
    for document in documents:
        action = {"create": {"_index": index, "_id": document["event_id"]}}
        lines.append(json.dumps(action, separators=(",", ":")))
        lines.append(json.dumps(document, ensure_ascii=False, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode()


def es(
    method: str,
    path: str,
    *,
    body: bytes | None = None,
    content_type: str | None = None,
) -> dict[str, Any]:
    curl = [
        "curl",
        "-fsS",
        "--max-time",
        "30",
        "--request",
        method,
    ]
    if content_type:
        curl += ["--header", f"Content-Type: {content_type}"]
    if body is not None:
        curl += ["--data-binary", "@-"]
    curl.append(f"http://127.0.0.1:9200/{path.lstrip('/')}")
    raw = kube(
        "exec",
        "-i",
        "-n",
        NAMESPACE,
        "-c",
        "elasticsearch",
        "elasticsearch-0",
        "--",
        *curl,
        input_bytes=body,
    )
    return object_from(raw, f"Elasticsearch {path}")


def es_status(path: str) -> int:
    raw = kube(
        "exec",
        "-n",
        NAMESPACE,
        "-c",
        "elasticsearch",
        "elasticsearch-0",
        "--",
        "curl",
        "-sS",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        f"http://127.0.0.1:9200/{path.lstrip('/')}",
    ).decode()
    if not raw.isdigit():
        raise AcceptanceError(f"无法解析 Elasticsearch HTTP 状态：{raw}")
    return int(raw)


def grafana(path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    curl = ["curl", "-fsS", "--max-time", "20"]
    body = None
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        curl += ["--header", "Content-Type: application/json", "--data-binary", "@-"]
    curl.append(f"http://127.0.0.1:3000/{path.lstrip('/')}")
    raw = kube(
        "exec",
        "-i",
        "-n",
        NAMESPACE,
        "deployment/grafana",
        "--",
        *curl,
        input_bytes=body,
    )
    return object_from(raw, f"Grafana {path}")


def result_frames(response: dict[str, Any], ref_id: str) -> list[dict[str, Any]]:
    result = response.get("results", {}).get(ref_id)
    if not isinstance(result, dict) or result.get("status") != 200:
        raise AcceptanceError(f"Grafana 查询 {ref_id} 失败：{result}")
    frames = result.get("frames", [])
    if not isinstance(frames, list):
        raise AcceptanceError(f"Grafana 查询 {ref_id} 的 frames 不是数组")
    return frames


def extract_log_total(response: dict[str, Any], ref_id: str) -> int:
    frames = result_frames(response, ref_id)
    if not frames:
        return 0
    total = frames[0].get("schema", {}).get("meta", {}).get("custom", {}).get("total")
    if not isinstance(total, int) or isinstance(total, bool):
        raise AcceptanceError(f"Grafana 查询 {ref_id} 缺少整数 total")
    return total


def columns(frame: dict[str, Any]) -> dict[str, list[Any]]:
    fields = frame.get("schema", {}).get("fields", [])
    values = frame.get("data", {}).get("values", [])
    if len(fields) != len(values):
        raise AcceptanceError("Grafana frame 字段和值数量不一致")
    return {
        field["name"]: value
        for field, value in zip(fields, values, strict=True)
        if isinstance(field, dict) and isinstance(value, list)
    }


def extract_terms_counts(response: dict[str, Any], ref_id: str) -> dict[str, int]:
    frames = result_frames(response, ref_id)
    if len(frames) != 1:
        raise AcceptanceError(f"级别聚合要求 1 个 frame，实际 {len(frames)}")
    data = columns(frames[0])
    terms, counts = data.get("log.level"), data.get("Count")
    if terms is None or counts is None or len(terms) != len(counts):
        raise AcceptanceError("级别聚合缺少 log.level/Count 或列长度不一致")
    return {term: int(count) for term, count in zip(terms, counts, strict=True)}


def extract_series_total(response: dict[str, Any], ref_id: str) -> int:
    total = 0
    for frame in result_frames(response, ref_id):
        fields = frame.get("schema", {}).get("fields", [])
        values = frame.get("data", {}).get("values", [])
        numeric = [
            value
            for field, value in zip(fields, values, strict=True)
            if field.get("type") == "number"
        ]
        if len(numeric) != 1:
            raise AcceptanceError(
                f"趋势查询 {ref_id} 要求一个 number 列，实际 {len(numeric)}"
            )
        total += sum(int(value) for value in numeric[0] if value is not None)
    return total


def nearest_rank(values: list[float], percentile: float) -> float:
    if not values or percentile <= 0 or percentile > 1:
        raise ValueError("样本不能为空，percentile 必须在 (0, 1] 内")
    ordered = sorted(values)
    return ordered[math.ceil(percentile * len(ordered)) - 1]


def query_model(ref_id: str, kind: str, lucene: str) -> dict[str, Any]:
    model: dict[str, Any] = {
        "refId": ref_id,
        "datasource": {"type": "elasticsearch", "uid": "logs-stage3"},
        "query": lucene,
        "timeField": "@timestamp",
        "maxDataPoints": 100,
        "intervalMs": 1_000,
    }
    if kind == "logs":
        model.update(metrics=[{"id": "1", "type": "logs"}], bucketAggs=[])
    elif kind == "levels":
        model.update(
            metrics=[{"id": "1", "type": "count"}],
            bucketAggs=[
                {
                    "field": "log.level",
                    "id": "2",
                    "settings": {
                        "min_doc_count": "1",
                        "order": "desc",
                        "orderBy": "_count",
                        "size": "10",
                    },
                    "type": "terms",
                }
            ],
        )
    else:
        model.update(
            alias=kind,
            intervalMs=60_000,
            metrics=[{"id": "1", "type": "count"}],
            bucketAggs=[
                {
                    "field": "@timestamp",
                    "id": "2",
                    "settings": {
                        "interval": "auto",
                        "min_doc_count": "0",
                        "trimEdges": "0",
                    },
                    "type": "date_histogram",
                }
            ],
        )
    return model


def query(from_ms: int, to_ms: int, models: list[dict[str, Any]]) -> dict[str, Any]:
    return grafana(
        "api/ds/query",
        {"from": str(from_ms), "to": str(to_ms), "queries": models},
    )


def verify_provisioning() -> None:
    health = grafana("api/health")
    datasource = grafana("api/datasources/uid/logs-stage3/health")
    dashboard = grafana("api/dashboards/uid/stage3-logs-overview")
    if health.get("database") != "ok" or health.get("version") != "13.1.0":
        raise AcceptanceError(f"Grafana 健康响应异常：{health}")
    if datasource.get("status") != "OK":
        raise AcceptanceError(f"Grafana 数据源不健康：{datasource}")
    if dashboard.get("meta", {}).get("provisioned") is not True:
        raise AcceptanceError("Grafana 仪表盘不是文件预置资源")
    definition = dashboard.get("dashboard", {})
    titles = {panel.get("title") for panel in definition.get("panels", [])}
    variables = {
        item.get("name")
        for item in definition.get("templating", {}).get("list", [])
    }
    if not {"日志明细", "日志级别分布", "ERROR / WARN 趋势"}.issubset(titles):
        raise AcceptanceError("Grafana 仪表盘缺少必需面板")
    if not {"service", "keyword"}.issubset(variables):
        raise AcceptanceError("Grafana 仪表盘缺少必需变量")


def wait_for_provisioning() -> None:
    last_error: Exception | None = None
    for _ in range(30):
        try:
            verify_provisioning()
            return
        except AcceptanceError as error:
            last_error = error
            time.sleep(1)
    raise AcceptanceError(f"Grafana 未在 30 秒内恢复预置资源：{last_error}")


def assert_no_drift() -> None:
    completed = run(
        [KUBECTL, f"--context={CONTEXT}", "diff", "-k", OVERLAY], check=False
    )
    if completed.returncode:
        output = (completed.stdout + completed.stderr).decode(
            "utf-8", errors="replace"
        )
        raise AcceptanceError(f"Grafana overlay 与集群存在声明漂移：\n{output}")


def deployment_uid(expected: str | None = None) -> str:
    deployment = object_from(
        kube("get", "deployment/grafana", "-n", NAMESPACE, "-o", "json"),
        "Grafana Deployment",
    )
    metadata, spec = deployment["metadata"], deployment["spec"]
    uid = metadata.get("uid")
    if not uid or (expected and uid != expected):
        raise AcceptanceError("Grafana Deployment UID 缺失或发生变化")
    if spec.get("replicas") != 1 or metadata.get("ownerReferences"):
        raise AcceptanceError("Grafana Deployment 副本数或 owner 不符合要求")
    if metadata.get("labels", {}).get("app.kubernetes.io/part-of") != "distributed-log-platform":
        raise AcceptanceError("Grafana Deployment 项目标识不匹配")
    return uid


def ready_pod() -> tuple[str, str]:
    items = object_from(
        kube(
            "get",
            "pods",
            "-n",
            NAMESPACE,
            "-l",
            "app.kubernetes.io/name=grafana",
            "-o",
            "json",
        ),
        "Grafana Pods",
    ).get("items", [])
    active = [pod for pod in items if not pod["metadata"].get("deletionTimestamp")]
    if len(active) != 1:
        raise AcceptanceError(f"Grafana 活跃 Pod 数量为 {len(active)}，要求 1")
    status = active[0].get("status", {}).get("containerStatuses", [])
    if len(status) != 1 or not status[0].get("ready") or status[0].get("restartCount"):
        raise AcceptanceError(f"Grafana Pod 未 Ready 或发生重启：{status}")
    return active[0]["metadata"]["name"], active[0]["metadata"]["uid"]


def workflow_lock() -> Any:
    runtime = Path(f"/run/user/{os.getuid()}")
    directory = runtime if runtime.is_dir() and os.access(runtime, os.W_OK) else Path("/tmp")
    handle = (
        directory / f"distributed-log-platform-filebeat-workflow-{os.getuid()}.lock"
    ).open("a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        handle.close()
        raise AcceptanceError("已有另一个部署或验收流程正在运行") from error
    return handle


def verify_queries(run_id: str, base_time: datetime) -> dict[str, Any]:
    start = int((base_time - timedelta(minutes=40)).timestamp() * 1_000)
    recent = int((base_time - timedelta(minutes=10)).timestamp() * 1_000)
    end = int((base_time + timedelta(minutes=1)).timestamp() * 1_000)
    run_filter = f'test_run_id:"{run_id}"'
    response = query(
        start,
        end,
        [
            query_model("ALL", "logs", run_filter),
            query_model("API", "logs", f'{run_filter} AND service.name:"api-service"'),
            query_model(
                "WORKER", "logs", f'{run_filter} AND service.name:"worker-service"'
            ),
            query_model(
                "KEYWORD", "logs", f'{run_filter} AND message:"{FIXED_KEYWORD}"'
            ),
            query_model(
                "NONE", "logs", f'{run_filter} AND message:"uc002-does-not-exist"'
            ),
        ],
    )
    totals = {
        name.lower(): extract_log_total(response, name)
        for name in ("ALL", "API", "WORKER", "KEYWORD", "NONE")
    }
    expected = {"all": 16, "api": 8, "worker": 8, "keyword": 4, "none": 0}
    if totals != expected:
        raise AcceptanceError(f"Grafana 明细过滤计数不匹配：{totals}")
    recent_total = extract_log_total(
        query(recent, end, [query_model("RECENT", "logs", run_filter)]), "RECENT"
    )
    aggregates = query(
        start,
        end,
        [
            query_model("LEVELS", "levels", run_filter),
            query_model("ERROR", "ERROR", f"{run_filter} AND log.level:ERROR"),
            query_model("WARN", "WARN", f"{run_filter} AND log.level:WARN"),
        ],
    )
    levels = extract_terms_counts(aggregates, "LEVELS")
    error_total = extract_series_total(aggregates, "ERROR")
    warn_total = extract_series_total(aggregates, "WARN")
    if recent_total != 8 or levels != EXPECTED_LEVEL_COUNTS:
        raise AcceptanceError(f"时间或级别计数不匹配：recent={recent_total} levels={levels}")
    if sum(levels.values()) != totals["all"] or (error_total, warn_total) != (4, 4):
        raise AcceptanceError("图表与明细数量不一致")
    return {
        **totals,
        "recent": recent_total,
        "levels": levels,
        "error": error_total,
        "warn": warn_total,
        "start": start,
        "end": end,
        "filter": run_filter,
    }


def measure(evidence: dict[str, Any], samples: int) -> tuple[float, float]:
    durations = []
    models = [
        query_model(
            "API", "logs", f'{evidence["filter"]} AND service.name:"api-service"'
        ),
        query_model(
            "KEYWORD",
            "logs",
            f'{evidence["filter"]} AND message:"{FIXED_KEYWORD}"',
        ),
        query_model("LEVELS", "levels", evidence["filter"]),
    ]
    for _ in range(samples):
        started = time.perf_counter()
        response = query(evidence["start"], evidence["end"], models)
        durations.append(time.perf_counter() - started)
        if (
            extract_log_total(response, "API") != 8
            or extract_log_total(response, "KEYWORD") != 4
            or extract_terms_counts(response, "LEVELS") != EXPECTED_LEVEL_COUNTS
        ):
            raise AcceptanceError("性能采样期间查询结果漂移")
    return nearest_rank(durations, 0.5), nearest_rank(durations, 0.95)


def scale(replicas: int) -> None:
    kube("scale", "deployment/grafana", "-n", NAMESPACE, f"--replicas={replicas}")


def main() -> int:
    run_id = os.environ.get("GRAFANA_ACCEPTANCE_RUN_ID") or (
        f"uc002-{datetime.now(timezone.utc):%Y%m%dt%H%M%Sz}-{os.getpid()}"
    )
    samples_text = os.environ.get("GRAFANA_ACCEPTANCE_SAMPLES", "10")
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise AcceptanceError("run_id 只能包含小写字母、数字和连字符，且不超过 48 字符")
    if not samples_text.isdigit() or not 2 <= int(samples_text) <= 30:
        raise AcceptanceError("GRAFANA_ACCEPTANCE_SAMPLES 必须是 2～30 的整数")
    if run([KUBECTL, "config", "current-context"]).stdout.decode().strip() != CONTEXT:
        raise AcceptanceError(f"当前 Kubernetes 上下文不是 {CONTEXT}")

    lock = workflow_lock()
    index = f"logs-stage3-{run_id}"
    created = False
    restoring = False
    failure: Exception | None = None
    cleanup_errors = []
    evidence: dict[str, Any] = {}
    old_uid = new_uid = ""
    base_time = datetime.now(timezone.utc).replace(microsecond=0)
    try:
        assert_no_drift()
        verify_provisioning()
        deployment = deployment_uid()
        old_name, old_uid = ready_pod()
        if es("PUT", index).get("acknowledged") is not True:
            raise AcceptanceError("临时索引创建失败")
        created = True
        bulk = es(
            "POST",
            "_bulk?refresh=wait_for",
            body=bulk_body(index, build_documents(run_id, base_time)),
            content_type="application/x-ndjson",
        )
        statuses = [item.get("create", {}).get("status") for item in bulk.get("items", [])]
        if bulk.get("errors") is not False or statuses != [201] * 16:
            raise AcceptanceError(f"固定数据集 Bulk 写入失败：{statuses}")
        evidence = verify_queries(run_id, base_time)

        # 缩容会删除 Pod 和临时 SQLite，恢复后只能依靠仓库 ConfigMap 重新预置。
        restoring = True
        scale(0)
        kube(
            "wait",
            "--for=delete",
            f"pod/{old_name}",
            "-n",
            NAMESPACE,
            "--timeout=60s",
        )
        scale(1)
        kube(
            "rollout",
            "status",
            "deployment/grafana",
            "-n",
            NAMESPACE,
            "--timeout=180s",
        )
        wait_for_provisioning()
        _, new_uid = ready_pod()
        if new_uid == old_uid:
            raise AcceptanceError("Grafana Pod UID 在重建后没有变化")
        run(["make", "--no-print-directory", "k8s-grafana-runtime-check"])
        deployment_uid(deployment)
        assert_no_drift()
        restoring = False

        evidence = verify_queries(run_id, base_time)
        evidence["p50"], evidence["p95"] = measure(evidence, int(samples_text))
        if evidence["p95"] >= 1:
            raise AcceptanceError(f"Grafana 查询 p95={evidence['p95']:.3f}s，未达到 1 秒目标")
    except Exception as error:  # 必须先恢复共享状态，再向调用方报告原始错误。
        failure = error
    finally:
        if restoring:
            try:
                scale(1)
                kube(
                    "rollout",
                    "status",
                    "deployment/grafana",
                    "-n",
                    NAMESPACE,
                    "--timeout=180s",
                )
            except Exception as error:
                cleanup_errors.append(f"恢复 Grafana 失败：{error}")
        if created:
            try:
                if es("DELETE", index).get("acknowledged") is not True or es_status(index) != 404:
                    cleanup_errors.append("临时索引未精确删除")
            except Exception as error:
                cleanup_errors.append(f"删除临时索引失败：{error}")
        lock.close()

    if failure or cleanup_errors:
        messages = ([str(failure)] if failure else []) + cleanup_errors
        raise AcceptanceError("；".join(messages))
    samples = int(samples_text)
    print(f"run_id={run_id}")
    print(f"index={index} documents=16 cleanup=deleted")
    print(
        f"filters=all:{evidence['all']} api:{evidence['api']} worker:{evidence['worker']} "
        f"recent:{evidence['recent']} keyword:{evidence['keyword']} none:{evidence['none']}"
    )
    print(f"levels={json.dumps(evidence['levels'], sort_keys=True)}")
    print(f"trends=ERROR:{evidence['error']} WARN:{evidence['warn']}")
    print(
        f"latency_samples={samples} p50={evidence['p50']:.3f}s "
        f"p95={evidence['p95']:.3f}s"
    )
    print(f"recovery=old_pod_uid:{old_uid} new_pod_uid:{new_uid} provisioned:true")
    print("UC-002 Grafana 固定数据集查询与 Pod 重建恢复验收通过")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AcceptanceError as error:
        print(f"UC-002 Grafana 验收失败：{error}", file=sys.stderr)
        raise SystemExit(1) from error
