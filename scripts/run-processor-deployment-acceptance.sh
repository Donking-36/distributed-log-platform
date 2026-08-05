#!/usr/bin/env bash

set -Eeuo pipefail

KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
KUSTOMIZE_OVERLAY="${KUSTOMIZE_OVERLAY:-deploy/kubernetes/overlays/local}"
PROCESSOR_ACCEPTANCE_RUN_ID="${PROCESSOR_ACCEPTANCE_RUN_ID:-}"

readonly KAFKA_POD="kafka-0"
readonly KAFKA_CONTAINER="kafka"
readonly ELASTICSEARCH_POD="elasticsearch-0"
readonly ELASTICSEARCH_CONTAINER="elasticsearch"
readonly PROCESSOR_DEPLOYMENT="log-processor"
readonly PROCESSOR_GROUP="stage3-log-processor-v1"
readonly TOPIC="logs.api-service"
readonly INDEX="logs-stage3-v1"
readonly KAFKA_HEAP_OPTS="-Xms32m -Xmx128m"
readonly EVENT_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# 从任意工作目录调用时都回到仓库根目录，保证相对路径和 Make 入口稳定。
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

fail() {
  echo "UC-001B 部署验收失败：$1" >&2
  exit 1
}

if [[ -z "${PROCESSOR_ACCEPTANCE_RUN_ID}" ]]; then
  PROCESSOR_ACCEPTANCE_RUN_ID="uc001b-$(date -u +%Y%m%dt%H%M%Sz)-$$"
fi
[[ "${PROCESSOR_ACCEPTANCE_RUN_ID}" =~ ^[a-z0-9][a-z0-9-]{0,47}$ ]] ||
  fail "PROCESSOR_ACCEPTANCE_RUN_ID 只能包含小写字母、数字和连字符，且不超过 48 字符"

readonly RUN_ID="${PROCESSOR_ACCEPTANCE_RUN_ID}"
readonly DUPLICATE_TEST_RUN_ID="${RUN_ID}-duplicate"
readonly RECOVERY_TEST_RUN_ID="${RUN_ID}-recovery"
readonly DUPLICATE_TEMPLATE='{"message":"{\"@timestamp\":\"__EVENT_TIME__\",\"event.sequence\":900001,\"log.level\":\"INFO\",\"message\":\"uc001b duplicate fixture\",\"service.name\":\"api-service\",\"test_run_id\":\"__TEST_RUN_ID__\"}","kubernetes":{"namespace":"stage3-logs","labels":{"service":"api-service"},"pod":{"name":"uc001b-acceptance","uid":"pod-uid-uc001b-duplicate"}},"container":{"id":"container-id-uc001b-duplicate"},"log":{"offset":900001,"file":{"path":"/var/log/containers/uc001b-acceptance.log"}}}'
readonly RECOVERY_TEMPLATE='{"message":"{\"@timestamp\":\"__EVENT_TIME__\",\"event.sequence\":900002,\"log.level\":\"WARN\",\"message\":\"uc001b recovery fixture\",\"service.name\":\"api-service\",\"test_run_id\":\"__TEST_RUN_ID__\"}","kubernetes":{"namespace":"stage3-logs","labels":{"service":"api-service"},"pod":{"name":"uc001b-acceptance","uid":"pod-uid-uc001b-recovery"}},"container":{"id":"container-id-uc001b-recovery"},"log":{"offset":900002,"file":{"path":"/var/log/containers/uc001b-acceptance.log"}}}'
duplicate_with_run_id="${DUPLICATE_TEMPLATE//__TEST_RUN_ID__/${DUPLICATE_TEST_RUN_ID}}"
recovery_with_run_id="${RECOVERY_TEMPLATE//__TEST_RUN_ID__/${RECOVERY_TEST_RUN_ID}}"
readonly DUPLICATE_VALUE="${duplicate_with_run_id//__EVENT_TIME__/${EVENT_TIME}}"
readonly RECOVERY_VALUE="${recovery_with_run_id//__EVENT_TIME__/${EVENT_TIME}}"

for command_name in "${KUBECTL}" awk sed flock readlink make date; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

source "scripts/lib/filebeat-workflow-lock.sh"
acquire_filebeat_workflow_lock || exit 1

restore_replicas=false
original_deployment_uid=""
cleanup() {
  local exit_code=$?
  local cleanup_failed=false
  trap - EXIT
  set +e

  if [[ "${restore_replicas}" == "true" ]]; then
    "${KUBECTL}" --context="${KUBE_CONTEXT}" scale \
      deployment/"${PROCESSOR_DEPLOYMENT}" \
      -n "${KUBE_NAMESPACE}" \
      --replicas=1 >/dev/null || cleanup_failed=true
    "${KUBECTL}" --context="${KUBE_CONTEXT}" rollout status \
      deployment/"${PROCESSOR_DEPLOYMENT}" \
      -n "${KUBE_NAMESPACE}" \
      --timeout=180s >/dev/null || cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == "true" ]]; then
    echo "UC-001B 部署验收清理未能恢复 processor 单副本" >&2
    exit 1
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

kafka_exec() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec \
    -n "${KUBE_NAMESPACE}" \
    -c "${KAFKA_CONTAINER}" \
    "${KAFKA_POD}" \
    -- env \
    "KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS}" \
    "KAFKA_GC_LOG_OPTS=" \
    "$@"
}

produce_lines() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec -i \
    -n "${KUBE_NAMESPACE}" \
    -c "${KAFKA_CONTAINER}" \
    "${KAFKA_POD}" \
    -- env \
    "KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS}" \
    "KAFKA_GC_LOG_OPTS=" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${TOPIC}" \
    --command-property acks=all
}

elasticsearch_curl() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec \
    -n "${KUBE_NAMESPACE}" \
    -c "${ELASTICSEARCH_CONTAINER}" \
    "${ELASTICSEARCH_POD}" \
    -- curl -fsS "$@"
}

document_count() {
  local test_run_id="$1" response
  response="$(elasticsearch_curl \
    -H 'Content-Type: application/json' \
    --data-binary "{\"query\":{\"term\":{\"test_run_id\":\"${test_run_id}\"}}}" \
    "http://localhost:9200/${INDEX}/_count")"
  sed -n 's/.*"count":\([0-9][0-9]*\).*/\1/p' <<<"${response}"
}

wait_for_document_count() {
  local test_run_id="$1" expected="$2" attempt actual
  for ((attempt = 1; attempt <= 60; attempt++)); do
    elasticsearch_curl -X POST "http://localhost:9200/${INDEX}/_refresh" >/dev/null
    actual="$(document_count "${test_run_id}")"
    [[ "${actual}" =~ ^[0-9]+$ ]] || fail "无法解析 ${test_run_id} 的文档数"
    ((actual <= expected)) || fail "${test_run_id} 文档数为 ${actual}，超过期望 ${expected}"
    if [[ "${actual}" == "${expected}" ]]; then
      return
    fi
    sleep 1
  done
  fail "${test_run_id} 未在 60 秒内达到文档数 ${expected}"
}

group_lag() {
  local description
  description="$(kafka_exec \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group "${PROCESSOR_GROUP}")"
  awk -v group="${PROCESSOR_GROUP}" '
    $1 == group && $6 ~ /^[0-9]+$/ { sum += $6; rows++ }
    END { if (rows > 0) print sum; else print "missing" }
  ' <<<"${description}"
}

wait_for_zero_lag() {
  local attempt lag
  for ((attempt = 1; attempt <= 12; attempt++)); do
    lag="$(group_lag)"
    if [[ "${lag}" == "0" ]]; then
      return
    fi
    [[ "${lag}" =~ ^[0-9]+$ ]] || fail "无法解析 processor consumer group lag：${lag}"
    sleep 5
  done
  fail "processor consumer group 未在 60 秒内归零"
}

current_processor_pod() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get pods \
    -n "${KUBE_NAMESPACE}" \
    -l app.kubernetes.io/name=log-processor \
    -o 'jsonpath={.items[0].metadata.name}'
}

assert_no_overlay_drift() {
  local output status
  set +e
  output="$("${KUBECTL}" --context="${KUBE_CONTEXT}" diff -k "${KUSTOMIZE_OVERLAY}" 2>&1)"
  status=$?
  set -e
  if [[ "${status}" -ne 0 ]]; then
    printf '%s\n' "${output}" >&2
    fail "应用 overlay 与集群存在声明漂移"
  fi
}

deployment_facts() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get \
    deployment/"${PROCESSOR_DEPLOYMENT}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.metadata.uid}{"|"}{.spec.replicas}{"|"}{.metadata.labels.app\.kubernetes\.io/part-of}{"|"}{.metadata.ownerReferences}'
}

assert_deployment_restored() {
  local facts
  facts="$(deployment_facts)"
  [[ "${facts}" == "${original_deployment_uid}|1|distributed-log-platform|" ]] ||
    fail "processor Deployment 未精确恢复：${facts}"
  assert_no_overlay_drift
}

actual_context="$("${KUBECTL}" config current-context)"
[[ "${actual_context}" == "${KUBE_CONTEXT}" ]] ||
  fail "Kubernetes 上下文为 ${actual_context}，要求 ${KUBE_CONTEXT}"

assert_no_overlay_drift
initial_deployment_facts="$(deployment_facts)"
IFS='|' read -r original_deployment_uid original_replicas original_part_of original_owners \
  <<<"${initial_deployment_facts}"
[[ -n "${original_deployment_uid}" ]] || fail "processor Deployment UID 缺失"
[[ "${original_replicas}" == "1" ]] || fail "processor 副本数为 ${original_replicas}，要求 1"
[[ "${original_part_of}" == "distributed-log-platform" ]] || fail "processor Deployment 项目标识不匹配"
[[ -z "${original_owners}" ]] || fail "processor Deployment 不应存在 ownerReference"

old_pod="$(current_processor_pod)"
old_uid="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get pod "${old_pod}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.metadata.uid}')"
old_ready="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get pod "${old_pod}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.status.containerStatuses[0].ready}')"
[[ "${old_ready}" == "true" ]] || fail "旧 processor Pod 尚未 Ready"

wait_for_zero_lag

printf '%s\n%s\n' "${DUPLICATE_VALUE}" "${DUPLICATE_VALUE}" | produce_lines
wait_for_zero_lag
wait_for_document_count "${DUPLICATE_TEST_RUN_ID}" 1

duplicate_evidence="$(elasticsearch_curl \
  -H 'Content-Type: application/json' \
  --data-binary "{\"size\":2,\"_source\":[\"event_id\",\"test_run_id\"],\"query\":{\"term\":{\"test_run_id\":\"${DUPLICATE_TEST_RUN_ID}\"}}}" \
  "http://localhost:9200/${INDEX}/_search?filter_path=hits.total,hits.hits._id,hits.hits._source")"

# 从这里开始修改副本数；退出 trap 必须可接管并恢复声明值 1。
restore_replicas=true
current_deployment_facts="$(deployment_facts)"
[[ "${current_deployment_facts}" == "${original_deployment_uid}|1|distributed-log-platform|" ]] ||
  fail "写前 processor Deployment 身份或状态已变化：${current_deployment_facts}"

"${KUBECTL}" --context="${KUBE_CONTEXT}" scale \
  deployment/"${PROCESSOR_DEPLOYMENT}" \
  -n "${KUBE_NAMESPACE}" \
  --replicas=0 >/dev/null

scaled_replicas="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get \
  deployment/"${PROCESSOR_DEPLOYMENT}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.spec.replicas}')"
[[ "${scaled_replicas}" == "0" ]] || fail "processor 未缩容到 0"

termination_observation=""
for ((attempt = 1; attempt <= 20; attempt++)); do
  termination_observation="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get pod "${old_pod}" \
    -n "${KUBE_NAMESPACE}" \
    --ignore-not-found \
    -o 'jsonpath={.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  if [[ -z "${termination_observation}" || "${termination_observation}" == *'|False' ]]; then
    break
  fi
  sleep 0.2
done

"${KUBECTL}" --context="${KUBE_CONTEXT}" wait \
  --for=delete pod/"${old_pod}" \
  -n "${KUBE_NAMESPACE}" \
  --timeout=60s >/dev/null

printf '%s\n' "${RECOVERY_VALUE}" | produce_lines

"${KUBECTL}" --context="${KUBE_CONTEXT}" scale \
  deployment/"${PROCESSOR_DEPLOYMENT}" \
  -n "${KUBE_NAMESPACE}" \
  --replicas=1 >/dev/null

"${KUBECTL}" --context="${KUBE_CONTEXT}" rollout status \
  deployment/"${PROCESSOR_DEPLOYMENT}" \
  -n "${KUBE_NAMESPACE}" \
  --timeout=180s >/dev/null

new_pod="$(current_processor_pod)"
new_facts="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get pod "${new_pod}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.metadata.uid}{"|"}{.status.containerStatuses[0].ready}{"|"}{.status.containerStatuses[0].restartCount}{"|"}{.status.containerStatuses[0].imageID}')"
IFS='|' read -r new_uid new_ready new_restarts new_image_id <<<"${new_facts}"
[[ "${new_uid}" != "${old_uid}" ]] || fail "processor Pod UID 未变化"
[[ "${new_ready}" == "true" && "${new_restarts}" == "0" ]] ||
  fail "新 processor Pod 状态为 ready=${new_ready} restarts=${new_restarts}"

make --no-print-directory k8s-processor-runtime-check >/dev/null
wait_for_zero_lag
wait_for_document_count "${RECOVERY_TEST_RUN_ID}" 1

recovery_evidence="$(elasticsearch_curl \
  -H 'Content-Type: application/json' \
  --data-binary "{\"size\":1,\"_source\":[\"event_id\",\"test_run_id\"],\"query\":{\"term\":{\"test_run_id\":\"${RECOVERY_TEST_RUN_ID}\"}}}" \
  "http://localhost:9200/${INDEX}/_search?filter_path=hits.total,hits.hits._id,hits.hits._source")"

assert_deployment_restored
restore_replicas=false

echo "run_id=${RUN_ID}"
echo "duplicate_test_run_id=${DUPLICATE_TEST_RUN_ID}"
echo "duplicate_evidence=${duplicate_evidence}"
echo "old_pod=${old_pod} old_uid=${old_uid} termination=${termination_observation:-deleted-before-observation}"
echo "new_pod=${new_pod} new_uid=${new_uid} ready=${new_ready} restarts=${new_restarts} image_id=${new_image_id}"
echo "recovery_test_run_id=${RECOVERY_TEST_RUN_ID}"
echo "recovery_evidence=${recovery_evidence}"
echo "consumer_group_lag=$(group_lag)"
echo "cleanup=Deployment UID/副本数/owner、应用 overlay 和单 Pod 均已恢复；无临时 Kubernetes 资源"
echo "说明=两条带唯一 test_run_id 的 ES 证据文档保留；Kafka fixture 由 24 小时策略清理"
echo "UC-001B 部署幂等与 Pod 重建续读验收通过"
