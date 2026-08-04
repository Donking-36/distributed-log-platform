#!/usr/bin/env bash
set -Eeuo pipefail

export LC_ALL=C

KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
KAFKA_CONSUMER_INTEGRATION_TIMEOUT="${KAFKA_CONSUMER_INTEGRATION_TIMEOUT:-90s}"
KAFKA_CONSUMER_TEST_RUN_ID="${KAFKA_CONSUMER_TEST_RUN_ID:-}"

readonly KAFKA_POD="kafka-0"
readonly KAFKA_CONTAINER="kafka"
readonly BOOTSTRAP_SERVER="localhost:9092"
readonly TOPICS_CLI="/opt/kafka/bin/kafka-topics.sh"
readonly PRODUCER_CLI="/opt/kafka/bin/kafka-console-producer.sh"
readonly GROUPS_CLI="/opt/kafka/bin/kafka-consumer-groups.sh"
readonly OFFSETS_CLI="/opt/kafka/bin/kafka-get-offsets.sh"
readonly KAFKA_CLI_HEAP_OPTS="-Xms32m -Xmx128m"

fail() {
  echo "$1" >&2
  exit 1
}

[[ "${KAFKA_CONSUMER_INTEGRATION_TIMEOUT}" =~ ^([1-9][0-9]*)s$ ]] ||
  fail "KAFKA_CONSUMER_INTEGRATION_TIMEOUT 必须是正整数秒，例如 90s"
readonly TIMEOUT_SECONDS="${BASH_REMATCH[1]}"

if [[ -z "${KAFKA_CONSUMER_TEST_RUN_ID}" ]]; then
  KAFKA_CONSUMER_TEST_RUN_ID="$(date -u +%Y%m%dt%H%M%Sz)-$$"
fi
[[ "${KAFKA_CONSUMER_TEST_RUN_ID}" =~ ^[a-z0-9][a-z0-9-]{0,63}$ ]] ||
  fail "KAFKA_CONSUMER_TEST_RUN_ID 只能包含小写字母、数字和连字符，且不超过 64 字符"

readonly RUN_ID="${KAFKA_CONSUMER_TEST_RUN_ID}"
readonly TOPIC="logs.integration.kafka-consumer.${RUN_ID}"
readonly GROUP_ID="integration.log-processor.${RUN_ID}"
readonly PIPELINE_GROUP_ID="integration.log-pipeline.${RUN_ID}"
readonly FIRST_VALUE='{"message":"{\"@timestamp\":\"2026-08-04T04:00:00Z\",\"event.sequence\":1,\"log.level\":\"INFO\",\"message\":\"pipeline integration first\",\"service.name\":\"api-service\",\"test_run_id\":\"pipeline-integration\"}","kubernetes":{"namespace":"stage3-logs","labels":{"service":"api-service"},"pod":{"name":"api-service-integration","uid":"pod-uid-pipeline-integration"}},"container":{"id":"container-id-pipeline-integration"},"log":{"offset":100,"file":{"path":"/var/log/containers/api-service-integration.log"}}}'
readonly SECOND_VALUE='{"message":"{\"@timestamp\":\"2026-08-04T04:00:01Z\",\"event.sequence\":2,\"log.level\":\"WARN\",\"message\":\"pipeline integration second\",\"service.name\":\"api-service\",\"test_run_id\":\"pipeline-integration\"}","kubernetes":{"namespace":"stage3-logs","labels":{"service":"api-service"},"pod":{"name":"api-service-integration","uid":"pod-uid-pipeline-integration"}},"container":{"id":"container-id-pipeline-integration"},"log":{"offset":101,"file":{"path":"/var/log/containers/api-service-integration.log"}}}'
readonly REMOTE_BINARY="/tmp/kafka-consumer-integration-${RUN_ID}.test"
readonly REMOTE_PIPELINE_BINARY="/tmp/kafka-pipeline-integration-${RUN_ID}.test"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

for command_name in "${KUBECTL}" go grep awk mktemp; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

actual_context="$(${KUBECTL} config current-context)"
[[ "${actual_context}" == "${KUBE_CONTEXT}" ]] ||
  fail "Kubernetes 上下文不匹配：实际 ${actual_context}，要求 ${KUBE_CONTEXT}"

pod_facts="$(${KUBECTL} \
  --context="${KUBE_CONTEXT}" \
  get pod "${KAFKA_POD}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.metadata.name}{"|"}{.status.phase}{"|"}{.status.containerStatuses[?(@.name=="kafka")].ready}')"
[[ "${pod_facts}" == "${KAFKA_POD}|Running|true" ]] ||
  fail "Kafka Pod 尚未就绪：${pod_facts:-缺失}"
"${repo_root}/scripts/verify-kafka-image.sh" pod

kafka_exec() {
  "${KUBECTL}" \
    --context="${KUBE_CONTEXT}" \
    exec \
    -n "${KUBE_NAMESPACE}" \
    -c "${KAFKA_CONTAINER}" \
    "${KAFKA_POD}" \
    -- \
    env \
    "KAFKA_HEAP_OPTS=${KAFKA_CLI_HEAP_OPTS}" \
    "KAFKA_GC_LOG_OPTS=" \
    "$@"
}

kafka_exec_stdin() {
  "${KUBECTL}" \
    --context="${KUBE_CONTEXT}" \
    exec \
    -i \
    -n "${KUBE_NAMESPACE}" \
    -c "${KAFKA_CONTAINER}" \
    "${KAFKA_POD}" \
    -- \
    env \
    "KAFKA_HEAP_OPTS=${KAFKA_CLI_HEAP_OPTS}" \
    "KAFKA_GC_LOG_OPTS=" \
    "$@"
}

exact_topic_regex() {
  local escaped="${1//./\\.}"
  printf '^%s$' "${escaped}"
}

topic_description() {
  local output
  output="$(kafka_exec \
    "${TOPICS_CLI}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --describe \
    --if-exists \
    --topic "$(exact_topic_regex "${TOPIC}")")" || return
  awk '/^[[:space:]]*Topic:/' <<<"${output}"
}

topic_id_from_description() {
  awk -F '\t' '
    /PartitionCount:/ {
      for (field = 1; field <= NF; field++) {
        value = $field
        sub(/^[[:space:]]+/, "", value)
        if (value ~ /^TopicId:[[:space:]]*/) {
          sub(/^TopicId:[[:space:]]*/, "", value)
          print value
          exit
        }
      }
    }
  '
}

group_state() {
  local group_id="$1"
  local groups
  groups="$(kafka_exec \
    "${GROUPS_CLI}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --list)" || return
  if grep -Fqx -- "${group_id}" <<<"${groups}"; then
    printf exists
  else
    printf absent
  fi
}

remote_binary_state() {
  local remote_path="$1"
  kafka_exec sh -c \
    'if [ -e "$1" ]; then printf exists; else printf absent; fi' \
    _ \
    "${remote_path}"
}

existing_topic="$(topic_description)"
[[ -z "${existing_topic}" ]] || fail "临时 Kafka topic 已存在，拒绝复用：${TOPIC}"
for candidate_group_id in "${GROUP_ID}" "${PIPELINE_GROUP_ID}"; do
  existing_group_state="$(group_state "${candidate_group_id}")"
  if [[ "${existing_group_state}" == "exists" ]]; then
    fail "临时 Kafka consumer group 已存在，拒绝复用：${candidate_group_id}"
  fi
  [[ "${existing_group_state}" == "absent" ]] ||
    fail "无法确认临时 Kafka consumer group 状态：${candidate_group_id}"
done

for candidate_remote_binary in "${REMOTE_BINARY}" "${REMOTE_PIPELINE_BINARY}"; do
  existing_remote_binary_state="$(remote_binary_state "${candidate_remote_binary}")"
  [[ "${existing_remote_binary_state}" == "absent" ]] ||
    fail "临时测试二进制路径已存在，拒绝覆盖：${candidate_remote_binary}"
done

umask 077
work_dir="$(mktemp -d)"
local_binary="${work_dir}/kafka-consumer-integration.test"
local_pipeline_binary="${work_dir}/kafka-pipeline-integration.test"
topic_create_attempted=false
remote_binary_attempted=false
remote_pipeline_binary_attempted=false
topic_id=""

cleanup_remote_binary() {
  local remote_path="$1"
  local current_state
  if ! kafka_exec rm -f -- "${remote_path}"; then
    echo "清理 Kafka 集成测试远端二进制失败：${remote_path}" >&2
    cleanup_failed=true
  elif ! current_state="$(remote_binary_state "${remote_path}")"; then
    echo "无法复核 Kafka 集成测试远端二进制：${remote_path}" >&2
    cleanup_failed=true
  elif [[ "${current_state}" != "absent" ]]; then
    echo "Kafka 集成测试远端二进制删除后仍存在：${remote_path}" >&2
    cleanup_failed=true
  fi
}

cleanup_group() {
  local group_id="$1"
  local current_state
  if ! current_state="$(group_state "${group_id}")"; then
    echo "无法查询 Kafka 集成测试 consumer group：${group_id}" >&2
    cleanup_failed=true
  elif [[ "${current_state}" == "exists" ]]; then
    if ! kafka_exec \
      "${GROUPS_CLI}" \
      --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --delete \
      --group "${group_id}" >/dev/null; then
      echo "清理 Kafka 集成测试 consumer group 失败：${group_id}" >&2
      cleanup_failed=true
    elif ! current_state="$(group_state "${group_id}")"; then
      echo "无法复核 Kafka 集成测试 consumer group：${group_id}" >&2
      cleanup_failed=true
    elif [[ "${current_state}" != "absent" ]]; then
      echo "Kafka 集成测试 consumer group 删除后仍存在：${group_id}" >&2
      cleanup_failed=true
    fi
  elif [[ "${current_state}" != "absent" ]]; then
    echo "Kafka 集成测试 consumer group 状态无效：${current_state}" >&2
    cleanup_failed=true
  fi
}

cleanup() {
  local exit_code=$?
  local cleanup_failed=false
  local current_description current_topic_id attempt
  local topic_query_failed=false
  local topic_deleted=false
  trap - EXIT
  set +e

  if [[ "${remote_binary_attempted}" == "true" ]]; then
    cleanup_remote_binary "${REMOTE_BINARY}"
  fi
  if [[ "${remote_pipeline_binary_attempted}" == "true" ]]; then
    cleanup_remote_binary "${REMOTE_PIPELINE_BINARY}"
  fi

  cleanup_group "${GROUP_ID}"
  cleanup_group "${PIPELINE_GROUP_ID}"

  if [[ "${topic_create_attempted}" == "true" ]]; then
    if ! current_description="$(topic_description)"; then
      echo "无法查询 Kafka 集成测试临时 topic：${TOPIC}" >&2
      cleanup_failed=true
    elif [[ -z "${current_description}" ]]; then
      topic_deleted=true
    elif [[ -z "${topic_id}" ]]; then
      echo "临时 Kafka topic 身份未确认，拒绝删除：${TOPIC}" >&2
      cleanup_failed=true
    else
      current_topic_id="$(topic_id_from_description <<<"${current_description}")"
      if [[ "${current_topic_id}" != "${topic_id}" ]]; then
        echo "临时 Kafka topic 身份已变化，拒绝删除：${TOPIC}" >&2
        cleanup_failed=true
      elif ! kafka_exec \
        "${TOPICS_CLI}" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --delete \
        --topic "$(exact_topic_regex "${TOPIC}")" >/dev/null; then
        echo "删除临时 Kafka topic 失败：${TOPIC}" >&2
        cleanup_failed=true
      else
        for ((attempt = 0; attempt < 30; attempt++)); do
          if ! current_description="$(topic_description)"; then
            topic_query_failed=true
            break
          fi
          if [[ -z "${current_description}" ]]; then
            topic_deleted=true
            break
          fi
          sleep 1
        done
        if [[ "${topic_query_failed}" == "true" ]]; then
          echo "删除后无法复核 Kafka 集成测试临时 topic：${TOPIC}" >&2
          cleanup_failed=true
        elif [[ "${topic_deleted}" != "true" ]]; then
          echo "临时 Kafka topic 删除后仍存在：${TOPIC}" >&2
          cleanup_failed=true
        fi
      fi
    fi
  fi

  rm -f -- "${local_binary}" "${local_pipeline_binary}"
  if ! rmdir -- "${work_dir}"; then
    echo "清理本地 Kafka 集成测试目录失败：${work_dir}" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == "true" && "${exit_code}" -eq 0 ]]; then
    exit_code=1
  fi
  if [[ "${exit_code}" -eq 0 ]]; then
    echo "Kafka 集成测试临时资源清理通过：topic、consumer groups、测试二进制均不存在"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

topic_create_attempted=true
kafka_exec \
  "${TOPICS_CLI}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --topic "${TOPIC}" \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=3600000 \
  --config min.insync.replicas=1 \
  >/dev/null

created_description="$(topic_description)"
topic_id="$(topic_id_from_description <<<"${created_description}")"
[[ "${topic_id}" =~ ^[A-Za-z0-9_-]+$ ]] ||
  fail "临时 Kafka topic ID 无效：${topic_id:-缺失}"
[[ "$(grep -c 'PartitionCount:' <<<"${created_description}")" -eq 1 ]] ||
  fail "临时 Kafka topic header 数量异常"
[[ "$(grep -c $'Partition: 0' <<<"${created_description}")" -eq 1 ]] ||
  fail "临时 Kafka topic 分区明细异常"

printf '%s\n%s\n' "${FIRST_VALUE}" "${SECOND_VALUE}" |
  kafka_exec_stdin \
    "${PRODUCER_CLI}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${TOPIC}" \
    --command-property acks=all

end_offset="$(kafka_exec \
  "${OFFSETS_CLI}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "$(exact_topic_regex "${TOPIC}")" |
  awk -F ':' -v topic="${TOPIC}" '$1 == topic && $2 == "0" { print $3 }')"
[[ "${end_offset}" == "2" ]] || fail "测试记录末端位点 = ${end_offset:-缺失}，期望 2"

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test \
  -c \
  -tags=integration \
  -o "${local_binary}" \
  ./internal/kafka

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test \
  -c \
  -tags=integration \
  -o "${local_pipeline_binary}" \
  ./internal/pipeline

remote_binary_attempted=true
"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  cp \
  -n "${KUBE_NAMESPACE}" \
  -c "${KAFKA_CONTAINER}" \
  "${local_binary}" \
  "${KAFKA_POD}:${REMOTE_BINARY}"

remote_pipeline_binary_attempted=true
"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  cp \
  -n "${KUBE_NAMESPACE}" \
  -c "${KAFKA_CONTAINER}" \
  "${local_pipeline_binary}" \
  "${KAFKA_POD}:${REMOTE_PIPELINE_BINARY}"
kafka_exec chmod 0700 "${REMOTE_BINARY}" "${REMOTE_PIPELINE_BINARY}"

kafka_exec \
  env \
  "KAFKA_TEST_BROKERS=${BOOTSTRAP_SERVER}" \
  "KAFKA_TEST_TOPIC=${TOPIC}" \
  "KAFKA_TEST_GROUP_ID=${GROUP_ID}" \
  "KAFKA_TEST_FIRST_VALUE=${FIRST_VALUE}" \
  "KAFKA_TEST_SECOND_VALUE=${SECOND_VALUE}" \
  "${REMOTE_BINARY}" \
  -test.v \
  -test.run '^TestConsumerResumesFromExplicitCommit$' \
  -test.timeout "${TIMEOUT_SECONDS}s"

kafka_exec \
  env \
  "KAFKA_TEST_BROKERS=${BOOTSTRAP_SERVER}" \
  "KAFKA_TEST_TOPIC=${TOPIC}" \
  "KAFKA_PIPELINE_GROUP_ID=${PIPELINE_GROUP_ID}" \
  "KAFKA_TEST_FIRST_VALUE=${FIRST_VALUE}" \
  "${REMOTE_PIPELINE_BINARY}" \
  -test.v \
  -test.run '^TestProcessorCommitsOriginalKafkaRecord$' \
  -test.timeout "${TIMEOUT_SECONDS}s"

group_description="$(kafka_exec \
  "${GROUPS_CLI}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --group "${GROUP_ID}")"
committed_offset="$(awk \
  -v group="${GROUP_ID}" \
  -v topic="${TOPIC}" \
  '$1 == group && $2 == topic && $3 == "0" { print $4 }' \
  <<<"${group_description}")"
[[ "${committed_offset}" == "2" ]] ||
  fail "consumer group 已提交下一位点 = ${committed_offset:-缺失}，期望 2"

pipeline_group_description="$(kafka_exec \
  "${GROUPS_CLI}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --group "${PIPELINE_GROUP_ID}")"
pipeline_committed_offset="$(awk \
  -v group="${PIPELINE_GROUP_ID}" \
  -v topic="${TOPIC}" \
  '$1 == group && $2 == topic && $3 == "0" { print $4 }' \
  <<<"${pipeline_group_description}")"
[[ "${pipeline_committed_offset}" == "1" ]] ||
  fail "pipeline consumer group 已提交下一位点 = ${pipeline_committed_offset:-缺失}，期望 1"

echo "Kafka 4.3.1 消费与 pipeline token 验证通过：run_id=${RUN_ID} topic_id=${topic_id} consumer_next=2 pipeline_next=1"
