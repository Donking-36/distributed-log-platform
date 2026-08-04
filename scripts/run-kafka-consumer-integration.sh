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
readonly FIRST_VALUE="kafka-consumer-${RUN_ID}-first"
readonly SECOND_VALUE="kafka-consumer-${RUN_ID}-second"
readonly REMOTE_BINARY="/tmp/kafka-consumer-integration-${RUN_ID}.test"

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
  local groups
  groups="$(kafka_exec \
    "${GROUPS_CLI}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --list)" || return
  if grep -Fqx -- "${GROUP_ID}" <<<"${groups}"; then
    printf exists
  else
    printf absent
  fi
}

remote_binary_state() {
  kafka_exec sh -c \
    'if [ -e "$1" ]; then printf exists; else printf absent; fi' \
    _ \
    "${REMOTE_BINARY}"
}

existing_topic="$(topic_description)"
[[ -z "${existing_topic}" ]] || fail "临时 Kafka topic 已存在，拒绝复用：${TOPIC}"
existing_group_state="$(group_state)"
if [[ "${existing_group_state}" == "exists" ]]; then
  fail "临时 Kafka consumer group 已存在，拒绝复用：${GROUP_ID}"
fi
[[ "${existing_group_state}" == "absent" ]] ||
  fail "无法确认临时 Kafka consumer group 状态：${GROUP_ID}"

existing_remote_binary_state="$(remote_binary_state)"
[[ "${existing_remote_binary_state}" == "absent" ]] ||
  fail "临时测试二进制路径已存在，拒绝覆盖：${REMOTE_BINARY}"

umask 077
work_dir="$(mktemp -d)"
local_binary="${work_dir}/kafka-consumer-integration.test"
topic_create_attempted=false
remote_binary_attempted=false
topic_id=""

cleanup() {
  local exit_code=$?
  local cleanup_failed=false
  local current_description current_group_state current_remote_binary_state current_topic_id attempt
  local topic_query_failed=false
  local topic_deleted=false
  trap - EXIT
  set +e

  if [[ "${remote_binary_attempted}" == "true" ]]; then
    if ! kafka_exec rm -f -- "${REMOTE_BINARY}"; then
      echo "清理 Kafka 集成测试远端二进制失败：${REMOTE_BINARY}" >&2
      cleanup_failed=true
    elif ! current_remote_binary_state="$(remote_binary_state)"; then
      echo "无法复核 Kafka 集成测试远端二进制：${REMOTE_BINARY}" >&2
      cleanup_failed=true
    elif [[ "${current_remote_binary_state}" != "absent" ]]; then
      echo "Kafka 集成测试远端二进制删除后仍存在：${REMOTE_BINARY}" >&2
      cleanup_failed=true
    fi
  fi

  if ! current_group_state="$(group_state)"; then
    echo "无法查询 Kafka 集成测试 consumer group：${GROUP_ID}" >&2
    cleanup_failed=true
  elif [[ "${current_group_state}" == "exists" ]]; then
    if ! kafka_exec \
      "${GROUPS_CLI}" \
      --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --delete \
      --group "${GROUP_ID}" >/dev/null; then
      echo "清理 Kafka 集成测试 consumer group 失败：${GROUP_ID}" >&2
      cleanup_failed=true
    elif ! current_group_state="$(group_state)"; then
      echo "无法复核 Kafka 集成测试 consumer group：${GROUP_ID}" >&2
      cleanup_failed=true
    elif [[ "${current_group_state}" != "absent" ]]; then
      echo "Kafka 集成测试 consumer group 删除后仍存在：${GROUP_ID}" >&2
      cleanup_failed=true
    fi
  elif [[ "${current_group_state}" != "absent" ]]; then
    echo "Kafka 集成测试 consumer group 状态无效：${current_group_state}" >&2
    cleanup_failed=true
  fi

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

  rm -f -- "${local_binary}"
  if ! rmdir -- "${work_dir}"; then
    echo "清理本地 Kafka 集成测试目录失败：${work_dir}" >&2
    cleanup_failed=true
  fi

  if [[ "${cleanup_failed}" == "true" && "${exit_code}" -eq 0 ]]; then
    exit_code=1
  fi
  if [[ "${exit_code}" -eq 0 ]]; then
    echo "Kafka 集成测试临时资源清理通过：topic、consumer group、测试二进制均不存在"
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

remote_binary_attempted=true
"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  cp \
  -n "${KUBE_NAMESPACE}" \
  -c "${KAFKA_CONTAINER}" \
  "${local_binary}" \
  "${KAFKA_POD}:${REMOTE_BINARY}"
kafka_exec chmod 0700 "${REMOTE_BINARY}"

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

echo "Kafka 4.3.1 消费续读验证通过：run_id=${RUN_ID} topic_id=${topic_id} first=0 repeated=0 resumed=1 committed_next=2"
