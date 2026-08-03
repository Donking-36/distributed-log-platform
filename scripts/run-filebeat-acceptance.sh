#!/usr/bin/env bash
set -Eeuo pipefail

readonly SERVICES=("api-service" "worker-service")
readonly JOBS=("api-service-acceptance" "worker-service-acceptance")
readonly TOPICS=(
  "logs.api-service"
  "logs.worker-service"
  "logs.unclassified"
  "logs.dlq"
)
readonly TOPIC_PARTITIONS=(3 3 1 1)
readonly EXPECTED_COUNT=20
readonly KAFKA_POD="kafka-0"
readonly KAFKA_CONTAINER="kafka"

KUBECTL="${KUBECTL:-kubectl}"
PYTHON="${PYTHON:-python3}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
FILEBEAT_NAMESPACE="${FILEBEAT_NAMESPACE:-stage3-collector}"
FILEBEAT_ACCEPTANCE_TIMEOUT="${FILEBEAT_ACCEPTANCE_TIMEOUT:-120s}"
FILEBEAT_ACCEPTANCE_SETTLE_SECONDS="${FILEBEAT_ACCEPTANCE_SETTLE_SECONDS:-5}"
FILEBEAT_ACCEPTANCE_MAX_RECORDS="${FILEBEAT_ACCEPTANCE_MAX_RECORDS:-5000}"
FILEBEAT_VERSION="${FILEBEAT_VERSION:-9.4.4}"

fail() {
  echo "$1" >&2
  exit 1
}

[[ "${FILEBEAT_ACCEPTANCE_TIMEOUT}" =~ ^([1-9][0-9]*)s$ ]] ||
  fail "FILEBEAT_ACCEPTANCE_TIMEOUT 必须是正整数秒，例如 120s"
readonly TIMEOUT_SECONDS="${BASH_REMATCH[1]}"
[[ "${FILEBEAT_ACCEPTANCE_SETTLE_SECONDS}" =~ ^[1-9][0-9]*$ ]] ||
  fail "FILEBEAT_ACCEPTANCE_SETTLE_SECONDS 必须是正整数"
[[ "${FILEBEAT_ACCEPTANCE_MAX_RECORDS}" =~ ^[1-9][0-9]*$ ]] ||
  fail "FILEBEAT_ACCEPTANCE_MAX_RECORDS 必须是正整数"

# 从任意工作目录调用时都回到仓库根目录，保证相对路径含义稳定。
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

for command_name in "${KUBECTL}" "${PYTHON}" flock grep readlink; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

if [[ -z "${ACCEPTANCE_RUN_ID:-}" ]]; then
  ACCEPTANCE_RUN_ID="uc001-filebeat-$(${PYTHON} -c 'import uuid; print(uuid.uuid4().hex)')"
fi
[[ "${ACCEPTANCE_RUN_ID}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$ ]] ||
  fail "ACCEPTANCE_RUN_ID 必须是 1..63 位 Kubernetes 标签安全字符"
export ACCEPTANCE_RUN_ID

# 固定名 Job、节点 fixture 和采集器重建共享同一把锁，避免证据窗口被并发流程改写。
source "${repo_root}/scripts/lib/filebeat-workflow-lock.sh"
acquire_filebeat_workflow_lock || exit 1
export FILEBEAT_WORKFLOW_LOCK_INHERITED=1

actual_context="$(${KUBECTL} config current-context)"
[[ "${actual_context}" == "${KUBE_CONTEXT}" ]] ||
  fail "Kubernetes 上下文不匹配：实际 ${actual_context}，要求 ${KUBE_CONTEXT}"

broker_status="$(${KUBECTL} --context="${KUBE_CONTEXT}" get statefulset kafka \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.spec.replicas}{"|"}{.status.readyReplicas}{"|"}{.status.currentReplicas}')"
[[ "${broker_status}" == "1|1|1" ]] || fail "Kafka 尚未就绪：${broker_status}"
filebeat_status="$(${KUBECTL} --context="${KUBE_CONTEXT}" get daemonset filebeat \
  -n "${FILEBEAT_NAMESPACE}" \
  -o 'jsonpath={.status.desiredNumberScheduled}{"|"}{.status.numberReady}{"|"}{.status.numberAvailable}')"
[[ "${filebeat_status}" == "1|1|1" ]] || fail "Filebeat 尚未就绪：${filebeat_status}"

umask 077
work_dir="$(mktemp -d)"
validator_stdout="${work_dir}/validator.out"
validator_stderr="${work_dir}/validator.err"
declare -a start_files=()
declare -a end_files=()
declare -a capture_files=()
declare -a source_files=()
declare -a pod_names=()
declare -a pod_uids=()
declare -A cursors=()

cleanup() {
  rm -f -- "${work_dir}"/*
  rmdir -- "${work_dir}"
}
trap cleanup EXIT

kafka_exec() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${KAFKA_POD}" \
    -n "${KUBE_NAMESPACE}" -c "${KAFKA_CONTAINER}" -- \
    env "KAFKA_HEAP_OPTS=-Xms32m -Xmx128m" "KAFKA_GC_LOG_OPTS=-Xlog:gc=off" "$@"
}

capture_offsets() {
  local topic="$1"
  local expected_partitions="$2"
  local output_file="$3"
  local matching_count

  kafka_exec /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "${topic}" >"${output_file}"

  matching_count="$(grep -Fc "${topic}:" "${output_file}" || true)"
  [[ "${matching_count}" -eq "${expected_partitions}" ]] ||
    fail "主题 ${topic} 位点清单异常：实际 ${matching_count} 个分区，要求 ${expected_partitions}"

  local partition
  for ((partition = 0; partition < expected_partitions; partition += 1)); do
    get_offset "${output_file}" "${topic}" "${partition}" >/dev/null
  done
}

get_offset() {
  local offset_file="$1"
  local topic="$2"
  local partition="$3"
  local prefix="${topic}:${partition}:"
  local matches=()
  local offset

  mapfile -t matches < <(grep -F "${prefix}" "${offset_file}" || true)
  [[ "${#matches[@]}" -eq 1 && "${matches[0]}" == "${prefix}"* ]] ||
    fail "主题 ${topic} 分区 ${partition} 位点缺失或重复"
  offset="${matches[0]#${prefix}}"
  [[ "${offset}" =~ ^[0-9]+$ ]] || fail "主题 ${topic} 分区 ${partition} 位点非法：${offset}"
  printf '%s' "${offset}"
}

# 每轮只消费上次游标到当前高水位，既不依赖消费者组，也不反复读取历史窗口。
consume_new_records() {
  local topic_index topic expected_partitions partition key cursor end count

  for topic_index in "${!TOPICS[@]}"; do
    topic="${TOPICS[topic_index]}"
    expected_partitions="${TOPIC_PARTITIONS[topic_index]}"
    capture_offsets "${topic}" "${expected_partitions}" "${end_files[topic_index]}"

    for ((partition = 0; partition < expected_partitions; partition += 1)); do
      key="${topic}|${partition}"
      cursor="${cursors[${key}]}"
      end="$(get_offset "${end_files[topic_index]}" "${topic}" "${partition}")"
      ((end >= cursor)) || fail "主题 ${topic} 分区 ${partition} 高水位倒退：${cursor} -> ${end}"
      count=$((end - cursor))
      ((count <= FILEBEAT_ACCEPTANCE_MAX_RECORDS)) ||
        fail "主题 ${topic} 分区 ${partition} 新增 ${count} 条，超过单轮安全上限 ${FILEBEAT_ACCEPTANCE_MAX_RECORDS}"
      if ((count == 0)); then
        continue
      fi

      kafka_exec /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "${topic}" \
        --partition "${partition}" \
        --offset "${cursor}" \
        --max-messages "${count}" \
        --timeout-ms 15000 \
        --command-property enable.auto.commit=false \
        --formatter-property print.partition=true \
        --formatter-property print.offset=true \
        --formatter-property print.key=true \
        --formatter-property print.value=true \
        >>"${capture_files[topic_index]}"
      cursors["${key}"]="${end}"
    done
  done
}

validator_command() {
  local args=(
    "${PYTHON}"
    scripts/validate_filebeat_kafka_output.py
    routes
    --run-id "${ACCEPTANCE_RUN_ID}"
    --count "${EXPECTED_COUNT}"
    --namespace "${KUBE_NAMESPACE}"
    --filebeat-version "${FILEBEAT_VERSION}"
  )
  local index

  for index in "${!TOPICS[@]}"; do
    args+=(--capture "${TOPICS[index]}=${capture_files[index]}")
  done
  for index in "${!SERVICES[@]}"; do
    args+=(
      --source "${SERVICES[index]}=${source_files[index]}"
      --pod "${SERVICES[index]}=${pod_names[index]},${pod_uids[index]}"
    )
  done
  "${args[@]}"
}

for topic_index in "${!TOPICS[@]}"; do
  start_files[topic_index]="${work_dir}/start-${topic_index}.txt"
  end_files[topic_index]="${work_dir}/end-${topic_index}.txt"
  capture_files[topic_index]="${work_dir}/capture-${topic_index}.txt"
  : >"${capture_files[topic_index]}"
  capture_offsets "${TOPICS[topic_index]}" "${TOPIC_PARTITIONS[topic_index]}" "${start_files[topic_index]}"

  for ((partition = 0; partition < TOPIC_PARTITIONS[topic_index]; partition += 1)); do
    cursors["${TOPICS[topic_index]}|${partition}"]="$(
      get_offset "${start_files[topic_index]}" "${TOPICS[topic_index]}" "${partition}"
    )"
  done
done

echo "启动 Filebeat 端到端验收：test_run_id=${ACCEPTANCE_RUN_ID}"
"${repo_root}/scripts/run-log-producer-acceptance.sh"

for service_index in "${!SERVICES[@]}"; do
  pod_output="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pods \
    -n "${KUBE_NAMESPACE}" -l "job-name=${JOBS[service_index]}" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"|"}{.metadata.uid}{"\n"}{end}')"
  pod_lines=()
  [[ -n "${pod_output}" ]] && mapfile -t pod_lines <<<"${pod_output}"
  [[ "${#pod_lines[@]}" -eq 1 ]] || fail "Job ${JOBS[service_index]} 必须精确关联 1 个 Pod"
  IFS='|' read -r pod_names[service_index] pod_uids[service_index] <<<"${pod_lines[0]}"
  [[ -n "${pod_names[service_index]}" && -n "${pod_uids[service_index]}" ]] ||
    fail "Job ${JOBS[service_index]} 的 Pod 身份不完整"

  source_files[service_index]="${work_dir}/source-${service_index}.log"
  "${KUBECTL}" --context="${KUBE_CONTEXT}" logs "${pod_names[service_index]}" \
    -n "${KUBE_NAMESPACE}" -c log-producer >"${source_files[service_index]}"
done

deadline=$((SECONDS + TIMEOUT_SECONDS))
while true; do
  consume_new_records
  if validator_command >"${validator_stdout}" 2>"${validator_stderr}"; then
    break
  else
    validator_status=$?
  fi

  if [[ "${validator_status}" -eq 1 ]]; then
    cat "${validator_stderr}" >&2
    fail "Filebeat Kafka 事件违反验收契约"
  fi
  [[ "${validator_status}" -eq 2 ]] || fail "Filebeat Kafka 校验器异常退出：${validator_status}"
  if ((SECONDS >= deadline)); then
    cat "${validator_stderr}" >&2
    fail "Filebeat 未在 ${FILEBEAT_ACCEPTANCE_TIMEOUT} 内收齐本次事件"
  fi
  sleep 2
done

# 收齐后继续观察一个短窗口，捕获延迟到达的重复或错误主题事件。
sleep "${FILEBEAT_ACCEPTANCE_SETTLE_SECONDS}"
consume_new_records
if ! validator_command >"${validator_stdout}" 2>"${validator_stderr}"; then
  cat "${validator_stderr}" >&2
  fail "Filebeat 稳定观察窗口出现重复冲突、缺失或错误路由"
fi

cat "${validator_stdout}"
for topic_index in "${!TOPICS[@]}"; do
  for ((partition = 0; partition < TOPIC_PARTITIONS[topic_index]; partition += 1)); do
    start="$(get_offset "${start_files[topic_index]}" "${TOPICS[topic_index]}" "${partition}")"
    key="${TOPICS[topic_index]}|${partition}"
    end="${cursors[${key}]}"
    echo "KAFKA_RANGE topic=${TOPICS[topic_index]} partition=${partition} start=${start} end=${end}"
  done
done
echo "Filebeat 端到端验收通过：test_run_id=${ACCEPTANCE_RUN_ID}"
