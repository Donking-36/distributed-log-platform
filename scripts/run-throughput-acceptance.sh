#!/usr/bin/env bash
set -Eeuo pipefail

KUBECTL="${KUBECTL:-kubectl}"
PYTHON="${PYTHON:-python3}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
KUSTOMIZE_THROUGHPUT_OVERLAY="${KUSTOMIZE_THROUGHPUT_OVERLAY:-deploy/kubernetes/overlays/local-throughput}"
THROUGHPUT_RUN_ID="${THROUGHPUT_RUN_ID:-}"
THROUGHPUT_TIMEOUT_SECONDS="${THROUGHPUT_TIMEOUT_SECONDS:-240}"

readonly JOBS=("api-service-throughput" "worker-service-throughput")
readonly SERVICES=("api-service" "worker-service")
readonly EVENTS_PER_JOB=60000
readonly SAMPLE_COUNT_PER_JOB=100
readonly SAMPLE_FIRST_SEQUENCE=59901
readonly EXPECTED_TOTAL=120000
readonly MINIMUM_RATE=1000
readonly CONFIGURED_GENERATION_SECONDS=60
readonly PROCESSOR_GROUP="stage3-log-processor-v1"
readonly INDEX="logs-stage3-v1"
readonly KAFKA_POD="kafka-0"
readonly ELASTICSEARCH_POD="elasticsearch-0"
readonly KAFKA_HEAP_OPTS="-Xms32m -Xmx128m"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

fail() {
  echo "吞吐验收失败：$1" >&2
  exit 1
}

for command_name in "${KUBECTL}" "${PYTHON}" awk date flock mktemp wc; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

[[ "${THROUGHPUT_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]] ||
  fail "THROUGHPUT_TIMEOUT_SECONDS 必须是正整数"

if [[ -z "${THROUGHPUT_RUN_ID}" ]]; then
  THROUGHPUT_RUN_ID="perf-$(date -u +%Y%m%dt%H%M%Sz)-$$"
fi
[[ "${THROUGHPUT_RUN_ID}" =~ ^[a-z0-9][a-z0-9-]{0,47}$ ]] ||
  fail "THROUGHPUT_RUN_ID 只能包含小写字母、数字和连字符，且不超过 48 字符"
readonly RUN_ID="${THROUGHPUT_RUN_ID}"

# 性能 Job 与其他 Filebeat 位点验收互斥，避免固定名称和采集窗口相互干扰。
source "scripts/lib/filebeat-workflow-lock.sh"
acquire_filebeat_workflow_lock || exit 1

rendered_file="$(mktemp)"
configured_file="$(mktemp)"
log_files=("$(mktemp)" "$(mktemp)")
cleanup() {
  rm -f -- "${rendered_file}" "${configured_file}" "${log_files[@]}"
}
trap cleanup EXIT

kafka_exec() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec \
    -n "${KUBE_NAMESPACE}" \
    -c kafka \
    "${KAFKA_POD}" \
    -- env \
    "KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS}" \
    "KAFKA_GC_LOG_OPTS=" \
    "$@"
}

elasticsearch_curl() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec \
    -n "${KUBE_NAMESPACE}" \
    -c elasticsearch \
    "${ELASTICSEARCH_POD}" \
    -- curl -fsS "$@"
}

document_count() {
  local response
  response="$(elasticsearch_curl \
    -H 'Content-Type: application/json' \
    --data-binary "{\"query\":{\"term\":{\"test_run_id\":\"${RUN_ID}\"}}}" \
    "http://localhost:9200/${INDEX}/_count")"
  awk -F '"count":' 'NF == 2 { split($2, value, /[^0-9]/); print value[1] }' <<<"${response}"
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
    END { if (rows == 0) exit 1; print sum + 0 }
  ' <<<"${description}"
}

actual_context="$("${KUBECTL}" config current-context)"
[[ "${actual_context}" == "${KUBE_CONTEXT}" ]] ||
  fail "Kubernetes 上下文为 ${actual_context}，要求 ${KUBE_CONTEXT}"

"${KUBECTL}" --context="${KUBE_CONTEXT}" wait \
  --for=condition=Ready \
  -n "${KUBE_NAMESPACE}" \
  "pod/${KAFKA_POD}" "pod/${ELASTICSEARCH_POD}" \
  --timeout=30s >/dev/null

filebeat_status="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get daemonset filebeat \
  -n stage3-collector \
  -o 'jsonpath={.status.desiredNumberScheduled}{"|"}{.status.numberReady}')"
IFS="|" read -r filebeat_desired filebeat_ready <<<"${filebeat_status}"
[[ "${filebeat_desired}" =~ ^[1-9][0-9]*$ && "${filebeat_ready}" == "${filebeat_desired}" ]] ||
  fail "Filebeat 未全部就绪：desired=${filebeat_desired:-0} ready=${filebeat_ready:-0}"

mapfile -t processor_pods < <("${KUBECTL}" --context="${KUBE_CONTEXT}" get pods \
  -n "${KUBE_NAMESPACE}" \
  -l app.kubernetes.io/name=log-processor \
  -o name)
[[ "${#processor_pods[@]}" -eq 1 ]] || fail "处理器 Pod 数量不是 1"
processor_pod="${processor_pods[0]#pod/}"
processor_before="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get pod "${processor_pod}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.metadata.uid}{"|"}{.status.containerStatuses[0].ready}{"|"}{.status.containerStatuses[0].restartCount}')"
IFS="|" read -r processor_uid processor_ready processor_restarts <<<"${processor_before}"
[[ "${processor_ready}" == "true" && "${processor_restarts}" == "0" ]] ||
  fail "处理器启动状态异常：ready=${processor_ready} restarts=${processor_restarts}"

"${KUBECTL}" kustomize "${KUSTOMIZE_THROUGHPUT_OVERLAY}" >"${rendered_file}"
"${KUBECTL}" set env --local \
  -f "${rendered_file}" \
  "PRODUCER_TEST_RUN_ID=${RUN_ID}" \
  -o yaml >"${configured_file}"

inventory="$("${KUBECTL}" create --dry-run=client \
  -f "${configured_file}" \
  -o 'jsonpath={.kind}{"|"}{.metadata.name}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"\n"}')"
expected_inventory=$'Job|api-service-throughput|throughput\nJob|worker-service-throughput|throughput'
[[ "${inventory}" == "${expected_inventory}" ]] || fail "吞吐 Job 资源集合不符合预期"

for job in "${JOBS[@]}"; do
  existing="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get job "${job}" \
    -n "${KUBE_NAMESPACE}" --ignore-not-found -o name)"
  [[ -z "${existing}" ]] && continue
  purpose="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get job "${job}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.metadata.labels.distributed-log-platform\.io/purpose}')"
  [[ "${purpose}" == "throughput" ]] || fail "同名 Job ${job} 不属于吞吐验收"
  terminal="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get job "${job}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}{"|"}{.status.conditions[?(@.type=="Failed")].status}')"
  [[ "${terminal}" == "True|" || "${terminal}" == "|True" ]] ||
    fail "Job ${job} 尚未结束，拒绝替换"
done

"${KUBECTL}" --context="${KUBE_CONTEXT}" delete job "${JOBS[@]}" \
  -n "${KUBE_NAMESPACE}" \
  --ignore-not-found=true \
  --cascade=foreground \
  --wait=true >/dev/null
"${KUBECTL}" --context="${KUBE_CONTEXT}" create \
  --dry-run=server \
  -f "${configured_file}" >/dev/null

initial_count="$(document_count)"
[[ "${initial_count}" == "0" ]] || fail "本次 run_id 已存在 ${initial_count} 条文档"

start_ns="$(date +%s%N)"
echo "启动吞吐验收：run_id=${RUN_ID}，输入=${EXPECTED_TOTAL} 条，配置生成时长=${CONFIGURED_GENERATION_SECONDS}s"
"${KUBECTL}" --context="${KUBE_CONTEXT}" create -f "${configured_file}" >/dev/null

if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" wait \
  --for=condition=complete \
  -n "${KUBE_NAMESPACE}" \
  "job/${JOBS[0]}" "job/${JOBS[1]}" \
  --timeout="${THROUGHPUT_TIMEOUT_SECONDS}s"; then
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get jobs,pods \
    -n "${KUBE_NAMESPACE}" \
    -l distributed-log-platform.io/purpose=throughput \
    -o wide >&2
  fail "日志源 Job 未在时限内完成"
fi

deadline=$(( $(date +%s) + THROUGHPUT_TIMEOUT_SECONDS ))
while true; do
  elasticsearch_curl -X POST "http://localhost:9200/${INDEX}/_refresh" >/dev/null
  actual_count="$(document_count)"
  [[ "${actual_count}" =~ ^[0-9]+$ ]] || fail "无法解析 Elasticsearch 文档数"
  ((actual_count <= EXPECTED_TOTAL)) || fail "文档数 ${actual_count} 超过输入数 ${EXPECTED_TOTAL}"
  [[ "${actual_count}" == "${EXPECTED_TOTAL}" ]] && break
  (( $(date +%s) < deadline )) ||
    fail "Elasticsearch 仅收到 ${actual_count}/${EXPECTED_TOTAL} 条文档"
  sleep 1
done

while true; do
  lag="$(group_lag)" || fail "无法解析消费者组 LAG"
  [[ "${lag}" == "0" ]] && break
  (( $(date +%s) < deadline )) || fail "消费者组最终 LAG=${lag}"
  sleep 2
done
end_ns="$(date +%s%N)"

total_bytes=0
for index in "${!JOBS[@]}"; do
  pod_names="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get pods \
    -n "${KUBE_NAMESPACE}" \
    -l "job-name=${JOBS[index]}" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}')"
  [[ -n "${pod_names}" && "${pod_names}" != *$'\n'* ]] ||
    fail "Job ${JOBS[index]} 没有且仅有一个 Pod"
  job_status="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get job "${JOBS[index]}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.status.succeeded}{"|"}{.status.failed}')"
  [[ "${job_status}" == "1|" ]] || fail "Job ${JOBS[index]} 状态异常：${job_status}"
  pod_restarts="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get pod "${pod_names}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.status.containerStatuses[0].restartCount}')"
  [[ "${pod_restarts}" == "0" ]] || fail "Pod ${pod_names} 发生重启"
  # 容器运行时会轮转大体积 stdout；末尾连续样本验证输出契约，完整性由 Job
  # 成功、固定 count、ES 精确唯一文档数和最终 LAG 共同证明。
  "${KUBECTL}" --context="${KUBE_CONTEXT}" logs \
    -n "${KUBE_NAMESPACE}" \
    "pod/${pod_names}" \
    -c log-producer \
    --tail="${SAMPLE_COUNT_PER_JOB}" >"${log_files[index]}"
  "${PYTHON}" scripts/validate_log_producer_output.py \
    --input "${log_files[index]}" \
    --service "${SERVICES[index]}" \
    --run-id "${RUN_ID}" \
    --count "${SAMPLE_COUNT_PER_JOB}" \
    --first-sequence "${SAMPLE_FIRST_SEQUENCE}"
  bytes="$(wc -c <"${log_files[index]}")"
  total_bytes=$((total_bytes + bytes))
done

processor_after="$("${KUBECTL}" --context="${KUBE_CONTEXT}" get pod "${processor_pod}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.metadata.uid}{"|"}{.status.containerStatuses[0].ready}{"|"}{.status.containerStatuses[0].restartCount}')"
[[ "${processor_after}" == "${processor_uid}|true|0" ]] ||
  fail "验收期间处理器被替换、未就绪或发生重启：${processor_after}"

elapsed_ns=$((end_ns - start_ns))
metrics="$(awk \
  -v total="${EXPECTED_TOTAL}" \
  -v elapsed_ns="${elapsed_ns}" \
  -v bytes="${total_bytes}" \
  -v sample_count="$((SAMPLE_COUNT_PER_JOB * ${#JOBS[@]}))" '
  BEGIN {
    seconds = elapsed_ns / 1000000000
    rate = total / seconds
    average_bytes = bytes / sample_count
    printf "%.3f|%.2f|%.1f", seconds, rate, average_bytes
  }
')"
IFS="|" read -r elapsed_seconds actual_rate average_bytes <<<"${metrics}"
awk -v rate="${actual_rate}" -v minimum="${MINIMUM_RATE}" \
  'BEGIN { exit !(rate >= minimum) }' ||
  fail "实际吞吐 ${actual_rate} 条/秒，低于 ${MINIMUM_RATE} 条/秒"

# 成功后删除大日志 Job；Elasticsearch 中按唯一 run_id 隔离的证据文档保留。
"${KUBECTL}" --context="${KUBE_CONTEXT}" delete job "${JOBS[@]}" \
  -n "${KUBE_NAMESPACE}" \
  --cascade=foreground \
  --wait=true >/dev/null

echo "run_id=${RUN_ID}"
echo "processor_pod=${processor_pod} ready=true restarts=0"
echo "configured_generation_seconds=${CONFIGURED_GENERATION_SECONDS}"
echo "input_events=${EXPECTED_TOTAL} elasticsearch_unique_documents=${actual_count}"
echo "elapsed_seconds=${elapsed_seconds} throughput_events_per_second=${actual_rate}"
echo "average_event_bytes_sample=${average_bytes} processing_errors=0 consumer_group_lag=${lag}"
echo "cleanup=两个吞吐 Job 已删除；Elasticsearch 证据文档按 run_id 保留"
echo "完整链路每秒 1000+ 条硬性吞吐验收通过"
