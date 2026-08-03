#!/usr/bin/env bash
set -Eeuo pipefail

readonly MODE="${1:-run}"
readonly JOB_NAME="kafka-topic-initializer"
readonly PURPOSE="topic-initialization"
readonly CONTAINER_NAME="kafka-topic-initializer"
readonly EXPECTED_TOPICS=(
  "logs.api-service"
  "logs.worker-service"
  "logs.unclassified"
  "logs.dlq"
)
readonly EXPECTED_PARTITIONS=(3 3 1 1)
readonly DIGEST_PATTERN='^sha256:[0-9a-f]{64}$'

KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
KUSTOMIZE_KAFKA_TOPICS_OVERLAY="${KUSTOMIZE_KAFKA_TOPICS_OVERLAY:-deploy/kubernetes/overlays/local-kafka-topics}"
KAFKA_TOPIC_INIT_TIMEOUT="${KAFKA_TOPIC_INIT_TIMEOUT:-300s}"
KAFKA_NODE_IMAGE="${KAFKA_NODE_IMAGE:-docker.io/apache/kafka:4.3.1}"
EXPECTED_KAFKA_CONFIG_DIGEST="${EXPECTED_KAFKA_CONFIG_DIGEST:-}"
readonly EXPECTED_JOB_IMAGE="${KAFKA_NODE_IMAGE#docker.io/}"

fail() {
  echo "$1" >&2
  exit 1
}

[[ "${MODE}" == "validate" || "${MODE}" == "run" ]] ||
  fail "用法：$0 validate|run"
[[ "${EXPECTED_KAFKA_CONFIG_DIGEST}" =~ ${DIGEST_PATTERN} ]] ||
  fail "EXPECTED_KAFKA_CONFIG_DIGEST 必须是完整的 sha256 摘要"
[[ "${KAFKA_TOPIC_INIT_TIMEOUT}" =~ ^([1-9][0-9]*)s$ ]] ||
  fail "KAFKA_TOPIC_INIT_TIMEOUT 必须是正整数秒，例如 300s"
readonly TIMEOUT_SECONDS="${BASH_REMATCH[1]}"

# 从任意工作目录调用时都回到仓库根目录，保证相对路径含义稳定。
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

for command_name in "${KUBECTL}" csplit flock grep; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

actual_context="$(${KUBECTL} config current-context)"
[[ "${actual_context}" == "${KUBE_CONTEXT}" ]] ||
  fail "Kubernetes 上下文不匹配：实际 ${actual_context}，要求 ${KUBE_CONTEXT}"

"${KUBECTL}" --context="${KUBE_CONTEXT}" get namespace "${KUBE_NAMESPACE}" >/dev/null
broker_status="$(${KUBECTL} \
  --context="${KUBE_CONTEXT}" \
  get statefulset kafka \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.spec.replicas}{"|"}{.status.readyReplicas}{"|"}{.status.currentReplicas}')"
[[ "${broker_status}" == "1|1|1" ]] ||
  fail "Kafka StatefulSet 尚未就绪：replicas|ready|current=${broker_status}"
"${KUBECTL}" --context="${KUBE_CONTEXT}" get service kafka -n "${KUBE_NAMESPACE}" >/dev/null

umask 077
work_dir="$(mktemp -d)"
rendered_file="${work_dir}/rendered.yaml"
preflight_job_file="${work_dir}/preflight-job.yaml"
log_file="${work_dir}/job.log"
config_map_file=""
job_file=""

cleanup() {
  rm -f -- "${work_dir}"/*.yaml "${log_file}"
  rmdir -- "${work_dir}"
}
trap cleanup EXIT

"${KUBECTL}" kustomize "${KUSTOMIZE_KAFKA_TOPICS_OVERLAY}" >"${rendered_file}"
csplit \
  --silent \
  --prefix="${work_dir}/resource-" \
  --suffix-format='%02d.yaml' \
  "${rendered_file}" \
  '/^---$/' \
  '{*}'

resource_count=0
for resource_file in "${work_dir}"/resource-*.yaml; do
  [[ -s "${resource_file}" ]] || continue
  ((resource_count += 1))
  inventory="$(${KUBECTL} \
    create \
    --dry-run=client \
    -f "${resource_file}" \
    -o 'jsonpath={.kind}{"|"}{.metadata.name}{"|"}{.metadata.namespace}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}')"
  IFS='|' read -r kind name namespace purpose <<<"${inventory}"
  [[ "${namespace}" == "${KUBE_NAMESPACE}" && "${purpose}" == "${PURPOSE}" ]] ||
    fail "主题初始化清单边界异常：${inventory}"

  case "${kind}" in
    ConfigMap)
      [[ -z "${config_map_file}" && "${name}" =~ ^kafka-topic-initializer-[a-z0-9]+$ ]] ||
        fail "主题初始化 ConfigMap 异常：${inventory}"
      config_map_file="${resource_file}"
      config_map_name="${name}"
      ;;
    Job)
      [[ -z "${job_file}" && "${name}" == "${JOB_NAME}" ]] ||
        fail "主题初始化 Job 异常：${inventory}"
      job_file="${resource_file}"
      ;;
    *)
      fail "主题初始化 overlay 包含越界资源：${inventory}"
      ;;
  esac
done

[[ "${resource_count}" -eq 2 && -n "${config_map_file}" && -n "${job_file}" ]] ||
  fail "主题初始化 overlay 必须只包含一个 ConfigMap 和一个 Job"

script_reference="$(${KUBECTL} \
  create \
  --dry-run=client \
  -f "${job_file}" \
  -o 'jsonpath={.spec.template.spec.volumes[?(@.name=="initializer-script")].configMap.name}')"
[[ "${script_reference}" == "${config_map_name}" ]] ||
  fail "Job 引用的脚本 ConfigMap 不匹配：${script_reference:-缺失}"

container_inventory="$(${KUBECTL} \
  create \
  --dry-run=client \
  -f "${job_file}" \
  -o 'jsonpath={range .spec.template.spec.containers[*]}{.name}{"|"}{.image}{"|"}{.imagePullPolicy}{"\n"}{end}')"
expected_container_inventory="${CONTAINER_NAME}|${EXPECTED_JOB_IMAGE}|Never"
[[ "${container_inventory}" == "${expected_container_inventory}" ]] ||
  fail "主题初始化 Job 容器身份异常：${container_inventory:-缺失}"
init_container_names="$(${KUBECTL} \
  create \
  --dry-run=client \
  -f "${job_file}" \
  -o 'jsonpath={range .spec.template.spec.initContainers[*]}{.name}{"\n"}{end}')"
[[ -z "${init_container_names}" ]] ||
  fail "主题初始化 Job 不允许包含 initContainer：${init_container_names}"
annotated_config_digest="$(${KUBECTL} \
  create \
  --dry-run=client \
  -f "${job_file}" \
  -o 'jsonpath={.spec.template.metadata.annotations.distributed-log-platform\.io/expected-config-digest}')"
[[ "${annotated_config_digest}" == "${EXPECTED_KAFKA_CONFIG_DIGEST}" ]] ||
  fail "主题初始化 Job 的镜像摘要注解不匹配：${annotated_config_digest:-缺失}"

"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  apply \
  --dry-run=server \
  -f "${config_map_file}" \
  >/dev/null

# 临时 generateName 让 API Server 在保留同名旧 Job 时也能校验完整 Pod 模板。
"${KUBECTL}" \
  patch \
  --local \
  -f "${job_file}" \
  --type=json \
  -p='[{"op":"add","path":"/metadata/generateName","value":"kafka-topic-initializer-preflight-"},{"op":"remove","path":"/metadata/name"}]' \
  -o yaml \
  >"${preflight_job_file}"
"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  create \
  --dry-run=server \
  -f "${preflight_job_file}" \
  >/dev/null

if [[ "${MODE}" == "validate" ]]; then
  echo "Kafka 主题初始化清单验证通过：1 ConfigMap + 1 Job，未写入集群"
  exit 0
fi

# 固定名 Job 不能并发重建；本地锁消除检查和删除之间的本机竞态。
lock_dir="/run/user/$(id -u)"
if [[ ! -d "${lock_dir}" || ! -w "${lock_dir}" ]]; then
  lock_dir="/tmp"
fi
exec 9>"${lock_dir}/distributed-log-platform-kafka-topic-initializer-$(id -u).lock"
flock -n 9 || fail "已有另一个 Kafka 主题初始化流程正在运行"

existing="$(${KUBECTL} \
  --context="${KUBE_CONTEXT}" \
  get job "${JOB_NAME}" \
  -n "${KUBE_NAMESPACE}" \
  --ignore-not-found \
  -o name)"
if [[ -n "${existing}" ]]; then
  existing_purpose="$(${KUBECTL} \
    --context="${KUBE_CONTEXT}" \
    get job "${JOB_NAME}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.metadata.labels.distributed-log-platform\.io/purpose}')"
  [[ "${existing_purpose}" == "${PURPOSE}" ]] ||
    fail "同名 Job 不属于本流程，拒绝删除：${JOB_NAME}"

  existing_terminal="$(${KUBECTL} \
    --context="${KUBE_CONTEXT}" \
    get job "${JOB_NAME}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}{"|"}{.status.conditions[?(@.type=="Failed")].status}')"
  [[ "${existing_terminal}" == "True|" || "${existing_terminal}" == "|True" ]] ||
    fail "同名 Job 尚未终止，拒绝替换：${JOB_NAME}"
  existing_uid="$(${KUBECTL} \
    --context="${KUBE_CONTEXT}" \
    get job "${JOB_NAME}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.metadata.uid}')"
fi

# Runner 自身在任何写操作前复用节点摘要门禁，不能依赖 Make 前置目标兜底。
"${repo_root}/scripts/verify-kafka-image.sh" node

# 新 ConfigMap 与旧终态 Job 可以并存；先写入脚本可避免删除证据后才发现写入失败。
"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  apply \
  -f "${config_map_file}"

if [[ -n "${existing}" ]]; then
  current_job_facts="$(${KUBECTL} \
    --context="${KUBE_CONTEXT}" \
    get job "${JOB_NAME}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.metadata.uid}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"|"}{.status.conditions[?(@.type=="Complete")].status}{"|"}{.status.conditions[?(@.type=="Failed")].status}')"
  [[ "${current_job_facts}" == "${existing_uid}|${PURPOSE}|True|" || \
    "${current_job_facts}" == "${existing_uid}|${PURPOSE}||True" ]] ||
    fail "同名 Job 在检查后发生变化，拒绝删除：${current_job_facts}"

  "${KUBECTL}" \
    --context="${KUBE_CONTEXT}" \
    delete job "${JOB_NAME}" \
    -n "${KUBE_NAMESPACE}" \
    --cascade=foreground \
    --wait=true
fi

"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  create \
  -f "${job_file}"

diagnostics() {
  set +e
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get jobs,pods \
    -n "${KUBE_NAMESPACE}" \
    -l "distributed-log-platform.io/purpose=${PURPOSE}" \
    -o wide >&2
  "${KUBECTL}" --context="${KUBE_CONTEXT}" describe job "${JOB_NAME}" \
    -n "${KUBE_NAMESPACE}" >&2
  "${KUBECTL}" --context="${KUBE_CONTEXT}" logs "job/${JOB_NAME}" \
    -n "${KUBE_NAMESPACE}" \
    -c "${CONTAINER_NAME}" >&2
  set -e
}

deadline=$((SECONDS + TIMEOUT_SECONDS))
while true; do
  terminal="$(${KUBECTL} \
    --context="${KUBE_CONTEXT}" \
    get job "${JOB_NAME}" \
    -n "${KUBE_NAMESPACE}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}{"|"}{.status.conditions[?(@.type=="Failed")].status}')"
  if [[ "${terminal}" == "True|" ]]; then
    break
  fi
  if [[ "${terminal}" == "|True" ]]; then
    diagnostics
    fail "Kafka 主题初始化 Job 执行失败"
  fi
  if ((SECONDS >= deadline)); then
    diagnostics
    fail "Kafka 主题初始化 Job 未在 ${KAFKA_TOPIC_INIT_TIMEOUT} 内完成"
  fi
  sleep 2
done

job_status="$(${KUBECTL} \
  --context="${KUBE_CONTEXT}" \
  get job "${JOB_NAME}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.status.succeeded}{"|"}{.status.failed}')"
IFS='|' read -r succeeded failed <<<"${job_status}"
[[ "${succeeded}" == "1" && "${failed:-0}" == "0" ]] ||
  fail "Job 状态异常：succeeded=${succeeded:-0} failed=${failed:-0}"

pod_output="$(${KUBECTL} \
  --context="${KUBE_CONTEXT}" \
  get pods \
  -n "${KUBE_NAMESPACE}" \
  -l "job-name=${JOB_NAME}" \
  -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}')"
pod_names=()
if [[ -n "${pod_output}" ]]; then
  mapfile -t pod_names <<<"${pod_output}"
fi
[[ "${#pod_names[@]}" -eq 1 ]] || fail "Job 关联 ${#pod_names[@]} 个 Pod，要求 1 个"
pod_name="${pod_names[0]}"

pod_status="$(${KUBECTL} \
  --context="${KUBE_CONTEXT}" \
  get pod "${pod_name}" \
  -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.status.phase}{"|"}{.status.containerStatuses[?(@.name=="kafka-topic-initializer")].restartCount}{"|"}{.status.containerStatuses[?(@.name=="kafka-topic-initializer")].imageID}{"|"}{.status.containerStatuses[?(@.name=="kafka-topic-initializer")].state.terminated.exitCode}')"
IFS='|' read -r phase restart_count runtime_image_id exit_code <<<"${pod_status}"
[[ "${phase}" == "Succeeded" && "${restart_count}" == "0" && "${exit_code}" == "0" ]] ||
  fail "Job Pod 状态异常：phase=${phase} restarts=${restart_count} exit=${exit_code}"
[[ "${runtime_image_id}" =~ (sha256:[0-9a-f]{64})$ ]] ||
  fail "无法解析 Job Pod 的运行时 imageID：${runtime_image_id:-缺失}"
actual_config_digest="${BASH_REMATCH[1]}"
[[ "${actual_config_digest}" == "${EXPECTED_KAFKA_CONFIG_DIGEST}" ]] ||
  fail "Job Pod 镜像不匹配：实际 ${actual_config_digest}，要求 ${EXPECTED_KAFKA_CONFIG_DIGEST}"

"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  logs "pod/${pod_name}" \
  -n "${KUBE_NAMESPACE}" \
  -c "${CONTAINER_NAME}" \
  >"${log_file}"

verified_lines=()
mapfile -t verified_lines < <(grep '^TOPIC_VERIFIED ' "${log_file}")
[[ "${#verified_lines[@]}" -eq 4 ]] ||
  fail "Job 日志包含 ${#verified_lines[@]} 条 TOPIC_VERIFIED，要求 4 条"

declare -A seen_topics=()
for index in "${!EXPECTED_TOPICS[@]}"; do
  expected_topic="${EXPECTED_TOPICS[index]}"
  matching_lines=()
  mapfile -t matching_lines < <(grep -F "TOPIC_VERIFIED name=${expected_topic} " "${log_file}")
  [[ "${#matching_lines[@]}" -eq 1 ]] ||
    fail "主题 ${expected_topic} 的验证证据为 ${#matching_lines[@]} 条，要求 1 条"

  read -r marker name_field topic_id_field remainder <<<"${matching_lines[0]}"
  topic_id="${topic_id_field#topic_id=}"
  [[ "${marker}" == "TOPIC_VERIFIED" && "${name_field}" == "name=${expected_topic}" ]] ||
    fail "主题 ${expected_topic} 的验证证据格式异常"
  [[ "${topic_id}" =~ ^[A-Za-z0-9_-]+$ ]] || fail "主题 ${expected_topic} 的 TopicId 无效"
  expected_line="TOPIC_VERIFIED name=${expected_topic} topic_id=${topic_id} partitions=${EXPECTED_PARTITIONS[index]} replication_factor=1 cleanup.policy=delete retention.ms=86400000 retention.bytes=134217728 segment.bytes=67108864 min.insync.replicas=1"
  [[ "${matching_lines[0]}" == "${expected_line}" ]] ||
    fail "主题 ${expected_topic} 的验证证据内容异常"
  seen_topics["${expected_topic}"]="${topic_id}"
done

printf '%s\n' "${verified_lines[@]}"
echo "Kafka 主题初始化通过：单 Pod、0 重启、镜像身份匹配、4 个主题严格验证"
