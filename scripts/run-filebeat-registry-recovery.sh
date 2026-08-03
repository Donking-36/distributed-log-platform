#!/usr/bin/env bash
set -Eeuo pipefail

readonly SERVICES=("api-service" "worker-service")
readonly PRE_JOBS=("api-service-acceptance" "worker-service-acceptance")
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
readonly MINIKUBE_CONTAINER="stage3-logs"
readonly REGISTRY_NODE_PATH="/var/lib/distributed-log-platform/filebeat-data"
readonly REGISTRY_POD_PATH="/usr/share/filebeat/data"

DOCKER="${DOCKER:-docker}"
KUBECTL="${KUBECTL:-kubectl}"
PYTHON="${PYTHON:-python3}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
FILEBEAT_NAMESPACE="${FILEBEAT_NAMESPACE:-stage3-collector}"
FILEBEAT_VERSION="${FILEBEAT_VERSION:-9.4.4}"
KUSTOMIZE_ACCEPTANCE_OVERLAY="${KUSTOMIZE_ACCEPTANCE_OVERLAY:-deploy/kubernetes/overlays/local-acceptance}"
FILEBEAT_RECOVERY_TIMEOUT="${FILEBEAT_RECOVERY_TIMEOUT:-180s}"
FILEBEAT_RECOVERY_SETTLE_SECONDS="${FILEBEAT_RECOVERY_SETTLE_SECONDS:-20}"
FILEBEAT_ACCEPTANCE_MAX_RECORDS="${FILEBEAT_ACCEPTANCE_MAX_RECORDS:-5000}"

fail() {
  echo "$1" >&2
  exit 1
}

[[ "${FILEBEAT_RECOVERY_TIMEOUT}" =~ ^([1-9][0-9]*)s$ ]] ||
  fail "FILEBEAT_RECOVERY_TIMEOUT 必须是正整数秒，例如 180s"
readonly TIMEOUT_SECONDS="${BASH_REMATCH[1]}"
[[ "${FILEBEAT_RECOVERY_SETTLE_SECONDS}" =~ ^[1-9][0-9]*$ ]] ||
  fail "FILEBEAT_RECOVERY_SETTLE_SECONDS 必须是正整数"
[[ "${FILEBEAT_ACCEPTANCE_MAX_RECORDS}" =~ ^[1-9][0-9]*$ ]] ||
  fail "FILEBEAT_ACCEPTANCE_MAX_RECORDS 必须是正整数"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

for command_name in "${DOCKER}" "${KUBECTL}" "${PYTHON}" csplit flock grep readlink; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

# 恢复测试跨越固定名基准 Job、独立恢复 Job 和采集器重建，整个窗口只允许一个流程。
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

# 访问宿主 registry 前核对 Minikube 外层容器身份，避免名称碰撞时读取错误目标。
minikube_facts="$(${DOCKER} inspect "${MINIKUBE_CONTAINER}" \
  --format '{{index .Config.Labels "created_by.minikube.sigs.k8s.io"}}|{{index .Config.Labels "mode.minikube.sigs.k8s.io"}}|{{index .Config.Labels "name.minikube.sigs.k8s.io"}}')"
[[ "${minikube_facts}" == "true|${KUBE_CONTEXT}|${KUBE_CONTEXT}" ]] ||
  fail "Minikube 容器身份异常：${minikube_facts}"

pre_run_id="uc001-registry-pre-$(${PYTHON} -c 'import uuid; print(uuid.uuid4().hex)')"
post_run_id="uc001-registry-post-$(${PYTHON} -c 'import uuid; print(uuid.uuid4().hex)')"
post_suffix="${post_run_id: -12}"
POST_JOBS=("api-service-registry-post-${post_suffix}" "worker-service-registry-post-${post_suffix}")

umask 077
work_dir="$(mktemp -d)"
split_dir="${work_dir}/split"
mkdir "${split_dir}"
rendered_file="${work_dir}/rendered.yaml"
configured_file="${work_dir}/configured.yaml"
declare -a split_files=("${split_dir}/job-00.yaml" "${split_dir}/job-01.yaml")
declare -a post_manifest_files=("${work_dir}/post-job-0.yaml" "${work_dir}/post-job-1.yaml")
declare -a post_source_files=("${work_dir}/post-source-0.log" "${work_dir}/post-source-1.log")
declare -a post_pod_names=()
declare -a post_pod_uids=()
declare -a post_job_uids=()
declare -a pre_job_evidence=()
declare -a pre_pod_evidence=()
declare -a pre_log_paths=()
declare -a start_files=()
declare -a end_files=()
declare -a capture_files=()
declare -A cursors=()
created_post_job_count=0
post_jobs_cleaned=0

cleanup() {
  local original_status=$?
  local cleanup_status=0
  local index
  trap - EXIT
  set +e
  if ((created_post_job_count > 0 && post_jobs_cleaned == 0)); then
    if ((original_status != 0)); then
      "${KUBECTL}" --context="${KUBE_CONTEXT}" get jobs,pods \
        -n "${KUBE_NAMESPACE}" \
        -l distributed-log-platform.io/purpose=registry-recovery \
        -o wide >&2
      for ((index = 0; index < created_post_job_count; index += 1)); do
        "${KUBECTL}" --context="${KUBE_CONTEXT}" logs "job/${POST_JOBS[index]}" \
          -n "${KUBE_NAMESPACE}" -c log-producer >&2
      done
    fi
    cleanup_created_post_jobs || cleanup_status=1
  fi
  rm -f -- "${split_dir}"/*
  rmdir -- "${split_dir}"
  rm -f -- "${work_dir}"/*
  rmdir -- "${work_dir}"
  if ((original_status == 0 && cleanup_status != 0)); then
    exit "${cleanup_status}"
  fi
  exit "${original_status}"
}
trap cleanup EXIT

kafka_exec() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${KAFKA_POD}" \
    -n "${KUBE_NAMESPACE}" -c "${KAFKA_CONTAINER}" -- \
    env "KAFKA_HEAP_OPTS=-Xms32m -Xmx128m" "KAFKA_GC_LOG_OPTS=-Xlog:gc=off" "$@"
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

capture_offsets() {
  local topic="$1"
  local expected_partitions="$2"
  local output_file="$3"
  local matching_count partition

  kafka_exec /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "${topic}" >"${output_file}"
  matching_count="$(grep -Fc "${topic}:" "${output_file}" || true)"
  [[ "${matching_count}" -eq "${expected_partitions}" ]] ||
    fail "主题 ${topic} 位点清单异常：实际 ${matching_count} 个分区，要求 ${expected_partitions}"
  for ((partition = 0; partition < expected_partitions; partition += 1)); do
    get_offset "${output_file}" "${topic}" "${partition}" >/dev/null
  done
}

# 每轮只读取游标到已锁定高水位的精确记录数；超出上限视为可能发生大规模回放。
capture_recovery_window() {
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
        fail "主题 ${topic} 分区 ${partition} 恢复窗口新增 ${count} 条，超过安全上限 ${FILEBEAT_ACCEPTANCE_MAX_RECORDS}"
      if ((count > 0)); then
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
      fi
      cursors["${key}"]="${end}"
    done
  done
}

daemonset_identity() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get daemonset filebeat \
    -n "${FILEBEAT_NAMESPACE}" -o json | "${PYTHON}" -c '
import json, sys
obj = json.load(sys.stdin)
labels = obj["metadata"].get("labels", {})
volumes = {item["name"]: item for item in obj["spec"]["template"]["spec"]["volumes"]}
containers = {item["name"]: item for item in obj["spec"]["template"]["spec"]["containers"]}
mounts = {item["name"]: item for item in containers["filebeat"]["volumeMounts"]}
values = (
    obj["metadata"]["uid"],
    labels.get("distributed-log-platform.io/purpose", ""),
    volumes["registry"]["hostPath"]["path"],
    mounts["registry"]["mountPath"],
    str(mounts["registry"].get("readOnly", False)).lower(),
)
print("|".join(values))'
}

filebeat_pod_identity() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get pods \
    -n "${FILEBEAT_NAMESPACE}" -l app.kubernetes.io/name=filebeat -o json | "${PYTHON}" -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
if len(items) != 1:
    raise SystemExit(f"Filebeat 必须精确关联 1 个 Pod，实际 {len(items)}")
pod = items[0]
owners = [item for item in pod["metadata"].get("ownerReferences", []) if item.get("controller")]
if len(owners) != 1:
    raise SystemExit(f"Filebeat Pod 必须精确包含 1 个 controller owner，实际 {len(owners)}")
statuses = {item["name"]: item for item in pod.get("status", {}).get("containerStatuses", [])}
if "filebeat" not in statuses:
    raise SystemExit("Filebeat Pod 缺少 filebeat 容器状态")
ready = next((item.get("status", "") for item in pod["status"].get("conditions", []) if item.get("type") == "Ready"), "")
owner = owners[0]
values = (
    pod["metadata"]["name"], pod["metadata"]["uid"], owner.get("apiVersion", ""),
    owner.get("kind", ""), owner.get("name", ""), owner.get("uid", ""),
    pod["spec"].get("nodeName", ""), pod["status"].get("phase", ""), ready,
    str(statuses["filebeat"].get("restartCount", "")),
)
print("|".join(values))'
}

registry_snapshot() {
  "${DOCKER}" exec "${MINIKUBE_CONTAINER}" /bin/sh -ec '
test -d "$1"
test -s "$1/meta.json"
test -s "$1/registry/filebeat/log.json"
printf "root=%s|meta=%s|content=" "$(stat -c "%d:%i" "$1")" "$(stat -c "%d:%i" "$1/meta.json")"
tr -d "\n" <"$1/meta.json"' registry-check "${REGISTRY_NODE_PATH}"
}

pod_registry_identity() {
  local pod_name="$1"
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${pod_name}" \
    -n "${FILEBEAT_NAMESPACE}" -c filebeat -- stat -c '%d:%i' "${REGISTRY_POD_PATH}"
}

capture_pre_evidence() {
  local index job job_evidence pod_evidence container_id log_path

  for index in "${!PRE_JOBS[@]}"; do
    job="${PRE_JOBS[index]}"
    job_evidence="$(${KUBECTL} --context="${KUBE_CONTEXT}" get job "${job}" \
      -n "${KUBE_NAMESPACE}" \
      -o 'jsonpath={.metadata.uid}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"|"}{.status.conditions[?(@.type=="Complete")].status}')"
    [[ "${job_evidence}" == *'|acceptance|True' ]] || fail "基准 Job ${job} 证据异常：${job_evidence}"
    pre_job_evidence[index]="${job_evidence}"

    pod_evidence="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pods \
      -n "${KUBE_NAMESPACE}" -l "job-name=${job}" -o json | "${PYTHON}" -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
if len(items) != 1:
    raise SystemExit(f"基准 Job 必须精确关联 1 个 Pod，实际 {len(items)}")
pod = items[0]
statuses = {item["name"]: item for item in pod["status"].get("containerStatuses", [])}
status = statuses.get("log-producer")
if status is None:
    raise SystemExit("基准 Pod 缺少 log-producer 容器状态")
print("|".join((pod["metadata"]["name"], pod["metadata"]["uid"], pod["spec"]["nodeName"], status["containerID"])))')"
    pre_pod_evidence[index]="${pod_evidence}"
    IFS='|' read -r _ _ _ container_id <<<"${pod_evidence}"
    container_id="${container_id#containerd://}"
    [[ "${container_id}" =~ ^[0-9a-f]{64}$ ]] || fail "基准 Pod ${job} 容器 ID 非法"
    IFS='|' read -r pre_pod_name _ _ _ <<<"${pod_evidence}"
    log_path="/var/log/containers/${pre_pod_name}_${KUBE_NAMESPACE}_log-producer-${container_id}.log"
    "${DOCKER}" exec "${MINIKUBE_CONTAINER}" /bin/sh -ec 'test -L "$1" && test -e "$1"' \
      pre-log-check "${log_path}"
    "${DOCKER}" exec "${MINIKUBE_CONTAINER}" grep -Fq -- "${pre_run_id}" "${log_path}" ||
      fail "基准节点日志不包含完整 pre_run_id：${log_path}"
    pre_log_paths[index]="${log_path}"
  done
}

assert_pre_evidence_retained() {
  local index job actual_job actual_pod

  for index in "${!PRE_JOBS[@]}"; do
    job="${PRE_JOBS[index]}"
    actual_job="$(${KUBECTL} --context="${KUBE_CONTEXT}" get job "${job}" \
      -n "${KUBE_NAMESPACE}" \
      -o 'jsonpath={.metadata.uid}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"|"}{.status.conditions[?(@.type=="Complete")].status}')"
    [[ "${actual_job}" == "${pre_job_evidence[index]}" ]] ||
      fail "恢复窗口内基准 Job ${job} 被替换或状态改变"
    actual_pod="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pods \
      -n "${KUBE_NAMESPACE}" -l "job-name=${job}" -o json | "${PYTHON}" -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
if len(items) != 1:
    raise SystemExit(f"基准 Job 必须精确关联 1 个 Pod，实际 {len(items)}")
pod = items[0]
statuses = {item["name"]: item for item in pod["status"].get("containerStatuses", [])}
status = statuses.get("log-producer")
if status is None:
    raise SystemExit("基准 Pod 缺少 log-producer 容器状态")
print("|".join((pod["metadata"]["name"], pod["metadata"]["uid"], pod["spec"]["nodeName"], status["containerID"])))')"
    [[ "${actual_pod}" == "${pre_pod_evidence[index]}" ]] ||
      fail "恢复窗口内基准 Pod ${job} 被替换"
    "${DOCKER}" exec "${MINIKUBE_CONTAINER}" /bin/sh -ec 'test -L "$1" && test -e "$1"' \
      pre-log-check "${pre_log_paths[index]}"
    "${DOCKER}" exec "${MINIKUBE_CONTAINER}" grep -Fq -- "${pre_run_id}" "${pre_log_paths[index]}" ||
      fail "恢复窗口结束时基准节点日志已不包含 pre_run_id：${pre_log_paths[index]}"
  done
}

create_post_jobs() {
  local index patch_json inventory

  "${KUBECTL}" kustomize "${KUSTOMIZE_ACCEPTANCE_OVERLAY}" >"${rendered_file}"
  "${KUBECTL}" set env --local -f "${rendered_file}" \
    "PRODUCER_TEST_RUN_ID=${post_run_id}" -o yaml >"${configured_file}"
  csplit --silent --prefix="${split_dir}/job-" --suffix-format='%02d.yaml' \
    "${configured_file}" '/^---$/' '{*}'

  for index in "${!POST_JOBS[@]}"; do
    patch_json="$(${PYTHON} -c '
import json, sys
name, run_id = sys.argv[1:]
print(json.dumps([
    {"op": "replace", "path": "/metadata/name", "value": name},
    {"op": "replace", "path": "/metadata/labels/app.kubernetes.io~1instance", "value": name},
    {"op": "replace", "path": "/metadata/labels/distributed-log-platform.io~1purpose", "value": "registry-recovery"},
    {"op": "add", "path": "/metadata/labels/distributed-log-platform.io~1run-id", "value": run_id},
    {"op": "replace", "path": "/spec/template/metadata/labels/app.kubernetes.io~1instance", "value": name},
    {"op": "replace", "path": "/spec/template/metadata/labels/distributed-log-platform.io~1purpose", "value": "registry-recovery"},
    {"op": "add", "path": "/spec/template/metadata/labels/distributed-log-platform.io~1run-id", "value": run_id},
]))' "${POST_JOBS[index]}" "${post_run_id}")"
    "${KUBECTL}" patch --local -f "${split_files[index]}" --type=json \
      -p="${patch_json}" -o yaml >"${post_manifest_files[index]}"

    inventory="$(${KUBECTL} create --dry-run=client -f "${post_manifest_files[index]}" \
      -o 'jsonpath={.kind}{"|"}{.metadata.name}{"|"}{.metadata.namespace}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"|"}{.spec.template.spec.containers[0].env[?(@.name=="PRODUCER_TEST_RUN_ID")].value}')"
    [[ "${inventory}" == "Job|${POST_JOBS[index]}|${KUBE_NAMESPACE}|registry-recovery|${post_run_id}" ]] ||
      fail "恢复 Job ${POST_JOBS[index]} 清单边界异常：${inventory}"
    [[ -z "$(${KUBECTL} --context="${KUBE_CONTEXT}" get job "${POST_JOBS[index]}" \
      -n "${KUBE_NAMESPACE}" --ignore-not-found -o name)" ]] ||
      fail "恢复 Job 已存在，拒绝替换：${POST_JOBS[index]}"
    "${KUBECTL}" --context="${KUBE_CONTEXT}" create --dry-run=server \
      -f "${post_manifest_files[index]}" >/dev/null
  done

  for index in "${!POST_JOBS[@]}"; do
    post_job_uids[index]="$(${KUBECTL} --context="${KUBE_CONTEXT}" create \
      -f "${post_manifest_files[index]}" -o 'jsonpath={.metadata.uid}')"
    [[ -n "${post_job_uids[index]}" ]] || fail "恢复 Job ${POST_JOBS[index]} 创建后缺少 UID"
    created_post_job_count=$((index + 1))
  done
  "${KUBECTL}" --context="${KUBE_CONTEXT}" wait --for=condition=complete \
    "job/${POST_JOBS[0]}" "job/${POST_JOBS[1]}" -n "${KUBE_NAMESPACE}" \
    --timeout="${FILEBEAT_RECOVERY_TIMEOUT}"

  for index in "${!POST_JOBS[@]}"; do
    post_identity="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pods \
      -n "${KUBE_NAMESPACE}" -l "job-name=${POST_JOBS[index]}" -o json | "${PYTHON}" -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
if len(items) != 1:
    raise SystemExit(f"恢复 Job 必须精确关联 1 个 Pod，实际 {len(items)}")
pod = items[0]
statuses = {item["name"]: item for item in pod["status"].get("containerStatuses", [])}
status = statuses.get("log-producer")
if pod["status"].get("phase") != "Succeeded" or status is None or status.get("restartCount") != 0:
    raise SystemExit("恢复 Job Pod 未成功或发生重启")
print("|".join((pod["metadata"]["name"], pod["metadata"]["uid"])))')"
    IFS='|' read -r post_pod_names[index] post_pod_uids[index] <<<"${post_identity}"
    "${KUBECTL}" --context="${KUBE_CONTEXT}" logs "${post_pod_names[index]}" \
      -n "${KUBE_NAMESPACE}" -c log-producer >"${post_source_files[index]}"
    "${PYTHON}" scripts/validate_log_producer_output.py \
      --input "${post_source_files[index]}" \
      --service "${SERVICES[index]}" \
      --run-id "${post_run_id}" \
      --count "${EXPECTED_COUNT}"
  done
}

routes_validator() {
  local args=(
    "${PYTHON}" scripts/validate_filebeat_kafka_output.py routes
    --run-id "${post_run_id}"
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
      --source "${SERVICES[index]}=${post_source_files[index]}"
      --pod "${SERVICES[index]}=${post_pod_names[index]},${post_pod_uids[index]}"
    )
  done
  "${args[@]}"
}

replay_validator() {
  local args=(
    "${PYTHON}" scripts/validate_filebeat_kafka_output.py replay --run-id "${pre_run_id}"
  )
  local index

  for index in "${!TOPICS[@]}"; do
    args+=(--capture "${TOPICS[index]}=${capture_files[index]}")
  done
  "${args[@]}"
}

cleanup_created_post_jobs() {
  local index identity
  local cleanup_failed=0

  for ((index = 0; index < created_post_job_count; index += 1)); do
    identity="$(${KUBECTL} --context="${KUBE_CONTEXT}" get job "${POST_JOBS[index]}" \
      -n "${KUBE_NAMESPACE}" --ignore-not-found \
      -o 'jsonpath={.metadata.uid}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"|"}{.metadata.labels.distributed-log-platform\.io/run-id}')"
    if [[ -z "${identity}" ]]; then
      continue
    fi
    if [[ "${identity}" != "${post_job_uids[index]}|registry-recovery|${post_run_id}" ]]; then
      echo "恢复 Job ${POST_JOBS[index]} 身份变化，拒绝清理：${identity}" >&2
      cleanup_failed=1
      continue
    fi
    if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" delete job "${POST_JOBS[index]}" \
      -n "${KUBE_NAMESPACE}" --cascade=foreground --wait=true; then
      cleanup_failed=1
    fi
  done
  ((cleanup_failed == 0)) || return 1
  post_jobs_cleaned=1
}

daemonset_facts="$(daemonset_identity)"
IFS='|' read -r daemonset_uid daemonset_purpose host_registry_path pod_registry_path registry_read_only <<<"${daemonset_facts}"
[[ "${daemonset_purpose}|${host_registry_path}|${pod_registry_path}|${registry_read_only}" == \
  "log-collection|${REGISTRY_NODE_PATH}|${REGISTRY_POD_PATH}|false" ]] ||
  fail "Filebeat DaemonSet registry 边界异常：${daemonset_facts}"

echo "运行 registry 重建前基准验收：test_run_id=${pre_run_id}"
ACCEPTANCE_RUN_ID="${pre_run_id}" "${repo_root}/scripts/run-filebeat-acceptance.sh"
capture_pre_evidence

# 基准批次已确认到达后才开启恢复窗口，窗口内任何相同 run-id 都是重新读取。
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

old_identity="$(filebeat_pod_identity)"
IFS='|' read -r old_pod_name old_pod_uid old_owner_api old_owner_kind old_owner_name \
  old_owner_uid old_node old_phase old_ready old_restart_count <<<"${old_identity}"
[[ "${old_owner_api}|${old_owner_kind}|${old_owner_name}|${old_owner_uid}" == \
  "apps/v1|DaemonSet|filebeat|${daemonset_uid}" ]] ||
  fail "拒绝删除 owner 不正确的 Filebeat Pod：${old_identity}"
[[ "${old_phase}|${old_ready}|${old_restart_count}" == "Running|True|0" ]] ||
  fail "重建前 Filebeat Pod 状态异常：${old_identity}"
registry_before="$(registry_snapshot)"
registry_root_before="${registry_before#root=}"
registry_root_before="${registry_root_before%%|*}"
[[ "$(pod_registry_identity "${old_pod_name}")" == "${registry_root_before}" ]] ||
  fail "重建前 Pod registry 挂载与节点目录身份不一致"

echo "重建 Filebeat Pod：name=${old_pod_name} uid=${old_pod_uid} registry=${registry_before}"
"${KUBECTL}" --context="${KUBE_CONTEXT}" delete pod "${old_pod_name}" \
  -n "${FILEBEAT_NAMESPACE}" --wait=false
"${KUBECTL}" --context="${KUBE_CONTEXT}" wait --for=delete "pod/${old_pod_name}" \
  -n "${FILEBEAT_NAMESPACE}" --timeout="${FILEBEAT_RECOVERY_TIMEOUT}"
"${KUBECTL}" --context="${KUBE_CONTEXT}" rollout status daemonset/filebeat \
  -n "${FILEBEAT_NAMESPACE}" --timeout="${FILEBEAT_RECOVERY_TIMEOUT}"

new_identity="$(filebeat_pod_identity)"
IFS='|' read -r new_pod_name new_pod_uid new_owner_api new_owner_kind new_owner_name \
  new_owner_uid new_node new_phase new_ready new_restart_count <<<"${new_identity}"
[[ "${new_owner_api}|${new_owner_kind}|${new_owner_name}|${new_owner_uid}" == \
  "apps/v1|DaemonSet|filebeat|${daemonset_uid}" ]] ||
  fail "重建后的 Filebeat Pod owner 异常：${new_identity}"
[[ "${new_pod_uid}" != "${old_pod_uid}" ]] || fail "Filebeat Pod UID 未改变"
[[ "${new_node}|${new_phase}|${new_ready}|${new_restart_count}" == "${old_node}|Running|True|0" ]] ||
  fail "重建后的 Filebeat Pod 状态或节点异常：${new_identity}"
"${repo_root}/scripts/verify-filebeat-image.sh" pod
registry_after="$(registry_snapshot)"
[[ "${registry_after}" == "${registry_before}" ]] ||
  fail "宿主 registry 目录或 meta 身份发生变化：${registry_before} -> ${registry_after}"
[[ "$(pod_registry_identity "${new_pod_name}")" == "${registry_root_before}" ]] ||
  fail "重建后 Pod registry 挂载与节点目录身份不一致"

echo "运行 registry 重建后独立验收：test_run_id=${post_run_id}"
create_post_jobs
validator_stdout="${work_dir}/validator.out"
validator_stderr="${work_dir}/validator.err"
deadline=$((SECONDS + TIMEOUT_SECONDS))
while true; do
  capture_recovery_window
  if routes_validator >"${validator_stdout}" 2>"${validator_stderr}"; then
    break
  else
    validator_status=$?
  fi
  if [[ "${validator_status}" -eq 1 ]]; then
    cat "${validator_stderr}" >&2
    fail "重建后批次违反 Filebeat 路由契约"
  fi
  [[ "${validator_status}" -eq 2 ]] || fail "Filebeat 路由校验器异常退出：${validator_status}"
  if ((SECONDS >= deadline)); then
    cat "${validator_stderr}" >&2
    fail "重建后批次未在 ${FILEBEAT_RECOVERY_TIMEOUT} 内收齐"
  fi
  sleep 2
done

sleep "${FILEBEAT_RECOVERY_SETTLE_SECONDS}"
capture_recovery_window
routes_validator >"${validator_stdout}" 2>"${validator_stderr}" || {
  cat "${validator_stderr}" >&2
  fail "恢复稳定观察窗口出现路由错误"
}
cat "${validator_stdout}"
replay_validator
assert_pre_evidence_retained

for topic_index in "${!TOPICS[@]}"; do
  for ((partition = 0; partition < TOPIC_PARTITIONS[topic_index]; partition += 1)); do
    start="$(get_offset "${start_files[topic_index]}" "${TOPICS[topic_index]}" "${partition}")"
    end="${cursors["${TOPICS[topic_index]}|${partition}"]}"
    echo "RECOVERY_RANGE topic=${TOPICS[topic_index]} partition=${partition} start=${start} end=${end}"
  done
done

cleanup_created_post_jobs || fail "恢复 Job 清理失败"
echo "Filebeat registry 重建恢复验收通过：old_uid=${old_pod_uid} new_uid=${new_pod_uid} registry=${registry_after} pre_run_id=${pre_run_id} post_run_id=${post_run_id}"
