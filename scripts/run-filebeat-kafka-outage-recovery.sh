#!/usr/bin/env bash
set -Eeuo pipefail

readonly SERVICES=("api-service" "worker-service")
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
readonly KAFKA_PVC="data-kafka-0"
readonly RECOVERY_PURPOSE="kafka-outage-recovery"

KUBECTL="${KUBECTL:-kubectl}"
PYTHON="${PYTHON:-python3}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
FILEBEAT_NAMESPACE="${FILEBEAT_NAMESPACE:-stage3-collector}"
FILEBEAT_VERSION="${FILEBEAT_VERSION:-9.4.4}"
KUSTOMIZE_ACCEPTANCE_OVERLAY="${KUSTOMIZE_ACCEPTANCE_OVERLAY:-deploy/kubernetes/overlays/local-acceptance}"
KUSTOMIZE_KAFKA_OVERLAY="${KUSTOMIZE_KAFKA_OVERLAY:-deploy/kubernetes/overlays/local-kafka}"
KUSTOMIZE_FILEBEAT_OVERLAY="${KUSTOMIZE_FILEBEAT_OVERLAY:-deploy/kubernetes/overlays/local-filebeat}"
FILEBEAT_OUTAGE_TIMEOUT="${FILEBEAT_OUTAGE_TIMEOUT:-300s}"
FILEBEAT_OUTAGE_SETTLE_SECONDS="${FILEBEAT_OUTAGE_SETTLE_SECONDS:-20}"
FILEBEAT_OUTAGE_PROBE_TIMEOUT="${FILEBEAT_OUTAGE_PROBE_TIMEOUT:-45s}"
FILEBEAT_ACCEPTANCE_MAX_RECORDS="${FILEBEAT_ACCEPTANCE_MAX_RECORDS:-5000}"

fail() {
  echo "$1" >&2
  exit 1
}

[[ "${FILEBEAT_OUTAGE_TIMEOUT}" =~ ^([1-9][0-9]*)s$ ]] ||
  fail "FILEBEAT_OUTAGE_TIMEOUT 必须是正整数秒，例如 300s"
readonly TIMEOUT_SECONDS="${BASH_REMATCH[1]}"
[[ "${FILEBEAT_OUTAGE_SETTLE_SECONDS}" =~ ^[1-9][0-9]*$ ]] ||
  fail "FILEBEAT_OUTAGE_SETTLE_SECONDS 必须是正整数"
[[ "${FILEBEAT_OUTAGE_PROBE_TIMEOUT}" =~ ^[1-9][0-9]*s$ ]] ||
  fail "FILEBEAT_OUTAGE_PROBE_TIMEOUT 必须是正整数秒，例如 45s"
[[ "${FILEBEAT_ACCEPTANCE_MAX_RECORDS}" =~ ^[1-9][0-9]*$ ]] ||
  fail "FILEBEAT_ACCEPTANCE_MAX_RECORDS 必须是正整数"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

for command_name in "${KUBECTL}" "${PYTHON}" csplit date flock grep readlink timeout; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

# Broker 短停会改 StatefulSet 副本并创建唯一 Job，必须与所有 Filebeat 写流程串行。
source "${repo_root}/scripts/lib/filebeat-workflow-lock.sh"
source "${repo_root}/scripts/lib/filebeat-kafka-evidence.sh"
acquire_filebeat_workflow_lock || exit 1
export FILEBEAT_WORKFLOW_LOCK_INHERITED=1

actual_context="$(${KUBECTL} config current-context)"
[[ "${actual_context}" == "${KUBE_CONTEXT}" ]] ||
  fail "Kubernetes 上下文不匹配：实际 ${actual_context}，要求 ${KUBE_CONTEXT}"

run_id="uc001-kafka-outage-$(${PYTHON} -c 'import uuid; print(uuid.uuid4().hex)')"
[[ "${run_id}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$ ]] ||
  fail "生成的中断批次 ID 不符合 Kubernetes 标签约束：${run_id}"
suffix="${run_id: -12}"
JOB_NAMES=("api-service-kafka-outage-${suffix}" "worker-service-kafka-outage-${suffix}")

umask 077
work_dir="$(mktemp -d)"
split_dir="${work_dir}/split"
mkdir "${split_dir}"
rendered_file="${work_dir}/rendered.yaml"
configured_file="${work_dir}/configured.yaml"
validator_stdout="${work_dir}/validator.out"
validator_stderr="${work_dir}/validator.err"
outage_baseline="${work_dir}/outage-baseline.txt"
declare -a split_files=("${split_dir}/job-00.yaml" "${split_dir}/job-01.yaml")
declare -a job_manifest_files=("${work_dir}/job-0.yaml" "${work_dir}/job-1.yaml")
declare -a source_files=("${work_dir}/source-0.log" "${work_dir}/source-1.log")
declare -a pod_names=()
declare -a pod_uids=()
declare -a pod_log_paths=()
declare -a job_uids=()
declare -a capture_files=()
declare -a end_files=()
declare -a probe_paths=()
declare -A cursors=()
created_job_count=0
attempted_job_count=0
jobs_cleaned=0
kafka_scale_attempted=0
kafka_scaled_down=0
kafka_sts_uid=""
probe_counter=0
filebeat_pod_name=""

kubectl_diff_clean() {
  local overlay="$1"
  local output_file="$2"

  if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" diff -k "${overlay}" >"${output_file}" 2>&1; then
    cat "${output_file}" >&2
    return 1
  fi
  [[ ! -s "${output_file}" ]] || {
    cat "${output_file}" >&2
    return 1
  }
}

kafka_statefulset_identity() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get statefulset kafka \
    -n "${KUBE_NAMESPACE}" -o json | "${PYTHON}" -c '
import json, sys
obj = json.load(sys.stdin)
meta, spec, status = obj["metadata"], obj["spec"], obj.get("status", {})
retention = spec.get("persistentVolumeClaimRetentionPolicy", {})
values = (
    meta["uid"], str(meta.get("generation", "")), str(status.get("observedGeneration", "")),
    str(spec.get("replicas", "")), str(status.get("readyReplicas", 0)),
    str(status.get("currentReplicas", 0)), str(status.get("updatedReplicas", 0)),
    status.get("currentRevision", ""), status.get("updateRevision", ""),
    spec.get("serviceName", ""), retention.get("whenScaled", ""),
    retention.get("whenDeleted", ""),
)
print("|".join(values))'
}

query_kafka_uid_replicas() {
  local facts
  local deadline=$((SECONDS + TIMEOUT_SECONDS))

  while true; do
    if facts="$(${KUBECTL} --context="${KUBE_CONTEXT}" get statefulset kafka \
      -n "${KUBE_NAMESPACE}" \
      -o 'jsonpath={.metadata.uid}{"|"}{.spec.replicas}')"; then
      printf '%s' "${facts}"
      return 0
    fi
    ((SECONDS < deadline)) || return 1
    sleep 2
  done
}

kafka_pod_identity() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get pod "${KAFKA_POD}" \
    -n "${KUBE_NAMESPACE}" -o json | "${PYTHON}" -c '
import json, sys
pod = json.load(sys.stdin)
owners = [item for item in pod["metadata"].get("ownerReferences", []) if item.get("controller")]
statuses = {item["name"]: item for item in pod.get("status", {}).get("containerStatuses", [])}
volumes = {item["name"]: item for item in pod["spec"].get("volumes", [])}
if len(owners) != 1 or "kafka" not in statuses or "data" not in volumes:
    raise SystemExit("Kafka Pod owner、容器状态或数据卷不唯一")
owner, status = owners[0], statuses["kafka"]
ready = next((item.get("status", "") for item in pod["status"].get("conditions", []) if item.get("type") == "Ready"), "")
values = (
    pod["metadata"]["uid"], owner.get("apiVersion", ""), owner.get("kind", ""),
    owner.get("name", ""), owner.get("uid", ""), pod["spec"].get("nodeName", ""),
    pod["status"].get("phase", ""), ready, str(status.get("restartCount", "")),
    status.get("containerID", ""), status.get("imageID", ""),
    volumes["data"].get("persistentVolumeClaim", {}).get("claimName", ""),
)
print("|".join(values))'
}

kafka_pvc_identity() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get pvc "${KAFKA_PVC}" \
    -n "${KUBE_NAMESPACE}" -o json | "${PYTHON}" -c '
import json, sys
obj = json.load(sys.stdin)
spec, status = obj["spec"], obj.get("status", {})
values = (
    obj["metadata"]["uid"], status.get("phase", ""), spec.get("volumeName", ""),
    spec.get("storageClassName", ""), spec.get("volumeMode", ""),
    ",".join(sorted(spec.get("accessModes", []))),
)
print("|".join(values))'
}

kafka_storage_identity() {
  filebeat_kafka_exec /bin/bash -ec '
meta=/var/lib/kafka/data/meta.properties
test -s "$meta"
sha="$(sha256sum "$meta")"
sha="${sha%% *}"
cluster="$(sed -n "s/^cluster.id=//p" "$meta")"
node="$(sed -n "s/^node.id=//p" "$meta")"
test -n "$sha" && test -n "$cluster" && test -n "$node"
printf "%s|%s|%s" "$sha" "$cluster" "$node"'
}

kafka_reported_cluster_id() {
  filebeat_kafka_exec /bin/bash -ec '
export KAFKA_HEAP_OPTS="-Xms32m -Xmx128m"
export KAFKA_GC_LOG_OPTS="-Xlog:gc=off"
exec /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092' |
    "${PYTHON}" -c '
import re, sys
matches = []
for line in sys.stdin:
    match = re.fullmatch(r"Cluster ID:\s*(\S+)\s*", line)
    if match:
        matches.append(match.group(1))
if len(matches) != 1:
    raise SystemExit(f"Kafka Cluster ID 输出不唯一：{matches!r}")
print(matches[0])'
}

filebeat_daemonset_identity() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get daemonset filebeat \
    -n "${FILEBEAT_NAMESPACE}" -o json | "${PYTHON}" -c '
import json, sys
obj = json.load(sys.stdin)
meta, status = obj["metadata"], obj.get("status", {})
values = (
    meta["uid"], str(meta.get("generation", "")), str(status.get("observedGeneration", "")),
    str(status.get("desiredNumberScheduled", 0)), str(status.get("numberReady", 0)),
    str(status.get("numberAvailable", 0)), str(status.get("numberUnavailable", 0)),
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
statuses = {item["name"]: item for item in pod.get("status", {}).get("containerStatuses", [])}
if len(owners) != 1 or "filebeat" not in statuses:
    raise SystemExit("Filebeat Pod owner 或容器状态不唯一")
owner, status = owners[0], statuses["filebeat"]
ready = next((item.get("status", "") for item in pod["status"].get("conditions", []) if item.get("type") == "Ready"), "")
values = (
    pod["metadata"]["name"], pod["metadata"]["uid"], owner.get("apiVersion", ""),
    owner.get("kind", ""), owner.get("name", ""), owner.get("uid", ""),
    pod["spec"].get("nodeName", ""), pod["status"].get("phase", ""), ready,
    str(status.get("restartCount", "")), status.get("containerID", ""),
    status.get("imageID", ""),
)
print("|".join(values))'
}

cleanup_probe_path() {
  local data_path="$1"

  [[ -n "${filebeat_pod_name}" ]] || return 0
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${filebeat_pod_name}" \
    -n "${FILEBEAT_NAMESPACE}" -c filebeat -- /bin/sh -ec '
case "$1" in
  /tmp/filebeat-outage-probe-*) ;;
  *) echo "拒绝清理非预期探针目录：$1" >&2; exit 1 ;;
esac
if [ -d "$1" ]; then
  rm -f -- "$1/meta.json"
  rmdir -- "$1"
fi' probe-cleanup "${data_path}"
}

run_output_probe() {
  local output_file="$1"
  local data_path status

  probe_counter=$((probe_counter + 1))
  data_path="/tmp/filebeat-outage-probe-${suffix}-${probe_counter}"
  probe_paths+=("${data_path}")
  if timeout --foreground "${FILEBEAT_OUTAGE_PROBE_TIMEOUT}" \
    "${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${filebeat_pod_name}" \
      -n "${FILEBEAT_NAMESPACE}" -c filebeat -- \
      filebeat test output -e -c /etc/filebeat.yml --path.data "${data_path}" \
      >"${output_file}" 2>&1; then
    status=0
  else
    status=$?
  fi
  cleanup_probe_path "${data_path}" || return 125
  return "${status}"
}

assert_positive_probe() {
  local output_file="$1"
  grep -Fq 'parse host... OK' "${output_file}" &&
    grep -Fq 'dns lookup... OK' "${output_file}" &&
    grep -Fq 'dial up... OK' "${output_file}"
}

assert_network_failure_probe() {
  local output_file="$1"
  grep -Fq 'parse host... OK' "${output_file}" &&
    grep -Fq 'dns lookup... OK' "${output_file}" &&
    grep -Fq 'dial up... ERROR' "${output_file}" &&
    grep -Eiq 'i/o timeout|operation timed out|connection refused|no route to host|unreachable|client has run out of available brokers' "${output_file}"
}

run_expected_positive_probe() {
  local output_file="$1"
  local status

  if run_output_probe "${output_file}"; then
    status=0
  else
    status=$?
  fi
  ((status != 125)) || return 1
  assert_positive_probe "${output_file}" || return 1
  echo "KAFKA_OUTPUT_PROBE raw_exit=${status} semantic=reachable"
  cat "${output_file}"
}

run_expected_failure_probe() {
  local output_file="$1"
  local status

  if run_output_probe "${output_file}"; then
    status=0
  else
    status=$?
  fi
  ((status != 125)) || return 1
  assert_network_failure_probe "${output_file}" || return 1
  echo "KAFKA_OUTPUT_PROBE raw_exit=${status} semantic=network-unreachable"
  cat "${output_file}"
}

wait_for_output_recovery() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local output_file="${work_dir}/probe-recovered.txt"

  while true; do
    if run_expected_positive_probe "${output_file}"; then
      return 0
    fi
    ((SECONDS < deadline)) || {
      cat "${output_file}" >&2
      return 1
    }
    sleep 2
  done
}

build_replica_patch() {
  local expected_replicas="$1"
  local target_replicas="$2"

  "${PYTHON}" -c '
import json, sys
uid, expected, target = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
print(json.dumps([
    {"op": "test", "path": "/metadata/uid", "value": uid},
    {"op": "test", "path": "/spec/replicas", "value": expected},
    {"op": "replace", "path": "/spec/replicas", "value": target},
]))' "${kafka_sts_uid}" "${expected_replicas}" "${target_replicas}"
}

scale_kafka_down() {
  local facts patch_json

  if ! facts="$(query_kafka_uid_replicas)"; then
    echo "缩容前无法查询 Kafka StatefulSet 身份" >&2
    return 1
  fi
  [[ "${facts}" == "${kafka_sts_uid}|1" ]] || {
    echo "缩容前 Kafka StatefulSet 身份或副本异常：${facts}" >&2
    return 1
  }
  patch_json="$(build_replica_patch 1 0)" || return 1
  kafka_scale_attempted=1
  if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" patch statefulset kafka \
    -n "${KUBE_NAMESPACE}" --type=json -p="${patch_json}" >/dev/null; then
    echo "Kafka StatefulSet 缩容请求失败；EXIT 将按 UID 查询并恢复" >&2
    return 1
  fi
  kafka_scaled_down=1
}

wait_for_kafka_down() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local facts pod_name

  while true; do
    if facts="$(kafka_statefulset_identity)"; then
      IFS='|' read -r uid generation observed replicas ready current updated \
        current_revision update_revision service_name when_scaled when_deleted <<<"${facts}"
      if [[ "${uid}|${generation}|${observed}|${replicas}|${ready}|${current}|${updated}|${current_revision}|${update_revision}|${service_name}|${when_scaled}|${when_deleted}" == \
        "${kafka_sts_uid}|${generation}|${generation}|0|0|0|0|${kafka_revision_before}|${kafka_revision_before}|kafka-headless|Retain|Retain" ]]; then
        if ! pod_name="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pod "${KAFKA_POD}" \
          -n "${KUBE_NAMESPACE}" --ignore-not-found -o name)"; then
          return 1
        fi
        [[ -z "${pod_name}" ]] && return 0
      fi
    fi
    ((SECONDS < deadline)) || return 1
    sleep 2
  done
}

restore_kafka_safely() {
  local facts uid replicas patch_json

  if ! facts="$(query_kafka_uid_replicas)"; then
    echo "恢复时无法查询 Kafka StatefulSet 身份" >&2
    return 1
  fi
  IFS='|' read -r uid replicas <<<"${facts}"
  [[ "${uid}" == "${kafka_sts_uid}" ]] || {
    echo "Kafka StatefulSet UID 已变化，拒绝覆盖：${facts}" >&2
    return 1
  }
  case "${replicas}" in
    0)
      patch_json="$(build_replica_patch 0 1)" || return 1
      if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" patch statefulset kafka \
        -n "${KUBE_NAMESPACE}" --type=json -p="${patch_json}" >/dev/null; then
        # 响应丢失时重新查询；只有同 UID 且已经为 1 才接管恢复结果。
        facts="$(query_kafka_uid_replicas)" || return 1
        [[ "${facts}" == "${kafka_sts_uid}|1" ]] || return 1
      fi
      ;;
    1)
      ;;
    *)
      echo "Kafka StatefulSet 副本数超出恢复边界：${facts}" >&2
      return 1
      ;;
  esac

  if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" rollout status statefulset/kafka \
    -n "${KUBE_NAMESPACE}" --timeout="${FILEBEAT_OUTAGE_TIMEOUT}"; then
    return 1
  fi
  if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" wait --for=condition=Ready "pod/${KAFKA_POD}" \
    -n "${KUBE_NAMESPACE}" --timeout="${FILEBEAT_OUTAGE_TIMEOUT}"; then
    return 1
  fi
  kafka_scaled_down=0
  kafka_scale_attempted=0
}

query_job_cleanup_identity() {
  local job_name="$1"
  local attempt actual

  for ((attempt = 1; attempt <= 5; attempt += 1)); do
    if actual="$(${KUBECTL} --context="${KUBE_CONTEXT}" get job "${job_name}" \
      -n "${KUBE_NAMESPACE}" --ignore-not-found \
      -o 'jsonpath={.metadata.uid}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"|"}{.metadata.labels.distributed-log-platform\.io/run-id}')"; then
      printf '%s' "${actual}"
      return 0
    fi
    sleep 2
  done
  return 1
}

cleanup_created_jobs() {
  local index identity actual_uid
  local cleanup_failed=0

  for ((index = 0; index < attempted_job_count; index += 1)); do
    if ! identity="$(query_job_cleanup_identity "${JOB_NAMES[index]}")"; then
      echo "查询中断恢复 Job ${JOB_NAMES[index]} 失败，不能确认是否需要清理" >&2
      cleanup_failed=1
      continue
    fi
    if [[ -z "${identity}" ]]; then
      continue
    fi
    actual_uid="${identity%%|*}"
    if [[ -z "${job_uids[index]:-}" ]]; then
      job_uids[index]="${actual_uid}"
    fi
    if [[ -z "${actual_uid}" || "${identity}" != "${job_uids[index]}|${RECOVERY_PURPOSE}|${run_id}" ]]; then
      echo "中断恢复 Job ${JOB_NAMES[index]} 身份变化，拒绝删除：${identity}" >&2
      cleanup_failed=1
      continue
    fi
    if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" delete job "${JOB_NAMES[index]}" \
      -n "${KUBE_NAMESPACE}" --cascade=foreground --wait=true; then
      cleanup_failed=1
    fi
  done
  ((cleanup_failed == 0)) || return 1
  attempted_job_count=0
  jobs_cleaned=1
}

assert_no_recovery_resources() {
  local residual

  if ! residual="$(${KUBECTL} --context="${KUBE_CONTEXT}" get jobs,pods \
    -n "${KUBE_NAMESPACE}" \
    -l "distributed-log-platform.io/purpose=${RECOVERY_PURPOSE}" -o name)"; then
    echo "查询中断恢复资源失败，不能确认清理状态" >&2
    return 1
  fi
  [[ -z "${residual}" ]] || {
    echo "中断恢复资源未清理：${residual}" >&2
    return 1
  }
}

cleanup() {
  local original_status=$?
  local cleanup_status=0
  local probe_path
  trap - EXIT INT TERM
  set +e

  if ((kafka_scale_attempted == 1 || kafka_scaled_down == 1)); then
    restore_kafka_safely || cleanup_status=1
    if ((kafka_scale_attempted == 0)) && [[ -n "${filebeat_pod_name}" ]]; then
      wait_for_output_recovery || cleanup_status=1
    fi
  fi
  if ((attempted_job_count > 0 && jobs_cleaned == 0)); then
    if ((original_status != 0)); then
      "${KUBECTL}" --context="${KUBE_CONTEXT}" get jobs,pods \
        -n "${KUBE_NAMESPACE}" \
        -l "distributed-log-platform.io/purpose=${RECOVERY_PURPOSE}" -o wide >&2
    fi
    cleanup_created_jobs || cleanup_status=1
  fi
  assert_no_recovery_resources >/dev/null 2>&1 || cleanup_status=1
  for probe_path in "${probe_paths[@]}"; do
    cleanup_probe_path "${probe_path}" >/dev/null 2>&1 || cleanup_status=1
  done
  rm -f -- "${split_dir}"/* || cleanup_status=1
  rmdir -- "${split_dir}" || cleanup_status=1
  rm -f -- "${work_dir}"/* || cleanup_status=1
  rmdir -- "${work_dir}" || cleanup_status=1

  if ((original_status == 0 && cleanup_status != 0)); then
    exit "${cleanup_status}"
  fi
  exit "${original_status}"
}
trap 'exit 130' INT
trap 'exit 143' TERM
trap cleanup EXIT

capture_all_offsets() {
  local output_file="$1"
  local topic_index topic_file

  : >"${output_file}"
  for topic_index in "${!TOPICS[@]}"; do
    topic_file="${work_dir}/offset-topic-${topic_index}.txt"
    filebeat_capture_offsets \
      "${TOPICS[topic_index]}" "${TOPIC_PARTITIONS[topic_index]}" "${topic_file}" || return 1
    cat "${topic_file}" >>"${output_file}" || return 1
    rm -f -- "${topic_file}" || return 1
  done
}

create_recovery_jobs() {
  local index patch_json inventory pod_identity container_id node_log_path container_link
  local existing_job recovered_identity recovered_uid owner_kind owner_name owner_uid
  local pod_node pod_phase pod_restarts

  "${KUBECTL}" kustomize "${KUSTOMIZE_ACCEPTANCE_OVERLAY}" >"${rendered_file}"
  "${KUBECTL}" set env --local -f "${rendered_file}" \
    "PRODUCER_TEST_RUN_ID=${run_id}" -o yaml >"${configured_file}"
  csplit --silent --prefix="${split_dir}/job-" --suffix-format='%02d.yaml' \
    "${configured_file}" '/^---$/' '{*}'

  for index in "${!JOB_NAMES[@]}"; do
    [[ -s "${split_files[index]}" ]] || fail "验收 Job 渲染结果缺少第 ${index} 份清单"
    patch_json="$(${PYTHON} -c '
import json, sys
name, run_id, purpose = sys.argv[1:]
print(json.dumps([
    {"op": "replace", "path": "/metadata/name", "value": name},
    {"op": "replace", "path": "/metadata/labels/app.kubernetes.io~1instance", "value": name},
    {"op": "replace", "path": "/metadata/labels/distributed-log-platform.io~1purpose", "value": purpose},
    {"op": "add", "path": "/metadata/labels/distributed-log-platform.io~1run-id", "value": run_id},
    {"op": "replace", "path": "/spec/template/metadata/labels/app.kubernetes.io~1instance", "value": name},
    {"op": "replace", "path": "/spec/template/metadata/labels/distributed-log-platform.io~1purpose", "value": purpose},
    {"op": "add", "path": "/spec/template/metadata/labels/distributed-log-platform.io~1run-id", "value": run_id},
]))' "${JOB_NAMES[index]}" "${run_id}" "${RECOVERY_PURPOSE}")"
    "${KUBECTL}" patch --local -f "${split_files[index]}" --type=json \
      -p="${patch_json}" -o yaml >"${job_manifest_files[index]}"

    inventory="$(${KUBECTL} create --dry-run=client -f "${job_manifest_files[index]}" \
      -o 'jsonpath={.kind}{"|"}{.metadata.name}{"|"}{.metadata.namespace}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"|"}{.metadata.labels.distributed-log-platform\.io/run-id}{"|"}{.spec.template.spec.containers[0].env[?(@.name=="PRODUCER_TEST_RUN_ID")].value}')"
    [[ "${inventory}" == "Job|${JOB_NAMES[index]}|${KUBE_NAMESPACE}|${RECOVERY_PURPOSE}|${run_id}|${run_id}" ]] ||
      fail "中断恢复 Job ${JOB_NAMES[index]} 清单边界异常：${inventory}"
    if ! existing_job="$(${KUBECTL} --context="${KUBE_CONTEXT}" get job "${JOB_NAMES[index]}" \
      -n "${KUBE_NAMESPACE}" --ignore-not-found -o name)"; then
      fail "查询中断恢复 Job ${JOB_NAMES[index]} 是否存在时失败"
    fi
    [[ -z "${existing_job}" ]] || fail "中断恢复 Job 已存在，拒绝替换：${JOB_NAMES[index]}"
    "${KUBECTL}" --context="${KUBE_CONTEXT}" create --dry-run=server \
      -f "${job_manifest_files[index]}" >/dev/null
  done

  for index in "${!JOB_NAMES[@]}"; do
    attempted_job_count=$((index + 1))
    if job_uids[index]="$(${KUBECTL} --context="${KUBE_CONTEXT}" create \
      -f "${job_manifest_files[index]}" -o 'jsonpath={.metadata.uid}')"; then
      created_job_count=$((index + 1))
    else
      # EXIT 清理会按唯一名称重试查询，并只接管标签完全匹配的对象。
      fail "中断恢复 Job ${JOB_NAMES[index]} 创建请求失败"
    fi
    if [[ -z "${job_uids[index]}" ]]; then
      recovered_identity="$(query_job_cleanup_identity "${JOB_NAMES[index]}")" ||
        fail "中断恢复 Job ${JOB_NAMES[index]} 创建后无法查询身份"
      recovered_uid="${recovered_identity%%|*}"
      [[ -n "${recovered_uid}" && "${recovered_identity}" == "${recovered_uid}|${RECOVERY_PURPOSE}|${run_id}" ]] ||
        fail "中断恢复 Job ${JOB_NAMES[index]} 创建后缺少可恢复身份"
      job_uids[index]="${recovered_uid}"
    fi
  done

  "${KUBECTL}" --context="${KUBE_CONTEXT}" wait --for=condition=complete \
    "job/${JOB_NAMES[0]}" "job/${JOB_NAMES[1]}" -n "${KUBE_NAMESPACE}" \
    --timeout="${FILEBEAT_OUTAGE_TIMEOUT}"

  for index in "${!JOB_NAMES[@]}"; do
    pod_identity="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pods \
      -n "${KUBE_NAMESPACE}" -l "job-name=${JOB_NAMES[index]}" -o json | "${PYTHON}" -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
if len(items) != 1:
    raise SystemExit(f"中断恢复 Job 必须精确关联 1 个 Pod，实际 {len(items)}")
pod = items[0]
owners = [item for item in pod["metadata"].get("ownerReferences", []) if item.get("controller")]
statuses = {item["name"]: item for item in pod.get("status", {}).get("containerStatuses", [])}
if len(owners) != 1 or "log-producer" not in statuses:
    raise SystemExit("中断恢复 Pod owner 或容器状态不唯一")
owner, status = owners[0], statuses["log-producer"]
values = (
    pod["metadata"]["name"], pod["metadata"]["uid"], owner.get("kind", ""),
    owner.get("name", ""), owner.get("uid", ""), pod["spec"].get("nodeName", ""),
    pod["status"].get("phase", ""), str(status.get("restartCount", "")),
    status.get("containerID", ""),
)
print("|".join(values))')"
    IFS='|' read -r pod_names[index] pod_uids[index] owner_kind owner_name owner_uid \
      pod_node pod_phase pod_restarts container_id <<<"${pod_identity}"
    [[ "${owner_kind}|${owner_name}|${owner_uid}|${pod_node}|${pod_phase}|${pod_restarts}" == \
      "Job|${JOB_NAMES[index]}|${job_uids[index]}|${filebeat_node}|Succeeded|0" ]] ||
      fail "中断恢复 Pod 身份或状态异常：${pod_identity}"
    container_id="${container_id#containerd://}"
    [[ "${container_id}" =~ ^[0-9a-f]{64}$ ]] ||
      fail "中断恢复 Pod 容器 ID 非法：${container_id}"

    "${KUBECTL}" --context="${KUBE_CONTEXT}" logs "${pod_names[index]}" \
      -n "${KUBE_NAMESPACE}" -c log-producer >"${source_files[index]}"
    "${PYTHON}" scripts/validate_log_producer_output.py \
      --input "${source_files[index]}" \
      --service "${SERVICES[index]}" \
      --run-id "${run_id}" \
      --count "${EXPECTED_COUNT}"

    node_log_path="/var/log/pods/${KUBE_NAMESPACE}_${pod_names[index]}_${pod_uids[index]}/log-producer/0.log"
    container_link="/var/log/containers/${pod_names[index]}_${KUBE_NAMESPACE}_log-producer-${container_id}.log"
    "${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${filebeat_pod_name}" \
      -n "${FILEBEAT_NAMESPACE}" -c filebeat -- /bin/sh -ec '
test -L "$1"
test "$(readlink -f "$1")" = "$2"
test "$(grep -Fc "$3" "$2")" -eq "$4"' node-log-check \
      "${container_link}" "${node_log_path}" "${run_id}" "${EXPECTED_COUNT}"
    pod_log_paths[index]="${node_log_path}"
  done
}

wait_for_filebeat_eof() {
  local log_path="$1"
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local evidence

  while true; do
    if evidence="$(${KUBECTL} --context="${KUBE_CONTEXT}" exec "${filebeat_pod_name}" \
      -n "${FILEBEAT_NAMESPACE}" -c filebeat -- /bin/sh -ec '
file="$1"
test -f "$file"
size="$(stat -c %s "$file")"
test "$size" -gt 0
filebeat_pid=""
for comm_file in /proc/[0-9]*/comm; do
  read -r process_name <"$comm_file" 2>/dev/null || continue
  test "$process_name" = "filebeat" || continue
  candidate="${comm_file#/proc/}"
  candidate="${candidate%/comm}"
  test -z "$filebeat_pid" || exit 1
  filebeat_pid="$candidate"
done
test -n "$filebeat_pid"
for descriptor in "/proc/$filebeat_pid/fd/"*; do
  target="$(readlink "$descriptor" 2>/dev/null || true)"
  test "$target" = "$file" || continue
  position=""
  while read -r key value rest; do
    if test "$key" = "pos:"; then
      position="$value"
      break
    fi
  done <"/proc/$filebeat_pid/fdinfo/${descriptor##*/}"
  case "$position" in
    ""|*[!0-9]*) continue ;;
  esac
  if test "$position" -ge "$size"; then
    printf "path=%s pid=%s fd=%s position=%s size=%s\n" "$file" "$filebeat_pid" "${descriptor##*/}" "$position" "$size"
    exit 0
  fi
done
exit 1' filebeat-eof "${log_path}" 2>/dev/null)"; then
      echo "FILEBEAT_EOF ${evidence}"
      return 0
    fi
    ((SECONDS < deadline)) || return 1
    sleep 2
  done
}

capture_recovery_window() {
  local topic_index topic partitions partition key cursor end count

  for topic_index in "${!TOPICS[@]}"; do
    topic="${TOPICS[topic_index]}"
    partitions="${TOPIC_PARTITIONS[topic_index]}"
    filebeat_capture_offsets "${topic}" "${partitions}" "${end_files[topic_index]}"
    for ((partition = 0; partition < partitions; partition += 1)); do
      key="${topic}|${partition}"
      cursor="${cursors[${key}]}"
      end="$(filebeat_get_offset "${end_files[topic_index]}" "${topic}" "${partition}")"
      ((end >= cursor)) || fail "主题 ${topic} 分区 ${partition} 高水位倒退：${cursor} -> ${end}"
      count=$((end - cursor))
      ((count <= FILEBEAT_ACCEPTANCE_MAX_RECORDS)) ||
        fail "主题 ${topic} 分区 ${partition} 恢复窗口新增 ${count} 条，超过安全上限 ${FILEBEAT_ACCEPTANCE_MAX_RECORDS}"
      if ((count > 0)); then
        filebeat_consume_partition_range \
          "${topic}" "${partition}" "${cursor}" "${count}" "${capture_files[topic_index]}"
      fi
      cursors["${key}"]="${end}"
    done
  done
}

routes_validator() {
  local args=(
    "${PYTHON}" scripts/validate_filebeat_kafka_output.py routes
    --run-id "${run_id}"
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

# 写前同时锁定控制器、Pod、PVC、Broker 数据身份、主题位点和采集器进程。
kafka_sts_before="$(kafka_statefulset_identity)"
IFS='|' read -r kafka_sts_uid kafka_generation kafka_observed kafka_replicas kafka_ready \
  kafka_current kafka_updated kafka_current_revision kafka_update_revision kafka_service_name \
  kafka_when_scaled kafka_when_deleted <<<"${kafka_sts_before}"
[[ "${kafka_generation}|${kafka_observed}|${kafka_replicas}|${kafka_ready}|${kafka_current}|${kafka_updated}" == \
  "${kafka_generation}|${kafka_generation}|1|1|1|1" ]] ||
  fail "Kafka StatefulSet 尚未稳定：${kafka_sts_before}"
[[ "${kafka_current_revision}|${kafka_update_revision}|${kafka_service_name}|${kafka_when_scaled}|${kafka_when_deleted}" == \
  "${kafka_current_revision}|${kafka_current_revision}|kafka-headless|Retain|Retain" ]] ||
  fail "Kafka revision、治理 Service 或 PVC 保留策略异常：${kafka_sts_before}"
kafka_revision_before="${kafka_current_revision}"

kafka_pod_before="$(kafka_pod_identity)"
IFS='|' read -r kafka_pod_uid kafka_owner_api kafka_owner_kind kafka_owner_name kafka_owner_uid \
  kafka_node kafka_phase kafka_ready_condition kafka_restarts _ kafka_image_id kafka_claim \
  <<<"${kafka_pod_before}"
[[ "${kafka_owner_api}|${kafka_owner_kind}|${kafka_owner_name}|${kafka_owner_uid}" == \
  "apps/v1|StatefulSet|kafka|${kafka_sts_uid}" ]] ||
  fail "Kafka Pod owner 异常：${kafka_pod_before}"
[[ "${kafka_phase}|${kafka_ready_condition}|${kafka_restarts}|${kafka_claim}" == \
  "Running|True|0|${KAFKA_PVC}" ]] || fail "Kafka Pod 状态或 PVC 挂载异常：${kafka_pod_before}"
kafka_pvc_before="$(kafka_pvc_identity)"
[[ "${kafka_pvc_before}" == *'|Bound|'* ]] || fail "Kafka PVC 尚未 Bound：${kafka_pvc_before}"
kafka_storage_before="$(kafka_storage_identity)"
IFS='|' read -r kafka_meta_sha kafka_meta_cluster kafka_meta_node <<<"${kafka_storage_before}"
[[ "${kafka_meta_node}" == "1" ]] || fail "Kafka meta.properties node.id 异常：${kafka_storage_before}"
[[ "$(kafka_reported_cluster_id)" == "${kafka_meta_cluster}" ]] ||
  fail "Kafka Broker Cluster ID 与 meta.properties 不一致"

filebeat_ds_before="$(filebeat_daemonset_identity)"
IFS='|' read -r filebeat_ds_uid filebeat_generation filebeat_observed filebeat_desired \
  filebeat_ready filebeat_available filebeat_unavailable <<<"${filebeat_ds_before}"
[[ "${filebeat_generation}|${filebeat_observed}|${filebeat_desired}|${filebeat_ready}|${filebeat_available}|${filebeat_unavailable}" == \
  "${filebeat_generation}|${filebeat_generation}|1|1|1|0" ]] ||
  fail "Filebeat DaemonSet 尚未稳定：${filebeat_ds_before}"
filebeat_pod_before="$(filebeat_pod_identity)"
IFS='|' read -r filebeat_pod_name filebeat_pod_uid filebeat_owner_api filebeat_owner_kind \
  filebeat_owner_name filebeat_owner_uid filebeat_node filebeat_phase filebeat_ready_condition \
  filebeat_restarts _ _ <<<"${filebeat_pod_before}"
[[ "${filebeat_owner_api}|${filebeat_owner_kind}|${filebeat_owner_name}|${filebeat_owner_uid}" == \
  "apps/v1|DaemonSet|filebeat|${filebeat_ds_uid}" ]] ||
  fail "Filebeat Pod owner 异常：${filebeat_pod_before}"
[[ "${filebeat_node}|${filebeat_phase}|${filebeat_ready_condition}|${filebeat_restarts}" == \
  "${kafka_node}|Running|True|0" ]] || fail "Filebeat Pod 状态或节点异常：${filebeat_pod_before}"

"${repo_root}/scripts/verify-kafka-image.sh" pod
"${repo_root}/scripts/verify-filebeat-image.sh" pod
kubectl_diff_clean "${KUSTOMIZE_KAFKA_OVERLAY}" "${work_dir}/kafka-diff-before.txt" ||
  fail "Kafka overlay 在故障注入前存在漂移"
kubectl_diff_clean "${KUSTOMIZE_FILEBEAT_OVERLAY}" "${work_dir}/filebeat-diff-before.txt" ||
  fail "Filebeat overlay 在故障注入前存在漂移"
assert_no_recovery_resources || fail "存在上一次遗留的中断恢复 Job"
capture_all_offsets "${outage_baseline}" || fail "无法记录 Kafka 中断前四主题位点"

pre_probe="${work_dir}/probe-before.txt"
run_expected_positive_probe "${pre_probe}" || {
  cat "${pre_probe}" >&2
  fail "故障注入前 Filebeat Kafka 输出探针失败"
}

echo "缩容 Kafka StatefulSet：uid=${kafka_sts_uid} old_pod_uid=${kafka_pod_uid} pvc=${kafka_pvc_before}"
scale_kafka_down || fail "Kafka StatefulSet 1→0 原子缩容失败"
wait_for_kafka_down || fail "Kafka Pod 未在限定时间内完全退出"
[[ "$(kafka_pvc_identity)" == "${kafka_pvc_before}" ]] || fail "Kafka 缩容后 PVC 身份发生变化"

down_probe="${work_dir}/probe-down.txt"
run_expected_failure_probe "${down_probe}" || {
  cat "${down_probe}" >&2
  fail "Kafka Pod 缺席时 Filebeat 输出探针未证明网络不可达"
}
outage_confirmed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
outage_confirmed_epoch="$(date +%s)"

echo "在 Kafka Pod 缺席窗口内创建日志批次：test_run_id=${run_id}"
create_recovery_jobs
jobs_completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
for index in "${!pod_log_paths[@]}"; do
  wait_for_filebeat_eof "${pod_log_paths[index]}" ||
    fail "Filebeat 未在 Kafka 缺席窗口内读完精确源文件：${pod_log_paths[index]}"
done

wait_for_kafka_down || fail "读取源日志后 Kafka Pod 已意外恢复"
[[ "$(filebeat_pod_identity)" == "${filebeat_pod_before}" ]] ||
  fail "Kafka 缺席窗口内 Filebeat Pod 或容器身份发生变化"
[[ "$(kafka_pvc_identity)" == "${kafka_pvc_before}" ]] ||
  fail "Kafka 缺席窗口内 PVC 身份发生变化"

down_probe_after_jobs="${work_dir}/probe-down-after-jobs.txt"
run_expected_failure_probe "${down_probe_after_jobs}" || {
  cat "${down_probe_after_jobs}" >&2
  fail "源日志读完后 Filebeat 输出探针未证明 Kafka 仍不可达"
}

restore_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
restore_started_epoch="$(date +%s)"
restore_kafka_safely || fail "Kafka StatefulSet 安全恢复失败"
wait_for_output_recovery || fail "Kafka 恢复后 Filebeat 输出未恢复"

kafka_sts_after="$(kafka_statefulset_identity)"
IFS='|' read -r kafka_after_uid kafka_after_generation kafka_after_observed kafka_after_replicas \
  kafka_after_ready kafka_after_current kafka_after_updated kafka_after_current_revision \
  kafka_after_update_revision kafka_after_service kafka_after_when_scaled kafka_after_when_deleted \
  <<<"${kafka_sts_after}"
[[ "${kafka_after_uid}|${kafka_after_generation}|${kafka_after_observed}|${kafka_after_replicas}|${kafka_after_ready}|${kafka_after_current}|${kafka_after_updated}" == \
  "${kafka_sts_uid}|${kafka_after_generation}|${kafka_after_generation}|1|1|1|1" ]] ||
  fail "Kafka 恢复后 StatefulSet 状态异常：${kafka_sts_after}"
[[ "${kafka_after_current_revision}|${kafka_after_update_revision}|${kafka_after_service}|${kafka_after_when_scaled}|${kafka_after_when_deleted}" == \
  "${kafka_revision_before}|${kafka_revision_before}|kafka-headless|Retain|Retain" ]] ||
  fail "Kafka 恢复后 revision 或保留策略发生变化：${kafka_sts_after}"

kafka_pod_after="$(kafka_pod_identity)"
IFS='|' read -r kafka_new_pod_uid new_owner_api new_owner_kind new_owner_name new_owner_uid \
  new_kafka_node new_kafka_phase new_kafka_ready new_kafka_restarts _ new_kafka_image_id new_kafka_claim \
  <<<"${kafka_pod_after}"
[[ "${new_owner_api}|${new_owner_kind}|${new_owner_name}|${new_owner_uid}" == \
  "apps/v1|StatefulSet|kafka|${kafka_sts_uid}" ]] || fail "恢复后 Kafka Pod owner 异常：${kafka_pod_after}"
[[ "${new_kafka_node}|${new_kafka_phase}|${new_kafka_ready}|${new_kafka_restarts}|${new_kafka_claim}" == \
  "${kafka_node}|Running|True|0|${KAFKA_PVC}" ]] || fail "恢复后 Kafka Pod 状态或 PVC 挂载异常：${kafka_pod_after}"
[[ "${kafka_new_pod_uid}" != "${kafka_pod_uid}" ]] || fail "Kafka 恢复后 Pod UID 未改变"
[[ "${new_kafka_image_id}" == "${kafka_image_id}" ]] || fail "Kafka 恢复后运行时镜像身份改变"
[[ "$(kafka_pvc_identity)" == "${kafka_pvc_before}" ]] || fail "Kafka 恢复后 PVC 身份改变"
[[ "$(kafka_storage_identity)" == "${kafka_storage_before}" ]] || fail "Kafka 恢复后 meta.properties 身份改变"
[[ "$(kafka_reported_cluster_id)" == "${kafka_meta_cluster}" ]] || fail "Kafka 恢复后 Cluster ID 改变"
"${repo_root}/scripts/verify-kafka-image.sh" pod

for topic_index in "${!TOPICS[@]}"; do
  capture_files[topic_index]="${work_dir}/capture-${topic_index}.txt"
  end_files[topic_index]="${work_dir}/end-${topic_index}.txt"
  : >"${capture_files[topic_index]}"
  for ((partition = 0; partition < TOPIC_PARTITIONS[topic_index]; partition += 1)); do
    cursors["${TOPICS[topic_index]}|${partition}"]="$(
      filebeat_get_offset "${outage_baseline}" "${TOPICS[topic_index]}" "${partition}"
    )"
  done
done

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
    fail "Kafka 短停恢复批次违反 Filebeat 路由契约"
  fi
  [[ "${validator_status}" -eq 2 ]] || fail "Filebeat 路由校验器异常退出：${validator_status}"
  if ((SECONDS >= deadline)); then
    cat "${validator_stderr}" >&2
    fail "Kafka 短停恢复批次未在 ${FILEBEAT_OUTAGE_TIMEOUT} 内收齐"
  fi
  sleep 2
done

recovery_complete_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
recovery_complete_epoch="$(date +%s)"
sleep "${FILEBEAT_OUTAGE_SETTLE_SECONDS}"
capture_recovery_window
routes_validator >"${validator_stdout}" 2>"${validator_stderr}" || {
  cat "${validator_stderr}" >&2
  fail "Kafka 短停恢复稳定观察窗口出现缺失、冲突重复或错误路由"
}
cat "${validator_stdout}"

[[ "$(filebeat_daemonset_identity)" == "${filebeat_ds_before}" ]] ||
  fail "恢复后 Filebeat DaemonSet 状态发生变化"
[[ "$(filebeat_pod_identity)" == "${filebeat_pod_before}" ]] ||
  fail "恢复后 Filebeat Pod 或容器身份发生变化"
[[ "$(kafka_statefulset_identity)" == "${kafka_sts_after}" ]] ||
  fail "稳定观察窗口内 Kafka StatefulSet 状态发生变化"
[[ "$(kafka_pod_identity)" == "${kafka_pod_after}" ]] ||
  fail "稳定观察窗口内 Kafka Pod 身份或状态发生变化"
[[ "$(kafka_pvc_identity)" == "${kafka_pvc_before}" ]] ||
  fail "稳定观察窗口内 Kafka PVC 身份发生变化"
[[ "$(kafka_storage_identity)" == "${kafka_storage_before}" ]] ||
  fail "稳定观察窗口内 Kafka 数据身份发生变化"

for topic_index in "${!TOPICS[@]}"; do
  for ((partition = 0; partition < TOPIC_PARTITIONS[topic_index]; partition += 1)); do
    start="$(filebeat_get_offset "${outage_baseline}" "${TOPICS[topic_index]}" "${partition}")"
    end="${cursors["${TOPICS[topic_index]}|${partition}"]}"
    echo "OUTAGE_RANGE topic=${TOPICS[topic_index]} partition=${partition} start=${start} end=${end}"
  done
done

cleanup_created_jobs || fail "中断恢复 Job 清理失败"
assert_no_recovery_resources || fail "中断恢复 Job 清理后仍有残留"
kubectl_diff_clean "${KUSTOMIZE_KAFKA_OVERLAY}" "${work_dir}/kafka-diff-after.txt" ||
  fail "Kafka 短停恢复后 Kafka overlay 存在漂移"
kubectl_diff_clean "${KUSTOMIZE_FILEBEAT_OVERLAY}" "${work_dir}/filebeat-diff-after.txt" ||
  fail "Kafka 短停恢复后 Filebeat overlay 存在漂移"

echo "Filebeat Kafka 短停恢复验收通过：test_run_id=${run_id} filebeat_pod_uid=${filebeat_pod_uid} old_kafka_pod_uid=${kafka_pod_uid} new_kafka_pod_uid=${kafka_new_pod_uid} pvc=${kafka_pvc_before} cluster_id=${kafka_meta_cluster} jobs_completed_at=${jobs_completed_at} outage_confirmed_at=${outage_confirmed_at} restore_started_at=${restore_started_at} outage_seconds=$((restore_started_epoch - outage_confirmed_epoch)) recovery_seconds=$((recovery_complete_epoch - restore_started_epoch)) recovery_complete_at=${recovery_complete_at}"
