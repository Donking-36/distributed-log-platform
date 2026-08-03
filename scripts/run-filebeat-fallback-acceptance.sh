#!/usr/bin/env bash
set -Eeuo pipefail

readonly TOPIC="logs.unclassified"
readonly PARTITION=0
readonly KAFKA_POD="kafka-0"
readonly KAFKA_CONTAINER="kafka"
readonly MINIKUBE_CONTAINER="stage3-logs"

DOCKER="${DOCKER:-docker}"
KUBECTL="${KUBECTL:-kubectl}"
PYTHON="${PYTHON:-python3}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
FILEBEAT_NAMESPACE="${FILEBEAT_NAMESPACE:-stage3-collector}"
FILEBEAT_VERSION="${FILEBEAT_VERSION:-9.4.4}"
FILEBEAT_FALLBACK_TIMEOUT="${FILEBEAT_FALLBACK_TIMEOUT:-120s}"
FILEBEAT_ACCEPTANCE_SETTLE_SECONDS="${FILEBEAT_ACCEPTANCE_SETTLE_SECONDS:-5}"

fail() {
  echo "$1" >&2
  exit 1
}

[[ "${FILEBEAT_FALLBACK_TIMEOUT}" =~ ^([1-9][0-9]*)s$ ]] ||
  fail "FILEBEAT_FALLBACK_TIMEOUT 必须是正整数秒，例如 120s"
readonly TIMEOUT_SECONDS="${BASH_REMATCH[1]}"
[[ "${FILEBEAT_ACCEPTANCE_SETTLE_SECONDS}" =~ ^[1-9][0-9]*$ ]] ||
  fail "FILEBEAT_ACCEPTANCE_SETTLE_SECONDS 必须是正整数"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

for command_name in "${DOCKER}" "${KUBECTL}" "${PYTHON}" flock readlink wc; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

if [[ -z "${FALLBACK_RUN_ID:-}" ]]; then
  FALLBACK_RUN_ID="uc001-fallback-$(${PYTHON} -c 'import uuid; print(uuid.uuid4().hex)')"
fi
[[ "${FALLBACK_RUN_ID}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$ ]] ||
  fail "FALLBACK_RUN_ID 必须是 1..63 位安全字符"

# 兜底 fixture 与固定名 Job、采集器重建共享互斥边界。
source "${repo_root}/scripts/lib/filebeat-workflow-lock.sh"
source "${repo_root}/scripts/lib/filebeat-kafka-evidence.sh"
acquire_filebeat_workflow_lock || exit 1
export FILEBEAT_WORKFLOW_LOCK_INHERITED=1

actual_context="$(${KUBECTL} config current-context)"
[[ "${actual_context}" == "${KUBE_CONTEXT}" ]] ||
  fail "Kubernetes 上下文不匹配：实际 ${actual_context}，要求 ${KUBE_CONTEXT}"
filebeat_status="$(${KUBECTL} --context="${KUBE_CONTEXT}" get daemonset filebeat \
  -n "${FILEBEAT_NAMESPACE}" \
  -o 'jsonpath={.status.desiredNumberScheduled}{"|"}{.status.numberReady}{"|"}{.status.numberAvailable}')"
[[ "${filebeat_status}" == "1|1|1" ]] || fail "Filebeat 尚未就绪：${filebeat_status}"

# 写节点临时日志前精确核对 Minikube 容器身份，避免名称碰撞时修改错误目标。
minikube_facts="$(${DOCKER} inspect "${MINIKUBE_CONTAINER}" \
  --format '{{index .Config.Labels "created_by.minikube.sigs.k8s.io"}}|{{index .Config.Labels "mode.minikube.sigs.k8s.io"}}|{{index .Config.Labels "name.minikube.sigs.k8s.io"}}')"
[[ "${minikube_facts}" == "true|${KUBE_CONTEXT}|${KUBE_CONTEXT}" ]] ||
  fail "Minikube 容器身份异常：${minikube_facts}"

umask 077
work_dir="$(mktemp -d)"
source_file="${work_dir}/source.log"
cri_file="${work_dir}/container.log"
capture_file="${work_dir}/capture.log"
validator_stdout="${work_dir}/validator.out"
validator_stderr="${work_dir}/validator.err"
fixture_created=0

container_id="$(${PYTHON} -c 'import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' "${FALLBACK_RUN_ID}")"
fixture_pod_name="filebeat-fallback-${container_id:0:12}"
anchor_identity="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pods \
  -n "${KUBE_NAMESPACE}" -l app.kubernetes.io/instance=api-service -o json | "${PYTHON}" -c '
import json, sys
items = [item for item in json.load(sys.stdin).get("items", []) if item.get("status", {}).get("phase") == "Running"]
if len(items) != 1:
    raise SystemExit(f"兜底 fixture 必须精确找到 1 个 Running api-service Pod，实际 {len(items)}")
pod = items[0]
names = {item["name"] for item in pod["spec"].get("containers", [])}
if "log-producer" not in names:
    raise SystemExit("锚点 Pod 缺少 log-producer 容器")
print("|".join((pod["metadata"]["name"], pod["metadata"]["uid"], pod["spec"]["nodeName"])))')"
IFS='|' read -r anchor_pod_name anchor_pod_uid anchor_node <<<"${anchor_identity}"
[[ "${anchor_node}" == "${KUBE_CONTEXT}" ]] || fail "锚点 Pod 不在目标 Minikube 节点：${anchor_identity}"
anchor_container_directory="/var/log/pods/${KUBE_NAMESPACE}_${anchor_pod_name}_${anchor_pod_uid}/log-producer"
node_log_file="${anchor_container_directory}/filebeat-fallback-${container_id}.log"
filebeat_log_path="/var/log/containers/${fixture_pod_name}_${KUBE_NAMESPACE}_log-producer-${container_id}.log"

cleanup() {
  set +e
  if [[ "${fixture_created}" -eq 1 ]]; then
    "${DOCKER}" exec "${MINIKUBE_CONTAINER}" rm -f -- "${filebeat_log_path}" "${node_log_file}"
  fi
  rm -f -- "${work_dir}"/*
  rmdir -- "${work_dir}"
}
trap cleanup EXIT

get_end_offset() {
  local output line prefix offset
  output="$(filebeat_kafka_exec /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "${TOPIC}")"
  prefix="${TOPIC}:${PARTITION}:"
  line="$(printf '%s\n' "${output}" | grep -F "${prefix}" || true)"
  [[ -n "${line}" && "${line}" != *$'\n'* ]] || fail "无法唯一解析 ${TOPIC} 位点"
  offset="${line#${prefix}}"
  [[ "${offset}" =~ ^[0-9]+$ ]] || fail "${TOPIC} 位点非法：${offset}"
  printf '%s' "${offset}"
}

consume_to_end() {
  local end count
  end="$(get_end_offset)"
  ((end >= cursor)) || fail "${TOPIC} 高水位倒退：${cursor} -> ${end}"
  count=$((end - cursor))
  if ((count > 0)); then
    filebeat_consume_partition_range \
      "${TOPIC}" "${PARTITION}" "${cursor}" "${count}" "${capture_file}"
    cursor="${end}"
  fi
}

validator_command() {
  "${PYTHON}" scripts/validate_filebeat_kafka_output.py fallback \
    --capture "${capture_file}" \
    --source "${source_file}" \
    --run-id "${FALLBACK_RUN_ID}" \
    --expected-path "${filebeat_log_path}" \
    --filebeat-version "${FILEBEAT_VERSION}"
}

timestamp="$(${PYTHON} -c 'from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00","Z"))')"
"${PYTHON}" -c 'import json,sys; print(json.dumps({"@timestamp":sys.argv[2],"event.sequence":1,"log.level":"INFO","message":"metadata fallback event","padding":"x"*1400,"service.name":"fallback-service","test_run_id":sys.argv[1]},separators=(",",":")))' \
  "${FALLBACK_RUN_ID}" "${timestamp}" >"${source_file}"
printf '%s stdout F %s\n' "${timestamp}" "$(<"${source_file}")" >"${cri_file}"
[[ "$(wc -c <"${cri_file}")" -gt 1024 ]] || fail "兜底 CRI fixture 未达到 filestream fingerprint 窗口"
: >"${capture_file}"
start_offset="$(get_end_offset)"
cursor="${start_offset}"

"${DOCKER}" exec "${MINIKUBE_CONTAINER}" /bin/sh -ec \
  'test ! -e "$1" && test ! -L "$1" && test -d "$2" && test ! -e "$3"' \
  fixture-check "${filebeat_log_path}" "${anchor_container_directory}" "${node_log_file}"
fixture_created=1
"${DOCKER}" cp "${cri_file}" "${MINIKUBE_CONTAINER}:${node_log_file}"
"${DOCKER}" exec "${MINIKUBE_CONTAINER}" chown 0:0 "${node_log_file}"
"${DOCKER}" exec "${MINIKUBE_CONTAINER}" chmod 0640 "${node_log_file}"
"${DOCKER}" exec "${MINIKUBE_CONTAINER}" ln -s "${node_log_file}" "${filebeat_log_path}"

echo "启动 Filebeat 元数据缺失兜底验收：test_run_id=${FALLBACK_RUN_ID}"
deadline=$((SECONDS + TIMEOUT_SECONDS))
while true; do
  consume_to_end
  if validator_command >"${validator_stdout}" 2>"${validator_stderr}"; then
    break
  else
    validator_status=$?
  fi
  if [[ "${validator_status}" -eq 1 ]]; then
    cat "${validator_stderr}" >&2
    fail "Filebeat 未分类兜底事件违反契约"
  fi
  [[ "${validator_status}" -eq 2 ]] || fail "兜底校验器异常退出：${validator_status}"
  if ((SECONDS >= deadline)); then
    cat "${validator_stderr}" >&2
    fail "Filebeat 未在 ${FILEBEAT_FALLBACK_TIMEOUT} 内发布兜底事件"
  fi
  sleep 2
done

sleep "${FILEBEAT_ACCEPTANCE_SETTLE_SECONDS}"
consume_to_end
if ! validator_command >"${validator_stdout}" 2>"${validator_stderr}"; then
  cat "${validator_stderr}" >&2
  fail "Filebeat 兜底稳定观察窗口出现契约错误"
fi

cat "${validator_stdout}"
echo "KAFKA_RANGE topic=${TOPIC} partition=${PARTITION} start=${start_offset} end=${cursor}"
echo "Filebeat 元数据缺失兜底验收通过：test_run_id=${FALLBACK_RUN_ID}"
