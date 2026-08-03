#!/usr/bin/env bash
set -Eeuo pipefail

readonly JOBS=(
  "api-service-acceptance"
  "worker-service-acceptance"
)
readonly SERVICES=(
  "api-service"
  "worker-service"
)
readonly EXPECTED_COUNT=20
readonly PREFLIGHT_PATCHES=(
  '[{"op":"add","path":"/metadata/generateName","value":"api-service-acceptance-preflight-"},{"op":"remove","path":"/metadata/name"}]'
  '[{"op":"add","path":"/metadata/generateName","value":"worker-service-acceptance-preflight-"},{"op":"remove","path":"/metadata/name"}]'
)
PODS=()
IMAGES=()
IMAGE_IDS=()

KUBECTL="${KUBECTL:-kubectl}"
PYTHON="${PYTHON:-python3}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
KUSTOMIZE_ACCEPTANCE_OVERLAY="${KUSTOMIZE_ACCEPTANCE_OVERLAY:-deploy/kubernetes/overlays/local-acceptance}"
ACCEPTANCE_TIMEOUT="${ACCEPTANCE_TIMEOUT:-60s}"

# 从任意工作目录调用时都回到仓库根目录，保证相对路径含义稳定。
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

for command_name in "${KUBECTL}" "${PYTHON}" csplit flock readlink; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "缺少验收命令：${command_name}" >&2
    exit 1
  fi
done

if [[ -z "${ACCEPTANCE_RUN_ID:-}" ]]; then
  ACCEPTANCE_RUN_ID="uc001a-$(
    "${PYTHON}" -c 'import uuid; print(uuid.uuid4().hex)'
  )"
fi

# 仅接受 Kubernetes 标签安全字符，便于后续把批次 ID 用于资源元数据。
if [[ ! "${ACCEPTANCE_RUN_ID}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$ ]]; then
  echo "ACCEPTANCE_RUN_ID 必须是 1..63 位标签安全字符" >&2
  exit 1
fi
if [[ "${ACCEPTANCE_RUN_ID}" == "day2-continuous-v1" ]]; then
  echo "验收批次 ID 不能复用持续 Deployment 的标识" >&2
  exit 1
fi

# 固定名 Job 与 Filebeat 位点窗口必须共享互斥边界，避免采集验收中途被替换。
source "${repo_root}/scripts/lib/filebeat-workflow-lock.sh"
acquire_filebeat_workflow_lock || exit 1
export FILEBEAT_WORKFLOW_LOCK_INHERITED=1

actual_context="$("${KUBECTL}" config current-context)"
if [[ "${actual_context}" != "${KUBE_CONTEXT}" ]]; then
  echo "Kubernetes 上下文不匹配：实际 ${actual_context}，要求 ${KUBE_CONTEXT}" >&2
  exit 1
fi

"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  get namespace "${KUBE_NAMESPACE}" \
  >/dev/null

umask 077
rendered_file="$(mktemp)"
configured_file="$(mktemp)"
split_dir="$(mktemp -d)"
split_files=(
  "${split_dir}/job-00.yaml"
  "${split_dir}/job-01.yaml"
)
preflight_files=("$(mktemp)" "$(mktemp)")
log_files=("$(mktemp)" "$(mktemp)")

cleanup() {
  rm -f -- \
    "${rendered_file}" \
    "${configured_file}" \
    "${split_files[@]}" \
    "${preflight_files[@]}" \
    "${log_files[@]}"
  rmdir -- "${split_dir}"
}
trap cleanup EXIT

"${KUBECTL}" \
  kustomize "${KUSTOMIZE_ACCEPTANCE_OVERLAY}" \
  >"${rendered_file}"

# kubectl 负责结构化修改两个 Job，避免 sed 拼接 YAML 或注入特殊字符。
"${KUBECTL}" \
  set env \
  --local \
  -f "${rendered_file}" \
  "PRODUCER_TEST_RUN_ID=${ACCEPTANCE_RUN_ID}" \
  -o yaml \
  >"${configured_file}"

inventory="$(
  "${KUBECTL}" \
    create \
    --dry-run=client \
    -f "${configured_file}" \
    -o 'jsonpath={.kind}{"|"}{.metadata.name}{"|"}{.metadata.namespace}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}{"\n"}'
)"
expected_inventory="$(
  printf \
    'Job|api-service-acceptance|%s|acceptance\nJob|worker-service-acceptance|%s|acceptance' \
    "${KUBE_NAMESPACE}" \
    "${KUBE_NAMESPACE}"
)"
if [[ "${inventory}" != "${expected_inventory}" ]]; then
  echo "验收清单资源集合不符合预期：" >&2
  printf '%s\n' "${inventory}" >&2
  exit 1
fi

configured_run_ids="$(
  "${KUBECTL}" \
    create \
    --dry-run=client \
    -f "${configured_file}" \
    -o 'jsonpath={.spec.template.spec.containers[0].env[?(@.name=="PRODUCER_TEST_RUN_ID")].value}{"\n"}'
)"
expected_run_ids="$(printf '%s\n%s' "${ACCEPTANCE_RUN_ID}" "${ACCEPTANCE_RUN_ID}")"
if [[ "${configured_run_ids}" != "${expected_run_ids}" ]]; then
  echo "两个 Job 未获得同一个本次批次 ID" >&2
  exit 1
fi

csplit \
  --silent \
  --prefix="${split_dir}/job-" \
  --suffix-format='%02d.yaml' \
  "${configured_file}" \
  '/^---$/' \
  '{*}'

# 使用临时 generateName 让服务端在保留旧 Job 证据时校验完整 Pod 模板。
for index in "${!JOBS[@]}"; do
  "${KUBECTL}" \
    patch \
    --local \
    -f "${split_files[index]}" \
    --type=json \
    -p="${PREFLIGHT_PATCHES[index]}" \
    -o yaml \
    >"${preflight_files[index]}"

  "${KUBECTL}" \
    --context="${KUBE_CONTEXT}" \
    create \
    --dry-run=server \
    -f "${preflight_files[index]}" \
    >/dev/null
done

for job in "${JOBS[@]}"; do
  existing="$(
    "${KUBECTL}" \
      --context="${KUBE_CONTEXT}" \
      get job "${job}" \
      -n "${KUBE_NAMESPACE}" \
      --ignore-not-found \
      -o name
  )"
  if [[ -z "${existing}" ]]; then
    continue
  fi

  purpose="$(
    "${KUBECTL}" \
      --context="${KUBE_CONTEXT}" \
      get job "${job}" \
      -n "${KUBE_NAMESPACE}" \
      -o 'jsonpath={.metadata.labels.distributed-log-platform\.io/purpose}'
  )"
  if [[ "${purpose}" != "acceptance" ]]; then
    echo "同名 Job ${job} 不属于本验收流程，拒绝删除" >&2
    exit 1
  fi

  complete="$(
    "${KUBECTL}" \
      --context="${KUBE_CONTEXT}" \
      get job "${job}" \
      -n "${KUBE_NAMESPACE}" \
      -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}'
  )"
  failed="$(
    "${KUBECTL}" \
      --context="${KUBE_CONTEXT}" \
      get job "${job}" \
      -n "${KUBE_NAMESPACE}" \
      -o 'jsonpath={.status.conditions[?(@.type=="Failed")].status}'
  )"
  if [[ "${complete}" != "True" && "${failed}" != "True" ]]; then
    echo "Job ${job} 尚未结束，拒绝替换" >&2
    exit 1
  fi
done

"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  delete job "${JOBS[@]}" \
  -n "${KUBE_NAMESPACE}" \
  --ignore-not-found=true \
  --cascade=foreground \
  --wait=true

# 删除旧终态 Job 后再由目标 API Server 验证，避免不可变模板造成假失败。
"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  create \
  --dry-run=server \
  -f "${configured_file}" \
  >/dev/null

echo "启动 log-producer 验收：test_run_id=${ACCEPTANCE_RUN_ID}"
"${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  create \
  -f "${configured_file}"

if "${KUBECTL}" \
  --context="${KUBE_CONTEXT}" \
  wait \
  --for=condition=complete \
  "job/${JOBS[0]}" \
  "job/${JOBS[1]}" \
  -n "${KUBE_NAMESPACE}" \
  --timeout="${ACCEPTANCE_TIMEOUT}"; then
  :
else
  wait_status=$?
  echo "验收 Job 未在 ${ACCEPTANCE_TIMEOUT} 内全部完成" >&2
  set +e
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get jobs,pods \
    -n "${KUBE_NAMESPACE}" \
    -l distributed-log-platform.io/purpose=acceptance \
    -o wide >&2
  for job in "${JOBS[@]}"; do
    "${KUBECTL}" --context="${KUBE_CONTEXT}" logs \
      -n "${KUBE_NAMESPACE}" \
      "job/${job}" \
      -c log-producer >&2
  done
  set -e
  exit "${wait_status}"
fi

# Job Complete 之外再核对单 Pod、零失败和零重启，避免只凭聚合状态验收。
for index in "${!JOBS[@]}"; do
  job_status="$(
    "${KUBECTL}" \
      --context="${KUBE_CONTEXT}" \
      get job "${JOBS[index]}" \
      -n "${KUBE_NAMESPACE}" \
      -o 'jsonpath={.status.succeeded}{"|"}{.status.failed}'
  )"
  IFS="|" read -r succeeded failed <<<"${job_status}"
  failed="${failed:-0}"
  if [[ "${succeeded}" != "1" || "${failed}" != "0" ]]; then
    echo \
      "Job ${JOBS[index]} 状态异常：succeeded=${succeeded:-0} failed=${failed}" \
      >&2
    exit 1
  fi

  pod_output="$(
    "${KUBECTL}" \
      --context="${KUBE_CONTEXT}" \
      get pods \
      -n "${KUBE_NAMESPACE}" \
      -l "job-name=${JOBS[index]}" \
      -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'
  )"
  pod_names=()
  if [[ -n "${pod_output}" ]]; then
    mapfile -t pod_names <<<"${pod_output}"
  fi
  if [[ "${#pod_names[@]}" -ne 1 ]]; then
    echo "Job ${JOBS[index]} 关联 ${#pod_names[@]} 个 Pod，期望 1 个" >&2
    exit 1
  fi
  PODS[index]="${pod_names[0]}"

  pod_status="$(
    "${KUBECTL}" \
      --context="${KUBE_CONTEXT}" \
      get pod "${PODS[index]}" \
      -n "${KUBE_NAMESPACE}" \
      -o 'jsonpath={.status.phase}{"|"}{.status.containerStatuses[?(@.name=="log-producer")].restartCount}{"|"}{.spec.containers[?(@.name=="log-producer")].image}{"|"}{.status.containerStatuses[?(@.name=="log-producer")].imageID}'
  )"
  IFS="|" read -r phase restart_count image image_id <<<"${pod_status}"
  if [[ "${phase}" != "Succeeded" || "${restart_count}" != "0" ]]; then
    echo \
      "Pod ${PODS[index]} 状态异常：phase=${phase} restarts=${restart_count}" \
      >&2
    exit 1
  fi
  if [[ -z "${image}" || -z "${image_id}" ]]; then
    echo "Pod ${PODS[index]} 缺少镜像身份" >&2
    exit 1
  fi
  IMAGES[index]="${image}"
  IMAGE_IDS[index]="${image_id}"
  echo \
    "${JOBS[index]}：单 Pod、成功、0 重启，image=${image}"
done

if [[ "${IMAGES[0]}" != "${IMAGES[1]}" || "${IMAGE_IDS[0]}" != "${IMAGE_IDS[1]}" ]]; then
  echo "两个验收 Job 没有运行同一镜像" >&2
  exit 1
fi
if [[ "${IMAGES[0]}" == *":latest" || "${IMAGES[0]}" == *":unconfigured" ]]; then
  echo "验收 Job 使用了未固定镜像：${IMAGES[0]}" >&2
  exit 1
fi

for index in "${!JOBS[@]}"; do
  "${KUBECTL}" \
    --context="${KUBE_CONTEXT}" \
    logs \
    -n "${KUBE_NAMESPACE}" \
    "pod/${PODS[index]}" \
    -c log-producer \
    >"${log_files[index]}"

  "${PYTHON}" scripts/validate_log_producer_output.py \
    --input "${log_files[index]}" \
    --service "${SERVICES[index]}" \
    --run-id "${ACCEPTANCE_RUN_ID}" \
    --count "${EXPECTED_COUNT}"
done

echo "log-producer 验收通过：test_run_id=${ACCEPTANCE_RUN_ID}"
