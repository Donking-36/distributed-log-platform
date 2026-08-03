#!/usr/bin/env bash
set -Eeuo pipefail

readonly MODE="${1:-run}"
readonly PURPOSE="log-collection"
readonly DAEMONSET_NAME="filebeat"
readonly SERVICE_ACCOUNT_NAME="filebeat"
readonly ROLE_NAME="filebeat-pod-metadata-reader"
readonly CONTAINER_NAME="filebeat"
readonly DIGEST_PATTERN='^sha256:[0-9a-f]{64}$'

KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
FILEBEAT_NAMESPACE="${FILEBEAT_NAMESPACE:-stage3-collector}"
KUSTOMIZE_FILEBEAT_OVERLAY="${KUSTOMIZE_FILEBEAT_OVERLAY:-deploy/kubernetes/overlays/local-filebeat}"
FILEBEAT_ROLLOUT_TIMEOUT="${FILEBEAT_ROLLOUT_TIMEOUT:-180s}"
FILEBEAT_NODE_IMAGE="${FILEBEAT_NODE_IMAGE:-docker.elastic.co/beats/filebeat-wolfi:9.4.4}"
EXPECTED_FILEBEAT_UPSTREAM_INDEX_DIGEST="${EXPECTED_FILEBEAT_UPSTREAM_INDEX_DIGEST:-}"
EXPECTED_FILEBEAT_UPSTREAM_AMD64_DIGEST="${EXPECTED_FILEBEAT_UPSTREAM_AMD64_DIGEST:-}"
EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST="${EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST:-}"
EXPECTED_FILEBEAT_CONFIG_DIGEST="${EXPECTED_FILEBEAT_CONFIG_DIGEST:-}"

fail() {
  echo "$1" >&2
  exit 1
}

require_digest() {
  local name="$1"
  local value="$2"

  [[ "${value}" =~ ${DIGEST_PATTERN} ]] || fail "${name} 必须是完整的 sha256 摘要"
}

assert_line_count() {
  local pattern="$1"
  local expected="$2"
  local description="$3"
  local actual

  actual="$(grep -Ec "${pattern}" "${rendered_config}" || true)"
  [[ "${actual}" -eq "${expected}" ]] ||
    fail "${description} 数量异常：实际 ${actual}，要求 ${expected}"
}

[[ "${MODE}" == "validate" || "${MODE}" == "run" ]] || fail "用法：$0 validate|run"
[[ "${FILEBEAT_ROLLOUT_TIMEOUT}" =~ ^([1-9][0-9]*)s$ ]] ||
  fail "FILEBEAT_ROLLOUT_TIMEOUT 必须是正整数秒，例如 180s"
require_digest "EXPECTED_FILEBEAT_UPSTREAM_INDEX_DIGEST" "${EXPECTED_FILEBEAT_UPSTREAM_INDEX_DIGEST}"
require_digest "EXPECTED_FILEBEAT_UPSTREAM_AMD64_DIGEST" "${EXPECTED_FILEBEAT_UPSTREAM_AMD64_DIGEST}"
require_digest "EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST" "${EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST}"
require_digest "EXPECTED_FILEBEAT_CONFIG_DIGEST" "${EXPECTED_FILEBEAT_CONFIG_DIGEST}"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

for command_name in "${KUBECTL}" csplit flock grep cmp readlink; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "缺少命令：${command_name}"
done

actual_context="$(${KUBECTL} config current-context)"
[[ "${actual_context}" == "${KUBE_CONTEXT}" ]] ||
  fail "Kubernetes 上下文不匹配：实际 ${actual_context}，要求 ${KUBE_CONTEXT}"

"${KUBECTL}" --context="${KUBE_CONTEXT}" get namespace "${KUBE_NAMESPACE}" >/dev/null
broker_status="$(${KUBECTL} --context="${KUBE_CONTEXT}" get statefulset kafka -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={.spec.replicas}{"|"}{.status.readyReplicas}{"|"}{.status.currentReplicas}')"
[[ "${broker_status}" == "1|1|1" ]] || fail "Kafka 尚未就绪：replicas|ready|current=${broker_status}"
"${KUBECTL}" --context="${KUBE_CONTEXT}" get service kafka -n "${KUBE_NAMESPACE}" >/dev/null

umask 077
work_dir="$(mktemp -d)"
rendered_file="${work_dir}/rendered.yaml"
rendered_config="${work_dir}/filebeat.yml"
namespace_file=""
config_map_file=""
service_account_file=""
daemonset_file=""
role_file=""
role_binding_file=""

cleanup() {
  rm -f -- "${work_dir}"/*
  rmdir -- "${work_dir}"
}
trap cleanup EXIT

"${KUBECTL}" kustomize "${KUSTOMIZE_FILEBEAT_OVERLAY}" >"${rendered_file}"
csplit --silent --prefix="${work_dir}/resource-" --suffix-format='%02d.yaml' \
  "${rendered_file}" '/^---$/' '{*}'

resource_count=0
for resource_file in "${work_dir}"/resource-*.yaml; do
  [[ -s "${resource_file}" ]] || continue
  ((resource_count += 1))
  inventory="$(${KUBECTL} create --dry-run=client -f "${resource_file}" \
    -o 'jsonpath={.kind}{"|"}{.metadata.name}{"|"}{.metadata.namespace}{"|"}{.metadata.labels.distributed-log-platform\.io/purpose}')"
  IFS='|' read -r kind name namespace purpose <<<"${inventory}"
  [[ "${purpose}" == "${PURPOSE}" ]] || fail "Filebeat 清单缺少用途边界：${inventory}"

  case "${kind}|${namespace}" in
    "Namespace|")
      [[ -z "${namespace_file}" && "${name}" == "${FILEBEAT_NAMESPACE}" ]] ||
        fail "Filebeat Namespace 异常：${inventory}"
      namespace_file="${resource_file}"
      ;;
    "ConfigMap|${FILEBEAT_NAMESPACE}")
      [[ -z "${config_map_file}" && "${name}" =~ ^filebeat-config-[a-z0-9]+$ ]] ||
        fail "Filebeat ConfigMap 异常：${inventory}"
      config_map_file="${resource_file}"
      config_map_name="${name}"
      ;;
    "ServiceAccount|${FILEBEAT_NAMESPACE}")
      [[ -z "${service_account_file}" && "${name}" == "${SERVICE_ACCOUNT_NAME}" ]] ||
        fail "Filebeat ServiceAccount 异常：${inventory}"
      service_account_file="${resource_file}"
      ;;
    "DaemonSet|${FILEBEAT_NAMESPACE}")
      [[ -z "${daemonset_file}" && "${name}" == "${DAEMONSET_NAME}" ]] ||
        fail "Filebeat DaemonSet 异常：${inventory}"
      daemonset_file="${resource_file}"
      ;;
    "Role|${KUBE_NAMESPACE}")
      [[ -z "${role_file}" && "${name}" == "${ROLE_NAME}" ]] ||
        fail "Filebeat Role 异常：${inventory}"
      role_file="${resource_file}"
      ;;
    "RoleBinding|${KUBE_NAMESPACE}")
      [[ -z "${role_binding_file}" && "${name}" == "${ROLE_NAME}" ]] ||
        fail "Filebeat RoleBinding 异常：${inventory}"
      role_binding_file="${resource_file}"
      ;;
    *)
      fail "Filebeat overlay 包含越界资源：${inventory}"
      ;;
  esac
done

[[ "${resource_count}" -eq 6 && -n "${namespace_file}" && -n "${config_map_file}" && \
  -n "${service_account_file}" && -n "${daemonset_file}" && -n "${role_file}" && \
  -n "${role_binding_file}" ]] || fail "Filebeat overlay 必须精确包含 6 个约定资源"

namespace_policy="$(${KUBECTL} create --dry-run=client -f "${namespace_file}" \
  -o 'jsonpath={.metadata.labels.pod-security\.kubernetes\.io/enforce}{"|"}{.metadata.labels.pod-security\.kubernetes\.io/warn}{"|"}{.metadata.labels.pod-security\.kubernetes\.io/audit}')"
[[ "${namespace_policy}" == "privileged|restricted|restricted" ]] ||
  fail "stage3-collector Pod Security 标签异常：${namespace_policy}"

role_rules="$(${KUBECTL} create --dry-run=client -f "${role_file}" \
  -o 'jsonpath={range .rules[*]}{range .apiGroups[*]}{@}{","}{end}{"|"}{range .resources[*]}{@}{","}{end}{"|"}{range .verbs[*]}{@}{","}{end}{"\n"}{end}')"
[[ "${role_rules}" == ",|pods,|get,list,watch," ]] || fail "Filebeat Role 超出 Pod 只读边界：${role_rules}"

binding_facts="$(${KUBECTL} create --dry-run=client -f "${role_binding_file}" \
  -o 'jsonpath={range .subjects[*]}{.kind}{"|"}{.name}{"|"}{.namespace}{"\n"}{end}{.roleRef.apiGroup}{"|"}{.roleRef.kind}{"|"}{.roleRef.name}')"
expected_binding="ServiceAccount|${SERVICE_ACCOUNT_NAME}|${FILEBEAT_NAMESPACE}
rbac.authorization.k8s.io|Role|${ROLE_NAME}"
[[ "${binding_facts}" == "${expected_binding}" ]] || fail "Filebeat RoleBinding 异常：${binding_facts}"

config_reference="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
  -o 'jsonpath={.spec.template.spec.volumes[?(@.name=="config")].configMap.name}')"
[[ "${config_reference}" == "${config_map_name}" ]] ||
  fail "DaemonSet 引用的配置 ConfigMap 不匹配：${config_reference:-缺失}"

container_inventory="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
  -o 'jsonpath={range .spec.template.spec.containers[*]}{.name}{"|"}{.image}{"|"}{.imagePullPolicy}{"\n"}{end}')"
[[ "${container_inventory}" == "${CONTAINER_NAME}|${FILEBEAT_NODE_IMAGE}|Never" ]] ||
  fail "Filebeat 容器身份异常：${container_inventory:-缺失}"
init_containers="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
  -o 'jsonpath={range .spec.template.spec.initContainers[*]}{.name}{"\n"}{end}')"
[[ -z "${init_containers}" ]] || fail "Filebeat DaemonSet 不允许 initContainer：${init_containers}"

pod_security="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
  -o 'jsonpath={.spec.template.spec.serviceAccountName}{"|"}{.spec.template.spec.automountServiceAccountToken}{"|"}{.spec.template.spec.hostNetwork}{"|"}{.spec.template.spec.hostPID}{"|"}{.spec.template.spec.hostIPC}{"|"}{.spec.template.spec.securityContext.runAsUser}{"|"}{.spec.template.spec.securityContext.runAsGroup}{"|"}{.spec.template.spec.securityContext.seccompProfile.type}')"
[[ "${pod_security}" == "filebeat|true||||0|0|RuntimeDefault" ]] ||
  fail "Filebeat Pod 安全上下文异常：${pod_security}"

container_security="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
  -o 'jsonpath={.spec.template.spec.containers[0].securityContext.privileged}{"|"}{.spec.template.spec.containers[0].securityContext.allowPrivilegeEscalation}{"|"}{.spec.template.spec.containers[0].securityContext.readOnlyRootFilesystem}{"|"}{range .spec.template.spec.containers[0].securityContext.capabilities.drop[*]}{@}{","}{end}{"|"}{range .spec.template.spec.containers[0].securityContext.capabilities.add[*]}{@}{","}{end}')"
[[ "${container_security}" == "false|false|true|ALL,|" ]] ||
  fail "Filebeat 容器安全上下文异常：${container_security}"

scheduling_and_resources="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
  -o 'jsonpath={.spec.template.spec.nodeSelector.kubernetes\.io/os}{"|"}{.spec.template.spec.nodeSelector.kubernetes\.io/arch}{"|"}{.spec.template.spec.containers[0].resources.requests.cpu}{"|"}{.spec.template.spec.containers[0].resources.requests.memory}{"|"}{.spec.template.spec.containers[0].resources.limits.cpu}{"|"}{.spec.template.spec.containers[0].resources.limits.memory}')"
[[ "${scheduling_and_resources}" == "linux|amd64|50m|128Mi|500m|256Mi" ]] ||
  fail "Filebeat 调度或资源边界异常：${scheduling_and_resources}"

host_paths="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
  -o 'jsonpath={range .spec.template.spec.volumes[?(@.hostPath)]}{.name}{"|"}{.hostPath.path}{"|"}{.hostPath.type}{"\n"}{end}')"
expected_host_paths="registry|/var/lib/distributed-log-platform/filebeat-data|DirectoryOrCreate
container-logs|/var/log/containers|Directory
pod-logs|/var/log/pods|Directory"
[[ "${host_paths}" == "${expected_host_paths}" ]] || fail "Filebeat hostPath 边界异常：${host_paths}"

mounts="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
  -o 'jsonpath={range .spec.template.spec.containers[0].volumeMounts[*]}{.name}{"|"}{.mountPath}{"|"}{.readOnly}{"\n"}{end}')"
expected_mounts="config|/etc/filebeat.yml|true
registry|/usr/share/filebeat/data|
container-logs|/var/log/containers|true
pod-logs|/var/log/pods|true
tmp|/tmp|"
[[ "${mounts}" == "${expected_mounts}" ]] || fail "Filebeat 挂载权限异常：${mounts}"

for annotation in \
  "upstream-index-digest|${EXPECTED_FILEBEAT_UPSTREAM_INDEX_DIGEST}" \
  "upstream-amd64-digest|${EXPECTED_FILEBEAT_UPSTREAM_AMD64_DIGEST}" \
  "local-import-digest|${EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST}" \
  "expected-config-digest|${EXPECTED_FILEBEAT_CONFIG_DIGEST}"; do
  IFS='|' read -r annotation_name expected_value <<<"${annotation}"
  actual_value="$(${KUBECTL} create --dry-run=client -f "${daemonset_file}" \
    -o "jsonpath={.spec.template.metadata.annotations.distributed-log-platform\\.io/${annotation_name}}")"
  [[ "${actual_value}" == "${expected_value}" ]] ||
    fail "Filebeat 摘要注解 ${annotation_name} 异常：${actual_value:-缺失}"
done

"${KUBECTL}" create --dry-run=client -f "${config_map_file}" \
  -o 'jsonpath={.data.filebeat\.yml}' >"${rendered_config}"
cmp --silent deploy/kubernetes/base/filebeat/filebeat.yml "${rendered_config}" ||
  fail "overlay 修改了未经审查的 Filebeat 配置内容"
assert_line_count '^filebeat\.inputs:$' 1 "Filebeat inputs 根节点"
assert_line_count '^  - type: filestream$' 1 "filestream 输入"
assert_line_count '^output\.[a-zA-Z0-9_-]+:$' 1 "Filebeat output"
assert_line_count '^  topics:$' 1 "Kafka topics 规则组"
assert_line_count '^    - topic: logs\.(api-service|worker-service)$' 2 "业务静态主题规则"
assert_line_count '^  topic: logs\.unclassified$' 1 "未分类默认主题"
grep -Fqx '      - /var/log/containers/*_stage3-logs_log-producer-*.log' "${rendered_config}" ||
  fail "Filebeat 采集允许列表异常"
grep -Fqx '  topic: logs.unclassified' "${rendered_config}" || fail "缺少固定未分类主题"
grep -Fqx '    - topic: logs.api-service' "${rendered_config}" || fail "缺少 api-service 静态路由"
grep -Fqx '    - topic: logs.worker-service' "${rendered_config}" || fail "缺少 worker-service 静态路由"
expected_topic_rules=$'  topics:\n    - topic: logs.api-service\n      when:\n        and:\n          - equals:\n              kubernetes.labels.service: api-service\n          - has_fields:\n              - kubernetes.pod.uid\n    - topic: logs.worker-service\n      when:\n        and:\n          - equals:\n              kubernetes.labels.service: worker-service\n          - has_fields:\n              - kubernetes.pod.uid'
grep -Fq "${expected_topic_rules}" "${rendered_config}" || fail "Filebeat 服务标签与主题映射异常"
grep -Fqx "          target_field: '@metadata.kafka_key'" "${rendered_config}" ||
  fail "Filebeat 缺少元数据失效时的稳定指纹键"
grep -Fqx "              to: '@metadata.kafka_key'" "${rendered_config}" ||
  fail "Filebeat 缺少 Pod UID 键复制"
grep -Fqx '            routing_reason: kubernetes_metadata_missing' "${rendered_config}" ||
  fail "Filebeat 缺少元数据失效路由原因"
grep -Fqx "  key: '%{[@metadata.kafka_key]}'" "${rendered_config}" ||
  fail "Kafka key 未使用已审查的正常/兜底键"
if grep -Fq 'output.elasticsearch' "${rendered_config}" || grep -Fq 'filebeat.autodiscover' "${rendered_config}" ||
  grep -Eq 'topic:.*%\{' "${rendered_config}" || grep -Fq 'drop_event:' "${rendered_config}"; then
  fail "Filebeat 配置包含禁止的 Elasticsearch、autodiscover、任意动态主题或静默丢弃"
fi

# 真实 Filebeat 二进制负责语义校验，并用非法协议版本反例证明失败状态会传播。
"${repo_root}/scripts/check-filebeat-config.sh"

# 已存在的应用命名空间允许完整服务端校验最小 Role 与 RoleBinding。
"${KUBECTL}" --context="${KUBE_CONTEXT}" apply --dry-run=server -f "${namespace_file}" >/dev/null
"${KUBECTL}" --context="${KUBE_CONTEXT}" apply --dry-run=server -f "${role_file}" >/dev/null
"${KUBECTL}" --context="${KUBE_CONTEXT}" apply --dry-run=server -f "${role_binding_file}" >/dev/null

collector_exists="$(${KUBECTL} --context="${KUBE_CONTEXT}" get namespace "${FILEBEAT_NAMESPACE}" \
  --ignore-not-found -o name)"
if [[ -n "${collector_exists}" ]]; then
  existing_namespace_facts="$(${KUBECTL} --context="${KUBE_CONTEXT}" get namespace "${FILEBEAT_NAMESPACE}" \
    -o 'jsonpath={.metadata.labels.distributed-log-platform\.io/purpose}{"|"}{.metadata.labels.pod-security\.kubernetes\.io/enforce}')"
  [[ "${existing_namespace_facts}" == "${PURPOSE}|privileged" ]] ||
    fail "现有 stage3-collector 不属于本流程：${existing_namespace_facts}"
  "${KUBECTL}" --context="${KUBE_CONTEXT}" apply --dry-run=server -f "${rendered_file}" >/dev/null
else
  # dry-run Namespace 不会真实存在，首次只能客户端校验其余命名空间资源；run 模式创建后再做完整服务端校验。
  "${KUBECTL}" apply --dry-run=client -f "${service_account_file}" >/dev/null
  "${KUBECTL}" apply --dry-run=client -f "${config_map_file}" >/dev/null
  "${KUBECTL}" apply --dry-run=client -f "${daemonset_file}" >/dev/null
fi

if [[ "${MODE}" == "validate" ]]; then
  if [[ -n "${collector_exists}" ]]; then
    echo "Filebeat 清单验证通过：6 个资源、完整服务端 dry-run、未写入集群"
  else
    echo "Filebeat 首次清单验证通过：Namespace/RBAC 服务端校验，其余资源客户端校验，未写入集群"
  fi
  exit 0
fi

# rollout 与所有 Filebeat 验收共享互斥边界，不能在恢复窗口中隐式替换 Pod。
source "${repo_root}/scripts/lib/filebeat-workflow-lock.sh"
acquire_filebeat_workflow_lock || exit 1
export FILEBEAT_WORKFLOW_LOCK_INHERITED=1

# Runner 自身在任何集群写操作前执行节点镜像身份门禁。
"${repo_root}/scripts/verify-filebeat-image.sh" node

assert_owned_or_absent() {
  local kind="$1"
  local name="$2"
  local namespace="$3"
  local args=(--context="${KUBE_CONTEXT}" get "${kind}" "${name}" --ignore-not-found)
  [[ -n "${namespace}" ]] && args+=(-n "${namespace}")

  existing_name="$(${KUBECTL} "${args[@]}" -o name)"
  [[ -z "${existing_name}" ]] && return
  existing_purpose="$(${KUBECTL} "${args[@]}" \
    -o 'jsonpath={.metadata.labels.distributed-log-platform\.io/purpose}')"
  [[ "${existing_purpose}" == "${PURPOSE}" ]] ||
    fail "同名资源不属于 Filebeat 流程，拒绝覆盖：${kind}/${name}"
}

assert_owned_or_absent namespace "${FILEBEAT_NAMESPACE}" ""
assert_owned_or_absent serviceaccount "${SERVICE_ACCOUNT_NAME}" "${FILEBEAT_NAMESPACE}"
assert_owned_or_absent daemonset "${DAEMONSET_NAME}" "${FILEBEAT_NAMESPACE}"
assert_owned_or_absent role "${ROLE_NAME}" "${KUBE_NAMESPACE}"
assert_owned_or_absent rolebinding "${ROLE_NAME}" "${KUBE_NAMESPACE}"
assert_owned_or_absent configmap "${config_map_name}" "${FILEBEAT_NAMESPACE}"

if [[ -z "${collector_exists}" ]]; then
  "${KUBECTL}" --context="${KUBE_CONTEXT}" apply -f "${namespace_file}"
  # Namespace 创建后立即补做所有资源的服务端准入；失败时只留下可识别的空专用命名空间。
  "${KUBECTL}" --context="${KUBE_CONTEXT}" apply --dry-run=server -f "${rendered_file}" >/dev/null
fi

"${KUBECTL}" --context="${KUBE_CONTEXT}" apply -f "${rendered_file}"

diagnostics() {
  set +e
  "${KUBECTL}" --context="${KUBE_CONTEXT}" get daemonsets,pods,configmaps,serviceaccounts \
    -n "${FILEBEAT_NAMESPACE}" -l "distributed-log-platform.io/purpose=${PURPOSE}" -o wide >&2
  "${KUBECTL}" --context="${KUBE_CONTEXT}" describe daemonset "${DAEMONSET_NAME}" \
    -n "${FILEBEAT_NAMESPACE}" >&2
  "${KUBECTL}" --context="${KUBE_CONTEXT}" logs daemonset/"${DAEMONSET_NAME}" \
    -n "${FILEBEAT_NAMESPACE}" --tail=200 >&2
  set -e
}

if ! "${KUBECTL}" --context="${KUBE_CONTEXT}" rollout status daemonset/"${DAEMONSET_NAME}" \
  -n "${FILEBEAT_NAMESPACE}" --timeout="${FILEBEAT_ROLLOUT_TIMEOUT}"; then
  diagnostics
  fail "Filebeat DaemonSet 部署失败"
fi

daemonset_status="$(${KUBECTL} --context="${KUBE_CONTEXT}" get daemonset "${DAEMONSET_NAME}" \
  -n "${FILEBEAT_NAMESPACE}" \
  -o 'jsonpath={.status.desiredNumberScheduled}{"|"}{.status.currentNumberScheduled}{"|"}{.status.numberReady}{"|"}{.status.numberAvailable}{"|"}{.status.numberUnavailable}')"
[[ "${daemonset_status}" == "1|1|1|1|" || "${daemonset_status}" == "1|1|1|1|0" ]] || {
  diagnostics
  fail "Filebeat DaemonSet 状态异常：desired|current|ready|available|unavailable=${daemonset_status}"
}

pod_output="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pods -n "${FILEBEAT_NAMESPACE}" \
  -l app.kubernetes.io/name=filebeat -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}')"
pod_names=()
[[ -n "${pod_output}" ]] && mapfile -t pod_names <<<"${pod_output}"
[[ "${#pod_names[@]}" -eq 1 ]] || fail "当前单节点集群应有 1 个 Filebeat Pod，实际 ${#pod_names[@]}"
pod_name="${pod_names[0]}"

pod_status="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pod "${pod_name}" -n "${FILEBEAT_NAMESPACE}" \
  -o 'jsonpath={.status.phase}{"|"}{.status.containerStatuses[?(@.name=="filebeat")].ready}{"|"}{.status.containerStatuses[?(@.name=="filebeat")].restartCount}{"|"}{.spec.nodeName}')"
[[ "${pod_status}" == "Running|true|0|stage3-logs" ]] || fail "Filebeat Pod 状态异常：${pod_status}"
"${repo_root}/scripts/verify-filebeat-image.sh" pod

# 再检查 API Server 中的最终对象，避免只验证渲染文件而漏掉准入修改或外部漂移。
runtime_role_rules="$(${KUBECTL} --context="${KUBE_CONTEXT}" get role "${ROLE_NAME}" -n "${KUBE_NAMESPACE}" \
  -o 'jsonpath={range .rules[*]}{range .apiGroups[*]}{@}{","}{end}{"|"}{range .resources[*]}{@}{","}{end}{"|"}{range .verbs[*]}{@}{","}{end}{"\n"}{end}')"
[[ "${runtime_role_rules}" == ",|pods,|get,list,watch," ]] ||
  fail "运行态 Filebeat Role 超出 Pod 只读边界：${runtime_role_rules}"
runtime_container_security="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pod "${pod_name}" -n "${FILEBEAT_NAMESPACE}" \
  -o 'jsonpath={.spec.containers[0].securityContext.privileged}{"|"}{.spec.containers[0].securityContext.allowPrivilegeEscalation}{"|"}{.spec.containers[0].securityContext.readOnlyRootFilesystem}{"|"}{range .spec.containers[0].securityContext.capabilities.drop[*]}{@}{","}{end}{"|"}{range .spec.containers[0].securityContext.capabilities.add[*]}{@}{","}{end}')"
[[ "${runtime_container_security}" == "false|false|true|ALL,|" ]] ||
  fail "运行态 Filebeat 容器安全上下文异常：${runtime_container_security}"
runtime_scheduling_and_resources="$(${KUBECTL} --context="${KUBE_CONTEXT}" get pod "${pod_name}" -n "${FILEBEAT_NAMESPACE}" \
  -o 'jsonpath={.spec.nodeSelector.kubernetes\.io/os}{"|"}{.spec.nodeSelector.kubernetes\.io/arch}{"|"}{.spec.containers[0].resources.requests.cpu}{"|"}{.spec.containers[0].resources.requests.memory}{"|"}{.spec.containers[0].resources.limits.cpu}{"|"}{.spec.containers[0].resources.limits.memory}')"
[[ "${runtime_scheduling_and_resources}" == "linux|amd64|50m|128Mi|500m|256Mi" ]] ||
  fail "运行态 Filebeat 调度或资源边界异常：${runtime_scheduling_and_resources}"

service_account="system:serviceaccount:${FILEBEAT_NAMESPACE}:${SERVICE_ACCOUNT_NAME}"
for verb in get list watch; do
  answer="$(${KUBECTL} --context="${KUBE_CONTEXT}" auth can-i "${verb}" pods -n "${KUBE_NAMESPACE}" --as="${service_account}")"
  [[ "${answer}" == "yes" ]] || fail "Filebeat ServiceAccount 缺少 ${verb} pods 权限"
done
for denied_check in \
  "get secrets ${KUBE_NAMESPACE}" \
  "create pods ${KUBE_NAMESPACE}" \
  "get pods kube-system" \
  "get nodes -" \
  "get namespaces -"; do
  read -r verb resource namespace <<<"${denied_check}"
  auth_args=(--context="${KUBE_CONTEXT}" auth can-i "${verb}" "${resource}" --as="${service_account}")
  [[ "${namespace}" != "-" ]] && auth_args+=(-n "${namespace}")
  # `kubectl auth can-i` 在答案为 no 时返回 1；必须与命令执行故障区分，不能直接吞掉状态码。
  if answer="$(${KUBECTL} "${auth_args[@]}")"; then
    fail "Filebeat ServiceAccount 获得越界权限：${denied_check}"
  else
    auth_status=$?
  fi
  [[ "${auth_status}" -eq 1 && "${answer}" == "no" ]] ||
    fail "Filebeat RBAC 反例执行异常：${denied_check} status=${auth_status} output=${answer:-空}"
done

"${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${pod_name}" -n "${FILEBEAT_NAMESPACE}" -- \
  /bin/sh -ec 'test -r /var/log/containers; test -r /var/log/pods; ! test -w /var/log/containers; ! test -w /var/log/pods; test -w /usr/share/filebeat/data; test ! -e /var/run/docker.sock'

"${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${pod_name}" -n "${FILEBEAT_NAMESPACE}" -- \
  /bin/sh -ec 'test_dir="$(mktemp -d /tmp/filebeat-check.XXXXXX)"; trap '\''rm -rf -- "${test_dir}"'\'' EXIT; filebeat test config -c /etc/filebeat.yml -e --strict.perms=false -E path.data="${test_dir}"; filebeat test output -c /etc/filebeat.yml -e --strict.perms=false -E path.data="${test_dir}"'

"${KUBECTL}" --context="${KUBE_CONTEXT}" logs "${pod_name}" -n "${FILEBEAT_NAMESPACE}" >"${work_dir}/filebeat.log"
if grep -Fq '"log.level":"error"' "${work_dir}/filebeat.log" || grep -Fq 'Exiting:' "${work_dir}/filebeat.log"; then
  diagnostics
  fail "Filebeat 启动日志包含错误"
fi

echo "Filebeat 部署通过：1 DaemonSet Pod、0 重启、镜像/RBAC/挂载/config/output 全部验证"
