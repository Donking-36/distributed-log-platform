#!/usr/bin/env bash
set -Eeuo pipefail

MINIKUBE="${MINIKUBE:-minikube}"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
FILEBEAT_NAMESPACE="${FILEBEAT_NAMESPACE:-stage3-collector}"
FILEBEAT_NODE_IMAGE="${FILEBEAT_NODE_IMAGE:-docker.elastic.co/beats/filebeat-wolfi:9.4.4}"
EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST="${EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST:-}"
EXPECTED_FILEBEAT_CONFIG_DIGEST="${EXPECTED_FILEBEAT_CONFIG_DIGEST:-}"
readonly DIGEST_PATTERN='^sha256:[0-9a-f]{64}$'

fail() {
  echo "$1" >&2
  exit 1
}

require_digest() {
  local name="$1"
  local value="$2"

  [[ "${value}" =~ ${DIGEST_PATTERN} ]] || fail "${name} 必须是完整的 sha256 摘要"
}

require_digest "EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST" "${EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST}"
require_digest "EXPECTED_FILEBEAT_CONFIG_DIGEST" "${EXPECTED_FILEBEAT_CONFIG_DIGEST}"

case "${1:-}" in
  node)
    command -v "${MINIKUBE}" >/dev/null 2>&1 || fail "缺少命令：${MINIKUBE}"
    command -v awk >/dev/null 2>&1 || fail "缺少命令：awk"

    image_table="$(
      "${MINIKUBE}" ssh -p "${KUBE_CONTEXT}" -- \
        sudo ctr -n k8s.io images list "name==${FILEBEAT_NODE_IMAGE}"
    )"
    actual_manifest_digest="$(
      awk -v image="${FILEBEAT_NODE_IMAGE}" \
        'NR > 1 && $1 == image { print $3; exit }' <<<"${image_table}"
    )"
    if ! actual_config_digest="$(
      "${MINIKUBE}" ssh -p "${KUBE_CONTEXT}" -- \
        sudo crictl inspecti -o go-template --template '{{.status.id}}' "${FILEBEAT_NODE_IMAGE}"
    )"; then
      fail "无法读取 Filebeat 节点镜像 config 摘要：${FILEBEAT_NODE_IMAGE}"
    fi
    actual_config_digest="${actual_config_digest//$'\r'/}"

    [[ "${actual_manifest_digest}" == "${EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST}" ]] ||
      fail "Filebeat 节点 manifest 不匹配：实际 ${actual_manifest_digest:-缺失}，要求 ${EXPECTED_FILEBEAT_LOCAL_MANIFEST_DIGEST}"
    [[ "${actual_config_digest}" == "${EXPECTED_FILEBEAT_CONFIG_DIGEST}" ]] ||
      fail "Filebeat 节点 config 不匹配：实际 ${actual_config_digest:-缺失}，要求 ${EXPECTED_FILEBEAT_CONFIG_DIGEST}"

    echo "Filebeat 节点镜像身份通过：manifest=${actual_manifest_digest} config=${actual_config_digest}"
    ;;
  pod)
    command -v "${KUBECTL}" >/dev/null 2>&1 || fail "缺少命令：${KUBECTL}"

    pod_output="$(
      "${KUBECTL}" --context="${KUBE_CONTEXT}" get pods -n "${FILEBEAT_NAMESPACE}" \
        -l app.kubernetes.io/name=filebeat \
        -o 'jsonpath={range .items[*]}{.metadata.name}{"|"}{.status.containerStatuses[?(@.name=="filebeat")].imageID}{"\n"}{end}'
    )"
    [[ -n "${pod_output}" ]] || fail "未找到 Filebeat Pod"

    pod_count=0
    while IFS='|' read -r pod_name runtime_image_id; do
      [[ -n "${pod_name}" ]] || continue
      ((pod_count += 1))
      [[ "${runtime_image_id}" =~ (sha256:[0-9a-f]{64})$ ]] ||
        fail "无法解析 Filebeat Pod ${pod_name} 的运行时 imageID：${runtime_image_id:-缺失}"
      actual_config_digest="${BASH_REMATCH[1]}"
      [[ "${actual_config_digest}" == "${EXPECTED_FILEBEAT_CONFIG_DIGEST}" ]] ||
        fail "Filebeat Pod ${pod_name} 镜像不匹配：实际 ${actual_config_digest}，要求 ${EXPECTED_FILEBEAT_CONFIG_DIGEST}"
    done <<<"${pod_output}"

    echo "Filebeat Pod 运行时镜像身份通过：pods=${pod_count} config=${EXPECTED_FILEBEAT_CONFIG_DIGEST}"
    ;;
  *)
    echo "用法：$0 node|pod" >&2
    exit 2
    ;;
esac
