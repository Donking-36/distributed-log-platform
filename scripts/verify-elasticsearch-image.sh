#!/usr/bin/env bash
set -Eeuo pipefail

MINIKUBE="${MINIKUBE:-minikube}"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
ELASTICSEARCH_NODE_IMAGE="${ELASTICSEARCH_NODE_IMAGE:-docker.io/library/elasticsearch:9.4.4}"
EXPECTED_ELASTICSEARCH_MANIFEST_DIGEST="${EXPECTED_ELASTICSEARCH_MANIFEST_DIGEST:-}"
EXPECTED_ELASTICSEARCH_CONFIG_DIGEST="${EXPECTED_ELASTICSEARCH_CONFIG_DIGEST:-}"
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

require_digest "EXPECTED_ELASTICSEARCH_MANIFEST_DIGEST" "${EXPECTED_ELASTICSEARCH_MANIFEST_DIGEST}"
require_digest "EXPECTED_ELASTICSEARCH_CONFIG_DIGEST" "${EXPECTED_ELASTICSEARCH_CONFIG_DIGEST}"

verify_runtime_image() {
  local container_name="$1"
  local runtime_image_id="$2"

  [[ "${runtime_image_id}" =~ (sha256:[0-9a-f]{64})$ ]] ||
    fail "无法从 Elasticsearch Pod 解析 ${container_name} 的运行时 imageID：${runtime_image_id:-缺失}"
  local actual_config_digest="${BASH_REMATCH[1]}"
  [[ "${actual_config_digest}" == "${EXPECTED_ELASTICSEARCH_CONFIG_DIGEST}" ]] ||
    fail "Elasticsearch Pod ${container_name} 镜像不匹配：实际 ${actual_config_digest}，要求 ${EXPECTED_ELASTICSEARCH_CONFIG_DIGEST}"
}

case "${1:-}" in
  node)
    command -v "${MINIKUBE}" >/dev/null 2>&1 || fail "缺少命令：${MINIKUBE}"
    command -v awk >/dev/null 2>&1 || fail "缺少命令：awk"

    image_table="$(
      "${MINIKUBE}" ssh -p "${KUBE_CONTEXT}" -- \
        sudo ctr -n k8s.io images list "name==${ELASTICSEARCH_NODE_IMAGE}"
    )"
    actual_manifest_digest="$(
      awk -v image="${ELASTICSEARCH_NODE_IMAGE}" \
        'NR > 1 && $1 == image { print $3; exit }' \
        <<<"${image_table}"
    )"
    if ! actual_config_digest="$(
      "${MINIKUBE}" ssh -p "${KUBE_CONTEXT}" -- \
        sudo crictl inspecti \
        -o go-template \
        --template '{{.status.id}}' \
        "${ELASTICSEARCH_NODE_IMAGE}"
    )"; then
      fail "无法读取 Elasticsearch 节点镜像 config 摘要：${ELASTICSEARCH_NODE_IMAGE}"
    fi
    actual_config_digest="${actual_config_digest//$'\r'/}"

    [[ "${actual_manifest_digest}" == "${EXPECTED_ELASTICSEARCH_MANIFEST_DIGEST}" ]] ||
      fail "Elasticsearch 节点镜像 manifest 不匹配：实际 ${actual_manifest_digest:-缺失}，要求 ${EXPECTED_ELASTICSEARCH_MANIFEST_DIGEST}"
    [[ "${actual_config_digest}" == "${EXPECTED_ELASTICSEARCH_CONFIG_DIGEST}" ]] ||
      fail "Elasticsearch 节点镜像 config 不匹配：实际 ${actual_config_digest:-缺失}，要求 ${EXPECTED_ELASTICSEARCH_CONFIG_DIGEST}"

    echo "Elasticsearch 节点镜像身份通过：manifest=${actual_manifest_digest} config=${actual_config_digest}"
    ;;
  pod)
    command -v "${KUBECTL}" >/dev/null 2>&1 || fail "缺少命令：${KUBECTL}"

    main_runtime_image_id="$(
      "${KUBECTL}" --context="${KUBE_CONTEXT}" get pod elasticsearch-0 \
        -n "${KUBE_NAMESPACE}" \
        -o 'jsonpath={.status.containerStatuses[?(@.name=="elasticsearch")].imageID}'
    )"
    init_runtime_image_id="$(
      "${KUBECTL}" --context="${KUBE_CONTEXT}" get pod elasticsearch-0 \
        -n "${KUBE_NAMESPACE}" \
        -o 'jsonpath={.status.initContainerStatuses[?(@.name=="prepare-config")].imageID}'
    )"

    verify_runtime_image "主容器 elasticsearch" "${main_runtime_image_id}"
    verify_runtime_image "初始化容器 prepare-config" "${init_runtime_image_id}"

    echo "Elasticsearch Pod 双容器运行时镜像身份通过：config=${EXPECTED_ELASTICSEARCH_CONFIG_DIGEST}"
    ;;
  *)
    echo "用法：$0 node|pod" >&2
    exit 2
    ;;
esac
