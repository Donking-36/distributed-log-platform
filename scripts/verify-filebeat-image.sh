#!/usr/bin/env bash
set -Eeuo pipefail

MINIKUBE="${MINIKUBE:-minikube}"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
FILEBEAT_NAMESPACE="${FILEBEAT_NAMESPACE:-stage3-collector}"
FILEBEAT_NODE_IMAGE="${FILEBEAT_NODE_IMAGE:-docker.elastic.co/beats/filebeat-wolfi:9.4.4}"

fail() {
  echo "$1" >&2
  exit 1
}

case "${1:-}" in
  node)
    command -v "${MINIKUBE}" >/dev/null 2>&1 || fail "缺少命令：${MINIKUBE}"

    "${MINIKUBE}" ssh -p "${KUBE_CONTEXT}" -- \
      sudo crictl inspecti "${FILEBEAT_NODE_IMAGE}" >/dev/null 2>&1 ||
      fail "Minikube 节点缺少 Filebeat 镜像：${FILEBEAT_NODE_IMAGE}"

    echo "Filebeat 节点镜像已就绪：${FILEBEAT_NODE_IMAGE}"
    ;;
  pod)
    command -v "${KUBECTL}" >/dev/null 2>&1 || fail "缺少命令：${KUBECTL}"

    pod_output="$(
      "${KUBECTL}" --context="${KUBE_CONTEXT}" get pods -n "${FILEBEAT_NAMESPACE}" \
        -l app.kubernetes.io/name=filebeat \
        -o 'jsonpath={range .items[*]}{.metadata.name}{"|"}{.spec.containers[?(@.name=="filebeat")].image}{"|"}{.status.containerStatuses[?(@.name=="filebeat")].imageID}{"|"}{.status.containerStatuses[?(@.name=="filebeat")].ready}{"\n"}{end}'
    )"
    [[ -n "${pod_output}" ]] || fail "未找到 Filebeat Pod"

    pod_count=0
    while IFS='|' read -r pod_name image image_id ready; do
      [[ -n "${pod_name}" ]] || continue
      ((pod_count += 1))
      [[ "${image}" == "${FILEBEAT_NODE_IMAGE}" ]] ||
        fail "Filebeat Pod ${pod_name} 版本异常：实际 ${image:-缺失}，要求 ${FILEBEAT_NODE_IMAGE}"
      [[ -n "${image_id}" && "${ready}" == "true" ]] ||
        fail "Filebeat Pod ${pod_name} 尚未使用该镜像就绪"
    done <<<"${pod_output}"

    echo "Filebeat Pod 镜像版本通过：pods=${pod_count} image=${FILEBEAT_NODE_IMAGE}"
    ;;
  *)
    echo "用法：$0 node|pod" >&2
    exit 2
    ;;
esac
