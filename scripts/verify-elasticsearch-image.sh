#!/usr/bin/env bash
set -Eeuo pipefail

MINIKUBE="${MINIKUBE:-minikube}"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
ELASTICSEARCH_NODE_IMAGE="${ELASTICSEARCH_NODE_IMAGE:-docker.io/library/elasticsearch:9.4.4}"

fail() {
  echo "$1" >&2
  exit 1
}

case "${1:-}" in
  node)
    command -v "${MINIKUBE}" >/dev/null 2>&1 || fail "缺少命令：${MINIKUBE}"

    "${MINIKUBE}" ssh -p "${KUBE_CONTEXT}" -- \
      sudo crictl inspecti "${ELASTICSEARCH_NODE_IMAGE}" >/dev/null 2>&1 ||
      fail "Minikube 节点缺少 Elasticsearch 镜像：${ELASTICSEARCH_NODE_IMAGE}"

    echo "Elasticsearch 节点镜像已就绪：${ELASTICSEARCH_NODE_IMAGE}"
    ;;
  pod)
    command -v "${KUBECTL}" >/dev/null 2>&1 || fail "缺少命令：${KUBECTL}"

    pod_facts="$(
      "${KUBECTL}" --context="${KUBE_CONTEXT}" get pod elasticsearch-0 \
        -n "${KUBE_NAMESPACE}" \
        -o 'jsonpath={.spec.containers[?(@.name=="elasticsearch")].image}{"|"}{.spec.initContainers[?(@.name=="prepare-config")].image}{"|"}{.status.containerStatuses[?(@.name=="elasticsearch")].imageID}{"|"}{.status.initContainerStatuses[?(@.name=="prepare-config")].imageID}'
    )"
    IFS='|' read -r main_image init_image main_image_id init_image_id <<<"${pod_facts}"

    [[ "${main_image}" == "${ELASTICSEARCH_NODE_IMAGE}" ]] ||
      fail "Elasticsearch 主容器版本异常：实际 ${main_image:-缺失}，要求 ${ELASTICSEARCH_NODE_IMAGE}"
    [[ "${init_image}" == "${ELASTICSEARCH_NODE_IMAGE}" ]] ||
      fail "Elasticsearch 初始化容器版本异常：实际 ${init_image:-缺失}，要求 ${ELASTICSEARCH_NODE_IMAGE}"
    [[ -n "${main_image_id}" && -n "${init_image_id}" ]] ||
      fail "Elasticsearch Pod 尚未形成完整运行时镜像记录"

    echo "Elasticsearch Pod 镜像版本通过：${ELASTICSEARCH_NODE_IMAGE}"
    ;;
  *)
    echo "用法：$0 node|pod" >&2
    exit 2
    ;;
esac
