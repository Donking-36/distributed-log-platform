#!/usr/bin/env bash
set -Eeuo pipefail

MINIKUBE="${MINIKUBE:-minikube}"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-stage3-logs}"
KUBE_NAMESPACE="${KUBE_NAMESPACE:-stage3-logs}"
KAFKA_NODE_IMAGE="${KAFKA_NODE_IMAGE:-docker.io/apache/kafka:4.3.1}"
KAFKA_POD_IMAGE="${KAFKA_POD_IMAGE:-apache/kafka:4.3.1}"

fail() {
  echo "$1" >&2
  exit 1
}

case "${1:-}" in
  node)
    command -v "${MINIKUBE}" >/dev/null 2>&1 || fail "缺少命令：${MINIKUBE}"

    # 本地部署只要求明确版本的镜像已经旁加载，不绑定特定机器转换出的摘要。
    "${MINIKUBE}" ssh -p "${KUBE_CONTEXT}" -- \
      sudo crictl inspecti "${KAFKA_NODE_IMAGE}" >/dev/null 2>&1 ||
      fail "Minikube 节点缺少 Kafka 镜像：${KAFKA_NODE_IMAGE}"

    echo "Kafka 节点镜像已就绪：${KAFKA_NODE_IMAGE}"
    ;;
  pod)
    command -v "${KUBECTL}" >/dev/null 2>&1 || fail "缺少命令：${KUBECTL}"

    pod_facts="$(
      "${KUBECTL}" --context="${KUBE_CONTEXT}" get pod kafka-0 \
        -n "${KUBE_NAMESPACE}" \
        -o 'jsonpath={.spec.containers[?(@.name=="kafka")].image}{"|"}{.spec.initContainers[?(@.name=="prepare-config")].image}{"|"}{.status.containerStatuses[?(@.name=="kafka")].imageID}{"|"}{.status.initContainerStatuses[?(@.name=="prepare-config")].imageID}'
    )"
    IFS='|' read -r main_image init_image main_image_id init_image_id <<<"${pod_facts}"

    [[ "${main_image}" == "${KAFKA_POD_IMAGE}" ]] ||
      fail "Kafka 主容器版本异常：实际 ${main_image:-缺失}，要求 ${KAFKA_POD_IMAGE}"
    [[ "${init_image}" == "${KAFKA_POD_IMAGE}" ]] ||
      fail "Kafka 初始化容器版本异常：实际 ${init_image:-缺失}，要求 ${KAFKA_POD_IMAGE}"
    [[ -n "${main_image_id}" && -n "${init_image_id}" ]] ||
      fail "Kafka Pod 尚未形成完整运行时镜像记录"

    echo "Kafka Pod 镜像版本通过：${KAFKA_POD_IMAGE}"
    ;;
  *)
    echo "用法：$0 node|pod" >&2
    exit 2
    ;;
esac
