#!/usr/bin/env bash

# 本库只封装 Filebeat 验收共同使用的 Kafka 证据操作，不负责创建资源或决定验收流程。
# 调用方必须先定义 KUBECTL、KUBE_CONTEXT、KUBE_NAMESPACE、KAFKA_POD 和 KAFKA_CONTAINER。

# filebeat_kafka_exec 在 Broker Pod 内执行 Kafka CLI，避免依赖本机安装 Kafka 工具。
filebeat_kafka_exec() {
  "${KUBECTL}" --context="${KUBE_CONTEXT}" exec "${KAFKA_POD}" \
    -n "${KUBE_NAMESPACE}" -c "${KAFKA_CONTAINER}" -- \
    env "KAFKA_HEAP_OPTS=-Xms32m -Xmx128m" "KAFKA_GC_LOG_OPTS=-Xlog:gc=off" "$@"
}

# filebeat_get_offset 从已保存的高水位文件中严格读取一个主题分区的唯一位点。
filebeat_get_offset() {
  local offset_file="$1"
  local topic="$2"
  local partition="$3"
  local prefix="${topic}:${partition}:"
  local matches=()
  local offset

  mapfile -t matches < <(grep -F "${prefix}" "${offset_file}" || true)
  [[ "${#matches[@]}" -eq 1 && "${matches[0]}" == "${prefix}"* ]] || {
    echo "主题 ${topic} 分区 ${partition} 位点缺失或重复" >&2
    return 1
  }
  offset="${matches[0]#${prefix}}"
  [[ "${offset}" =~ ^[0-9]+$ ]] || {
    echo "主题 ${topic} 分区 ${partition} 位点非法：${offset}" >&2
    return 1
  }
  printf '%s' "${offset}"
}

# filebeat_capture_offsets 保存主题全部分区的高水位，并验证分区数量和位点格式。
filebeat_capture_offsets() {
  local topic="$1"
  local expected_partitions="$2"
  local output_file="$3"
  local matching_count partition

  filebeat_kafka_exec /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "${topic}" >"${output_file}"

  matching_count="$(grep -Fc "${topic}:" "${output_file}" || true)"
  [[ "${matching_count}" -eq "${expected_partitions}" ]] || {
    echo "主题 ${topic} 位点清单异常：实际 ${matching_count} 个分区，要求 ${expected_partitions}" >&2
    return 1
  }
  for ((partition = 0; partition < expected_partitions; partition += 1)); do
    filebeat_get_offset "${output_file}" "${topic}" "${partition}" >/dev/null || return 1
  done
}

# filebeat_consume_partition_range 只读取调用方锁定的有界分区窗口，不使用消费者组位点。
filebeat_consume_partition_range() {
  local topic="$1"
  local partition="$2"
  local start_offset="$3"
  local count="$4"
  local output_file="$5"

  [[ "${start_offset}" =~ ^[0-9]+$ && "${count}" =~ ^[1-9][0-9]*$ ]] || {
    echo "Kafka 有界消费参数非法：topic=${topic} partition=${partition} start=${start_offset} count=${count}" >&2
    return 1
  }

  filebeat_kafka_exec /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${topic}" \
    --partition "${partition}" \
    --offset "${start_offset}" \
    --max-messages "${count}" \
    --timeout-ms 15000 \
    --command-property enable.auto.commit=false \
    --formatter-property print.partition=true \
    --formatter-property print.offset=true \
    --formatter-property print.key=true \
    --formatter-property print.value=true \
    >>"${output_file}"
}
