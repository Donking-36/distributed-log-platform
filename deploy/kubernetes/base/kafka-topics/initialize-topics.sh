#!/usr/bin/env bash
set -Eeuo pipefail

export LC_ALL=C

readonly BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
readonly TOPICS_CLI="/opt/kafka/bin/kafka-topics.sh"
readonly CONFIGS_CLI="/opt/kafka/bin/kafka-configs.sh"
readonly BROKER_API_CLI="/opt/kafka/bin/kafka-broker-api-versions.sh"
readonly REPLICATION_FACTOR=1
readonly CLEANUP_POLICY="delete"
readonly RETENTION_MS=86400000
readonly RETENTION_BYTES=134217728
readonly SEGMENT_BYTES=67108864
readonly MIN_INSYNC_REPLICAS=1
readonly TOPICS=(
  "logs.api-service"
  "logs.worker-service"
  "logs.unclassified"
  "logs.dlq"
)
readonly PARTITIONS=(3 3 1 1)
readonly CONFIG_KEYS=(
  "cleanup.policy"
  "retention.ms"
  "retention.bytes"
  "segment.bytes"
  "min.insync.replicas"
)
readonly CONFIG_VALUES=(
  "${CLEANUP_POLICY}"
  "${RETENTION_MS}"
  "${RETENTION_BYTES}"
  "${SEGMENT_BYTES}"
  "${MIN_INSYNC_REPLICAS}"
)

VALIDATED_TOPIC_ID=""

fail() {
  echo "主题初始化失败：$1" >&2
  exit 1
}

# describe/alter 的 --topic 参数是正则；固定主题必须转义点号并锚定首尾。
exact_topic_regex() {
  local escaped="${1//./\\.}"
  printf '^%s$' "${escaped}"
}

describe_topic() {
  local topic="$1"
  local output

  if ! output="$(${TOPICS_CLI} \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --describe \
    --if-exists \
    --topic "$(exact_topic_regex "${topic}")")"; then
    fail "无法 describe 主题 ${topic}"
  fi

  printf '%s' "${output}"
}

describe_topic_configs() {
  local topic="$1"
  local output

  if ! output="$(${CONFIGS_CLI} \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --entity-type topics \
    --entity-name "${topic}" \
    --describe)"; then
    fail "无法读取主题 ${topic} 的动态配置"
  fi

  printf '%s' "${output}"
}

extract_tab_field() {
  local line="$1"
  local field_name="$2"

  awk -F '\t' -v wanted="${field_name}" '
    {
      for (column = 1; column <= NF; column++) {
        field = $column
        sub(/^[[:space:]]+/, "", field)
        prefix = wanted ":"
        if (substr(field, 1, length(prefix)) == prefix) {
          sub("^" prefix "[[:space:]]*", "", field)
          sub(/[[:space:]]+$/, "", field)
          print field
          exit
        }
      }
    }
  ' <<<"${line}"
}

# kafka-topics 的 Configs 字段会混入继承值，且配置值中的逗号没有转义；
# 显式 override 必须改用 kafka-configs 的逐行动态配置输出验证。
validate_topic_configs() {
  local topic="$1"
  local description="$2"
  local header entry config_pair key value known expected_value line_index key_index
  local -a lines=()
  local -A seen_keys=()

  mapfile -t lines < <(awk 'NF { print }' <<<"${description}")
  [[ "${#lines[@]}" -ge 1 ]] || fail "主题 ${topic} 的动态配置输出为空"
  header="${lines[0]}"
  [[ "${header}" == "Dynamic configs for topic ${topic} are:" ]] ||
    fail "主题 ${topic} 的动态配置 header 异常：${header}"

  for ((line_index = 1; line_index < ${#lines[@]}; line_index++)); do
    entry="${lines[line_index]}"
    entry="${entry#"${entry%%[![:space:]]*}"}"
    [[ "${entry}" == *" sensitive="*" synonyms={"* ]] ||
      fail "主题 ${topic} 的动态配置行格式异常：${entry}"
    config_pair="${entry%% sensitive=*}"
    [[ "${config_pair}" == *=* ]] || fail "主题 ${topic} 存在无效动态配置：${entry}"
    key="${config_pair%%=*}"
    value="${config_pair#*=}"

    known=false
    expected_value=""
    for key_index in "${!CONFIG_KEYS[@]}"; do
      if [[ "${key}" == "${CONFIG_KEYS[key_index]}" ]]; then
        known=true
        expected_value="${CONFIG_VALUES[key_index]}"
        break
      fi
    done
    [[ "${known}" == "true" ]] || fail "主题 ${topic} 存在未受管动态配置 ${key}"
    [[ -z "${seen_keys[${key}]+present}" ]] || fail "主题 ${topic} 的动态配置 ${key} 重复"
    seen_keys["${key}"]=1
    [[ "${value}" == "${expected_value}" ]] ||
      fail "主题 ${topic} 的 ${key}=${value}，要求 ${expected_value}"
    [[ "${entry}" == *"DYNAMIC_TOPIC_CONFIG:${key}=${value}"* ]] ||
      fail "主题 ${topic} 的 ${key} 不是主题级动态配置"
  done

  [[ "${#seen_keys[@]}" -eq "${#CONFIG_KEYS[@]}" ]] ||
    fail "主题 ${topic} 的显式动态配置数量为 ${#seen_keys[@]}，要求 ${#CONFIG_KEYS[@]}"
  for key in "${CONFIG_KEYS[@]}"; do
    [[ -n "${seen_keys[${key}]+present}" ]] || fail "主题 ${topic} 缺少显式动态配置 ${key}"
  done
}

# 现有主题只做严格断言，不自动扩分区、改副本或缩短保留期。
validate_topic() {
  local topic="$1"
  local expected_partitions="$2"
  local description="$3"
  local header header_topic topic_id partition_count replication_factor
  local line partition_id leader replicas isr adding removing
  local -a headers partition_lines
  local -A seen_partitions=()

  mapfile -t headers < <(awk '/PartitionCount:/ { print }' <<<"${description}")
  [[ "${#headers[@]}" -eq 1 ]] ||
    fail "主题 ${topic} 的 header 数量为 ${#headers[@]}，要求 1"
  header="${headers[0]}"

  header_topic="$(extract_tab_field "${header}" "Topic")"
  topic_id="$(extract_tab_field "${header}" "TopicId")"
  partition_count="$(extract_tab_field "${header}" "PartitionCount")"
  replication_factor="$(extract_tab_field "${header}" "ReplicationFactor")"

  [[ "${header_topic}" == "${topic}" ]] || fail "describe 返回了意外主题 ${header_topic:-缺失}"
  [[ "${topic_id}" =~ ^[A-Za-z0-9_-]+$ ]] || fail "主题 ${topic} 的 TopicId 无效：${topic_id:-缺失}"
  [[ "${partition_count}" == "${expected_partitions}" ]] ||
    fail "主题 ${topic} 的分区数为 ${partition_count:-缺失}，要求 ${expected_partitions}"
  [[ "${replication_factor}" == "${REPLICATION_FACTOR}" ]] ||
    fail "主题 ${topic} 的副本因子为 ${replication_factor:-缺失}，要求 ${REPLICATION_FACTOR}"

  mapfile -t partition_lines < <(awk '/Partition:/ && !/PartitionCount:/ { print }' <<<"${description}")
  [[ "${#partition_lines[@]}" -eq "${expected_partitions}" ]] ||
    fail "主题 ${topic} 的分区明细为 ${#partition_lines[@]} 行，要求 ${expected_partitions}"

  for line in "${partition_lines[@]}"; do
    [[ "$(extract_tab_field "${line}" "Topic")" == "${topic}" ]] ||
      fail "主题 ${topic} 的分区明细包含其他主题"
    partition_id="$(extract_tab_field "${line}" "Partition")"
    leader="$(extract_tab_field "${line}" "Leader")"
    replicas="$(extract_tab_field "${line}" "Replicas")"
    isr="$(extract_tab_field "${line}" "Isr")"
    adding="$(extract_tab_field "${line}" "Adding Replicas")"
    removing="$(extract_tab_field "${line}" "Removing Replicas")"

    [[ "${partition_id}" =~ ^[0-9]+$ ]] || fail "主题 ${topic} 存在无效分区 ID"
    [[ -z "${seen_partitions[${partition_id}]+present}" ]] ||
      fail "主题 ${topic} 的分区 ${partition_id} 重复"
    seen_partitions["${partition_id}"]=1
    [[ "${leader}" =~ ^[0-9]+$ ]] || fail "主题 ${topic} 分区 ${partition_id} 没有可用 Leader"
    [[ "${replicas}" =~ ^[0-9]+$ ]] ||
      fail "主题 ${topic} 分区 ${partition_id} 的副本集合异常：${replicas:-缺失}"
    [[ "${isr}" == "${replicas}" ]] ||
      fail "主题 ${topic} 分区 ${partition_id} 未达到完整 ISR"
    [[ -z "${adding}" && -z "${removing}" ]] ||
      fail "主题 ${topic} 分区 ${partition_id} 正在重分配副本"
  done

  for ((partition_id = 0; partition_id < expected_partitions; partition_id++)); do
    [[ -n "${seen_partitions[${partition_id}]+present}" ]] ||
      fail "主题 ${topic} 缺少分区 ${partition_id}"
  done

  VALIDATED_TOPIC_ID="${topic_id}"
}

expect_validation_failure() {
  local case_name="$1"
  shift

  if ("$@" >/dev/null 2>&1); then
    fail "解析反例被错误放行：${case_name}"
  fi
}

run_parser_self_test() {
  local valid_topology composite_cleanup missing_override extra_override wrong_replication incomplete_isr reassigning
  local valid_configs

  valid_topology=$'Topic: logs.fixture\tTopicId: Fixture_123\tPartitionCount: 1\tReplicationFactor: 1\tConfigs: cleanup.policy=delete\n\tTopic: logs.fixture\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: N/A\tLastKnownElr: N/A'
  valid_configs=$'Dynamic configs for topic logs.fixture are:\n  cleanup.policy=delete sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=delete, DEFAULT_CONFIG:log.cleanup.policy=delete}\n  min.insync.replicas=1 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:min.insync.replicas=1, DEFAULT_CONFIG:min.insync.replicas=1}\n  retention.bytes=134217728 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.bytes=134217728, DEFAULT_CONFIG:log.retention.bytes=-1}\n  retention.ms=86400000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=86400000}\n  segment.bytes=67108864 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:segment.bytes=67108864, DEFAULT_CONFIG:log.segment.bytes=1073741824}'

  validate_topic "logs.fixture" 1 "${valid_topology}"
  validate_topic_configs "logs.fixture" "${valid_configs}"

  composite_cleanup="${valid_configs//cleanup.policy=delete/cleanup.policy=delete,compact}"
  missing_override=$'Dynamic configs for topic logs.fixture are:\n  cleanup.policy=delete sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=delete}\n  min.insync.replicas=1 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:min.insync.replicas=1}\n  retention.bytes=134217728 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.bytes=134217728}\n  segment.bytes=67108864 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:segment.bytes=67108864}'
  extra_override="${valid_configs}"$'\n  compression.type=gzip sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:compression.type=gzip}'
  wrong_replication="${valid_topology/ReplicationFactor: 1/ReplicationFactor: 2}"
  incomplete_isr="${valid_topology/Isr: 1/Isr: }"
  reassigning="${valid_topology}"$'\tAdding Replicas: 2\tRemoving Replicas: '

  expect_validation_failure "cleanup.policy 复合值" \
    validate_topic_configs "logs.fixture" "${composite_cleanup}"
  expect_validation_failure "缺少主题级 override" \
    validate_topic_configs "logs.fixture" "${missing_override}"
  expect_validation_failure "额外主题级 override" \
    validate_topic_configs "logs.fixture" "${extra_override}"
  expect_validation_failure "错误副本因子" \
    validate_topic "logs.fixture" 1 "${wrong_replication}"
  expect_validation_failure "ISR 不完整" \
    validate_topic "logs.fixture" 1 "${incomplete_isr}"
  expect_validation_failure "副本重分配" \
    validate_topic "logs.fixture" 1 "${reassigning}"

  echo "Kafka 主题初始化解析自测通过：1 个正例、6 个失败反例"
}

case "${1:-}" in
  self-test)
    run_parser_self_test
    exit 0
    ;;
  "")
    ;;
  *)
    fail "用法：$0 [self-test]"
    ;;
esac

for command_path in "${TOPICS_CLI}" "${CONFIGS_CLI}" "${BROKER_API_CLI}"; do
  [[ -x "${command_path}" ]] || fail "缺少 Kafka CLI：${command_path}"
done

broker_ready=false
for ((attempt = 1; attempt <= 30; attempt++)); do
  if "${BROKER_API_CLI}" --bootstrap-server "${BOOTSTRAP_SERVER}" >/dev/null 2>&1; then
    broker_ready=true
    break
  fi
  sleep 2
done
[[ "${broker_ready}" == "true" ]] || fail "60 秒内无法连接 Broker ${BOOTSTRAP_SERVER}"

declare -a missing_topics=()

# 第一阶段先检查全部已存在主题，避免发现致命漂移前已经修改部分集群状态。
for index in "${!TOPICS[@]}"; do
  topic="${TOPICS[index]}"
  description="$(describe_topic "${topic}")"
  if [[ -z "${description//[[:space:]]/}" ]]; then
    missing_topics+=("${index}")
    continue
  fi
  validate_topic "${topic}" "${PARTITIONS[index]}" "${description}"
  config_description="$(describe_topic_configs "${topic}")"
  validate_topic_configs "${topic}" "${config_description}"
done

for index in "${missing_topics[@]}"; do
  topic="${TOPICS[index]}"
  "${TOPICS_CLI}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS[index]}" \
    --replication-factor "${REPLICATION_FACTOR}" \
    --config "cleanup.policy=${CLEANUP_POLICY}" \
    --config "retention.ms=${RETENTION_MS}" \
    --config "retention.bytes=${RETENTION_BYTES}" \
    --config "segment.bytes=${SEGMENT_BYTES}" \
    --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}"
done

# 第二阶段重新读取 Broker 事实，并输出供宿主 runner 严格核对的稳定证据。
for index in "${!TOPICS[@]}"; do
  topic="${TOPICS[index]}"
  description="$(describe_topic "${topic}")"
  [[ -n "${description//[[:space:]]/}" ]] || fail "主题 ${topic} 创建后仍不存在"
  validate_topic "${topic}" "${PARTITIONS[index]}" "${description}"
  config_description="$(describe_topic_configs "${topic}")"
  validate_topic_configs "${topic}" "${config_description}"
  printf \
    'TOPIC_VERIFIED name=%s topic_id=%s partitions=%s replication_factor=%s cleanup.policy=%s retention.ms=%s retention.bytes=%s segment.bytes=%s min.insync.replicas=%s\n' \
    "${topic}" \
    "${VALIDATED_TOPIC_ID}" \
    "${PARTITIONS[index]}" \
    "${REPLICATION_FACTOR}" \
    "${CLEANUP_POLICY}" \
    "${RETENTION_MS}" \
    "${RETENTION_BYTES}" \
    "${SEGMENT_BYTES}" \
    "${MIN_INSYNC_REPLICAS}"
done
