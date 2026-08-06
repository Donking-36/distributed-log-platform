#!/usr/bin/env bash
set -Eeuo pipefail

DOCKER="${DOCKER:-docker}"
FILEBEAT_LOCAL_IMAGE="${FILEBEAT_LOCAL_IMAGE:-docker.elastic.co/beats/filebeat-wolfi:9.4.4}"

fail() {
  echo "$1" >&2
  exit 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
config_file="${repo_root}/deploy/kubernetes/base/filebeat/filebeat.yml"

command -v "${DOCKER}" >/dev/null 2>&1 || fail "缺少命令：${DOCKER}"
[[ -f "${config_file}" ]] || fail "缺少 Filebeat 配置：${config_file}"
image_facts="$(${DOCKER} image inspect --platform linux/amd64 "${FILEBEAT_LOCAL_IMAGE}" \
  --format '{{.Architecture}}|{{.Os}}')"
expected_image_facts="amd64|linux"
[[ "${image_facts}" == "${expected_image_facts}" ]] ||
  fail "Filebeat 镜像平台不匹配：实际 ${image_facts}，要求 ${expected_image_facts}"

umask 077
work_dir="$(mktemp -d)"
cleanup() {
  rm -f -- "${work_dir}"/*.log
  rmdir -- "${work_dir}"
}
trap cleanup EXIT

common_args=(
  run --rm --pull=never
  --user 0:0
  --read-only
  --env NODE_NAME=stage3-logs
  --mount "type=bind,source=${config_file},target=/etc/filebeat.yml,readonly"
  --tmpfs /usr/share/filebeat/data:rw,noexec,nosuid,nodev,size=16m
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m
  "${FILEBEAT_LOCAL_IMAGE}"
  test config
  -c /etc/filebeat.yml
  -e
  --strict.perms=false
)

"${DOCKER}" "${common_args[@]}" >"${work_dir}/baseline.log" 2>&1
grep -Fqx 'Config OK' "${work_dir}/baseline.log" || fail "Filebeat 基线配置未输出 Config OK"

# 使用相同参数位置先证明最高合法协议版本可解析，防止反例因参数拼接错误而假通过。
"${DOCKER}" "${common_args[@]}" -E output.kafka.version=4.1.0 >"${work_dir}/legal-version.log" 2>&1
grep -Fqx 'Config OK' "${work_dir}/legal-version.log" || fail "Filebeat Kafka 4.1.0 协议正例未输出 Config OK"

# 非法版本必须由 Filebeat 的协议解析器拒绝，不能把 Docker 或参数故障当作有效反例。
if "${DOCKER}" "${common_args[@]}" -E output.kafka.version=4.3.1 >"${work_dir}/illegal-version.log" 2>&1; then
  fail "Filebeat 非法 Kafka 协议版本反例意外通过"
else
  illegal_status=$?
fi
[[ "${illegal_status}" -eq 1 ]] || fail "Filebeat Kafka 版本反例退出码异常：${illegal_status}"
grep -Fq "unknown/unsupported kafka version '4.3.1' accessing 'output.kafka.version'" \
  "${work_dir}/illegal-version.log" || fail "Filebeat Kafka 版本反例失败原因不匹配"

echo "Filebeat 配置检查通过：镜像平台、基线/4.1.0 正例及 4.3.1 反例全部匹配"
