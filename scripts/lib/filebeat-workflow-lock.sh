#!/usr/bin/env bash

# acquire_filebeat_workflow_lock 串行化会创建固定名 Job、节点 fixture 或重建采集器 Pod 的流程。
# 恢复脚本持锁调用子验收时，子进程必须继承同一个文件描述符，不能通过环境变量绕过互斥。
acquire_filebeat_workflow_lock() {
  local lock_dir lock_file inherited_target

  lock_dir="/run/user/$(id -u)"
  if [[ ! -d "${lock_dir}" || ! -w "${lock_dir}" ]]; then
    lock_dir="/tmp"
  fi
  lock_file="${lock_dir}/distributed-log-platform-filebeat-workflow-$(id -u).lock"

  case "${FILEBEAT_WORKFLOW_LOCK_INHERITED:-0}" in
    0)
      exec 9>"${lock_file}"
      flock -n 9 || {
        echo "已有另一个 Filebeat 部署或验收流程正在运行" >&2
        return 1
      }
      ;;
    1)
      [[ -e /proc/self/fd/9 ]] || {
        echo "声明继承 Filebeat 工作流锁，但文件描述符 9 不存在" >&2
        return 1
      }
      inherited_target="$(readlink -f /proc/self/fd/9)"
      [[ "${inherited_target}" == "${lock_file}" ]] || {
        echo "继承的 Filebeat 工作流锁目标不正确：${inherited_target}" >&2
        return 1
      }
      flock -n 9 || {
        echo "继承的 Filebeat 工作流锁无效" >&2
        return 1
      }
      ;;
    *)
      echo "FILEBEAT_WORKFLOW_LOCK_INHERITED 只能是 0 或 1" >&2
      return 1
      ;;
  esac
}
