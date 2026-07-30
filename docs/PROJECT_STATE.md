# Project State

- Updated: 2026-07-30 16:07 +08:00
- Current day/use case: Day 1 — 文档基线完成，准备创建本地提交检查点
- Active branch: `main`
- Last green commit: 无；仓库尚无提交
- Working tree: 恢复锚点、需求、架构和两份 ADR 均已验证，尚未提交

## Completed and verified

- WSL 与工具基线：
  - 验证：`whoami`、`echo "$WSL_DISTRO_NAME"`、`free -h`、`command -v go`、`go version`、`docker version`
  - 结果：用户 `yanami`；Ubuntu-24.04；内存 7.6 GiB、swap 2 GiB；Go 1.26.5；Docker Client/Server 29.6.2 正常。
- `stage3-logs` 内存与集群健康：
  - 验证：`minikube profile list`、`docker inspect stage3-logs`、`kubectl get nodes -o wide`、`kubectl get pods -n kube-system -o wide`
  - 结果：Docker driver、containerd runtime；外层内存限制 `6442450944` 字节；节点 `Ready`，全部系统 Pod `Running` 且 Ready。
- CNI 故障已修复：
  - 证据：Kindnet 镜像拉取连接重置/TLS 超时，导致 `ImagePullBackOff`、CNI 未初始化和节点 `NotReady`。
  - 处理：从宿主 Docker 拉取精确镜像，经 `minikube image load --daemon` 旁加载并滚动重启 Kindnet；未删除或重配 profile。
  - 结果：Kindnet、CoreDNS 和 storage-provisioner 恢复为 `Running`。
- 本地仓库与恢复锚点：
  - 结果：独立目录 `/home/yanami/projects/distributed-log-platform`；Git 身份 `Donking-36 <1633354106@qq.com>`；已初始化 `main`，无远端、无提交。
  - 文档：已创建 `AGENTS.md`、`docs/PLAN.md`、`docs/PROJECT_STATE.md`、`docs/requirements.md`、`docs/architecture.md` 和两份 ADR。
  - 验证：源指南与 `docs/PLAN.md` 的 SHA-256 均为 `72a456cc420b9694067f4264b3444518da317d9bdc96b2eb4bc5e79ebc303607`。
  - 需求基线：UC-001/UC-002 的固定测试数据、验收、日志契约、量化口径和非目标已记录；`docs/requirements.md` SHA-256 为 `2ca5359521115708c5cb4c24e766fee41e9120c57994448b01e7211b8d542034`。
  - 架构基线：核心数据流、组件职责、Kubernetes 形态、安全边界、验证层次和诚实的版本门禁已记录；未验证应用版本保持 `TBD`。
  - 架构决策：ADR-001 固定 Kafka→Go processor→Elasticsearch 主链；ADR-002 固定至少一次投递、确定性 ID、连续 offset 前沿和 DLQ 边界。
  - 文件校验：`architecture.md`、ADR-001、ADR-002 的 SHA-256 分别为 `9d3a625c0c98e609d76015ffc8e781c6fac7d75905ddbad361ce140ef053135a`、`f3636080f77ce4099d682d6a0fd417e54e564f882918502dc8727dbce9552326`、`c5a34496b599a1682471d2de8b52fcf1c8e3f9330b9ce799fb78af7863d97a9b`。
- CPU 限制验证：
  - 验证：Docker `NanoCpus`、quota/period、cpuset，以及容器内 `nproc`、`cpu.max`、`cpuset.cpus.effective`。
  - 结果：`nano=0 quota=0 period=0 cpuset=`；`nproc=20`；`cpu.max=max 100000`；有效 CPU 为 `0-19`。
  - profile 配置：`Memory=6144`、`CPUs=4`、Docker driver、containerd runtime。
  - 修复：经用户明确授权，执行 `docker update --cpus 4 stage3-logs`；未删除、重建或重启 profile。
  - 再验证：`NanoCpus=4000000000`，`cpu.max=400000 100000`，内存仍为 `6442450944` 字节；节点 Ready，全部系统 Pod 保持 Running。
  - 结论：4 CPU、6 GiB 外层限制和集群健康门禁均已通过。`nproc=20` 表示可见逻辑 CPU 数，不否定 cgroup 的 4 CPU 时间配额。

## Current blocker

- 无。

## Decisions since last checkpoint

- 新仓库位于 WSL Linux 文件系统，不在 `/mnt/d/codesource/go2` 根目录初始化。
- Kindnet 采用镜像旁加载的最小修复，保留现有 `stage3-logs` profile。
- 对 Minikube 未落实的 CPU 限制采用 Docker 实时 quota 修复，避免删除或重建健康集群。
- 应用镜像和 Go 客户端版本必须先通过对应兼容性冒烟，再替换架构矩阵中的 `TBD`；禁止凭记忆选择 `latest`。

## Unverified assumptions

- 若 Minikube 未来重新创建外层容器，Docker 实时 CPU quota 是否仍会保持，尚未验证；恢复工作时应重新检查。
- Kafka、Filebeat、Elasticsearch、Grafana 和 Go 客户端的实际固定版本尚未验证。

## Next single action

- 审查文件清单并创建第一个本地恢复锚点提交；不创建远端或 push。

## Resume commands

```bash
cd ~/projects/distributed-log-platform
git status --short --branch
kubectl config current-context
kubectl --context=stage3-logs get nodes
docker inspect stage3-logs --format 'nano={{.HostConfig.NanoCpus}} quota={{.HostConfig.CpuQuota}} period={{.HostConfig.CpuPeriod}}'
docker exec stage3-logs cat /sys/fs/cgroup/cpu.max
```
