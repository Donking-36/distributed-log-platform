# 项目状态

- 更新时间：2026-07-30 16:34 +08:00
- 当前天数/用例：第 1 天——Go 模块与中文文档基线已验证，准备定义演示程序的最小行为
- 当前分支：`feature/bootstrap`
- 最后绿灯提交：`3643955 docs: define use cases and architecture baseline`
- 工作区：8 份 Markdown 文档已中文化；`README.md`、`.gitignore`、`go.mod` 未跟踪

## 已完成并验证

- WSL 与工具基线：
  - 验证：`whoami`、`echo "$WSL_DISTRO_NAME"`、`free -h`、`command -v go`、`go version`、`docker version`
  - 结果：用户 `yanami`；Ubuntu-24.04；内存 7.6 GiB、交换空间 2 GiB；Go 1.26.5；Docker 客户端和服务端 29.6.2 正常。
- Go 模块：
  - 验证：`cat go.mod`、`go version`、`go env GOMOD`
  - 结果：模块路径为 `github.com/Donking-36/distributed-log-platform`；`go.mod` 声明 Go 1.26.5；实际工具链为 `go1.26.5 linux/amd64`。
- `stage3-logs` 内存与集群健康：
  - 验证：`minikube profile list`、`docker inspect stage3-logs`、`kubectl get nodes -o wide`、`kubectl get pods -n kube-system -o wide`
  - 结果：Docker 驱动、containerd 运行时；外层内存限制 `6442450944` 字节；节点已就绪，全部系统 Pod 正常运行且已就绪。
- CNI 故障已修复：
  - 证据：Kindnet 镜像拉取连接重置/TLS 超时，导致 `ImagePullBackOff`、CNI 未初始化和节点 `NotReady`。
  - 处理：从宿主 Docker 拉取精确镜像，经 `minikube image load --daemon` 旁加载并滚动重启 Kindnet；未删除或重配配置档。
  - 结果：Kindnet、CoreDNS 和 storage-provisioner 恢复为 `Running`。
- 本地仓库与恢复锚点：
  - 结果：独立目录 `/home/yanami/projects/distributed-log-platform`；Git 身份 `Donking-36 <1633354106@qq.com>`；仓库从 `main` 初始化。
  - 文档：已创建 `AGENTS.md`、`docs/PLAN.md`、`docs/PROJECT_STATE.md`、`docs/requirements.md`、`docs/architecture.md` 和两份 ADR。
  - 验证：建仓首次复制时，源指南与 `docs/PLAN.md` 的 SHA-256 相同；随后按用户决定完成中文化和 Go 版本更新，当前不再要求与原件逐字相同。
  - 中文化：`AGENTS.md`、`README.md`、`docs/PLAN.md`、`docs/PROJECT_STATE.md`、需求、架构和两份 ADR 的读者向标题与叙述均已改为中文；命令、路径、产品名、字段名和必要技术标识保持原样。
  - 需求基线：UC-001/UC-002 的固定测试数据、验收、日志契约、量化口径和非目标已记录；`docs/requirements.md` SHA-256 为 `754a84de18bedcb38642f7fb93f7122e54275ab3ee82c54f4c21d935ebb97132`。
  - 架构基线：核心数据流、组件职责、Kubernetes 形态、安全边界、验证层次和诚实的版本门禁已记录；未验证应用版本保持待定。
  - 架构决策：ADR-001 固定 Kafka→Go 处理器→Elasticsearch 主链；ADR-002 固定至少一次投递、确定性标识、连续位点前沿和死信队列边界。
  - 文件校验：`docs/PLAN.md`、`architecture.md`、ADR-001、ADR-002 的 SHA-256 分别为 `839a947426a14c7d36626128d8d75f55ef842323fdd641cc4d32fecae23a5294`、`4e557898ced128d948575ac214b3f911a7134f0278065bb82371c6b1ace19ba9`、`0f7d22b279c78ff06953036a9c7abfd21d3451ff8f2e66fc1ae390fc9c14b5c7`、`0eca4d40a8d9f9130f0fb06803a3bf5e12e19ffe21efe74c642a4766ed82379a`。
  - 本地提交：`1c848e0 chore: initialize stage three repository`；`3643955 docs: define use cases and architecture baseline`。
  - 本地分支：`main`、`develop`、`feature/bootstrap`；当前为 `feature/bootstrap`。
  - 仓库入口：已创建并复核简洁 `README.md` 和 `.gitignore`；没有声称尚不存在的构建、测试或部署命令可用。
  - 远端事实：用户已创建 `https://github.com/Donking-36/distributed-log-platform.git`；本地尚未配置 `origin`，未推送。
- CPU 限制验证：
  - 验证：Docker `NanoCpus`、配额/周期、处理器集合，以及容器内 `nproc`、`cpu.max`、`cpuset.cpus.effective`。
  - 结果：`nano=0 quota=0 period=0 cpuset=`；`nproc=20`；`cpu.max=max 100000`；有效 CPU 为 `0-19`。
  - 配置档：`Memory=6144`、`CPUs=4`、Docker 驱动、containerd 运行时。
  - 修复：经用户明确授权，执行 `docker update --cpus 4 stage3-logs`；未删除、重建或重启配置档。
  - 再验证：`NanoCpus=4000000000`，`cpu.max=400000 100000`，内存仍为 `6442450944` 字节；节点 Ready，全部系统 Pod 保持 Running。
  - 结论：4 CPU、6 GiB 外层限制和集群健康门禁均已通过。`nproc=20` 表示可见逻辑 CPU 数，不否定 cgroup 的 4 CPU 时间配额。

## 当前阻塞

- 无。

## 自上次检查点以来的决策

- 新仓库位于 WSL Linux 文件系统，不在 `/mnt/d/codesource/go2` 根目录初始化。
- Kindnet 采用镜像旁加载的最小修复，保留现有 `stage3-logs` 配置档。
- 对 Minikube 未落实的 CPU 限制采用 Docker 实时配额修复，避免删除或重建健康集群。
- 应用镜像和 Go 客户端版本必须先通过对应兼容性冒烟，再替换架构矩阵中的“待定”；禁止凭记忆选择 `latest`。
- 用户明确选择 `github.com/Donking-36/distributed-log-platform` 作为模块路径，并选择 Go 1.26.5 作为开发、`go.mod` 与持续集成的统一版本；该决定覆盖原计划的 Go 1.22 最低目标。
- 所有读者向文档统一使用中文；只有命令、路径、产品名、字段名和必要技术标识保留原文。

## 未验证假设

- 若 Minikube 未来重新创建外层容器，Docker 实时处理器配额是否仍会保持，尚未验证；恢复工作时应重新检查。
- Kafka、Filebeat、Elasticsearch、Grafana 和 Go 客户端的实际固定版本尚未验证。

## 唯一下一步

- 定义 `demo-app` 的最小输入输出和退出行为，并先写一个能失败的测试；暂不创建完整项目骨架。

## 恢复命令

```bash
cd ~/projects/distributed-log-platform
git status --short --branch
git log -5 --oneline --decorate
kubectl config current-context
kubectl --context=stage3-logs get nodes
docker inspect stage3-logs --format 'nano={{.HostConfig.NanoCpus}} quota={{.HostConfig.CpuQuota}} period={{.HostConfig.CpuPeriod}}'
docker exec stage3-logs cat /sys/fs/cgroup/cpu.max
```
