# 跨机器部署与演示指南

本文用于在另一台 Windows 电脑上，从空的 WSL2/Minikube 环境恢复并演示
`distributed-log-platform`。推荐先在当前电脑导出已经验证的六个镜像，再把镜像包
和仓库一起复制到新电脑；这样不需要重新构建历史固定标签，也不依赖演示现场访问
Docker Hub。

本文只覆盖本地教学演示，不把单节点 Minikube 当作生产高可用环境。

## 1. 演示内容

完整链路如下：

```text
api-service / worker-service
  → Kubernetes 容器标准输出
  → Filebeat DaemonSet
  → Kafka logs.<service>
  → Go log-processor
  → Elasticsearch logs-stage3-*
  → Grafana 分布式日志检索仪表盘
```

演示应证明：

- 两个服务持续输出结构化 JSON 日志；
- Filebeat 补充 Kubernetes 元数据并按服务路由到 Kafka；
- `log-processor` 校验、规范化并使用稳定 `event_id` 幂等写入 Elasticsearch；
- Grafana 支持时间、服务、关键词筛选，以及级别分布和 ERROR/WARN 趋势；
- `log-processor` 或 Grafana Pod 重建后可以恢复；
- 固定验收数据和临时资源能够自动清理。

## 2. 已验证版本与资源

推荐尽量保持下列版本，特别是 Minikube、Kubernetes 和容器运行时版本。镜像旁加载
后的 manifest 可能受运行时版本影响，版本差异过大可能触发仓库的镜像身份门禁。

| 项目 | 已验证值 |
|---|---|
| 系统 | Windows 11 + WSL2 Ubuntu 24.04 x86-64 |
| Go | 1.26.5，项目要求精确匹配 |
| Docker Engine | 29.6.2 |
| Minikube | 1.38.1 |
| Kubernetes | v1.35.1 |
| kubectl | v1.36.3；客户端应与集群相差不超过一个次版本 |
| 容器运行时 | containerd 2.2.1 |
| Minikube 资源 | 4 CPU、6144 MiB 内存 |
| 其他工具 | Git、GNU Make、Python 3、curl、sha256sum |

建议新电脑至少具备：

- x86-64 处理器；ARM 电脑不能直接使用本文导出的 amd64 镜像包；
- 16 GiB 物理内存；
- WSL 可用内存不少于 7 GiB；
- 至少 15 GiB 可用磁盘空间；六个镜像展开后约 5.3 GiB，Minikube、PVC 和日志还会
  继续占用空间；
- 演示期间关闭大型 IDE、虚拟机和其他高内存容器。

## 3. 在当前电脑准备迁移包

### 3.1 确认仓库和镜像

在当前电脑的 WSL Ubuntu 中执行：

```bash
cd ~/projects/distributed-log-platform
git switch develop
git pull --ff-only
git status --short --branch

docker image inspect \
  distributed-log-platform/log-producer:d20fc7f \
  distributed-log-platform/log-processor:9a776ee \
  apache/kafka:4.3.1 \
  docker.elastic.co/beats/filebeat-wolfi:9.4.4 \
  elasticsearch:9.4.4 \
  grafana/grafana:13.1.0 \
  --format '{{.RepoTags}}|{{.Id}}'
```

预期 `git status` 没有未提交内容，六个镜像都能打印标签和镜像 ID。任何镜像不存在
时先停止，不要在演示前临时使用 `latest` 代替。

### 3.2 导出仓库和镜像

```bash
mkdir -p ~/stage3-demo-transfer

cd ~/projects/distributed-log-platform
git bundle create \
  ~/stage3-demo-transfer/distributed-log-platform.bundle \
  --all

docker save \
  --output ~/stage3-demo-transfer/stage3-images.tar \
  distributed-log-platform/log-producer:d20fc7f \
  distributed-log-platform/log-processor:9a776ee \
  apache/kafka:4.3.1 \
  docker.elastic.co/beats/filebeat-wolfi:9.4.4 \
  elasticsearch:9.4.4 \
  grafana/grafana:13.1.0

# 可选但推荐：为网络受限的新电脑保存当前项目的 Go 模块缓存。
go mod download
tar -C "$(go env GOPATH)" \
  -czf ~/stage3-demo-transfer/go-module-cache.tar.gz \
  pkg/mod

cd ~/stage3-demo-transfer
sha256sum \
  distributed-log-platform.bundle \
  stage3-images.tar \
  go-module-cache.tar.gz \
  > SHA256SUMS

git bundle verify distributed-log-platform.bundle
sha256sum --check SHA256SUMS
ls -lh
```

将整个 `stage3-demo-transfer` 目录复制到移动硬盘或安全的文件传输位置。镜像包超过
4 GiB 时，目标磁盘不要使用 FAT32；使用 NTFS 或 exFAT。

如果新电脑能够稳定访问 GitHub，也可以不复制 Git bundle，直接克隆远端仓库；
镜像包仍建议复制。

## 4. 在新电脑安装基础环境

### 4.1 Windows 与 WSL2

如果尚未安装 WSL，在管理员 PowerShell 中执行：

```powershell
wsl --install -d Ubuntu-24.04
```

安装完成后重启电脑，并首次打开 Ubuntu 创建 Linux 用户。确认发行版使用 WSL2：

```powershell
wsl --version
wsl --list --verbose
```

如果 Ubuntu 的 `VERSION` 不是 `2`，按照微软 WSL 文档调整。官方说明见
[安装 WSL](https://learn.microsoft.com/windows/wsl/install)。

### 4.2 Docker Desktop

安装 Docker Desktop，选择 WSL2 backend，并在 Docker Desktop 的 WSL Integration
中启用 Ubuntu 24.04。官方步骤见
[Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/)
和 [Docker Desktop WSL2](https://docs.docker.com/desktop/features/wsl/use-wsl/)。

打开 Ubuntu，验证 Linux 容器引擎：

```bash
docker version
docker info --format 'architecture={{.Architecture}} cpus={{.NCPU}} memory={{.MemTotal}}'
```

预期架构为 `x86_64` 或 `amd64`，客户端和服务端都能返回版本。

### 4.3 WSL 工具

```bash
sudo apt update
sudo apt install -y git make python3 curl ca-certificates

git --version
make --version
python3 --version
curl --version
```

### 4.4 Go 1.26.5

从 [Go 官方安装页](https://go.dev/doc/install) 安装 Linux amd64 的 Go 1.26.5，
不要把新版本解压到旧的 Go 目录之上。安装后验证：

```bash
command -v go
go version
```

预期输出包含：

```text
go version go1.26.5 linux/amd64
```

项目的 `make check` 会拒绝其他 Go 版本。

### 4.5 kubectl v1.36.3

```bash
cd /tmp
curl -LO https://dl.k8s.io/release/v1.36.3/bin/linux/amd64/kubectl
curl -LO https://dl.k8s.io/release/v1.36.3/bin/linux/amd64/kubectl.sha256
echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
sudo install -m 0755 kubectl /usr/local/bin/kubectl
kubectl version --client
```

官方兼容性和其他安装方式见
[安装 kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)。

### 4.6 Minikube v1.38.1

```bash
cd /tmp
curl -LO \
  https://storage.googleapis.com/minikube/releases/v1.38.1/minikube-linux-amd64
curl -LO \
  https://storage.googleapis.com/minikube/releases/v1.38.1/minikube-linux-amd64.sha256
echo "$(cat minikube-linux-amd64.sha256)  minikube-linux-amd64" | \
  sha256sum --check
sudo install -m 0755 minikube-linux-amd64 /usr/local/bin/minikube
minikube version
```

Minikube 启动参数说明见
[minikube start](https://minikube.sigs.k8s.io/docs/commands/start/)。

## 5. 在新电脑恢复仓库

把迁移目录复制到新电脑的 WSL 文件系统，例如 `~/stage3-demo-transfer`，不要长期
从 `/mnt/c` 运行仓库，否则文件 I/O、权限和脚本行为可能不稳定。

### 5.1 有网络：从 GitHub 克隆

```bash
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/Donking-36/distributed-log-platform.git
cd distributed-log-platform
git switch develop
git pull --ff-only
```

### 5.2 无网络：从 Git bundle 克隆

```bash
mkdir -p ~/projects
cd ~/projects
git clone \
  ~/stage3-demo-transfer/distributed-log-platform.bundle \
  distributed-log-platform

cd distributed-log-platform
git switch develop
git remote set-url \
  origin \
  https://github.com/Donking-36/distributed-log-platform.git
```

无论使用哪种方式，都执行：

```bash
cd ~/projects/distributed-log-platform
git status --short --branch
git log -5 --oneline --decorate
git merge-base --is-ancestor 1ebdb87 HEAD
```

如果复制了 Go 模块缓存，在首次工程检查前恢复：

```bash
mkdir -p "$(go env GOPATH)"
tar -C "$(go env GOPATH)" \
  -xzf ~/stage3-demo-transfer/go-module-cache.tar.gz
```

然后运行工程检查：

```bash
cd ~/projects/distributed-log-platform
make check
```

`git merge-base` 返回 0 表示当前 `develop` 至少包含 UC-002 的合并提交。`make check`
应通过 Go 版本、格式、Shell 语法、Python 单测、`go vet`、Go 单测和构建。未复制
模块缓存时，首次检查需要联网下载 `go.mod` 中固定的依赖。

## 6. 导入固定镜像

先验证传输文件没有损坏：

```bash
cd ~/stage3-demo-transfer
sha256sum --check SHA256SUMS
docker load --input stage3-images.tar
```

确认六个固定标签：

```bash
docker image inspect \
  distributed-log-platform/log-producer:d20fc7f \
  distributed-log-platform/log-processor:9a776ee \
  apache/kafka:4.3.1 \
  docker.elastic.co/beats/filebeat-wolfi:9.4.4 \
  elasticsearch:9.4.4 \
  grafana/grafana:13.1.0 \
  --format '{{.RepoTags}}|{{.Id}}'
```

## 7. 创建 stage3-logs 集群

先确认没有同名旧配置档：

```bash
minikube profile list
```

如果新电脑没有 `stage3-logs`，执行：

```bash
minikube start -p stage3-logs \
  --driver=docker \
  --container-runtime=containerd \
  --kubernetes-version=v1.35.1 \
  --cpus=4 \
  --memory=6144

docker update --cpus 4 stage3-logs
```

`docker update` 用于确保外层 Minikube 容器实际受到 4 CPU CFS 配额约束。随后验证：

```bash
minikube status -p stage3-logs
kubectl --context=stage3-logs get nodes -o wide

docker inspect stage3-logs \
  --format 'nano={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}}'

docker exec stage3-logs cat /sys/fs/cgroup/cpu.max
```

预期：

- 节点为 `Ready`；
- 内存为 `6442450944` 字节；
- `NanoCpus` 为 `4000000000`；
- `cpu.max` 类似 `400000 100000`。

如果 Minikube 报内存不足，先执行 `free -h`。WSL 可用内存不足 7 GiB 时，应先增加
WSL 内存并重启 WSL，不要把 Minikube 内存降到 6 GiB 以下后继续宣称环境一致。

## 8. 将镜像旁加载到 Minikube

```bash
minikube image load -p stage3-logs --daemon=true \
  distributed-log-platform/log-producer:d20fc7f

minikube image load -p stage3-logs --daemon=true \
  distributed-log-platform/log-processor:9a776ee

minikube image load -p stage3-logs --daemon=true \
  apache/kafka:4.3.1

minikube image load -p stage3-logs --daemon=true \
  docker.elastic.co/beats/filebeat-wolfi:9.4.4

minikube image load -p stage3-logs --daemon=true \
  elasticsearch:9.4.4

minikube image load -p stage3-logs --daemon=true \
  grafana/grafana:13.1.0
```

执行部署前镜像门禁：

```bash
cd ~/projects/distributed-log-platform
make k8s-kafka-image-check
make k8s-elasticsearch-image-check
make k8s-filebeat-image-check
make k8s-grafana-image-check
make k8s-processor-image-check
```

全部通过后才能继续。如果只出现节点 manifest 摘要不匹配，不要直接修改 Makefile
或跳过门禁；先确认新电脑使用 x86-64、Minikube v1.38.1、Kubernetes v1.35.1 和
containerd 2.2.1，然后重新旁加载。配置摘要也不一致通常表示导入了错误镜像。

## 9. 从空集群部署完整平台

部署顺序不能随意改变：Kafka 和主题先就绪，随后部署 Elasticsearch 和模板，最后
启动处理器、日志源、Filebeat 和 Grafana。

```bash
cd ~/projects/distributed-log-platform

make k8s-kafka-deploy
make k8s-kafka-topics

make k8s-elasticsearch-deploy
make k8s-elasticsearch-template

make k8s-deploy
make k8s-filebeat-deploy
make k8s-grafana-deploy
```

这些入口会执行服务端 dry-run、等待 rollout，并在适用位置核对运行时镜像身份。
首次启动 Elasticsearch 可能需要几分钟。

## 10. 部署后检查

```bash
make k8s-kafka-status
make k8s-kafka-topics-status
make k8s-elasticsearch-status
make k8s-status
make k8s-filebeat-status
make k8s-grafana-status

kubectl --context=stage3-logs get pods -n stage3-logs
kubectl --context=stage3-logs get pods -n stage3-collector
```

预期持续组件为 `Running` 和 Ready，主题初始化 Job 为 `Completed`。应看到：

```text
api-service
worker-service
kafka-0
elasticsearch-0
log-processor
grafana
filebeat（位于 stage3-collector）
```

历史或初始化 Job 显示 `Completed` 是正常状态，不需要把它们改成 `Running`。

## 11. 现场演示流程

下面是一套约 10～15 分钟的演示顺序。

### 11.1 展示两个日志源

```bash
kubectl --context=stage3-logs logs \
  -n stage3-logs \
  -l app.kubernetes.io/instance=api-service \
  --tail=3

kubectl --context=stage3-logs logs \
  -n stage3-logs \
  -l app.kubernetes.io/instance=worker-service \
  --tail=3
```

预期每行都是 JSON，包含 `@timestamp`、`event.sequence`、`log.level`、`message`、
`service.name` 和 `test_run_id`。

### 11.2 展示 Kafka 主题和消费者进度

```bash
kubectl --context=stage3-logs exec \
  -n stage3-logs kafka-0 -- \
  env KAFKA_GC_LOG_OPTS= KAFKA_HEAP_OPTS=-Xmx64m \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

kubectl --context=stage3-logs exec \
  -n stage3-logs kafka-0 -- \
  env KAFKA_GC_LOG_OPTS= KAFKA_HEAP_OPTS=-Xmx64m \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group stage3-log-processor-v1
```

预期主题包含 `logs.api-service`、`logs.worker-service`、`logs.unclassified` 和
`logs.dlq`。持续链路稳定后，活动分区的 `LAG` 应收敛到 0。

### 11.3 展示 Elasticsearch 最新文档

```bash
kubectl --context=stage3-logs exec \
  -n stage3-logs elasticsearch-0 \
  -c elasticsearch -- \
  curl -fsS \
  'http://localhost:9200/logs-stage3-*/_search?size=2&sort=%40timestamp%3Adesc&filter_path=hits.total,hits.hits._source.%40timestamp,hits.hits._source.service.name,hits.hits._source.log.level,hits.hits._source.message'
```

预期能看到 `api-service` 和 `worker-service` 的最新日志。这个结果证明当前数据已经
穿过 Filebeat、Kafka 和 Go `log-processor`，而不是只停留在应用标准输出。

### 11.4 打开 Grafana

单独打开一个终端并保持运行：

```bash
kubectl --context=stage3-logs port-forward \
  -n stage3-logs service/grafana 3000:3000
```

浏览器访问 [http://127.0.0.1:3000](http://127.0.0.1:3000)，然后进入：

```text
Dashboards → 阶段三日志平台 → 分布式日志检索
```

建议现场依次演示：

1. 时间范围选择“最近 15 分钟”或“最近 1 小时”；
2. 服务切换为 `api-service`、`worker-service`；
3. 关键词输入 `log event`；
4. 展开一条日志查看完整字段；
5. 展示日志级别分布和 ERROR/WARN 趋势面板。

持续日志源当前只生成 INFO，ERROR/WARN 图表可能为空。这不代表面板失效；下一节
使用固定的四级日志数据自动验证这些查询。

### 11.5 演示 Grafana 固定数据集和重建恢复

先在端口转发终端按 `Ctrl+C`，然后执行：

```bash
make k8s-grafana-acceptance \
  GRAFANA_ACCEPTANCE_RUN_ID="demo-$(date -u +%Y%m%dT%H%M%SZ)"
```

该入口会自动验证：

- 16 条完整文档；
- 两个服务各 8 条；
- DEBUG、INFO、WARN、ERROR 各 4 条；
- 服务、时间、关键词和无结果查询；
- 明细与聚合数量一致；
- 查询 p50/p95；
- Grafana 缩容到 0、删除临时 SQLite 后重新预置数据源和仪表盘；
- 临时索引删除并复核 404。

验收会替换 Grafana Pod，因此旧的端口转发中断是正常现象。需要继续浏览时重新执行
端口转发命令。

### 11.6 可选：演示处理器幂等和恢复

```bash
make k8s-processor-acceptance \
  PROCESSOR_ACCEPTANCE_RUN_ID="demo-processor-$(date -u +%Y%m%dT%H%M%SZ)"
```

该入口会验证相同事件重复投递后 Elasticsearch 仍只有一个稳定 ID 文档，并在
`log-processor` 停机窗口写入恢复事件，证明新 Pod 可以继续消费且最终 LAG=0。

Grafana 和处理器验收会修改 Pod 或副本数，必须串行执行，不要在两个终端同时运行。

## 12. 常见问题

### 12.1 `go: command not found` 或版本不匹配

```bash
command -v go
go version
```

确保 PATH 中能找到 Go 1.26.5。不要通过修改 Makefile 放宽版本来掩盖环境问题。

### 12.2 Minikube 启动后节点不是 Ready

```bash
minikube status -p stage3-logs
minikube logs -p stage3-logs --problems
docker inspect stage3-logs --format '{{.State.Status}} {{.HostConfig.Memory}}'
```

常见原因是 Docker Desktop 未启动、WSL 集成未开启、内存不足或虚拟化未启用。

### 12.3 Pod 为 `ImagePullBackOff` 或 `ErrImageNeverPull`

```bash
minikube image ls -p stage3-logs | grep -E \
  'log-producer|log-processor|kafka|filebeat|elasticsearch|grafana'

kubectl --context=stage3-logs describe pod \
  -n stage3-logs <POD名称>
```

重新执行对应的 `minikube image load --daemon=true`。本地 overlay 使用
`imagePullPolicy: Never`，因此节点里没有镜像时 Kubernetes 不会联网补救。

### 12.4 Pod 不就绪或反复重启

```bash
kubectl --context=stage3-logs get events \
  -n stage3-logs \
  --sort-by=.lastTimestamp

kubectl --context=stage3-logs logs \
  -n stage3-logs deployment/log-processor \
  --tail=100

kubectl --context=stage3-logs logs \
  -n stage3-collector \
  -l app.kubernetes.io/name=filebeat \
  --tail=100
```

先处理最早的依赖错误。Kafka、主题或 Elasticsearch 未就绪时，不要反复重启处理器。

### 12.5 Grafana 页面打不开

确认端口转发终端仍在运行。如果本机 3000 端口被占用：

```bash
kubectl --context=stage3-logs port-forward \
  -n stage3-logs service/grafana 3001:3000
```

然后访问 [http://127.0.0.1:3001](http://127.0.0.1:3001)。

### 12.6 Grafana 没有最新日志

先把时间范围扩大到最近 1 小时并刷新，然后检查：

```bash
kubectl --context=stage3-logs get pods -n stage3-logs
kubectl --context=stage3-logs get pods -n stage3-collector
kubectl --context=stage3-logs logs \
  -n stage3-logs deployment/log-processor \
  --tail=100
```

如果 Elasticsearch 只读查询能看到最新文档而 Grafana 为空，重点检查 Grafana
数据源健康和筛选变量；如果 Elasticsearch 也没有最新文档，则从日志源、Filebeat、
Kafka、处理器的顺序向后排查。

## 13. 演示结束与下次恢复

端口转发使用 `Ctrl+C` 结束。需要释放资源但保留集群和 PVC：

```bash
minikube stop -p stage3-logs
```

下次演示：

```bash
minikube start -p stage3-logs
kubectl --context=stage3-logs wait \
  --for=condition=Ready node/stage3-logs \
  --timeout=120s
kubectl --context=stage3-logs get pods -n stage3-logs
kubectl --context=stage3-logs get pods -n stage3-collector
```

不要使用 `minikube delete -p stage3-logs` 作为日常关闭命令；删除配置档会同时失去
本地集群状态和 PVC 数据，需要重新执行完整部署流程。

## 14. 演示前最终清单

- [ ] 新电脑为 x86-64，WSL2、Docker、Go、kubectl、Minikube、Make、Python 可用；
- [ ] `make check` 通过；
- [ ] 镜像包校验通过，六个镜像已经导入 Docker 和 Minikube；
- [ ] `stage3-logs` 节点 Ready，4 CPU/6 GiB 门禁满足；
- [ ] Kafka、四个主题、Elasticsearch、索引模板、应用、Filebeat、处理器和 Grafana
      均已部署；
- [ ] 两个日志源能输出 JSON，Elasticsearch 能查到最新文档；
- [ ] Grafana 端口转发和仪表盘可以打开；
- [ ] 固定数据集验收命令已经单独试跑；
- [ ] 演示时不并行执行两个会替换 Pod 的验收入口；
- [ ] 演示结束使用 `minikube stop`，不误删配置档。
