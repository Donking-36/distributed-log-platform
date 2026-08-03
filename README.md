# 分布式日志平台

阶段三学习项目：在本地 WSL2 Minikube 集群上构建一条小型、可观测、
经过测试且可复现的日志处理链路。

仓库：[Donking-36/distributed-log-platform](https://github.com/Donking-36/distributed-log-platform)

## 核心链路

```text
Pod 标准输出
  → Filebeat DaemonSet
  → Kafka logs.<service>
  → Go log-processor
  → Elasticsearch logs-stage3-*
  → Grafana
```

`log-processor` 是规划中唯一的 Kafka 到 Elasticsearch 业务处理路径。链路采用
至少一次投递模型；`log-processor` 将使用确定性事件 ID，实现 Elasticsearch
写入幂等。

## 范围

`v0.1.0` 必须完成：

- UC-001：采集、路由、处理并存储 Kubernetes Pod 日志。
- UC-002：按时间、服务和关键词查询，并通过 Grafana 聚合和下钻。

Prometheus、HPA 和告警延后到 UC-001/UC-002 全部通过之后。
UC-004、生产级多节点高可用、多租户、自研网页界面和重复的 Go 查询接口
不在本轮范围内。

## 已验证的本地基线

- WSL 发行版：Ubuntu 24.04
- Minikube 配置档：`stage3-logs`
- Kubernetes：v1.35.1，使用 containerd 2.2.1
- 外层资源限制：4 CPU、6 GiB 内存
- Go 工具链和项目基线：1.26.5

Kafka 已选定 Apache 官方 JVM 镜像 4.3.1，并通过宿主 Docker 与 Kubernetes
单节点验证。Filebeat、Elasticsearch 和 Grafana 的镜像版本仍须通过各自兼容性
冒烟后选定。任何部署清单都不得使用 `latest`。

## 当前可运行组件

`log-producer` 是应用日志来源，只向标准输出写入一行一个 JSON 对象，不直接
连接 Kafka 或 Elasticsearch。Filebeat 后续负责采集这些日志并生产到 Kafka。

| 环境变量 | 必填 | 默认值 | 作用 |
|---|---|---|---|
| `PRODUCER_SERVICE_NAME` | 是 | 无 | Kubernetes 中由 Pod `service` 标签通过 Downward API 注入，写入原始 `service.name` |
| `PRODUCER_TEST_RUN_ID` | 是 | 无 | 标识一次可重复验收批次 |
| `PRODUCER_COUNT` | 否 | `20` | 正数表示固定批次的事件数；`0` 表示持续发送直到收到退出信号；负数非法 |
| `PRODUCER_INTERVAL` | 否 | `1s` | 相邻事件的固定间隔，使用 Go duration 且必须大于零；第一条立即输出 |

本地生成两条日志：

```bash
PRODUCER_SERVICE_NAME=api-service \
PRODUCER_TEST_RUN_ID=local-001 \
PRODUCER_COUNT=2 \
PRODUCER_INTERVAL=10ms \
go run ./cmd/log-producer
```

## 容器镜像

根目录 Dockerfile 使用 Go 1.26.5 多阶段构建，只把静态二进制复制到固定摘要的
Alpine 3.23 运行镜像。容器以 UID/GID 10001 运行，并显式使用 SIGTERM 作为
停止信号。

`make image` 只接受默认的开发标签 `dev`，或干净工作区的当前提交短 SHA；
因此不会生成 `latest` 或来源不明的任意标签。同一提交标签一旦用于部署或验收，
按流程约定不再覆盖：

```bash
IMAGE_TAG="$(git rev-parse --short HEAD)"
make image IMAGE_TAG="$IMAGE_TAG"

docker run --rm \
  -e PRODUCER_SERVICE_NAME=api-service \
  -e PRODUCER_TEST_RUN_ID=container-local-001 \
  -e PRODUCER_COUNT=2 \
  -e PRODUCER_INTERVAL=10ms \
  "distributed-log-platform/log-producer:$IMAGE_TAG"
```

`make image` 是显式的镜像构建入口，不属于默认 `make check`。基础持续集成只
运行无需 Docker 的快速门禁；容器构建和运行验收在相关功能分支中单独执行。

## Kubernetes 本地部署

`deploy/kubernetes/base/namespace` 统一保存 `stage3-logs` 命名空间和 Restricted
策略；`base/log-producer` 保存两个持续 Deployment，
`base/log-producer-acceptance` 保存两个固定批次 Job，`base/kafka` 保存 Kafka
Service、StatefulSet 和运行配置。持续应用、验收 Job 与有状态 Kafka 分别使用
`overlays/local`、`overlays/local-acceptance`、`overlays/local-kafka`，共享基础定义
但独立部署，避免应用更新隐式改动 Broker 或 PVC。

本地 overlay 固定使用已经验证的应用代码提交 `d20fc7f`。首次部署前，先确认
本地 Docker 中存在该标签并将其旁加载到 Minikube：

```bash
docker image inspect distributed-log-platform/log-producer:d20fc7f
minikube image load \
  -p stage3-logs \
  distributed-log-platform/log-producer:d20fc7f

make k8s-render
make k8s-validate
make k8s-deploy
make k8s-status
```

如果本地不存在该镜像，应从 Git 提交 `d20fc7f` 构建，不能把其他工作区内容
冒充为这个标签。`k8s-render` 只输出最终 YAML；`k8s-validate` 使用 API Server
做服务端 dry-run；`k8s-deploy` 在确认当前上下文为 `stage3-logs` 后才应用。

### Kafka 单节点基线

Kafka base 固定官方多架构索引摘要；本地 Minikube 使用按官方 amd64 摘要拉取、
再旁加载的 `4.3.1` 标签和 `imagePullPolicy: Never`。Docker daemon 导入 containerd
时会转换 manifest media type，因此 local overlay 同时记录上游索引、上游 amd64、
镜像 config 和本地导入 manifest 四个摘要。标签和注解只用于引用与留证，部署
门禁还会读取节点内的实际 manifest/config 摘要，并在滚动后核对初始化容器与
主容器的 imageID：

```bash
docker pull \
  apache/kafka@sha256:ccd1314e47ec76909e01f86308b4dcf2064f19f7c89759234322314b0e319e26
docker tag \
  apache/kafka@sha256:ccd1314e47ec76909e01f86308b4dcf2064f19f7c89759234322314b0e319e26 \
  apache/kafka:4.3.1
minikube image load \
  -p stage3-logs \
  --daemon=true \
  apache/kafka:4.3.1

make k8s-kafka-render
make k8s-kafka-validate
make k8s-kafka-image-check
make k8s-kafka-deploy
make k8s-kafka-status
```

`k8s-kafka-deploy` 会自动重复部署前镜像检查，并在 rollout 后执行双容器运行时
imageID 检查；任一摘要不匹配都会在写入或完成声明前失败。

该入口部署一个 combined KRaft 节点、普通客户端 Service、Headless Service 和
2 GiB PVC。Broker 请求 250m CPU/768 MiB，限制为 1 CPU/1536 MiB，JVM 堆为
512 MiB；初始化容器先把镜像自带配置复制到可写配置卷，主容器再生成最终配置。
两者均以 UID/GID 1000 运行并保持只读根文件系统。自动建主题已关闭，四个项目
主题将在下一步由独立的幂等初始化流程创建。当前形态使用明文监听器且没有高可用，
只用于本地开发。

查看 KRaft 状态：

```bash
kubectl --context=stage3-logs exec -n stage3-logs kafka-0 -- \
  env KAFKA_GC_LOG_OPTS= KAFKA_HEAP_OPTS=-Xmx64m \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

查看两个真实日志源：

```bash
kubectl --context=stage3-logs logs \
  -n stage3-logs \
  -l app.kubernetes.io/instance=api-service \
  --tail=5

kubectl --context=stage3-logs logs \
  -n stage3-logs \
  -l app.kubernetes.io/instance=worker-service \
  --tail=5
```

持续 Deployment 不承担“每服务精确 20 条”的验收；执行下列独立入口：

```bash
make k8s-acceptance-render
make k8s-acceptance
```

`k8s-acceptance` 默认生成形如 `uc001a-<UUID>` 的唯一 `test_run_id`，也可通过
`ACCEPTANCE_RUN_ID=<新值>` 显式指定。脚本先核对清单只能包含目标命名空间中的
两个预期 Job，再用临时名称完成服务端 dry-run；它拒绝删除仍在运行的同名 Job，
只精确替换两个已终止 Job。两个 Job 均使用 `PRODUCER_COUNT=20`、
`backoffLimit=0` 和 `restartPolicy=Never`。

验收入口会确认以下条件，其中 Python 标准库校验器负责逐行 JSON 契约：

- Job 成功且容器没有重启；
- 输出恰好 20 行合法 JSON；
- `service.name` 与权威 Pod `service` 标签对应；
- 两个服务共享本次 `test_run_id`；
- `event.sequence` 严格为 `1..20`，消息和级别符合日志源契约。

成功的 Job 会保留为现场证据，下一次执行时才精确替换。失败的 Job 同样保留
供 `kubectl describe` 和 `kubectl logs` 排查；该入口不修改持续 Deployment，
也不属于默认 `make check`。

## 项目文档

- [`docs/requirements.md`](docs/requirements.md)：UC-001/UC-002 的范围、日志契约和验收要求。
- [`docs/architecture.md`](docs/architecture.md)：组件职责、部署拓扑和兼容性门禁。
- [`docs/test-report.md`](docs/test-report.md)：已完成切片的真实命令、结果与边界。
- [`docs/adr/`](docs/adr/)：核心链路、投递语义和幂等策略等架构决策。

## 本地开发前检查

```bash
cd ~/projects/distributed-log-platform
git status --short --branch
git log -5 --oneline --decorate
kubectl config current-context
kubectl --context=stage3-logs get nodes
```

确认当前 Kubernetes 上下文为 `stage3-logs`，且节点处于就绪状态。若
Minikube 重建外层容器，应重新核验 4 CPU 和 6 GiB 内存限制。

## 工程检查

```bash
make fmt-check
make vet
make test
make build
make check
make image IMAGE_TAG="$(git rev-parse --short HEAD)"
make k8s-render
make k8s-validate
make k8s-status
make k8s-kafka-render
make k8s-kafka-validate
make k8s-kafka-image-check
make k8s-kafka-runtime-check
make k8s-kafka-status
```

`make check` 聚合 Go 1.26.5 版本、格式、静态检查、测试和构建门禁，且不会
修改工作区。需要主动格式化代码时执行 `make fmt`。

## 开发流程

- 长期分支：`main`、`develop`。
- 功能开发从 `develop` 创建目标单一的 `feature/*` 分支，并使用约定式提交。
- 只为真实行为添加测试和命令目标，不建立永远成功的占位检查。
- 远端仓库为 `https://github.com/Donking-36/distributed-log-platform.git`。

## 当前状态

环境、容量门禁、需求、架构、ADR 和 Go 模块基线已经完成。`log-producer`
已经实现固定批次与持续模式、确定性 JSON 输出、固定发送间隔和可取消等待，
并通过单元测试、静态检查、构建、本地运行和容器冒烟。两个非 root
`log-producer` Deployment 已通过 Kustomize 部署到 `stage3-logs`；两个独立
验收 Job 已验证各输出 20 条连续 JSON。Downward API 身份、安全上下文、资源
限制和真实日志均已验证。Kafka 4.3.1 官方 JVM 镜像已经固定摘要，并通过宿主
Docker 单节点主题与生产/消费冒烟；Kubernetes 中的 Service、StatefulSet、
Restricted 安全上下文、资源边界、2 GiB PVC 和 KRaft DNS 也已验证。同一 PVC
上的 Pod 重建保留了 KRaft 元数据；主题和消息恢复尚未验证。Kubernetes 主题
初始化与 Filebeat 尚未部署。
