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

Kafka 已选定 Apache 官方 JVM 镜像 4.3.1；Filebeat 已选定官方 Wolfi 镜像
9.4.4；Elasticsearch 已选定 Elastic Team 维护的 Docker Official Image 9.4.4。
三者均已通过固定摘要和 Minikube 运行时验证，Kafka/Filebeat 还完成了真实消息
链路验证，Elasticsearch 完成了模板与 Bulk 幂等冒烟。Grafana 镜像仍须通过兼容性
冒烟后选定。任何部署清单都不得使用 `latest`。

## 当前可运行组件

`log-producer` 是应用日志来源，只向标准输出写入一行一个 JSON 对象，不直接
连接 Kafka 或 Elasticsearch。Filebeat DaemonSet 已负责采集这些日志并生产到
Kafka；业务 JSON 在 Kafka 外层事件的 `message` 中保持原样。

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
Service、StatefulSet 和运行配置，`base/kafka-topics` 保存一次性主题初始化 Job；
`base/filebeat` 与 `base/filebeat-metadata-access` 分别保存采集器和目标命名空间
Pod-only RBAC；`base/elasticsearch` 保存单节点 StatefulSet、Service、PVC 契约
和索引模板，`components/elasticsearch-image` 单独固定镜像摘要。
持续应用、验收 Job、有状态 Kafka 与主题初始化分别使用 `overlays/local`、
`overlays/local-acceptance`、`overlays/local-kafka`、`overlays/local-kafka-topics`、
`overlays/local-filebeat`、`overlays/local-elasticsearch`，共享基础定义但独立
运行，避免应用、采集、主题或搜索存储操作隐式改动其他组件、Namespace 或 PVC。

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
主题由下方独立的一次性流程创建。当前形态使用明文监听器且没有高可用，只用于
本地开发。

查看 KRaft 状态：

```bash
kubectl --context=stage3-logs exec -n stage3-logs kafka-0 -- \
  env KAFKA_GC_LOG_OPTS= KAFKA_HEAP_OPTS=-Xmx64m \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

### Kafka 项目主题初始化

主题初始化 overlay 只渲染一个带内容哈希的脚本 ConfigMap 和一个固定名 Job，
不包含 Kafka StatefulSet、Service、Namespace 或 PVC。Job 通过集群内地址
`kafka:9092` 管理以下主题：

| 主题 | 分区 | 副本 |
|---|---:|---:|
| `logs.api-service` | 3 | 1 |
| `logs.worker-service` | 3 | 1 |
| `logs.unclassified` | 1 | 1 |
| `logs.dlq` | 1 | 1 |

每个主题都显式设置 `cleanup.policy=delete`、24 小时保留、每分区 128 MiB、
64 MiB segment 和 `min.insync.replicas=1`。`retention.bytes` 是每分区限制，不能
把它解释为整个主题的总容量。

一次性 Job 请求 50m CPU/128 MiB，限制 500m CPU/384 MiB，CLI 堆为
32～128 MiB。Job 自身 180 秒到期且不重试，宿主 runner 默认等待 300 秒，以便
Kubernetes 先形成明确失败状态，再收集 Pod、事件和日志。

```bash
make k8s-kafka-topics-render
make k8s-kafka-topics-validate
make k8s-kafka-topics
make k8s-kafka-topics-status
```

`k8s-kafka-topics-validate` 检查当前上下文、Broker `1/1`、客户端 Service、严格的
“1 ConfigMap + 1 Job”资源边界、单容器镜像/拉取策略/摘要注解，再做服务端
dry-run，全程不写集群；
`k8s-kafka-topics` 先复用节点镜像摘要门禁，再安全替换属于本流程且已经终止的
同名 Job。缺失主题会被创建；已有主题的分区、副本、ISR、重分配状态或上述显式
配置只要不一致就直接失败，不自动扩分区、重分配副本、修改保留期或删除主题。
显式配置使用 `kafka-configs` 的动态主题配置输出验证，不把 Broker 继承值误写成
主题级 override。完成的 Job 会保留，作为最近一次执行证据；相关解析正反例已
纳入默认 `make check`。

### Elasticsearch 单节点基线

Elasticsearch 使用 Elastic Team 维护的 Docker Official Image 9.4.4。base 固定
多架构索引摘要，本地 overlay 使用已经旁加载的 linux/amd64 标签并保存上游索引、
上游 amd64、节点 manifest 和 config 四层证据：

```bash
docker pull \
  docker.io/library/elasticsearch@sha256:c060ba28f5cfea4eedd8fb85bd5f6bf7d120e53040ee038a289c28979af7128c
docker tag \
  docker.io/library/elasticsearch@sha256:c060ba28f5cfea4eedd8fb85bd5f6bf7d120e53040ee038a289c28979af7128c \
  docker.io/library/elasticsearch:9.4.4
minikube image load \
  -p stage3-logs \
  docker.io/library/elasticsearch:9.4.4

make k8s-elasticsearch-render
make k8s-elasticsearch-validate
make k8s-elasticsearch-image-check
make k8s-elasticsearch-deploy
make k8s-elasticsearch-template
make k8s-elasticsearch-status
```

该入口部署 1 个 StatefulSet、普通 ClusterIP Service、Headless Service 和 5 GiB
Retain PVC，不创建 NodePort、LoadBalancer 或 Ingress。主容器请求 500m CPU/2 GiB，
限制 1500m CPU/2 GiB，JVM 堆固定为 1 GiB。初始化容器把镜像默认配置复制到可写
配置卷，主容器随后以 UID/GID 1000、Restricted 安全上下文和只读根文件系统运行。

`local-elasticsearch` 为避免特权 sysctl 初始化而禁用 mmap，并关闭 Elasticsearch
认证和 TLS；这只适用于隔离的本地 Minikube，不能用于对外环境。索引模板
`logs-stage3-v1` 匹配 `logs-stage3-*`，使用优先级 501 覆盖内置 `logs-*-*`
模板；字段严格遵循最小日志契约，1 分片、0 副本。

### Filebeat 节点采集

Filebeat 9.4.4 Wolfi 运行在独立的 `stage3-collector`。它只读挂载
`/var/log/containers`、`/var/log/pods`，并把 registry 写入项目专用宿主路径；
ServiceAccount 只能在 `stage3-logs` 对 Pod 执行 `get/list/watch`。节点日志实际
为 `root:root 0640`，因此容器以 UID 0 读取，但仍禁止提权、丢弃全部 capability、
使用只读根文件系统且不启用 privileged。

本地清单使用 `imagePullPolicy: Never`，配置门禁也使用 `--pull=never`；因此新环境
必须先按官方 linux/amd64 摘要准备并旁加载镜像：

```bash
docker pull \
  docker.elastic.co/beats/filebeat-wolfi@sha256:3d14aa62612275ffae45891e523e9b29f23eb647032809190eb60f6b4a549379
docker tag \
  docker.elastic.co/beats/filebeat-wolfi@sha256:3d14aa62612275ffae45891e523e9b29f23eb647032809190eb60f6b4a549379 \
  docker.elastic.co/beats/filebeat-wolfi:9.4.4
minikube image load \
  -p stage3-logs \
  --daemon=true \
  docker.elastic.co/beats/filebeat-wolfi:9.4.4
```

```bash
make filebeat-config-check
make k8s-filebeat-render
make k8s-filebeat-validate
make k8s-filebeat-deploy
make k8s-filebeat-status
```

Filebeat 9.4.4 的 Kafka 输出协议版本显式固定为 `4.1.0`；这是客户端兼容协议，
不是 Broker 镜像版本。`4.1.0` 配置正例已经连接 Kafka 4.3.1，故意配置为不受支持
的 `4.3.1` 时门禁按预期失败。

正常事件只有在 Pod `service` 标签与 Pod UID 都存在时才静态路由，Kafka key 为
Pod UID。当 `add_kubernetes_metadata` 未能补齐 Pod UID（例如旧 Pod 已从 API
消失或启动阶段缓存尚未命中）时，严格的日志路径允许列表仍保留来源边界；事件进入
`logs.unclassified`，`fields.routing_reason=kubernetes_metadata_missing`，key
改用 Filebeat 指纹 `sha256("|log.file.path|<path>|")` 的小写十六进制结果，避免
冷启动静默丢弃。

端到端入口先记录四个 Topic 的分区位点，再运行带同一唯一 `test_run_id` 的两个
固定批次 Job；校验器逐字比较源 stdout 与 Kafka `message`，并验证 Pod UID key、
Kubernetes 元数据、主题隔离和物理重复。兜底入口只在 Minikube 节点的唯一临时
路径注入一条 CRI 日志，完成后精确清理：

```bash
make k8s-filebeat-acceptance
make k8s-filebeat-fallback-acceptance
make k8s-filebeat-registry-recovery
make k8s-filebeat-kafka-outage-recovery
```

四个入口均已通过。`k8s-filebeat-registry-recovery` 是有写操作的恢复验收：它会
先核对 DaemonSet owner 和 registry 身份，再精确删除当前 Filebeat Pod，证明旧
批次不回放且独立新批次仍可到达。`k8s-filebeat-kafka-outage-recovery` 也是有写
操作的恢复验收：它在共享锁内把已核对身份的单 Broker StatefulSet 从 1 副本缩为
0，确认 Kafka 不可达后才生成 40 条唯一日志，再恢复为 1；退出陷阱会按原 UID
恢复副本数，并验证同一 StatefulSet、PVC、Cluster ID、主题位点和 Filebeat
实例。两个入口都不能当作只读状态检查。该结果只覆盖有界的本地单 Broker 短停，
不代表多节点故障转移、无限中断或磁盘队列能力。

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
make k8s-kafka-topics-render
make k8s-kafka-topics-validate
make k8s-kafka-topics-status
make k8s-elasticsearch-render
make k8s-elasticsearch-validate
make k8s-elasticsearch-image-check
make k8s-elasticsearch-runtime-check
make k8s-elasticsearch-status
```

`make check` 聚合 Go 1.26.5 版本、格式、静态检查、测试和构建门禁，并执行所有
Shell 脚本语法检查、Kafka 主题解析自测及 Filebeat Kafka 校验器十一项标准库测试；
它不会修改工作区。需要主动格式化代码时执行 `make fmt`。

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
上的 Pod 重建保留了 KRaft 元数据。独立主题初始化 Job 已显式创建四个项目主题，
双次运行保持 Topic ID 不变；在不重跑 Job 的前提下重建 Broker Pod 后，四个
Topic ID、拓扑和配置仍保持。Filebeat 9.4.4 Wolfi 已以独立 DaemonSet 部署，
镜像、Pod-only RBAC、精确 hostPath、registry、安全上下文和 Kafka 连接门禁均
通过。唯一批次的 api/worker 共 40 条逻辑事件已完成四主题有界位点对账，0 物理
重复、无跨主题路由；元数据缺失 fixture 也已用稳定路径指纹进入
`logs.unclassified`。Filebeat Pod 重建后，宿主 registry 与 Beat UUID 保持，旧
批次 0 回放且新批次 40/40 到达。受控地把单 Broker 从 1 副本缩为 0 后，在 Pod
缺席窗口产生的 40 条日志已于恢复后全部进入正确主题，0 条物理重复；StatefulSet
UID、PVC UID/PV 和 Cluster ID 保持，故障前位点连续可读并在恢复后继续推进。该
证据不外推为多节点高可用、任意长中断、队列容量或磁盘损坏恢复保证。

Elasticsearch 9.4.4 已以单节点 StatefulSet 部署，5 GiB PVC、ClusterIP、
Restricted 安全上下文、只读根、1 GiB 堆和镜像身份均已验证。索引模板的 14 个
字段类型、1 分片/0 副本以及 `dynamic: strict` 已生效；真实 Bulk `create` 首次
返回 201，同一 `_id` 重复创建返回单项 409，索引内仍只有 1 个文档。该证据只
证明 Elasticsearch 服务端契约，尚未证明 `log-processor` 的 Kafka 消费与端到端
写入。
