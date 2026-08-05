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

`log-processor` 是唯一的 Kafka 到 Elasticsearch 业务处理路径。链路采用
至少一次投递模型；`log-processor` 使用确定性事件 ID，实现 Elasticsearch
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
9.4.4；Elasticsearch 已选定 Elastic Team 维护的 Docker Official Image 9.4.4；
Grafana 已选定官方 OSS 镜像 13.1.0。四者均已通过固定摘要和 Minikube 运行时
验证，Kafka/Filebeat 还完成了真实消息链路验证，Elasticsearch 完成了模板与
Bulk 幂等冒烟，Grafana 完成了数据源、仪表盘和三类面板查询冒烟。任何部署清单
都不得使用 `latest`。

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

`log-processor` 已提供可编译的持续运行入口。它严格串行执行
`Poll → 有界投递 → 必要时写入 DLQ → 源位点提交`，当前记录未得到提交确认时
不会拉取下一条。启动配置如下：

| 环境变量 | 必填 | 默认值 | 作用 |
|---|---|---|---|
| `PROCESSOR_KAFKA_BROKERS` | 是 | 无 | 逗号分隔的 Kafka `host:port` 地址 |
| `PROCESSOR_KAFKA_GROUP_ID` | 是 | 无 | 消费者组 ID |
| `PROCESSOR_KAFKA_TOPICS` | 是 | 无 | 逗号分隔的源主题；禁止包含 `logs.dlq` |
| `PROCESSOR_ELASTICSEARCH_ENDPOINT` | 是 | 无 | Elasticsearch HTTP(S) 地址 |
| `PROCESSOR_ELASTICSEARCH_INDEX` | 是 | 无 | 写入索引名 |
| `PROCESSOR_WRITE_TIMEOUT` | 否 | `10s` | 单次 Elasticsearch 写入超时 |
| `PROCESSOR_COMMIT_TIMEOUT` | 否 | `10s` | Kafka 源位点提交超时 |
| `PROCESSOR_DLQ_PUBLISH_TIMEOUT` | 否 | `10s` | DLQ 发布确认超时 |
| `PROCESSOR_HEALTH_ADDRESS` | 否 | `:8080` | `/healthz`、`/readyz` 的监听地址，端口必须为 1～65535 的数字 |

处理器运行时提供两个标准库 HTTP 探针：处理循环正常运行时两者均返回 200；
优雅退出期间 `/readyz` 立即返回 503，而 `/healthz` 在 HTTP 排空完成前保持 200；
启动未完成或任一运行单元异常退出时两者均为 503。探针请求不直接访问 Kafka 或
Elasticsearch；依赖故障由 Runner 的运行结果驱动应用退出，避免探针制造额外流量。

构建入口会同时链接两个程序，但不会在仓库中留下二进制：

```bash
make build
```

真实 Kafka 4.3.1→Elasticsearch 9.4.4/DLQ 小载荷联动验收已经通过：有效记录写入
Elasticsearch，永久无效记录写入 `logs.dlq` 并确认源位点，后续有效记录仍会继续
处理。处理器健康接口、独立容器镜像和 Kubernetes 持续 Deployment 也已通过；
重复投递幂等、SIGTERM 就绪撤销和 Pod 重建续读也已通过固化验收入口。

## 容器镜像

根目录 Dockerfile 使用 Go 1.26.5 多阶段构建，分别生成 `log-producer` 和
`log-processor` 静态二进制，再复制到共用的固定摘要 Alpine 3.23 运行基线。
两个最终镜像只包含各自的程序，均以 UID/GID 10001 运行，并显式使用 SIGTERM
作为停止信号。

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

处理器使用独立入口并复用相同标签门禁：

```bash
make processor-image IMAGE_TAG="$IMAGE_TAG"
```

`make image` 和 `make processor-image` 都是显式镜像构建入口，不属于默认
`make check`。基础持续集成只运行无需 Docker 的快速门禁；容器构建和运行验收
在相关功能分支中单独执行。

## Kubernetes 本地部署

`deploy/kubernetes/base/namespace` 统一保存 `stage3-logs` 命名空间和 Restricted
策略；`base/log-producer` 保存两个持续 Deployment，
`base/log-processor` 保存 Kafka→Elasticsearch 持续处理 Deployment 和非敏感配置，
`base/log-producer-acceptance` 保存两个固定批次 Job，`base/kafka` 保存 Kafka
Service、StatefulSet 和运行配置，`base/kafka-topics` 保存一次性主题初始化 Job；
`base/filebeat` 与 `base/filebeat-metadata-access` 分别保存采集器和目标命名空间
Pod-only RBAC；`base/elasticsearch` 保存单节点 StatefulSet、Service、PVC 契约
和索引模板；`base/grafana` 保存日志数据源、仪表盘、Deployment 和 ClusterIP
Service。应用及第三方组件的镜像身份由固定摘要或本地旁加载门禁约束。
持续应用、验收 Job、有状态 Kafka 与主题初始化分别使用 `overlays/local`、
`overlays/local-acceptance`、`overlays/local-kafka`、`overlays/local-kafka-topics`、
`overlays/local-filebeat`、`overlays/local-elasticsearch`、
`overlays/local-grafana`，共享基础定义但独立运行，避免应用、采集、主题或搜索
存储操作隐式改动其他组件、Namespace 或 PVC。

本地 overlay 的 producer 固定为 `d20fc7f`，processor 固定为 `9a776ee`。首次
部署前，先确认两个镜像存在并旁加载到 Minikube：

```bash
docker image inspect distributed-log-platform/log-producer:d20fc7f
docker image inspect distributed-log-platform/log-processor:9a776ee

minikube image load \
  -p stage3-logs \
  distributed-log-platform/log-producer:d20fc7f
minikube image load \
  -p stage3-logs \
  distributed-log-platform/log-processor:9a776ee

make k8s-render
make k8s-validate
make k8s-processor-image-check
make k8s-deploy
make k8s-status
```

镜像必须从对应干净 Git 提交构建，不能用其他工作区内容冒充固定标签。
`k8s-render` 只输出最终 YAML；`k8s-validate` 使用 API Server 做服务端 dry-run。
`k8s-deploy` 会先核对 processor 标签在节点内实际对应的 manifest/config 摘要，
应用后等待 Deployment Ready，再核对 Pod 运行时 config 摘要。

processor 使用单副本 `Recreate`、30 秒终止宽限、startup/readiness/liveness HTTP
探针、100m/128Mi 请求和 500m/256Mi 上限，并以 UID/GID 10001、只读根文件系统
运行。当前本地 Kafka 和 Elasticsearch 无认证，连接参数没有敏感值，因此只生成
ConfigMap，不创建空壳 Secret。

部署稳定后可执行 UC-001B 的幂等与恢复验收：

```bash
make k8s-processor-acceptance \
  PROCESSOR_ACCEPTANCE_RUN_ID=uc001b-local-001
```

该入口先取得共享验收锁并确认 overlay 无漂移，然后向固定业务主题写入两条完全
相同的事件，验证 Elasticsearch 中 `_id` 与 `event_id` 相同且唯一。随后将 processor
缩到 0，确认旧 Pod 在 SIGTERM 期间 Ready=False，在停机窗口写入恢复事件，再恢复
单副本并验证新 Pod UID、运行时镜像、消费者组 LAG=0 和恢复文档。退出时会接管并
恢复 Deployment 副本数，最后复核 UID、owner、overlay 和唯一 Pod；不创建临时
Kubernetes 资源。两条带唯一 `test_run_id` 的 ES 文档作为证据保留，Kafka fixture
由主题 24 小时保留策略清理。

### Grafana 日志检索

Grafana 13.1.0 直接查询 Elasticsearch，不经过重复的 Go 查询接口。数据源固定
使用 `logs-stage3-*` 和 `@timestamp`；预置仪表盘包含日志明细、日志级别分布、
ERROR/WARN 趋势、服务变量、关键词变量和时间范围。日志行可展开查看完整字段。

本地节点无法稳定直接访问 Docker Hub 时，先由宿主拉取并旁加载固定标签；部署
入口会在写入前核对节点 manifest/config，完成后再核对 Pod imageID：

```bash
docker pull docker.io/grafana/grafana:13.1.0
minikube image load \
  -p stage3-logs \
  docker.io/grafana/grafana:13.1.0

make k8s-grafana-render
make k8s-grafana-validate
make k8s-grafana-image-check
make k8s-grafana-deploy
make k8s-grafana-status
```

Service 仅为集群内 `ClusterIP`。需要在本机浏览时临时转发端口：

```bash
kubectl --context=stage3-logs port-forward \
  -n stage3-logs service/grafana 3000:3000
```

然后访问 `http://127.0.0.1:3000`。当前本地演示关闭 Basic 登录并启用匿名只读；
它不代表生产认证方案。Grafana 使用临时 SQLite，不保存人工界面修改，数据源和
仪表盘必须由仓库文件恢复。

部署稳定后可运行固定数据集验收：

```bash
make k8s-grafana-acceptance \
  GRAFANA_ACCEPTANCE_RUN_ID=uc002-local-001
```

该入口使用共享工作流锁，在临时 `logs-stage3-*` 索引写入 16 条完整文档，验证
两个服务、四个级别、两个时间窗口、固定错误关键词、无结果、明细与聚合数量
一致，并重复测量一次三查询请求的 p50/p95。随后把 Grafana 缩到 0 删除 Pod 和
临时 SQLite，再恢复单副本，验证新 Pod UID、镜像身份、数据源和仪表盘自动恢复。
退出时删除临时索引并复核 404，Deployment 声明与 overlay 必须无漂移。

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

- [`docs/DEMO_GUIDE.md`](docs/DEMO_GUIDE.md)：在另一台 Windows/WSL2 电脑上迁移镜像、从空集群部署并完成现场演示。
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
字段类型、1 分片/0 副本以及 `dynamic: strict` 已生效。`internal/elasticsearch`
现已使用官方 Go v9.4.2 客户端完成不可变的 14 字段文档转换、Bulk `create` 编码
和逐项结果分类；真实 9.4.4 冒烟首次返回 `201/created`，同一 `_id` 重复写入返回
`409/duplicate`，索引内仍只有 1 个文档，临时索引已删除。该证据已经覆盖
Go→Elasticsearch 写入边界。

`internal/kafka` 已固定 `franz-go v1.21.5`，建立一次只拉取一条记录的最小消费
边界：自动提交关闭，调用方只有显式确认当前记录后才能拉取下一条；提交失败会
保留当前记录，关闭消费者不会隐式推进位点。真实 Kafka 4.3.1 验收证明同一
消费者组关闭前未确认的位点会被再次读取，显式确认后重启则从下一位点继续。

`internal/pipeline` 已建立单记录编排。`Processor` 负责一次处理：解析 Kafka value、
构造不可变文档、执行一次 Elasticsearch Bulk `create`，并且只在逐项结果为
`created` 或 `duplicate` 时提交原始 Kafka 记录。Elasticsearch 写入与 Kafka
提交使用独立超时；永久无效、可重试、系统故障和取消均不提交，ES 已接受但
Kafka 提交失败则返回独立的 `commit_failure`，不会误报整条记录成功。

`DeliveryCycle` 在此基础上按 ADR-002 对同一原始记录执行最多六次完整尝试，
只重试 `retryable_failure` 和 `commit_failure`。五次等待上限固定为 250 ms、
500 ms、1 s、2 s、4 s，并使用全抖动；父级上下文可中断等待且优先终止周期。
提交响应丢失时会重放完整处理，依靠稳定 `_id` 从 `created` 收敛为
`duplicate` 后再次提交。聚焦竞态测试语句覆盖率为 91.4%。

真实 Kafka 4.3.1 的隔离集成测试还证明，`Consumer.Poll` 返回的原始记录能够穿过
pipeline 并由同一 Consumer 提交，Broker 侧下一位点为 1。该测试仍使用受控
Elasticsearch writer，只负责锁定不可伪造的原始记录身份。

永久无效记录的 DLQ 单元边界也已建立：独立 Kafka 生产者固定同步写入
`logs.dlq`，`DeadLetterHandler` 只有在获得 Broker 发布确认后才提交原始源记录。
DLQ 记录使用稳定的源坐标 key，原始载荷以 Base64 无损保存；错误摘要只包含
受控字段名和固定文案，不复制原始值。该边界已覆盖发布失败、取消和源提交失败，
并已在下述真实联动验收中接入 Kafka。

`Runner` 已把 Poll、有界投递和 DLQ 接成严格串行循环：正常投递只有源位点已提交
才继续；只有永久无效记录进入 `DeadLetterHandler`，且 DLQ 发布和源位点提交均
确认后才继续；任何其他未解决错误都会在下一次 Poll 前停止。`cmd/log-processor`
已完成环境校验、真实依赖组装、SIGINT/SIGTERM 正常停止和 10 秒有界资源关闭。

`make kafka-consumer-integration` 现在同时复现消费续读、pipeline token 和真实
Runner 联动：唯一源主题按“有效→永久无效→有效”写入三条受控记录，最终断言
Runner 组提交点为 3、Elasticsearch 只有两条确定性 ID 文档、`logs.dlq` 恰好新增
一条且 envelope/Base64 原文正确。临时主题、三个消费者组、临时 ES 索引和测试
二进制都会删除并复核；共享 `logs.dlq` 的验收记录不被危险截断，由 24 小时保留
策略清理。健康接口已完成进程内状态和协同退出验证；处理器镜像、Kubernetes
部署、重复投递幂等和 Pod 重启恢复仍待实现。
