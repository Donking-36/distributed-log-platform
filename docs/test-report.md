# 测试报告

## UC-001A：日志源固定批次验收

- 日期：2026-07-31
- 集群：Minikube `stage3-logs`，Kubernetes v1.35.1
- 应用镜像：`distributed-log-platform/log-producer:d20fc7f`
- 最终批次：`uc001a-20260731-02`

### 验收范围

本次只验证两个 `log-producer` 一次性 Job 的生命周期、身份和标准输出契约，
以及它们不会干扰持续运行的 Deployment。Kafka 和 Filebeat 尚未部署，因此
本节不代表 UC-001A 全链路完成。

### 执行入口

```bash
make k8s-acceptance ACCEPTANCE_RUN_ID=uc001a-20260731-02
```

该入口依次完成：

1. 断言清单只包含目标命名空间中的两个预期 Job；
2. 使用临时名称执行服务端 dry-run；
3. 拒绝替换仍在运行或不属于本流程的同名 Job；
4. 精确替换两个旧终态 Job；
5. 等待两个新 Job 完成；
6. 验证单 Pod、零重启、相同镜像身份和逐行 JSON 契约。

### 结果

| 项目 | `api-service` | `worker-service` |
|---|---:|---:|
| Job 状态 | `Complete` | `Complete` |
| 成功/失败/活动 Pod | `1/0/0` | `1/0/0` |
| 容器退出 | `Completed`，退出码 0 | `Completed`，退出码 0 |
| 容器重启 | 0 | 0 |
| JSON 行数 | 20 | 20 |
| 序号 | `1..20` | `1..20` |
| `test_run_id` | `uc001a-20260731-02` | `uc001a-20260731-02` |
| 日志 SHA-256 | `ef877b78b501acce3761342d55b4fba1d7806258c85c34ffc0290d500e9da8a9` | `9e1827d597217500644786a67d9c70c7264d045173c9c9f7ba65b5b6830f6831` |

两个 Job 与两个持续 Deployment 的实际 imageID 均为：

```text
sha256:4ec3350c7875b1009eab2b0f2dad13c295afcd41615b7a3a691154d2e77f829b
```

日志首末事件分别为序号 1 和 20；`service.name` 与 Pod 的权威 `service`
标签一致，`log.level=INFO`，`message` 与序号对应。

验收前后，持续 Deployment 的 UID、generation、Pod UID、启动时间、重启数和
imageID 均未改变，副本保持 `Ready/Available=1/1`。持续日志序号仍在增长：

- `api-service`：`3305 -> 4073`
- `worker-service`：`3306 -> 4074`

目标命名空间中没有 Warning 事件，持续 overlay 的 `kubectl diff` 为空。

### 工程与反向门禁

以下检查均通过：

```bash
make check
go test -race -cover ./...
make k8s-validate
git diff --check
```

Go 包语句覆盖率为 `91.0%`。反向用例确认校验器会拒绝 19 行输出、重复序号和
非正数 `count`；运行脚本会拒绝非法批次 ID、持续 Deployment 的批次 ID，以及
底层 `kubectl` 失败。

### 未覆盖边界

- 该次日志源验收当时尚未部署 Kafka、Filebeat；后续 Kafka 结果见下方独立章节。
  主题路由和 Kubernetes 元数据补充仍未验证。
- Job stdout 的“精确 20 行”不等同于后续 Kafka 至少一次投递的物理消息数量；
  Kafka 阶段必须按唯一序号和 `test_run_id` 单独验收。
- 原始日志只用于本次校验，不提交仓库；报告保存可重复命令、摘要和校验和。

## Kafka 4.3.1：单节点兼容性冒烟

- 日期：2026-08-03
- Docker：29.6.2，linux/amd64
- Kafka：4.3.1，官方 JVM 镜像
- 多架构索引摘要：`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- linux/amd64 清单与本地 imageID：`sha256:ccd1314e47ec76909e01f86308b4dcf2064f19f7c89759234322314b0e319e26`

### 验收范围

本次在宿主 Docker 中验证官方 Kafka 镜像的单节点 combined KRaft 启动、主题
管理、带键消息生产和消费，以及当前资源包络。它不验证 Kubernetes 监听器、
持久化、TLS/SASL、Filebeat、故障恢复或高可用。

### 镜像与运行边界

镜像来自 Apache 官方下载页指向的 `apache/kafka:4.3.1`，实际按 amd64 清单
摘要拉取。容器使用非 root `appuser`，内置 OpenJDK 21.0.11。临时容器
`stage3-kafka-compat` 带项目用途标签，限制为 1 CPU、1.5 GiB 内存、相同
memory-swap 和 512 个 PID，不映射宿主端口、不配置自动重启。

Broker API 就绪检查成功。KRaft 状态显示：

```text
ClusterId: 5L6g3nShT-eMCtK--X86sw
LeaderId: 1
MaxFollowerLag: 0
CurrentVoters: [{"id": 1, "endpoints": ["CONTROLLER://localhost:9093"]}]
```

一次稳定资源快照为：

```text
CPU 1.23%
内存 543.8 MiB / 1.5 GiB（35.40%）
PID 103
```

该快照不是峰值，也不直接作为 Kubernetes 最终资源配置。

### 主题与消息结果

按架构基线成功创建并 describe：

| 主题 | 分区 | 复制因子 |
|---|---:|---:|
| `logs.api-service` | 3 | 1 |
| `logs.worker-service` | 3 | 1 |
| `logs.unclassified` | 1 | 1 |
| `logs.dlq` | 1 | 1 |

使用批次 `kafka-compat-20260803-01` 向两个业务主题各写入两条 JSON。分别从头
消费后，消息数量、`service.name`、共享 `test_run_id` 和 `event.sequence` `1..2`
均自动校验通过；分区位点复核随后发现首条键的编码异常。

第一次从 PowerShell 向 Linux CLI 传输消息时，输入流首行带 UTF-8 BOM，使
第一条键实际变成 `﻿pod-worker-uid`，两条看似同键的消息因此落入不同分区。
这不是 Kafka 分区器错误。修正验证改为在 WSL 内直接运行真实 `log-producer`，
使用当前 CLI 参数 `--reader-property` 与 `--formatter-property`，并通过
`--command-property` 显式设置 `acks=all` 和 `partitioner.ignore.keys=false`，
结果为：

```text
messages=3
key=pod-worker-uid
partition=0
values=identical
```

三条消息使用同一键、同一分区，消费值与 `log-producer` 原始输出逐字一致，
`event.sequence` 为 `1..3`。

### 关闭与清理

Broker 日志没有 ERROR/FATAL，`OOMKilled=false`。Docker 发送 SIGTERM 后容器
退出码为 143；日志明确显示 Broker、Controller、LogManager 和 RaftClient 均
完成 graceful shutdown，因此该退出码按信号终止语义记录，不误判为崩溃。

用途标签复核后，临时容器和镜像声明产生的三个匿名卷均已精确删除；没有执行
全局 prune，固定 Kafka 镜像继续保留在本机缓存中供后续 Kubernetes 使用。

### 结论与未覆盖边界

- Kafka 4.3.1 官方 JVM 镜像适用于本项目下一步的本地 Kubernetes 设计基线。
- 当前只证明单节点 KRaft 和容器内 Admin/Producer/Consumer 闭环可用。
- Filebeat 对 Kafka 4.3.1 的真实生产兼容性仍需在 Filebeat 版本选定后验证。
- 该次宿主 Docker 冒烟不覆盖 Kubernetes；后续结果见下一节。
- combined KRaft 是开发形态，不代表生产级控制器隔离或高可用。

## Kafka 4.3.1：Kubernetes 单节点基线

- 日期：2026-08-03
- Kubernetes：v1.35.1，containerd 2.2.1
- 命名空间：`stage3-logs`，Pod Security Restricted enforce
- 最终 StatefulSet revision：`kafka-6684fcbcb`

### 拓扑与镜像证据

Kafka 使用独立 `local-kafka` overlay。普通 `kafka` Service 提供 9092 客户端
入口；`kafka-headless` 发布 9092/9093 和未就绪地址，为 `kafka-0` 提供稳定
Broker/Controller DNS。StatefulSet 使用一个 combined KRaft 副本和 2 GiB
`ReadWriteOnce` PVC，删除与缩容策略均为 Retain。

base 固定官方多架构索引摘要，local overlay 使用已旁加载的 `apache/kafka:4.3.1`
和 `imagePullPolicy: Never`。从 Docker daemon 导入 containerd 时，manifest
media type 从 OCI 转为 Docker v2，本地 manifest 摘要因此变为：

```text
upstream-index=sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
upstream-amd64=sha256:ccd1314e47ec76909e01f86308b4dcf2064f19f7c89759234322314b0e319e26
local-import=sha256:f8f865a3222d807cf1e6c515ca447cb2fb604ddc57f0a007a02a9ce79bd7a511
config=sha256:47dccc76b32761bc57462b8753144cdbb73a16b123b1d13d3eedb92bb7952b11
```

PowerShell JSON 自动比较确认上游和本地 manifest 的 config digest、config 大小
以及 12 个 layer 的摘要和大小全部相同。`make k8s-kafka-image-check` 还会在部署
前读取节点 `ctr` 的标签目标 manifest 和 `crictl` 的 config 摘要；使用全零错误
manifest 的反例按预期退出 1。rollout 后同时检查初始化容器和主容器 imageID；
临时假 `kubectl` 返回“主容器正确、init 错误”时也按预期退出 1。最终两个
imageID 都等于上述 config digest，且均为 0 次重启。

### 生效配置与资源边界

Broker 最终生效配置包括：

```text
advertised.listeners=PLAINTEXT://kafka-0.kafka-headless.stage3-logs.svc.cluster.local:9092
controller.quorum.voters=1@kafka-0.kafka-headless.stage3-logs.svc.cluster.local:9093
process.roles=broker,controller
auto.create.topics.enable=false
log.dirs=/var/lib/kafka/data
log.retention.hours=24
log.retention.bytes=134217728
log.segment.bytes=67108864
```

内部 offsets、transaction 和 share coordinator 主题的默认创建配置为 3 个分区，
复制因子和最小 ISR 均按单节点开发边界设为 1；本轮没有触发这些内部主题创建。
工作负载请求 250m CPU/768 MiB，限制为 1 CPU/1536 MiB，JVM 堆为 512 MiB；
容器内证据为：

```text
cpu.max=100000 100000
memory.max=1610612736
memory.current=524591104
pids.current=119
```

镜像以 UID/GID 1000 运行，禁用 ServiceLinks 和 ServiceAccount token，使用
RuntimeDefault seccomp、禁止提权并丢弃全部 capabilities。初始化容器和主容器
均保持只读根文件系统；`/opt/kafka/config`、`/tmp` 和 `/var/lib/kafka/data`
三个显式挂载点满足官方入口所需写路径。

### 失败分支与恢复证据

首版 Pod 已成功格式化 PVC，但官方脚本随后尝试创建
`/opt/kafka/logs/kafkaServer-gc.log`，在只读根文件系统上进入 CrashLoop。没有
通过关闭只读根文件系统绕过；修复将 `LOG_DIR` 指向 `/tmp/kafka-logs`，并用
`KAFKA_GC_LOG_OPTS=-Xlog:gc:stdout:time,tags` 把 GC 事件交给容器 stdout。

完整启动日志随后暴露一条 `Reconfiguration failed`：空 `emptyDir` 遮住了镜像
自带的 `tools-log4j2.yaml`，而官方格式化 JVM 在生成最终配置前已经需要它。
修复增加 `prepare-config` 初始化容器，把镜像原始配置复制到共享配置卷并断言
`server.properties`、`log4j2.yaml`、`tools-log4j2.yaml` 非空。首次使用 `cp -a`
虽然复制出文件，但因 Restricted 容器不能保留卷根属性而退出 1；改为只复制
内容的 `cp -R` 后，初始化容器退出码 0、0 次重启，未放宽权限。

旧的未就绪 StatefulSet revision 阻塞滚动修复。核对 Pod owner 为 StatefulSet
`kafka`、PVC 为 `data-kafka-0` 后，只删除失败的 `kafka-0` Pod，PVC 未删除。
新 Pod 按修复模板重建并 Ready。随后再次滚动更新收敛 GC 日志，验证结果为：

```text
PVC UID=6bc802f0-4c09-478c-bf7d-a61dcb25dd13
PVC volume=pvc-6bc802f0-4c09-478c-bf7d-a61dcb25dd13
final Pod UID=13e17f46-1d3a-4cde-aaa8-53a53053573d
ClusterId=7KrkiFZsTlGTY-F1HBdr9Q
LeaderId=1
LeaderEpoch=3
MaxFollowerLag=0
```

Pod 身份改变而 PVC、Cluster ID 和 voter 保持稳定，证明当前单节点元数据能在
同卷 Pod 重建后恢复；本节当时没有证明主题或消息恢复，主题结果见下一节。最终
完整 1371 行启动日志中
`ERROR`、`FATAL`、`Reconfiguration`、`Read-only` 和 `StatusConsoleListener`
匹配数均为 0。

### 门禁与未覆盖边界

`make check`、三套 Kustomize 渲染、应用与 Kafka 服务端 dry-run、镜像身份门禁、
`git diff --check` 均通过；持续应用 overlay 与 Kafka overlay 的 `kubectl diff`
均为空。Broker API、Kafka 4.3.1 版本和 KRaft quorum 命令均成功。

本节的后续主题创建与重启保留结果见下一节；集群内生产/消费和
Filebeat→Kafka 仍未验证。plaintext、单副本和 combined KRaft 只适用于本地开发，
不代表 TLS/SASL、高可用或生产容量。liveness 使用 TCP 以避免周期性探针 JVM；
它不能单独识别“端口仍监听但 Broker API 无响应”的故障。

## Kafka 4.3.1：Kubernetes 项目主题初始化与恢复

- 日期：2026-08-03
- 入口：`make k8s-kafka-topics`
- 资源：一个哈希 ConfigMap、一个固定名 Job，不包含 Broker 或 PVC

### 主题契约与安全边界

主题 Job 通过 `kafka:9092` 连接已有 Broker，自动建主题仍关闭。四个主题的最终
契约为：

| 主题 | Topic ID | 分区 | 副本 |
|---|---|---:|---:|
| `logs.api-service` | `F1dCjCVcR2OEbUvD1NijAQ` | 3 | 1 |
| `logs.worker-service` | `FpyNrYc_Ti2Rf0C4p1jEdQ` | 3 | 1 |
| `logs.unclassified` | `BC8NMPRjSYuGOjb0xgOM8w` | 1 | 1 |
| `logs.dlq` | `zAeElgK9Sw6L7OsZZc_h3Q` | 1 | 1 |

四个主题均显式验证：

```text
cleanup.policy=delete
retention.ms=86400000
retention.bytes=134217728
segment.bytes=67108864
min.insync.replicas=1
```

`retention.bytes` 按分区计算。脚本先检查全部已存在主题，再创建缺失主题；已有主题
只做严格断言，不自动扩分区、重分配副本、修改保留期或删除主题。Job 使用
`backoffLimit: 0`，不以自动重试掩盖失败；完成后保留，宿主 runner 只会替换用途
标签正确且已经终止的同名 Job。

### 清单、镜像与反例门禁

`make k8s-kafka-topics-validate` 确认 overlay 恰好渲染一个
`kafka-topic-initializer-<hash>` ConfigMap 和一个 `kafka-topic-initializer` Job，
并用临时 `generateName` 完成服务端 dry-run，全程未写入集群。故意把包含
Namespace、Service、StatefulSet 与 PVC 的 `local-kafka` overlay 传给 runner 时，
门禁识别第一个越界 Namespace 并按预期退出非零。

独立审查又发现直接调用 runner 时可以绕过 Make 的节点摘要前置目标。修复后，
runner 在任何写操作前严格断言 Job 只有一个 `kafka-topic-initializer` 容器、镜像为
`apache/kafka:4.3.1`、拉取策略为 `Never`、摘要注解匹配，并自行执行节点
manifest/config 门禁。把预期节点标签改成 `apache/kafka:4.3.0` 的无写入反例按
预期失败；最终真实运行的顺序为节点摘要通过、ConfigMap apply、再次核对旧 Job
UID/用途/终态、删除旧 Job、创建新 Job。

Broker、配置初始化容器和主题 Job 共享同一 Kafka 镜像 Component。抽取前后
`kubectl diff -k deploy/kubernetes/overlays/local-kafka` 为空；运行前节点 manifest/
config 门禁通过，Job Pod 的运行时 config digest 为：

```text
sha256:47dccc76b32761bc57462b8753144cdbb73a16b123b1d13d3eedb92bb7952b11
```

Job 以 UID/GID 1000、Restricted、只读根文件系统运行；关闭 token 与 ServiceLinks，
只挂载只读脚本和 64 MiB `/tmp`，没有 Kubernetes RBAC 或 Kafka 数据 PVC。资源
请求为 50m CPU/128 MiB，限制为 500m CPU/384 MiB，CLI 堆为 32～128 MiB；Job
180 秒到期且不重试，runner 默认 300 秒超时用于等待终态和收集诊断。

### 双次运行与 Pod 重建

第一次运行创建 ConfigMap `kafka-topic-initializer-66bf646fdd` 和单 Pod Job；Job
成功、Pod 0 重启，日志按当时解析输出四条 `TOPIC_VERIFIED`；后续审查发现配置
来源证明不足并按下文修复、复跑。
第二次运行先安全删除已完成的同名 Job，ConfigMap 为 `unchanged`；四个 Topic ID
与第一次完全相同，证明重复入口不会重复创建或替换正确主题。

解析审查发现 `kafka-topics --describe` header 的 `Configs` 会混入 Broker 继承值，
且不能无歧义表示 `cleanup.policy=delete,compact` 这类含逗号值。修复后，该命令
只负责 Topic ID、分区、副本、Leader、ISR 和重分配验证；五项显式 override 改由
`kafka-configs --describe` 的逐行 `DYNAMIC_TOPIC_CONFIG` 验证，并拒绝任何缺项、
额外动态配置或错误值。默认 `make check` 新增纯本地解析自测，1 个正例和
`delete,compact`、缺项、额外项、错误副本、ISR 不完整、重分配 6 个失败反例均
通过。

修复后的最终真实运行创建当前 ConfigMap
`kafka-topic-initializer-m49d5b8kth`；Job UID 为
`0c967ba6-156f-4f67-a973-ed5d6f1ebc0d`，从 `2026-08-03T05:02:12Z` 到
`05:03:28Z` 完成，单 Pod、0 重启，运行时镜像摘要匹配，四个 Topic ID 仍与上表
一致。此前 ConfigMap `kafka-topic-initializer-66bf646fdd` 作为旧脚本证据保留，
当前 Job 只引用新哈希；本轮没有执行无选择器的清理。

在解析审查与最终复跑之前，曾在不重跑初始化 Job 的前提下执行恢复验证。删除前
先确认 `kafka-0` 归属
`StatefulSet/kafka`、PVC 为 Bound、主题 Job 为 Complete；结果为：

```text
old Pod UID=13e17f46-1d3a-4cde-aaa8-53a53053573d
new Pod UID=a239ea77-f954-49b4-8d16-3a5b3cb8f981
PVC UID=6bc802f0-4c09-478c-bf7d-a61dcb25dd13
topic Job UID=9f7be223-c940-46f8-89c2-db2b24dbe329
ClusterId=7KrkiFZsTlGTY-F1HBdr9Q
LeaderEpoch=4
MaxFollowerLag=0
```

Pod UID 改变，PVC UID 与主题 Job UID 不变。新 Broker Ready 后、任何主题初始化
入口再次运行前，直接 describe 得到相同的四个 Topic ID、分区、副本、ISR 和五项
有效配置值；该旧检查本身不能证明配置来源。后续修复版 Job 在任何主题写入前通过
`kafka-configs` 确认五项均为主题级动态 override，且该轮四个主题都已存在，没有
执行 create 或 alter。重建后双容器运行时镜像检查再次通过，完整 1402 行启动日志中
`ERROR`、`FATAL`、`Reconfiguration`、`Read-only`、`StatusConsoleListener` 匹配数
为 0。

### 未覆盖边界

本节证明单节点开发环境中的主题定义、幂等初始化和同一 PVC 上的主题元数据恢复；
尚未在 Kubernetes 中写入或消费消息，因此不证明消息恢复、Filebeat→Kafka、
多 Broker 副本恢复、TLS/SASL、高可用或生产容量。四个主题的创建不是事务；若
中途失败，只有已经创建且契约正确的主题可以保留并由重跑继续创建余下主题；
任何漂移都会继续失败并要求人工判断处理，不能宣称自动收敛或原子完成。

## Filebeat 9.4.4：Kubernetes 采集、路由与兜底验收

- 日期：2026-08-03
- Kubernetes：v1.35.1，containerd 2.2.1
- Filebeat：`docker.elastic.co/beats/filebeat-wolfi:9.4.4`
- Kafka：4.3.1，Filebeat 协议版本显式设为 `4.1.0`

### 镜像与配置门禁

上游多架构索引摘要为
`sha256:e323c1c7c3bec7ea979cef3827f53ff2d576e1d3020e5d56cb9e40d6b49c48ca`，
linux/amd64 清单摘要为
`sha256:3d14aa62612275ffae45891e523e9b29f23eb647032809190eb60f6b4a549379`。
镜像旁加载到 Minikube 节点后，实际 manifest 摘要为
`sha256:a700abba5534b71456b1e6fb44c40f5ac7582ec1a9c2f458a672cbf98bea1eb9`，
CRI config 摘要为
`sha256:fa7ab9fc5ce34d22947367ce44f7e4091cfca1fa3f39a2acfd97bceede6646bf`。
部署前门禁核对了架构、上游 amd64 清单、节点 manifest/config；部署后又核对
Pod 的 `imageID`，三层身份均匹配。

`filebeat test config` 的基础正例与 `output.kafka.version: "4.1.0"` 正例均输出
`Config OK`。故意改为 Filebeat 不支持的 `4.3.1` 时，命令退出码为 1，且错误信息
明确指出 Kafka 版本不受支持，证明门禁不会把连接成功误当成协议配置正确。

### 部署、安全与采集边界

Filebeat 以单节点 DaemonSet 运行在项目专用 `stage3-collector` 命名空间；应用继续
保留在 Restricted 的 `stage3-logs`。采集器命名空间因只读 `hostPath` 使用
Privileged enforce、Restricted warn/audit，但容器本身保持 `privileged=false`、
禁止提权、丢弃全部 capabilities、RuntimeDefault seccomp 和只读根文件系统。
容器使用 UID/GID 0 读取节点上实际为 `root:root 0640` 的日志文件；它没有 Docker
socket，唯一宿主写路径是项目专用 Filebeat registry。

跨命名空间 RBAC 仅允许专用 ServiceAccount 对 `stage3-logs` Pod 执行
`get/list/watch`。运行时反例确认它不能读取 Secret、Node、Namespace、
`kube-system` Pod，也不能创建 Pod。最终 DaemonSet 为 1 个 Running/Ready Pod、
0 重启；资源请求为 50m CPU/128 MiB，限制为 500m CPU/256 MiB，部署后观测内存
当前值约 46.4 MiB、峰值约 79.8 MiB。

采集器只扫描
`/var/log/containers/*_stage3-logs_log-producer-*.log`，使用固定 `filestream` ID 和
CRI stdout parser。业务 JSON 不在 Filebeat 中展开，而是逐字保留在 `message`；
`log-processor` 后续负责业务校验、规范化与 `event_id`。已知 Pod `service` 标签
静态路由到两个业务主题，未知或暂时缺少元数据的事件进入 `logs.unclassified`；
Filebeat 从不写 `logs.dlq` 或 Elasticsearch。

初版配置曾对缺少 Kubernetes 元数据的事件执行 `drop_event`。启动统计显示读取
50,632 条时有 29,211 条被过滤，旧容器日志与元数据缓存就绪竞态都可能触发数据
丢失。最终配置删除该过滤器：存在 Pod UID 时以 UID 作为 Kafka key；缺少元数据
时以 `log.file.path` 的稳定指纹作为 key，并写入
`fields.routing_reason=kubernetes_metadata_missing`。

### 正常 api/worker 路由验收

运行入口为 `make k8s-filebeat-acceptance`。脚本先记录四个主题各分区的起始位点，
再用唯一批次启动两个各 20 条的 `log-producer` Job，最后只消费本次起止位点范围，
由独立 Python 验证器按 `test_run_id` 严格对账。

最终批次为 `uc001-filebeat-2db71a8adcea4a828cbeed7c7563d779`，结果如下：

```text
api-service:    20 logical events, partition 1
worker-service: 20 logical events, partition 2
total:          40 logical events, 40 physical records, 0 duplicates
unclassified:   0 matching events
dlq:            0 matching events
```

每条事件均验证了原始 `message` 字节、连续 `event.sequence=1..20`、服务主题、
`kubernetes.namespace=stage3-logs`、Pod 名称、真实 Pod UID、容器名、
`filestream`/stdout 来源和 Filebeat 版本。Kafka key 与实际 Pod UID 完全相同，
同一 Pod 的全部事件落入同一分区。消费范围内还包含持续 Deployment 的正常日志，
验证器通过唯一批次 ID 排除它们，没有把旧数据误计为验收成功。

2026-08-04 在实现 `log-processor` 解析器前，验收器新增 `log.offset` 必须存在、
类型为整数且不小于 0 的门禁；缺失、负数、小数和布尔值四个反例均先失败后转绿。
真实批次 `uc001-filebeat-b20edd4bbf544226b977ac5efc33bdb6` 随后重新执行，
api/worker 各 20 条、合计 40 条逻辑/40 条物理记录、0 重复，全部通过源文件位点
门禁。这里的 `log.offset` 与 Kafka 抓取行中的 record offset 是两个独立字段。

### 缺少元数据的兜底验收

`make k8s-filebeat-fallback-acceptance` 在 Minikube 节点创建一条大于 1024 字节的
临时 CRI 日志和对应 `/var/log/containers` 符号链接，路径仍满足项目采集范围，
但不对应真实 Pod。最终批次
`uc001-fallback-30b700c86c1e424eaf985da8c794ed61` 使
`logs.unclassified` 分区 0 从位点 7 增长到 8；验证结果为 1 条逻辑事件、
1 条物理记录、0 重复。

该事件完整保留原始 `message`，没有伪造 Pod UID，包含明确的 metadata-missing
原因，Kafka key 与从真实 `log.file.path` 重新计算的指纹一致。第一次真实运行
`uc001-fallback-f7cab9d6a754406baf6b685024376262` 按预期暴露了验证器少计算末尾
字段分隔符的问题；修正为与 Filebeat fingerprint processor 完全相同的输入格式后，
单元测试和真实复跑均通过。两轮临时日志、符号链接和空目录均已按精确路径清理，
节点上没有残留 `filebeat-fallback-*` 文件。

双条件路由部署后的第一次兜底回归
`uc001-fallback-cce9cefba07844f080d5e964f812fa24` 超时，证明放在伪 Pod 目录中的
fixture 可能被 kubelet 在 Filebeat scanner 命中前当作孤儿日志清理。测试没有降低
等待或判定标准；最终实现把唯一临时文件放入真实 Running `api-service` Pod 的受管
日志目录，但符号链接仍使用不存在的容器 ID，确保 Kubernetes 元数据无法补齐。
最终批次 `uc001-fallback-b24cea7aeb474cbb948aad51f61c9fe7` 使
`logs.unclassified` 位点从 12 增长到 13，1 条逻辑/1 条物理/0 重复，再次验证
原始消息、缺失原因和路径指纹。临时文件与符号链接均已删除。

### Filebeat registry Pod 重建恢复

入口为 `make k8s-filebeat-registry-recovery`。整个流程使用统一工作流锁；部署、
普通验收、兜底验收和恢复验收不能并发替换 Filebeat Pod 或固定名 Job。删除前，
脚本严格核对单个 Running/Ready Filebeat Pod 的 controller owner 为同一
`DaemonSet/filebeat` UID，并保留重建前两个固定验收 Job、Pod 与节点日志到完整
恢复窗口结束。

重建前批次为 `uc001-registry-pre-533e753f6e1947beb705952d3018d0c4`，两个服务
各 20 条均完成主题、Pod UID key、Kubernetes 元数据和原始消息对账。随后记录四
主题高水位并删除该 Filebeat Pod。恢复身份如下：

```text
old Pod UID=8f23ed3e-d4ed-4eea-bd91-7151857f57e8
new Pod UID=a7d3ee84-760c-4607-8c55-e8b9e9fa094b
registry root device:inode=2096:328901
meta.json device:inode=2096:328984
Beat UUID=d13edf01-7908-4639-9b42-939ca013e482
first_start=2026-08-03T06:01:58.696671607Z
```

新 Pod 位于同一节点、Running/Ready、0 重启，运行时镜像摘要仍匹配；Pod 内 data
挂载与节点 registry 的 device/inode 相同。目录身份、`meta.json` inode、UUID 和
`first_start` 前后逐项一致，registry 日志文件存在且非空。

重建后使用不替换 pre Job 的独立名称 Job，批次为
`uc001-registry-post-b4931e51fd7b4dbd9c00b5695907c26b`。结果为 40 条逻辑事件、
40 条物理记录、0 重复；收齐后继续观察 20 秒并消费到最终高水位。在从 pre 批次
完成到 post 稳定窗口结束的全部四主题记录中，结构化解析确认旧 pre run-id 再次
出现 0 次；窗口结束时 pre Job/Pod UID 未变，节点源日志仍包含完整 pre run-id。
恢复专用 Job 最后按 UID、用途和 run-id 三重匹配后前台删除，没有残留。

### Kafka 单 Broker 短时停止后的积压恢复

入口为 `make k8s-filebeat-kafka-outage-recovery`，通过统一 Filebeat 工作流锁与其他
部署、验收和恢复操作互斥。验收最终采用 Kafka StatefulSet 受控缩容 1→0→1，
而不是 NetworkPolicy：预实验确认 Kindnet 能阻止新连接，但既有 Kafka TCP 连接
仍可继续使用，因此脚本在位点未稳定时拒绝创建日志 Job，恢复策略并完成清理；
这种证据不足以声明严格故障窗口。

最终批次为
`uc001-kafka-outage-2a0e9228f79e484f904c609d0c7affbe`。任何写操作前，脚本核对
Kafka StatefulSet UID、generation、revision、原副本数、Retain 策略，Kafka Pod
owner、镜像、0 重启，PVC UID/PV、`meta.properties` 摘要和 Broker Cluster ID，
同时记录四个主题的全部分区位点。Filebeat Pod 必须 Ready、0 重启、运行时镜像
正确，Kafka/Filebeat Kustomize diff 必须为空。关键初始身份如下：

```text
Kafka StatefulSet UID=f7d599d7-f2ab-489f-bf69-4ec539f12cf1
Kafka 原 Pod UID=a239ea77-f954-49b4-8d16-3a5b3cb8f981
Filebeat Pod UID=a7d3ee84-760c-4607-8c55-e8b9e9fa094b
PVC UID=6bc802f0-4c09-478c-bf7d-a61dcb25dd13
PV=pvc-6bc802f0-4c09-478c-bf7d-a61dcb25dd13
Cluster ID=7KrkiFZsTlGTY-F1HBdr9Q
meta.properties SHA-256=66c665545ba66b5ede82ad0fed60f681a51c67b23ff632b7267e674493632eb6
```

缩容使用带 `metadata.uid` 和原 `spec.replicas` 两个 `test` 条件的 JSON Patch，
`EXIT` 陷阱在任何后续失败时都按原 StatefulSet UID 尝试恢复副本数。只有在
StatefulSet 状态为 0/0/0、`kafka-0` 已不存在且 Filebeat output 输出明确包含
解析成功、DNS 成功和拨号失败时，脚本才创建两个各 20 条的唯一 Job。Filebeat
9.4.4 在连接拒绝时会打印 `ERROR` 却返回原始退出码 0，因此门禁校验命令的语义
输出，而不把退出码 0 误判成可达；正常连接正例仍必须同时通过。

故障窗口内两个 Job 各输出 20 行合法 JSON。恢复 Kafka 前，脚本从同一 Filebeat
Pod 的唯一 Filebeat 进程读取 `/proc`，确认两份精确 CRI 文件都已读到 EOF：

```text
api-service:    pid=7 fd=11 position=4955 size=4955
worker-service: pid=7 fd=10 position=5015 size=5015
outage_confirmed_at=2026-08-03T09:01:38Z
jobs_completed_at=2026-08-03T09:01:44Z
restore_started_at=2026-08-03T09:01:46Z
已确认不可用窗口=8 秒
```

恢复后 Kafka 新 Pod UID 为 `2312bde8-ee5b-48f4-9252-c72fb43e666f`，Ready、0
重启；StatefulSet UID/revision、PVC UID/PV、运行时镜像、`meta.properties`
摘要与 Cluster ID 均保持。Filebeat Pod UID、容器身份和 0 重启也保持。恢复完成
时间为 `2026-08-03T09:02:53Z`，从开始恢复到完整对账用时 67 秒。

校验器从故障前位点到恢复后高水位消费四个主题，并以精确 run-id 过滤同时运行的
持续日志：40 条逻辑事件、40 条物理记录、0 条逐字节相同重复、0 缺失、0 错误
路由；`logs.unclassified` 和 `logs.dlq` 没有本批事件。继续稳定观察 20 秒后结果
不变。两个 Job 最后按 UID、用途和 run-id 三重匹配删除，集群中没有故障验收
残留，Kafka/Filebeat overlay diff 仍为空。

### Filebeat 事件解析与确定性 ID 单元验证

2026-08-04 完成 `internal/event` 的纯 Go 解析和 `event_id v1` 计算边界。
ID 测试覆盖 UTF-8 固定向量、原始业务 JSON 保持、相同时刻跨时区稳定性、六个
身份字段逐项变化、非身份字段与 Filebeat 外层时间不影响结果、长度前缀防字段
边界碰撞，以及缺失稳定身份时的永久错误分类。解析器同时拒绝零值业务时间，
避免出现解析成功但生成 ID 失败的不一致状态。
固定向量结果为
`sha256:0bf68546f85a708f7b01b1f96c3738a1c6d1c406e6b20844dce629407de3ad97`，
从真实 Filebeat 双层结构解析得到的向量结果为
`sha256:c8eaed2985adc25f832a6835101d75f6fb563f1e10556f5d1d232890d7555055`。

`make check`、`git diff --check` 和 `go test -race -cover ./...` 均通过；
`internal/event` 语句覆盖率为 97.3%。本验证只证明纯函数契约，尚未声明 Kafka
消费、Elasticsearch 写入或端到端幂等已经完成。

### 当前边界

本节已经证明镜像身份、配置反例、最小 RBAC、运行时安全、正常服务路由、Pod UID
分区键、Kubernetes 元数据、原始消息保持、缺少元数据时的稳定兜底以及 Filebeat
Pod 重建后的 registry 连续性；还证明了受控单 Broker 1→0→1 的有界故障窗口内，
同一 PVC 上故障前位点连续可读并继续推进，新产生的 40 条日志恢复后完整投递。
它不代表多节点故障转移、任意长中断、队列饱和、节点磁盘丢失、TLS/SASL 或
生产容量，也尚未覆盖
`log-processor`、Elasticsearch 与 Grafana 的后半段链路。
