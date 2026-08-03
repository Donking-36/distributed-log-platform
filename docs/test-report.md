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
同卷 Pod 重建后恢复；本步没有证明主题或消息恢复。最终完整 1371 行启动日志中
`ERROR`、`FATAL`、`Reconfiguration`、`Read-only` 和 `StatusConsoleListener`
匹配数均为 0。

### 门禁与未覆盖边界

`make check`、三套 Kustomize 渲染、应用与 Kafka 服务端 dry-run、镜像身份门禁、
`git diff --check` 均通过；持续应用 overlay 与 Kafka overlay 的 `kubectl diff`
均为空。Broker API、Kafka 4.3.1 版本和 KRaft quorum 命令均成功。

本节尚未创建四个项目主题，也未验证主题在重启后的保留、集群内生产/消费或
Filebeat→Kafka。plaintext、单副本和 combined KRaft 只适用于本地开发，不代表
TLS/SASL、高可用或生产容量。liveness 使用 TCP 以避免周期性探针 JVM；它不能
单独识别“端口仍监听但 Broker API 无响应”的故障。
