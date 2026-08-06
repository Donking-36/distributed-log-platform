# UC-001 与 UC-002 验收演示指南

本文依据岗位要求文档中的 UC-001“Kubernetes 集群日志采集”和 UC-002“日志全文
检索与聚合分析”编写，用于在已经准备好的 `stage3-logs` Minikube 环境中完成现场
验收。跨机器安装、固定镜像导入和从空集群部署请先参照
[`DEMO_GUIDE.md`](DEMO_GUIDE.md)。

本文区分两种证据：

- 现场命令：当场产生唯一批次并给出可复核结果；
- 历史证据：仓库 [`test-report.md`](test-report.md) 中已经完成的较长故障与性能验收。

所有会替换 Pod、缩放副本或创建验收 Job 的入口都必须串行执行。不要同时运行
Filebeat、处理器、Grafana 或吞吐验收。

## 1. 验收范围与完成边界

### 1.1 岗位要求与本项目证据对应关系

| 要求 | 本项目证据 | 当前结论 |
|---|---|---|
| Filebeat 以 DaemonSet 采集 Pod 标准输出 | `make k8s-filebeat-status`、`make k8s-filebeat-acceptance` | 已完成 |
| 补充 namespace、pod_name、service_name 等 Kubernetes 元数据 | Filebeat 固定批次逐条校验 | 已完成 |
| JSON 日志按服务写入 Kafka 主题 | `logs.api-service`、`logs.worker-service` 路由对账 | 已完成 |
| Kafka 日志经 Go 服务写入 Elasticsearch | `log-processor`、ES 最新文档和消费者组 LAG | 已完成 |
| 不丢失、不重复 | 固定批次 40/40、0 重复；稳定 `event_id` 幂等写入 | 已完成 |
| Pod 重建后继续采集 | `make k8s-filebeat-registry-recovery` | 已完成，本地单节点范围 |
| Kafka 短时不可用后补投 | `make k8s-filebeat-kafka-outage-recovery` | 已完成，有界短停范围 |
| 完整链路每秒处理 1000+ 条 | `make perf`，历史实测 1430.66 条/秒 | 已完成，本机固定资源范围 |
| 按时间、服务、关键词检索 | Grafana 页面和 `make k8s-grafana-acceptance` | 已完成 |
| 日志明细、级别分布、异常趋势和下钻 | Grafana 三类预置面板 | 已完成 |
| 查询响应不超过 1 秒 | 固定数据集三次 p95：0.284、0.339、0.273 秒 | 已完成，本机固定数据集范围 |

岗位要求还包含全链路延迟不超过 3 秒、Prometheus、HPA 和更高层级的可观测性。
当前项目把每秒 1000+ 条作为硬性性能门禁；没有单独固化“每条日志 3 秒内可检索”
的自动验收，Prometheus 与 HPA 也尚未实现。因此演示时不要宣称这些条目已完成。
单节点 Minikube 的恢复和性能结果也不能外推为生产高可用或生产容量。

### 1.2 推荐时间安排

| 阶段 | 内容 | 预计时间 |
|---|---|---:|
| 演示前 | 启动集群、检查组件、执行工程门禁 | 5～10 分钟 |
| UC-001 核心 | 日志源、Filebeat 路由、Kafka、处理器、ES | 8～12 分钟 |
| UC-001 性能 | 120000 条完整链路吞吐 | 1～2 分钟 |
| UC-002 | Grafana 页面和固定数据集自动验收 | 5～8 分钟 |
| 可选恢复 | Filebeat、Kafka、处理器、Grafana 恢复 | 视现场时间决定 |

正式演示建议现场运行核心验收和吞吐验收；耗时较长的 Filebeat registry 与 Kafka
短停恢复可以展示历史报告，评委要求时再单独执行。

## 2. 演示前准备

### 2.1 启动并确认集群

目的：恢复关机后暂停的 Minikube，并确认后续 Make 入口不会操作错误集群。

```bash
cd ~/projects/distributed-log-platform

minikube start -p stage3-logs
kubectl config use-context stage3-logs
kubectl --context=stage3-logs wait \
  --for=condition=Ready node/stage3-logs \
  --timeout=120s

minikube status -p stage3-logs
kubectl config current-context
```

预期结果：Host、Kubelet、API Server 均为 `Running`，节点 Ready，当前上下文为
`stage3-logs`。如果节点长时间 `NotReady`，先执行：

```bash
minikube stop -p stage3-logs
minikube start -p stage3-logs
kubectl --context=stage3-logs wait \
  --for=condition=Ready node/stage3-logs \
  --timeout=180s
```

仍失败时停止验收，查看 `minikube logs -p stage3-logs`，不要继续运行写集群命令。

### 2.2 确认资源与工程状态

目的：证明本次演示使用约定的 4 CPU、6 GiB 和 Go 1.26.5 基线，并排除代码问题。

```bash
grep -E '"(CPUs|Memory|Driver|ContainerRuntime)"' \
  ~/.minikube/profiles/stage3-logs/config.json

docker inspect stage3-logs \
  --format 'cpu={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}}'

go version
git status --short --branch
make check
```

预期结果：配置显示 `CPUs: 4`、`Memory: 6144`、Docker driver 和 containerd；容器
内存为 `6442450944` 字节，CPU 限额为 `4000000000` NanoCPUs；Go 为 1.26.5；
`make check` 全部通过。

异常分支：

- Go 版本不匹配：切换到 Go 1.26.5 后重试，不跳过版本门禁；
- 工作区有改动：先确认改动来源，不要为了演示使用 `git reset --hard`；
- 镜像缺失：按照 `DEMO_GUIDE.md` 执行 `make k8s-images`，不要使用 `latest`。

### 2.3 确认全部组件就绪

目的：建立验收前基线，避免把基础组件未启动误判为功能失败。

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

预期持续组件：

- `api-service`、`worker-service`、`log-processor`、`grafana` 为 Ready；
- `kafka-0`、`elasticsearch-0` 为 Ready；
- `stage3-collector` 中每个节点有一个 Ready 的 Filebeat Pod；
- 主题初始化 Job 为 `Completed`，这不是异常。

如果某个组件不存在，应按依赖顺序恢复，不能颠倒 Kafka、Elasticsearch 与处理器：

```bash
make k8s-kafka-deploy
make k8s-kafka-topics
make k8s-elasticsearch-deploy
make k8s-elasticsearch-template
make k8s-deploy
make k8s-filebeat-deploy
make k8s-grafana-deploy
```

## 3. UC-001 验收：Kubernetes 集群日志采集

### 3.1 先讲清楚数据链路

现场用一句话说明：两个 Go `log-producer` 只向容器标准输出写 JSON；Filebeat
DaemonSet 读取节点 CRI 日志、补充 Kubernetes 元数据并按服务路由到 Kafka；Go
`log-processor` 消费 Kafka，生成稳定 `event_id`，批量幂等写入 Elasticsearch。

```text
Pod stdout
  → Filebeat DaemonSet
  → Kafka logs.<service>
  → Go log-processor
  → Elasticsearch logs-stage3-*
```

这种拆分让日志源不依赖 Kafka/Elasticsearch SDK，采集、缓冲、处理和存储职责相互
独立；Kafka 位点只在 ES 写入成功或确认重复后提交。

### 3.2 展示结构化日志源

目的：证明触发源确实是 Kubernetes Pod 标准输出，而不是脚本直接写 Kafka。

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

预期每行都是一个 JSON 对象，包含 `@timestamp`、`event.sequence`、`log.level`、
`message`、`service.name` 和 `test_run_id`；两个服务的 `service.name` 不相同。

异常分支：没有输出时先运行 `make k8s-status`；Pod 不 Ready 则执行
`kubectl describe pod` 和 `kubectl logs --previous`，不要直接重建整个集群。

### 3.3 固定批次验证 Filebeat 元数据与 Kafka 路由

目的：生成两个服务各 20 条的新日志，并按唯一 `test_run_id` 精确对账。本入口
记录 Kafka 各分区起止位点，只统计本批次，不会把历史日志算入结果。

```bash
UC001_RUN_ID="uc001-demo-$(date -u +%Y%m%dt%H%M%Sz)"
make k8s-filebeat-acceptance \
  ACCEPTANCE_RUN_ID="$UC001_RUN_ID"
```

预期关键输出：

```text
api-service:    20 logical events
worker-service: 20 logical events
total:          40 logical events, 40 physical records, 0 duplicates
unclassified:   0 matching events
dlq:            0 matching events
```

脚本还会逐条验证：

- 原始 `message` 是合法 JSON，序号连续为 1～20；
- `kubernetes.namespace=stage3-logs`；
- Pod 名称、Pod UID、容器名和 `log.file.path` 完整；
- `service.name` 与 Pod 的权威 `service` 标签一致；
- `api-service` 只进入 `logs.api-service`；
- `worker-service` 只进入 `logs.worker-service`；
- Kafka key 使用真实 Pod UID；
- `logs.unclassified` 和 `logs.dlq` 中没有本批次事件。

异常分支：

- `40 logical events` 未收齐：查看 Filebeat 与 Kafka 状态，不要立即重跑覆盖现场；
- 出现重复：保存完整输出和 run_id，检查 Filebeat registry 与 Kafka 起止位点；
- 路由错误：核对 Pod `service` 标签和 Filebeat 路由配置；
- 超时但组件 Ready：检查 `kubectl logs -n stage3-collector -l app.kubernetes.io/name=filebeat`。

### 3.4 展示 Kafka 主题与消费者进度

目的：证明日志经过 Kafka 解耦，而且 Go 处理器已经追平当前业务分区。

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

预期主题包括 `logs.api-service`、`logs.worker-service`、`logs.unclassified` 和
`logs.dlq`。稳定后各活动分区 `LAG` 应收敛到 0。瞬时非零 LAG 代表正在处理，
等待数秒后复查；持续增长才是异常。

### 3.5 展示 Elasticsearch 最终结果

目的：证明数据已经穿过 Filebeat、Kafka 和 Go 处理器，而不只是停留在 stdout。

```bash
kubectl --context=stage3-logs exec \
  -n stage3-logs elasticsearch-0 \
  -c elasticsearch -- \
  curl -fsS \
  --request POST \
  --header 'Content-Type: application/json' \
  --data-binary "{\"size\":4,\"track_total_hits\":true,\"sort\":[{\"ingested_at\":\"desc\"}],\"query\":{\"term\":{\"test_run_id\":\"$UC001_RUN_ID\"}},\"_source\":[\"@timestamp\",\"ingested_at\",\"service.name\",\"kubernetes.pod.name\",\"log.level\",\"message\",\"test_run_id\"]}" \
  'http://localhost:9200/logs-stage3-*/_search?filter_path=hits.total,hits.hits._id,hits.hits._source'
```

预期 `hits.total.value=40`，结果中的 `test_run_id` 等于本次 `$UC001_RUN_ID`，并能
看到两个服务的文档。字段包含业务时间、写入时间、服务名、Pod 名、级别和消息，
且每个 hit 都有稳定 `_id`。如果数量暂未达到 40，等待消费者组 LAG=0 后重试；
如果仍为空，依次检查处理器日志和 Elasticsearch 健康状态。

### 3.6 验证处理器幂等与重建续读

目的：证明同一事件重复投递不会在 ES 生成两份文档，并证明处理器停机窗口的消息
在新 Pod 启动后继续消费。

```bash
make k8s-processor-acceptance \
  PROCESSOR_ACCEPTANCE_RUN_ID="uc001-processor-demo-$(date -u +%Y%m%dt%H%M%Sz)"
```

预期关键结果：重复 fixture 在 ES 中只有一个文档，`_id=event_id`；处理器旧 Pod
在 SIGTERM 期间变为 Ready=False；新 Pod UID 与旧 UID 不同、镜像身份不变、0 重启，
恢复事件写入 ES，消费者组最终 `LAG=0`。

该入口会缩放处理器副本，必须等它完全结束后再运行其他验收。如果中途失败，脚本
会尝试恢复原副本数；随后执行 `make k8s-status` 确认处理器 Ready。

### 3.7 验证每秒 1000+ 条完整链路吞吐

目的：验证从 Kubernetes 调度与 stdout 开始，经过 Filebeat、Kafka、Go 批处理、
Elasticsearch Bulk 和 Kafka 提交的整体持续处理能力。这里验证的是完整链路吞吐，
不是“同时启动 1000 个线程”。

```bash
make perf \
  THROUGHPUT_RUN_ID="perf-demo-$(date -u +%Y%m%dt%H%M%Sz)"
```

脚本创建两个一次性 Job，每个服务以 1 ms 间隔生成 60000 条，共 120000 条。通过
条件必须同时满足：

- `throughput_events_per_second >= 1000`；
- `input_events=120000`；
- `elasticsearch_unique_documents=120000`；
- `processing_errors=0`；
- 处理器 Ready、0 重启；
- `consumer_group_lag=0`。

历史通过证据：

```text
run_id=perf-20260805t082838z-434544
input_events=120000 elasticsearch_unique_documents=120000
elapsed_seconds=83.877 throughput_events_per_second=1430.66
processing_errors=0 consumer_group_lag=0
```

成功后两个大日志 Job 自动删除，ES 文档按唯一 run_id 保留。若吞吐低于 1000，先
记录本次完整输出和机器负载；确认仍为 4 CPU/6 GiB、没有并行验收，再检查处理器
重启、ES 状态和最终 LAG。不要只增加超时时间来伪造通过。

### 3.8 可选：演示 Filebeat 与 Kafka 故障恢复

下面两项已有真实历史证据，现场时间充足或评委明确要求时再执行，且必须逐个运行。

```bash
make k8s-filebeat-registry-recovery
```

预期：Filebeat Pod UID 改变，但宿主 registry、Beat UUID 和首次启动时间保持；重建
前旧批次回放 0 条，重建后新批次 40/40 到达且 0 重复。

```bash
make k8s-filebeat-kafka-outage-recovery
```

预期：脚本受控将单 Broker 从 1 缩到 0，在确认不可达后产生 40 条日志，再恢复为
1；StatefulSet、PVC 和 Cluster ID 保持，40 条最终全部正确投递，0 缺失、0 重复，
Filebeat 不重启。

第二个入口会真实缩放 Kafka。演示中途即使失败，也要等待退出清理完成，然后执行：

```bash
make k8s-kafka-status
make k8s-filebeat-status
```

本证据只覆盖本地单 Broker 的有界短停，不代表多节点自动故障转移或任意长中断。

### 3.9 UC-001 通过判定

现场记录下列五项即可判定 UC-001 核心验收通过：

- [ ] 两个服务 Pod 能持续输出合法 JSON；
- [ ] 固定批次为 40 条逻辑事件、40 条物理记录、0 重复、0 错误路由；
- [ ] 四个 Kafka 主题存在，处理器消费者组最终 LAG=0；
- [ ] Elasticsearch 能查到带 Kubernetes 元数据和稳定 ID 的最终文档；
- [ ] 吞吐不少于 1000 条/秒，输入数等于 ES 唯一文档数且错误为 0。

## 4. UC-002 验收：日志全文检索与聚合分析

### 4.1 说明 Grafana 为什么能查到 Elasticsearch

Grafana 通过声明式配置的数据源直接连接集群内
`http://elasticsearch:9200`，索引模式为 `logs-stage3-*`，时间字段为
`@timestamp`。仪表盘中的查询由 Grafana 内置 Elasticsearch 插件转换为 ES 查询，
因此本项目不再增加一层只做转发的 Go 查询服务。

### 4.2 打开 Grafana 仪表盘

目的：现场展示最终用户的查询入口。单独打开一个终端并保持运行：

```bash
kubectl --context=stage3-logs port-forward \
  -n stage3-logs service/grafana 3000:3000
```

浏览器访问 <http://127.0.0.1:3000>，进入：

```text
Dashboards → 阶段三日志平台 → 分布式日志检索
```

本地演示启用了匿名 Viewer，不需要登录。页面打不开时先确认端口转发终端仍在运行；
3000 端口占用时可改为 `13000:3000`，并访问 <http://127.0.0.1:13000>。

### 4.3 按验收顺序操作页面

1. 时间范围选择“最近 15 分钟”或“最近 1 小时”，确认日志明细出现；
2. 服务变量先选 `api-service`，再选 `worker-service`，确认结果随服务变化；
3. 关键词输入 `log event`，确认只保留消息命中的日志；
4. 输入一个不可能存在的关键词，确认结果为空而页面不报错；
5. 清空关键词，展开一条日志，查看时间、级别、服务、Pod、消息和 `event_id`；
6. 展示“日志级别分布”面板；
7. 展示“ERROR/WARN 趋势”面板。

持续日志源主要产生 INFO，因此 ERROR/WARN 面板现场可能较少或为空。四种级别及异常
趋势的精确数量由下一节的固定数据集验收证明，不应为了让图更好看而手工修改数据。

### 4.4 自动验证过滤、聚合、性能和重建恢复

先在端口转发终端按 `Ctrl+C`。目的：避免 Grafana Pod 被验收脚本替换后，旧端口
转发产生干扰。然后执行：

```bash
make k8s-grafana-acceptance \
  GRAFANA_ACCEPTANCE_RUN_ID="uc002-demo-$(date -u +%Y%m%dt%H%M%Sz)"
```

脚本会创建一个唯一临时 `logs-stage3-*` 索引，写入 16 条固定文档，然后通过
Grafana 数据源 API 验证：

- 全部日志 16 条；
- `api-service` 和 `worker-service` 各 8 条；
- 最近时间窗口 8 条；
- 关键词 `uc002-fixed-error` 命中 4 条；
- 不存在的关键词命中 0 条；
- DEBUG、INFO、WARN、ERROR 各 4 条；
- ERROR 和 WARN 趋势各 4 条；
- 明细总数与聚合数一致；
- 连续 10 次查询的 p50、p95；
- Grafana 缩到 0 并丢失临时 SQLite 后，数据源、仪表盘和查询自动恢复；
- 临时 ES 索引删除后再次查询返回 404。

预期关键输出形态：

```text
documents=16; all=16; api=8; worker=8; recent=8; keyword=4; none=0
levels=DEBUG:4,ERROR:4,INFO:4,WARN:4; trends=ERROR:4,WARN:4
latency_samples=10; p50=<1s; p95=<1s
old_pod_uid=<旧 UID>
new_pod_uid=<新 UID>
```

历史三次 p95 分别为 0.284、0.339 和 0.273 秒，均低于岗位要求的 1 秒。这里证明
的是固定本机、固定数据集下通过 Grafana 数据源 API 的查询耗时，不代表浏览器总
渲染时间或生产查询容量。

异常分支：

- 数据源健康检查失败：先执行 `make k8s-elasticsearch-status`；
- 仪表盘不存在：执行 `make k8s-grafana-deploy` 恢复声明式配置；
- 固定计数不一致：保存 run_id 和完整输出，不要手工改 ES 数据；
- p95 超过 1 秒：确认没有同时运行吞吐或故障验收，再检查 ES 与 Grafana 资源；
- 脚本中途失败：先执行 `make k8s-grafana-status`，确认副本已恢复为 1。

需要继续浏览页面时，重新运行端口转发命令。自动验收会删除临时固定数据索引，
因此页面恢复显示持续链路中的真实日志是正常行为。

### 4.5 UC-002 通过判定

- [ ] 时间范围能改变查询窗口；
- [ ] 服务变量能分别筛选 `api-service` 和 `worker-service`；
- [ ] 关键词能命中目标日志，无结果条件返回 0；
- [ ] 日志明细可展开并查看完整字段；
- [ ] 级别分布及 ERROR/WARN 趋势的固定计数正确；
- [ ] 10 次固定查询 p95 小于 1 秒；
- [ ] Grafana Pod 与临时 SQLite 重建后，数据源和仪表盘自动恢复。

## 5. 现场收尾与证据记录

### 5.1 最终健康检查

```bash
make k8s-status
make k8s-kafka-status
make k8s-elasticsearch-status
make k8s-filebeat-status
make k8s-grafana-status
```

预期所有持续组件恢复 Ready，Kafka/ES PVC 仍存在，处理器最终 LAG=0。若某个恢复
验收失败，先恢复组件健康，再结束演示。

### 5.2 建议保存的最小证据

每次正式验收至少记录：

- 日期、机器、4 CPU/6 GiB、Go/Kubernetes 版本；
- Git 提交 SHA；
- UC-001、UC-002 和性能验收的唯一 run_id；
- 固定批次 40/40、0 重复和路由结果；
- ES 查询样例与 Kafka 最终 LAG=0；
- 120000 条输入、ES 唯一文档数、吞吐、错误数；
- Grafana 固定计数、p50/p95、旧/新 Pod UID；
- 所有异常、重试和未通过项，不能只保留成功截图。

已有完整历史证据见 [`test-report.md`](test-report.md)。

### 5.3 暂停环境

需要保留 Kafka/Elasticsearch PVC 和下次演示状态时，使用：

```bash
minikube stop -p stage3-logs
```

不要使用 `minikube delete`。`stop` 只暂停集群；下次执行
`minikube start -p stage3-logs` 即可恢复。
