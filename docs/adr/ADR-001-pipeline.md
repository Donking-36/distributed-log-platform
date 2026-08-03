# ADR-001：核心日志管道与组件边界

- 状态：已接受
- 日期：2026-07-30
- 更新日期：2026-08-03

## 背景

原始需求要求 Kubernetes 日志经由 Filebeat 和 Kafka 进入
Elasticsearch，但没有明确由谁消费 Kafka。它还允许为 Grafana
增加可选的 Go 查询层。如果这两个问题都不作决定，数据链路会无法闭合，
或者项目会增加职责重复的服务。

本地项目必须保持精简，体现 Kafka 的作用，并包含一个有实质职责的 Go
服务，同时不引入生产规模的基础设施。

## 决策

`v0.1.0` 固定采用以下流程：

```text
Pod 标准输出
  → 节点容器日志文件
  → Filebeat DaemonSet
  → Kafka logs.<service>
  → Go log-processor
  → Elasticsearch logs-stage3-*
  → Grafana
```

组件边界如下：

1. `log-producer` 只向标准输出产生可预测的 JSON，绝不直接写入 Kafka 或
   Elasticsearch。
2. Filebeat 只采集目标命名空间和工作负载，排除自身及基础设施日志，
   补充 Kubernetes 元数据，并依据允许列表中的 Pod `service` 标签进行
   路由。它不做业务聚合，也绝不写入 Elasticsearch。
3. Kafka 负责缓冲，并将采集与处理解耦；它不是长期检索存储。
4. Go `log-processor` 是 Kafka 到 Elasticsearch 的唯一业务处理路径，
   负责校验、规范化、生成事件 ID 并写入 Elasticsearch。
5. Elasticsearch 负责索引和聚合；它不是消息队列。
6. Grafana 直接查询 Elasticsearch，不保存日志主数据。若要增加 Go
   查询接口，必须先有后续被接受的需求。

初始验收数据集采用以下约定：

- 同一个日志源镜像分别部署为 `api-service` 和 `worker-service`；
- 主题为 `logs.api-service` 和 `logs.worker-service`；
- 每个业务主题有三个分区，副本因子为 1；
- Kubernetes 服务标签与 Pod UID 都存在时使用稳定的 Pod UID 作为分区键；
- Pod `service` 标签是服务身份和路由的权威来源，Deployment 通过
  Downward API 把它注入 `PRODUCER_SERVICE_NAME`；
- `log-processor` 校验原始 `service.name` 与权威标签是否一致，不一致的
  事件进入 `logs.dlq`；
- 未知或缺失的服务标签只能进入固定的 `logs.unclassified`，绝不据此动态
  创建主题。
- 严格路径允许列表命中、但 `add_kubernetes_metadata` 未补齐 Pod UID 时
  （例如旧 Pod 已从 API 消失或启动阶段缓存尚未命中），Filebeat 不丢弃事件或
  伪造 Pod UID；它以 `sha256("|log.file.path|<path>|")` 的小写十六进制指纹
  作为稳定 key，标记元数据失效原因，并写入 `logs.unclassified`。

每个服务使用三个分区，可以在不改变初始主题契约的情况下开展后续消费者扩容
实验。副本因子为 1 是明确的单节点开发选择，不代表
系统具备高可用。

`log-processor` 只订阅明确列入允许列表的业务主题，不消费
`logs.unclassified` 或 `logs.dlq`。回退主题的消息数量和抽样记录，
是拒绝无效路由标签的可观测证据。

## 后果

### 正面影响

- 必需的数据管道可以端到端闭合。
- Kafka 具有清晰的缓冲和重放职责。
- Go 服务承担有实质意义的分布式系统行为。
- Grafana 预置保持简单，避免重复的查询模型。
- 受控的主题名称可防止标签值创建无限数量的主题。

### 负面影响

- 处理器必须处理 Kafka 偏移量、重试、毒消息和 Elasticsearch
  部分失败。
- 在 `v0.1.0` 中，Grafana 与 Elasticsearch 映射耦合。
- 单节点 Kafka 和 Elasticsearch 无法证明节点级高可用。
- 固定回退主题需要明确的监控和清理。

## 已拒绝的替代方案

- **Filebeat 直接写入 Elasticsearch：**这会去掉 Kafka，并绕过必需的
  Go 处理路径。
- **`log-producer` 直接写入 Kafka 或 Elasticsearch：**这会让生产者与
  基础设施耦合，并绕过节点级采集证据。
- **增加 Logstash：**这会重复处理器的职责，并削弱 Go 学习目标。
- **在 `v0.1.0` 中增加 Go 查询接口：**当前没有授权或隔离需求，这会与
  Grafana/Elasticsearch 查询重复。
- **直接用任意标签文本生成主题名称：**这会造成主题爆炸，并让路由
  验收失去确定性。
- **所有日志共用一个主题：**这会失去按服务路由的必要证据。
- **将 Elasticsearch 用作队列，或将 Kafka 用作长期检索：**这会把持久化
  和查询职责分配给错误的组件。
- **现在就部署生产级多节点 Kafka/Elasticsearch：**这超出本地学习范围，
  单节点恢复测试也不能推出这种能力。

## 验证义务

- `api-service` 和 `worker-service` 共同使用同一个唯一 `test_run_id`，并分别
  产生 20 条带编号的事件。
- 临时 Kafka 消费者能在正确的服务主题中观察到全部 20 个预期的唯一序号，
  且不存在跨主题路由或 Filebeat 递归采集。
- 缺失或未知服务标签的事件只进入 `logs.unclassified`，不会创建任意
  主题，也不会被正常处理器消费。
- 无 API 元数据的节点日志 fixture 使用可复算的路径指纹 key，只进入
  `logs.unclassified`，原始 `message` 保持不变，临时节点文件验收后精确清理。
- 演示事件无需人工写入 Elasticsearch 即可到达。
- 对一个固定的 `test_run_id`，在声明的测试边界内，N 个逻辑唯一输入产生
  N 个 Elasticsearch 唯一文档，并可通过已预置的 Grafana
  路径查询。
- Grafana 视图使用同一个 Elasticsearch 数据源，并可从仓库中的
  预置文件恢复。
