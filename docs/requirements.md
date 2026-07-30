# Requirements

## 1. 目标

在本地 WSL2 Minikube 环境中构建一条可复现、可验证的分布式日志链路，使 Kubernetes Pod 日志能够被可靠采集、缓冲、处理、检索和可视化。

`v0.1.0` 必须完成：

- UC-001：Pod 日志经 Filebeat、Kafka 和 Go `log-processor` 进入 Elasticsearch。
- UC-002：Grafana 能按时间、服务和关键词检索日志，并展示可核对的聚合和详情。

## 2. `v0.1.0` 范围

- `demo-app` 输出可预测、可编号的结构化 JSON 日志。
- Filebeat 以 DaemonSet 运行，仅采集目标 namespace/工作负载，并补充 Kubernetes 元数据。
- Kafka 使用单节点开发配置，按 `logs.<service>` topic 解耦采集和处理。
- Go `log-processor` 消费 Kafka，校验和规范化事件，生成稳定 `event_id`，幂等写入 Elasticsearch。
- Elasticsearch 保存可全文检索、可按时间、服务和日志级别聚合的事件。
- Grafana 数据源和 dashboard 由仓库文件自动配置。
- 提供单元测试、组件集成测试和一条可重复执行的端到端冒烟测试。
- 提供从空 namespace 部署、验收和清理的文档与命令。

## 3. UC-001：日志采集、处理与存储

### 3.1 UC-001A：Pod 日志进入 Kafka

**Given**

- `stage3-logs` namespace 中用同一 `demo-app` 镜像部署 `demo-api` 和 `demo-worker`，并设置受控的 Pod `service` label。
- 两个服务使用同一个唯一 `test_run_id`，各输出 20 条带可预测序号的事件。
- Filebeat DaemonSet 已限定采集目标，排除自身及基础设施日志。

**When**

- `demo-app` 向 stdout 输出一批结构化 JSON 日志。

**Then**

- Filebeat 从 Kubernetes 节点容器日志目录采集事件。
- 每条事件包含 namespace、Pod 名称/UID、容器标识和服务名等必要元数据。
- Filebeat 根据受控的 Pod `service` label 选择 topic，规范化后的事件字段使用 `service.name`。
- `demo-api` 和 `demo-worker` 分别路由到 `logs.demo-api` 和 `logs.demo-worker`，两个服务不串流。
- 两个 topic 中均能观察到 20 个预期唯一序号；原始 Kafka 物理消息允许因至少一次投递而重复。
- 未知或缺失服务标签不能生成任意 topic，必须进入固定 fallback 或被拒绝并计数。
- 非目标工作负载及 Filebeat 自身日志不会进入这些业务 topic。

### 3.2 UC-001B：Kafka 日志幂等写入 Elasticsearch

**Given**

- Kafka 中存在符合最小日志契约的事件。
- `log-processor` 使用固定 consumer group 消费目标 topic。

**When**

- `log-processor` 校验、规范化事件并写入 Elasticsearch。

**Then**

- Kafka→Elasticsearch 的业务处理只经过 `log-processor`；Filebeat 不直写 Elasticsearch。
- `event_id` 由稳定字段按固定顺序和分隔规则计算。
- Elasticsearch 文档 `_id` 使用 `event_id`；重复投递不会增加唯一文档数。
- Elasticsearch 写入成功后才确认 Kafka 消息。
- 可重试错误使用有上限的退避；毒消息进入 `logs.dlq` 或 ADR 选定的等价路径，并以测试证明不会永久阻塞分区。
- `log-processor` 提供 `/healthz`、`/readyz`，以非 root 用户运行，并支持优雅终止。

### 3.3 UC-001 验收

- Filebeat 确实以 DaemonSet 运行并采集 Pod 日志目录。
- 临时 Kafka consumer 能展示 `demo-api`、`demo-worker` 的真实原始消息；不以 Filebeat 自身日志代替链路证据。
- 两个受控服务正确路由到各自的 topic，20 个预期唯一序号均可观察到。
- Kafka 事件包含验收所需的 Kubernetes 元数据。
- 在同一 `test_run_id`、声明的缓冲容量和故障窗口内，N 个逻辑唯一输入对应 N 个 Elasticsearch 唯一文档。
- 对同一事件重复投递后，唯一文档数不增加。
- `log-processor` 重启后能够继续处理。
- Kafka 短时不可用时，故障窗口、恢复时间、缓冲边界、丢失数和重复物理消息有实际测试记录。

## 4. UC-002：检索、聚合与下钻

**Given**

- Elasticsearch 中存在带唯一 `test_run_id` 的已知测试数据，至少覆盖两个服务、四种日志级别、多个时间点和一个固定错误关键词。
- Grafana 数据源和 dashboard 通过仓库文件配置。

**When**

- 用户选择时间范围、服务和关键词，或从聚合图表下钻到明细。

**Then**

- Grafana 返回匹配的日志明细。
- 页面展示日志明细、各服务日志级别占比、ERROR/WARN 时间趋势和详情下钻；这里的趋势不是 AI 异常检测。
- 明细与聚合使用同一 Elasticsearch 数据源，数量能够相互核对。
- 无匹配数据时页面可理解，不显示系统错误。
- 从空 namespace 重新部署后，数据源和 dashboard 能由仓库文件恢复。

### 4.1 UC-002 验收

- 时间、服务和关键词过滤均有固定数据集测试。
- 在相同过滤条件和时间范围下，各级别聚合之和可与明细总数核对；ERROR/WARN 趋势分桶总数可与相同级别的明细数核对。
- dashboard provisioning 可重复执行。
- 固定数据规模下重复执行查询，记录环境、p50 和 p95；若未达到 1 秒，先检查 mapping、时间范围和聚合，不把 1 秒当作未经验证的硬保证。

## 5. 最小日志契约

| 字段 | 要求 |
|---|---|
| `@timestamp` | 事件发生时间，UTC |
| `event_id` | 稳定字段计算得到的确定性 SHA-256 标识 |
| `message` | 可全文检索的日志正文 |
| `log.level` | 规范化日志级别 |
| `service.name` | 服务路由和查询维度 |
| `test_run_id` | 冒烟/性能测试批次标识；生产事件允许为空 |
| `kubernetes.namespace` | 目标 namespace |
| `kubernetes.pod.name` | Pod 名称 |
| `kubernetes.pod.uid` | Pod 稳定身份的一部分 |
| `container.id` | 容器身份 |
| `log.file.path` | 采集来源 |
| `log.offset` | 源日志位置，用于稳定标识 |
| `ingested_at` | 平台写入时间，UTC |

## 6. 非功能要求与量化口径

| 目标 | 可验证口径 | 不作出的承诺 |
|---|---|---|
| 正确性 | 在固定数据集、缓冲容量和故障窗口内，输入数等于最终唯一文档数 | 不承诺无限断网、磁盘耗尽或任意故障下绝对无丢失 |
| 幂等 | 重复投递同一事件不增加 Elasticsearch 唯一文档数 | 不宣称全链路无限条件下 exactly-once |
| 延迟 | 在声明的资源、日志大小和负载下，以 `@timestamp`→`ingested_at` 测管道摄取延迟并记录 p50/p95/p99，目标 p95 ≤ 3 秒；页面查询/刷新另测 | 不以单次截图代替统计结果，也不把摄取延迟冒充页面可视化延迟 |
| 吞吐 | 在固定本机环境中测试 1000 events/s 实验目标，记录事件大小、持续时长、资源、错误率、唯一文档数、实际达到值和瓶颈 | 不把单节点 Minikube 结果外推为生产容量或未验证的发布保证 |
| 恢复 | 验证 Pod 重建、consumer 重启和 Kafka 短时不可用后的恢复 | 不把单节点恢复测试描述成节点级或多可用区高可用 |
| 查询 | 在固定数据规模下重复测量 Grafana/Elasticsearch 查询 p50/p95 | 不把排查阈值写成生产 SLA |

Filebeat 和 Kafka 消费链路采用至少一次投递；幂等由稳定 `event_id` 与 Elasticsearch `_id` 保证。

## 7. 本轮非目标

- UC-004 PyTorch 异常检测。
- 生产级多节点 Kafka/Elasticsearch 高可用。
- 多租户、RBAC 用户系统、跨集群采集和生产级 TLS/证书体系。
- 自研 Web 前端。
- 与 Grafana 重复的 Go 查询 API。
- 为未来假设提前引入微服务框架、依赖注入框架或复杂领域分层。

UC-003、Prometheus、HPA、多分区和扩展性能加固只在 UC-001/UC-002 核心验收全绿后进入。

## 8. `v0.1.0` 完成条件

- UC-001、UC-002 的验收行为都有可重复执行的命令和结果。
- `go test -race ./...`、`go vet ./...` 和格式检查通过。
- 空 namespace 可部署、验收和清理。
- 配置、dashboard 和文档可由仓库恢复。
- Git 提交与 PR 历史可读；GitHub 仓库创建和推送由用户执行。
- 仓库中没有凭据、真实通知地址、生成日志、数据卷、二进制或本机配置。
- `docs/PROJECT_STATE.md` 指向实际最后绿灯状态和唯一下一步。
