# Repository Guidance

## 目标

- 在 WSL2 的独立 `stage3-logs` Minikube 环境中，按用例构建小而可观测、可测试、可复现的分布式日志平台。
- `v0.1.0` 必须完成 UC-001 日志链路和 UC-002 Grafana 检索、聚合与下钻。

## 范围

- 核心链路固定为：Pod stdout → Filebeat → Kafka → Go `log-processor` → Elasticsearch → Grafana。
- UC-001/UC-002 验收全绿后才做 UC-003 或 `v0.2.0` 加固；UC-004、生产级多节点高可用、多租户/RBAC、自研前端和重复查询 API 不在本轮范围。
- 本仓库独立实现，不扫描、复制或依赖其他项目的代码、历史或目录结构。

## 工程约束

- Filebeat 不直写 Elasticsearch；Kafka→Elasticsearch 只由 `log-processor` 处理；Grafana 在 `v0.1.0` 直查 Elasticsearch。
- 投递语义为至少一次：使用确定性 `event_id` 作为 Elasticsearch `_id`，写入成功后才确认 Kafka 消息，不宣称无限条件下 exactly-once。
- `go.mod` 优先声明 Go 1.22；标准库优先，使用 `log/slog`、`net/http`；接口只放外部边界，配置启动时校验，后台任务支持 context 取消和优雅退出。
- 使用 `stage3-logs` profile、独立 namespace、Kustomize 和固定镜像版本；禁止 `latest`，不提交真实 Secret，不修改项目 namespace 以外的集群资源。
- 自研 Deployment 必须配置 probes、资源 request/limit 和优雅终止；Filebeat 只采集目标工作负载并排除自身及基础设施日志。
- 使用 `main`、`develop` 和聚焦的 `feature/*` 分支，采用 Conventional Commits；不提交凭据、日志、数据卷、二进制或本机配置。

## 验证要求

- 每项完成声明都要附可复现的命令、输入、输出和结果；不能用“看起来可用”代替证据。
- 修改后运行与风险相称的最小检查；基础入口为 `make fmt-check`、`make vet`、`make test`。
- 集成、E2E、恢复和性能入口只在能力真实实现后加入，禁止永远成功的占位检查。
- 完成要求：验收行为已验证、相关测试通过、配置可复现、状态文档已更新且 diff 不含无关修改。

## 协作方式

- 新对话或压缩后先读 `AGENTS.md`、`docs/PLAN.md`、`docs/PROJECT_STATE.md` 和相关 ADR，再核对 Git、Kubernetes context 与最快相关验证；仓库事实优先于聊天记忆。
- 默认导师模式：每次推进一个可验证阶段，给一组相关命令并说明目的、预期和异常分支；审查证据后再继续。只有用户明确要求时才直接实现。
- 修改前说明文件范围和预期行为；一次只处理一个用例或一个失败测试，不顺手重构无关内容。
- 重要验证后、每天结束时及上下文压缩前重写 `docs/PROJECT_STATE.md`，只保留当前事实、证据、阻塞、未验证假设和唯一下一步。
- Codex 不创建或推送 GitHub 仓库，也不执行 merge、发布或外部服务变更；远端动作由用户决定并执行。
