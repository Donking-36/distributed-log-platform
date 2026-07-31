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

Kafka、Filebeat、Elasticsearch 和 Grafana 的镜像版本暂不选定，需先通过
兼容性冒烟测试。任何部署清单都不得使用 `latest`。

## 当前可运行组件

`log-producer` 是应用日志来源，只向标准输出写入一行一个 JSON 对象，不直接
连接 Kafka 或 Elasticsearch。Filebeat 后续负责采集这些日志并生产到 Kafka。

| 环境变量 | 必填 | 默认值 | 作用 |
|---|---|---|---|
| `PRODUCER_SERVICE_NAME` | 是 | 无 | Kubernetes 中由 Pod `service` 标签通过 Downward API 注入，写入原始 `service.name` |
| `PRODUCER_TEST_RUN_ID` | 是 | 无 | 标识一次可重复验收批次 |
| `PRODUCER_COUNT` | 否 | `20` | 本次生成的事件数量，必须大于零 |

本地生成两条日志：

```bash
PRODUCER_SERVICE_NAME=api-service \
PRODUCER_TEST_RUN_ID=local-001 \
PRODUCER_COUNT=2 \
go run ./cmd/log-producer
```

## 项目文档

- [`docs/requirements.md`](docs/requirements.md)：UC-001/UC-002 的范围、日志契约和验收要求。
- [`docs/architecture.md`](docs/architecture.md)：组件职责、部署拓扑和兼容性门禁。
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
```

`make check` 聚合 Go 1.26.5 版本、格式、静态检查、测试和构建门禁，且不会
修改工作区。需要主动格式化代码时执行 `make fmt`。

## 开发流程

- 长期分支：`main`、`develop`。
- 当前启动工作分支：`feature/bootstrap`。
- 使用目标单一的 `feature/*` 分支和约定式提交。
- 只为真实行为添加测试和命令目标，不建立永远成功的占位检查。
- 远端仓库为 `https://github.com/Donking-36/distributed-log-platform.git`。

## 当前状态

环境、容量门禁、需求、架构、ADR 和 Go 模块基线已经完成。`log-producer`
已经实现配置加载、确定性 JSON 输出和可测试入口，并通过单元测试、静态检查、
构建与本地运行冒烟。最小持续集成工作流已配置为执行 `make check`，托管
运行待首次推送后验证；容器镜像和 Kubernetes 部署清单尚未创建。
