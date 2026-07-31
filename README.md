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

## 开发流程

- 长期分支：`main`、`develop`。
- 当前启动工作分支：`feature/bootstrap`。
- 使用目标单一的 `feature/*` 分支和约定式提交。
- 只为真实行为添加测试和命令目标，不建立永远成功的占位检查。
- 远端仓库为 `https://github.com/Donking-36/distributed-log-platform.git`。

## 当前状态

环境、容量门禁、需求、架构、ADR 和 Go 模块基线已经完成。
`feature/bootstrap` 已推送到远端。应用代码、Makefile 目标、持续集成和部署
清单尚未创建。
