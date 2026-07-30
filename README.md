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

## 首先阅读

1. [`AGENTS.md`](AGENTS.md) — 长期有效的工程与协作规则。
2. [`docs/PLAN.md`](docs/PLAN.md) — 完整范围和分阶段实施计划。
3. [`docs/PROJECT_STATE.md`](docs/PROJECT_STATE.md) — 最后验证状态和唯一下一步。
4. [`docs/requirements.md`](docs/requirements.md) — UC-001/UC-002 验收要求。
5. [`docs/architecture.md`](docs/architecture.md) 和
   [`docs/adr/`](docs/adr/) — 架构和已接受的决策。

## 安全恢复工作

```bash
cd ~/projects/distributed-log-platform
git status --short --branch
git log -5 --oneline --decorate
kubectl config current-context
kubectl --context=stage3-logs get nodes
```

不得修改其他 Kubernetes 上下文。若 Minikube 重建其外层容器，应重新核验
`docs/PROJECT_STATE.md` 中记录的 4 CPU/6 GiB 限制。

## 开发流程

- 长期分支：`main`、`develop`。
- 当前启动工作分支：`feature/bootstrap`。
- 使用目标单一的 `feature/*` 分支和约定式提交。
- 只为真实行为添加测试和命令目标，不建立永远成功的占位检查。
- GitHub 仓库创建和推送由用户执行。

## 当前状态

环境、容量门禁、需求、架构和 ADR 基线已经完成。远端仓库由用户创建，但本地
尚未配置 `origin`，也没有推送任何内容。应用代码、Makefile 目标、持续集成和部署
清单尚未创建。
