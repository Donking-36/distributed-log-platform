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

- 尚未部署 Kafka、Filebeat，也未验证主题路由或 Kubernetes 元数据补充。
- Job stdout 的“精确 20 行”不等同于后续 Kafka 至少一次投递的物理消息数量；
  Kafka 阶段必须按唯一序号和 `test_run_id` 单独验收。
- 原始日志只用于本次校验，不提交仓库；报告保存可重复命令、摘要和校验和。
