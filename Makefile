.DEFAULT_GOAL := check

GO ?= go
GOFMT ?= gofmt
DOCKER ?= docker
MINIKUBE ?= minikube
PYTHON ?= python3
KUBECTL ?= kubectl
GO_VERSION := 1.26.5
EXPECTED_GO_VERSION := go$(GO_VERSION)
GO_FILES := $(shell find . -type f -name '*.go' -not -path './vendor/*')
SHELL_FILES := $(shell find scripts -type f -name '*.sh')
IMAGE_REPOSITORY ?= distributed-log-platform/log-producer
PROCESSOR_IMAGE_REPOSITORY ?= distributed-log-platform/log-processor
IMAGE_TAG ?= dev
KUBE_CONTEXT ?= stage3-logs
KUBE_NAMESPACE ?= stage3-logs
KUSTOMIZE_OVERLAY ?= deploy/kubernetes/overlays/local
PROCESSOR_ROLLOUT_TIMEOUT ?= 180s
PROCESSOR_ACCEPTANCE_RUN_ID ?=
KUSTOMIZE_ACCEPTANCE_OVERLAY ?= deploy/kubernetes/overlays/local-acceptance
KUSTOMIZE_THROUGHPUT_OVERLAY ?= deploy/kubernetes/overlays/local-throughput
THROUGHPUT_RUN_ID ?=
THROUGHPUT_TIMEOUT_SECONDS ?= 240
KUSTOMIZE_KAFKA_OVERLAY ?= deploy/kubernetes/overlays/local-kafka
KUSTOMIZE_KAFKA_TOPICS_OVERLAY ?= deploy/kubernetes/overlays/local-kafka-topics
KUSTOMIZE_FILEBEAT_OVERLAY ?= deploy/kubernetes/overlays/local-filebeat
KUSTOMIZE_ELASTICSEARCH_OVERLAY ?= deploy/kubernetes/overlays/local-elasticsearch
ELASTICSEARCH_INDEX_TEMPLATE ?= deploy/kubernetes/base/elasticsearch/index-template.json
KUSTOMIZE_GRAFANA_OVERLAY ?= deploy/kubernetes/overlays/local-grafana
GRAFANA_ROLLOUT_TIMEOUT ?= 180s
GRAFANA_DASHBOARD ?= deploy/kubernetes/base/grafana/dashboards/logs-overview.json
GRAFANA_ACCEPTANCE_RUN_ID ?=
GRAFANA_ACCEPTANCE_SAMPLES ?= 10
ACCEPTANCE_RUN_ID ?=
ACCEPTANCE_TIMEOUT ?= 60s
KAFKA_ROLLOUT_TIMEOUT ?= 300s
KAFKA_TOPIC_INIT_TIMEOUT ?= 300s
KAFKA_CONSUMER_INTEGRATION_TIMEOUT ?= 90s
KAFKA_CONSUMER_TEST_RUN_ID ?=
ELASTICSEARCH_ROLLOUT_TIMEOUT ?= 300s
FILEBEAT_NAMESPACE ?= stage3-collector
FILEBEAT_ROLLOUT_TIMEOUT ?= 180s
FILEBEAT_ACCEPTANCE_TIMEOUT ?= 120s
FILEBEAT_ACCEPTANCE_SETTLE_SECONDS ?= 5
FILEBEAT_ACCEPTANCE_MAX_RECORDS ?= 5000
FILEBEAT_FALLBACK_TIMEOUT ?= 120s
FILEBEAT_RECOVERY_TIMEOUT ?= 180s
FILEBEAT_RECOVERY_SETTLE_SECONDS ?= 20
FILEBEAT_OUTAGE_TIMEOUT ?= 300s
FILEBEAT_OUTAGE_SETTLE_SECONDS ?= 20
FILEBEAT_OUTAGE_PROBE_TIMEOUT ?= 45s
FILEBEAT_VERSION ?= 9.4.4
FILEBEAT_LOCAL_IMAGE ?= docker.elastic.co/beats/filebeat-wolfi:9.4.4

# 第三方组件保留明确版本，自研组件使用目标机器重新构建的 dev 镜像。
# Minikube 导入产生的机器相关摘要不进入仓库，也不作为部署前置条件。
KAFKA_NODE_IMAGE ?= docker.io/apache/kafka:4.3.1
FILEBEAT_NODE_IMAGE ?= docker.elastic.co/beats/filebeat-wolfi:9.4.4
ELASTICSEARCH_NODE_IMAGE ?= docker.io/library/elasticsearch:9.4.4
GRAFANA_NODE_IMAGE ?= docker.io/grafana/grafana:13.1.0
PROCESSOR_NODE_IMAGE ?= docker.io/distributed-log-platform/log-processor:dev

export MINIKUBE KUBECTL KUBE_CONTEXT KUBE_NAMESPACE KAFKA_NODE_IMAGE
export KUSTOMIZE_KAFKA_TOPICS_OVERLAY KAFKA_TOPIC_INIT_TIMEOUT
export KAFKA_CONSUMER_INTEGRATION_TIMEOUT KAFKA_CONSUMER_TEST_RUN_ID
export DOCKER KUSTOMIZE_FILEBEAT_OVERLAY FILEBEAT_NAMESPACE FILEBEAT_ROLLOUT_TIMEOUT FILEBEAT_LOCAL_IMAGE
export FILEBEAT_ACCEPTANCE_TIMEOUT FILEBEAT_ACCEPTANCE_SETTLE_SECONDS FILEBEAT_ACCEPTANCE_MAX_RECORDS FILEBEAT_VERSION
export FILEBEAT_FALLBACK_TIMEOUT FILEBEAT_RECOVERY_TIMEOUT FILEBEAT_RECOVERY_SETTLE_SECONDS
export FILEBEAT_OUTAGE_TIMEOUT FILEBEAT_OUTAGE_SETTLE_SECONDS FILEBEAT_OUTAGE_PROBE_TIMEOUT
export FILEBEAT_NODE_IMAGE
export KUSTOMIZE_ELASTICSEARCH_OVERLAY ELASTICSEARCH_ROLLOUT_TIMEOUT ELASTICSEARCH_NODE_IMAGE
export KUSTOMIZE_GRAFANA_OVERLAY GRAFANA_ROLLOUT_TIMEOUT GRAFANA_NODE_IMAGE
export GRAFANA_ACCEPTANCE_RUN_ID GRAFANA_ACCEPTANCE_SAMPLES
export KUSTOMIZE_THROUGHPUT_OVERLAY THROUGHPUT_RUN_ID THROUGHPUT_TIMEOUT_SECONDS

.PHONY: check version-check fmt fmt-check shell-check log-producer-validator-test filebeat-validator-test grafana-query-validator-test vet test build validate-image-tag image processor-image k8s-images k8s-load-images \
	k8s-context-check k8s-render k8s-validate k8s-processor-image-check \
	k8s-processor-runtime-check k8s-processor-acceptance k8s-deploy k8s-status \
	k8s-acceptance-render k8s-acceptance k8s-kafka-render k8s-kafka-validate k8s-kafka-image-check \
	k8s-kafka-runtime-check k8s-kafka-deploy k8s-kafka-status k8s-kafka-topics-render \
	k8s-kafka-topics-validate k8s-kafka-topics k8s-kafka-topics-status kafka-topic-initializer-test \
	kafka-consumer-integration \
	k8s-elasticsearch-render k8s-elasticsearch-validate k8s-elasticsearch-image-check \
	k8s-elasticsearch-runtime-check k8s-elasticsearch-deploy k8s-elasticsearch-status k8s-elasticsearch-template \
	k8s-grafana-render k8s-grafana-validate k8s-grafana-image-check k8s-grafana-runtime-check \
	k8s-grafana-deploy k8s-grafana-status k8s-grafana-acceptance \
	filebeat-config-check k8s-filebeat-render k8s-filebeat-validate k8s-filebeat-image-check \
	k8s-filebeat-runtime-check k8s-filebeat-deploy k8s-filebeat-status k8s-filebeat-acceptance \
	k8s-filebeat-fallback-acceptance k8s-filebeat-registry-recovery k8s-filebeat-kafka-outage-recovery perf

# check 聚合所有只读工程门禁，适合提交前和持续集成调用。
check: version-check fmt-check shell-check log-producer-validator-test filebeat-validator-test grafana-query-validator-test vet test build kafka-topic-initializer-test

# version-check 保证本地命令使用仓库约定的 Go 工具链。
version-check:
	@actual="$$($(GO) env GOVERSION)"; \
	if [ "$$actual" != "$(EXPECTED_GO_VERSION)" ]; then \
		echo "Go 版本不匹配：实际 $$actual，要求 $(EXPECTED_GO_VERSION)"; \
		exit 1; \
	fi

# fmt 显式修改仓库中的 Go 源文件。
fmt:
	$(GOFMT) -w $(GO_FILES)

# fmt-check 仅报告格式问题，不修改工作区。
fmt-check:
	@files="$$($(GOFMT) -l $(GO_FILES))"; \
	status=$$?; \
	if [ "$$status" -ne 0 ]; then \
		echo "Go 格式检查执行失败"; \
		exit "$$status"; \
	fi; \
	if [ -n "$$files" ]; then \
		echo "以下 Go 文件需要格式化："; \
		echo "$$files"; \
		exit 1; \
	fi

# shell-check 只做语法门禁，不引入 Docker 或 Kubernetes 依赖。
shell-check:
	bash -n $(SHELL_FILES)

# Filebeat Kafka 校验器使用纯标准库 fixture 覆盖成功、未收齐与契约错误分支。
log-producer-validator-test:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m unittest scripts.test_validate_log_producer_output

filebeat-validator-test:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m unittest scripts.test_validate_filebeat_kafka_output

grafana-query-validator-test:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m unittest scripts.test_verify_grafana_queries

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

# build 单独验证 main 包能够完成链接，不生成仓库内二进制。
build:
	$(GO) build -o /dev/null ./cmd/log-producer
	$(GO) build -o /dev/null ./cmd/log-processor

# kafka-topic-initializer-test 纯本地验证 Kafka 4.3.1 文本解析的正反例，不连接集群。
kafka-topic-initializer-test:
	@bash deploy/kubernetes/base/kafka-topics/initialize-topics.sh self-test

# kafka-consumer-integration 验证续读及真实 Runner→Elasticsearch/DLQ 链路，并清理唯一临时资源。
kafka-consumer-integration: version-check k8s-context-check
	@scripts/run-kafka-consumer-integration.sh

# 两个自研镜像共用同一标签门禁，避免生成 latest 或来源不明的任意标签。
validate-image-tag:
	@if [ -z "$(strip $(IMAGE_TAG))" ]; then \
		echo "镜像标签不能为空"; \
		exit 1; \
	fi
	@if [ "$(IMAGE_TAG)" != "dev" ]; then \
		expected_tag="$$(git rev-parse --short HEAD)"; \
		if [ "$(IMAGE_TAG)" != "$$expected_tag" ]; then \
			echo "非 dev 镜像标签必须等于当前提交短 SHA：期望 $$expected_tag，实际 $(IMAGE_TAG)"; \
			exit 1; \
		fi; \
		if [ -n "$$(git status --porcelain --untracked-files=normal)" ]; then \
			echo "提交镜像标签要求 Git 工作区干净：$(IMAGE_TAG)"; \
			exit 1; \
		fi; \
	fi

image: validate-image-tag
	$(DOCKER) build \
		--build-arg GO_VERSION=$(GO_VERSION) \
		--target log-producer \
		--tag $(IMAGE_REPOSITORY):$(IMAGE_TAG) \
		.

# processor-image 构建独立处理器镜像，不改变既有 log-producer 镜像入口。
processor-image: validate-image-tag
	$(DOCKER) build \
		--build-arg GO_VERSION=$(GO_VERSION) \
		--target log-processor \
		--tag $(PROCESSOR_IMAGE_REPOSITORY):$(IMAGE_TAG) \
		.

# k8s-images 在目标机器准备全部本地演示镜像；重复执行会复用 Docker 缓存。
k8s-images: k8s-context-check
	@$(MAKE) --no-print-directory image processor-image IMAGE_TAG=dev
	@$(DOCKER) image inspect $(KAFKA_NODE_IMAGE) >/dev/null 2>&1 || $(DOCKER) pull $(KAFKA_NODE_IMAGE)
	@$(DOCKER) image inspect $(FILEBEAT_NODE_IMAGE) >/dev/null 2>&1 || $(DOCKER) pull $(FILEBEAT_NODE_IMAGE)
	@$(DOCKER) image inspect $(ELASTICSEARCH_NODE_IMAGE) >/dev/null 2>&1 || $(DOCKER) pull $(ELASTICSEARCH_NODE_IMAGE)
	@$(DOCKER) image inspect $(GRAFANA_NODE_IMAGE) >/dev/null 2>&1 || $(DOCKER) pull $(GRAFANA_NODE_IMAGE)
	@$(MAKE) --no-print-directory k8s-load-images

# k8s-load-images 只加载本机已有镜像，供离线迁移包恢复时使用。
k8s-load-images: k8s-context-check
	$(MINIKUBE) image load -p $(KUBE_CONTEXT) --overwrite=true $(IMAGE_REPOSITORY):dev
	$(MINIKUBE) image load -p $(KUBE_CONTEXT) --overwrite=true $(PROCESSOR_IMAGE_REPOSITORY):dev
	$(MINIKUBE) image load -p $(KUBE_CONTEXT) --overwrite=true $(KAFKA_NODE_IMAGE)
	$(MINIKUBE) image load -p $(KUBE_CONTEXT) --overwrite=true $(FILEBEAT_NODE_IMAGE)
	$(MINIKUBE) image load -p $(KUBE_CONTEXT) --overwrite=true $(ELASTICSEARCH_NODE_IMAGE)
	$(MINIKUBE) image load -p $(KUBE_CONTEXT) --overwrite=true $(GRAFANA_NODE_IMAGE)
	@echo "本地演示镜像已加载到 $(KUBE_CONTEXT)"

# k8s-context-check 在任何集群写操作前确认当前上下文，防止误操作其他集群。
k8s-context-check:
	@actual="$$($(KUBECTL) config current-context)"; \
	status=$$?; \
	if [ "$$status" -ne 0 ]; then \
		echo "读取 Kubernetes 当前上下文失败"; \
		exit "$$status"; \
	fi; \
	if [ "$$actual" != "$(KUBE_CONTEXT)" ]; then \
		echo "Kubernetes 上下文不匹配：实际 $$actual，要求 $(KUBE_CONTEXT)"; \
		exit 1; \
	fi

# k8s-render 只渲染最终清单，不连接或修改集群。
k8s-render:
	@$(KUBECTL) kustomize $(KUSTOMIZE_OVERLAY)

# k8s-validate 使用目标 API Server 校验最终清单，但不持久化资源。
k8s-validate: k8s-context-check
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		--dry-run=server \
		-k $(KUSTOMIZE_OVERLAY)

# k8s-processor-image-check 确认目标机器已经加载本地处理器镜像。
k8s-processor-image-check: k8s-context-check
	@$(MINIKUBE) ssh -p $(KUBE_CONTEXT) -- \
		sudo crictl inspecti '$(PROCESSOR_NODE_IMAGE)' >/dev/null 2>&1 || { \
		echo "Minikube 节点缺少 log-processor 镜像：$(PROCESSOR_NODE_IMAGE)"; \
		exit 1; \
	}
	@echo "log-processor 节点镜像已就绪：$(PROCESSOR_NODE_IMAGE)"

# k8s-processor-runtime-check 确认唯一处理器 Pod 使用本地 dev 镜像并已经就绪。
k8s-processor-runtime-check: k8s-context-check
	@pod_count="$$($(KUBECTL) --context=$(KUBE_CONTEXT) get pods \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=log-processor \
		-o name | wc -l | tr -d ' ')"; \
	if [ "$$pod_count" != "1" ]; then \
		echo "log-processor Pod 数量为 $$pod_count，要求 1"; \
		exit 1; \
	fi; \
	spec_image="$$($(KUBECTL) --context=$(KUBE_CONTEXT) get pods \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=log-processor \
		-o 'jsonpath={.items[0].spec.containers[?(@.name=="log-processor")].image}')"; \
	if [ "$$spec_image" != "$(PROCESSOR_NODE_IMAGE)" ]; then \
		echo "log-processor Pod 镜像版本异常：实际 $${spec_image:-缺失}，要求 $(PROCESSOR_NODE_IMAGE)"; \
		exit 1; \
	fi; \
	runtime_facts="$$($(KUBECTL) --context=$(KUBE_CONTEXT) get pods \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=log-processor \
		-o 'jsonpath={.items[0].status.containerStatuses[?(@.name=="log-processor")].imageID}{"|"}{.items[0].status.containerStatuses[?(@.name=="log-processor")].ready}')"; \
	runtime_image_id="$${runtime_facts%%|*}"; \
	runtime_ready="$${runtime_facts##*|}"; \
	if [ -z "$$runtime_image_id" ] || [ "$$runtime_ready" != "true" ]; then \
		echo "log-processor Pod 尚未使用本地镜像就绪：$${runtime_facts:-缺失}"; \
		exit 1; \
	fi; \
	echo "log-processor Pod 镜像版本通过：$(PROCESSOR_NODE_IMAGE)"

# k8s-processor-acceptance 验证部署幂等、SIGTERM 就绪撤销和消费者组续读。
k8s-processor-acceptance: k8s-processor-image-check k8s-processor-runtime-check
	@PROCESSOR_ACCEPTANCE_RUN_ID='$(PROCESSOR_ACCEPTANCE_RUN_ID)' \
		scripts/run-processor-deployment-acceptance.sh

# perf 运行两条真实日志源到 Elasticsearch 的固定 60 秒硬性吞吐验收。
perf: k8s-processor-image-check k8s-processor-runtime-check
	@THROUGHPUT_RUN_ID='$(THROUGHPUT_RUN_ID)' \
		THROUGHPUT_TIMEOUT_SECONDS='$(THROUGHPUT_TIMEOUT_SECONDS)' \
		scripts/run-throughput-acceptance.sh

# k8s-deploy 通过准入和镜像存在性检查后部署应用，并重启 Pod 使用最新本地构建。
k8s-deploy: k8s-validate k8s-processor-image-check
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		-k $(KUSTOMIZE_OVERLAY)
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		rollout restart \
		deployment/api-service \
		deployment/worker-service \
		deployment/log-processor \
		-n $(KUBE_NAMESPACE)
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		rollout status \
		deployment/api-service \
		-n $(KUBE_NAMESPACE) \
		--timeout=$(PROCESSOR_ROLLOUT_TIMEOUT)
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		rollout status \
		deployment/worker-service \
		-n $(KUBE_NAMESPACE) \
		--timeout=$(PROCESSOR_ROLLOUT_TIMEOUT)
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		rollout status \
		deployment/log-processor \
		-n $(KUBE_NAMESPACE) \
		--timeout=$(PROCESSOR_ROLLOUT_TIMEOUT)
	@$(MAKE) --no-print-directory k8s-processor-runtime-check

k8s-status:
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		get deployments,pods \
		-n $(KUBE_NAMESPACE) \
		-l 'app.kubernetes.io/name in (log-producer,log-processor)' \
		-o wide

# k8s-kafka-render 只渲染独立的 Kafka overlay，不连接或修改集群。
k8s-kafka-render:
	@$(KUBECTL) kustomize $(KUSTOMIZE_KAFKA_OVERLAY)

# k8s-kafka-validate 使用目标 API Server 校验 Kafka 清单，但不创建资源。
k8s-kafka-validate: k8s-context-check
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		--dry-run=server \
		-k $(KUSTOMIZE_KAFKA_OVERLAY)

# k8s-kafka-image-check 确认明确版本的 Kafka 镜像已经旁加载。
k8s-kafka-image-check: k8s-context-check
	@scripts/verify-kafka-image.sh node

# k8s-kafka-runtime-check 确认主容器和初始化容器使用 Kafka 4.3.1。
k8s-kafka-runtime-check: k8s-context-check
	@scripts/verify-kafka-image.sh pod

# k8s-kafka-deploy 与应用部署解耦，并在应用前后核对明确版本。
k8s-kafka-deploy: k8s-kafka-validate k8s-kafka-image-check
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		-k $(KUSTOMIZE_KAFKA_OVERLAY)
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		rollout status \
		statefulset/kafka \
		-n $(KUBE_NAMESPACE) \
		--timeout=$(KAFKA_ROLLOUT_TIMEOUT)
	@scripts/verify-kafka-image.sh pod

k8s-kafka-status:
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		get statefulsets,pods,services,persistentvolumeclaims \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=kafka \
		-o wide

# k8s-kafka-topics-render 只渲染一次性主题初始化资源，不包含 Broker 或命名空间。
k8s-kafka-topics-render:
	@$(KUBECTL) kustomize $(KUSTOMIZE_KAFKA_TOPICS_OVERLAY)

# k8s-kafka-topics-validate 校验资源边界与服务端准入，全程不写入集群。
k8s-kafka-topics-validate: k8s-context-check
	@scripts/run-kafka-topic-initializer.sh validate

# k8s-kafka-topics 安全重建固定名 Job；runner 自身在写入前执行节点镜像门禁。
k8s-kafka-topics: k8s-kafka-topics-validate
	@scripts/run-kafka-topic-initializer.sh run

k8s-kafka-topics-status:
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		get jobs,pods,configmaps \
		-n $(KUBE_NAMESPACE) \
		-l distributed-log-platform.io/purpose=topic-initialization \
		-o wide

# k8s-elasticsearch-render 只渲染单节点 Elasticsearch，不连接或修改集群。
k8s-elasticsearch-render:
	@$(KUBECTL) kustomize $(KUSTOMIZE_ELASTICSEARCH_OVERLAY)

# k8s-elasticsearch-validate 使用目标 API Server 校验清单，但不创建资源。
k8s-elasticsearch-validate: k8s-context-check
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		--dry-run=server \
		-k $(KUSTOMIZE_ELASTICSEARCH_OVERLAY)

# k8s-elasticsearch-image-check 确认明确版本的 Elasticsearch 镜像已经旁加载。
k8s-elasticsearch-image-check: k8s-context-check
	@scripts/verify-elasticsearch-image.sh node

# k8s-elasticsearch-runtime-check 核对主容器和配置初始化容器的镜像版本。
k8s-elasticsearch-runtime-check: k8s-context-check
	@scripts/verify-elasticsearch-image.sh pod

k8s-elasticsearch-deploy: k8s-elasticsearch-validate k8s-elasticsearch-image-check
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		-k $(KUSTOMIZE_ELASTICSEARCH_OVERLAY)
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		rollout status \
		statefulset/elasticsearch \
		-n $(KUBE_NAMESPACE) \
		--timeout=$(ELASTICSEARCH_ROLLOUT_TIMEOUT)
	@scripts/verify-elasticsearch-image.sh pod

k8s-elasticsearch-status:
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		get statefulsets,pods,services,persistentvolumeclaims \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=elasticsearch \
		-o wide

# 模板从仓库经标准输入发送给 Pod 内 curl；入口幂等覆盖同名模板并设置明确超时。
k8s-elasticsearch-template: k8s-elasticsearch-runtime-check
	@test -s "$(ELASTICSEARCH_INDEX_TEMPLATE)" || { \
		echo "Elasticsearch 索引模板不存在或为空：$(ELASTICSEARCH_INDEX_TEMPLATE)"; \
		exit 1; \
	}
	@response="$$($(KUBECTL) --context=$(KUBE_CONTEXT) exec pod/elasticsearch-0 \
		-i -n $(KUBE_NAMESPACE) -c elasticsearch -- \
		curl --silent --show-error --fail-with-body \
		--connect-timeout 5 \
		--max-time 30 \
		--request PUT \
		--header 'Content-Type: application/json' \
		--data-binary @- \
		http://127.0.0.1:9200/_index_template/logs-stage3-v1 \
		< "$(ELASTICSEARCH_INDEX_TEMPLATE)")"; \
	if [ "$$response" != '{"acknowledged":true}' ]; then \
		echo "Elasticsearch 索引模板响应异常：$$response"; \
		exit 1; \
	fi; \
	echo "Elasticsearch 索引模板已确认：logs-stage3-v1"

# k8s-grafana-render 只渲染声明式数据源、仪表盘和单实例部署，不连接集群。
k8s-grafana-render:
	@$(KUBECTL) kustomize $(KUSTOMIZE_GRAFANA_OVERLAY)

# k8s-grafana-validate 先解析仪表盘 JSON，再使用目标 API Server 校验资源但不持久化。
k8s-grafana-validate: k8s-context-check
	@$(PYTHON) -m json.tool $(GRAFANA_DASHBOARD) >/dev/null
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		--dry-run=server \
		-k $(KUSTOMIZE_GRAFANA_OVERLAY)

# k8s-grafana-image-check 确认明确版本的 Grafana 镜像已经旁加载。
k8s-grafana-image-check: k8s-context-check
	@$(MINIKUBE) ssh -p $(KUBE_CONTEXT) -- \
		sudo crictl inspecti '$(GRAFANA_NODE_IMAGE)' >/dev/null 2>&1 || { \
		echo "Minikube 节点缺少 Grafana 镜像：$(GRAFANA_NODE_IMAGE)"; \
		exit 1; \
	}
	@echo "Grafana 节点镜像已就绪：$(GRAFANA_NODE_IMAGE)"

# k8s-grafana-runtime-check 核对 Pod 声明版本和就绪状态。
k8s-grafana-runtime-check: k8s-context-check
	@pod_count="$$($(KUBECTL) --context=$(KUBE_CONTEXT) get pods \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=grafana \
		-o name | wc -l | tr -d ' ')"; \
	if [ "$$pod_count" != "1" ]; then \
		echo "Grafana Pod 数量为 $$pod_count，要求 1"; \
		exit 1; \
	fi; \
	spec_image="$$($(KUBECTL) --context=$(KUBE_CONTEXT) get deployment/grafana \
		-n $(KUBE_NAMESPACE) \
		-o 'jsonpath={.spec.template.spec.containers[?(@.name=="grafana")].image}')"; \
	if [ "$$spec_image" != "$(GRAFANA_NODE_IMAGE)" ]; then \
		echo "Grafana 声明镜像不匹配：实际 $${spec_image:-缺失}，要求 $(GRAFANA_NODE_IMAGE)"; \
		exit 1; \
	fi; \
	runtime_facts="$$($(KUBECTL) --context=$(KUBE_CONTEXT) get pods \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=grafana \
		-o 'jsonpath={.items[0].status.containerStatuses[?(@.name=="grafana")].imageID}{"|"}{.items[0].status.containerStatuses[?(@.name=="grafana")].ready}')"; \
	runtime_image_id="$${runtime_facts%%|*}"; \
	runtime_ready="$${runtime_facts##*|}"; \
	if [ -z "$$runtime_image_id" ] || [ "$$runtime_ready" != "true" ]; then \
		echo "Grafana Pod 尚未使用该版本镜像就绪：$${runtime_facts:-缺失}"; \
		exit 1; \
	fi; \
	echo "Grafana Pod 镜像版本通过：$(GRAFANA_NODE_IMAGE)"

k8s-grafana-deploy: k8s-grafana-validate k8s-grafana-image-check
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		-k $(KUSTOMIZE_GRAFANA_OVERLAY)
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		rollout status \
		deployment/grafana \
		-n $(KUBE_NAMESPACE) \
		--timeout=$(GRAFANA_ROLLOUT_TIMEOUT)
	@$(MAKE) --no-print-directory k8s-grafana-runtime-check

k8s-grafana-status:
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		get deployments,pods,services \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=grafana \
		-o wide

# k8s-grafana-acceptance 验证固定数据集过滤、聚合、查询延迟和临时 SQLite 重建恢复。
k8s-grafana-acceptance: k8s-grafana-image-check k8s-grafana-runtime-check
	@$(PYTHON) scripts/verify_grafana_queries.py

# filebeat-config-check 使用固定官方镜像验证配置，并包含一个非法协议版本反例。
filebeat-config-check:
	@scripts/check-filebeat-config.sh

# k8s-filebeat-render 渲染跨 stage3-collector/stage3-logs 的独立采集 overlay。
k8s-filebeat-render:
	@$(KUBECTL) kustomize $(KUSTOMIZE_FILEBEAT_OVERLAY)

# k8s-filebeat-validate 精确校验资源、RBAC、挂载、安全上下文和服务端准入，不写集群。
k8s-filebeat-validate: k8s-context-check
	@scripts/run-filebeat-deployment.sh validate

# k8s-filebeat-image-check 确认明确版本的 Filebeat 镜像已经旁加载。
k8s-filebeat-image-check: k8s-context-check
	@scripts/verify-filebeat-image.sh node

# k8s-filebeat-runtime-check 核对全部 DaemonSet Pod 的镜像版本和就绪状态。
k8s-filebeat-runtime-check: k8s-context-check
	@scripts/verify-filebeat-image.sh pod

# k8s-filebeat-deploy 使用自包含 runner 执行写前门禁、部署和运行后验证。
k8s-filebeat-deploy: k8s-filebeat-validate
	@scripts/run-filebeat-deployment.sh run

k8s-filebeat-status:
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		get daemonsets,pods,configmaps,serviceaccounts \
		-n $(FILEBEAT_NAMESPACE) \
		-l distributed-log-platform.io/purpose=log-collection \
		-o wide

	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		get roles,rolebindings \
		-n $(KUBE_NAMESPACE) \
		-l distributed-log-platform.io/purpose=log-collection \
		-o wide

# k8s-filebeat-acceptance 复用固定批次 Job，并按 Kafka 有界位点验证采集、元数据和路由。
k8s-filebeat-acceptance: export ACCEPTANCE_RUN_ID := $(ACCEPTANCE_RUN_ID)
k8s-filebeat-acceptance: k8s-context-check k8s-filebeat-runtime-check
	@scripts/run-filebeat-acceptance.sh

# k8s-filebeat-fallback-acceptance 注入一条无 API 元数据的临时节点日志并验证未分类兜底。
k8s-filebeat-fallback-acceptance: k8s-context-check k8s-filebeat-runtime-check
	@scripts/run-filebeat-fallback-acceptance.sh

# k8s-filebeat-registry-recovery 重建采集器 Pod，证明宿主 registry 防止旧批次回放且新批次仍可到达。
k8s-filebeat-registry-recovery: k8s-context-check k8s-filebeat-runtime-check
	@scripts/run-filebeat-registry-recovery.sh

# k8s-filebeat-kafka-outage-recovery 临时缩容单 Broker，证明有界短停窗口内的积压会在恢复后补投。
k8s-filebeat-kafka-outage-recovery: k8s-context-check k8s-kafka-runtime-check k8s-filebeat-runtime-check
	@scripts/run-filebeat-kafka-outage-recovery.sh

# k8s-acceptance-render 只渲染两个一次性 Job；批次 ID 必须由运行入口注入。
k8s-acceptance-render:
	@$(KUBECTL) kustomize $(KUSTOMIZE_ACCEPTANCE_OVERLAY)

# k8s-acceptance 生成或接收唯一批次 ID，重建两个已终止 Job 并验证各 20 条日志。
k8s-acceptance: export ACCEPTANCE_RUN_ID := $(ACCEPTANCE_RUN_ID)
k8s-acceptance: export ACCEPTANCE_TIMEOUT := $(ACCEPTANCE_TIMEOUT)
k8s-acceptance: export KUBECTL := $(KUBECTL)
k8s-acceptance: export PYTHON := $(PYTHON)
k8s-acceptance: export KUBE_CONTEXT := $(KUBE_CONTEXT)
k8s-acceptance: export KUBE_NAMESPACE := $(KUBE_NAMESPACE)
k8s-acceptance: export KUSTOMIZE_ACCEPTANCE_OVERLAY := $(KUSTOMIZE_ACCEPTANCE_OVERLAY)
k8s-acceptance: k8s-context-check
	@scripts/run-log-producer-acceptance.sh
