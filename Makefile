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
IMAGE_REPOSITORY ?= distributed-log-platform/log-producer
IMAGE_TAG ?= dev
KUBE_CONTEXT ?= stage3-logs
KUBE_NAMESPACE ?= stage3-logs
KUSTOMIZE_OVERLAY ?= deploy/kubernetes/overlays/local
KUSTOMIZE_ACCEPTANCE_OVERLAY ?= deploy/kubernetes/overlays/local-acceptance
KUSTOMIZE_KAFKA_OVERLAY ?= deploy/kubernetes/overlays/local-kafka
KUSTOMIZE_KAFKA_TOPICS_OVERLAY ?= deploy/kubernetes/overlays/local-kafka-topics
ACCEPTANCE_RUN_ID ?=
ACCEPTANCE_TIMEOUT ?= 60s
KAFKA_ROLLOUT_TIMEOUT ?= 300s
KAFKA_TOPIC_INIT_TIMEOUT ?= 300s

# 这些值与 local-kafka overlay 构成同一镜像身份基线，更新时必须连同证据一起修改。
override KAFKA_NODE_IMAGE := docker.io/apache/kafka:4.3.1
override EXPECTED_KAFKA_MANIFEST_DIGEST := sha256:f8f865a3222d807cf1e6c515ca447cb2fb604ddc57f0a007a02a9ce79bd7a511
override EXPECTED_KAFKA_CONFIG_DIGEST := sha256:47dccc76b32761bc57462b8753144cdbb73a16b123b1d13d3eedb92bb7952b11

# 镜像校验脚本只读取显式导出的项目参数，不自行维护另一份摘要常量。
export MINIKUBE KUBECTL KUBE_CONTEXT KUBE_NAMESPACE KAFKA_NODE_IMAGE
export EXPECTED_KAFKA_MANIFEST_DIGEST EXPECTED_KAFKA_CONFIG_DIGEST
export KUSTOMIZE_KAFKA_TOPICS_OVERLAY KAFKA_TOPIC_INIT_TIMEOUT

.PHONY: check version-check fmt fmt-check vet test build image k8s-context-check k8s-render k8s-validate k8s-deploy k8s-status \
	k8s-acceptance-render k8s-acceptance k8s-kafka-render k8s-kafka-validate k8s-kafka-image-check \
	k8s-kafka-runtime-check k8s-kafka-deploy k8s-kafka-status k8s-kafka-topics-render \
	k8s-kafka-topics-validate k8s-kafka-topics k8s-kafka-topics-status kafka-topic-initializer-test

# check 聚合所有只读工程门禁，适合提交前和持续集成调用。
check: version-check fmt-check vet test build kafka-topic-initializer-test

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

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

# build 单独验证 main 包能够完成链接，不生成仓库内二进制。
build:
	$(GO) build -o /dev/null ./cmd/log-producer

# kafka-topic-initializer-test 纯本地验证 Kafka 4.3.1 文本解析的正反例，不连接集群。
kafka-topic-initializer-test:
	@bash deploy/kubernetes/base/kafka-topics/initialize-topics.sh self-test

# image 只允许开发标签或干净工作区的当前提交短 SHA，避免产生来源不明的镜像。
image:
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
	$(DOCKER) build \
		--build-arg GO_VERSION=$(GO_VERSION) \
		--target log-producer \
		--tag $(IMAGE_REPOSITORY):$(IMAGE_TAG) \
		.

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

# k8s-deploy 在服务端 dry-run 通过后，才向明确的项目上下文应用本地 overlay。
k8s-deploy: k8s-validate
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		apply \
		-k $(KUSTOMIZE_OVERLAY)

k8s-status:
	$(KUBECTL) \
		--context=$(KUBE_CONTEXT) \
		get deployments,pods \
		-n $(KUBE_NAMESPACE) \
		-l app.kubernetes.io/name=log-producer \
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

# k8s-kafka-image-check 在写集群前校验 Minikube 节点内标签实际指向的摘要。
k8s-kafka-image-check: k8s-context-check
	@scripts/verify-kafka-image.sh node

# k8s-kafka-runtime-check 确认主容器和初始化容器都使用预期 config digest。
k8s-kafka-runtime-check: k8s-context-check
	@scripts/verify-kafka-image.sh pod

# k8s-kafka-deploy 与应用部署解耦，并在应用前后分别验证镜像身份。
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
