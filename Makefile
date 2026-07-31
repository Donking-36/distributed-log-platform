.DEFAULT_GOAL := check

GO ?= go
GOFMT ?= gofmt
DOCKER ?= docker
GO_VERSION := 1.26.5
EXPECTED_GO_VERSION := go$(GO_VERSION)
GO_FILES := $(shell find . -type f -name '*.go' -not -path './vendor/*')
IMAGE_REPOSITORY ?= distributed-log-platform/log-producer
IMAGE_TAG ?= dev

.PHONY: check version-check fmt fmt-check vet test build image

# check 聚合所有只读工程门禁，适合提交前和持续集成调用。
check: version-check fmt-check vet test build

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
