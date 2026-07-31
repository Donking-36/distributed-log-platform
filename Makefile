.DEFAULT_GOAL := check

GO ?= go
GOFMT ?= gofmt
EXPECTED_GO_VERSION := go1.26.5
GO_FILES := $(shell find . -type f -name '*.go' -not -path './vendor/*')

.PHONY: check version-check fmt fmt-check vet test build

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
