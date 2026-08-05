# syntax=docker/dockerfile:1@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89

ARG GO_VERSION=1.26.5
ARG ALPINE_VERSION=3.23
ARG GO_IMAGE_DIGEST=sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2
ARG ALPINE_IMAGE_DIGEST=sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine@${GO_IMAGE_DIGEST} AS go-builder-base

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.* ./
RUN go mod download

FROM go-builder-base AS log-producer-builder

COPY cmd/log-producer ./cmd/log-producer

# log-producer 只依赖标准库，关闭 CGO 可生成不依赖运行时 libc 的静态二进制。
# trimpath 与 buildvcs=false 避免把本机路径和构建上下文元数据写入产物。
RUN mkdir -p /out \
    && CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
        go build \
        -trimpath \
        -buildvcs=false \
        -ldflags="-s -w" \
        -o /out/log-producer \
        ./cmd/log-producer

FROM go-builder-base AS log-processor-builder

COPY cmd/log-processor ./cmd/log-processor
COPY internal ./internal

# log-processor 的 Kafka 与 Elasticsearch 客户端均为纯 Go 实现；静态链接使运行镜像
# 不依赖构建阶段的 libc 和 Go 工具链。
RUN mkdir -p /out \
    && CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
        go build \
        -trimpath \
        -buildvcs=false \
        -ldflags="-s -w" \
        -o /out/log-processor \
        ./cmd/log-processor

FROM alpine:${ALPINE_VERSION}@${ALPINE_IMAGE_DIGEST} AS runtime-base

ARG APP_UID=10001
ARG APP_GID=10001

# 运行镜像只保留二进制和非 root 身份，降低文件系统与进程权限。
RUN addgroup -S -g "${APP_GID}" app \
    && adduser -S -D -H -u "${APP_UID}" -G app app

USER app:app
STOPSIGNAL SIGTERM

FROM runtime-base AS log-producer

COPY --from=log-producer-builder --chown=app:app /out/log-producer /usr/local/bin/log-producer

ENTRYPOINT ["/usr/local/bin/log-producer"]

FROM runtime-base AS log-processor

COPY --from=log-processor-builder --chown=app:app /out/log-processor /usr/local/bin/log-processor

# 8080 是 PROCESSOR_HEALTH_ADDRESS 的默认端口；实际监听地址仍由启动配置决定。
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/log-processor"]
