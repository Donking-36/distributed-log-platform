# syntax=docker/dockerfile:1@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89

ARG GO_VERSION=1.26.5
ARG ALPINE_VERSION=3.23
ARG GO_IMAGE_DIGEST=sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2
ARG ALPINE_IMAGE_DIGEST=sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine@${GO_IMAGE_DIGEST} AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.* ./
RUN go mod download

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

FROM alpine:${ALPINE_VERSION}@${ALPINE_IMAGE_DIGEST} AS log-producer

ARG APP_UID=10001
ARG APP_GID=10001

# 运行镜像只保留二进制和非 root 身份，降低文件系统与进程权限。
RUN addgroup -S -g "${APP_GID}" app \
    && adduser -S -D -H -u "${APP_UID}" -G app app

COPY --from=builder --chown=app:app /out/log-producer /usr/local/bin/log-producer

USER app:app
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/log-producer"]
