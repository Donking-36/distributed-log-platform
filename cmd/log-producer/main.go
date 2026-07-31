package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// main 组装真实进程依赖，并把启动失败转换为非零退出码。
func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	err := execute(
		ctx,
		os.LookupEnv,
		os.Stdout,
		time.Now,
		waitForInterval,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log-producer: %v\n", err)
		os.Exit(1)
	}
}

// execute 连接配置加载与日志生成逻辑，并保留可测试的依赖注入边界。
func execute(
	ctx context.Context,
	lookupEnv func(string) (string, bool),
	output io.Writer,
	now func() time.Time,
	wait waitFunc,
) error {
	if ctx == nil {
		return fmt.Errorf("context must not be nil")
	}
	if output == nil {
		return fmt.Errorf("output must not be nil")
	}
	if now == nil {
		return fmt.Errorf("clock must not be nil")
	}
	if wait == nil {
		return fmt.Errorf("wait function must not be nil")
	}

	cfg, err := loadConfig(lookupEnv)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	if err := writeEvents(ctx, output, cfg, now, wait); err != nil {
		// 持续模式以信号取消作为正常生命周期终点；有限批次必须保留取消错误，
		// 避免 Kubernetes Job 在未写完目标事件时被误判为成功。
		if cfg.Count == 0 && errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("write events: %w", err)
	}

	return nil
}
