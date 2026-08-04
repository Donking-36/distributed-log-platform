package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const shutdownTimeout = 10 * time.Second

// main 建立信号 context 和结构化日志，并把启动或运行失败转换为非零退出码。
func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	err := execute(
		ctx,
		os.LookupEnv,
		func(cfg config) (*application, error) {
			return buildApplication(cfg, logger)
		},
	)
	if err != nil {
		logger.Error("log-processor 退出", "error", err)
		os.Exit(1)
	}
}

// execute 连接配置、应用生命周期和关闭流程，保留可单测的组合边界。
func execute(
	ctx context.Context,
	lookupEnv func(string) (string, bool),
	builder applicationBuilder,
) error {
	if ctx == nil {
		return errors.New("运行 context 不能为空")
	}
	if lookupEnv == nil {
		return errors.New("环境变量查询函数不能为空")
	}
	if builder == nil {
		return errors.New("应用构造函数不能为空")
	}

	cfg, err := loadConfig(lookupEnv)
	if err != nil {
		return fmt.Errorf("加载 log-processor 配置: %w", err)
	}
	applicationInstance, err := builder(cfg)
	if err != nil {
		return fmt.Errorf("组装 log-processor: %w", err)
	}
	if applicationInstance == nil || applicationInstance.run == nil ||
		applicationInstance.close == nil {
		return errors.New("log-processor application 未初始化")
	}

	runErr := applicationInstance.run(ctx)
	// 信号触发的父 context 取消是正常生命周期终点；其他来源的取消仍需暴露。
	if errors.Is(runErr, context.Canceled) && errors.Is(ctx.Err(), context.Canceled) {
		runErr = nil
	}

	shutdownContext, cancelShutdown := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	closeErr := applicationInstance.close(shutdownContext)
	cancelShutdown()

	var errorsToReturn []error
	if runErr != nil {
		errorsToReturn = append(
			errorsToReturn,
			fmt.Errorf("运行 log-processor: %w", runErr),
		)
	}
	if closeErr != nil {
		errorsToReturn = append(
			errorsToReturn,
			fmt.Errorf("关闭 log-processor: %w", closeErr),
		)
	}
	return errors.Join(errorsToReturn...)
}
