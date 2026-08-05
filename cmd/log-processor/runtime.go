package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
)

type runtimeComponent uint8

const (
	runnerComponent runtimeComponent = iota
	healthComponent
)

type runtimeResult struct {
	component runtimeComponent
	err       error
}

// runApplication 协调处理循环和健康服务：任一方异常退出都会停止另一方。
func runApplication(
	ctx context.Context,
	runner func(context.Context) error,
	health *healthService,
	state *healthState,
) error {
	if ctx == nil {
		return errors.New("运行 context 不能为空")
	}
	if runner == nil {
		return errors.New("处理循环不能为空")
	}
	if health == nil || health.listen == nil || health.serve == nil ||
		health.shutdown == nil {
		return errors.New("健康检查服务未初始化")
	}
	if state == nil {
		return errors.New("健康状态不能为空")
	}

	state.set(healthStarting)
	defer state.set(healthStopped)
	if err := ctx.Err(); err != nil {
		return err
	}

	// 先同步绑定端口；若地址不可用，Runner 不会启动，也不会短暂误报就绪。
	listener, err := health.listen()
	if err != nil {
		return fmt.Errorf("监听健康检查地址: %w", err)
	}
	if listener == nil {
		return errors.New("健康检查监听器不能为空")
	}

	runContext, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	results := make(chan runtimeResult, 2)
	go func() {
		results <- runtimeResult{component: healthComponent, err: health.serve(listener)}
	}()
	go func() {
		results <- runtimeResult{component: runnerComponent, err: runner(runContext)}
	}()
	state.set(healthRunning)

	errorsToReturn := make([]error, 0, 4)
	remainingResults := 2
	select {
	case <-ctx.Done():
		state.set(healthDraining)
		errorsToReturn = append(errorsToReturn, ctx.Err())
	case result := <-results:
		remainingResults--
		if ctx.Err() != nil {
			state.set(healthDraining)
			errorsToReturn = append(errorsToReturn, ctx.Err())
			if stoppedErr := stoppedRuntimeError(result); stoppedErr != nil {
				errorsToReturn = append(errorsToReturn, stoppedErr)
			}
		} else {
			state.set(healthStopped)
			errorsToReturn = append(errorsToReturn, unexpectedRuntimeError(result))
		}
	}

	// 先撤销 Runner，再关闭 HTTP；draining 状态会在优雅关闭期间立即撤销就绪。
	cancelRun()
	shutdownContext, cancelShutdown := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer cancelShutdown()
	if shutdownErr := health.shutdown(shutdownContext); shutdownErr != nil {
		errorsToReturn = append(
			errorsToReturn,
			fmt.Errorf("关闭健康检查服务: %w", shutdownErr),
		)
	}
	// Shutdown 可能早于 Serve 真正进入阻塞；再次关闭监听器可消除该竞态。
	if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
		errorsToReturn = append(
			errorsToReturn,
			fmt.Errorf("关闭健康检查监听器: %w", closeErr),
		)
	}

	for remainingResults > 0 {
		select {
		case result := <-results:
			remainingResults--
			if stoppedErr := stoppedRuntimeError(result); stoppedErr != nil {
				errorsToReturn = append(errorsToReturn, stoppedErr)
			}
		case <-shutdownContext.Done():
			// 截止时再收一次结果，避免完成与超时同时发生时误报。
			select {
			case result := <-results:
				remainingResults--
				if stoppedErr := stoppedRuntimeError(result); stoppedErr != nil {
					errorsToReturn = append(errorsToReturn, stoppedErr)
				}
			default:
				errorsToReturn = append(
					errorsToReturn,
					fmt.Errorf("等待应用运行单元退出: %w", shutdownContext.Err()),
				)
				return errors.Join(errorsToReturn...)
			}
		}
	}
	return errors.Join(errorsToReturn...)
}

func unexpectedRuntimeError(result runtimeResult) error {
	switch result.component {
	case runnerComponent:
		if result.err == nil {
			return errors.New("处理循环意外退出")
		}
		return fmt.Errorf("处理循环意外退出: %w", result.err)
	case healthComponent:
		if result.err == nil {
			return errors.New("健康检查服务意外退出")
		}
		return fmt.Errorf("健康检查服务意外退出: %w", result.err)
	default:
		return errors.New("未知运行单元意外退出")
	}
}

func stoppedRuntimeError(result runtimeResult) error {
	switch result.component {
	case runnerComponent:
		if result.err == nil || errors.Is(result.err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("停止处理循环: %w", result.err)
	case healthComponent:
		if result.err == nil || errors.Is(result.err, http.ErrServerClosed) ||
			errors.Is(result.err, net.ErrClosed) {
			return nil
		}
		return fmt.Errorf("停止健康检查服务: %w", result.err)
	default:
		return errors.New("未知运行单元停止")
	}
}
