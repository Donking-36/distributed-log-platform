package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// logEvent 定义日志生产器写出的单行 JSON 事件契约。
type logEvent struct {
	Timestamp   string `json:"@timestamp"`
	Sequence    int    `json:"event.sequence"`
	Level       string `json:"log.level"`
	Message     string `json:"message"`
	ServiceName string `json:"service.name"`
	TestRunID   string `json:"test_run_id"`
}

// waitFunc 抽象两条事件之间的等待边界，使发送节奏无需依赖真实睡眠即可测试。
type waitFunc func(context.Context, time.Duration) error

// writeEvents 按顺序生成指定数量的日志事件并逐行写入 output。
// 第一条事件立即写出，后续事件只在可取消的固定间隔后写出。
func writeEvents(
	ctx context.Context,
	output io.Writer,
	cfg config,
	now func() time.Time,
	wait waitFunc,
) error {
	encoder := json.NewEncoder(output)

	for sequence := 1; sequence <= cfg.Count; sequence++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("before event %d: %w", sequence, err)
		}

		event := logEvent{
			Timestamp: now().UTC().Format(
				time.RFC3339Nano,
			),
			Sequence:    sequence,
			Level:       "INFO",
			Message:     fmt.Sprintf("log event %d", sequence),
			ServiceName: cfg.ServiceName,
			TestRunID:   cfg.TestRunID,
		}

		if err := encoder.Encode(event); err != nil {
			return fmt.Errorf(
				"encode event %d: %w",
				sequence,
				err,
			)
		}

		if sequence == cfg.Count {
			continue
		}
		if err := wait(ctx, cfg.Interval); err != nil {
			return fmt.Errorf(
				"wait before event %d: %w",
				sequence+1,
				err,
			)
		}
	}

	return nil
}

// waitForInterval 使用独立计时器等待下一条事件，并同时监听进程取消信号。
func waitForInterval(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
