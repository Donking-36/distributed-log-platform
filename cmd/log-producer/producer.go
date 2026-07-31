package main

import (
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

// writeEvents 按顺序生成指定数量的日志事件并逐行写入 output。
// now 由调用方注入，确保时间相关行为可以被确定性测试。
func writeEvents(
	output io.Writer,
	cfg config,
	now func() time.Time,
) error {
	encoder := json.NewEncoder(output)

	for sequence := 1; sequence <= cfg.Count; sequence++ {
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
	}

	return nil
}
