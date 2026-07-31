package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestExecuteWritesConfiguredEvents(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
		"PRODUCER_TEST_RUN_ID":  "run-001",
		"PRODUCER_COUNT":        "2",
	}
	fixedTime := time.Date(
		2026,
		time.July,
		31,
		2,
		0,
		0,
		0,
		time.UTC,
	)
	var output bytes.Buffer

	err := execute(
		context.Background(),
		lookupEnvFrom(values),
		&output,
		func() time.Time {
			return fixedTime
		},
		immediateWait,
	)
	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}

	decoder := json.NewDecoder(&output)
	for sequence := 1; sequence <= 2; sequence++ {
		var got logEvent
		if err := decoder.Decode(&got); err != nil {
			t.Fatalf("decode event %d: %v", sequence, err)
		}

		want := logEvent{
			Timestamp:   fixedTime.Format(time.RFC3339Nano),
			Sequence:    sequence,
			Level:       "INFO",
			Message:     fmt.Sprintf("log event %d", sequence),
			ServiceName: "api-service",
			TestRunID:   "run-001",
		}
		if got != want {
			t.Errorf("event %d = %+v, want %+v", sequence, got, want)
		}
	}

	var extra logEvent
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("decode trailing event error = %v, want EOF", err)
	}
}

func TestExecuteTreatsContinuousCancellationAsSuccess(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
		"PRODUCER_TEST_RUN_ID":  "run-001",
		"PRODUCER_COUNT":        "0",
	}
	ctx, cancel := context.WithCancel(context.Background())
	waitCalls := 0

	err := execute(
		ctx,
		lookupEnvFrom(values),
		io.Discard,
		time.Now,
		func(ctx context.Context, _ time.Duration) error {
			waitCalls++
			cancel()
			return ctx.Err()
		},
	)
	if err != nil {
		t.Fatalf("execute() error = %v, want nil", err)
	}
	if waitCalls != 1 {
		t.Fatalf("wait calls = %d, want 1", waitCalls)
	}
}

func TestExecuteReturnsFiniteCancellation(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
		"PRODUCER_TEST_RUN_ID":  "run-001",
		"PRODUCER_COUNT":        "2",
	}
	ctx, cancel := context.WithCancel(context.Background())
	waitCalls := 0

	err := execute(
		ctx,
		lookupEnvFrom(values),
		io.Discard,
		time.Now,
		func(ctx context.Context, _ time.Duration) error {
			waitCalls++
			cancel()
			return ctx.Err()
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"execute() error = %v, want context canceled",
			err,
		)
	}
	if !strings.Contains(err.Error(), "write events") {
		t.Fatalf("error = %q, want write events context", err)
	}
	if waitCalls != 1 {
		t.Fatalf("wait calls = %d, want 1", waitCalls)
	}
}

func TestExecuteReturnsConfigError(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
	}

	err := execute(
		context.Background(),
		lookupEnvFrom(values),
		&bytes.Buffer{},
		time.Now,
		immediateWait,
	)
	if err == nil {
		t.Fatal("execute() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "load configuration") {
		t.Fatalf("error = %q", err)
	}
	if !strings.Contains(
		err.Error(),
		"PRODUCER_TEST_RUN_ID must not be empty",
	) {
		t.Fatalf("error = %q", err)
	}
}

func TestExecuteRejectsInvalidDependencies(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
		"PRODUCER_TEST_RUN_ID":  "run-001",
	}

	tests := []struct {
		name        string
		ctx         context.Context
		output      io.Writer
		now         func() time.Time
		wait        waitFunc
		wantMessage string
	}{
		{
			name:        "nil context",
			ctx:         nil,
			output:      &bytes.Buffer{},
			now:         time.Now,
			wait:        immediateWait,
			wantMessage: "context must not be nil",
		},
		{
			name:        "nil output",
			ctx:         context.Background(),
			output:      nil,
			now:         time.Now,
			wait:        immediateWait,
			wantMessage: "output must not be nil",
		},
		{
			name:        "nil clock",
			ctx:         context.Background(),
			output:      &bytes.Buffer{},
			now:         nil,
			wait:        immediateWait,
			wantMessage: "clock must not be nil",
		},
		{
			name:        "nil wait function",
			ctx:         context.Background(),
			output:      &bytes.Buffer{},
			now:         time.Now,
			wait:        nil,
			wantMessage: "wait function must not be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := execute(
				tt.ctx,
				lookupEnvFrom(values),
				tt.output,
				tt.now,
				tt.wait,
			)
			if err == nil {
				t.Fatal("execute() succeeded, want error")
			}
			if !strings.Contains(err.Error(), tt.wantMessage) {
				t.Fatalf(
					"error = %q, want substring %q",
					err,
					tt.wantMessage,
				)
			}
		})
	}
}

func TestExecuteReturnsWriteError(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
		"PRODUCER_TEST_RUN_ID":  "run-001",
		"PRODUCER_COUNT":        "2",
	}
	waitCalled := false

	err := execute(
		context.Background(),
		lookupEnvFrom(values),
		errorWriter{},
		time.Now,
		func(context.Context, time.Duration) error {
			waitCalled = true
			return nil
		},
	)
	if err == nil {
		t.Fatal("execute() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "write events") {
		t.Fatalf("error = %q", err)
	}
	if !strings.Contains(err.Error(), "forced write failure") {
		t.Fatalf("error = %q", err)
	}
	if waitCalled {
		t.Fatal("wait called after write failure")
	}
}

func TestExecuteReturnsWaitError(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
		"PRODUCER_TEST_RUN_ID":  "run-001",
		"PRODUCER_COUNT":        "2",
		"PRODUCER_INTERVAL":     "1s",
	}

	err := execute(
		context.Background(),
		lookupEnvFrom(values),
		io.Discard,
		time.Now,
		func(context.Context, time.Duration) error {
			return errors.New("forced wait failure")
		},
	)
	if err == nil {
		t.Fatal("execute() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "write events") {
		t.Fatalf("error = %q", err)
	}
	if !strings.Contains(err.Error(), "forced wait failure") {
		t.Fatalf("error = %q", err)
	}
}

// immediateWait 为入口测试提供不依赖真实时间流逝的等待边界。
func immediateWait(context.Context, time.Duration) error {
	return nil
}

// errorWriter 为入口错误传播测试提供可控的写入失败。
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("forced write failure")
}
