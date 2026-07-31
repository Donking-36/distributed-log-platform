package main

import (
	"bytes"
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
		lookupEnvFrom(values),
		&output,
		func() time.Time {
			return fixedTime
		},
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

func TestExecuteReturnsConfigError(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
	}

	err := execute(
		lookupEnvFrom(values),
		&bytes.Buffer{},
		time.Now,
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
		output      io.Writer
		now         func() time.Time
		wantMessage string
	}{
		{
			name:        "nil output",
			output:      nil,
			now:         time.Now,
			wantMessage: "output must not be nil",
		},
		{
			name:        "nil clock",
			output:      &bytes.Buffer{},
			now:         nil,
			wantMessage: "clock must not be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := execute(
				lookupEnvFrom(values),
				tt.output,
				tt.now,
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
		"PRODUCER_COUNT":        "1",
	}

	err := execute(
		lookupEnvFrom(values),
		errorWriter{},
		time.Now,
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
}

// errorWriter 为入口错误传播测试提供可控的写入失败。
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("forced write failure")
}
