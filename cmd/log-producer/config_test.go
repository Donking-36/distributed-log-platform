package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigUsesEnvironment(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": " api-service ",
		"PRODUCER_TEST_RUN_ID":  " run-001 ",
		"PRODUCER_COUNT":        " 8 ",
		"PRODUCER_INTERVAL":     " 250ms ",
	}

	cfg, err := loadConfig(lookupEnvFrom(values))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	if cfg.ServiceName != "api-service" {
		t.Errorf(
			"ServiceName = %q, want %q",
			cfg.ServiceName,
			"api-service",
		)
	}
	if cfg.TestRunID != "run-001" {
		t.Errorf(
			"TestRunID = %q, want %q",
			cfg.TestRunID,
			"run-001",
		)
	}
	if cfg.Count != 8 {
		t.Errorf("Count = %d, want 8", cfg.Count)
	}
	if cfg.Interval != 250*time.Millisecond {
		t.Errorf(
			"Interval = %s, want %s",
			cfg.Interval,
			250*time.Millisecond,
		)
	}
}

func TestLoadConfigUsesDefaults(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
		"PRODUCER_TEST_RUN_ID":  "run-001",
	}

	cfg, err := loadConfig(lookupEnvFrom(values))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	if cfg.Count != 20 {
		t.Errorf("Count = %d, want 20", cfg.Count)
	}
	if cfg.Interval != time.Second {
		t.Errorf("Interval = %s, want %s", cfg.Interval, time.Second)
	}
}

func TestLoadConfigAllowsZeroCountForContinuousMode(t *testing.T) {
	values := map[string]string{
		"PRODUCER_SERVICE_NAME": "api-service",
		"PRODUCER_TEST_RUN_ID":  "run-001",
		"PRODUCER_COUNT":        "0",
	}

	cfg, err := loadConfig(lookupEnvFrom(values))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	if cfg.Count != 0 {
		t.Errorf("Count = %d, want 0", cfg.Count)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name        string
		values      map[string]string
		wantMessage string
	}{
		{
			name: "missing service name",
			values: map[string]string{
				"PRODUCER_TEST_RUN_ID": "run-001",
			},
			wantMessage: "PRODUCER_SERVICE_NAME must not be empty",
		},
		{
			name: "missing test run ID",
			values: map[string]string{
				"PRODUCER_SERVICE_NAME": "api-service",
			},
			wantMessage: "PRODUCER_TEST_RUN_ID must not be empty",
		},
		{
			name: "invalid count",
			values: map[string]string{
				"PRODUCER_SERVICE_NAME": "api-service",
				"PRODUCER_TEST_RUN_ID":  "run-001",
				"PRODUCER_COUNT":        "many",
			},
			wantMessage: "PRODUCER_COUNT must be a valid integer",
		},
		{
			name: "negative count",
			values: map[string]string{
				"PRODUCER_SERVICE_NAME": "api-service",
				"PRODUCER_TEST_RUN_ID":  "run-001",
				"PRODUCER_COUNT":        "-1",
			},
			wantMessage: "PRODUCER_COUNT must not be negative",
		},
		{
			name: "empty interval",
			values: map[string]string{
				"PRODUCER_SERVICE_NAME": "api-service",
				"PRODUCER_TEST_RUN_ID":  "run-001",
				"PRODUCER_INTERVAL":     " ",
			},
			wantMessage: "PRODUCER_INTERVAL must be a valid duration",
		},
		{
			name: "invalid interval",
			values: map[string]string{
				"PRODUCER_SERVICE_NAME": "api-service",
				"PRODUCER_TEST_RUN_ID":  "run-001",
				"PRODUCER_INTERVAL":     "fast",
			},
			wantMessage: "PRODUCER_INTERVAL must be a valid duration",
		},
		{
			name: "zero interval",
			values: map[string]string{
				"PRODUCER_SERVICE_NAME": "api-service",
				"PRODUCER_TEST_RUN_ID":  "run-001",
				"PRODUCER_INTERVAL":     "0s",
			},
			wantMessage: "PRODUCER_INTERVAL must be greater than zero",
		},
		{
			name: "negative interval",
			values: map[string]string{
				"PRODUCER_SERVICE_NAME": "api-service",
				"PRODUCER_TEST_RUN_ID":  "run-001",
				"PRODUCER_INTERVAL":     "-1s",
			},
			wantMessage: "PRODUCER_INTERVAL must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfig(lookupEnvFrom(tt.values))
			if err == nil {
				t.Fatal("loadConfig() succeeded, want error")
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

func TestLoadConfigRejectsNilLookup(t *testing.T) {
	_, err := loadConfig(nil)
	if err == nil {
		t.Fatal("loadConfig() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "environment lookup must not be nil") {
		t.Fatalf("error = %q", err)
	}
}

// lookupEnvFrom 构造隔离的环境变量查询函数，避免测试依赖进程环境。
func lookupEnvFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, exists := values[name]
		return value, exists
	}
}
