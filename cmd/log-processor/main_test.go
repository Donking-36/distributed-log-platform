package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestExecuteRunsAndClosesApplication(t *testing.T) {
	t.Parallel()

	runCalls := 0
	closeCalls := 0
	buildCalls := 0
	err := execute(
		context.Background(),
		processorLookupEnv(validProcessorEnvironment()),
		func(config config) (*application, error) {
			buildCalls++
			if config.KafkaGroupID != "log-processor-v1" {
				t.Fatalf("builder config = %#v", config)
			}
			return &application{
				run: func(context.Context) error {
					runCalls++
					return nil
				},
				close: func(ctx context.Context) error {
					closeCalls++
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("Close context 缺少 deadline")
					}
					return nil
				},
			}, nil
		},
	)
	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if buildCalls != 1 || runCalls != 1 || closeCalls != 1 {
		t.Fatalf("build/run/close = %d/%d/%d，期望 1/1/1", buildCalls, runCalls, closeCalls)
	}
}

func TestExecuteTreatsSignalCancellationAsNormalStop(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	closeCalls := 0
	err := execute(
		ctx,
		processorLookupEnv(validProcessorEnvironment()),
		func(config) (*application, error) {
			return &application{
				run: func(context.Context) error {
					cancel()
					return context.Canceled
				},
				close: func(ctx context.Context) error {
					closeCalls++
					if ctx.Err() != nil {
						t.Fatalf("Close context 已取消: %v", ctx.Err())
					}
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("Close context 缺少 deadline")
					}
					return nil
				},
			}, nil
		},
	)
	if err != nil {
		t.Fatalf("execute() error = %v，期望正常停止", err)
	}
	if closeCalls != 1 {
		t.Fatalf("close calls = %d，期望 1", closeCalls)
	}
}

func TestExecutePreservesUnexpectedCancellation(t *testing.T) {
	t.Parallel()

	closeCalls := 0
	err := execute(
		context.Background(),
		processorLookupEnv(validProcessorEnvironment()),
		func(config) (*application, error) {
			return &application{
				run: func(context.Context) error { return context.Canceled },
				close: func(context.Context) error {
					closeCalls++
					return nil
				},
			}, nil
		},
	)
	if !errors.Is(err, context.Canceled) || closeCalls != 1 {
		t.Fatalf("execute() error/close = %v/%d，期望保留取消且关闭一次", err, closeCalls)
	}
}

func TestExecutePreservesRunAndCloseFailures(t *testing.T) {
	t.Parallel()

	runErr := errors.New("runner failed")
	closeErr := errors.New("close failed")
	err := execute(
		context.Background(),
		processorLookupEnv(validProcessorEnvironment()),
		func(config) (*application, error) {
			return &application{
				run:   func(context.Context) error { return runErr },
				close: func(context.Context) error { return closeErr },
			}, nil
		},
	)
	if !errors.Is(err, runErr) || !errors.Is(err, closeErr) {
		t.Fatalf("execute() error = %v，期望同时保留 run/close 错误", err)
	}
}

func TestExecuteStopsBeforeBuildOnConfigFailure(t *testing.T) {
	t.Parallel()

	buildCalls := 0
	err := execute(
		context.Background(),
		processorLookupEnv(map[string]string{}),
		func(config) (*application, error) {
			buildCalls++
			return nil, nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), processorKafkaBrokersEnv) {
		t.Fatalf("execute() error = %v", err)
	}
	if buildCalls != 0 {
		t.Fatalf("build calls = %d，期望 0", buildCalls)
	}
}

func TestExecuteRejectsInvalidDependencies(t *testing.T) {
	t.Parallel()

	validLookup := processorLookupEnv(validProcessorEnvironment())
	validBuilder := func(config) (*application, error) {
		return &application{
			run:   func(context.Context) error { return nil },
			close: func(context.Context) error { return nil },
		}, nil
	}
	tests := []struct {
		name    string
		ctx     context.Context
		lookup  func(string) (string, bool)
		builder applicationBuilder
	}{
		{name: "nil context", lookup: validLookup, builder: validBuilder},
		{name: "nil lookup", ctx: context.Background(), builder: validBuilder},
		{name: "nil builder", ctx: context.Background(), lookup: validLookup},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := execute(test.ctx, test.lookup, test.builder); err == nil {
				t.Fatal("execute() error = nil")
			}
		})
	}
}

func TestExecuteRejectsUninitializedApplication(t *testing.T) {
	t.Parallel()

	builders := []applicationBuilder{
		func(config) (*application, error) { return nil, nil },
		func(config) (*application, error) { return &application{}, nil },
		func(config) (*application, error) {
			return &application{run: func(context.Context) error { return nil }}, nil
		},
	}
	for index, builder := range builders {
		if err := execute(
			context.Background(),
			processorLookupEnv(validProcessorEnvironment()),
			builder,
		); err == nil {
			t.Fatalf("builder[%d] execute() error = nil", index)
		}
	}
}

func TestShutdownTimeoutIsPositive(t *testing.T) {
	t.Parallel()
	if shutdownTimeout <= 0 || shutdownTimeout > 30*time.Second {
		t.Fatalf("shutdownTimeout = %v", shutdownTimeout)
	}
}
