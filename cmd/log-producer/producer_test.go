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

func TestWriteEventsWritesDeterministicJSONLines(t *testing.T) {
	var output bytes.Buffer
	fixedTime := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

	cfg := config{
		ServiceName: "api-service",
		TestRunID:   "run-001",
		Count:       2,
		Interval:    time.Second,
	}

	err := writeEvents(
		context.Background(),
		&output,
		cfg,
		func() time.Time {
			return fixedTime
		},
		func(context.Context, time.Duration) error { return nil },
	)
	if err != nil {
		t.Fatalf("writeEvents() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != cfg.Count {
		t.Fatalf("line count = %d, want %d", len(lines), cfg.Count)
	}

	type decodedEvent struct {
		Timestamp   string `json:"@timestamp"`
		Sequence    int    `json:"event.sequence"`
		Level       string `json:"log.level"`
		Message     string `json:"message"`
		ServiceName string `json:"service.name"`
		TestRunID   string `json:"test_run_id"`
	}

	for index, line := range lines {
		var got decodedEvent
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("decode line %d: %v", index+1, err)
		}

		sequence := index + 1
		want := decodedEvent{
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
}

func TestWriteEventsWaitsOnlyBetweenEvents(t *testing.T) {
	cfg := config{
		ServiceName: "api-service",
		TestRunID:   "run-001",
		Count:       3,
		Interval:    250 * time.Millisecond,
	}
	var output bytes.Buffer
	var waits []time.Duration
	var writtenCounts []int

	err := writeEvents(
		context.Background(),
		&output,
		cfg,
		time.Now,
		func(_ context.Context, duration time.Duration) error {
			waits = append(waits, duration)
			writtenCounts = append(
				writtenCounts,
				strings.Count(output.String(), "\n"),
			)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("writeEvents() error = %v", err)
	}

	if len(waits) != cfg.Count-1 {
		t.Fatalf("wait count = %d, want %d", len(waits), cfg.Count-1)
	}
	for index, duration := range waits {
		if duration != cfg.Interval {
			t.Errorf(
				"wait %d = %s, want %s",
				index+1,
				duration,
				cfg.Interval,
			)
		}
	}
	wantWrittenCounts := []int{1, 2}
	for index, want := range wantWrittenCounts {
		if writtenCounts[index] != want {
			t.Errorf(
				"written count before wait %d = %d, want %d",
				index+1,
				writtenCounts[index],
				want,
			)
		}
	}
}

func TestWriteEventsDoesNotWaitAfterOnlyEvent(t *testing.T) {
	waitCalls := 0

	err := writeEvents(
		context.Background(),
		io.Discard,
		config{
			ServiceName: "api-service",
			TestRunID:   "run-001",
			Count:       1,
			Interval:    time.Second,
		},
		time.Now,
		func(context.Context, time.Duration) error {
			waitCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("writeEvents() error = %v", err)
	}
	if waitCalls != 0 {
		t.Fatalf("wait calls = %d, want 0", waitCalls)
	}
}

func TestWriteEventsStopsWhenWaitIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var output bytes.Buffer
	cfg := config{
		ServiceName: "api-service",
		TestRunID:   "run-001",
		Count:       3,
		Interval:    time.Second,
	}

	err := writeEvents(
		ctx,
		&output,
		cfg,
		time.Now,
		func(ctx context.Context, _ time.Duration) error {
			cancel()
			return ctx.Err()
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("writeEvents() error = %v, want context canceled", err)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("line count = %d, want 1", len(lines))
	}
}

func TestWriteEventsHonorsCanceledContextBeforeFirstEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var output bytes.Buffer

	err := writeEvents(
		ctx,
		&output,
		config{
			ServiceName: "api-service",
			TestRunID:   "run-001",
			Count:       1,
			Interval:    time.Second,
		},
		time.Now,
		func(context.Context, time.Duration) error { return nil },
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("writeEvents() error = %v, want context canceled", err)
	}
	if output.Len() != 0 {
		t.Fatalf("output length = %d, want 0", output.Len())
	}
}

func TestWaitForIntervalHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitForInterval(ctx, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForInterval() error = %v, want context canceled", err)
	}
}

func TestWaitForIntervalReturnsAfterTimer(t *testing.T) {
	err := waitForInterval(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatalf("waitForInterval() error = %v", err)
	}
}
