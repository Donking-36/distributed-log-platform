package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	}

	err := writeEvents(&output, cfg, func() time.Time {
		return fixedTime
	})
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
