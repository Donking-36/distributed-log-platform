package elasticsearch

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/event"
)

func TestNewDocumentMatchesStorageContract(t *testing.T) {
	t.Parallel()

	input := newTestEvent(t)
	original := input
	ingestedAt := time.Date(
		2026, 8, 4, 11, 4, 5, 987654321,
		time.FixedZone("UTC+8", 8*60*60),
	)

	document, err := NewDocument(input, ingestedAt)
	if err != nil {
		t.Fatalf("NewDocument() error = %v", err)
	}

	wantID, err := event.EventID(input)
	if err != nil {
		t.Fatalf("EventID() error = %v", err)
	}
	if document.id != wantID {
		t.Fatalf("document ID = %q, want %q", document.id, wantID)
	}

	var got map[string]any
	if err := json.Unmarshal(document.source, &got); err != nil {
		t.Fatalf("document source is not valid JSON: %v", err)
	}

	want := map[string]any{
		"@timestamp": "2026-08-03T08:22:16.507475585Z",
		"event_id":   wantID,
		"event": map[string]any{
			"sequence": float64(7),
		},
		"message": "数据库连接失败\n请检查连接",
		"log": map[string]any{
			"level": "ERROR",
			"file": map[string]any{
				"path": "/var/log/containers/api-service.log",
			},
			"offset": float64(42),
		},
		"service": map[string]any{
			"name": "api-service",
		},
		"test_run_id": "bulk-test-001",
		"kubernetes": map[string]any{
			"namespace": "stage3-logs",
			"pod": map[string]any{
				"name": "api-service-abc",
				"uid":  "pod-uid-001",
			},
		},
		"container": map[string]any{
			"id": "container-id-001",
		},
		"ingested_at": "2026-08-04T03:04:05.987654321Z",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("document source = %#v, want %#v", got, want)
	}
	if leaves := countJSONLeaves(got); leaves != 14 {
		t.Fatalf("document has %d leaf fields, want 14", leaves)
	}
	if !reflect.DeepEqual(input, original) {
		t.Fatal("NewDocument() changed its input event")
	}
}

func TestNewDocumentRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	t.Run("zero ingestion time", func(t *testing.T) {
		t.Parallel()

		_, err := NewDocument(newTestEvent(t), time.Time{})
		if err == nil {
			t.Fatal("NewDocument() error = nil, want an error")
		}
	})

	t.Run("invalid stable identity", func(t *testing.T) {
		t.Parallel()

		input := newTestEvent(t)
		input.PodUID = ""
		_, err := NewDocument(input, time.Now())
		if !errors.Is(err, event.ErrInvalid) {
			t.Fatalf("NewDocument() error = %v, want event.ErrInvalid", err)
		}
	})
}

func newTestEvent(t *testing.T) event.Event {
	t.Helper()

	const payload = `{
  "message":"{\"@timestamp\":\"2026-08-03T16:22:16.507475585+08:00\",\"event.sequence\":7,\"log.level\":\"error\",\"message\":\"数据库连接失败\\n请检查连接\",\"service.name\":\"api-service\",\"test_run_id\":\"bulk-test-001\"}",
  "kubernetes":{
    "namespace":"stage3-logs",
    "labels":{"service":"api-service"},
    "pod":{"name":"api-service-abc","uid":"pod-uid-001"}
  },
  "container":{"id":"container-id-001"},
  "log":{"offset":42,"file":{"path":"/var/log/containers/api-service.log"}}
}`

	parsed, err := event.ParseFilebeat([]byte(payload))
	if err != nil {
		t.Fatalf("ParseFilebeat() error = %v", err)
	}
	return parsed
}

func countJSONLeaves(value any) int {
	switch typed := value.(type) {
	case map[string]any:
		total := 0
		for _, child := range typed {
			total += countJSONLeaves(child)
		}
		return total
	default:
		return 1
	}
}
