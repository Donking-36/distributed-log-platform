package event

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// validFilebeatPayload 保留真实 Kafka 记录的双层结构：外层是 Filebeat 事件，
// message 字段内部才是 log-producer 写出的单行 JSON。
const validFilebeatPayload = `{
  "@timestamp":"2000-01-01T00:00:00Z",
  "message":"{\"@timestamp\":\"2026-08-03T08:22:16.507475585Z\",\"event.sequence\":1,\"log.level\":\" info \",\"message\":\"log event 1\",\"service.name\":\"api-service\",\"test_run_id\":\"run-1\",\"future.inner\":true}",
  "kubernetes":{
    "namespace":"stage3-logs",
    "labels":{"service":"api-service"},
    "pod":{"name":"api-pod","uid":"pod-uid"}
  },
  "container":{"id":"container-id"},
  "log":{
    "offset":0,
    "file":{"path":"/var/log/containers/api-service_stage3-logs_log-producer-container-id.log"}
  },
  "agent":{"version":"9.4.4"}
}`

func TestParseFilebeatNormalizesBusinessEvent(t *testing.T) {
	t.Parallel()

	got, err := ParseFilebeat([]byte(validFilebeatPayload))
	if err != nil {
		t.Fatalf("ParseFilebeat() error = %v", err)
	}

	wantTimestamp := time.Date(2026, 8, 3, 8, 22, 16, 507475585, time.UTC)
	if !got.Timestamp.Equal(wantTimestamp) || got.Timestamp.Location() != time.UTC {
		t.Fatalf("Timestamp = %v, want UTC %v", got.Timestamp, wantTimestamp)
	}
	if got.Sequence != 1 {
		t.Fatalf("Sequence = %d, want 1", got.Sequence)
	}
	if got.Level != "INFO" {
		t.Fatalf("Level = %q, want INFO", got.Level)
	}
	if got.Message != "log event 1" {
		t.Fatalf("Message = %q, want %q", got.Message, "log event 1")
	}
	if got.ServiceName != "api-service" {
		t.Fatalf("ServiceName = %q, want api-service", got.ServiceName)
	}
	if got.TestRunID != "run-1" {
		t.Fatalf("TestRunID = %q, want run-1", got.TestRunID)
	}
	if got.Namespace != "stage3-logs" || got.PodName != "api-pod" || got.PodUID != "pod-uid" {
		t.Fatalf("Kubernetes identity = %#v, want stage3-logs/api-pod/pod-uid", got)
	}
	if got.ContainerID != "container-id" {
		t.Fatalf("ContainerID = %q, want container-id", got.ContainerID)
	}
	if got.LogFilePath != "/var/log/containers/api-service_stage3-logs_log-producer-container-id.log" {
		t.Fatalf("LogFilePath = %q, want expected container path", got.LogFilePath)
	}
	if got.LogOffset != 0 {
		t.Fatalf("LogOffset = %d, want 0", got.LogOffset)
	}
}

func TestParseFilebeatUsesInnerTimestampAndConvertsToUTC(t *testing.T) {
	t.Parallel()

	payload := strings.Replace(
		validFilebeatPayload,
		`2026-08-03T08:22:16.507475585Z`,
		`2026-08-03T16:22:16.507475585+08:00`,
		1,
	)
	got, err := ParseFilebeat([]byte(payload))
	if err != nil {
		t.Fatalf("ParseFilebeat() error = %v", err)
	}
	want := time.Date(2026, 8, 3, 8, 22, 16, 507475585, time.UTC)
	if !got.Timestamp.Equal(want) || got.Timestamp.Location() != time.UTC {
		t.Fatalf("Timestamp = %v, want UTC %v", got.Timestamp, want)
	}
}

func TestParseFilebeatAllowsMissingOrEmptyTestRunID(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"missing": strings.Replace(validFilebeatPayload, `,\"test_run_id\":\"run-1\"`, ``, 1),
		"empty":   strings.Replace(validFilebeatPayload, `\"test_run_id\":\"run-1\"`, `\"test_run_id\":\"\"`, 1),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseFilebeat([]byte(payload))
			if err != nil {
				t.Fatalf("ParseFilebeat() error = %v", err)
			}
			if got.TestRunID != "" {
				t.Fatalf("TestRunID = %q, want empty", got.TestRunID)
			}
		})
	}
}

func TestParseFilebeatRejectsPermanentInvalidInput(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		payload string
		field   string
	}{
		"outer json is invalid": {
			payload: `{`,
			field:   "payload",
		},
		"outer value is not object": {
			payload: `[]`,
			field:   "payload",
		},
		"message is missing": {
			payload: strings.Replace(validFilebeatPayload, `"message":"{\"@timestamp`, `"unused":"{\"@timestamp`, 1),
			field:   "message",
		},
		"message is not string": {
			payload: strings.Replace(validFilebeatPayload, `"message":"{\"@timestamp\":\"2026-08-03T08:22:16.507475585Z\",\"event.sequence\":1,\"log.level\":\" info \",\"message\":\"log event 1\",\"service.name\":\"api-service\",\"test_run_id\":\"run-1\",\"future.inner\":true}"`, `"message":17`, 1),
			field:   "message",
		},
		"business json is invalid": {
			payload: strings.Replace(validFilebeatPayload, `"message":"{\"@timestamp`, `"message":"not-json`, 1),
			field:   "message",
		},
		"business value is not object": {
			payload: strings.Replace(validFilebeatPayload, `"message":"{\"@timestamp\":\"2026-08-03T08:22:16.507475585Z\",\"event.sequence\":1,\"log.level\":\" info \",\"message\":\"log event 1\",\"service.name\":\"api-service\",\"test_run_id\":\"run-1\",\"future.inner\":true}"`, `"message":"[]"`, 1),
			field:   "message",
		},
		"namespace is empty": {
			payload: strings.Replace(validFilebeatPayload, `"namespace":"stage3-logs"`, `"namespace":""`, 1),
			field:   "kubernetes.namespace",
		},
		"pod name is empty": {
			payload: strings.Replace(validFilebeatPayload, `"name":"api-pod"`, `"name":""`, 1),
			field:   "kubernetes.pod.name",
		},
		"pod uid is empty": {
			payload: strings.Replace(validFilebeatPayload, `"uid":"pod-uid"`, `"uid":""`, 1),
			field:   "kubernetes.pod.uid",
		},
		"service label is empty": {
			payload: strings.Replace(validFilebeatPayload, `"service":"api-service"`, `"service":""`, 1),
			field:   "kubernetes.labels.service",
		},
		"container id is empty": {
			payload: strings.Replace(validFilebeatPayload, `"id":"container-id"`, `"id":""`, 1),
			field:   "container.id",
		},
		"log path is empty": {
			payload: strings.Replace(validFilebeatPayload, `"path":"/var/log/containers/api-service_stage3-logs_log-producer-container-id.log"`, `"path":""`, 1),
			field:   "log.file.path",
		},
		"log offset is missing": {
			payload: strings.Replace(validFilebeatPayload, `"offset":0`, `"unused":0`, 1),
			field:   "log.offset",
		},
		"log offset is negative": {
			payload: strings.Replace(validFilebeatPayload, `"offset":0`, `"offset":-1`, 1),
			field:   "log.offset",
		},
		"log offset is not integer": {
			payload: strings.Replace(validFilebeatPayload, `"offset":0`, `"offset":1.5`, 1),
			field:   "log.offset",
		},
		"timestamp is invalid": {
			payload: strings.Replace(validFilebeatPayload, `2026-08-03T08:22:16.507475585Z`, `not-a-time`, 1),
			field:   "@timestamp",
		},
		"sequence is zero": {
			payload: strings.Replace(validFilebeatPayload, `\"event.sequence\":1`, `\"event.sequence\":0`, 1),
			field:   "event.sequence",
		},
		"sequence is not integer": {
			payload: strings.Replace(validFilebeatPayload, `\"event.sequence\":1`, `\"event.sequence\":1.5`, 1),
			field:   "event.sequence",
		},
		"level is empty": {
			payload: strings.Replace(validFilebeatPayload, `\"log.level\":\" info \"`, `\"log.level\":\" \"`, 1),
			field:   "log.level",
		},
		"body is empty": {
			payload: strings.Replace(validFilebeatPayload, `\"message\":\"log event 1\"`, `\"message\":\" \"`, 1),
			field:   "message",
		},
		"source service is empty": {
			payload: strings.Replace(validFilebeatPayload, `\"service.name\":\"api-service\"`, `\"service.name\":\"\"`, 1),
			field:   "service.name",
		},
		"test run id is null": {
			payload: strings.Replace(validFilebeatPayload, `\"test_run_id\":\"run-1\"`, `\"test_run_id\":null`, 1),
			field:   "test_run_id",
		},
		"service identity mismatches": {
			payload: strings.Replace(validFilebeatPayload, `\"service.name\":\"api-service\"`, `\"service.name\":\"worker-service\"`, 1),
			field:   "service.name",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseFilebeat([]byte(test.payload))
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("error = %v, want ErrInvalid", err)
			}
			var validationErr *ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("error = %T %v, want *ValidationError", err, err)
			}
			if validationErr.Field != test.field {
				t.Fatalf("ValidationError.Field = %q, want %q", validationErr.Field, test.field)
			}
		})
	}
}
