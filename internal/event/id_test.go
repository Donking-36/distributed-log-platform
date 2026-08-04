package event

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestEventIDMatchesFixedUTF8Vector(t *testing.T) {
	t.Parallel()

	input := Event{
		Timestamp:   time.Date(2026, 8, 3, 8, 22, 16, 507475585, time.UTC),
		PodUID:      "pod-uid",
		ContainerID: "container-id",
		LogFilePath: "/var/log/containers/api.log",
		LogOffset:   42,
		rawMessage:  `{"message":"你好|1:2"}`,
	}

	got, err := EventID(input)
	if err != nil {
		t.Fatalf("EventID() error = %v", err)
	}
	const want = "sha256:0bf68546f85a708f7b01b1f96c3738a1c6d1c406e6b20844dce629407de3ad97"
	if got != want {
		t.Fatalf("EventID() = %q, want %q", got, want)
	}
}

func TestEventIDUsesOriginalParsedBusinessJSON(t *testing.T) {
	t.Parallel()

	parsed, err := ParseFilebeat([]byte(validFilebeatPayload))
	if err != nil {
		t.Fatalf("ParseFilebeat() error = %v", err)
	}
	got, err := EventID(parsed)
	if err != nil {
		t.Fatalf("EventID() error = %v", err)
	}
	const want = "sha256:c8eaed2985adc25f832a6835101d75f6fb563f1e10556f5d1d232890d7555055"
	if got != want {
		t.Fatalf("EventID() = %q, want %q", got, want)
	}
}

func TestEventIDIsStableForSameInstantInDifferentZones(t *testing.T) {
	t.Parallel()

	utc := validIDEvent()
	local := utc
	local.Timestamp = utc.Timestamp.In(time.FixedZone("UTC+8", 8*60*60))

	utcID, err := EventID(utc)
	if err != nil {
		t.Fatalf("UTC EventID() error = %v", err)
	}
	localID, err := EventID(local)
	if err != nil {
		t.Fatalf("local EventID() error = %v", err)
	}
	if utcID != localID {
		t.Fatalf("same instant produced different IDs: %q != %q", utcID, localID)
	}
}

func TestEventIDChangesWhenStableIdentityChanges(t *testing.T) {
	t.Parallel()

	base := validIDEvent()
	baseID, err := EventID(base)
	if err != nil {
		t.Fatalf("base EventID() error = %v", err)
	}

	tests := map[string]func(*Event){
		"pod uid":      func(value *Event) { value.PodUID = "another-pod" },
		"container id": func(value *Event) { value.ContainerID = "another-container" },
		"file path":    func(value *Event) { value.LogFilePath = "/another/path.log" },
		"source offset": func(value *Event) {
			value.LogOffset++
		},
		"timestamp":   func(value *Event) { value.Timestamp = value.Timestamp.Add(time.Nanosecond) },
		"raw message": func(value *Event) { value.rawMessage = `{"message":"changed"}` },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			changed := base
			mutate(&changed)
			changedID, eventErr := EventID(changed)
			if eventErr != nil {
				t.Fatalf("EventID() error = %v", eventErr)
			}
			if changedID == baseID {
				t.Fatalf("identity change did not change ID: %q", changedID)
			}
		})
	}
}

func TestEventIDIgnoresNonIdentityFields(t *testing.T) {
	t.Parallel()

	base := validIDEvent()
	baseID, err := EventID(base)
	if err != nil {
		t.Fatalf("base EventID() error = %v", err)
	}

	tests := map[string]func(*Event){
		"sequence":     func(value *Event) { value.Sequence = 99 },
		"level":        func(value *Event) { value.Level = "ERROR" },
		"message body": func(value *Event) { value.Message = "another body" },
		"service":      func(value *Event) { value.ServiceName = "worker-service" },
		"test run id":  func(value *Event) { value.TestRunID = "another-run" },
		"namespace":    func(value *Event) { value.Namespace = "another-namespace" },
		"pod name":     func(value *Event) { value.PodName = "another-pod-name" },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			changed := base
			mutate(&changed)
			changedID, eventErr := EventID(changed)
			if eventErr != nil {
				t.Fatalf("EventID() error = %v", eventErr)
			}
			if changedID != baseID {
				t.Fatalf("non-identity field changed ID: %q != %q", changedID, baseID)
			}
		})
	}
}

func TestEventIDIgnoresFilebeatOuterTimestamp(t *testing.T) {
	t.Parallel()

	base, err := ParseFilebeat([]byte(validFilebeatPayload))
	if err != nil {
		t.Fatalf("base ParseFilebeat() error = %v", err)
	}
	changedPayload := strings.Replace(
		validFilebeatPayload,
		`"@timestamp":"2000-01-01T00:00:00Z"`,
		`"@timestamp":"2099-12-31T23:59:59Z"`,
		1,
	)
	changed, err := ParseFilebeat([]byte(changedPayload))
	if err != nil {
		t.Fatalf("changed ParseFilebeat() error = %v", err)
	}

	baseID, err := EventID(base)
	if err != nil {
		t.Fatalf("base EventID() error = %v", err)
	}
	changedID, err := EventID(changed)
	if err != nil {
		t.Fatalf("changed EventID() error = %v", err)
	}
	if changedID != baseID {
		t.Fatalf("outer timestamp changed ID: %q != %q", changedID, baseID)
	}
}

func TestEventIDLengthPrefixPreventsFieldBoundaryCollision(t *testing.T) {
	t.Parallel()

	left := validIDEvent()
	left.PodUID = "ab"
	left.ContainerID = "c"
	right := validIDEvent()
	right.PodUID = "a"
	right.ContainerID = "bc"

	leftID, err := EventID(left)
	if err != nil {
		t.Fatalf("left EventID() error = %v", err)
	}
	rightID, err := EventID(right)
	if err != nil {
		t.Fatalf("right EventID() error = %v", err)
	}
	if leftID == rightID {
		t.Fatalf("different field boundaries collided: %q", leftID)
	}
}

func TestEventIDRejectsMissingStableIdentity(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mutate func(*Event)
		field  string
	}{
		"pod uid": {
			mutate: func(value *Event) { value.PodUID = "" },
			field:  "kubernetes.pod.uid",
		},
		"container id": {
			mutate: func(value *Event) { value.ContainerID = "" },
			field:  "container.id",
		},
		"file path": {
			mutate: func(value *Event) { value.LogFilePath = "" },
			field:  "log.file.path",
		},
		"negative offset": {
			mutate: func(value *Event) { value.LogOffset = -1 },
			field:  "log.offset",
		},
		"timestamp": {
			mutate: func(value *Event) { value.Timestamp = time.Time{} },
			field:  "@timestamp",
		},
		"raw message": {
			mutate: func(value *Event) { value.rawMessage = "" },
			field:  "message",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			input := validIDEvent()
			test.mutate(&input)
			_, err := EventID(input)
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

func TestEventIDFormat(t *testing.T) {
	t.Parallel()

	got, err := EventID(validIDEvent())
	if err != nil {
		t.Fatalf("EventID() error = %v", err)
	}
	if !strings.HasPrefix(got, "sha256:") || len(got) != len("sha256:")+64 {
		t.Fatalf("EventID() returned invalid format %q", got)
	}
}

func validIDEvent() Event {
	return Event{
		Timestamp:   time.Date(2026, 8, 3, 8, 22, 16, 507475585, time.UTC),
		PodUID:      "pod-uid",
		ContainerID: "container-id",
		LogFilePath: "/var/log/containers/api.log",
		LogOffset:   42,
		rawMessage:  `{"message":"log event"}`,
	}
}
