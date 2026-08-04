package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBuildBulkBodyUsesCreateActionsAndStableIDs(t *testing.T) {
	t.Parallel()

	firstEvent := newTestEvent(t)
	secondEvent := firstEvent
	secondEvent.LogOffset++

	ingestedAt := time.Date(2026, 8, 4, 3, 4, 5, 0, time.UTC)
	first, err := NewDocument(firstEvent, ingestedAt)
	if err != nil {
		t.Fatalf("first NewDocument() error = %v", err)
	}
	second, err := NewDocument(secondEvent, ingestedAt)
	if err != nil {
		t.Fatalf("second NewDocument() error = %v", err)
	}

	body, err := buildBulkBody(
		"logs-stage3-2026.08.04",
		[]Document{first, second},
	)
	if err != nil {
		t.Fatalf("buildBulkBody() error = %v", err)
	}
	if len(body) == 0 || body[len(body)-1] != '\n' {
		t.Fatal("bulk body must end with a newline")
	}

	lines := bytes.Split(body, []byte{'\n'})
	if len(lines) != 5 || len(lines[4]) != 0 {
		t.Fatalf("bulk body has %d split lines, want four lines plus terminator", len(lines))
	}

	documents := []Document{first, second}
	for index, document := range documents {
		var action struct {
			Create struct {
				Index string `json:"_index"`
				ID    string `json:"_id"`
			} `json:"create"`
		}
		if err := json.Unmarshal(lines[index*2], &action); err != nil {
			t.Fatalf("action %d is invalid JSON: %v", index, err)
		}
		if action.Create.Index != "logs-stage3-2026.08.04" {
			t.Fatalf("action %d index = %q", index, action.Create.Index)
		}
		if action.Create.ID != document.id {
			t.Fatalf("action %d ID = %q, want %q", index, action.Create.ID, document.id)
		}

		var source map[string]any
		if err := json.Unmarshal(lines[index*2+1], &source); err != nil {
			t.Fatalf("source %d is invalid JSON: %v", index, err)
		}
		if source["event_id"] != document.id {
			t.Fatalf("source %d event_id = %v, want %q", index, source["event_id"], document.id)
		}
	}

	if bytes.Contains(lines[0], []byte(`"index"`)) {
		t.Fatalf("bulk action used index instead of create: %s", lines[0])
	}
}

func TestBuildBulkBodyValidatesIndexAndDocuments(t *testing.T) {
	t.Parallel()

	document, err := NewDocument(newTestEvent(t), time.Now())
	if err != nil {
		t.Fatalf("NewDocument() error = %v", err)
	}

	for _, index := range []string{
		"",
		"logs-stage3-",
		"logs-stage3-*",
		"logs-stage3-UPPER",
		"other-2026.08.04",
	} {
		index := index
		t.Run(fmt.Sprintf("index=%q", index), func(t *testing.T) {
			t.Parallel()

			_, err := buildBulkBody(index, []Document{document})
			if err == nil {
				t.Fatal("buildBulkBody() error = nil, want an error")
			}
		})
	}

	if _, err := buildBulkBody("logs-stage3-valid", []Document{{}}); err == nil {
		t.Fatal("buildBulkBody() accepted a zero-value document")
	}
	body, err := buildBulkBody("logs-stage3-valid", nil)
	if err != nil {
		t.Fatalf("buildBulkBody(nil) error = %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("buildBulkBody(nil) returned %d bytes, want 0", len(body))
	}
}

func TestParseBulkResponseClassifiesEveryItemInOrder(t *testing.T) {
	t.Parallel()

	documents := testDocuments(t, 6)
	items := []map[string]any{
		bulkResponseItem(documents[0].id, 201, ""),
		bulkResponseItem(documents[1].id, 409, "version_conflict_engine_exception"),
		bulkResponseItem(documents[2].id, 429, "es_rejected_execution_exception"),
		bulkResponseItem(documents[3].id, 503, "unavailable_shards_exception"),
		bulkResponseItem(documents[4].id, 400, "document_parsing_exception"),
		bulkResponseItem(documents[5].id, 403, "security_exception"),
	}
	body, err := json.Marshal(map[string]any{
		"errors": true,
		"items":  items,
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	results, err := parseBulkResponse(bytes.NewReader(body), documents)
	if err != nil {
		t.Fatalf("parseBulkResponse() error = %v", err)
	}
	wantKinds := []ResultKind{
		ResultCreated,
		ResultDuplicate,
		ResultRetryableFailure,
		ResultRetryableFailure,
		ResultSystemFailure,
		ResultSystemFailure,
	}
	for index, result := range results {
		if result.ID != documents[index].id {
			t.Fatalf("result %d ID = %q, want %q", index, result.ID, documents[index].id)
		}
		if result.Kind != wantKinds[index] {
			t.Fatalf("result %d kind = %q, want %q", index, result.Kind, wantKinds[index])
		}
	}
	if results[1].ErrorType != "version_conflict_engine_exception" {
		t.Fatalf("duplicate ErrorType = %q", results[1].ErrorType)
	}
}

func TestClassifyBulkItemUsesConservativeFailureRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    int
		errorType string
		want      ResultKind
	}{
		{name: "created", status: 201, want: ResultCreated},
		{name: "duplicate", status: 409, errorType: "version_conflict_engine_exception", want: ResultDuplicate},
		{name: "unexpected conflict", status: 409, errorType: "cluster_block_exception", want: ResultSystemFailure},
		{name: "too many requests", status: 429, errorType: "es_rejected_execution_exception", want: ResultRetryableFailure},
		{name: "internal error", status: 500, errorType: "exception", want: ResultRetryableFailure},
		{name: "bad gateway", status: 502, errorType: "exception", want: ResultRetryableFailure},
		{name: "unavailable", status: 503, errorType: "exception", want: ResultRetryableFailure},
		{name: "gateway timeout", status: 504, errorType: "exception", want: ResultRetryableFailure},
		{name: "document parsing", status: 400, errorType: "document_parsing_exception", want: ResultSystemFailure},
		{name: "mapping conflict", status: 400, errorType: "mapper_parsing_exception", want: ResultSystemFailure},
		{name: "strict mapping", status: 400, errorType: "strict_dynamic_mapping_exception", want: ResultSystemFailure},
		{name: "authorization", status: 403, errorType: "security_exception", want: ResultSystemFailure},
		{name: "missing index", status: 404, errorType: "index_not_found_exception", want: ResultSystemFailure},
		{name: "unexpected success", status: 200, want: ResultSystemFailure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := classifyBulkItem(test.status, test.errorType); got != test.want {
				t.Fatalf("classifyBulkItem(%d, %q) = %q, want %q", test.status, test.errorType, got, test.want)
			}
		})
	}
}

func TestParseBulkResponseRejectsUntrustworthyCorrelation(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	validItem := bulkResponseItem(document.id, 201, "")
	tests := map[string]string{
		"malformed JSON":       `{`,
		"missing item":         `{"errors":false,"items":[]}`,
		"extra item":           fmt.Sprintf(`{"errors":false,"items":[%s,%s]}`, mustJSON(t, validItem), mustJSON(t, validItem)),
		"wrong action":         fmt.Sprintf(`{"errors":false,"items":[{"index":{"_id":%q,"status":201}}]}`, document.id),
		"mismatched ID":        `{"errors":false,"items":[{"create":{"_id":"wrong","status":201}}]}`,
		"missing item error":   fmt.Sprintf(`{"errors":true,"items":[{"create":{"_id":%q,"status":429}}]}`, document.id),
		"inconsistent summary": fmt.Sprintf(`{"errors":false,"items":[{"create":{"_id":%q,"status":429,"error":{"type":"es_rejected_execution_exception"}}}]}`, document.id),
		"trailing JSON":        fmt.Sprintf(`{"errors":false,"items":[%s]} {}`, mustJSON(t, validItem)),
	}

	for name, body := range tests {
		body := body
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			results, err := parseBulkResponse(strings.NewReader(body), []Document{document})
			if err == nil {
				t.Fatalf("parseBulkResponse() results = %#v, error = nil", results)
			}
			if results != nil {
				t.Fatalf("parseBulkResponse() returned partial results %#v", results)
			}
		})
	}
}

func testDocuments(t *testing.T, count int) []Document {
	t.Helper()

	base := newTestEvent(t)
	documents := make([]Document, count)
	for index := range documents {
		input := base
		input.LogOffset += int64(index)
		document, err := NewDocument(input, time.Date(2026, 8, 4, 3, 4, 5, 0, time.UTC))
		if err != nil {
			t.Fatalf("NewDocument(%d) error = %v", index, err)
		}
		documents[index] = document
	}
	return documents
}

func bulkResponseItem(id string, status int, errorType string) map[string]any {
	item := map[string]any{
		"_id":    id,
		"status": status,
	}
	if errorType != "" {
		item["error"] = map[string]any{"type": errorType, "reason": "不进入结果的测试详情"}
	}
	return map[string]any{"create": item}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return string(encoded)
}

func TestResultKindValuesRemainStableForMetrics(t *testing.T) {
	t.Parallel()

	got := []ResultKind{
		ResultCreated,
		ResultDuplicate,
		ResultRetryableFailure,
		ResultSystemFailure,
	}
	want := []ResultKind{"created", "duplicate", "retryable_failure", "system_failure"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result kinds = %#v, want %#v", got, want)
	}
}
