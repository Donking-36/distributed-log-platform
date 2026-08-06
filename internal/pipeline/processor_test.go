package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

const validRecordPayload = `{
  "message":"{\"@timestamp\":\"2026-08-04T04:00:00Z\",\"event.sequence\":7,\"log.level\":\"error\",\"message\":\"database unavailable\",\"service.name\":\"api-service\",\"test_run_id\":\"pipeline-test\"}",
  "kubernetes":{
    "namespace":"stage3-logs",
    "labels":{"service":"api-service"},
    "pod":{"name":"api-service-abc","uid":"pod-uid-001"}
  },
  "container":{"id":"container-id-001"},
  "log":{"offset":42,"file":{"path":"/var/log/containers/api-service.log"}}
}`

func TestProcessorCommitsOnlyAcceptedElasticsearchResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		writeResult   elasticsearch.ResultKind
		wantKind      ResultKind
		wantCommitted bool
		wantError     bool
	}{
		{
			name:          "created",
			writeResult:   elasticsearch.ResultCreated,
			wantKind:      ResultCreated,
			wantCommitted: true,
		},
		{
			name:          "duplicate",
			writeResult:   elasticsearch.ResultDuplicate,
			wantKind:      ResultDuplicate,
			wantCommitted: true,
		},
		{
			name:        "retryable item failure",
			writeResult: elasticsearch.ResultRetryableFailure,
			wantKind:    ResultRetryableFailure,
			wantError:   true,
		},
		{
			name:        "system item failure",
			writeResult: elasticsearch.ResultSystemFailure,
			wantKind:    ResultSystemFailure,
			wantError:   true,
		},
		{
			name:        "unknown item result",
			writeResult: elasticsearch.ResultKind("future_result"),
			wantKind:    ResultSystemFailure,
			wantError:   true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			order := make([]string, 0, 2)
			writer := &fakeBulkWriter{
				results: []elasticsearch.CreateResult{{
					Kind:       test.writeResult,
					StatusCode: 201,
				}},
				order: &order,
			}
			committer := &fakeRecordCommitter{order: &order}
			processor := newTestProcessor(t, writer, committer)
			record := validKafkaRecord()

			result, err := processor.Process(context.Background(), record)
			if (err != nil) != test.wantError {
				t.Fatalf("Process() error = %v, wantError %t", err, test.wantError)
			}
			if result.Kind != test.wantKind || result.Committed != test.wantCommitted {
				t.Fatalf("Process() result = %#v, want kind %q committed %t",
					result, test.wantKind, test.wantCommitted)
			}
			if result.ElasticsearchResult != test.writeResult {
				t.Fatalf("ElasticsearchResult = %q, want %q",
					result.ElasticsearchResult, test.writeResult)
			}
			if writer.calls != 1 || !writer.sawDeadline {
				t.Fatalf("writer calls/deadline = %d/%t, want 1/true",
					writer.calls, writer.sawDeadline)
			}
			config := validConfig()
			if writer.index != config.Index || writer.documentCount != 1 {
				t.Fatalf("writer index/documents = %q/%d, want %q/1",
					writer.index, writer.documentCount, config.Index)
			}
			assertTimeoutBudget(t, "write", writer.deadlineBudget, config.WriteTimeout)

			wantCommitCalls := 0
			wantOrder := []string{"write"}
			if test.wantCommitted {
				wantCommitCalls = 1
				wantOrder = append(wantOrder, "commit")
			}
			if committer.calls != wantCommitCalls {
				t.Fatalf("commit calls = %d, want %d", committer.calls, wantCommitCalls)
			}
			if wantCommitCalls == 1 && !committer.sawDeadline {
				t.Fatal("Commit context has no deadline")
			}
			if wantCommitCalls == 1 {
				assertTimeoutBudget(t, "commit", committer.deadlineBudget, config.CommitTimeout)
				if len(committer.records) != 1 || !reflect.DeepEqual(committer.records[0], record) {
					t.Fatalf("Commit record = %#v, want original %#v", committer.records, record)
				}
			}
			if strings.Join(order, ",") != strings.Join(wantOrder, ",") {
				t.Fatalf("call order = %v, want %v", order, wantOrder)
			}
		})
	}
}

func TestProcessorRejectsPermanentInvalidRecordWithoutExternalCalls(t *testing.T) {
	t.Parallel()

	writer := &fakeBulkWriter{}
	committer := &fakeRecordCommitter{}
	processor := newTestProcessor(t, writer, committer)
	record := validKafkaRecord()
	record.Value = []byte(`{"message":"TOP-SECRET"}`)

	result, err := processor.Process(context.Background(), record)
	if !errors.Is(err, event.ErrInvalid) {
		t.Fatalf("Process() error = %v, want event.ErrInvalid", err)
	}
	if result.Kind != ResultInvalid || result.Committed {
		t.Fatalf("Process() result = %#v, want invalid/uncommitted", result)
	}
	if writer.calls != 0 || committer.calls != 0 {
		t.Fatalf("invalid record caused external calls: write=%d commit=%d",
			writer.calls, committer.calls)
	}
	if strings.Contains(err.Error(), "TOP-SECRET") {
		t.Fatalf("error exposed raw payload: %v", err)
	}
}

func TestProcessorTreatsDocumentInvariantFailureAsSystemFailure(t *testing.T) {
	t.Parallel()

	writer := &fakeBulkWriter{}
	committer := &fakeRecordCommitter{}
	processor, err := newProcessor(
		validConfig(),
		writer,
		committer,
		func() time.Time { return time.Time{} },
	)
	if err != nil {
		t.Fatalf("newProcessor() error = %v", err)
	}

	result, err := processor.Process(context.Background(), validKafkaRecord())
	if err == nil {
		t.Fatal("Process() error = nil, want document invariant failure")
	}
	if result.Kind != ResultSystemFailure || result.Committed {
		t.Fatalf("Process() result = %#v, want system_failure/uncommitted", result)
	}
	if writer.calls != 0 || committer.calls != 0 {
		t.Fatalf("document failure caused external calls: write=%d commit=%d",
			writer.calls, committer.calls)
	}
}

func TestProcessorClassifiesWriteErrorsWithoutCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		results   []elasticsearch.CreateResult
		err       error
		wantKind  ResultKind
		wantCause error
	}{
		{
			name:     "retryable request",
			err:      &classifiedTestError{retryable: true},
			wantKind: ResultRetryableFailure,
		},
		{
			name:     "system request",
			err:      &classifiedTestError{retryable: false},
			wantKind: ResultSystemFailure,
		},
		{
			name:      "canceled request",
			err:       context.Canceled,
			wantKind:  ResultCanceled,
			wantCause: context.Canceled,
		},
		{
			name:      "deadline exceeded",
			err:       context.DeadlineExceeded,
			wantKind:  ResultRetryableFailure,
			wantCause: context.DeadlineExceeded,
		},
		{
			name: "error wins over an apparent result",
			results: []elasticsearch.CreateResult{{
				Kind: elasticsearch.ResultCreated,
			}},
			err:      &classifiedTestError{retryable: true},
			wantKind: ResultRetryableFailure,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			writer := &fakeBulkWriter{results: test.results, err: test.err}
			committer := &fakeRecordCommitter{}
			processor := newTestProcessor(t, writer, committer)

			result, err := processor.Process(context.Background(), validKafkaRecord())
			if err == nil {
				t.Fatal("Process() error = nil, want write failure")
			}
			if result.Kind != test.wantKind || result.Committed {
				t.Fatalf("Process() result = %#v, want kind %q uncommitted",
					result, test.wantKind)
			}
			if test.wantCause != nil && !errors.Is(err, test.wantCause) {
				t.Fatalf("Process() error = %v, want cause %v", err, test.wantCause)
			}
			if committer.calls != 0 {
				t.Fatalf("write failure commit calls = %d, want 0", committer.calls)
			}
		})
	}
}

func TestProcessorRejectsUntrustworthySingleRecordResults(t *testing.T) {
	t.Parallel()

	for name, results := range map[string][]elasticsearch.CreateResult{
		"empty": nil,
		"multiple": {
			{Kind: elasticsearch.ResultCreated},
			{Kind: elasticsearch.ResultDuplicate},
		},
	} {
		results := results
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			writer := &fakeBulkWriter{results: results}
			committer := &fakeRecordCommitter{}
			processor := newTestProcessor(t, writer, committer)

			result, err := processor.Process(context.Background(), validKafkaRecord())
			if err == nil {
				t.Fatal("Process() error = nil, want result cardinality failure")
			}
			if result.Kind != ResultSystemFailure || result.Committed {
				t.Fatalf("Process() result = %#v, want system_failure/uncommitted", result)
			}
			if committer.calls != 0 {
				t.Fatalf("untrustworthy result commit calls = %d, want 0", committer.calls)
			}
		})
	}
}

func TestProcessorReportsCommitFailureAfterAcceptedWrite(t *testing.T) {
	t.Parallel()

	for _, writeResult := range []elasticsearch.ResultKind{
		elasticsearch.ResultCreated,
		elasticsearch.ResultDuplicate,
	} {
		writeResult := writeResult
		t.Run(string(writeResult), func(t *testing.T) {
			t.Parallel()

			wantErr := errors.New("coordinator unavailable")
			order := make([]string, 0, 2)
			writer := &fakeBulkWriter{
				results: []elasticsearch.CreateResult{{Kind: writeResult}},
				order:   &order,
			}
			committer := &fakeRecordCommitter{err: wantErr, order: &order}
			processor := newTestProcessor(t, writer, committer)

			result, err := processor.Process(context.Background(), validKafkaRecord())
			if !errors.Is(err, wantErr) {
				t.Fatalf("Process() error = %v, want %v", err, wantErr)
			}
			if result.Kind != ResultCommitFailure || result.Committed {
				t.Fatalf("Process() result = %#v, want commit_failure/uncommitted", result)
			}
			if result.ElasticsearchResult != writeResult {
				t.Fatalf("ElasticsearchResult = %q, want %q",
					result.ElasticsearchResult, writeResult)
			}
			if writer.calls != 1 || committer.calls != 1 {
				t.Fatalf("write/commit calls = %d/%d, want 1/1",
					writer.calls, committer.calls)
			}
			if strings.Join(order, ",") != "write,commit" {
				t.Fatalf("call order = %v, want [write commit]", order)
			}
		})
	}
}

func TestProcessorEnforcesIndependentExternalTimeouts(t *testing.T) {
	t.Parallel()

	t.Run("write timeout", func(t *testing.T) {
		t.Parallel()

		config := validConfig()
		config.WriteTimeout = 25 * time.Millisecond
		writer := &fakeBulkWriter{waitForContext: true}
		committer := &fakeRecordCommitter{}
		processor := newTestProcessorWithConfig(t, config, writer, committer)

		result, err := processor.Process(context.Background(), validKafkaRecord())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Process() error = %v, want context.DeadlineExceeded", err)
		}
		if result.Kind != ResultRetryableFailure || result.Committed {
			t.Fatalf("Process() result = %#v, want retryable_failure/uncommitted", result)
		}
		if writer.calls != 1 || committer.calls != 0 {
			t.Fatalf("write/commit calls = %d/%d, want 1/0", writer.calls, committer.calls)
		}
		assertTimeoutBudget(t, "write", writer.deadlineBudget, config.WriteTimeout)
	})

	t.Run("commit timeout", func(t *testing.T) {
		t.Parallel()

		config := validConfig()
		config.CommitTimeout = 25 * time.Millisecond
		writer := &fakeBulkWriter{results: []elasticsearch.CreateResult{{
			Kind: elasticsearch.ResultCreated,
		}}}
		committer := &fakeRecordCommitter{waitForContext: true}
		processor := newTestProcessorWithConfig(t, config, writer, committer)

		result, err := processor.Process(context.Background(), validKafkaRecord())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Process() error = %v, want context.DeadlineExceeded", err)
		}
		if result.Kind != ResultCommitFailure || result.Committed {
			t.Fatalf("Process() result = %#v, want commit_failure/uncommitted", result)
		}
		if result.ElasticsearchResult != elasticsearch.ResultCreated {
			t.Fatalf("ElasticsearchResult = %q, want created", result.ElasticsearchResult)
		}
		if writer.calls != 1 || committer.calls != 1 {
			t.Fatalf("write/commit calls = %d/%d, want 1/1", writer.calls, committer.calls)
		}
		assertTimeoutBudget(t, "commit", committer.deadlineBudget, config.CommitTimeout)
	})
}

func TestProcessorDoesNotCommitWhenParentIsCanceledAfterAcceptedWrite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	writer := &fakeBulkWriter{
		results:    []elasticsearch.CreateResult{{Kind: elasticsearch.ResultDuplicate}},
		afterWrite: cancel,
	}
	committer := &fakeRecordCommitter{}
	processor := newTestProcessor(t, writer, committer)

	result, err := processor.Process(ctx, validKafkaRecord())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Process() error = %v, want context.Canceled", err)
	}
	if result.Kind != ResultCommitFailure || result.Committed {
		t.Fatalf("Process() result = %#v, want commit_failure/uncommitted", result)
	}
	if result.ElasticsearchResult != elasticsearch.ResultDuplicate {
		t.Fatalf("ElasticsearchResult = %q, want duplicate", result.ElasticsearchResult)
	}
	if writer.calls != 1 || committer.calls != 0 {
		t.Fatalf("write/commit calls = %d/%d, want 1/0", writer.calls, committer.calls)
	}
}

func TestProcessorStopsBeforeWriteWhenParentContextIsCanceled(t *testing.T) {
	t.Parallel()

	writer := &fakeBulkWriter{}
	committer := &fakeRecordCommitter{}
	processor := newTestProcessor(t, writer, committer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := processor.Process(ctx, validKafkaRecord())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Process() error = %v, want context.Canceled", err)
	}
	if result.Kind != ResultCanceled || result.Committed {
		t.Fatalf("Process() result = %#v, want canceled/uncommitted", result)
	}
	if writer.calls != 0 || committer.calls != 0 {
		t.Fatalf("canceled context caused external calls: write=%d commit=%d",
			writer.calls, committer.calls)
	}
}

func TestNewProcessorValidatesDependenciesAndTimeouts(t *testing.T) {
	t.Parallel()

	writer := &fakeBulkWriter{}
	committer := &fakeRecordCommitter{}
	valid := validConfig()

	tests := []struct {
		name      string
		config    Config
		writer    BulkWriter
		committer RecordCommitter
	}{
		{name: "empty index", config: Config{WriteTimeout: time.Second, CommitTimeout: time.Second}, writer: writer, committer: committer},
		{name: "zero write timeout", config: Config{Index: valid.Index, CommitTimeout: time.Second}, writer: writer, committer: committer},
		{name: "zero commit timeout", config: Config{Index: valid.Index, WriteTimeout: time.Second}, writer: writer, committer: committer},
		{name: "nil writer", config: valid, committer: committer},
		{name: "nil committer", config: valid, writer: writer},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := New(test.config, test.writer, test.committer); err == nil {
				t.Fatal("New() error = nil, want validation failure")
			}
		})
	}
}

func newTestProcessor(
	t *testing.T,
	writer BulkWriter,
	committer RecordCommitter,
) *Processor {
	t.Helper()

	return newTestProcessorWithConfig(t, validConfig(), writer, committer)
}

func newTestProcessorWithConfig(
	t *testing.T,
	config Config,
	writer BulkWriter,
	committer RecordCommitter,
) *Processor {
	t.Helper()

	processor, err := newProcessor(
		config,
		writer,
		committer,
		func() time.Time {
			return time.Date(2026, 8, 4, 4, 0, 1, 123456789, time.UTC)
		},
	)
	if err != nil {
		t.Fatalf("newProcessor() error = %v", err)
	}
	return processor
}

func validConfig() Config {
	return Config{
		Index:         "logs-stage3-pipeline-test",
		WriteTimeout:  2 * time.Second,
		CommitTimeout: 7 * time.Second,
	}
}

func validKafkaRecord() kafka.Record {
	return kafka.Record{
		Topic:     "logs.api-service",
		Partition: 1,
		Offset:    17,
		Key:       []byte("pod-uid-001"),
		Value:     []byte(validRecordPayload),
	}
}

type fakeBulkWriter struct {
	results        []elasticsearch.CreateResult
	err            error
	order          *[]string
	afterWrite     func()
	waitForContext bool

	calls          int
	index          string
	documentCount  int
	sawDeadline    bool
	deadlineBudget time.Duration
}

func (writer *fakeBulkWriter) CreateBatch(
	ctx context.Context,
	index string,
	documents []elasticsearch.Document,
) ([]elasticsearch.CreateResult, error) {
	writer.calls++
	writer.index = index
	writer.documentCount = len(documents)
	deadline, hasDeadline := ctx.Deadline()
	writer.sawDeadline = hasDeadline
	writer.deadlineBudget = time.Until(deadline)
	if writer.order != nil {
		*writer.order = append(*writer.order, "write")
	}
	if writer.waitForContext {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if writer.afterWrite != nil {
		writer.afterWrite()
	}
	return writer.results, writer.err
}

type fakeRecordCommitter struct {
	err            error
	order          *[]string
	waitForContext bool

	calls          int
	records        []kafka.Record
	sawDeadline    bool
	deadlineBudget time.Duration
}

func (committer *fakeRecordCommitter) Commit(
	ctx context.Context,
	record kafka.Record,
) error {
	committer.calls++
	committer.records = append(committer.records, record)
	deadline, hasDeadline := ctx.Deadline()
	committer.sawDeadline = hasDeadline
	committer.deadlineBudget = time.Until(deadline)
	if committer.order != nil {
		*committer.order = append(*committer.order, "commit")
	}
	if committer.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	return committer.err
}

func assertTimeoutBudget(t *testing.T, name string, got, want time.Duration) {
	t.Helper()

	const tolerance = 500 * time.Millisecond
	if got <= want-tolerance || got > want {
		t.Fatalf("%s timeout budget = %v, want (%v, %v]",
			name, got, want-tolerance, want)
	}
}

type classifiedTestError struct {
	retryable bool
}

func (err *classifiedTestError) Error() string {
	return "classified test error"
}

func (err *classifiedTestError) Retryable() bool {
	return err.retryable
}
