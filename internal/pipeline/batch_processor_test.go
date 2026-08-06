package pipeline

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

func TestBatchProcessorWritesAndCommitsValidBatchOnce(t *testing.T) {
	t.Parallel()

	records := validKafkaBatch(3)
	writer := &fakeBulkWriter{results: []elasticsearch.CreateResult{
		{Kind: elasticsearch.ResultCreated},
		{Kind: elasticsearch.ResultCreated},
		{Kind: elasticsearch.ResultCreated},
	}}
	committer := &fakeBatchCommitter{}
	processor := newTestBatchProcessor(t, writer, committer)

	result, err := processor.Process(context.Background(), records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.Kind != ResultCreated || result.Created != 3 || result.Duplicate != 0 || !result.Committed {
		t.Fatalf("批次结果 = %#v", result)
	}
	if writer.calls != 1 || writer.documentCount != 3 {
		t.Fatalf("Bulk calls/documents = %d/%d，期望 1/3", writer.calls, writer.documentCount)
	}
	if committer.calls != 1 || !reflect.DeepEqual(committer.records, records) {
		t.Fatalf("批次提交 = calls %d records %#v，期望原始记录", committer.calls, committer.records)
	}
	assertTimeoutBudget(t, "batch write", writer.deadlineBudget, validConfig().WriteTimeout)
	assertTimeoutBudget(t, "batch commit", committer.deadlineBudget, validConfig().CommitTimeout)
}

func TestBatchProcessorAcceptsMixedCreatedAndDuplicateResults(t *testing.T) {
	t.Parallel()

	records := validKafkaBatch(3)
	writer := &fakeBulkWriter{results: []elasticsearch.CreateResult{
		{Kind: elasticsearch.ResultDuplicate},
		{Kind: elasticsearch.ResultCreated},
		{Kind: elasticsearch.ResultDuplicate},
	}}
	committer := &fakeBatchCommitter{}
	processor := newTestBatchProcessor(t, writer, committer)

	result, err := processor.Process(context.Background(), records)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if result.Kind != ResultCreated || result.Created != 1 || result.Duplicate != 2 || !result.Committed {
		t.Fatalf("混合批次结果 = %#v", result)
	}
}

func TestBatchProcessorFallsBackBeforeExternalCallsForInvalidRecord(t *testing.T) {
	t.Parallel()

	records := validKafkaBatch(3)
	records[1].Value = []byte(`{"message":"not-json"}`)
	writer := &fakeBulkWriter{}
	committer := &fakeBatchCommitter{}
	processor := newTestBatchProcessor(t, writer, committer)

	result, err := processor.Process(context.Background(), records)
	if err == nil || !errors.Is(err, event.ErrInvalid) {
		t.Fatalf("Process() error = %v，期望永久无效错误", err)
	}
	var fallback *BatchFallbackError
	if !errors.As(err, &fallback) || fallback.RecordIndex != 1 {
		t.Fatalf("fallback = %#v，期望 index 1", fallback)
	}
	if result.Kind != ResultInvalid || writer.calls != 0 || committer.calls != 0 {
		t.Fatalf("无效批次 result=%#v write/commit=%d/%d", result, writer.calls, committer.calls)
	}
}

func TestBatchProcessorRetriesWholeBatchForRetryableItem(t *testing.T) {
	t.Parallel()

	records := validKafkaBatch(3)
	writer := &fakeBulkWriter{results: []elasticsearch.CreateResult{
		{Kind: elasticsearch.ResultCreated},
		{Kind: elasticsearch.ResultRetryableFailure, StatusCode: 429, ErrorType: "es_rejected_execution_exception"},
		{Kind: elasticsearch.ResultCreated},
	}}
	committer := &fakeBatchCommitter{}
	processor := newTestBatchProcessor(t, writer, committer)

	result, err := processor.Process(context.Background(), records)
	if err == nil || result.Kind != ResultRetryableFailure || result.Committed {
		t.Fatalf("Process() result/error = %#v/%v", result, err)
	}
	if result.Created != 2 || result.Duplicate != 0 || committer.calls != 0 {
		t.Fatalf("部分结果/提交 = %#v/%d", result, committer.calls)
	}
}

func TestBatchProcessorPreservesAcceptedCountsAfterCommitFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("batch commit unavailable")
	records := validKafkaBatch(2)
	writer := &fakeBulkWriter{results: []elasticsearch.CreateResult{
		{Kind: elasticsearch.ResultCreated},
		{Kind: elasticsearch.ResultDuplicate},
	}}
	committer := &fakeBatchCommitter{err: wantErr}
	processor := newTestBatchProcessor(t, writer, committer)

	result, err := processor.Process(context.Background(), records)
	if !errors.Is(err, wantErr) || result.Kind != ResultCommitFailure || result.Committed {
		t.Fatalf("Process() result/error = %#v/%v", result, err)
	}
	if result.Created != 1 || result.Duplicate != 1 || committer.calls != 1 {
		t.Fatalf("提交失败结果 = %#v，calls=%d", result, committer.calls)
	}
}

func TestBatchProcessorRejectsEmptyBatchAndResultCountMismatch(t *testing.T) {
	t.Parallel()

	processor := newTestBatchProcessor(t, &fakeBulkWriter{}, &fakeBatchCommitter{})
	if result, err := processor.Process(context.Background(), nil); err == nil || result.Kind != ResultSystemFailure {
		t.Fatalf("空批次 result/error = %#v/%v", result, err)
	}

	records := validKafkaBatch(2)
	processor = newTestBatchProcessor(t,
		&fakeBulkWriter{results: []elasticsearch.CreateResult{{Kind: elasticsearch.ResultCreated}}},
		&fakeBatchCommitter{},
	)
	if result, err := processor.Process(context.Background(), records); err == nil || result.Kind != ResultSystemFailure {
		t.Fatalf("结果数量错误 result/error = %#v/%v", result, err)
	}
}

func newTestBatchProcessor(
	t *testing.T,
	writer BulkWriter,
	committer BatchRecordCommitter,
) *BatchProcessor {
	t.Helper()

	processor, err := newBatchProcessor(
		validConfig(),
		writer,
		committer,
		func() time.Time {
			return time.Date(2026, 8, 5, 7, 0, 0, 123456789, time.UTC)
		},
	)
	if err != nil {
		t.Fatalf("newBatchProcessor() error = %v", err)
	}
	return processor
}

func validKafkaBatch(count int) []kafka.Record {
	records := make([]kafka.Record, count)
	for index := range records {
		record := validKafkaRecord()
		record.Offset += int64(index)
		record.Value = []byte(strings.Replace(
			validRecordPayload,
			`"offset":42`,
			`"offset":`+strconv.Itoa(42+index),
			1,
		))
		records[index] = record
	}
	return records
}

type fakeBatchCommitter struct {
	err            error
	waitForContext bool

	calls          int
	records        []kafka.Record
	sawDeadline    bool
	deadlineBudget time.Duration
}

func (committer *fakeBatchCommitter) CommitBatch(
	ctx context.Context,
	records []kafka.Record,
) error {
	committer.calls++
	committer.records = append([]kafka.Record(nil), records...)
	deadline, hasDeadline := ctx.Deadline()
	committer.sawDeadline = hasDeadline
	committer.deadlineBudget = time.Until(deadline)
	if committer.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	return committer.err
}
