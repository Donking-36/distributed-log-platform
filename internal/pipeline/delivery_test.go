package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

func TestDeliveryCycleReturnsAcceptedResultWithoutRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind elasticsearch.ResultKind
		want ResultKind
	}{
		{name: "created", kind: elasticsearch.ResultCreated, want: ResultCreated},
		{name: "duplicate", kind: elasticsearch.ResultDuplicate, want: ResultDuplicate},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			writer := &fakeBulkWriter{results: []elasticsearch.CreateResult{{Kind: test.kind}}}
			committer := &fakeRecordCommitter{}
			processor := newTestProcessor(t, writer, committer)
			cycle, err := NewDeliveryCycle(processor)
			if err != nil {
				t.Fatalf("NewDeliveryCycle() error = %v", err)
			}

			result, err := cycle.Deliver(context.Background(), validKafkaRecord())
			if err != nil {
				t.Fatalf("Deliver() error = %v", err)
			}
			if result.LastResult.Kind != test.want || !result.LastResult.Committed {
				t.Fatalf("Deliver() last result = %#v, want %q/committed", result.LastResult, test.want)
			}
			if result.Attempts != 1 || result.Exhausted {
				t.Fatalf("Deliver() attempts/exhausted = %d/%t, want 1/false", result.Attempts, result.Exhausted)
			}
			if writer.calls != 1 || committer.calls != 1 {
				t.Fatalf("write/commit calls = %d/%d, want 1/1", writer.calls, committer.calls)
			}
		})
	}
}

func TestNewDeliveryCycleUsesDefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	writer := &scriptedBulkWriter{steps: []deliveryWriteStep{
		{err: &deliveryRetryableError{cause: errors.New("temporary failure")}},
		{results: []elasticsearch.CreateResult{{
			Kind:       elasticsearch.ResultCreated,
			StatusCode: 201,
		}}},
	}}
	committer := &scriptedCommitter{errors: []error{nil}}
	processor := newTestProcessor(t, writer, committer)
	cycle, err := NewDeliveryCycle(processor)
	if err != nil {
		t.Fatalf("NewDeliveryCycle() error = %v", err)
	}

	result, err := cycle.Deliver(context.Background(), validKafkaRecord())
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if result.LastResult.Kind != ResultCreated || !result.LastResult.Committed {
		t.Fatalf("Deliver() last result = %#v, want created/committed", result.LastResult)
	}
	if result.Attempts != 2 || result.Exhausted {
		t.Fatalf("Deliver() attempts/exhausted = %d/%t, want 2/false",
			result.Attempts, result.Exhausted)
	}
	if writer.calls != 2 || committer.calls != 1 {
		t.Fatalf("write/commit calls = %d/%d, want 2/1", writer.calls, committer.calls)
	}
}

func TestDeliveryCycleUsesExactADRBudgetAndSucceedsOnSixthAttempt(t *testing.T) {
	t.Parallel()

	steps := make([]deliveryWriteStep, 0, 6)
	for attempt := 1; attempt <= 5; attempt++ {
		steps = append(steps, deliveryWriteStep{err: &deliveryRetryableError{
			cause: fmt.Errorf("temporary write failure %d", attempt),
		}})
	}
	steps = append(steps, deliveryWriteStep{results: []elasticsearch.CreateResult{{
		Kind:       elasticsearch.ResultCreated,
		StatusCode: 201,
	}}})

	writer := &scriptedBulkWriter{steps: steps}
	committer := &scriptedCommitter{errors: []error{nil}}
	processor := newTestProcessor(t, writer, committer)
	wantCaps := []time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		4 * time.Second,
	}
	wantWaits := []time.Duration{
		0,
		125 * time.Millisecond,
		750 * time.Millisecond,
		1500 * time.Millisecond,
		4 * time.Second,
	}
	var gotCaps []time.Duration
	var gotWaits []time.Duration
	cycle := newTestDeliveryCycle(
		t,
		processor,
		func(limit time.Duration) time.Duration {
			gotCaps = append(gotCaps, limit)
			return wantWaits[len(gotCaps)-1]
		},
		func(ctx context.Context, delay time.Duration) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			gotWaits = append(gotWaits, delay)
			return nil
		},
	)

	result, err := cycle.Deliver(context.Background(), validKafkaRecord())
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if result.LastResult.Kind != ResultCreated || !result.LastResult.Committed {
		t.Fatalf("Deliver() last result = %#v, want created/committed", result.LastResult)
	}
	if result.Attempts != 6 || result.Exhausted {
		t.Fatalf("Deliver() attempts/exhausted = %d/%t, want 6/false", result.Attempts, result.Exhausted)
	}
	if writer.calls != 6 || committer.calls != 1 {
		t.Fatalf("write/commit calls = %d/%d, want 6/1", writer.calls, committer.calls)
	}
	if !reflect.DeepEqual(gotCaps, wantCaps) {
		t.Fatalf("jitter caps = %v, want %v", gotCaps, wantCaps)
	}
	if !reflect.DeepEqual(gotWaits, wantWaits) {
		t.Fatalf("wait durations = %v, want %v", gotWaits, wantWaits)
	}
}

func TestDeliveryCycleExhaustsRetryableFailureWithoutCommit(t *testing.T) {
	t.Parallel()

	steps := make([]deliveryWriteStep, 0, 6)
	causes := make([]error, 0, 6)
	for attempt := 1; attempt <= 6; attempt++ {
		cause := fmt.Errorf("temporary write failure %d", attempt)
		causes = append(causes, cause)
		steps = append(steps, deliveryWriteStep{err: &deliveryRetryableError{cause: cause}})
	}
	writer := &scriptedBulkWriter{steps: steps}
	committer := &scriptedCommitter{}
	processor := newTestProcessor(t, writer, committer)
	var caps []time.Duration
	var waits []time.Duration
	cycle := newTestDeliveryCycle(
		t,
		processor,
		func(limit time.Duration) time.Duration {
			caps = append(caps, limit)
			return limit
		},
		func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		},
	)

	result, err := cycle.Deliver(context.Background(), validKafkaRecord())
	if err == nil || !errors.Is(err, causes[5]) {
		t.Fatalf("Deliver() error = %v, want final retryable cause", err)
	}
	if result.LastResult.Kind != ResultRetryableFailure || result.LastResult.Committed {
		t.Fatalf("Deliver() last result = %#v, want retryable/uncommitted", result.LastResult)
	}
	if result.Attempts != 6 || !result.Exhausted {
		t.Fatalf("Deliver() attempts/exhausted = %d/%t, want 6/true", result.Attempts, result.Exhausted)
	}
	if writer.calls != 6 || committer.calls != 0 || len(caps) != 5 || len(waits) != 5 {
		t.Fatalf("write/commit/jitter/wait calls = %d/%d/%d/%d, want 6/0/5/5",
			writer.calls, committer.calls, len(caps), len(waits))
	}
	if strings.Contains(err.Error(), validRecordPayload) {
		t.Fatalf("error exposed raw payload: %v", err)
	}
}

func TestDeliveryCycleReplaysFullProcessAfterCommitFailure(t *testing.T) {
	t.Parallel()

	commitFailure := errors.New("commit response lost")
	writer := &scriptedBulkWriter{steps: []deliveryWriteStep{
		{results: []elasticsearch.CreateResult{{Kind: elasticsearch.ResultCreated, StatusCode: 201}}},
		{results: []elasticsearch.CreateResult{{Kind: elasticsearch.ResultDuplicate, StatusCode: 409}}},
	}}
	committer := &scriptedCommitter{errors: []error{commitFailure, nil}}
	clockCalls := 0
	processor, err := newProcessor(
		validConfig(),
		writer,
		committer,
		func() time.Time {
			clockCalls++
			return time.Date(2026, 8, 4, 4, 0, clockCalls, 0, time.UTC)
		},
	)
	if err != nil {
		t.Fatalf("newProcessor() error = %v", err)
	}
	var caps []time.Duration
	var waits []time.Duration
	cycle := newTestDeliveryCycle(
		t,
		processor,
		func(limit time.Duration) time.Duration {
			caps = append(caps, limit)
			return limit / 2
		},
		func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		},
	)
	record := validKafkaRecord()

	result, err := cycle.Deliver(context.Background(), record)
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if result.LastResult.Kind != ResultDuplicate || !result.LastResult.Committed {
		t.Fatalf("Deliver() last result = %#v, want duplicate/committed", result.LastResult)
	}
	if result.Attempts != 2 || result.Exhausted {
		t.Fatalf("Deliver() attempts/exhausted = %d/%t, want 2/false", result.Attempts, result.Exhausted)
	}
	if writer.calls != 2 || committer.calls != 2 {
		t.Fatalf("write/commit calls = %d/%d, want 2/2", writer.calls, committer.calls)
	}
	if len(writer.documents) != 2 || len(writer.documents[0]) != 1 || len(writer.documents[1]) != 1 {
		t.Fatalf("written document batches = %#v, want two single-document batches", writer.documents)
	}
	firstDocument := writer.documents[0][0]
	secondDocument := writer.documents[1][0]
	if firstDocument.ID() == "" || firstDocument.ID() != secondDocument.ID() {
		t.Fatalf("retried document IDs = %q/%q, want same non-empty ID",
			firstDocument.ID(), secondDocument.ID())
	}
	if reflect.DeepEqual(firstDocument, secondDocument) {
		t.Fatal("retried documents unexpectedly equal; test clock should change ingested_at")
	}
	if clockCalls != 2 {
		t.Fatalf("processor clock calls = %d, want 2", clockCalls)
	}
	if !reflect.DeepEqual(committer.records, []kafka.Record{record, record}) {
		t.Fatalf("committed records = %#v, want original record twice", committer.records)
	}
	if !reflect.DeepEqual(caps, []time.Duration{250 * time.Millisecond}) ||
		!reflect.DeepEqual(waits, []time.Duration{125 * time.Millisecond}) {
		t.Fatalf("jitter caps/waits = %v/%v, want [250ms]/[125ms]", caps, waits)
	}
}

func TestDeliveryCycleExhaustsCommitFailureAndPreservesLastResult(t *testing.T) {
	t.Parallel()

	steps := make([]deliveryWriteStep, 0, 6)
	commitErrors := make([]error, 0, 6)
	for attempt := 1; attempt <= 6; attempt++ {
		kind := elasticsearch.ResultDuplicate
		status := 409
		if attempt == 1 {
			kind = elasticsearch.ResultCreated
			status = 201
		}
		steps = append(steps, deliveryWriteStep{results: []elasticsearch.CreateResult{{
			Kind: kind, StatusCode: status,
		}}})
		commitErrors = append(commitErrors, fmt.Errorf("commit failure %d", attempt))
	}
	writer := &scriptedBulkWriter{steps: steps}
	committer := &scriptedCommitter{errors: commitErrors}
	processor := newTestProcessor(t, writer, committer)
	waits := 0
	cycle := newTestDeliveryCycle(
		t,
		processor,
		func(limit time.Duration) time.Duration { return limit },
		func(_ context.Context, _ time.Duration) error {
			waits++
			return nil
		},
	)

	result, err := cycle.Deliver(context.Background(), validKafkaRecord())
	if err == nil || !errors.Is(err, commitErrors[5]) {
		t.Fatalf("Deliver() error = %v, want final commit cause", err)
	}
	if result.LastResult.Kind != ResultCommitFailure ||
		result.LastResult.ElasticsearchResult != elasticsearch.ResultDuplicate ||
		result.LastResult.Committed {
		t.Fatalf("Deliver() last result = %#v, want commit_failure/duplicate/uncommitted", result.LastResult)
	}
	if result.Attempts != 6 || !result.Exhausted || writer.calls != 6 || committer.calls != 6 || waits != 5 {
		t.Fatalf("attempts/exhausted/write/commit/waits = %d/%t/%d/%d/%d, want 6/true/6/6/5",
			result.Attempts, result.Exhausted, writer.calls, committer.calls, waits)
	}
}

func TestDeliveryCycleDoesNotRetryTerminalResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		record     kafka.Record
		writeStep  *deliveryWriteStep
		wantKind   ResultKind
		wantWrites int
	}{
		{
			name: "permanently invalid",
			record: func() kafka.Record {
				record := validKafkaRecord()
				record.Value = []byte(`{"message":"TOP-SECRET"}`)
				return record
			}(),
			wantKind: ResultInvalid,
		},
		{
			name:   "system failure",
			record: validKafkaRecord(),
			writeStep: &deliveryWriteStep{results: []elasticsearch.CreateResult{{
				Kind: elasticsearch.ResultSystemFailure, StatusCode: 400,
			}}},
			wantKind:   ResultSystemFailure,
			wantWrites: 1,
		},
		{
			name:       "canceled external call",
			record:     validKafkaRecord(),
			writeStep:  &deliveryWriteStep{err: context.Canceled},
			wantKind:   ResultCanceled,
			wantWrites: 1,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var steps []deliveryWriteStep
			if test.writeStep != nil {
				steps = []deliveryWriteStep{*test.writeStep}
			}
			writer := &scriptedBulkWriter{steps: steps}
			committer := &scriptedCommitter{}
			processor := newTestProcessor(t, writer, committer)
			jitterCalls := 0
			waitCalls := 0
			cycle := newTestDeliveryCycle(
				t,
				processor,
				func(time.Duration) time.Duration { jitterCalls++; return 0 },
				func(context.Context, time.Duration) error { waitCalls++; return nil },
			)

			result, err := cycle.Deliver(context.Background(), test.record)
			if err == nil {
				t.Fatal("Deliver() error = nil, want terminal failure")
			}
			if result.LastResult.Kind != test.wantKind || result.LastResult.Committed {
				t.Fatalf("Deliver() last result = %#v, want %q/uncommitted", result.LastResult, test.wantKind)
			}
			if result.Attempts != 1 || result.Exhausted {
				t.Fatalf("Deliver() attempts/exhausted = %d/%t, want 1/false", result.Attempts, result.Exhausted)
			}
			if writer.calls != test.wantWrites || committer.calls != 0 || jitterCalls != 0 || waitCalls != 0 {
				t.Fatalf("write/commit/jitter/wait calls = %d/%d/%d/%d, want %d/0/0/0",
					writer.calls, committer.calls, jitterCalls, waitCalls, test.wantWrites)
			}
		})
	}
}

func TestDeliveryCycleGivesParentCancellationPriorityOverCommitRetry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	writer := &fakeBulkWriter{
		results:    []elasticsearch.CreateResult{{Kind: elasticsearch.ResultCreated, StatusCode: 201}},
		afterWrite: cancel,
	}
	committer := &fakeRecordCommitter{}
	processor := newTestProcessor(t, writer, committer)
	jitterCalls := 0
	waitCalls := 0
	cycle := newTestDeliveryCycle(
		t,
		processor,
		func(time.Duration) time.Duration { jitterCalls++; return 0 },
		func(context.Context, time.Duration) error { waitCalls++; return nil },
	)

	result, err := cycle.Deliver(ctx, validKafkaRecord())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Deliver() error = %v, want context.Canceled", err)
	}
	if result.LastResult.Kind != ResultCommitFailure ||
		result.LastResult.ElasticsearchResult != elasticsearch.ResultCreated ||
		result.Attempts != 1 || result.Exhausted {
		t.Fatalf("Deliver() result = %#v, want one non-exhausted commit failure", result)
	}
	if writer.calls != 1 || committer.calls != 0 || jitterCalls != 0 || waitCalls != 0 {
		t.Fatalf("write/commit/jitter/wait calls = %d/%d/%d/%d, want 1/0/0/0",
			writer.calls, committer.calls, jitterCalls, waitCalls)
	}
}

func TestDeliveryCycleCancellationInterruptsRetryWait(t *testing.T) {
	t.Parallel()

	writer := &scriptedBulkWriter{steps: []deliveryWriteStep{
		{err: &deliveryRetryableError{cause: errors.New("temporary failure")}},
		{results: []elasticsearch.CreateResult{{Kind: elasticsearch.ResultCreated}}},
	}}
	committer := &scriptedCommitter{}
	processor := newTestProcessor(t, writer, committer)
	waitStarted := make(chan struct{})
	cycle := newTestDeliveryCycle(
		t,
		processor,
		func(time.Duration) time.Duration { return 250 * time.Millisecond },
		func(ctx context.Context, _ time.Duration) error {
			close(waitStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		result DeliveryResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := cycle.Deliver(ctx, validKafkaRecord())
		done <- outcome{result: result, err: err}
	}()

	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("Deliver() did not enter retry wait")
	}
	cancel()
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("Deliver() error = %v, want context.Canceled", got.err)
		}
		if got.result.Attempts != 1 || got.result.Exhausted ||
			got.result.LastResult.Kind != ResultRetryableFailure {
			t.Fatalf("Deliver() result = %#v, want one non-exhausted retryable failure", got.result)
		}
	case <-time.After(time.Second):
		t.Fatal("Deliver() did not stop after context cancellation")
	}
	if writer.calls != 1 || committer.calls != 0 {
		t.Fatalf("write/commit calls = %d/%d, want 1/0", writer.calls, committer.calls)
	}
}

func TestWaitForRetryHonorsCancellation(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithCancel(context.Background())
	checked := make(chan struct{})
	ctx := &observedErrContext{Context: parent, checked: checked}
	done := make(chan error, 1)
	go func() {
		done <- waitForRetry(ctx, time.Minute)
	}()
	select {
	case <-checked:
		// 确认入口 Err 检查已返回 nil 后再取消，避免只测试到提前返回分支。
	case <-time.After(time.Second):
		t.Fatal("waitForRetry() did not check context before waiting")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waitForRetry() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitForRetry() did not stop after context cancellation")
	}
}

func TestJitterWithSourceCoversFullRange(t *testing.T) {
	t.Parallel()

	const limit = 4 * time.Second
	tests := []struct {
		name string
		draw func(int64) int64
		want time.Duration
	}{
		{name: "zero", draw: func(int64) int64 { return 0 }, want: 0},
		{name: "inclusive cap", draw: func(bound int64) int64 { return bound - 1 }, want: limit},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := jitterWithSource(limit, func(bound int64) int64 {
				if bound != int64(limit)+1 {
					t.Fatalf("random bound = %d, want %d", bound, int64(limit)+1)
				}
				return test.draw(bound)
			})
			if got != test.want {
				t.Fatalf("jitterWithSource() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestFullJitterStaysWithinLimit(t *testing.T) {
	t.Parallel()

	const limit = 250 * time.Millisecond
	for sample := 0; sample < 32; sample++ {
		got := fullJitter(limit)
		if got < 0 || got > limit {
			t.Fatalf("fullJitter() = %v, want [0,%v]", got, limit)
		}
	}
}

func TestValidateAttemptResultRejectsInconsistentContracts(t *testing.T) {
	t.Parallel()

	failure := errors.New("attempt failure")
	tests := []struct {
		name      string
		result    Result
		err       error
		wantRetry bool
		wantError bool
	}{
		{name: "created", result: Result{
			Kind: ResultCreated, ElasticsearchResult: elasticsearch.ResultCreated, Committed: true,
		}},
		{name: "duplicate", result: Result{
			Kind: ResultDuplicate, ElasticsearchResult: elasticsearch.ResultDuplicate, Committed: true,
		}},
		{name: "retryable", result: Result{Kind: ResultRetryableFailure}, err: failure, wantRetry: true},
		{name: "commit failure", result: Result{
			Kind: ResultCommitFailure, ElasticsearchResult: elasticsearch.ResultCreated,
		}, err: failure, wantRetry: true},
		{name: "invalid", result: Result{Kind: ResultInvalid}, err: failure},
		{name: "system", result: Result{Kind: ResultSystemFailure}, err: failure},
		{name: "canceled", result: Result{Kind: ResultCanceled}, err: failure},
		{name: "success with error", result: Result{
			Kind: ResultCreated, ElasticsearchResult: elasticsearch.ResultCreated, Committed: true,
		}, err: failure, wantError: true},
		{name: "success without commit", result: Result{Kind: ResultCreated}, wantError: true},
		{name: "success without commit and with error", result: Result{
			Kind: ResultCreated, ElasticsearchResult: elasticsearch.ResultCreated,
		}, err: failure, wantError: true},
		{name: "failure without error", result: Result{Kind: ResultRetryableFailure}, wantError: true},
		{name: "failure marked committed", result: Result{
			Kind: ResultSystemFailure, Committed: true,
		}, wantError: true},
		{name: "created with mismatched Elasticsearch result", result: Result{
			Kind: ResultCreated, ElasticsearchResult: elasticsearch.ResultDuplicate, Committed: true,
		}, wantError: true},
		{name: "duplicate with mismatched Elasticsearch result", result: Result{
			Kind: ResultDuplicate, ElasticsearchResult: elasticsearch.ResultCreated, Committed: true,
		}, wantError: true},
		{name: "commit failure without accepted Elasticsearch result", result: Result{
			Kind: ResultCommitFailure, ElasticsearchResult: elasticsearch.ResultRetryableFailure,
		}, err: failure, wantError: true},
		{name: "unknown result", result: Result{Kind: ResultKind("future")}, err: failure, wantError: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			retry, err := validateAttemptResult(test.result, test.err)
			if retry != test.wantRetry || (err != nil) != test.wantError {
				t.Fatalf("validateAttemptResult() = %t/%v, want %t/error=%t",
					retry, err, test.wantRetry, test.wantError)
			}
		})
	}
}

func TestNewDeliveryCycleValidatesDependenciesAndContext(t *testing.T) {
	t.Parallel()

	if _, err := NewDeliveryCycle(nil); err == nil {
		t.Fatal("NewDeliveryCycle(nil) error = nil")
	}
	processor := newTestProcessor(t, &fakeBulkWriter{}, &fakeRecordCommitter{})
	if _, err := newDeliveryCycle(processor, nil, waitForRetry); err == nil {
		t.Fatal("newDeliveryCycle(nil jitter) error = nil")
	}
	if _, err := newDeliveryCycle(processor, fullJitter, nil); err == nil {
		t.Fatal("newDeliveryCycle(nil wait) error = nil")
	}
	cycle, err := NewDeliveryCycle(processor)
	if err != nil {
		t.Fatalf("NewDeliveryCycle() error = %v", err)
	}
	result, err := cycle.Deliver(nil, validKafkaRecord())
	if err == nil || result.Attempts != 0 || result.LastResult.Kind != ResultSystemFailure {
		t.Fatalf("Deliver(nil) = %#v/%v, want zero-attempt system failure", result, err)
	}
}

func newTestDeliveryCycle(
	t *testing.T,
	processor *Processor,
	jitter jitterFunc,
	wait waitFunc,
) *DeliveryCycle {
	t.Helper()
	cycle, err := newDeliveryCycle(processor, jitter, wait)
	if err != nil {
		t.Fatalf("newDeliveryCycle() error = %v", err)
	}
	return cycle
}

type deliveryWriteStep struct {
	results []elasticsearch.CreateResult
	err     error
}

type scriptedBulkWriter struct {
	steps     []deliveryWriteStep
	calls     int
	documents [][]elasticsearch.Document
}

func (writer *scriptedBulkWriter) CreateBatch(
	_ context.Context,
	_ string,
	documents []elasticsearch.Document,
) ([]elasticsearch.CreateResult, error) {
	if writer.calls >= len(writer.steps) {
		return nil, fmt.Errorf("unexpected write call %d", writer.calls+1)
	}
	writer.documents = append(writer.documents, append([]elasticsearch.Document(nil), documents...))
	step := writer.steps[writer.calls]
	writer.calls++
	return step.results, step.err
}

type scriptedCommitter struct {
	errors  []error
	calls   int
	records []kafka.Record
}

func (committer *scriptedCommitter) Commit(_ context.Context, record kafka.Record) error {
	if committer.calls >= len(committer.errors) {
		return fmt.Errorf("unexpected commit call %d", committer.calls+1)
	}
	committer.records = append(committer.records, record)
	err := committer.errors[committer.calls]
	committer.calls++
	return err
}

type deliveryRetryableError struct {
	cause error
}

func (err *deliveryRetryableError) Error() string {
	return err.cause.Error()
}

func (err *deliveryRetryableError) Unwrap() error {
	return err.cause
}

func (err *deliveryRetryableError) Retryable() bool {
	return true
}

type observedErrContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (ctx *observedErrContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.checked) })
	return err
}
