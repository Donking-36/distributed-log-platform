package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

func TestBatchDeliveryCycleReplaysWholeBatchAfterCommitFailure(t *testing.T) {
	t.Parallel()

	records := validKafkaBatch(3)
	wantErr := errors.New("commit response lost")
	attempt := 0
	waits := make([]time.Duration, 0, 1)
	cycle := newTestBatchDeliveryCycle(t,
		func(context.Context, []kafka.Record) (BatchResult, error) {
			attempt++
			if attempt == 1 {
				return BatchResult{Kind: ResultCommitFailure, Created: 3}, wantErr
			}
			return BatchResult{Kind: ResultDuplicate, Duplicate: 3, Committed: true}, nil
		},
		func(limit time.Duration) time.Duration { return limit },
		func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		},
	)

	result, err := cycle.Deliver(context.Background(), records)
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if result.Attempts != 2 || result.Exhausted || !result.LastResult.Committed ||
		result.LastResult.Duplicate != 3 {
		t.Fatalf("批量投递结果 = %#v", result)
	}
	if len(waits) != 1 || waits[0] != deliveryInitialDelay {
		t.Fatalf("等待 = %v，期望 [%v]", waits, deliveryInitialDelay)
	}
}

func TestBatchDeliveryCycleReturnsFallbackWithoutRetry(t *testing.T) {
	t.Parallel()

	records := validKafkaBatch(2)
	cause := &event.ValidationError{Field: "message", Reason: "不能为空"}
	fallback := &BatchFallbackError{RecordIndex: 1, err: cause}
	attempts := 0
	cycle := newTestBatchDeliveryCycle(t,
		func(context.Context, []kafka.Record) (BatchResult, error) {
			attempts++
			return BatchResult{Kind: ResultInvalid}, fallback
		},
		func(time.Duration) time.Duration { t.Fatal("fallback 不应生成抖动"); return 0 },
		func(context.Context, time.Duration) error { t.Fatal("fallback 不应等待"); return nil },
	)

	result, err := cycle.Deliver(context.Background(), records)
	if !errors.Is(err, event.ErrInvalid) || attempts != 1 || result.Attempts != 1 || result.Exhausted {
		t.Fatalf("Deliver() result/error/attempts = %#v/%v/%d", result, err, attempts)
	}
}

func TestBatchDeliveryCycleExhaustsRetryableBatch(t *testing.T) {
	t.Parallel()

	records := validKafkaBatch(2)
	wantErr := errors.New("Elasticsearch overloaded")
	attempts := 0
	waits := 0
	cycle := newTestBatchDeliveryCycle(t,
		func(context.Context, []kafka.Record) (BatchResult, error) {
			attempts++
			return BatchResult{Kind: ResultRetryableFailure}, wantErr
		},
		func(time.Duration) time.Duration { return 0 },
		func(context.Context, time.Duration) error { waits++; return nil },
	)

	result, err := cycle.Deliver(context.Background(), records)
	if !errors.Is(err, wantErr) || attempts != deliveryMaxAttempts || waits != deliveryMaxAttempts-1 ||
		!result.Exhausted || result.Attempts != deliveryMaxAttempts {
		t.Fatalf("Deliver() result/error attempts/waits = %#v/%v %d/%d",
			result, err, attempts, waits)
	}
}

func TestBatchDeliveryCycleRejectsInconsistentSuccess(t *testing.T) {
	t.Parallel()

	records := validKafkaBatch(2)
	cycle := newTestBatchDeliveryCycle(t,
		func(context.Context, []kafka.Record) (BatchResult, error) {
			return BatchResult{Kind: ResultCreated, Created: 1, Committed: true}, nil
		},
		fullJitter,
		waitForRetry,
	)

	if _, err := cycle.Deliver(context.Background(), records); err == nil {
		t.Fatal("提交数量与批次不一致时期望失败")
	}
}

func newTestBatchDeliveryCycle(
	t *testing.T,
	process processBatchFunc,
	jitter jitterFunc,
	wait waitFunc,
) *BatchDeliveryCycle {
	t.Helper()
	cycle, err := newBatchDeliveryCycle(process, jitter, wait)
	if err != nil {
		t.Fatalf("newBatchDeliveryCycle() error = %v", err)
	}
	return cycle
}
