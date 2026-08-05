package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

func TestBatchRunnerUsesBatchFastPath(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	records := validKafkaBatch(3)
	pollCalls := 0
	batchCalls := 0
	singleCalls := 0
	runner := newTestBatchRunner(t, 500,
		func(ctx context.Context, limit int) ([]kafka.Record, error) {
			pollCalls++
			if pollCalls == 1 {
				if limit != 500 {
					t.Fatalf("batch limit = %d，期望 500", limit)
				}
				return records, nil
			}
			cancel()
			return nil, ctx.Err()
		},
		func(_ context.Context, got []kafka.Record) (BatchDeliveryResult, error) {
			batchCalls++
			if !reflect.DeepEqual(got, records) {
				t.Fatalf("batch records = %#v", got)
			}
			return BatchDeliveryResult{
				LastResult: BatchResult{Kind: ResultCreated, Created: 3, Committed: true},
				Attempts:   1,
			}, nil
		},
		func(context.Context, kafka.Record) (DeliveryResult, error) {
			singleCalls++
			return DeliveryResult{}, nil
		},
		func(context.Context, kafka.Record, DeliveryResult, error) (DeadLetterResult, error) {
			return DeadLetterResult{}, nil
		},
	)

	if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v，期望 context.Canceled", err)
	}
	if pollCalls != 2 || batchCalls != 1 || singleCalls != 0 {
		t.Fatalf("poll/batch/single = %d/%d/%d，期望 2/1/0", pollCalls, batchCalls, singleCalls)
	}
}

func TestBatchRunnerFallsBackToOrderedSingleRecordAndDLQ(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	records := validKafkaBatch(3)
	pollCalls := 0
	order := make([]int64, 0, 3)
	dlqCalls := 0
	runner := newTestBatchRunner(t, 100,
		func(ctx context.Context, _ int) ([]kafka.Record, error) {
			pollCalls++
			if pollCalls == 1 {
				return records, nil
			}
			cancel()
			return nil, ctx.Err()
		},
		func(context.Context, []kafka.Record) (BatchDeliveryResult, error) {
			cause := &event.ValidationError{Field: "message", Reason: "不能为空"}
			return BatchDeliveryResult{LastResult: BatchResult{Kind: ResultInvalid}, Attempts: 1},
				&BatchFallbackError{RecordIndex: 1, err: cause}
		},
		func(_ context.Context, record kafka.Record) (DeliveryResult, error) {
			order = append(order, record.Offset)
			if record.Offset == records[1].Offset {
				return DeliveryResult{LastResult: Result{Kind: ResultInvalid}, Attempts: 1},
					&event.ValidationError{Field: "message", Reason: "不能为空"}
			}
			return DeliveryResult{
				LastResult: Result{
					Kind:                ResultCreated,
					ElasticsearchResult: elasticsearch.ResultCreated,
					Committed:           true,
				},
				Attempts: 1,
			}, nil
		},
		func(
			_ context.Context,
			record kafka.Record,
			_ DeliveryResult,
			_ error,
		) (DeadLetterResult, error) {
			dlqCalls++
			if record.Offset != records[1].Offset {
				t.Fatalf("DLQ offset = %d", record.Offset)
			}
			return DeadLetterResult{Published: true, Committed: true}, nil
		},
	)

	if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v，期望 context.Canceled", err)
	}
	wantOrder := []int64{records[0].Offset, records[1].Offset, records[2].Offset}
	if !reflect.DeepEqual(order, wantOrder) || dlqCalls != 1 {
		t.Fatalf("single order/DLQ = %v/%d，期望 %v/1", order, dlqCalls, wantOrder)
	}
}

func TestBatchRunnerStopsOnUnresolvedBatch(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("batch delivery exhausted")
	singleCalls := 0
	runner := newTestBatchRunner(t, 100,
		func(context.Context, int) ([]kafka.Record, error) { return validKafkaBatch(2), nil },
		func(context.Context, []kafka.Record) (BatchDeliveryResult, error) {
			return BatchDeliveryResult{
				LastResult: BatchResult{Kind: ResultRetryableFailure},
				Attempts:   deliveryMaxAttempts,
				Exhausted:  true,
			}, wantErr
		},
		func(context.Context, kafka.Record) (DeliveryResult, error) {
			singleCalls++
			return DeliveryResult{}, nil
		},
		func(context.Context, kafka.Record, DeliveryResult, error) (DeadLetterResult, error) {
			return DeadLetterResult{}, nil
		},
	)

	if err := runner.Run(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v，期望 %v", err, wantErr)
	}
	if singleCalls != 0 {
		t.Fatalf("未解决批次触发单记录次数 = %d，期望 0", singleCalls)
	}
}

func TestNewBatchRunnerValidatesBatchSize(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := newBatchRunner(
		0,
		func(context.Context, int) ([]kafka.Record, error) { return nil, nil },
		func(context.Context, []kafka.Record) (BatchDeliveryResult, error) {
			return BatchDeliveryResult{}, nil
		},
		func(context.Context, kafka.Record) (DeliveryResult, error) { return DeliveryResult{}, nil },
		func(context.Context, kafka.Record, DeliveryResult, error) (DeadLetterResult, error) {
			return DeadLetterResult{}, nil
		},
		logger,
	)
	if err == nil {
		t.Fatal("零 batch size 期望失败")
	}
}

func newTestBatchRunner(
	t *testing.T,
	batchSize int,
	poll pollBatchFunc,
	deliverBatch deliverBatchFunc,
	deliverSingle deliverRecordFunc,
	handleDeadLetter handleDeadLetterFunc,
) *BatchRunner {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner, err := newBatchRunner(
		batchSize,
		poll,
		deliverBatch,
		deliverSingle,
		handleDeadLetter,
		logger,
	)
	if err != nil {
		t.Fatalf("newBatchRunner() error = %v", err)
	}
	return runner
}
