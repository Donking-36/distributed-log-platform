package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

func TestRunnerContinuesAfterCommittedDelivery(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	record := validKafkaRecord()
	order := make([]string, 0, 3)
	pollCalls := 0
	deliverCalls := 0
	handleCalls := 0
	runner := newTestRunner(t,
		func(ctx context.Context) (kafka.Record, error) {
			pollCalls++
			order = append(order, "poll")
			if pollCalls == 1 {
				return record, nil
			}
			cancel()
			return kafka.Record{}, ctx.Err()
		},
		func(context.Context, kafka.Record) (DeliveryResult, error) {
			deliverCalls++
			order = append(order, "deliver")
			return DeliveryResult{
				LastResult: Result{
					Kind:                ResultCreated,
					ElasticsearchResult: elasticsearch.ResultCreated,
					Committed:           true,
				},
				Attempts: 1,
			}, nil
		},
		func(context.Context, kafka.Record, DeliveryResult, error) (DeadLetterResult, error) {
			handleCalls++
			return DeadLetterResult{}, nil
		},
	)

	if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v，期望 context.Canceled", err)
	}
	if pollCalls != 2 || deliverCalls != 1 || handleCalls != 0 {
		t.Fatalf("poll/deliver/dlq = %d/%d/%d，期望 2/1/0",
			pollCalls, deliverCalls, handleCalls)
	}
	if !reflect.DeepEqual(order, []string{"poll", "deliver", "poll"}) {
		t.Fatalf("调用顺序 = %v", order)
	}
}

func TestRunnerRoutesInvalidRecordToDeadLetterBeforeContinuing(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	record := validKafkaRecord()
	cause := &event.ValidationError{Field: "message", Reason: "不能为空"}
	failure := DeliveryResult{LastResult: Result{Kind: ResultInvalid}, Attempts: 1}
	order := make([]string, 0, 4)
	pollCalls := 0
	runner := newTestRunner(t,
		func(ctx context.Context) (kafka.Record, error) {
			pollCalls++
			order = append(order, "poll")
			if pollCalls == 1 {
				return record, nil
			}
			cancel()
			return kafka.Record{}, ctx.Err()
		},
		func(_ context.Context, got kafka.Record) (DeliveryResult, error) {
			order = append(order, "deliver")
			if !reflect.DeepEqual(got, record) {
				t.Fatalf("Deliver record = %#v，期望原始记录", got)
			}
			return failure, cause
		},
		func(
			_ context.Context,
			gotRecord kafka.Record,
			gotFailure DeliveryResult,
			gotCause error,
		) (DeadLetterResult, error) {
			order = append(order, "dlq")
			if !reflect.DeepEqual(gotRecord, record) || !reflect.DeepEqual(gotFailure, failure) ||
				!errors.Is(gotCause, event.ErrInvalid) {
				t.Fatalf("DLQ 参数未保留原始记录/结果/原因")
			}
			return DeadLetterResult{Published: true, Committed: true}, nil
		},
	)

	if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v，期望 context.Canceled", err)
	}
	if !reflect.DeepEqual(order, []string{"poll", "deliver", "dlq", "poll"}) {
		t.Fatalf("调用顺序 = %v", order)
	}
}

func TestRunnerDoesNotPollAfterContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	runner := newTestRunner(t,
		func(context.Context) (kafka.Record, error) {
			calls++
			return kafka.Record{}, nil
		},
		func(context.Context, kafka.Record) (DeliveryResult, error) {
			calls++
			return DeliveryResult{}, nil
		},
		func(context.Context, kafka.Record, DeliveryResult, error) (DeadLetterResult, error) {
			calls++
			return DeadLetterResult{}, nil
		},
	)

	if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v，期望 context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("取消后依赖调用次数 = %d，期望 0", calls)
	}
}

func TestRunnerStopsWithoutPollingPastUnresolvedRecord(t *testing.T) {
	t.Parallel()

	record := validKafkaRecord()
	deliveryErr := errors.New("delivery exhausted")
	dlqErr := errors.New("DLQ unavailable")
	tests := []struct {
		name          string
		failure       DeliveryResult
		deliveryError error
		handleResult  DeadLetterResult
		handleError   error
		wantError     error
		wantHandle    int
	}{
		{
			name: "成功结果缺少提交确认立即停止",
			failure: DeliveryResult{
				LastResult: Result{
					Kind:                ResultCreated,
					ElasticsearchResult: elasticsearch.ResultCreated,
				},
				Attempts: 1,
			},
		},
		{
			name: "非永久错误立即停止",
			failure: DeliveryResult{
				LastResult: Result{Kind: ResultRetryableFailure}, Attempts: 6, Exhausted: true,
			},
			deliveryError: deliveryErr,
			wantError:     deliveryErr,
		},
		{
			name:          "死信写入失败立即停止",
			failure:       DeliveryResult{LastResult: Result{Kind: ResultInvalid}, Attempts: 1},
			deliveryError: &event.ValidationError{Field: "payload", Reason: "无效"},
			handleError:   dlqErr,
			wantError:     dlqErr,
			wantHandle:    1,
		},
		{
			name:          "死信结果未提交立即停止",
			failure:       DeliveryResult{LastResult: Result{Kind: ResultInvalid}, Attempts: 1},
			deliveryError: &event.ValidationError{Field: "payload", Reason: "无效"},
			handleResult:  DeadLetterResult{Published: true},
			wantHandle:    1,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			pollCalls := 0
			handleCalls := 0
			runner := newTestRunner(t,
				func(context.Context) (kafka.Record, error) {
					pollCalls++
					return record, nil
				},
				func(context.Context, kafka.Record) (DeliveryResult, error) {
					return test.failure, test.deliveryError
				},
				func(context.Context, kafka.Record, DeliveryResult, error) (DeadLetterResult, error) {
					handleCalls++
					return test.handleResult, test.handleError
				},
			)

			err := runner.Run(context.Background())
			if err == nil {
				t.Fatal("Run() error = nil")
			}
			if test.wantError != nil && !errors.Is(err, test.wantError) {
				t.Fatalf("Run() error = %v，期望包含 %v", err, test.wantError)
			}
			if pollCalls != 1 || handleCalls != test.wantHandle {
				t.Fatalf("poll/dlq = %d/%d，期望 1/%d", pollCalls, handleCalls, test.wantHandle)
			}
		})
	}
}

func TestNewRunnerValidatesDependencies(t *testing.T) {
	t.Parallel()

	poll := func(context.Context) (kafka.Record, error) { return kafka.Record{}, nil }
	deliver := func(context.Context, kafka.Record) (DeliveryResult, error) {
		return DeliveryResult{}, nil
	}
	handle := func(context.Context, kafka.Record, DeliveryResult, error) (DeadLetterResult, error) {
		return DeadLetterResult{}, nil
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name    string
		poll    pollRecordFunc
		deliver deliverRecordFunc
		handle  handleDeadLetterFunc
		logger  *slog.Logger
	}{
		{name: "nil poll", deliver: deliver, handle: handle, logger: logger},
		{name: "nil deliver", poll: poll, handle: handle, logger: logger},
		{name: "nil handle", poll: poll, deliver: deliver, logger: logger},
		{name: "nil logger", poll: poll, deliver: deliver, handle: handle},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := newRunner(test.poll, test.deliver, test.handle, test.logger); err == nil {
				t.Fatal("newRunner() error = nil")
			}
		})
	}

	var nilRunner *Runner
	if err := nilRunner.Run(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "未初始化") {
		t.Fatalf("nil Runner.Run() error = %v", err)
	}
}

func newTestRunner(
	t *testing.T,
	poll pollRecordFunc,
	deliver deliverRecordFunc,
	handle handleDeadLetterFunc,
) *Runner {
	t.Helper()
	runner, err := newRunner(
		poll,
		deliver,
		handle,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("newRunner() error = %v", err)
	}
	return runner
}
