package pipeline

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

func TestDeadLetterHandlerPublishesBeforeCommittingInvalidRecord(t *testing.T) {
	t.Parallel()

	record := invalidKafkaRecordWithSensitivePayload()
	failure, cause := invalidDelivery(t, record)
	order := make([]string, 0, 2)
	writer := &fakeDeadLetterWriter{order: &order}
	committer := &fakeRecordCommitter{order: &order}
	handler := newTestDeadLetterHandler(t, validDeadLetterConfig(), writer, committer)

	result, err := handler.Handle(context.Background(), record, failure, cause)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if !result.Published || !result.Committed {
		t.Fatalf("Handle() result = %#v，期望 published/committed", result)
	}
	if strings.Join(order, ",") != "publish,commit" {
		t.Fatalf("调用顺序 = %v，期望 [publish commit]", order)
	}
	if writer.calls != 1 || committer.calls != 1 {
		t.Fatalf("publish/commit calls = %d/%d，期望 1/1", writer.calls, committer.calls)
	}
	if !writer.sawDeadline || !committer.sawDeadline {
		t.Fatalf("publish/commit deadline = %t/%t，期望 true/true",
			writer.sawDeadline, committer.sawDeadline)
	}
	config := validDeadLetterConfig()
	assertTimeoutBudget(t, "DLQ publish", writer.deadlineBudget, config.PublishTimeout)
	assertTimeoutBudget(t, "DLQ commit", committer.deadlineBudget, config.CommitTimeout)
	if !reflect.DeepEqual(committer.records, []kafka.Record{record}) {
		t.Fatalf("Commit records = %#v，期望原始记录", committer.records)
	}
	if string(writer.key) != "v1|16:logs.api-service|1|17" {
		t.Fatalf("DLQ key = %q，期望稳定源位置 key", writer.key)
	}
	if strings.Contains(string(writer.value), "TOP-SECRET") {
		t.Fatal("DLQ JSON 未以 Base64 保存原始载荷")
	}

	var envelope deadLetterWireEnvelope
	if err := json.Unmarshal(writer.value, &envelope); err != nil {
		t.Fatalf("解析 DLQ JSON: %v", err)
	}
	if envelope.SchemaVersion != 1 ||
		envelope.Source.Topic != record.Topic ||
		envelope.Source.Partition != record.Partition ||
		envelope.Source.Offset != record.Offset {
		t.Fatalf("DLQ schema/source = %#v", envelope)
	}
	if envelope.Error.Category != "event_validation" ||
		envelope.Error.Field != "service.name" ||
		envelope.Error.Summary != "service.name 不符合日志事件契约" {
		t.Fatalf("DLQ error = %#v", envelope.Error)
	}
	if envelope.Attempts != 1 || envelope.TestRunID != "pipeline-test" {
		t.Fatalf("DLQ attempts/test_run_id = %d/%q，期望 1/pipeline-test",
			envelope.Attempts, envelope.TestRunID)
	}
	if envelope.OriginalPayload.Encoding != "base64" {
		t.Fatalf("payload encoding = %q，期望 base64", envelope.OriginalPayload.Encoding)
	}
	decoded, err := base64.StdEncoding.DecodeString(envelope.OriginalPayload.Data)
	if err != nil {
		t.Fatalf("解码 DLQ payload: %v", err)
	}
	if !reflect.DeepEqual(decoded, record.Value) {
		t.Fatalf("DLQ payload 未无损保留原始字节")
	}
}

func TestDeadLetterHandlerPreservesArbitraryBytesAndOmitsMissingTestRunID(t *testing.T) {
	t.Parallel()

	record := validKafkaRecord()
	record.Value = []byte{0xff, 0xfe, 0x00, 'T', 'O', 'P', '-', 'S', 'E', 'C', 'R', 'E', 'T'}
	failure, cause := invalidDelivery(t, record)
	writer := &fakeDeadLetterWriter{}
	committer := &fakeRecordCommitter{}
	handler := newTestDeadLetterHandler(t, validDeadLetterConfig(), writer, committer)

	result, err := handler.Handle(context.Background(), record, failure, cause)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if !result.Published || !result.Committed {
		t.Fatalf("Handle() result = %#v，期望 published/committed", result)
	}
	if !json.Valid(writer.value) {
		t.Fatalf("DLQ value 不是合法 JSON: %q", writer.value)
	}
	if bytes.Contains(writer.value, []byte(`"test_run_id"`)) {
		t.Fatalf("无法恢复 test_run_id 时仍输出了字段: %s", writer.value)
	}
	if bytes.Contains(writer.value, []byte("TOP-SECRET")) {
		t.Fatal("任意字节载荷未使用 Base64 隔离")
	}

	var envelope deadLetterWireEnvelope
	if err := json.Unmarshal(writer.value, &envelope); err != nil {
		t.Fatalf("解析 DLQ JSON: %v", err)
	}
	if envelope.Error.Field != "payload" ||
		envelope.Error.Summary != "payload 不符合日志事件契约" {
		t.Fatalf("DLQ error = %#v", envelope.Error)
	}
	decoded, err := base64.StdEncoding.DecodeString(envelope.OriginalPayload.Data)
	if err != nil {
		t.Fatalf("解码 DLQ payload: %v", err)
	}
	if !bytes.Equal(decoded, record.Value) {
		t.Fatalf("任意字节载荷未无损往返: got=%v want=%v", decoded, record.Value)
	}
}

func TestDeadLetterHandlerDoesNotExposeValidationReason(t *testing.T) {
	t.Parallel()

	record := validKafkaRecord()
	failure := DeliveryResult{
		LastResult: Result{Kind: ResultInvalid},
		Attempts:   1,
	}
	cause := &event.ValidationError{Field: "message", Reason: "TOP-SECRET"}
	writer := &fakeDeadLetterWriter{}
	handler := newTestDeadLetterHandler(
		t,
		validDeadLetterConfig(),
		writer,
		&fakeRecordCommitter{},
	)

	if _, err := handler.Handle(context.Background(), record, failure, cause); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if bytes.Contains(writer.value, []byte("TOP-SECRET")) {
		t.Fatalf("DLQ JSON 泄漏校验原因: %s", writer.value)
	}
	var envelope deadLetterWireEnvelope
	if err := json.Unmarshal(writer.value, &envelope); err != nil {
		t.Fatalf("解析 DLQ JSON: %v", err)
	}
	if envelope.Error.Summary != "message 不符合日志事件契约" {
		t.Fatalf("DLQ summary = %q", envelope.Error.Summary)
	}
}

func TestDeadLetterHandlerDoesNotCommitWhenPublishFails(t *testing.T) {
	t.Parallel()

	record := invalidKafkaRecordWithSensitivePayload()
	failure, cause := invalidDelivery(t, record)
	wantErr := errors.New("DLQ broker unavailable")
	writer := &fakeDeadLetterWriter{err: wantErr}
	committer := &fakeRecordCommitter{}
	handler := newTestDeadLetterHandler(t, validDeadLetterConfig(), writer, committer)

	result, err := handler.Handle(context.Background(), record, failure, cause)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Handle() error = %v，期望包含 %v", err, wantErr)
	}
	if result.Published || result.Committed {
		t.Fatalf("Handle() result = %#v，期望未发布且未提交", result)
	}
	if writer.calls != 1 || committer.calls != 0 {
		t.Fatalf("publish/commit calls = %d/%d，期望 1/0", writer.calls, committer.calls)
	}
	if strings.Contains(err.Error(), "TOP-SECRET") {
		t.Fatalf("Handle() error 泄漏原始载荷: %v", err)
	}
}

func TestDeadLetterHandlerCancellationNeverCommitsUnconfirmedRecord(t *testing.T) {
	t.Parallel()

	t.Run("开始前取消", func(t *testing.T) {
		record := invalidKafkaRecordWithSensitivePayload()
		failure, cause := invalidDelivery(t, record)
		writer := &fakeDeadLetterWriter{}
		committer := &fakeRecordCommitter{}
		handler := newTestDeadLetterHandler(t, validDeadLetterConfig(), writer, committer)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := handler.Handle(ctx, record, failure, cause)
		if !errors.Is(err, context.Canceled) || result.Published || result.Committed {
			t.Fatalf("Handle() = %#v/%v，期望 canceled 且无外部成功", result, err)
		}
		if writer.calls != 0 || committer.calls != 0 {
			t.Fatalf("publish/commit calls = %d/%d，期望 0/0", writer.calls, committer.calls)
		}
	})

	t.Run("发布超时", func(t *testing.T) {
		record := invalidKafkaRecordWithSensitivePayload()
		failure, cause := invalidDelivery(t, record)
		config := validDeadLetterConfig()
		config.PublishTimeout = 25 * time.Millisecond
		writer := &fakeDeadLetterWriter{waitForContext: true}
		committer := &fakeRecordCommitter{}
		handler := newTestDeadLetterHandler(t, config, writer, committer)

		result, err := handler.Handle(context.Background(), record, failure, cause)
		if !errors.Is(err, context.DeadlineExceeded) || result.Published || result.Committed {
			t.Fatalf("Handle() = %#v/%v，期望 publish timeout", result, err)
		}
		if writer.calls != 1 || committer.calls != 0 {
			t.Fatalf("publish/commit calls = %d/%d，期望 1/0", writer.calls, committer.calls)
		}
	})

	t.Run("发布确认后父级取消", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		record := invalidKafkaRecordWithSensitivePayload()
		failure, cause := invalidDelivery(t, record)
		writer := &fakeDeadLetterWriter{afterWrite: cancel}
		committer := &fakeRecordCommitter{}
		handler := newTestDeadLetterHandler(t, validDeadLetterConfig(), writer, committer)

		result, err := handler.Handle(ctx, record, failure, cause)
		if !errors.Is(err, context.Canceled) || !result.Published || result.Committed {
			t.Fatalf("Handle() = %#v/%v，期望 published/canceled/uncommitted", result, err)
		}
		if writer.calls != 1 || committer.calls != 0 {
			t.Fatalf("publish/commit calls = %d/%d，期望 1/0", writer.calls, committer.calls)
		}
	})
}

func TestDeadLetterHandlerPreservesPublishedStateWhenCommitFails(t *testing.T) {
	t.Parallel()

	record := invalidKafkaRecordWithSensitivePayload()
	failure, cause := invalidDelivery(t, record)
	wantErr := errors.New("commit response lost")
	writer := &fakeDeadLetterWriter{}
	committer := &fakeRecordCommitter{err: wantErr}
	handler := newTestDeadLetterHandler(t, validDeadLetterConfig(), writer, committer)

	result, err := handler.Handle(context.Background(), record, failure, cause)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Handle() error = %v，期望包含 %v", err, wantErr)
	}
	if !result.Published || result.Committed {
		t.Fatalf("Handle() result = %#v，期望 published/uncommitted", result)
	}
	if writer.calls != 1 || committer.calls != 1 {
		t.Fatalf("publish/commit calls = %d/%d，期望 1/1", writer.calls, committer.calls)
	}
}

func TestDeadLetterHandlerPreservesPublishedStateWhenCommitTimesOut(t *testing.T) {
	t.Parallel()

	record := invalidKafkaRecordWithSensitivePayload()
	failure, cause := invalidDelivery(t, record)
	config := validDeadLetterConfig()
	config.CommitTimeout = 25 * time.Millisecond
	writer := &fakeDeadLetterWriter{}
	committer := &fakeRecordCommitter{waitForContext: true}
	handler := newTestDeadLetterHandler(t, config, writer, committer)

	result, err := handler.Handle(context.Background(), record, failure, cause)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Handle() error = %v，期望 context.DeadlineExceeded", err)
	}
	if !result.Published || result.Committed {
		t.Fatalf("Handle() result = %#v，期望 published/uncommitted", result)
	}
	if writer.calls != 1 || committer.calls != 1 {
		t.Fatalf("publish/commit calls = %d/%d，期望 1/1", writer.calls, committer.calls)
	}
}

func TestDeadLetterHandlerRejectsNonPoisonContracts(t *testing.T) {
	t.Parallel()

	record := invalidKafkaRecordWithSensitivePayload()
	validFailure, validCause := invalidDelivery(t, record)
	tests := []struct {
		name    string
		failure DeliveryResult
		cause   error
	}{
		{name: "非永久错误", failure: DeliveryResult{
			LastResult: Result{Kind: ResultSystemFailure}, Attempts: 1,
		}, cause: errors.New("system failure")},
		{name: "无尝试次数", failure: DeliveryResult{
			LastResult: Result{Kind: ResultInvalid}, Attempts: 0,
		}, cause: validCause},
		{name: "已耗尽", failure: DeliveryResult{
			LastResult: Result{Kind: ResultInvalid}, Attempts: 1, Exhausted: true,
		}, cause: validCause},
		{name: "已提交", failure: DeliveryResult{
			LastResult: Result{Kind: ResultInvalid, Committed: true}, Attempts: 1,
		}, cause: validCause},
		{name: "包含 Elasticsearch 结果", failure: DeliveryResult{
			LastResult: Result{
				Kind: ResultInvalid, ElasticsearchResult: elasticsearch.ResultCreated,
			}, Attempts: 1,
		}, cause: validCause},
		{name: "原因不是永久校验错误", failure: validFailure, cause: errors.New("other")},
		{name: "缺少结构化校验错误", failure: validFailure, cause: event.ErrInvalid},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			writer := &fakeDeadLetterWriter{}
			committer := &fakeRecordCommitter{}
			handler := newTestDeadLetterHandler(t, validDeadLetterConfig(), writer, committer)
			result, err := handler.Handle(context.Background(), record, test.failure, test.cause)
			if err == nil || result.Published || result.Committed {
				t.Fatalf("Handle() = %#v/%v，期望契约失败", result, err)
			}
			if writer.calls != 0 || committer.calls != 0 {
				t.Fatalf("非法契约触发外部调用: publish=%d commit=%d",
					writer.calls, committer.calls)
			}
		})
	}
}

func TestNewDeadLetterHandlerValidatesDependenciesAndTimeouts(t *testing.T) {
	t.Parallel()

	writer := &fakeDeadLetterWriter{}
	committer := &fakeRecordCommitter{}
	valid := validDeadLetterConfig()
	tests := []struct {
		name      string
		config    DeadLetterConfig
		writer    DeadLetterWriter
		committer RecordCommitter
	}{
		{name: "zero publish timeout", config: DeadLetterConfig{CommitTimeout: time.Second}, writer: writer, committer: committer},
		{name: "zero commit timeout", config: DeadLetterConfig{PublishTimeout: time.Second}, writer: writer, committer: committer},
		{name: "nil writer", config: valid, committer: committer},
		{name: "nil committer", config: valid, writer: writer},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewDeadLetterHandler(test.config, test.writer, test.committer); err == nil {
				t.Fatal("NewDeadLetterHandler() error = nil")
			}
		})
	}

	handler := newTestDeadLetterHandler(t, valid, writer, committer)
	// nil context 是本测试要覆盖的非法输入，显式类型变量用于区分生产调用。
	var nilContext context.Context
	result, err := handler.Handle(nilContext, validKafkaRecord(), DeliveryResult{}, nil)
	if err == nil || result.Published || result.Committed {
		t.Fatalf("Handle(nil) = %#v/%v，期望失败", result, err)
	}
	var nilHandler *DeadLetterHandler
	result, err = nilHandler.Handle(context.Background(), validKafkaRecord(), DeliveryResult{}, nil)
	if err == nil || result.Published || result.Committed {
		t.Fatalf("nil Handler = %#v/%v，期望失败", result, err)
	}
}

func invalidDelivery(t *testing.T, record kafka.Record) (DeliveryResult, error) {
	t.Helper()
	processor := newTestProcessor(t, &fakeBulkWriter{}, &fakeRecordCommitter{})
	cycle, err := NewDeliveryCycle(processor)
	if err != nil {
		t.Fatalf("NewDeliveryCycle() error = %v", err)
	}
	failure, cause := cycle.Deliver(context.Background(), record)
	if !errors.Is(cause, event.ErrInvalid) || failure.LastResult.Kind != ResultInvalid ||
		failure.Attempts != 1 || failure.Exhausted || failure.LastResult.Committed {
		t.Fatalf("invalid Deliver() = %#v/%v", failure, cause)
	}
	return failure, cause
}

func invalidKafkaRecordWithSensitivePayload() kafka.Record {
	record := validKafkaRecord()
	payload := strings.Replace(validRecordPayload, "database unavailable", "TOP-SECRET", 1)
	payload = strings.Replace(payload, `\"service.name\":\"api-service\"`,
		`\"service.name\":\"worker-service\"`, 1)
	record.Value = []byte(payload)
	return record
}

func validDeadLetterConfig() DeadLetterConfig {
	return DeadLetterConfig{
		PublishTimeout: 3 * time.Second,
		CommitTimeout:  5 * time.Second,
	}
}

func newTestDeadLetterHandler(
	t *testing.T,
	config DeadLetterConfig,
	writer DeadLetterWriter,
	committer RecordCommitter,
) *DeadLetterHandler {
	t.Helper()
	handler, err := NewDeadLetterHandler(config, writer, committer)
	if err != nil {
		t.Fatalf("NewDeadLetterHandler() error = %v", err)
	}
	return handler
}

type fakeDeadLetterWriter struct {
	err            error
	order          *[]string
	waitForContext bool
	afterWrite     func()

	calls          int
	key            []byte
	value          []byte
	sawDeadline    bool
	deadlineBudget time.Duration
}

func (writer *fakeDeadLetterWriter) Write(ctx context.Context, key, value []byte) error {
	writer.calls++
	writer.key = append([]byte(nil), key...)
	writer.value = append([]byte(nil), value...)
	deadline, hasDeadline := ctx.Deadline()
	writer.sawDeadline = hasDeadline
	writer.deadlineBudget = time.Until(deadline)
	if writer.order != nil {
		*writer.order = append(*writer.order, "publish")
	}
	if writer.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	if writer.afterWrite != nil {
		writer.afterWrite()
	}
	return writer.err
}

type deadLetterWireEnvelope struct {
	SchemaVersion int `json:"schema_version"`
	Source        struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Offset    int64  `json:"offset"`
	} `json:"source"`
	Error struct {
		Category string `json:"category"`
		Field    string `json:"field"`
		Summary  string `json:"summary"`
	} `json:"error"`
	Attempts        int    `json:"attempts"`
	TestRunID       string `json:"test_run_id"`
	OriginalPayload struct {
		Encoding string `json:"encoding"`
		Data     string `json:"data"`
	} `json:"original_payload"`
}
