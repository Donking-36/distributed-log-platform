package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

const deadLetterSchemaVersion = 1

// DeadLetterConfig 保存死信发布和源位点提交的独立时间预算。
type DeadLetterConfig struct {
	PublishTimeout time.Duration
	CommitTimeout  time.Duration
}

// DeadLetterWriter 是 pipeline 使用的 Kafka 死信写入边界。
// 实现只有在 Broker 确认写入后才能返回 nil，且错误不得包含 key 或原始载荷。
type DeadLetterWriter interface {
	Write(context.Context, []byte, []byte) error
}

// DeadLetterResult 分别记录死信发布和源位点提交是否获得明确确认。
type DeadLetterResult struct {
	Published bool
	Committed bool
}

// DeadLetterHandler 编排永久无效记录的隔离与确认。
// 它不负责识别普通故障、重试 DLQ、Poll 循环、健康状态或客户端关闭。
type DeadLetterHandler struct {
	config    DeadLetterConfig
	writer    DeadLetterWriter
	committer RecordCommitter
}

// NewDeadLetterHandler 校验依赖和两个独立超时并创建死信处理器。
func NewDeadLetterHandler(
	config DeadLetterConfig,
	writer DeadLetterWriter,
	committer RecordCommitter,
) (*DeadLetterHandler, error) {
	if config.PublishTimeout <= 0 {
		return nil, errors.New("Kafka 死信写入超时必须大于 0")
	}
	if config.CommitTimeout <= 0 {
		return nil, errors.New("Kafka 死信记录提交超时必须大于 0")
	}
	if writer == nil {
		return nil, errors.New("Kafka DeadLetterWriter 不能为空")
	}
	if committer == nil {
		return nil, errors.New("Kafka RecordCommitter 不能为空")
	}
	return &DeadLetterHandler{config: config, writer: writer, committer: committer}, nil
}

// Handle 把永久无效记录编码后写入 DLQ，再提交同一条源记录。
// 发布失败或发布确认不确定时绝不提交；发布成功但提交失败时保留 Published=true。
func (handler *DeadLetterHandler) Handle(
	ctx context.Context,
	record kafka.Record,
	failure DeliveryResult,
	cause error,
) (DeadLetterResult, error) {
	if ctx == nil {
		return DeadLetterResult{}, errors.New("死信处理 context 不能为空")
	}
	if handler == nil || handler.writer == nil || handler.committer == nil {
		return DeadLetterResult{}, errors.New("pipeline DeadLetterHandler 未初始化")
	}
	if err := ctx.Err(); err != nil {
		return DeadLetterResult{}, recordError(record, "开始死信处理前 context 已取消", err)
	}

	validationError, err := validateDeadLetterFailure(record, failure, cause)
	if err != nil {
		return DeadLetterResult{}, recordError(record, "校验死信处理契约", err)
	}
	envelope := deadLetterEnvelope{
		SchemaVersion: deadLetterSchemaVersion,
		Source: deadLetterSource{
			Topic:     record.Topic,
			Partition: record.Partition,
			Offset:    record.Offset,
		},
		Error: deadLetterError{
			Category: "event_validation",
			Field:    validationError.Field,
			Summary:  validationError.Field + " 不符合日志事件契约",
		},
		Attempts:  failure.Attempts,
		TestRunID: event.BestEffortTestRunID(record.Value),
		OriginalPayload: deadLetterPayload{
			Encoding: "base64",
			Data:     base64.StdEncoding.EncodeToString(record.Value),
		},
	}
	value, err := json.Marshal(envelope)
	if err != nil {
		return DeadLetterResult{}, recordError(record, "编码死信记录", err)
	}
	key := fmt.Appendf(nil,
		"v%d|%d:%s|%d|%d",
		deadLetterSchemaVersion,
		len(record.Topic),
		record.Topic,
		record.Partition,
		record.Offset,
	)

	publishContext, cancelPublish := context.WithTimeout(ctx, handler.config.PublishTimeout)
	err = handler.writer.Write(publishContext, key, value)
	cancelPublish()
	if err != nil {
		return DeadLetterResult{}, recordError(record, "写入 Kafka 死信主题", err)
	}
	result := DeadLetterResult{Published: true}
	if err := ctx.Err(); err != nil {
		return result, recordError(record, "提交死信源位点前 context 已取消", err)
	}

	commitContext, cancelCommit := context.WithTimeout(ctx, handler.config.CommitTimeout)
	err = handler.committer.Commit(commitContext, record)
	cancelCommit()
	if err != nil {
		return result, recordError(record, "提交死信源位点", err)
	}
	result.Committed = true
	return result, nil
}

func validateDeadLetterFailure(
	record kafka.Record,
	failure DeliveryResult,
	cause error,
) (*event.ValidationError, error) {
	if strings.TrimSpace(record.Topic) == "" || record.Topic != strings.TrimSpace(record.Topic) {
		return nil, errors.New("Kafka 源主题不能为空或包含首尾空白")
	}
	if record.Partition < 0 || record.Offset < 0 {
		return nil, errors.New("Kafka 源分区和位点不能为负数")
	}
	if failure.LastResult.Kind != ResultInvalid {
		return nil, fmt.Errorf("结果 %q 不是永久无效记录", failure.LastResult.Kind)
	}
	if failure.Attempts < 1 {
		return nil, errors.New("永久无效记录必须至少经过一次处理尝试")
	}
	if failure.Exhausted {
		return nil, errors.New("永久无效记录不能标记为重试耗尽")
	}
	if failure.LastResult.Committed {
		return nil, errors.New("已提交记录不能再次进入死信路径")
	}
	if failure.LastResult.ElasticsearchResult != "" {
		return nil, errors.New("永久无效记录不能包含 Elasticsearch 结果")
	}
	if !errors.Is(cause, event.ErrInvalid) {
		return nil, errors.New("死信原因不是永久事件校验错误")
	}
	var validationError *event.ValidationError
	if !errors.As(cause, &validationError) || validationError == nil {
		return nil, errors.New("死信原因缺少结构化事件校验错误")
	}
	if !isSafeValidationField(validationError.Field) {
		return nil, errors.New("事件校验字段不是安全字段名")
	}
	return validationError, nil
}

func isSafeValidationField(field string) bool {
	if field == "" || len(field) > 128 {
		return false
	}
	for _, character := range field {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("@._-", character) {
			continue
		}
		return false
	}
	return true
}

type deadLetterEnvelope struct {
	SchemaVersion   int               `json:"schema_version"`
	Source          deadLetterSource  `json:"source"`
	Error           deadLetterError   `json:"error"`
	Attempts        int               `json:"attempts"`
	TestRunID       string            `json:"test_run_id,omitempty"`
	OriginalPayload deadLetterPayload `json:"original_payload"`
}

type deadLetterSource struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type deadLetterError struct {
	Category string `json:"category"`
	Field    string `json:"field"`
	Summary  string `json:"summary"`
}

type deadLetterPayload struct {
	Encoding string `json:"encoding"`
	Data     string `json:"data"`
}
