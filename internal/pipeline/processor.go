// Package pipeline 编排单条 Kafka 日志记录的解析、幂等写入和显式确认。
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

// Config 保存单条处理外部调用所需的稳定配置。
// 两个超时彼此独立，避免 Elasticsearch 写入耗尽 Kafka 提交的全部时间预算。
type Config struct {
	Index         string
	WriteTimeout  time.Duration
	CommitTimeout time.Duration
}

// BulkWriter 是 pipeline 使用的 Elasticsearch 外部写入边界。
// 实现必须按输入顺序返回可信逐项结果；出现任何不可信响应时返回错误和 nil 结果。
// 返回错误不得包含原始日志正文，避免上层包装后泄漏业务载荷。
type BulkWriter interface {
	CreateBatch(
		context.Context,
		string,
		[]elasticsearch.Document,
	) ([]elasticsearch.CreateResult, error)
}

// RecordCommitter 是 pipeline 使用的 Kafka 显式提交边界。
// 实现只有在 Broker 确认提交后才能返回 nil。
// 返回错误不得包含原始日志正文；定位信息只使用 topic、partition 和 offset。
type RecordCommitter interface {
	Commit(context.Context, kafka.Record) error
}

// Processor 串联一条 Kafka 记录的纯逻辑转换和两个外部边界。
// 它不负责 Poll 循环、重试、DLQ、健康状态或客户端关闭。
type Processor struct {
	config    Config
	writer    BulkWriter
	committer RecordCommitter
	now       func() time.Time
}

type retryableError interface {
	error
	Retryable() bool
}

// New 校验依赖和超时，并创建单记录处理器。
func New(config Config, writer BulkWriter, committer RecordCommitter) (*Processor, error) {
	return newProcessor(config, writer, committer, time.Now)
}

func newProcessor(
	config Config,
	writer BulkWriter,
	committer RecordCommitter,
	now func() time.Time,
) (*Processor, error) {
	validatedConfig, err := validateProcessingDependencies(config, writer, now)
	if err != nil {
		return nil, err
	}
	if committer == nil {
		return nil, errors.New("Kafka RecordCommitter 不能为空")
	}

	return &Processor{
		config:    validatedConfig,
		writer:    writer,
		committer: committer,
		now:       now,
	}, nil
}

func validateProcessingDependencies(
	config Config,
	writer BulkWriter,
	now func() time.Time,
) (Config, error) {
	config.Index = strings.TrimSpace(config.Index)
	if config.Index == "" {
		return Config{}, errors.New("Elasticsearch 索引名不能为空")
	}
	if config.WriteTimeout <= 0 {
		return Config{}, errors.New("Elasticsearch 写入超时必须大于 0")
	}
	if config.CommitTimeout <= 0 {
		return Config{}, errors.New("Kafka 提交超时必须大于 0")
	}
	if writer == nil {
		return Config{}, errors.New("Elasticsearch BulkWriter 不能为空")
	}
	if now == nil {
		return Config{}, errors.New("处理器时钟不能为空")
	}
	return config, nil
}

// Process 依次解析、构造文档、执行一次 Bulk create，并按逐项结果决定是否提交。
// 传入的 record 会原样交给 Commit，以保留 internal/kafka 的不透明待确认令牌。
// 返回 nil 错误只可能对应 created/duplicate 且 Committed=true。
func (processor *Processor) Process(
	ctx context.Context,
	record kafka.Record,
) (Result, error) {
	if ctx == nil {
		return Result{Kind: ResultSystemFailure}, errors.New("处理 context 不能为空")
	}
	if processor == nil || processor.writer == nil || processor.committer == nil || processor.now == nil {
		return Result{Kind: ResultSystemFailure}, errors.New("pipeline Processor 未初始化")
	}
	if err := ctx.Err(); err != nil {
		return Result{Kind: ResultCanceled}, recordError(record, "开始处理前 context 已取消", err)
	}

	normalized, err := event.ParseFilebeat(record.Value)
	if err != nil {
		kind := ResultSystemFailure
		if errors.Is(err, event.ErrInvalid) {
			kind = ResultInvalid
		}
		return Result{Kind: kind}, recordError(record, "解析 Filebeat 事件", err)
	}

	document, err := elasticsearch.NewDocument(normalized, processor.now())
	if err != nil {
		// ParseFilebeat 成功后再违反文档不变量，说明代码或时钟配置有问题，
		// 不能把它降级成普通毒消息并在未来错误推进位点。
		return Result{Kind: ResultSystemFailure}, recordError(
			record,
			"构造 Elasticsearch 文档",
			err,
		)
	}

	writeContext, cancelWrite := context.WithTimeout(ctx, processor.config.WriteTimeout)
	results, err := processor.writer.CreateBatch(
		writeContext,
		processor.config.Index,
		[]elasticsearch.Document{document},
	)
	cancelWrite()
	if err != nil {
		return Result{Kind: classifyWriteError(err)}, recordError(
			record,
			"写入 Elasticsearch",
			err,
		)
	}
	if len(results) != 1 {
		return Result{Kind: ResultSystemFailure}, recordError(
			record,
			"校验 Elasticsearch 逐项结果",
			fmt.Errorf("结果数量为 %d，期望 1", len(results)),
		)
	}

	writeResult := results[0]
	result := Result{ElasticsearchResult: writeResult.Kind}
	switch writeResult.Kind {
	case elasticsearch.ResultCreated:
		result.Kind = ResultCreated
	case elasticsearch.ResultDuplicate:
		result.Kind = ResultDuplicate
	case elasticsearch.ResultRetryableFailure:
		result.Kind = ResultRetryableFailure
		return result, itemResultError(record, writeResult)
	case elasticsearch.ResultSystemFailure:
		result.Kind = ResultSystemFailure
		return result, itemResultError(record, writeResult)
	default:
		result.Kind = ResultSystemFailure
		return result, recordError(
			record,
			"校验 Elasticsearch 逐项结果",
			fmt.Errorf("未知结果类型 %q", writeResult.Kind),
		)
	}

	if err := ctx.Err(); err != nil {
		result.Kind = ResultCommitFailure
		return result, recordError(record, "提交 Kafka 位点前 context 已取消", err)
	}
	commitContext, cancelCommit := context.WithTimeout(ctx, processor.config.CommitTimeout)
	err = processor.committer.Commit(commitContext, record)
	cancelCommit()
	if err != nil {
		result.Kind = ResultCommitFailure
		return result, recordError(record, "提交 Kafka 位点", err)
	}

	result.Committed = true
	return result, nil
}

func classifyWriteError(err error) ResultKind {
	if errors.Is(err, context.Canceled) {
		return ResultCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ResultRetryableFailure
	}

	var classified retryableError
	if errors.As(err, &classified) && classified.Retryable() {
		return ResultRetryableFailure
	}
	return ResultSystemFailure
}

func itemResultError(record kafka.Record, result elasticsearch.CreateResult) error {
	return recordError(
		record,
		"处理 Elasticsearch 逐项结果",
		fmt.Errorf(
			"类型 %q，HTTP %d，错误类型 %q",
			result.Kind,
			result.StatusCode,
			result.ErrorType,
		),
	)
}

func recordError(record kafka.Record, action string, err error) error {
	return fmt.Errorf(
		"%s，Kafka 记录 %q/%d/%d: %w",
		action,
		record.Topic,
		record.Partition,
		record.Offset,
		err,
	)
}
