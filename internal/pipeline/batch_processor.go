package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

// BatchRecordCommitter 是批量快路径使用的 Kafka 提交边界。
// 实现只有在 Broker 确认整批位点后才能返回 nil。
type BatchRecordCommitter interface {
	CommitBatch(context.Context, []kafka.Record) error
}

// BatchResult 保存一次批量处理尝试的整体结果。
// Created 和 Duplicate 可以混合出现；Committed 只有在 Kafka 明确确认整批提交后为 true。
type BatchResult struct {
	Kind      ResultKind
	Created   int
	Duplicate int
	Committed bool
}

// BatchFallbackError 表示批内存在永久无效记录，且尚未发生任何外部写入。
// Runner 收到该错误后应按原顺序退回已经验证的单记录/DLQ 路径。
type BatchFallbackError struct {
	RecordIndex int
	err         error
}

func (err *BatchFallbackError) Error() string {
	return fmt.Sprintf("批次第 %d 条记录需要单记录处理: %v", err.RecordIndex, err.err)
}

func (err *BatchFallbackError) Unwrap() error {
	return err.err
}

// BatchProcessor 负责正常流量的批量解析、一次 Bulk create 和一次 Kafka 提交。
// 它不处理永久无效记录、重试等待、Poll 循环、DLQ 或客户端生命周期。
type BatchProcessor struct {
	config    Config
	writer    BulkWriter
	committer BatchRecordCommitter
	now       func() time.Time
}

// NewBatchProcessor 创建批量处理器。
func NewBatchProcessor(
	config Config,
	writer BulkWriter,
	committer BatchRecordCommitter,
) (*BatchProcessor, error) {
	return newBatchProcessor(config, writer, committer, time.Now)
}

func newBatchProcessor(
	config Config,
	writer BulkWriter,
	committer BatchRecordCommitter,
	now func() time.Time,
) (*BatchProcessor, error) {
	validatedConfig, err := validateProcessingDependencies(config, writer, now)
	if err != nil {
		return nil, err
	}
	if committer == nil {
		return nil, errors.New("Kafka BatchRecordCommitter 不能为空")
	}
	return &BatchProcessor{
		config:    validatedConfig,
		writer:    writer,
		committer: committer,
		now:       now,
	}, nil
}

// Process 批量处理一组 Kafka 记录。
// 全批有效时只执行一次 Elasticsearch Bulk 和一次 Kafka CommitBatch；任何逐项失败
// 都不会推进位点。永久无效记录在外部调用前触发 BatchFallbackError。
func (processor *BatchProcessor) Process(
	ctx context.Context,
	records []kafka.Record,
) (BatchResult, error) {
	if ctx == nil {
		return BatchResult{Kind: ResultSystemFailure}, errors.New("批量处理 context 不能为空")
	}
	if processor == nil || processor.writer == nil || processor.committer == nil ||
		processor.now == nil {
		return BatchResult{Kind: ResultSystemFailure}, errors.New("pipeline BatchProcessor 未初始化")
	}
	if len(records) == 0 {
		return BatchResult{Kind: ResultSystemFailure}, errors.New("Kafka 批次不能为空")
	}
	if err := ctx.Err(); err != nil {
		return BatchResult{Kind: ResultCanceled}, batchRecordsError(records, "开始处理前 context 已取消", err)
	}

	ingestedAt := processor.now()
	documents := make([]elasticsearch.Document, len(records))
	for index, record := range records {
		normalized, err := event.ParseFilebeat(record.Value)
		if err != nil {
			wrapped := recordError(record, "解析 Filebeat 事件", err)
			if errors.Is(err, event.ErrInvalid) {
				return BatchResult{Kind: ResultInvalid}, &BatchFallbackError{
					RecordIndex: index,
					err:         wrapped,
				}
			}
			return BatchResult{Kind: ResultSystemFailure}, wrapped
		}

		document, err := elasticsearch.NewDocument(normalized, ingestedAt)
		if err != nil {
			return BatchResult{Kind: ResultSystemFailure}, recordError(
				record,
				"构造 Elasticsearch 文档",
				err,
			)
		}
		documents[index] = document
	}

	writeContext, cancelWrite := context.WithTimeout(ctx, processor.config.WriteTimeout)
	results, err := processor.writer.CreateBatch(writeContext, processor.config.Index, documents)
	cancelWrite()
	if err != nil {
		return BatchResult{Kind: classifyWriteError(err)}, batchRecordsError(
			records,
			"写入 Elasticsearch",
			err,
		)
	}
	if len(results) != len(records) {
		return BatchResult{Kind: ResultSystemFailure}, batchRecordsError(
			records,
			"校验 Elasticsearch 逐项结果",
			fmt.Errorf("结果数量为 %d，期望 %d", len(results), len(records)),
		)
	}

	result := BatchResult{}
	retryableIndex := -1
	systemIndex := -1
	for index, writeResult := range results {
		switch writeResult.Kind {
		case elasticsearch.ResultCreated:
			result.Created++
		case elasticsearch.ResultDuplicate:
			result.Duplicate++
		case elasticsearch.ResultRetryableFailure:
			if retryableIndex == -1 {
				retryableIndex = index
			}
		case elasticsearch.ResultSystemFailure:
			if systemIndex == -1 {
				systemIndex = index
			}
		default:
			return BatchResult{Kind: ResultSystemFailure}, recordError(
				records[index],
				"校验 Elasticsearch 逐项结果",
				fmt.Errorf("未知结果类型 %q", writeResult.Kind),
			)
		}
	}
	if systemIndex >= 0 {
		result.Kind = ResultSystemFailure
		return result, itemResultError(records[systemIndex], results[systemIndex])
	}
	if retryableIndex >= 0 {
		result.Kind = ResultRetryableFailure
		return result, itemResultError(records[retryableIndex], results[retryableIndex])
	}

	result.Kind = ResultDuplicate
	if result.Created > 0 {
		result.Kind = ResultCreated
	}
	if err := ctx.Err(); err != nil {
		result.Kind = ResultCommitFailure
		return result, batchRecordsError(records, "提交 Kafka 位点前 context 已取消", err)
	}
	commitContext, cancelCommit := context.WithTimeout(ctx, processor.config.CommitTimeout)
	err = processor.committer.CommitBatch(commitContext, records)
	cancelCommit()
	if err != nil {
		result.Kind = ResultCommitFailure
		return result, batchRecordsError(records, "提交 Kafka 批次", err)
	}

	result.Committed = true
	return result, nil
}

func batchRecordsError(records []kafka.Record, action string, err error) error {
	if len(records) == 0 {
		return fmt.Errorf("%s，空 Kafka 批次: %w", action, err)
	}
	first := records[0]
	last := records[len(records)-1]
	return fmt.Errorf(
		"%s，Kafka 批次 %q/%d/%d 到 %q/%d/%d（%d 条）: %w",
		action,
		first.Topic,
		first.Partition,
		first.Offset,
		last.Topic,
		last.Partition,
		last.Offset,
		len(records),
		err,
	)
}
