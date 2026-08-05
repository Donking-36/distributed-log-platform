package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

type pollBatchFunc func(context.Context, int) ([]kafka.Record, error)
type deliverBatchFunc func(context.Context, []kafka.Record) (BatchDeliveryResult, error)

// BatchRunner 优先走批量快路径，并在批内出现永久无效记录时按原顺序退回单记录/DLQ。
type BatchRunner struct {
	batchSize        int
	poll             pollBatchFunc
	deliverBatch     deliverBatchFunc
	deliverSingle    deliverRecordFunc
	handleDeadLetter handleDeadLetterFunc
	logger           *slog.Logger
}

// NewBatchRunner 绑定 Kafka 批量拉取、批量投递和既有单记录异常路径。
func NewBatchRunner(
	batchSize int,
	consumer *kafka.Consumer,
	batchCycle *BatchDeliveryCycle,
	singleCycle *DeliveryCycle,
	deadLetter *DeadLetterHandler,
	logger *slog.Logger,
) (*BatchRunner, error) {
	if consumer == nil {
		return nil, errors.New("Kafka Consumer 不能为空")
	}
	if batchCycle == nil {
		return nil, errors.New("pipeline BatchDeliveryCycle 不能为空")
	}
	if singleCycle == nil {
		return nil, errors.New("pipeline DeliveryCycle 不能为空")
	}
	if deadLetter == nil {
		return nil, errors.New("pipeline DeadLetterHandler 不能为空")
	}
	return newBatchRunner(
		batchSize,
		consumer.PollBatch,
		batchCycle.Deliver,
		singleCycle.Deliver,
		deadLetter.Handle,
		logger,
	)
}

func newBatchRunner(
	batchSize int,
	poll pollBatchFunc,
	deliverBatch deliverBatchFunc,
	deliverSingle deliverRecordFunc,
	handleDeadLetter handleDeadLetterFunc,
	logger *slog.Logger,
) (*BatchRunner, error) {
	if batchSize <= 0 {
		return nil, errors.New("Kafka 批量拉取大小必须大于 0")
	}
	if poll == nil {
		return nil, errors.New("Kafka PollBatch 函数不能为空")
	}
	if deliverBatch == nil {
		return nil, errors.New("pipeline 批量 Deliver 函数不能为空")
	}
	if deliverSingle == nil {
		return nil, errors.New("pipeline 单记录 Deliver 函数不能为空")
	}
	if handleDeadLetter == nil {
		return nil, errors.New("pipeline DLQ 处理函数不能为空")
	}
	if logger == nil {
		return nil, errors.New("日志记录器不能为空")
	}
	return &BatchRunner{
		batchSize:        batchSize,
		poll:             poll,
		deliverBatch:     deliverBatch,
		deliverSingle:    deliverSingle,
		handleDeadLetter: handleDeadLetter,
		logger:           logger,
	}, nil
}

// Run 持续处理 Kafka 批次，直到父 context 取消或当前批次仍未解决。
func (runner *BatchRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("运行 context 不能为空")
	}
	if runner == nil || runner.batchSize <= 0 || runner.poll == nil ||
		runner.deliverBatch == nil || runner.deliverSingle == nil ||
		runner.handleDeadLetter == nil || runner.logger == nil {
		return errors.New("pipeline BatchRunner 未初始化")
	}

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("停止 Kafka 批量处理循环: %w", err)
		}
		records, err := runner.poll(ctx, runner.batchSize)
		if err != nil {
			return fmt.Errorf("拉取下一批 Kafka 记录: %w", err)
		}
		if len(records) == 0 {
			return errors.New("Kafka PollBatch 返回空批次且无错误")
		}

		delivery, deliveryErr := runner.deliverBatch(ctx, records)
		if deliveryErr == nil {
			if _, err := validateBatchAttemptResult(delivery.LastResult, nil, len(records)); err != nil {
				return batchRecordsError(records, "校验已完成批量投递", err)
			}
			runner.logger.DebugContext(
				ctx,
				"日志批次已写入并提交",
				"records", len(records),
				"created", delivery.LastResult.Created,
				"duplicate", delivery.LastResult.Duplicate,
				"attempts", delivery.Attempts,
			)
			continue
		}

		var fallback *BatchFallbackError
		if delivery.LastResult.Kind != ResultInvalid || !errors.As(deliveryErr, &fallback) {
			return fmt.Errorf("当前 Kafka 批次投递未完成: %w", deliveryErr)
		}
		for _, record := range records {
			if err := handleSingleRecord(
				ctx,
				record,
				runner.deliverSingle,
				runner.handleDeadLetter,
				runner.logger,
			); err != nil {
				return err
			}
		}
	}
}
