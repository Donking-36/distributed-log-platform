package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

type pollRecordFunc func(context.Context) (kafka.Record, error)
type deliverRecordFunc func(context.Context, kafka.Record) (DeliveryResult, error)
type handleDeadLetterFunc func(
	context.Context,
	kafka.Record,
	DeliveryResult,
	error,
) (DeadLetterResult, error)

// Runner 串行驱动 Kafka 拉取、单记录投递和永久无效记录隔离。
// 当前记录获得源位点提交确认前，Runner 绝不会拉取下一条记录。
type Runner struct {
	poll             pollRecordFunc
	deliver          deliverRecordFunc
	handleDeadLetter handleDeadLetterFunc
	logger           *slog.Logger
}

// NewRunner 绑定现有的 Kafka、投递周期和 DLQ 边界，创建持续处理运行器。
func NewRunner(
	consumer *kafka.Consumer,
	cycle *DeliveryCycle,
	deadLetter *DeadLetterHandler,
	logger *slog.Logger,
) (*Runner, error) {
	if consumer == nil {
		return nil, errors.New("Kafka Consumer 不能为空")
	}
	if cycle == nil {
		return nil, errors.New("pipeline DeliveryCycle 不能为空")
	}
	if deadLetter == nil {
		return nil, errors.New("pipeline DeadLetterHandler 不能为空")
	}
	return newRunner(consumer.Poll, cycle.Deliver, deadLetter.Handle, logger)
}

func newRunner(
	poll pollRecordFunc,
	deliver deliverRecordFunc,
	handleDeadLetter handleDeadLetterFunc,
	logger *slog.Logger,
) (*Runner, error) {
	if poll == nil {
		return nil, errors.New("Kafka Poll 函数不能为空")
	}
	if deliver == nil {
		return nil, errors.New("pipeline Deliver 函数不能为空")
	}
	if handleDeadLetter == nil {
		return nil, errors.New("pipeline DLQ 处理函数不能为空")
	}
	if logger == nil {
		return nil, errors.New("日志记录器不能为空")
	}
	return &Runner{
		poll:             poll,
		deliver:          deliver,
		handleDeadLetter: handleDeadLetter,
		logger:           logger,
	}, nil
}

// Run 逐条处理 Kafka 记录，直到父 context 取消或当前记录仍未解决。
// 返回错误时调用方必须关闭消费者，让未提交记录由同一消费者组重新获取。
func (runner *Runner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("运行 context 不能为空")
	}
	if runner == nil || runner.poll == nil || runner.deliver == nil ||
		runner.handleDeadLetter == nil || runner.logger == nil {
		return errors.New("pipeline Runner 未初始化")
	}

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("停止 Kafka 处理循环: %w", err)
		}

		record, err := runner.poll(ctx)
		if err != nil {
			return fmt.Errorf("拉取下一条 Kafka 记录: %w", err)
		}

		delivery, deliveryErr := runner.deliver(ctx, record)
		if deliveryErr == nil {
			if err := validateCommittedDelivery(delivery); err != nil {
				return recordError(record, "校验已完成投递", err)
			}
			runner.logger.DebugContext(
				ctx,
				"日志记录已写入并提交",
				"topic", record.Topic,
				"partition", record.Partition,
				"offset", record.Offset,
				"result", delivery.LastResult.Kind,
				"attempts", delivery.Attempts,
			)
			continue
		}

		if delivery.LastResult.Kind != ResultInvalid {
			return fmt.Errorf("当前 Kafka 记录投递未完成: %w", deliveryErr)
		}
		if err := ctx.Err(); err != nil {
			return recordError(record, "进入死信处理前 context 已取消", err)
		}

		deadLetterResult, err := runner.handleDeadLetter(ctx, record, delivery, deliveryErr)
		if err != nil {
			return fmt.Errorf("当前 Kafka 记录死信处理未完成: %w", err)
		}
		if !deadLetterResult.Published || !deadLetterResult.Committed {
			return recordError(
				record,
				"校验死信处理结果",
				fmt.Errorf(
					"published=%t, committed=%t，期望均为 true",
					deadLetterResult.Published,
					deadLetterResult.Committed,
				),
			)
		}
		runner.logger.WarnContext(
			ctx,
			"永久无效日志已隔离并提交",
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"attempts", delivery.Attempts,
		)
	}
}

func validateCommittedDelivery(delivery DeliveryResult) error {
	if delivery.Attempts < 1 {
		return errors.New("已完成投递的尝试次数必须至少为 1")
	}
	if delivery.Exhausted {
		return errors.New("已完成投递不能标记为重试耗尽")
	}
	_, err := validateAttemptResult(delivery.LastResult, nil)
	return err
}
