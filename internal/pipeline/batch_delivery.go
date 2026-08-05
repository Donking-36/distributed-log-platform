package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

type processBatchFunc func(context.Context, []kafka.Record) (BatchResult, error)

// BatchDeliveryResult 保存一次整批有界投递周期的最终状态。
type BatchDeliveryResult struct {
	LastResult BatchResult
	Attempts   int
	Exhausted  bool
}

// BatchDeliveryCycle 对同一 Kafka 批次执行有界重试。
// 写入或提交不确定时会重放完整批次，依靠稳定 Elasticsearch _id 收敛重复。
type BatchDeliveryCycle struct {
	process processBatchFunc
	jitter  jitterFunc
	wait    waitFunc
}

// NewBatchDeliveryCycle 使用与单记录路径相同的六次有界预算创建批量投递周期。
func NewBatchDeliveryCycle(processor *BatchProcessor) (*BatchDeliveryCycle, error) {
	if processor == nil {
		return nil, errors.New("pipeline BatchProcessor 不能为空")
	}
	return newBatchDeliveryCycle(processor.Process, fullJitter, waitForRetry)
}

func newBatchDeliveryCycle(
	process processBatchFunc,
	jitter jitterFunc,
	wait waitFunc,
) (*BatchDeliveryCycle, error) {
	if process == nil {
		return nil, errors.New("批量处理函数不能为空")
	}
	if jitter == nil {
		return nil, errors.New("重试抖动函数不能为空")
	}
	if wait == nil {
		return nil, errors.New("重试等待函数不能为空")
	}
	return &BatchDeliveryCycle{process: process, jitter: jitter, wait: wait}, nil
}

// Deliver 最多重放同一批次六次。永久无效记录不重试，由 BatchRunner 退回单记录路径。
func (cycle *BatchDeliveryCycle) Deliver(
	ctx context.Context,
	records []kafka.Record,
) (BatchDeliveryResult, error) {
	if ctx == nil {
		return BatchDeliveryResult{LastResult: BatchResult{Kind: ResultSystemFailure}},
			errors.New("批量投递 context 不能为空")
	}
	if cycle == nil || cycle.process == nil || cycle.jitter == nil || cycle.wait == nil {
		return BatchDeliveryResult{LastResult: BatchResult{Kind: ResultSystemFailure}},
			errors.New("pipeline BatchDeliveryCycle 未初始化")
	}
	if len(records) == 0 {
		return BatchDeliveryResult{LastResult: BatchResult{Kind: ResultSystemFailure}},
			errors.New("Kafka 投递批次不能为空")
	}
	if err := ctx.Err(); err != nil {
		return BatchDeliveryResult{LastResult: BatchResult{Kind: ResultCanceled}},
			batchRecordsError(records, "开始投递前 context 已取消", err)
	}

	delayLimit := deliveryInitialDelay
	var deliveryResult BatchDeliveryResult
	for attempt := 1; attempt <= deliveryMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return deliveryResult, batchRecordsError(records, "开始下一次投递前 context 已取消", err)
		}

		attemptResult, attemptErr := cycle.process(ctx, records)
		deliveryResult = BatchDeliveryResult{LastResult: attemptResult, Attempts: attempt}
		retry, contractErr := validateBatchAttemptResult(attemptResult, attemptErr, len(records))
		if contractErr != nil {
			return deliveryResult, batchRecordsError(records, "校验批量处理结果契约", contractErr)
		}
		if attemptErr == nil {
			return deliveryResult, nil
		}
		if !retry {
			return deliveryResult, attemptErr
		}
		if err := ctx.Err(); err != nil {
			return deliveryResult, batchRecordsError(records, "批量投递周期已取消", err)
		}
		if attempt == deliveryMaxAttempts {
			deliveryResult.Exhausted = true
			return deliveryResult, fmt.Errorf(
				"批量投递周期在 %d 次尝试后耗尽: %w",
				attempt,
				attemptErr,
			)
		}

		delay := cycle.jitter(delayLimit)
		if delay < 0 || delay > delayLimit {
			return deliveryResult, batchRecordsError(
				records,
				"生成重试抖动",
				fmt.Errorf("等待 %v 超出 [0,%v]", delay, delayLimit),
			)
		}
		if err := cycle.wait(ctx, delay); err != nil {
			return deliveryResult, batchRecordsError(records, "等待下一次批量投递", err)
		}
		if delayLimit < deliveryMaximumDelay {
			delayLimit *= 2
			if delayLimit > deliveryMaximumDelay {
				delayLimit = deliveryMaximumDelay
			}
		}
	}

	return deliveryResult, errors.New("批量投递周期进入不可达状态")
}

func validateBatchAttemptResult(result BatchResult, attemptErr error, expected int) (bool, error) {
	if expected <= 0 {
		return false, errors.New("批次记录数必须大于 0")
	}
	if result.Created < 0 || result.Duplicate < 0 || result.Created+result.Duplicate > expected {
		return false, fmt.Errorf(
			"批次 accepted 数量非法: created=%d duplicate=%d expected=%d",
			result.Created,
			result.Duplicate,
			expected,
		)
	}
	accepted := result.Created + result.Duplicate
	if result.Committed {
		if attemptErr != nil {
			return false, errors.New("已提交批次不能同时返回错误")
		}
		if accepted != expected {
			return false, fmt.Errorf("已提交批次 accepted=%d，期望 %d", accepted, expected)
		}
		if result.Created > 0 && result.Kind != ResultCreated {
			return false, fmt.Errorf("包含 created 的批次结果必须为 %q", ResultCreated)
		}
		if result.Created == 0 && result.Kind != ResultDuplicate {
			return false, fmt.Errorf("全 duplicate 批次结果必须为 %q", ResultDuplicate)
		}
		return false, nil
	}
	if attemptErr == nil {
		return false, fmt.Errorf("未提交批次结果 %q 必须返回错误", result.Kind)
	}

	switch result.Kind {
	case ResultRetryableFailure:
		return true, nil
	case ResultCommitFailure:
		if accepted != expected {
			return false, fmt.Errorf("提交失败批次 accepted=%d，期望 %d", accepted, expected)
		}
		return true, nil
	case ResultInvalid:
		if accepted != 0 {
			return false, errors.New("永久无效批次不能包含已写入结果")
		}
		return false, nil
	case ResultSystemFailure, ResultCanceled:
		return false, nil
	case ResultCreated, ResultDuplicate:
		return false, fmt.Errorf("成功批次结果 %q 缺少 Kafka 提交确认", result.Kind)
	default:
		return false, fmt.Errorf("未知批量处理结果 %q", result.Kind)
	}
}
