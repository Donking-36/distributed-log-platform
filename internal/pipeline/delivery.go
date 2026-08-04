package pipeline

import (
	"context"
	"errors"
	"fmt"
	rand "math/rand/v2"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

const (
	deliveryMaxAttempts  = 6
	deliveryInitialDelay = 250 * time.Millisecond
	deliveryMaximumDelay = 4 * time.Second
)

type jitterFunc func(time.Duration) time.Duration
type waitFunc func(context.Context, time.Duration) error

// DeliveryCycle 为一条 Kafka 记录执行 ADR-002 规定的有界投递周期。
// 它只协调完整的 Processor 尝试和等待，不负责 Poll、DLQ、健康状态或客户端关闭。
type DeliveryCycle struct {
	processor *Processor
	jitter    jitterFunc
	wait      waitFunc
}

// NewDeliveryCycle 使用 ADR-002 的固定预算创建单记录投递周期。
func NewDeliveryCycle(processor *Processor) (*DeliveryCycle, error) {
	return newDeliveryCycle(processor, fullJitter, waitForRetry)
}

func newDeliveryCycle(
	processor *Processor,
	jitter jitterFunc,
	wait waitFunc,
) (*DeliveryCycle, error) {
	if processor == nil {
		return nil, errors.New("pipeline Processor 不能为空")
	}
	if jitter == nil {
		return nil, errors.New("重试抖动函数不能为空")
	}
	if wait == nil {
		return nil, errors.New("重试等待函数不能为空")
	}
	return &DeliveryCycle{processor: processor, jitter: jitter, wait: wait}, nil
}

// Deliver 对同一原始记录最多执行六次完整 Process 尝试。
// 只有 retryable_failure 和 commit_failure 会进入下一次尝试；父 context 取消
// 始终优先终止周期。返回 nil 错误只表示最后一次结果已成功提交。
func (cycle *DeliveryCycle) Deliver(
	ctx context.Context,
	record kafka.Record,
) (DeliveryResult, error) {
	if ctx == nil {
		return DeliveryResult{LastResult: Result{Kind: ResultSystemFailure}},
			errors.New("投递 context 不能为空")
	}
	if cycle == nil || cycle.processor == nil || cycle.jitter == nil || cycle.wait == nil {
		return DeliveryResult{LastResult: Result{Kind: ResultSystemFailure}},
			errors.New("pipeline DeliveryCycle 未初始化")
	}
	if err := ctx.Err(); err != nil {
		return DeliveryResult{LastResult: Result{Kind: ResultCanceled}},
			recordError(record, "开始投递前 context 已取消", err)
	}

	delayLimit := deliveryInitialDelay
	var deliveryResult DeliveryResult
	for attempt := 1; attempt <= deliveryMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return deliveryResult, recordError(record, "开始下一次投递前 context 已取消", err)
		}

		attemptResult, attemptErr := cycle.processor.Process(ctx, record)
		deliveryResult = DeliveryResult{
			LastResult: attemptResult,
			Attempts:   attempt,
		}
		retry, contractErr := validateAttemptResult(attemptResult, attemptErr)
		if contractErr != nil {
			return deliveryResult, recordError(record, "校验单次处理结果契约", contractErr)
		}
		if attemptErr == nil {
			return deliveryResult, nil
		}
		if !retry {
			return deliveryResult, attemptErr
		}

		// Processor 可能在 Elasticsearch 已接受后因父 context 取消而返回
		// commit_failure；这里必须让取消优先，不能把它误判成新的投递机会。
		if err := ctx.Err(); err != nil {
			return deliveryResult, recordError(record, "投递周期已取消", err)
		}
		if attempt == deliveryMaxAttempts {
			deliveryResult.Exhausted = true
			return deliveryResult, fmt.Errorf(
				"单记录投递周期在 %d 次尝试后耗尽: %w",
				attempt,
				attemptErr,
			)
		}

		delay := cycle.jitter(delayLimit)
		if delay < 0 || delay > delayLimit {
			return deliveryResult, recordError(
				record,
				"生成重试抖动",
				fmt.Errorf("等待 %v 超出 [0,%v]", delay, delayLimit),
			)
		}
		if err := cycle.wait(ctx, delay); err != nil {
			return deliveryResult, recordError(record, "等待下一次投递", err)
		}
		if delayLimit < deliveryMaximumDelay {
			delayLimit *= 2
			if delayLimit > deliveryMaximumDelay {
				delayLimit = deliveryMaximumDelay
			}
		}
	}

	return deliveryResult, errors.New("单记录投递周期进入不可达状态")
}

func validateAttemptResult(result Result, attemptErr error) (bool, error) {
	if result.Committed {
		if attemptErr != nil {
			return false, errors.New("已提交结果不能同时返回错误")
		}
		if result.Kind != ResultCreated && result.Kind != ResultDuplicate {
			return false, fmt.Errorf("结果 %q 不允许标记为已提交", result.Kind)
		}
		if err := validateAcceptedResult(result); err != nil {
			return false, err
		}
		return false, nil
	}
	if attemptErr == nil {
		return false, fmt.Errorf("未提交结果 %q 必须返回错误", result.Kind)
	}

	switch result.Kind {
	case ResultRetryableFailure:
		return true, nil
	case ResultCommitFailure:
		if err := validateAcceptedResult(result); err != nil {
			return false, err
		}
		return true, nil
	case ResultInvalid, ResultSystemFailure, ResultCanceled:
		return false, nil
	case ResultCreated, ResultDuplicate:
		return false, fmt.Errorf("成功结果 %q 缺少 Kafka 提交确认", result.Kind)
	default:
		return false, fmt.Errorf("未知处理结果 %q", result.Kind)
	}
}

func validateAcceptedResult(result Result) error {
	switch result.Kind {
	case ResultCreated:
		if result.ElasticsearchResult != elasticsearch.ResultCreated {
			return fmt.Errorf("结果 %q 必须对应 Elasticsearch %q", result.Kind, elasticsearch.ResultCreated)
		}
	case ResultDuplicate:
		if result.ElasticsearchResult != elasticsearch.ResultDuplicate {
			return fmt.Errorf("结果 %q 必须对应 Elasticsearch %q", result.Kind, elasticsearch.ResultDuplicate)
		}
	case ResultCommitFailure:
		if result.ElasticsearchResult != elasticsearch.ResultCreated &&
			result.ElasticsearchResult != elasticsearch.ResultDuplicate {
			return fmt.Errorf("结果 %q 必须保留已接受的 Elasticsearch 结果", result.Kind)
		}
	}
	return nil
}

func fullJitter(limit time.Duration) time.Duration {
	return jitterWithSource(limit, rand.Int64N)
}

func jitterWithSource(limit time.Duration, draw func(int64) int64) time.Duration {
	// 上界加一使全抖动覆盖闭区间 [0, limit]；本项目最大 4 秒，不存在溢出。
	return time.Duration(draw(int64(limit) + 1))
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	if ctx == nil {
		return errors.New("重试等待 context 不能为空")
	}
	if delay < 0 {
		return fmt.Errorf("重试等待不能为负数: %v", delay)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
