package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	// ErrRecordPending 表示当前记录尚未成功提交，继续拉取可能越过失败记录。
	ErrRecordPending = errors.New("Kafka 当前记录尚未确认")
	// ErrRecordNotPending 表示提交目标不是该消费者当前唯一的待确认记录。
	ErrRecordNotPending = errors.New("Kafka 记录不是当前待确认记录")
	// ErrConsumerClosed 表示消费者已经关闭。
	ErrConsumerClosed = errors.New("Kafka 消费者已经关闭")
	// ErrPollInProgress 表示同一消费者发生了并发拉取。
	ErrPollInProgress = errors.New("Kafka 消费者正在执行另一次拉取")
)

type consumerClient interface {
	poll(context.Context, int) ([]*kgo.Record, error)
	commit(context.Context, []*kgo.Record) error
	allowRebalance()
	closeAllowingRebalance()
}

// Consumer 封装单条拉取与显式位点提交边界。
// Poll 和 Commit 应由同一处理循环串行调用；Close 可以安全重复调用。
type Consumer struct {
	mu        sync.Mutex
	client    consumerClient
	pending   []*recordToken
	polling   bool
	closed    bool
	closeOnce sync.Once
}

// NewConsumer 创建关闭自动提交、并在处理期间阻止重平衡的消费者。
// 创建客户端不会隐式提交任何位点；新消费者组从主题起始位置消费。
func NewConsumer(config Config) (*Consumer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(config.clientOptions()...)
	if err != nil {
		return nil, fmt.Errorf("创建 Kafka 消费者: %w", err)
	}
	return newConsumer(&franzConsumerClient{client: client}), nil
}

func newConsumer(client consumerClient) *Consumer {
	return &Consumer{client: client}
}

// Poll 拉取一条记录，保留原有单记录调用方的稳定边界。
func (consumer *Consumer) Poll(ctx context.Context) (Record, error) {
	records, err := consumer.PollBatch(ctx, 1)
	if err != nil {
		return Record{}, err
	}
	return records[0], nil
}

// PollBatch 最多拉取 maxRecords 条记录，并把整批标记为待确认。
// 批次清空前禁止下一次拉取；调用方可以一次提交整批，或从批次首条开始逐条提交，
// 从而在正常流量走 Bulk 快路径、遇到永久无效记录时安全退回既有单记录/DLQ 路径。
func (consumer *Consumer) PollBatch(ctx context.Context, maxRecords int) ([]Record, error) {
	if ctx == nil {
		return nil, errors.New("Kafka 拉取 context 不能为空")
	}
	if maxRecords <= 0 {
		return nil, errors.New("Kafka 批量拉取上限必须大于 0")
	}
	if consumer == nil || consumer.client == nil {
		return nil, ErrConsumerClosed
	}

	consumer.mu.Lock()
	if consumer.closed {
		consumer.mu.Unlock()
		return nil, ErrConsumerClosed
	}
	if len(consumer.pending) != 0 {
		consumer.mu.Unlock()
		return nil, ErrRecordPending
	}
	if consumer.polling {
		consumer.mu.Unlock()
		return nil, ErrPollInProgress
	}
	consumer.polling = true
	consumer.mu.Unlock()

	defer func() {
		consumer.mu.Lock()
		consumer.polling = false
		consumer.mu.Unlock()
	}()

	for {
		sources, err := consumer.client.poll(ctx, maxRecords)

		consumer.mu.Lock()
		if consumer.closed {
			consumer.mu.Unlock()
			return nil, ErrConsumerClosed
		}
		if err != nil {
			consumer.client.allowRebalance()
			consumer.mu.Unlock()
			return nil, fmt.Errorf("拉取 Kafka 记录: %w", err)
		}
		if len(sources) == 0 {
			// franz-go 在重平衡和内部唤醒时可能返回零记录且无错误。
			// 当前没有业务记录需要保护，先放行重平衡，再继续等待同一 Poll 调用。
			consumer.client.allowRebalance()
			consumer.mu.Unlock()
			continue
		}

		records := make([]Record, len(sources))
		consumer.pending = make([]*recordToken, len(sources))
		for index, source := range sources {
			token := &recordToken{source: source}
			consumer.pending[index] = token
			records[index] = projectRecord(source, token)
		}
		consumer.mu.Unlock()
		return records, nil
	}
}

// Commit 同步提交当前批次首条记录的下一位点。
// 批次仍有剩余记录时继续阻止重平衡，供批处理退回单记录/DLQ 路径时安全推进。
// 调用方必须使用有界且可取消的 context；重试预算耗尽后应关闭消费者，
// 不能放行当前记录后继续拉取，以免越过未确认位点。
func (consumer *Consumer) Commit(ctx context.Context, record Record) error {
	return consumer.commitPending(ctx, []Record{record}, false)
}

// CommitBatch 一次同步提交当前完整批次。
// 只有 Broker 明确确认后才清空待确认记录并允许重平衡；失败时整批保持不变，
// 调用方可以依靠稳定 Elasticsearch _id 重放完整 Bulk。
func (consumer *Consumer) CommitBatch(ctx context.Context, records []Record) error {
	return consumer.commitPending(ctx, records, true)
}

func (consumer *Consumer) commitPending(
	ctx context.Context,
	records []Record,
	requireWholeBatch bool,
) error {
	if ctx == nil {
		return errors.New("Kafka 提交 context 不能为空")
	}
	if consumer == nil || consumer.client == nil {
		return ErrConsumerClosed
	}

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if consumer.closed {
		return ErrConsumerClosed
	}
	if len(records) == 0 || len(consumer.pending) < len(records) {
		return ErrRecordNotPending
	}
	if requireWholeBatch && len(records) != len(consumer.pending) {
		return ErrRecordNotPending
	}

	sources := make([]*kgo.Record, len(records))
	for index, record := range records {
		if record.token == nil || record.token != consumer.pending[index] {
			return ErrRecordNotPending
		}
		sources[index] = consumer.pending[index].source
	}
	if err := consumer.client.commit(ctx, sources); err != nil {
		first := sources[0]
		last := sources[len(sources)-1]
		return fmt.Errorf(
			"提交 Kafka 批次 %s/%d/%d 到 %s/%d/%d（%d 条）: %w",
			first.Topic,
			first.Partition,
			first.Offset,
			last.Topic,
			last.Partition,
			last.Offset,
			len(sources),
			err,
		)
	}

	consumer.pending = consumer.pending[len(records):]
	if len(consumer.pending) == 0 {
		consumer.client.allowRebalance()
	}

	return nil
}

// Close 允许尚被阻止的重平衡并关闭底层客户端，但绝不提交待确认记录。
// 因此处理中断时，同一消费者组下次仍会收到未确认记录。
func (consumer *Consumer) Close() {
	if consumer == nil {
		return
	}
	consumer.closeOnce.Do(func() {
		consumer.mu.Lock()
		consumer.closed = true
		consumer.pending = nil
		client := consumer.client
		consumer.mu.Unlock()

		if client != nil {
			client.closeAllowingRebalance()
		}
	})
}

type franzConsumerClient struct {
	client        *kgo.Client
	deferredError error
}

func (client *franzConsumerClient) poll(ctx context.Context, maxRecords int) ([]*kgo.Record, error) {
	if client == nil || client.client == nil {
		return nil, ErrConsumerClosed
	}
	if client.deferredError != nil {
		err := client.deferredError
		client.deferredError = nil
		return nil, err
	}
	fetches := client.client.PollRecords(ctx, maxRecords)
	records, fetchError := inspectFetches(fetches)
	if len(records) != 0 {
		// 当前批次不能放回 franz-go 缓冲区；先交付它，再在下一次 Poll 报告同批次分区错误。
		client.deferredError = fetchError
		return records, nil
	}
	if fetchError != nil {
		return nil, fetchError
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

func inspectFetches(fetches kgo.Fetches) ([]*kgo.Record, error) {
	records := fetches.Records()
	fetchErrors := fetches.Errors()
	errorsToReport := make([]error, 0, len(fetchErrors))
	for _, fetchError := range fetchErrors {
		if fetchError.Err == nil {
			continue
		}
		if fetchError.Topic == "" {
			errorsToReport = append(errorsToReport, fetchError.Err)
			continue
		}
		errorsToReport = append(
			errorsToReport,
			fmt.Errorf("主题 %q 分区 %d: %w", fetchError.Topic, fetchError.Partition, fetchError.Err),
		)
	}
	return records, errors.Join(errorsToReport...)
}

func (client *franzConsumerClient) commit(ctx context.Context, records []*kgo.Record) error {
	return client.client.CommitRecords(ctx, records...)
}

func (client *franzConsumerClient) allowRebalance() {
	client.client.AllowRebalance()
}

func (client *franzConsumerClient) closeAllowingRebalance() {
	client.client.CloseAllowingRebalance()
}
