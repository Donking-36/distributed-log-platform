package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// DeadLetterTopic 是永久无效日志的固定隔离主题。
	DeadLetterTopic            = "logs.dlq"
	deadLetterProducerClientID = "log-processor-dlq"
)

// ErrDeadLetterProducerClosed 表示死信生产者已经关闭或未正确初始化。
var ErrDeadLetterProducerClosed = errors.New("Kafka 死信生产者已经关闭")

// DeadLetterProducerConfig 定义独立 DLQ 生产者所需的最小连接配置。
type DeadLetterProducerConfig struct {
	Brokers []string
}

func (config DeadLetterProducerConfig) validate() error {
	return validateBrokers(config.Brokers)
}

func (config DeadLetterProducerConfig) clientOptions() []kgo.Opt {
	brokers := append([]string(nil), config.Brokers...)
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(deadLetterProducerClientID),
		kgo.DefaultProduceTopic(DeadLetterTopic),
		kgo.DefaultProduceTopicAlways(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// 保留 franz-go 默认幂等写，同时允许外层超时真正终止同步等待。
		// Broker 响应不确定时可能产生物理重复，因此 DLQ key 必须稳定。
		kgo.AllowIdempotentProduceCancellation(),
	}
}

type deadLetterProducerClient interface {
	produce(context.Context, *kgo.Record) error
	close()
}

// DeadLetterProducer 把一条完整 DLQ JSON 同步写入固定主题。
// Write 只有在 franz-go 收到 Broker 确认后才返回 nil；Close 可以安全重复调用。
type DeadLetterProducer struct {
	mu     sync.Mutex
	client deadLetterProducerClient
	closed bool
}

// NewDeadLetterProducer 创建使用 acks=all 的独立死信生产者。
// 独立客户端避免消费者配置和生产确认语义相互污染。
func NewDeadLetterProducer(config DeadLetterProducerConfig) (*DeadLetterProducer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(config.clientOptions()...)
	if err != nil {
		return nil, fmt.Errorf("创建 Kafka 死信生产者: %w", err)
	}
	return newDeadLetterProducer(&franzDeadLetterProducerClient{client: client}), nil
}

func newDeadLetterProducer(client deadLetterProducerClient) *DeadLetterProducer {
	return &DeadLetterProducer{client: client}
}

// Write 同步发布一条 DLQ 记录，并复制 key/value 以隔离调用方后续修改。
// 调用方必须提供有界 context；返回错误不得包含 key 或原始载荷。
func (producer *DeadLetterProducer) Write(ctx context.Context, key, value []byte) error {
	if ctx == nil {
		return errors.New("Kafka 死信写入 context 不能为空")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("开始 Kafka 死信写入前 context 已取消: %w", err)
	}
	if len(key) == 0 {
		return errors.New("Kafka 死信记录 key 不能为空")
	}
	if len(value) == 0 {
		return errors.New("Kafka 死信记录 value 不能为空")
	}
	if producer == nil {
		return ErrDeadLetterProducerClosed
	}

	producer.mu.Lock()
	defer producer.mu.Unlock()
	if producer.closed || producer.client == nil {
		return ErrDeadLetterProducerClosed
	}

	record := &kgo.Record{
		Topic: DeadLetterTopic,
		Key:   bytes.Clone(key),
		Value: bytes.Clone(value),
	}
	if err := producer.client.produce(ctx, record); err != nil {
		return fmt.Errorf("写入 Kafka 死信主题 %q: %w", DeadLetterTopic, err)
	}
	return nil
}

// Close 等待当前同步写入结束后关闭底层客户端；不会隐式写入或提交任何记录。
func (producer *DeadLetterProducer) Close() {
	if producer == nil {
		return
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if producer.closed {
		return
	}
	producer.closed = true
	if producer.client != nil {
		producer.client.close()
	}
}

type franzDeadLetterProducerClient struct {
	client *kgo.Client
}

func (client *franzDeadLetterProducerClient) produce(ctx context.Context, record *kgo.Record) error {
	if client == nil || client.client == nil {
		return ErrDeadLetterProducerClosed
	}
	return client.client.ProduceSync(ctx, record).FirstErr()
}

func (client *franzDeadLetterProducerClient) close() {
	if client != nil && client.client != nil {
		client.client.Close()
	}
}
