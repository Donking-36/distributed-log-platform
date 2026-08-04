package kafka

import (
	"bytes"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record 是交给处理器的稳定 Kafka 记录投影。
// Key 和 Value 已复制，不依赖 franz-go 内部缓冲区；内部令牌用于拒绝伪造或跨消费者提交。
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte

	token *recordToken
}

type recordToken struct {
	source *kgo.Record
}

func projectRecord(source *kgo.Record, token *recordToken) Record {
	return Record{
		Topic:     source.Topic,
		Partition: source.Partition,
		Offset:    source.Offset,
		Key:       bytes.Clone(source.Key),
		Value:     bytes.Clone(source.Value),
		token:     token,
	}
}
