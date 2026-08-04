package kafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDeadLetterProducerConfigValidate(t *testing.T) {
	t.Parallel()

	valid := DeadLetterProducerConfig{
		Brokers: []string{"kafka.stage3-logs.svc.cluster.local:9092"},
	}
	tests := []struct {
		name   string
		mutate func(*DeadLetterProducerConfig)
	}{
		{name: "缺少 broker", mutate: func(config *DeadLetterProducerConfig) { config.Brokers = nil }},
		{name: "broker 为空", mutate: func(config *DeadLetterProducerConfig) { config.Brokers = []string{" "} }},
		{name: "broker 缺少端口", mutate: func(config *DeadLetterProducerConfig) { config.Brokers = []string{"kafka"} }},
		{name: "broker 重复", mutate: func(config *DeadLetterProducerConfig) {
			config.Brokers = []string{"kafka:9092", "kafka:9092"}
		}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			config := valid
			config.Brokers = append([]string(nil), valid.Brokers...)
			test.mutate(&config)
			if err := config.validate(); err == nil {
				t.Fatal("期望配置校验失败，实际成功")
			}
		})
	}

	if err := valid.validate(); err != nil {
		t.Fatalf("合法配置校验失败: %v", err)
	}
}

func TestNewDeadLetterProducerFixesAcknowledgementAndCancellationOptions(t *testing.T) {
	t.Parallel()

	producer, err := NewDeadLetterProducer(DeadLetterProducerConfig{
		Brokers: []string{"127.0.0.1:9092"},
	})
	if err != nil {
		t.Fatalf("NewDeadLetterProducer() error = %v", err)
	}
	t.Cleanup(producer.Close)

	franz, ok := producer.client.(*franzDeadLetterProducerClient)
	if !ok {
		t.Fatalf("底层客户端类型 = %T，期望 *franzDeadLetterProducerClient", producer.client)
	}
	if got := franz.client.OptValue(kgo.RequiredAcks); !reflect.DeepEqual(got, kgo.AllISRAcks()) {
		t.Fatalf("RequiredAcks = %#v，期望 %#v", got, kgo.AllISRAcks())
	}
	if got := franz.client.OptValue(kgo.DefaultProduceTopic); got != DeadLetterTopic {
		t.Fatalf("DefaultProduceTopic = %#v，期望 %q", got, DeadLetterTopic)
	}
	if enabled, ok := franz.client.OptValue(kgo.DefaultProduceTopicAlways).(bool); !ok || !enabled {
		t.Fatalf("DefaultProduceTopicAlways = %#v，期望 true",
			franz.client.OptValue(kgo.DefaultProduceTopicAlways))
	}
	if enabled, ok := franz.client.OptValue(kgo.AllowIdempotentProduceCancellation).(bool); !ok || !enabled {
		t.Fatalf("AllowIdempotentProduceCancellation = %#v，期望 true",
			franz.client.OptValue(kgo.AllowIdempotentProduceCancellation))
	}
	if disabled, ok := franz.client.OptValue(kgo.DisableIdempotentWrite).(bool); !ok || disabled {
		t.Fatalf("DisableIdempotentWrite = %#v，期望 false",
			franz.client.OptValue(kgo.DisableIdempotentWrite))
	}
}

func TestDeadLetterProducerWritesOneCopiedRecordToFixedTopic(t *testing.T) {
	t.Parallel()

	client := &fakeDeadLetterProducerClient{}
	producer := newDeadLetterProducer(client)
	t.Cleanup(producer.Close)
	key := []byte("v1|16:logs.api-service|1|17")
	value := []byte(`{"schema_version":1}`)

	if err := producer.Write(context.Background(), key, value); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	key[0] = 'X'
	value[0] = 'X'

	if len(client.records) != 1 {
		t.Fatalf("produce calls = %d，期望 1", len(client.records))
	}
	record := client.records[0]
	if record.Topic != DeadLetterTopic {
		t.Fatalf("record topic = %q，期望 %q", record.Topic, DeadLetterTopic)
	}
	if string(record.Key) != "v1|16:logs.api-service|1|17" ||
		string(record.Value) != `{"schema_version":1}` {
		t.Fatalf("record key/value = %q/%q，未保持调用时内容", record.Key, record.Value)
	}
}

func TestDeadLetterProducerRejectsInvalidCallsAndPreservesCause(t *testing.T) {
	t.Parallel()

	t.Run("nil context", func(t *testing.T) {
		producer := newDeadLetterProducer(&fakeDeadLetterProducerClient{})
		t.Cleanup(producer.Close)
		if err := producer.Write(nil, []byte("key"), []byte("value")); err == nil {
			t.Fatal("Write(nil) error = nil")
		}
	})

	t.Run("empty key", func(t *testing.T) {
		producer := newDeadLetterProducer(&fakeDeadLetterProducerClient{})
		t.Cleanup(producer.Close)
		if err := producer.Write(context.Background(), nil, []byte("value")); err == nil {
			t.Fatal("Write(empty key) error = nil")
		}
	})

	t.Run("empty value", func(t *testing.T) {
		producer := newDeadLetterProducer(&fakeDeadLetterProducerClient{})
		t.Cleanup(producer.Close)
		if err := producer.Write(context.Background(), []byte("key"), nil); err == nil {
			t.Fatal("Write(empty value) error = nil")
		}
	})

	t.Run("backend error", func(t *testing.T) {
		wantErr := errors.New("broker unavailable")
		producer := newDeadLetterProducer(&fakeDeadLetterProducerClient{err: wantErr})
		t.Cleanup(producer.Close)
		err := producer.Write(
			context.Background(),
			[]byte("key"),
			[]byte("TOP-SECRET"),
		)
		if !errors.Is(err, wantErr) {
			t.Fatalf("Write() error = %v，期望包含 %v", err, wantErr)
		}
		if strings.Contains(err.Error(), "TOP-SECRET") {
			t.Fatalf("Write() error 泄漏原始载荷: %v", err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		producer := newDeadLetterProducer(&fakeDeadLetterProducerClient{waitForContext: true})
		t.Cleanup(producer.Close)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := producer.Write(ctx, []byte("key"), []byte("value")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Write() error = %v，期望 context.Canceled", err)
		}
	})
}

func TestDeadLetterProducerCloseIsIdempotentAndStopsWrites(t *testing.T) {
	t.Parallel()

	client := &fakeDeadLetterProducerClient{}
	producer := newDeadLetterProducer(client)
	producer.Close()
	producer.Close()
	if client.closeCalls != 1 {
		t.Fatalf("close calls = %d，期望 1", client.closeCalls)
	}
	if err := producer.Write(context.Background(), []byte("key"), []byte("value")); !errors.Is(err, ErrDeadLetterProducerClosed) {
		t.Fatalf("关闭后 Write() error = %v，期望 %v", err, ErrDeadLetterProducerClosed)
	}

	var nilProducer *DeadLetterProducer
	nilProducer.Close()
	if err := nilProducer.Write(context.Background(), []byte("key"), []byte("value")); !errors.Is(err, ErrDeadLetterProducerClosed) {
		t.Fatalf("nil producer Write() error = %v，期望 %v", err, ErrDeadLetterProducerClosed)
	}
}

type fakeDeadLetterProducerClient struct {
	records        []*kgo.Record
	err            error
	waitForContext bool
	closeCalls     int
}

func (client *fakeDeadLetterProducerClient) produce(ctx context.Context, record *kgo.Record) error {
	client.records = append(client.records, record)
	if client.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	return client.err
}

func (client *fakeDeadLetterProducerClient) close() {
	client.closeCalls++
}
