//go:build integration

package kafka

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestConsumerResumesFromExplicitCommit(t *testing.T) {
	brokers := strings.Split(requiredEnvironment(t, "KAFKA_TEST_BROKERS"), ",")
	for index := range brokers {
		brokers[index] = strings.TrimSpace(brokers[index])
	}
	topic := requiredEnvironment(t, "KAFKA_TEST_TOPIC")
	groupID := requiredEnvironment(t, "KAFKA_TEST_GROUP_ID")
	firstValue := requiredEnvironment(t, "KAFKA_TEST_FIRST_VALUE")
	secondValue := requiredEnvironment(t, "KAFKA_TEST_SECOND_VALUE")

	newClient := func() *Consumer {
		consumer, err := NewConsumer(Config{
			Brokers: brokers,
			GroupID: groupID,
			Topics:  []string{topic},
		})
		if err != nil {
			t.Fatalf("创建 Kafka 集成测试消费者: %v", err)
		}
		t.Cleanup(consumer.Close)
		return consumer
	}
	poll := func(consumer *Consumer, stage string) Record {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		record, err := consumer.Poll(ctx)
		if err != nil {
			t.Fatalf("%s拉取记录: %v", stage, err)
		}
		return record
	}
	commit := func(consumer *Consumer, record Record, stage string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := consumer.Commit(ctx, record); err != nil {
			t.Fatalf("%s提交记录: %v", stage, err)
		}
	}

	firstConsumer := newClient()
	first := poll(firstConsumer, "首次")
	assertIntegrationRecord(t, first, topic, 0, firstValue)
	firstConsumer.Close() // 故意不提交，用于证明关闭不会隐式推进位点。

	repeatedConsumer := newClient()
	repeated := poll(repeatedConsumer, "未提交后重启")
	assertIntegrationRecord(t, repeated, topic, first.Offset, firstValue)
	commit(repeatedConsumer, repeated, "重复记录")
	repeatedConsumer.Close()

	resumedConsumer := newClient()
	resumed := poll(resumedConsumer, "显式提交后重启")
	assertIntegrationRecord(t, resumed, topic, first.Offset+1, secondValue)
	commit(resumedConsumer, resumed, "续读记录")
	resumedConsumer.Close()

	t.Logf(
		"topic=%s group=%s partition=0 first_offset=%d repeated_offset=%d resumed_offset=%d committed_next_offset=%d",
		topic,
		groupID,
		first.Offset,
		repeated.Offset,
		resumed.Offset,
		resumed.Offset+1,
	)
}

func requiredEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("缺少集成测试环境变量 %s", name)
	}
	return value
}

func assertIntegrationRecord(
	t *testing.T,
	record Record,
	topic string,
	offset int64,
	value string,
) {
	t.Helper()
	if record.Topic != topic || record.Partition != 0 || record.Offset != offset {
		t.Fatalf(
			"记录位置 = %s/%d/%d，期望 %s/0/%d",
			record.Topic,
			record.Partition,
			record.Offset,
			topic,
			offset,
		)
	}
	if string(record.Value) != value {
		t.Fatalf("记录值 = %q，期望 %q", record.Value, value)
	}
}
