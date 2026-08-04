//go:build integration

package pipeline

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

func TestProcessorCommitsOriginalKafkaRecord(t *testing.T) {
	brokers := strings.Split(requiredIntegrationEnvironment(t, "KAFKA_TEST_BROKERS"), ",")
	for index := range brokers {
		brokers[index] = strings.TrimSpace(brokers[index])
	}
	topic := requiredIntegrationEnvironment(t, "KAFKA_TEST_TOPIC")
	groupID := requiredIntegrationEnvironment(t, "KAFKA_PIPELINE_GROUP_ID")
	wantValue := requiredIntegrationEnvironment(t, "KAFKA_TEST_FIRST_VALUE")

	consumer, err := kafka.NewConsumer(kafka.Config{
		Brokers: brokers,
		GroupID: groupID,
		Topics:  []string{topic},
	})
	if err != nil {
		t.Fatalf("创建 pipeline 集成测试消费者: %v", err)
	}
	t.Cleanup(consumer.Close)

	pollContext, cancelPoll := context.WithTimeout(context.Background(), 30*time.Second)
	record, err := consumer.Poll(pollContext)
	cancelPoll()
	if err != nil {
		t.Fatalf("拉取 pipeline 集成测试记录: %v", err)
	}
	if record.Topic != topic || record.Partition != 0 || record.Offset != 0 {
		t.Fatalf("记录位置 = %s/%d/%d，期望 %s/0/0",
			record.Topic, record.Partition, record.Offset, topic)
	}
	if string(record.Value) != wantValue {
		t.Fatalf("记录 value 与受控 fixture 不一致")
	}

	writer := &integrationBulkWriter{}
	processor, err := New(
		Config{
			Index:         "logs-stage3-pipeline-token-integration",
			WriteTimeout:  10 * time.Second,
			CommitTimeout: 10 * time.Second,
		},
		writer,
		consumer,
	)
	if err != nil {
		t.Fatalf("创建 pipeline 集成测试处理器: %v", err)
	}

	processContext, cancelProcess := context.WithTimeout(context.Background(), 30*time.Second)
	result, err := processor.Process(processContext, record)
	cancelProcess()
	if err != nil {
		t.Fatalf("处理真实 Kafka 记录: %v", err)
	}
	if result.Kind != ResultCreated || !result.Committed {
		t.Fatalf("处理结果 = %#v，期望 created/committed", result)
	}
	if result.ElasticsearchResult != elasticsearch.ResultCreated || writer.calls != 1 {
		t.Fatalf("Elasticsearch 结果/调用次数 = %q/%d，期望 created/1",
			result.ElasticsearchResult, writer.calls)
	}

	t.Logf(
		"topic=%s group=%s partition=0 processed_offset=%d committed_next_offset=%d",
		topic,
		groupID,
		record.Offset,
		record.Offset+1,
	)
}

type integrationBulkWriter struct {
	calls int
}

func (writer *integrationBulkWriter) CreateBatch(
	ctx context.Context,
	index string,
	documents []elasticsearch.Document,
) ([]elasticsearch.CreateResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if index != "logs-stage3-pipeline-token-integration" || len(documents) != 1 {
		return nil, fmt.Errorf("pipeline 集成测试写入契约异常: index=%q documents=%d", index, len(documents))
	}
	writer.calls++
	return []elasticsearch.CreateResult{{
		Kind:       elasticsearch.ResultCreated,
		StatusCode: 201,
	}}, nil
}

func requiredIntegrationEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("缺少集成测试环境变量 %s", name)
	}
	return value
}
