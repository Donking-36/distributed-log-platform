//go:build integration

package pipeline

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/event"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

const runnerIntegrationIndexPrefix = "logs-stage3-runner-integration-"

func TestRunnerProcessesValidInvalidValidAgainstRealServices(t *testing.T) {
	brokers := strings.Split(requiredIntegrationEnvironment(t, "KAFKA_TEST_BROKERS"), ",")
	for index := range brokers {
		brokers[index] = strings.TrimSpace(brokers[index])
	}
	topic := requiredIntegrationEnvironment(t, "KAFKA_TEST_TOPIC")
	groupID := requiredIntegrationEnvironment(t, "KAFKA_RUNNER_GROUP_ID")
	firstValue := []byte(requiredIntegrationEnvironment(t, "KAFKA_TEST_FIRST_VALUE"))
	invalidValue := []byte(requiredIntegrationEnvironment(t, "KAFKA_TEST_SECOND_VALUE"))
	thirdValue := []byte(requiredIntegrationEnvironment(t, "KAFKA_TEST_THIRD_VALUE"))
	testRunID := requiredIntegrationEnvironment(t, "RUNNER_TEST_RUN_ID")
	endpoint := strings.TrimRight(
		requiredIntegrationEnvironment(t, "ELASTICSEARCH_URL"),
		"/",
	)
	index := requiredIntegrationEnvironment(t, "ELASTICSEARCH_TEST_INDEX")
	if !strings.HasPrefix(index, runnerIntegrationIndexPrefix) ||
		len(index) == len(runnerIntegrationIndexPrefix) {
		t.Fatalf("ELASTICSEARCH_TEST_INDEX 不符合 Runner 临时索引约定: %q", index)
	}
	dlqStartOffset, err := strconv.ParseInt(
		requiredIntegrationEnvironment(t, "KAFKA_DLQ_START_OFFSET"),
		10,
		64,
	)
	if err != nil || dlqStartOffset < 0 {
		t.Fatalf("KAFKA_DLQ_START_OFFSET 必须是非负整数")
	}

	assertRunnerIntegrationIndexAbsent(t, endpoint, index)
	t.Cleanup(func() {
		cleanupRunnerIntegrationIndex(t, endpoint, index)
	})

	consumer, err := kafka.NewConsumer(kafka.Config{
		Brokers: brokers,
		GroupID: groupID,
		Topics:  []string{topic},
	})
	if err != nil {
		t.Fatalf("创建 Runner 集成测试消费者: %v", err)
	}
	t.Cleanup(consumer.Close)

	elasticsearchClient, err := elasticsearch.NewClient(endpoint)
	if err != nil {
		t.Fatalf("创建 Runner 集成测试 Elasticsearch 客户端: %v", err)
	}
	t.Cleanup(func() {
		closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelClose()
		if err := elasticsearchClient.Close(closeContext); err != nil {
			t.Errorf("关闭 Runner 集成测试 Elasticsearch 客户端: %v", err)
		}
	})

	deadLetterProducer, err := kafka.NewDeadLetterProducer(kafka.DeadLetterProducerConfig{
		Brokers: brokers,
	})
	if err != nil {
		t.Fatalf("创建 Runner 集成测试 DLQ 生产者: %v", err)
	}
	t.Cleanup(deadLetterProducer.Close)

	processor, err := New(
		Config{
			Index:         index,
			WriteTimeout:  10 * time.Second,
			CommitTimeout: 10 * time.Second,
		},
		elasticsearchClient,
		consumer,
	)
	if err != nil {
		t.Fatalf("创建 Runner 集成测试处理器: %v", err)
	}
	cycle, err := NewDeliveryCycle(processor)
	if err != nil {
		t.Fatalf("创建 Runner 集成测试投递周期: %v", err)
	}
	deadLetterHandler, err := NewDeadLetterHandler(
		DeadLetterConfig{
			PublishTimeout: 10 * time.Second,
			CommitTimeout:  10 * time.Second,
		},
		deadLetterProducer,
		consumer,
	)
	if err != nil {
		t.Fatalf("创建 Runner 集成测试死信处理器: %v", err)
	}

	// 第四次 Poll 只负责确定性结束测试。Runner 只有在第三条记录已经处理并提交后
	// 才会发起下一次 Poll，因此这里不会把 ES 已写入但 Kafka 未提交误判为成功。
	controlledRecords := 0
	controlledStop := errors.New("Runner 集成测试受控记录已处理完毕")
	runner, err := newRunner(
		func(ctx context.Context) (kafka.Record, error) {
			if controlledRecords == 3 {
				return kafka.Record{}, controlledStop
			}
			record, pollErr := consumer.Poll(ctx)
			if pollErr == nil {
				controlledRecords++
			}
			return record, pollErr
		},
		cycle.Deliver,
		deadLetterHandler.Handle,
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("创建 Runner 集成测试运行器: %v", err)
	}

	runContext, cancelRun := context.WithTimeout(context.Background(), 60*time.Second)
	runErr := runner.Run(runContext)
	runContextErr := runContext.Err()
	cancelRun()
	if runContextErr != nil {
		t.Fatalf("Runner 集成测试超时: %v", runContextErr)
	}
	if !errors.Is(runErr, controlledStop) || controlledRecords != 3 {
		t.Fatalf("Runner 结束结果 = %v，已拉取 %d 条，期望受控结束且恰好三条", runErr, controlledRecords)
	}

	assertRunnerIntegrationDocuments(t, endpoint, index, firstValue, thirdValue)
	dlqRecord := readRunnerIntegrationDLQ(t, brokers, dlqStartOffset)
	assertRunnerIntegrationDLQ(
		t,
		dlqRecord,
		topic,
		invalidValue,
		testRunID,
		dlqStartOffset,
	)

	t.Logf(
		"真实 Runner 链路通过: topic=%s group=%s source_next=3 es_index=%s es_documents=2 dlq_offset=%d",
		topic,
		groupID,
		index,
		dlqRecord.Offset,
	)
}

func assertRunnerIntegrationIndexAbsent(t *testing.T, endpoint, index string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := runnerIntegrationRequest(ctx, http.MethodHead, endpoint, url.PathEscape(index))
	if err != nil {
		t.Fatalf("确认 Runner 临时索引不存在: %v", err)
	}
	if status != http.StatusNotFound {
		t.Fatalf("拒绝复用已存在的 Runner 临时索引 %q: HTTP %d", index, status)
	}
}

func cleanupRunnerIntegrationIndex(t *testing.T, endpoint, index string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := runnerIntegrationRequest(ctx, http.MethodDelete, endpoint, url.PathEscape(index))
	if err != nil {
		t.Errorf("清理 Runner 临时索引 %q: %v", index, err)
		return
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		t.Errorf("清理 Runner 临时索引 %q 返回 HTTP %d", index, status)
		return
	}
	status, err = runnerIntegrationRequest(ctx, http.MethodHead, endpoint, url.PathEscape(index))
	if err != nil || status != http.StatusNotFound {
		t.Errorf("复核 Runner 临时索引 %q: HTTP %d, error=%v", index, status, err)
	}
}

func assertRunnerIntegrationDocuments(
	t *testing.T,
	endpoint string,
	index string,
	validPayloads ...[]byte,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	indexPath := url.PathEscape(index)
	status, err := runnerIntegrationRequest(ctx, http.MethodPost, endpoint, indexPath+"/_refresh")
	if err != nil || status != http.StatusOK {
		t.Fatalf("刷新 Runner 临时索引: HTTP %d, error=%v", status, err)
	}
	count, err := runnerIntegrationDocumentCount(ctx, endpoint, indexPath)
	if err != nil {
		t.Fatalf("统计 Runner 临时索引文档: %v", err)
	}
	if count != int64(len(validPayloads)) {
		t.Fatalf("Runner 临时索引文档数 = %d，期望 %d", count, len(validPayloads))
	}
	for _, payload := range validPayloads {
		normalized, err := event.ParseFilebeat(payload)
		if err != nil {
			t.Fatalf("解析 Runner 有效 fixture: %v", err)
		}
		documentID, err := event.EventID(normalized)
		if err != nil {
			t.Fatalf("计算 Runner 有效 fixture 事件 ID: %v", err)
		}
		status, err := runnerIntegrationRequest(
			ctx,
			http.MethodHead,
			endpoint,
			indexPath+"/_doc/"+url.PathEscape(documentID),
		)
		if err != nil || status != http.StatusOK {
			t.Fatalf("确认 Elasticsearch 文档 %q: HTTP %d, error=%v", documentID, status, err)
		}
	}
}

func runnerIntegrationDocumentCount(ctx context.Context, endpoint, indexPath string) (int64, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		endpoint+"/"+indexPath+"/_count",
		nil,
	)
	if err != nil {
		return 0, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("Elasticsearch count 返回 HTTP %d", response.StatusCode)
	}
	var result struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return 0, err
	}
	return result.Count, nil
}

func readRunnerIntegrationDLQ(t *testing.T, brokers []string, startOffset int64) *kgo.Record {
	t.Helper()
	observer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("log-processor-runner-integration-observer"),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			kafka.DeadLetterTopic: {0: kgo.NewOffset().At(startOffset)},
		}),
	)
	if err != nil {
		t.Fatalf("创建 Runner DLQ 观察客户端: %v", err)
	}
	t.Cleanup(observer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fetches := observer.PollRecords(ctx, 1)
	for _, fetchError := range fetches.Errors() {
		if fetchError.Err != nil {
			t.Fatalf("读取 Runner DLQ 记录: topic=%q partition=%d error=%v",
				fetchError.Topic, fetchError.Partition, fetchError.Err)
		}
	}
	records := fetches.Records()
	if len(records) != 1 {
		t.Fatalf("Runner DLQ 记录数 = %d，期望 1", len(records))
	}
	return records[0]
}

func assertRunnerIntegrationDLQ(
	t *testing.T,
	record *kgo.Record,
	sourceTopic string,
	originalPayload []byte,
	testRunID string,
	wantOffset int64,
) {
	t.Helper()
	if record == nil {
		t.Fatal("Runner DLQ 记录不能为空")
	}
	if record.Topic != kafka.DeadLetterTopic || record.Partition != 0 || record.Offset != wantOffset {
		t.Fatalf("DLQ 位置 = %s/%d/%d，期望 %s/0/%d",
			record.Topic, record.Partition, record.Offset, kafka.DeadLetterTopic, wantOffset)
	}
	wantKey := fmt.Sprintf("v1|%d:%s|0|1", len(sourceTopic), sourceTopic)
	if string(record.Key) != wantKey {
		t.Fatalf("DLQ key = %q，期望 %q", record.Key, wantKey)
	}

	var envelope deadLetterEnvelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		t.Fatalf("解析 Runner DLQ envelope: %v", err)
	}
	if envelope.SchemaVersion != deadLetterSchemaVersion ||
		envelope.Source.Topic != sourceTopic ||
		envelope.Source.Partition != 0 ||
		envelope.Source.Offset != 1 ||
		envelope.Error.Category != "event_validation" ||
		envelope.Error.Field != "service.name" ||
		envelope.Attempts != 1 ||
		envelope.TestRunID != testRunID ||
		envelope.OriginalPayload.Encoding != "base64" {
		t.Fatalf("DLQ envelope 与永久无效记录契约不一致: %#v", envelope)
	}
	decodedPayload, err := base64.StdEncoding.DecodeString(envelope.OriginalPayload.Data)
	if err != nil {
		t.Fatalf("解码 DLQ 原始载荷: %v", err)
	}
	if !bytes.Equal(decodedPayload, originalPayload) {
		t.Fatalf("DLQ 原始载荷与源记录不一致")
	}
}

func runnerIntegrationRequest(
	ctx context.Context,
	method string,
	endpoint string,
	path string,
) (int, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		method,
		endpoint+"/"+strings.TrimLeft(path, "/"),
		nil,
	)
	if err != nil {
		return 0, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode, nil
}
