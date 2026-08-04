package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerPollProjectsAndCopiesRecord(t *testing.T) {
	t.Parallel()

	raw := &kgo.Record{
		Topic:     "logs.api-service",
		Partition: 2,
		Offset:    17,
		Key:       []byte("event-key"),
		Value:     []byte(`{"message":"hello"}`),
	}
	client := &fakeConsumerClient{records: []*kgo.Record{raw}}
	consumer := newConsumer(client)
	t.Cleanup(consumer.Close)

	record, err := consumer.Poll(context.Background())
	if err != nil {
		t.Fatalf("拉取记录: %v", err)
	}
	if record.Topic != raw.Topic || record.Partition != raw.Partition || record.Offset != raw.Offset {
		t.Fatalf("记录位置 = %s/%d/%d，期望 %s/%d/%d",
			record.Topic, record.Partition, record.Offset,
			raw.Topic, raw.Partition, raw.Offset,
		)
	}
	if !reflect.DeepEqual(record.Key, raw.Key) || !reflect.DeepEqual(record.Value, raw.Value) {
		t.Fatalf("记录内容 = key %q value %q，期望 key %q value %q",
			record.Key, record.Value, raw.Key, raw.Value,
		)
	}
	if client.pollLimits[0] != 1 {
		t.Fatalf("单次最大拉取数 = %d，期望 1", client.pollLimits[0])
	}

	raw.Key[0] = 'X'
	raw.Value[0] = 'X'
	if string(record.Key) != "event-key" || string(record.Value) != `{"message":"hello"}` {
		t.Fatalf("投影记录被底层缓冲区修改: key %q value %q", record.Key, record.Value)
	}
}

func TestConsumerRequiresCommitBeforeNextPoll(t *testing.T) {
	t.Parallel()

	client := &fakeConsumerClient{records: []*kgo.Record{
		{Topic: "logs.api-service", Partition: 0, Offset: 4},
		{Topic: "logs.api-service", Partition: 0, Offset: 5},
	}}
	consumer := newConsumer(client)
	t.Cleanup(consumer.Close)

	first, err := consumer.Poll(context.Background())
	if err != nil {
		t.Fatalf("首次拉取: %v", err)
	}
	if _, err := consumer.Poll(context.Background()); !errors.Is(err, ErrRecordPending) {
		t.Fatalf("存在待确认记录时再次拉取错误 = %v，期望 %v", err, ErrRecordPending)
	}
	if len(client.pollLimits) != 1 {
		t.Fatalf("底层拉取次数 = %d，期望 1", len(client.pollLimits))
	}

	if err := consumer.Commit(context.Background(), first); err != nil {
		t.Fatalf("提交首条记录: %v", err)
	}
	if len(client.committed) != 1 || client.committed[0].Offset != 4 {
		t.Fatalf("底层提交记录 = %#v，期望 offset 4", client.committed)
	}
	if client.allowCalls != 1 {
		t.Fatalf("放行重平衡次数 = %d，期望 1", client.allowCalls)
	}

	second, err := consumer.Poll(context.Background())
	if err != nil {
		t.Fatalf("提交后拉取下一条: %v", err)
	}
	if second.Offset != 5 {
		t.Fatalf("下一条 offset = %d，期望 5", second.Offset)
	}
}

func TestConsumerRejectsRecordOutsideCurrentPending(t *testing.T) {
	t.Parallel()

	firstClient := &fakeConsumerClient{records: []*kgo.Record{
		{Topic: "logs.api-service", Partition: 0, Offset: 9},
	}}
	secondClient := &fakeConsumerClient{records: []*kgo.Record{
		{Topic: "logs.worker-service", Partition: 1, Offset: 3},
	}}
	firstConsumer := newConsumer(firstClient)
	secondConsumer := newConsumer(secondClient)
	t.Cleanup(firstConsumer.Close)
	t.Cleanup(secondConsumer.Close)

	first, err := firstConsumer.Poll(context.Background())
	if err != nil {
		t.Fatalf("第一消费者拉取: %v", err)
	}
	second, err := secondConsumer.Poll(context.Background())
	if err != nil {
		t.Fatalf("第二消费者拉取: %v", err)
	}

	for name, record := range map[string]Record{
		"零值记录":    {},
		"其他消费者记录": second,
	} {
		t.Run(name, func(t *testing.T) {
			if err := firstConsumer.Commit(context.Background(), record); !errors.Is(err, ErrRecordNotPending) {
				t.Fatalf("提交错误 = %v，期望 %v", err, ErrRecordNotPending)
			}
		})
	}
	if len(firstClient.committed) != 0 || firstClient.allowCalls != 0 {
		t.Fatalf("非法提交触发底层操作: commits %d allows %d",
			len(firstClient.committed), firstClient.allowCalls,
		)
	}

	if err := firstConsumer.Commit(context.Background(), first); err != nil {
		t.Fatalf("提交当前记录: %v", err)
	}
	if err := firstConsumer.Commit(context.Background(), first); !errors.Is(err, ErrRecordNotPending) {
		t.Fatalf("重复提交错误 = %v，期望 %v", err, ErrRecordNotPending)
	}
}

func TestConsumerRetainsPendingAfterCommitFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("temporary commit failure")
	client := &fakeConsumerClient{
		records:    []*kgo.Record{{Topic: "logs.api-service", Partition: 0, Offset: 11}},
		commitErrs: []error{wantErr, nil},
	}
	consumer := newConsumer(client)
	t.Cleanup(consumer.Close)

	record, err := consumer.Poll(context.Background())
	if err != nil {
		t.Fatalf("拉取记录: %v", err)
	}
	if err := consumer.Commit(context.Background(), record); !errors.Is(err, wantErr) {
		t.Fatalf("首次提交错误 = %v，期望 %v", err, wantErr)
	}
	if client.allowCalls != 0 {
		t.Fatalf("失败提交后放行重平衡次数 = %d，期望 0", client.allowCalls)
	}
	if _, err := consumer.Poll(context.Background()); !errors.Is(err, ErrRecordPending) {
		t.Fatalf("失败提交后再次拉取错误 = %v，期望 %v", err, ErrRecordPending)
	}

	if err := consumer.Commit(context.Background(), record); err != nil {
		t.Fatalf("重试提交: %v", err)
	}
	if len(client.committed) != 2 || client.allowCalls != 1 {
		t.Fatalf("重试后 commits = %d allows = %d，期望 2/1",
			len(client.committed), client.allowCalls,
		)
	}
}

func TestConsumerCanceledPollReleasesRebalanceBeforeClose(t *testing.T) {
	t.Parallel()

	consumer, err := NewConsumer(Config{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "log-processor-canceled-poll-test",
		Topics:  []string{"logs.api-service"},
	})
	if err != nil {
		t.Fatalf("创建消费者: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := consumer.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消拉取错误 = %v，期望 %v", err, context.Canceled)
	}

	closed := make(chan struct{})
	go func() {
		consumer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("取消拉取后 Close 被重平衡阻塞")
	}
}

func TestInspectFetchesPreservesRecordAndPartitionError(t *testing.T) {
	t.Parallel()

	wantRecord := &kgo.Record{
		Topic:     "logs.api-service",
		Partition: 0,
		Offset:    8,
	}
	wantErr := errors.New("partition unavailable")
	secondErr := errors.New("partition authorization failed")
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "logs.api-service",
			Partitions: []kgo.FetchPartition{
				{Partition: 0, Records: []*kgo.Record{wantRecord}},
				{Partition: 1, Err: wantErr},
				{Partition: 2, Err: secondErr},
			},
		}},
	}}

	record, err := inspectFetches(fetches)
	if record != wantRecord {
		t.Fatalf("返回记录 = %#v，期望 %#v", record, wantRecord)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("返回错误 = %v，期望包含 %v", err, wantErr)
	}
	if !errors.Is(err, secondErr) {
		t.Fatalf("返回错误 = %v，期望包含 %v", err, secondErr)
	}

	fakeErr := context.DeadlineExceeded
	record, err = inspectFetches(kgo.NewErrFetch(fakeErr))
	if record != nil || !errors.Is(err, fakeErr) {
		t.Fatalf("错误 fetch 返回 record=%#v err=%v，期望 nil/%v", record, err, fakeErr)
	}
}

func TestConsumerPollAndCloseErrors(t *testing.T) {
	t.Parallel()

	t.Run("nil context", func(t *testing.T) {
		consumer := newConsumer(&fakeConsumerClient{})
		t.Cleanup(consumer.Close)
		if _, err := consumer.Poll(nil); err == nil {
			t.Fatal("nil context 拉取期望失败")
		}
	})

	t.Run("backend error", func(t *testing.T) {
		wantErr := errors.New("fetch failed")
		consumer := newConsumer(&fakeConsumerClient{pollErr: wantErr})
		t.Cleanup(consumer.Close)
		if _, err := consumer.Poll(context.Background()); !errors.Is(err, wantErr) {
			t.Fatalf("拉取错误 = %v，期望 %v", err, wantErr)
		}
		if got := consumer.client.(*fakeConsumerClient).allowCalls; got != 1 {
			t.Fatalf("拉取错误后放行重平衡次数 = %d，期望 1", got)
		}
	})

	t.Run("empty poll", func(t *testing.T) {
		consumer := newConsumer(&fakeConsumerClient{})
		t.Cleanup(consumer.Close)
		if _, err := consumer.Poll(context.Background()); !errors.Is(err, ErrNoRecord) {
			t.Fatalf("空拉取错误 = %v，期望 %v", err, ErrNoRecord)
		}
		if got := consumer.client.(*fakeConsumerClient).allowCalls; got != 1 {
			t.Fatalf("空拉取后放行重平衡次数 = %d，期望 1", got)
		}
	})

	t.Run("close is idempotent and never commits", func(t *testing.T) {
		client := &fakeConsumerClient{records: []*kgo.Record{
			{Topic: "logs.api-service", Partition: 0, Offset: 21},
		}}
		consumer := newConsumer(client)
		if _, err := consumer.Poll(context.Background()); err != nil {
			t.Fatalf("关闭前拉取: %v", err)
		}
		consumer.Close()
		consumer.Close()

		if client.closeCalls != 1 {
			t.Fatalf("关闭次数 = %d，期望 1", client.closeCalls)
		}
		if len(client.committed) != 0 || client.allowCalls != 0 {
			t.Fatalf("关闭触发隐式操作: commits %d allows %d",
				len(client.committed), client.allowCalls,
			)
		}
		if _, err := consumer.Poll(context.Background()); !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("关闭后拉取错误 = %v，期望 %v", err, ErrConsumerClosed)
		}
		if err := consumer.Commit(context.Background(), Record{}); !errors.Is(err, ErrConsumerClosed) {
			t.Fatalf("关闭后提交错误 = %v，期望 %v", err, ErrConsumerClosed)
		}
	})
}

type fakeConsumerClient struct {
	records    []*kgo.Record
	pollErr    error
	commitErrs []error

	pollLimits []int
	committed  []*kgo.Record
	allowCalls int
	closeCalls int
}

func (client *fakeConsumerClient) poll(_ context.Context, maxRecords int) (*kgo.Record, error) {
	client.pollLimits = append(client.pollLimits, maxRecords)
	if client.pollErr != nil {
		return nil, client.pollErr
	}
	if len(client.records) == 0 {
		return nil, nil
	}
	record := client.records[0]
	client.records = client.records[1:]
	return record, nil
}

func (client *fakeConsumerClient) commit(_ context.Context, record *kgo.Record) error {
	client.committed = append(client.committed, record)
	if len(client.commitErrs) == 0 {
		return nil
	}
	err := client.commitErrs[0]
	client.commitErrs = client.commitErrs[1:]
	return err
}

func (client *fakeConsumerClient) allowRebalance() {
	client.allowCalls++
}

func (client *fakeConsumerClient) closeAllowingRebalance() {
	client.closeCalls++
}
