package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunCloseOperationsStartsAllAndRespectsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 3)
	blocked := make(chan struct{})
	blockedDone := make(chan struct{})
	result := make(chan error, 1)

	go func() {
		result <- runCloseOperations(
			ctx,
			func() error {
				started <- struct{}{}
				<-blocked
				close(blockedDone)
				return nil
			},
			func() error {
				started <- struct{}{}
				return nil
			},
			func() error {
				started <- struct{}{}
				return nil
			},
		)
	}()

	for range 3 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("关闭操作未全部启动")
		}
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runCloseOperations() error = %v，期望 context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("取消后关闭等待未及时返回")
	}

	close(blocked)
	select {
	case <-blockedDone:
	case <-time.After(time.Second):
		t.Fatal("被阻塞的关闭操作未完成")
	}
}
