package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunApplicationDoesNotStartRunnerWhenListenFails(t *testing.T) {
	t.Parallel()

	listenErr := errors.New("address already in use")
	var runnerCalled atomic.Bool
	state := newHealthState()
	health := &healthService{
		listen:   func() (net.Listener, error) { return nil, listenErr },
		serve:    func(net.Listener) error { return nil },
		shutdown: func(context.Context) error { return nil },
	}
	err := runApplication(
		context.Background(),
		func(context.Context) error {
			runnerCalled.Store(true)
			return nil
		},
		health,
		state,
	)
	if !errors.Is(err, listenErr) || runnerCalled.Load() {
		t.Fatalf("error/runner = %v/%t", err, runnerCalled.Load())
	}
	if state.current() != healthStopped {
		t.Fatalf("phase = %d，期望 stopped", state.current())
	}
}

func TestRunApplicationStopsHTTPWhenRunnerFails(t *testing.T) {
	t.Parallel()

	runnerErr := errors.New("runner failed")
	state := newHealthState()
	control := newHealthServiceControl(state, http.ErrServerClosed, nil)
	err := runApplication(
		context.Background(),
		func(context.Context) error { return runnerErr },
		control.service,
		state,
	)
	if !errors.Is(err, runnerErr) {
		t.Fatalf("runApplication() error = %v", err)
	}
	control.assertStopped(t)
	if control.shutdownCalls.Load() != 1 || state.current() != healthStopped {
		t.Fatalf("shutdown/phase = %d/%d", control.shutdownCalls.Load(), state.current())
	}
}

func TestRunApplicationRejectsSilentRunnerExit(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	control := newHealthServiceControl(state, http.ErrServerClosed, nil)
	err := runApplication(
		context.Background(),
		func(context.Context) error { return nil },
		control.service,
		state,
	)
	if err == nil || !strings.Contains(err.Error(), "处理循环意外退出") {
		t.Fatalf("runApplication() error = %v", err)
	}
	control.assertStopped(t)
}

func TestRunApplicationDrainsAfterParentCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	state := newHealthState()
	control := newHealthServiceControl(state, http.ErrServerClosed, nil)
	runnerStarted := make(chan struct{})
	runnerStopped := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- runApplication(
			ctx,
			func(runContext context.Context) error {
				close(runnerStarted)
				<-runContext.Done()
				close(runnerStopped)
				return runContext.Err()
			},
			control.service,
			state,
		)
	}()
	waitForSignal(t, runnerStarted, "Runner 未启动")
	waitForPhase(t, state, healthRunning)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runApplication() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("父 context 取消后应用未退出")
	}
	waitForSignal(t, runnerStopped, "Runner 未停止")
	control.assertStopped(t)
	if control.phaseAtShutdown.Load() != uint32(healthDraining) {
		t.Fatalf("shutdown phase = %d，期望 draining", control.phaseAtShutdown.Load())
	}
}

func TestRunApplicationCancelsRunnerWhenHTTPFails(t *testing.T) {
	t.Parallel()

	serverErr := errors.New("accept failed")
	state := newHealthState()
	control := newHealthServiceControl(state, serverErr, nil)
	runnerStopped := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- runApplication(
			context.Background(),
			func(ctx context.Context) error {
				<-ctx.Done()
				close(runnerStopped)
				return ctx.Err()
			},
			control.service,
			state,
		)
	}()
	waitForSignal(t, control.started, "健康检查服务未启动")
	control.releaseServer()
	select {
	case err := <-result:
		if !errors.Is(err, serverErr) {
			t.Fatalf("runApplication() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("健康检查服务失败后应用未退出")
	}
	waitForSignal(t, runnerStopped, "健康检查服务失败后 Runner 未停止")
}

func TestRunApplicationTreatsEarlyServerClosedAsFailure(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	control := newHealthServiceControl(state, http.ErrServerClosed, nil)
	control.releaseServer()
	err := runApplication(
		context.Background(),
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		control.service,
		state,
	)
	if !errors.Is(err, http.ErrServerClosed) ||
		!strings.Contains(err.Error(), "健康检查服务意外退出") {
		t.Fatalf("runApplication() error = %v", err)
	}
}

func TestRunApplicationPreservesRunnerAndShutdownFailures(t *testing.T) {
	t.Parallel()

	runnerErr := errors.New("runner failed")
	shutdownErr := errors.New("shutdown failed")
	state := newHealthState()
	control := newHealthServiceControl(state, http.ErrServerClosed, shutdownErr)
	err := runApplication(
		context.Background(),
		func(context.Context) error { return runnerErr },
		control.service,
		state,
	)
	if !errors.Is(err, runnerErr) || !errors.Is(err, shutdownErr) {
		t.Fatalf("runApplication() error = %v", err)
	}
	control.assertStopped(t)
}

type healthServiceControl struct {
	service         *healthService
	started         chan struct{}
	stopped         chan struct{}
	releaseOnce     sync.Once
	release         chan struct{}
	shutdownCalls   atomic.Int32
	phaseAtShutdown atomic.Uint32
}

func newHealthServiceControl(
	state *healthState,
	serveErr error,
	shutdownErr error,
) *healthServiceControl {
	control := &healthServiceControl{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
		release: make(chan struct{}),
	}
	listener := newTestListener()
	control.service = &healthService{
		listen: func() (net.Listener, error) { return listener, nil },
		serve: func(net.Listener) error {
			close(control.started)
			<-control.release
			close(control.stopped)
			return serveErr
		},
		shutdown: func(context.Context) error {
			control.shutdownCalls.Add(1)
			control.phaseAtShutdown.Store(uint32(state.current()))
			control.releaseServer()
			return shutdownErr
		},
	}
	return control
}

func (control *healthServiceControl) releaseServer() {
	control.releaseOnce.Do(func() { close(control.release) })
}

func (control *healthServiceControl) assertStopped(t *testing.T) {
	t.Helper()
	waitForSignal(t, control.stopped, "健康检查服务未停止")
}

func waitForSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}

func waitForPhase(t *testing.T, state *healthState, expected healthPhase) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for state.current() != expected {
		if time.Now().After(deadline) {
			t.Fatalf("phase = %d，期望 %d", state.current(), expected)
		}
		time.Sleep(time.Millisecond)
	}
}

type testListener struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newTestListener() *testListener {
	return &testListener{closed: make(chan struct{})}
}

func (listener *testListener) Accept() (net.Conn, error) {
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *testListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (listener *testListener) Addr() net.Addr {
	return testAddress("health-test")
}

type testAddress string

func (address testAddress) Network() string { return "test" }
func (address testAddress) String() string  { return string(address) }
