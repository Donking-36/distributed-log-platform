package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	healthReadHeaderTimeout = 2 * time.Second
	healthWriteTimeout      = 5 * time.Second
	healthIdleTimeout       = 30 * time.Second
	healthMaxHeaderBytes    = 8 << 10
)

type healthPhase uint32

const (
	healthStarting healthPhase = iota
	healthRunning
	healthDraining
	healthStopped
)

// healthState 使用单一原子状态，避免健康与就绪出现互相矛盾的组合。
type healthState struct {
	phase atomic.Uint32
}

func newHealthState() *healthState {
	return &healthState{}
}

func (state *healthState) set(phase healthPhase) {
	state.phase.Store(uint32(phase))
}

func (state *healthState) current() healthPhase {
	return healthPhase(state.phase.Load())
}

// healthService 封装标准库 HTTP 服务的生命周期边界，便于验证异常退出路径。
type healthService struct {
	listen   func() (net.Listener, error)
	serve    func(net.Listener) error
	shutdown func(context.Context) error
}

func newHealthService(address string, state *healthState) (*healthService, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("健康检查监听地址不能为空")
	}
	if state == nil {
		return nil, errors.New("健康状态不能为空")
	}

	server := &http.Server{
		Addr:              address,
		Handler:           newHealthHandler(state),
		ReadHeaderTimeout: healthReadHeaderTimeout,
		WriteTimeout:      healthWriteTimeout,
		IdleTimeout:       healthIdleTimeout,
		MaxHeaderBytes:    healthMaxHeaderBytes,
	}
	return &healthService{
		listen: func() (net.Listener, error) {
			return net.Listen("tcp", address)
		},
		serve:    server.Serve,
		shutdown: server.Shutdown,
	}, nil
}

func newHealthHandler(state *healthState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		phase := state.current()
		writeProbeResponse(
			writer,
			request,
			phase == healthRunning || phase == healthDraining,
		)
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		writeProbeResponse(writer, request, state.current() == healthRunning)
	})
	return mux
}

func writeProbeResponse(
	writer http.ResponseWriter,
	request *http.Request,
	success bool,
) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if success {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
		return
	}
	writer.WriteHeader(http.StatusServiceUnavailable)
	_, _ = writer.Write([]byte("unavailable\n"))
}
