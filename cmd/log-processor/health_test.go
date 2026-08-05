package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthHandlerReportsLifecycleState(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	handler := newHealthHandler(state)
	tests := []struct {
		name   string
		phase  healthPhase
		health int
		ready  int
	}{
		{name: "启动中", phase: healthStarting, health: 503, ready: 503},
		{name: "运行中", phase: healthRunning, health: 200, ready: 200},
		{name: "排空中", phase: healthDraining, health: 200, ready: 503},
		{name: "已停止", phase: healthStopped, health: 503, ready: 503},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state.set(test.phase)
			for path, expected := range map[string]int{
				"/healthz": test.health,
				"/readyz":  test.ready,
			} {
				request := httptest.NewRequest(http.MethodGet, path, nil)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != expected {
					t.Fatalf("%s status = %d，期望 %d", path, response.Code, expected)
				}
				if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain") {
					t.Fatalf("%s Content-Type = %q", path, contentType)
				}
			}
		})
	}
}
