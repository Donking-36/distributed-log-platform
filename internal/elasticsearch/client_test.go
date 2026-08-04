package elasticsearch

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type performerFunc func(*http.Request) (*http.Response, error)

func (function performerFunc) Perform(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestClientCreateBatchSendsOneNDJSONRequest(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	var calls atomic.Int32
	client := newClient(performerFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		if request.Method != http.MethodPost {
			t.Errorf("request method = %q, want POST", request.Method)
		}
		if request.URL.Path != "/_bulk" {
			t.Errorf("request path = %q, want /_bulk", request.URL.Path)
		}
		if got := request.Header.Get("Content-Type"); got != "application/x-ndjson" {
			t.Errorf("Content-Type = %q", got)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		if !strings.Contains(string(body), `"create"`) || !strings.Contains(string(body), document.id) {
			t.Errorf("request body does not contain create action and ID: %s", body)
		}
		return bulkHTTPResponse(
			http.StatusOK,
			`{"errors":false,"items":[{"create":{"_id":"`+document.id+`","status":201}}]}`,
		), nil
	}), nil)

	results, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	)
	if err != nil {
		t.Fatalf("CreateBatch() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("transport calls = %d, want 1", calls.Load())
	}
	if len(results) != 1 || results[0].Kind != ResultCreated {
		t.Fatalf("CreateBatch() results = %#v", results)
	}
}

func TestClientCreateBatchSkipsTransportForEmptyBatch(t *testing.T) {
	t.Parallel()

	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("transport must not be called for an empty batch")
		return nil, nil
	}), nil)
	results, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		nil,
	)
	if err != nil {
		t.Fatalf("CreateBatch(nil) error = %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("CreateBatch(nil) results = %#v", results)
	}
}

func TestClientCreateBatchClassifiesRequestFailures(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	tests := []struct {
		name      string
		status    int
		retryable bool
	}{
		{name: "request timeout", status: 408, retryable: true},
		{name: "too many requests", status: 429, retryable: true},
		{name: "bad request", status: 400, retryable: false},
		{name: "unauthorized", status: 401, retryable: false},
		{name: "server error", status: 500, retryable: true},
		{name: "service unavailable", status: 503, retryable: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
				return bulkHTTPResponse(test.status, `{"error":{"type":"test"}}`), nil
			}), nil)
			results, err := client.CreateBatch(
				context.Background(),
				"logs-stage3-client-test",
				[]Document{document},
			)
			if err == nil {
				t.Fatalf("CreateBatch() results = %#v, error = nil", results)
			}
			if results != nil {
				t.Fatalf("CreateBatch() returned partial results %#v", results)
			}
			var requestErr *RequestError
			if !errors.As(err, &requestErr) {
				t.Fatalf("error = %T %v, want *RequestError", err, err)
			}
			if requestErr.StatusCode != test.status {
				t.Fatalf("StatusCode = %d, want %d", requestErr.StatusCode, test.status)
			}
			if requestErr.Retryable() != test.retryable {
				t.Fatalf("Retryable() = %t, want %t", requestErr.Retryable(), test.retryable)
			}
		})
	}
}

func TestClientCreateBatchDoesNotRetryUnknownTransportFailure(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	transportErr := errors.New("client is closed")
	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	}), nil)
	_, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	)
	var requestErr *RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error = %T %v, want *RequestError", err, err)
	}
	if requestErr.Retryable() {
		t.Fatal("unknown transport error must be classified conservatively")
	}
	if !errors.Is(err, transportErr) {
		t.Fatalf("error = %v, want wrapped transport error", err)
	}
}

func TestClientCreateBatchRetriesTemporaryNetworkFailure(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	transportErr := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: syscall.ECONNRESET,
	}
	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	}), nil)
	_, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	)
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || !requestErr.Retryable() {
		t.Fatalf("CreateBatch() error = %v, want retryable RequestError", err)
	}
}

func TestPermanentSocketFailuresAreNotRetryable(t *testing.T) {
	t.Parallel()

	for name, cause := range map[string]error{
		"permission denied":   syscall.EACCES,
		"too many open files": syscall.EMFILE,
	} {
		cause := cause
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := &net.OpError{Op: "dial", Net: "tcp", Err: cause}
			if isRetryableTransportError(err) {
				t.Fatalf("%v must not be retryable", err)
			}
		})
	}
}

func TestInvalidNetworkAddressIsNotRetryable(t *testing.T) {
	t.Parallel()

	err := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: &net.AddrError{Err: "invalid port", Addr: "127.0.0.1:99999"},
	}
	if isRetryableTransportError(err) {
		t.Fatal("net.AddrError wrapped by net.OpError must not be retryable")
	}
}

func TestClientCreateBatchClosesBodyWhenTransportReturnsResponseAndError(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	body := &trackingReadCloser{Reader: strings.NewReader(`{"error":"proxy failure"}`)}
	transportErr := errors.New("product check failed")
	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Header:     make(http.Header),
			Body:       body,
		}, transportErr
	}), nil)

	_, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	)
	if !errors.Is(err, transportErr) {
		t.Fatalf("CreateBatch() error = %v, want wrapped transport error", err)
	}
	if !body.closed.Load() {
		t.Fatal("response body was not closed when transport returned both response and error")
	}
}

func TestClientCreateBatchPreservesCancellation(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := newClient(performerFunc(func(request *http.Request) (*http.Response, error) {
		return nil, request.Context().Err()
	}), nil)
	_, err := client.CreateBatch(
		ctx,
		"logs-stage3-client-test",
		[]Document{document},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	var requestErr *RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error = %T %v, want *RequestError", err, err)
	}
	if requestErr.Retryable() {
		t.Fatal("graceful cancellation must not be classified as retryable")
	}
}

func TestClientCreateBatchClosesResponseBody(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	body := &trackingReadCloser{Reader: strings.NewReader(
		`{"errors":false,"items":[{"create":{"_id":"` + document.id + `","status":201}}]}`,
	)}
	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
		}, nil
	}), nil)
	if _, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	); err != nil {
		t.Fatalf("CreateBatch() error = %v", err)
	}
	if !body.closed.Load() {
		t.Fatal("response body was not closed")
	}
}

func TestClientCreateBatchClassifiesResponseReadFailureAsRetryable(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	readErr := errors.New("unexpected EOF")
	body := &failingReadCloser{err: readErr}
	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
		}, nil
	}), nil)
	_, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	)
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || !requestErr.Retryable() {
		t.Fatalf("CreateBatch() error = %v, want retryable RequestError", err)
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("CreateBatch() error = %v, want wrapped read error", err)
	}
	if !body.closed.Load() {
		t.Fatal("response body was not closed after read failure")
	}
}

func TestClientCreateBatchPreservesResponseReadCancellation(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	body := &failingReadCloser{err: context.Canceled}
	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
		}, nil
	}), nil)
	_, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	)
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || requestErr.Retryable() {
		t.Fatalf("CreateBatch() error = %v, want non-retryable RequestError", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateBatch() error = %v, want context.Canceled", err)
	}
}

func TestClientCreateBatchRejectsInvalidResponse(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		return bulkHTTPResponse(http.StatusOK, `{`), nil
	}), nil)
	results, err := client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	)
	if err == nil || results != nil {
		t.Fatalf("CreateBatch() results = %#v, error = %v", results, err)
	}
}

func TestNewClientValidatesEndpoint(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{
		"   ",
		"localhost:9200",
		"ftp://localhost:9200",
		"http://",
		"http://user:password@localhost:9200",
		"http://localhost:9200?debug=true",
		"http://localhost:9200#fragment",
		"http://127.0.0.1:0",
		"http://127.0.0.1:99999",
		"http://127.0.0.1:",
	} {
		endpoint := endpoint
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()

			if _, err := NewClient(endpoint); err == nil {
				t.Fatal("NewClient() error = nil, want an error")
			}
		})
	}
}

func TestNewClientDisablesTransportRetries(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writer.Header().Set("X-Elastic-Product", "Elasticsearch")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(`{"error":{"type":"unavailable_shards_exception"}}`))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	document := testDocuments(t, 1)[0]
	_, err = client.CreateBatch(
		context.Background(),
		"logs-stage3-client-test",
		[]Document{document},
	)
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || !requestErr.Retryable() {
		t.Fatalf("CreateBatch() error = %v, want retryable RequestError", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("official transport calls = %d, want exactly 1", calls.Load())
	}
}

func TestClientCloseDelegatesOnce(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	client := newClient(nil, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("close calls = %d, want 1", calls.Load())
	}
}

func bulkHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

type trackingReadCloser struct {
	io.Reader
	closed atomic.Bool
}

type failingReadCloser struct {
	err    error
	closed atomic.Bool
}

func (body *failingReadCloser) Read([]byte) (int, error) {
	return 0, body.err
}

func (body *failingReadCloser) Close() error {
	body.closed.Store(true)
	return nil
}

func (body *trackingReadCloser) Close() error {
	body.closed.Store(true)
	return nil
}

func TestRequestDeadlineIsRetryable(t *testing.T) {
	t.Parallel()

	document := testDocuments(t, 1)[0]
	client := newClient(performerFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	}), nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.CreateBatch(ctx, "logs-stage3-client-test", []Document{document})
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || !requestErr.Retryable() {
		t.Fatalf("deadline error = %v, want retryable RequestError", err)
	}
}
