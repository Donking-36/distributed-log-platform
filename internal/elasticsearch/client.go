package elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	elasticclient "github.com/elastic/go-elasticsearch/v9"
)

type requestPerformer interface {
	Perform(*http.Request) (*http.Response, error)
}

// Client 负责一次 Bulk HTTP 往返和可信响应解析。
// 它不执行重试、退避、Kafka 位点提交或 DLQ 写入，这些策略由后续处理器统一管理。
type Client struct {
	performer requestPerformer
	close     func(context.Context) error
}

// RequestError 表示尚未取得可信逐项结果的请求级故障。
// StatusCode 为 0 时，底层错误来自 context、网络或 transport。
type RequestError struct {
	StatusCode int
	retryable  bool
	err        error
}

// Error 返回不包含 Elasticsearch 响应正文的安全错误摘要。
func (err *RequestError) Error() string {
	if err.StatusCode != 0 {
		return fmt.Sprintf("Elasticsearch Bulk 请求返回 HTTP %d", err.StatusCode)
	}
	return fmt.Sprintf("Elasticsearch Bulk 请求失败: %v", err.err)
}

// Unwrap 保留 context、网络或 transport 错误链。
func (err *RequestError) Unwrap() error {
	return err.err
}

// Retryable 报告该请求级故障是否适合由处理器按有界策略重试。
func (err *RequestError) Retryable() bool {
	return err.retryable
}

// NewClient 使用官方 Elasticsearch v9 transport 创建客户端。
// transport 内置重试被显式关闭，避免与 ADR-002 的应用级重试周期叠加。
func NewClient(endpoint string) (*Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, errors.New("Elasticsearch endpoint 不能为空")
	}
	parsedEndpoint, err := url.Parse(endpoint)
	if err != nil ||
		(parsedEndpoint.Scheme != "http" && parsedEndpoint.Scheme != "https") ||
		parsedEndpoint.Hostname() == "" ||
		!validEndpointPort(parsedEndpoint) ||
		parsedEndpoint.User != nil ||
		parsedEndpoint.RawQuery != "" ||
		parsedEndpoint.Fragment != "" {
		return nil, errors.New("Elasticsearch endpoint 必须是不含凭据、查询或片段的绝对 HTTP(S) URL")
	}

	base, err := elasticclient.NewBase(
		elasticclient.WithAddresses(endpoint),
		elasticclient.WithAutoDrainBody(),
		elasticclient.WithTransportOptions(
			elastictransport.WithDisableRetry(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("创建 Elasticsearch v9 客户端: %w", err)
	}

	return newClient(base, base.Close), nil
}

func validEndpointPort(endpoint *url.URL) bool {
	if endpoint == nil || strings.HasSuffix(endpoint.Host, ":") {
		return false
	}
	port := endpoint.Port()
	if port == "" {
		return true
	}
	value, err := strconv.Atoi(port)
	return err == nil && value >= 1 && value <= 65535
}

func newClient(
	performer requestPerformer,
	closeFunction func(context.Context) error,
) *Client {
	return &Client{performer: performer, close: closeFunction}
}

// CreateBatch 发送一次 NDJSON Bulk create 请求，并按输入顺序返回逐项结果。
// 任一响应完整性校验失败时返回 nil 结果，调用方不得据此确认任何源记录。
func (client *Client) CreateBatch(
	ctx context.Context,
	index string,
	documents []Document,
) ([]CreateResult, error) {
	if ctx == nil {
		return nil, errors.New("Elasticsearch Bulk context 不能为空")
	}
	body, err := buildBulkBody(index, documents)
	if err != nil {
		return nil, err
	}
	if len(documents) == 0 {
		return []CreateResult{}, nil
	}
	if client == nil || client.performer == nil {
		return nil, errors.New("Elasticsearch 客户端未初始化")
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"/_bulk",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("创建 Elasticsearch Bulk 请求: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-ndjson")
	request.Header.Set("Accept", "application/json")

	response, err := client.performer.Perform(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		cause := err
		if ctxErr := ctx.Err(); ctxErr != nil {
			cause = ctxErr
		}
		return nil, &RequestError{
			retryable: isRetryableTransportError(cause),
			err:       cause,
		}
	}
	if response == nil || response.Body == nil {
		return nil, errors.New("Elasticsearch transport 返回空响应")
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, &RequestError{
			StatusCode: response.StatusCode,
			retryable:  isRetryableRequestStatus(response.StatusCode),
		}
	}

	results, err := parseBulkResponse(response.Body, documents)
	if err == nil {
		return results, nil
	}
	var readErr *bulkResponseReadError
	if errors.As(err, &readErr) {
		cause := error(readErr)
		if ctxErr := ctx.Err(); ctxErr != nil {
			cause = ctxErr
		}
		return nil, &RequestError{
			retryable: !errors.Is(cause, context.Canceled),
			err:       cause,
		}
	}
	return nil, err
}

// Close 关闭官方 transport 的连接资源；使用测试替身时可以为空操作。
func (client *Client) Close(ctx context.Context) error {
	if client == nil || client.close == nil {
		return nil
	}
	return client.close(ctx)
}

func isRetryableRequestStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooManyRequests ||
		status >= 500
}

func isRetryableTransportError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// 证书、TLS 协议和确定的 DNS 不存在通常需要修正配置，重试不会自行恢复。
	var certificateInvalid x509.CertificateInvalidError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameError x509.HostnameError
	var recordHeaderError tls.RecordHeaderError
	if errors.As(err, &certificateInvalid) ||
		errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostnameError) ||
		errors.As(err, &recordHeaderError) {
		return false
	}

	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		return dnsError.Timeout() || dnsError.Temporary()
	}
	var invalidAddress net.InvalidAddrError
	if errors.As(err, &invalidAddress) {
		return false
	}
	var addressError *net.AddrError
	var parseError *net.ParseError
	var unknownNetwork net.UnknownNetworkError
	if errors.As(err, &addressError) ||
		errors.As(err, &parseError) ||
		errors.As(err, &unknownNetwork) {
		return false
	}
	if isRetryableSocketError(err) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func isRetryableSocketError(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ENETDOWN) ||
		errors.Is(err, syscall.ENETRESET) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTDOWN) ||
		errors.Is(err, syscall.EHOSTUNREACH)
}
