//go:build integration

package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

const integrationIndexPrefix = "logs-stage3-go-client-smoke-"

func TestClientCreateBatchAgainstElasticsearch(t *testing.T) {
	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv("ELASTICSEARCH_URL")), "/")
	if endpoint == "" {
		t.Fatal("ELASTICSEARCH_URL 不能为空")
	}
	index := strings.TrimSpace(os.Getenv("ELASTICSEARCH_TEST_INDEX"))
	if !writableIndexPattern.MatchString(index) ||
		!strings.HasPrefix(index, integrationIndexPrefix) ||
		len(index) == len(integrationIndexPrefix) {
		t.Fatalf("ELASTICSEARCH_TEST_INDEX 不符合临时索引约定: %q", index)
	}

	client, err := NewClient(endpoint)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	status, err := integrationRequest(ctx, http.MethodHead, endpoint, index)
	if err != nil {
		t.Fatalf("确认临时索引不存在: %v", err)
	}
	if status != http.StatusNotFound {
		t.Fatalf("拒绝复用已存在的临时索引 %q: HTTP %d", index, status)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		status, requestErr := integrationRequest(cleanupCtx, http.MethodDelete, endpoint, index)
		if requestErr != nil {
			t.Errorf("清理临时索引 %q: %v", index, requestErr)
			return
		}
		if status != http.StatusOK && status != http.StatusNotFound {
			t.Errorf("清理临时索引 %q 返回 HTTP %d", index, status)
		}
	})

	document, err := NewDocument(newTestEvent(t), time.Now().UTC())
	if err != nil {
		t.Fatalf("NewDocument() error = %v", err)
	}
	first, err := client.CreateBatch(ctx, index, []Document{document})
	if err != nil {
		t.Fatalf("first CreateBatch() error = %v", err)
	}
	if len(first) != 1 || first[0].Kind != ResultCreated || first[0].StatusCode != 201 {
		t.Fatalf("first CreateBatch() results = %#v", first)
	}

	second, err := client.CreateBatch(ctx, index, []Document{document})
	if err != nil {
		t.Fatalf("second CreateBatch() error = %v", err)
	}
	if len(second) != 1 || second[0].Kind != ResultDuplicate || second[0].StatusCode != 409 {
		t.Fatalf("second CreateBatch() results = %#v", second)
	}

	status, err = integrationRequest(ctx, http.MethodPost, endpoint, index+"/_refresh")
	if err != nil || status != http.StatusOK {
		t.Fatalf("refresh index: status = %d, error = %v", status, err)
	}
	count, err := integrationDocumentCount(ctx, endpoint, index)
	if err != nil {
		t.Fatalf("count documents: %v", err)
	}
	if count != 1 {
		t.Fatalf("document count = %d, want 1", count)
	}

	t.Logf(
		"Elasticsearch 真实冒烟通过: index=%s first=%d/%s second=%d/%s count=%d",
		index,
		first[0].StatusCode,
		first[0].Kind,
		second[0].StatusCode,
		second[0].Kind,
		count,
	)
}

func integrationDocumentCount(
	ctx context.Context,
	endpoint string,
	index string,
) (int64, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		endpoint+"/"+url.PathEscape(index)+"/_count",
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
		return 0, fmt.Errorf("count returned HTTP %d", response.StatusCode)
	}
	var result struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return 0, err
	}
	return result.Count, nil
}

func integrationRequest(
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
