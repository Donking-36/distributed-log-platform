package elasticsearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

const maxBulkResponseBytes = 8 << 20

var writableIndexPattern = regexp.MustCompile(`^logs-stage3-[a-z0-9][a-z0-9._-]*$`)

// ResultKind 表示单个 create 动作的持久化结果类别。
// 调用方必须逐项处理，不能根据 Bulk 请求级 HTTP 200 推断整批成功。
type ResultKind string

const (
	// ResultCreated 表示文档已由本次请求创建，可以确认对应源记录。
	ResultCreated ResultKind = "created"
	// ResultDuplicate 表示稳定 _id 已存在，可以按 ADR-002 视为幂等成功。
	ResultDuplicate ResultKind = "duplicate"
	// ResultRetryableFailure 表示该项遇到临时故障，不能确认源记录。
	ResultRetryableFailure ResultKind = "retryable_failure"
	// ResultSystemFailure 表示权限、索引或未知协议故障，必须停止推进并排查。
	ResultSystemFailure ResultKind = "system_failure"
)

type bulkResponseReadError struct {
	err error
}

func (err *bulkResponseReadError) Error() string {
	return fmt.Sprintf("读取 Elasticsearch Bulk 响应: %v", err.err)
}

func (err *bulkResponseReadError) Unwrap() error {
	return err.err
}

// CreateResult 保存一个 create 动作的顺序相关结果。
// ErrorType 只保留 Elasticsearch 错误类型，不传播可能含日志正文的 reason。
type CreateResult struct {
	ID         string
	Kind       ResultKind
	StatusCode int
	ErrorType  string
}

type bulkResponse struct {
	Errors *bool                     `json:"errors"`
	Items  []map[string]bulkItemData `json:"items"`
}

type bulkItemData struct {
	ID     string         `json:"_id"`
	Status int            `json:"status"`
	Error  *bulkItemError `json:"error"`
}

type bulkItemError struct {
	Type string `json:"type"`
}

func buildBulkBody(index string, documents []Document) ([]byte, error) {
	if !writableIndexPattern.MatchString(index) {
		return nil, fmt.Errorf("Elasticsearch 索引名不符合 logs-stage3-* 约定: %q", index)
	}
	if len(documents) == 0 {
		return nil, nil
	}

	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for position, document := range documents {
		if strings.TrimSpace(document.id) == "" || !json.Valid(document.source) {
			return nil, fmt.Errorf("第 %d 个 Elasticsearch 文档无效", position)
		}

		action := map[string]any{
			"create": map[string]string{
				"_index": index,
				"_id":    document.id,
			},
		}
		if err := encoder.Encode(action); err != nil {
			return nil, fmt.Errorf("编码第 %d 个 Bulk create 动作: %w", position, err)
		}
		body.Write(document.source)
		body.WriteByte('\n')
	}

	return body.Bytes(), nil
}

func parseBulkResponse(reader io.Reader, documents []Document) ([]CreateResult, error) {
	limited, err := io.ReadAll(io.LimitReader(reader, maxBulkResponseBytes+1))
	if err != nil {
		return nil, &bulkResponseReadError{err: err}
	}
	if len(limited) > maxBulkResponseBytes {
		return nil, fmt.Errorf("Elasticsearch Bulk 响应超过 %d 字节上限", maxBulkResponseBytes)
	}

	var response bulkResponse
	if err := json.Unmarshal(limited, &response); err != nil {
		return nil, fmt.Errorf("解析 Elasticsearch Bulk 响应: %w", err)
	}
	if response.Errors == nil {
		return nil, errors.New("Elasticsearch Bulk 响应缺少 errors 字段")
	}
	if len(response.Items) != len(documents) {
		return nil, fmt.Errorf(
			"Elasticsearch Bulk 结果数量不匹配: 实际 %d，期望 %d",
			len(response.Items),
			len(documents),
		)
	}

	results := make([]CreateResult, len(documents))
	hasFailures := false
	for position, actions := range response.Items {
		if len(actions) != 1 {
			return nil, fmt.Errorf("第 %d 个 Bulk 结果动作数量不是 1", position)
		}
		item, ok := actions["create"]
		if !ok {
			return nil, fmt.Errorf("第 %d 个 Bulk 结果不是 create 动作", position)
		}
		if item.ID != documents[position].id {
			return nil, fmt.Errorf("第 %d 个 Bulk 结果 ID 与请求不匹配", position)
		}

		failed := item.Status < 200 || item.Status >= 300
		if failed {
			hasFailures = true
			if item.Error == nil || strings.TrimSpace(item.Error.Type) == "" {
				return nil, fmt.Errorf("第 %d 个失败的 Bulk 结果缺少错误类型", position)
			}
		} else if item.Error != nil {
			return nil, fmt.Errorf("第 %d 个成功的 Bulk 结果意外包含错误", position)
		}

		errorType := ""
		if item.Error != nil {
			errorType = item.Error.Type
		}
		results[position] = CreateResult{
			ID:         item.ID,
			Kind:       classifyBulkItem(item.Status, errorType),
			StatusCode: item.Status,
			ErrorType:  errorType,
		}
	}

	if *response.Errors != hasFailures {
		return nil, errors.New("Elasticsearch Bulk errors 汇总与逐项状态不一致")
	}
	return results, nil
}

func classifyBulkItem(status int, errorType string) ResultKind {
	switch {
	case status == 201:
		return ResultCreated
	case status == 409 && errorType == "version_conflict_engine_exception":
		return ResultDuplicate
	case status == 429 || status >= 500:
		return ResultRetryableFailure
	default:
		// 固定结构文档发生 4xx 通常表示模板漂移、权限或代码契约故障。
		// 在没有可证明的单记录错误原因前统一停推，避免误送 DLQ 后推进位点。
		return ResultSystemFailure
	}
}
