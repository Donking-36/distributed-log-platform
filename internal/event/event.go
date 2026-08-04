// Package event 定义日志处理器的规范事件及其永久校验错误。
package event

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalid 表示输入记录永久不符合事件契约，后续处理器可据此选择死信路径，
// 而不是把它误判为需要无限重试的基础设施故障。
var ErrInvalid = errors.New("无效日志事件")

// Event 是从 Filebeat 双层 JSON 中校验并规范化得到的业务事件。
// Kafka record offset 不属于该结构；LogOffset 始终表示节点源日志文件位置。
type Event struct {
	Timestamp   time.Time
	Sequence    int64
	Level       string
	Message     string
	ServiceName string
	TestRunID   string
	Namespace   string
	PodName     string
	PodUID      string
	ContainerID string
	LogFilePath string
	LogOffset   int64

	// rawMessage 保留 Filebeat message 解码后的原始业务 JSON，仅用于生成稳定事件 ID。
	// 该字段不对外导出，也不会进入后续 Elasticsearch 文档。
	rawMessage string
}

// ValidationError 指明永久无效事件对应的契约字段。
// 测试和调用方应判断 Field 或 ErrInvalid，不依赖完整错误文本。
type ValidationError struct {
	Field  string
	Reason string
}

// Error 返回不包含原始日志正文的安全错误摘要。
func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Reason)
}

// Unwrap 允许调用方通过 errors.Is 将具体字段错误统一归类为 ErrInvalid。
func (e *ValidationError) Unwrap() error {
	return ErrInvalid
}

func invalid(field, reason string) error {
	return &ValidationError{Field: field, Reason: reason}
}
