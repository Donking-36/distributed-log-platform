// Package elasticsearch 实现规范日志事件到 Elasticsearch Bulk create 的最小写入边界。
package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/event"
)

// Document 是已经完成事件标识计算和存储字段编码的不可变写入文档。
// 字段保持不导出，防止调用方在事件 ID 生成后改变正文，造成 _id 与 event_id 不一致。
type Document struct {
	id     string
	source []byte
}

type storedDocument struct {
	Timestamp time.Time `json:"@timestamp"`
	EventID   string    `json:"event_id"`
	Event     struct {
		Sequence int64 `json:"sequence"`
	} `json:"event"`
	Message string `json:"message"`
	Log     struct {
		Level string `json:"level"`
		File  struct {
			Path string `json:"path"`
		} `json:"file"`
		Offset int64 `json:"offset"`
	} `json:"log"`
	Service struct {
		Name string `json:"name"`
	} `json:"service"`
	TestRunID  string `json:"test_run_id"`
	Kubernetes struct {
		Namespace string `json:"namespace"`
		Pod       struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		} `json:"pod"`
	} `json:"kubernetes"`
	Container struct {
		ID string `json:"id"`
	} `json:"container"`
	IngestedAt time.Time `json:"ingested_at"`
}

// NewDocument 将已规范化事件转换为严格匹配 logs-stage3-v1 模板的 14 字段文档。
// ingestedAt 由上层每批读取一次后传入，既保留真实写入时间，也使转换可确定性测试。
func NewDocument(input event.Event, ingestedAt time.Time) (Document, error) {
	if ingestedAt.IsZero() {
		return Document{}, errors.New("ingested_at 不能为空")
	}

	id, err := event.EventID(input)
	if err != nil {
		return Document{}, fmt.Errorf("生成 Elasticsearch 文档事件 ID: %w", err)
	}

	var source storedDocument
	source.Timestamp = input.Timestamp.UTC()
	source.EventID = id
	source.Event.Sequence = input.Sequence
	source.Message = input.Message
	source.Log.Level = input.Level
	source.Log.File.Path = input.LogFilePath
	source.Log.Offset = input.LogOffset
	source.Service.Name = input.ServiceName
	source.TestRunID = input.TestRunID
	source.Kubernetes.Namespace = input.Namespace
	source.Kubernetes.Pod.Name = input.PodName
	source.Kubernetes.Pod.UID = input.PodUID
	source.Container.ID = input.ContainerID
	source.IngestedAt = ingestedAt.UTC()

	encoded, err := json.Marshal(source)
	if err != nil {
		return Document{}, fmt.Errorf("编码 Elasticsearch 文档: %w", err)
	}

	return Document{id: id, source: encoded}, nil
}
