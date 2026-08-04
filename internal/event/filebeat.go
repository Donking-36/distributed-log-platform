package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type filebeatEvent struct {
	Message    json.RawMessage `json:"message"`
	Kubernetes struct {
		Namespace json.RawMessage `json:"namespace"`
		Labels    struct {
			Service json.RawMessage `json:"service"`
		} `json:"labels"`
		Pod struct {
			Name json.RawMessage `json:"name"`
			UID  json.RawMessage `json:"uid"`
		} `json:"pod"`
	} `json:"kubernetes"`
	Container struct {
		ID json.RawMessage `json:"id"`
	} `json:"container"`
	Log struct {
		Offset json.RawMessage `json:"offset"`
		File   struct {
			Path json.RawMessage `json:"path"`
		} `json:"file"`
	} `json:"log"`
}

type producerEvent struct {
	Timestamp   json.RawMessage `json:"@timestamp"`
	Sequence    json.RawMessage `json:"event.sequence"`
	Level       json.RawMessage `json:"log.level"`
	Message     json.RawMessage `json:"message"`
	ServiceName json.RawMessage `json:"service.name"`
	TestRunID   json.RawMessage `json:"test_run_id"`
}

// ParseFilebeat 解码 Filebeat 外层事件及 message 内的 log-producer JSON，
// 校验稳定身份字段并返回与传输和存储客户端无关的规范事件。
func ParseFilebeat(payload []byte) (Event, error) {
	var envelope filebeatEvent
	if err := decodeObject(payload, &envelope); err != nil {
		return Event{}, invalid("payload", "必须是合法 JSON 对象")
	}

	rawMessage, err := requiredString(envelope.Message, "message")
	if err != nil {
		return Event{}, err
	}
	var source producerEvent
	if err := decodeObject([]byte(rawMessage), &source); err != nil {
		return Event{}, invalid("message", "必须包含合法的业务 JSON 对象")
	}

	namespace, err := requiredString(envelope.Kubernetes.Namespace, "kubernetes.namespace")
	if err != nil {
		return Event{}, err
	}
	podName, err := requiredString(envelope.Kubernetes.Pod.Name, "kubernetes.pod.name")
	if err != nil {
		return Event{}, err
	}
	podUID, err := requiredString(envelope.Kubernetes.Pod.UID, "kubernetes.pod.uid")
	if err != nil {
		return Event{}, err
	}
	authoritativeService, err := requiredString(
		envelope.Kubernetes.Labels.Service,
		"kubernetes.labels.service",
	)
	if err != nil {
		return Event{}, err
	}
	containerID, err := requiredString(envelope.Container.ID, "container.id")
	if err != nil {
		return Event{}, err
	}
	logFilePath, err := requiredString(envelope.Log.File.Path, "log.file.path")
	if err != nil {
		return Event{}, err
	}
	logOffset, err := requiredInteger(envelope.Log.Offset, "log.offset", 0)
	if err != nil {
		return Event{}, err
	}

	timestampText, err := requiredString(source.Timestamp, "@timestamp")
	if err != nil {
		return Event{}, err
	}
	timestamp, parseErr := time.Parse(time.RFC3339Nano, timestampText)
	if parseErr != nil {
		return Event{}, invalid("@timestamp", "必须是 RFC3339 时间")
	}
	sequence, err := requiredInteger(source.Sequence, "event.sequence", 1)
	if err != nil {
		return Event{}, err
	}
	level, err := requiredString(source.Level, "log.level")
	if err != nil {
		return Event{}, err
	}
	body, err := requiredString(source.Message, "message")
	if err != nil {
		return Event{}, err
	}
	sourceService, err := requiredString(source.ServiceName, "service.name")
	if err != nil {
		return Event{}, err
	}
	if sourceService != authoritativeService {
		return Event{}, invalid(
			"service.name",
			fmt.Sprintf("必须与权威标签 %q 一致", authoritativeService),
		)
	}
	testRunID, err := optionalString(source.TestRunID, "test_run_id")
	if err != nil {
		return Event{}, err
	}

	return Event{
		Timestamp:   timestamp.UTC(),
		Sequence:    sequence,
		Level:       strings.ToUpper(strings.TrimSpace(level)),
		Message:     body,
		ServiceName: authoritativeService,
		TestRunID:   testRunID,
		Namespace:   namespace,
		PodName:     podName,
		PodUID:      podUID,
		ContainerID: containerID,
		LogFilePath: logFilePath,
		LogOffset:   logOffset,
	}, nil
}

func decodeObject(data []byte, destination any) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return ErrInvalid
	}
	return json.Unmarshal(trimmed, destination)
}

func requiredString(raw json.RawMessage, field string) (string, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", invalid(field, "不能为空")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", invalid(field, "必须是字符串")
	}
	if strings.TrimSpace(value) == "" {
		return "", invalid(field, "不能为空")
	}
	return value, nil
}

func optionalString(raw json.RawMessage, field string) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", invalid(field, "存在时必须是字符串")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", invalid(field, "必须是字符串")
	}
	return value, nil
}

func requiredInteger(raw json.RawMessage, field string, minimum int64) (int64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, invalid(field, "不能为空")
	}
	value, err := strconv.ParseInt(string(trimmed), 10, 64)
	if err != nil {
		return 0, invalid(field, "必须是十进制整数")
	}
	if value < minimum {
		return 0, invalid(field, fmt.Sprintf("必须大于等于 %d", minimum))
	}
	return value, nil
}
