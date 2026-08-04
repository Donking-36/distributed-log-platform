package event

import (
	"encoding/json"
	"strings"
)

// BestEffortTestRunID 尝试从 Filebeat 双层 JSON 中提取测试批次标识。
// 该函数只提供 DLQ 排障元数据；任何解码失败都会返回空字符串，不能替代严格事件校验。
func BestEffortTestRunID(payload []byte) string {
	var envelope filebeatEvent
	if err := decodeObject(payload, &envelope); err != nil {
		return ""
	}

	var rawMessage string
	if err := json.Unmarshal(envelope.Message, &rawMessage); err != nil {
		return ""
	}

	var source producerEvent
	if err := decodeObject([]byte(rawMessage), &source); err != nil {
		return ""
	}

	var testRunID string
	if err := json.Unmarshal(source.TestRunID, &testRunID); err != nil {
		return ""
	}
	if strings.TrimSpace(testRunID) == "" {
		return ""
	}
	return testRunID
}
