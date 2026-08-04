package event

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

const eventIDVersion = "v1"

// EventID 根据 ADR-002 规定的稳定身份字段生成确定性事件 ID。
// 字段采用字节长度前缀编码，避免字段边界歧义；同一事件重复处理时结果保持一致。
func EventID(input Event) (string, error) {
	if strings.TrimSpace(input.PodUID) == "" {
		return "", invalid("kubernetes.pod.uid", "不能为空")
	}
	if strings.TrimSpace(input.ContainerID) == "" {
		return "", invalid("container.id", "不能为空")
	}
	if strings.TrimSpace(input.LogFilePath) == "" {
		return "", invalid("log.file.path", "不能为空")
	}
	if input.LogOffset < 0 {
		return "", invalid("log.offset", "必须大于等于 0")
	}
	if input.Timestamp.IsZero() {
		return "", invalid("@timestamp", "不能为空")
	}
	if input.rawMessage == "" {
		return "", invalid("message", "不能为空")
	}

	identity := []string{
		input.PodUID,
		input.ContainerID,
		input.LogFilePath,
		strconv.FormatInt(input.LogOffset, 10),
		input.Timestamp.UTC().Format(time.RFC3339Nano),
		input.rawMessage,
	}

	var encoded strings.Builder
	encoded.WriteString(eventIDVersion)
	encoded.WriteByte('|')
	for _, value := range identity {
		// Go 字符串长度按字节计算，与 ADR-002 的 UTF-8 字节长度约定一致。
		encoded.WriteString(strconv.Itoa(len(value)))
		encoded.WriteByte(':')
		encoded.WriteString(value)
	}

	digest := sha256.Sum256([]byte(encoded.String()))
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}
