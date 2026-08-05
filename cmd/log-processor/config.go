package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Donking-36/distributed-log-platform/internal/kafka"
)

const (
	processorKafkaBrokersEnv             = "PROCESSOR_KAFKA_BROKERS"
	processorKafkaGroupIDEnv             = "PROCESSOR_KAFKA_GROUP_ID"
	processorKafkaTopicsEnv              = "PROCESSOR_KAFKA_TOPICS"
	processorElasticsearchEndpointEnv    = "PROCESSOR_ELASTICSEARCH_ENDPOINT"
	processorElasticsearchIndexEnv       = "PROCESSOR_ELASTICSEARCH_INDEX"
	processorWriteTimeoutEnv             = "PROCESSOR_WRITE_TIMEOUT"
	processorCommitTimeoutEnv            = "PROCESSOR_COMMIT_TIMEOUT"
	processorDeadLetterPublishTimeoutEnv = "PROCESSOR_DLQ_PUBLISH_TIMEOUT"
	processorHealthAddressEnv            = "PROCESSOR_HEALTH_ADDRESS"
	processorBatchSizeEnv                = "PROCESSOR_BATCH_SIZE"

	defaultWriteTimeout             = 10 * time.Second
	defaultCommitTimeout            = 10 * time.Second
	defaultDeadLetterPublishTimeout = 10 * time.Second
	defaultHealthAddress            = ":8080"
	defaultBatchSize                = 500
	maximumBatchSize                = 5000
)

// config 保存 log-processor 启动所需的完整命令级配置。
type config struct {
	KafkaBrokers             []string
	KafkaGroupID             string
	KafkaTopics              []string
	ElasticsearchEndpoint    string
	ElasticsearchIndex       string
	WriteTimeout             time.Duration
	CommitTimeout            time.Duration
	DeadLetterPublishTimeout time.Duration
	HealthAddress            string
	BatchSize                int
}

// loadConfig 读取、规范化并一次性校验环境配置，避免处理循环接收半有效状态。
func loadConfig(lookupEnv func(string) (string, bool)) (config, error) {
	if lookupEnv == nil {
		return config{}, errors.New("环境变量查询函数不能为空")
	}

	brokers, err := requiredList(lookupEnv, processorKafkaBrokersEnv)
	if err != nil {
		return config{}, err
	}
	topics, err := requiredList(lookupEnv, processorKafkaTopicsEnv)
	if err != nil {
		return config{}, err
	}
	for _, topic := range topics {
		if topic == kafka.DeadLetterTopic {
			return config{}, fmt.Errorf(
				"%s 不能订阅 %s，避免死信循环",
				processorKafkaTopicsEnv,
				kafka.DeadLetterTopic,
			)
		}
	}

	groupID, err := requiredValue(lookupEnv, processorKafkaGroupIDEnv)
	if err != nil {
		return config{}, err
	}
	endpoint, err := requiredValue(lookupEnv, processorElasticsearchEndpointEnv)
	if err != nil {
		return config{}, err
	}
	index, err := requiredValue(lookupEnv, processorElasticsearchIndexEnv)
	if err != nil {
		return config{}, err
	}

	writeTimeout, err := optionalPositiveDuration(
		lookupEnv,
		processorWriteTimeoutEnv,
		defaultWriteTimeout,
	)
	if err != nil {
		return config{}, err
	}
	commitTimeout, err := optionalPositiveDuration(
		lookupEnv,
		processorCommitTimeoutEnv,
		defaultCommitTimeout,
	)
	if err != nil {
		return config{}, err
	}
	deadLetterPublishTimeout, err := optionalPositiveDuration(
		lookupEnv,
		processorDeadLetterPublishTimeoutEnv,
		defaultDeadLetterPublishTimeout,
	)
	if err != nil {
		return config{}, err
	}
	healthAddress, err := optionalListenAddress(
		lookupEnv,
		processorHealthAddressEnv,
		defaultHealthAddress,
	)
	if err != nil {
		return config{}, err
	}
	batchSize, err := optionalBoundedPositiveInt(
		lookupEnv,
		processorBatchSizeEnv,
		defaultBatchSize,
		maximumBatchSize,
	)
	if err != nil {
		return config{}, err
	}

	return config{
		KafkaBrokers:             brokers,
		KafkaGroupID:             groupID,
		KafkaTopics:              topics,
		ElasticsearchEndpoint:    endpoint,
		ElasticsearchIndex:       index,
		WriteTimeout:             writeTimeout,
		CommitTimeout:            commitTimeout,
		DeadLetterPublishTimeout: deadLetterPublishTimeout,
		HealthAddress:            healthAddress,
		BatchSize:                batchSize,
	}, nil
}

func requiredValue(
	lookupEnv func(string) (string, bool),
	name string,
) (string, error) {
	value, exists := lookupEnv(name)
	value = strings.TrimSpace(value)
	if !exists || value == "" {
		return "", fmt.Errorf("%s 不能为空", name)
	}
	return value, nil
}

func requiredList(
	lookupEnv func(string) (string, bool),
	name string,
) ([]string, error) {
	value, err := requiredValue(lookupEnv, name)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for index, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("%s 第 %d 项是空成员", name, index+1)
		}
		if _, exists := seen[part]; exists {
			return nil, fmt.Errorf("%s 包含重复成员 %q", name, part)
		}
		seen[part] = struct{}{}
		result = append(result, part)
	}
	return result, nil
}

func optionalPositiveDuration(
	lookupEnv func(string) (string, bool),
	name string,
	defaultValue time.Duration,
) (time.Duration, error) {
	value, exists := lookupEnv(name)
	if !exists {
		return defaultValue, nil
	}
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s 必须是有效的 Go duration: %w", name, err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("%s 必须大于 0", name)
	}
	return duration, nil
}

func optionalBoundedPositiveInt(
	lookupEnv func(string) (string, bool),
	name string,
	defaultValue int,
	maximum int,
) (int, error) {
	value, exists := lookupEnv(name)
	if !exists {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s 必须是整数: %w", name, err)
	}
	if parsed < 1 || parsed > maximum {
		return 0, fmt.Errorf("%s 必须在 1 到 %d 之间", name, maximum)
	}
	return parsed, nil
}

// optionalListenAddress 校验显式数值端口，避免把拼写错误拖到依赖组装之后。
func optionalListenAddress(
	lookupEnv func(string) (string, bool),
	name string,
	defaultValue string,
) (string, error) {
	value, exists := lookupEnv(name)
	if !exists {
		value = defaultValue
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s 不能为空", name)
	}

	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("%s 必须是 host:port 格式: %w", name, err)
	}
	if host != strings.TrimSpace(host) || portText != strings.TrimSpace(portText) {
		return "", fmt.Errorf("%s 不能包含多余空白", name)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("%s 端口必须在 1 到 65535 之间", name)
	}
	return value, nil
}
