package main

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	producerServiceNameEnv = "PRODUCER_SERVICE_NAME"
	producerTestRunIDEnv   = "PRODUCER_TEST_RUN_ID"
	producerCountEnv       = "PRODUCER_COUNT"

	// defaultProducerCount 与 UC-001 中每个服务的基准事件数保持一致。
	defaultProducerCount = 20
)

// config 定义日志生产器当前行为所需的命令级配置。
type config struct {
	ServiceName string
	TestRunID   string
	Count       int
}

// loadConfig 从注入的环境查询函数读取配置，并在返回前完成规范化和校验。
func loadConfig(
	lookupEnv func(string) (string, bool),
) (config, error) {
	if lookupEnv == nil {
		return config{}, fmt.Errorf(
			"environment lookup must not be nil",
		)
	}

	cfg := config{
		Count: defaultProducerCount,
	}

	if value, exists := lookupEnv(producerServiceNameEnv); exists {
		cfg.ServiceName = strings.TrimSpace(value)
	}
	if value, exists := lookupEnv(producerTestRunIDEnv); exists {
		cfg.TestRunID = strings.TrimSpace(value)
	}
	if value, exists := lookupEnv(producerCountEnv); exists {
		count, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return config{}, fmt.Errorf(
				"%s must be a valid integer: %w",
				producerCountEnv,
				err,
			)
		}
		cfg.Count = count
	}

	if err := cfg.validate(); err != nil {
		return config{}, fmt.Errorf(
			"validate producer configuration: %w",
			err,
		)
	}

	return cfg, nil
}

// validate 检查会影响日志路由和固定批次验收的配置约束。
func (c config) validate() error {
	if strings.TrimSpace(c.ServiceName) == "" {
		return fmt.Errorf(
			"%s must not be empty",
			producerServiceNameEnv,
		)
	}
	if strings.TrimSpace(c.TestRunID) == "" {
		return fmt.Errorf(
			"%s must not be empty",
			producerTestRunIDEnv,
		)
	}
	if c.Count <= 0 {
		return fmt.Errorf(
			"%s must be greater than zero",
			producerCountEnv,
		)
	}

	return nil
}
