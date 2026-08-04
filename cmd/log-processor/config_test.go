package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigParsesRequiredValuesAndDefaults(t *testing.T) {
	t.Parallel()

	values := validProcessorEnvironment()
	values[processorKafkaBrokersEnv] = " kafka:9092, kafka-1:9092 "
	values[processorKafkaTopicsEnv] = " logs.api-service,logs.worker-service "

	config, err := loadConfig(processorLookupEnv(values))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if strings.Join(config.KafkaBrokers, ",") != "kafka:9092,kafka-1:9092" ||
		strings.Join(config.KafkaTopics, ",") != "logs.api-service,logs.worker-service" {
		t.Fatalf("brokers/topics = %v/%v", config.KafkaBrokers, config.KafkaTopics)
	}
	if config.WriteTimeout != defaultWriteTimeout ||
		config.CommitTimeout != defaultCommitTimeout ||
		config.DeadLetterPublishTimeout != defaultDeadLetterPublishTimeout {
		t.Fatalf("timeouts = %v/%v/%v", config.WriteTimeout, config.CommitTimeout,
			config.DeadLetterPublishTimeout)
	}
}

func TestLoadConfigParsesExplicitTimeouts(t *testing.T) {
	t.Parallel()

	values := validProcessorEnvironment()
	values[processorWriteTimeoutEnv] = "3s"
	values[processorCommitTimeoutEnv] = "4s"
	values[processorDeadLetterPublishTimeoutEnv] = "5s"
	config, err := loadConfig(processorLookupEnv(values))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if config.WriteTimeout != 3*time.Second || config.CommitTimeout != 4*time.Second ||
		config.DeadLetterPublishTimeout != 5*time.Second {
		t.Fatalf("timeouts = %v/%v/%v", config.WriteTimeout, config.CommitTimeout,
			config.DeadLetterPublishTimeout)
	}
}

func TestLoadConfigRejectsMissingRequiredValues(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		processorKafkaBrokersEnv,
		processorKafkaGroupIDEnv,
		processorKafkaTopicsEnv,
		processorElasticsearchEndpointEnv,
		processorElasticsearchIndexEnv,
	} {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			values := validProcessorEnvironment()
			delete(values, name)
			if _, err := loadConfig(processorLookupEnv(values)); err == nil ||
				!strings.Contains(err.Error(), name) {
				t.Fatalf("loadConfig() error = %v，期望包含 %s", err, name)
			}
		})
	}
}

func TestLoadConfigRejectsUnsafeListsAndTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		env    string
		value  string
		needle string
	}{
		{name: "空 broker 成员", env: processorKafkaBrokersEnv, value: "kafka:9092,,kafka-1:9092", needle: "空成员"},
		{name: "重复 broker", env: processorKafkaBrokersEnv, value: "kafka:9092,kafka:9092", needle: "重复"},
		{name: "重复 topic", env: processorKafkaTopicsEnv, value: "logs.api-service,logs.api-service", needle: "重复"},
		{name: "订阅 DLQ", env: processorKafkaTopicsEnv, value: "logs.api-service,logs.dlq", needle: "logs.dlq"},
		{name: "非法超时", env: processorWriteTimeoutEnv, value: "soon", needle: processorWriteTimeoutEnv},
		{name: "非正超时", env: processorCommitTimeoutEnv, value: "0s", needle: "大于 0"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			values := validProcessorEnvironment()
			values[test.env] = test.value
			if _, err := loadConfig(processorLookupEnv(values)); err == nil ||
				!strings.Contains(err.Error(), test.needle) {
				t.Fatalf("loadConfig() error = %v，期望包含 %q", err, test.needle)
			}
		})
	}
}

func TestLoadConfigRejectsNilLookup(t *testing.T) {
	t.Parallel()
	if _, err := loadConfig(nil); err == nil {
		t.Fatal("loadConfig(nil) error = nil")
	}
}

func validProcessorEnvironment() map[string]string {
	return map[string]string{
		processorKafkaBrokersEnv:          "kafka:9092",
		processorKafkaGroupIDEnv:          "log-processor-v1",
		processorKafkaTopicsEnv:           "logs.api-service,logs.worker-service",
		processorElasticsearchEndpointEnv: "http://elasticsearch:9200",
		processorElasticsearchIndexEnv:    "logs-stage3-v1",
	}
}

func processorLookupEnv(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
