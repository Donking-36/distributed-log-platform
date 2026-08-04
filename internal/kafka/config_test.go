package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	valid := Config{
		Brokers: []string{"kafka.stage3-logs.svc.cluster.local:9092"},
		GroupID: "log-processor",
		Topics:  []string{"logs.api-service", "logs.worker-service"},
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "缺少 broker", mutate: func(config *Config) { config.Brokers = nil }},
		{name: "broker 为空", mutate: func(config *Config) { config.Brokers = []string{" "} }},
		{name: "broker 缺少端口", mutate: func(config *Config) { config.Brokers = []string{"kafka"} }},
		{name: "broker 重复", mutate: func(config *Config) {
			config.Brokers = []string{"kafka:9092", "kafka:9092"}
		}},
		{name: "缺少消费者组", mutate: func(config *Config) { config.GroupID = "" }},
		{name: "消费者组包含首尾空白", mutate: func(config *Config) { config.GroupID = " group " }},
		{name: "缺少主题", mutate: func(config *Config) { config.Topics = nil }},
		{name: "主题为空", mutate: func(config *Config) { config.Topics = []string{""} }},
		{name: "主题名称非法", mutate: func(config *Config) { config.Topics = []string{"logs/api"} }},
		{name: "主题重复", mutate: func(config *Config) {
			config.Topics = []string{"logs.api-service", "logs.api-service"}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			config := valid
			config.Brokers = append([]string(nil), valid.Brokers...)
			config.Topics = append([]string(nil), valid.Topics...)
			test.mutate(&config)

			if err := config.validate(); err == nil {
				t.Fatal("期望配置校验失败，实际成功")
			}
		})
	}

	if err := valid.validate(); err != nil {
		t.Fatalf("合法配置校验失败: %v", err)
	}
}

func TestNewConsumerFixesSafeGroupOptions(t *testing.T) {
	t.Parallel()

	consumer, err := NewConsumer(Config{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "log-processor-options-test",
		Topics:  []string{"logs.api-service"},
	})
	if err != nil {
		t.Fatalf("创建消费者: %v", err)
	}
	t.Cleanup(consumer.Close)

	franz, ok := consumer.client.(*franzConsumerClient)
	if !ok {
		t.Fatalf("底层客户端类型 = %T，期望 *franzConsumerClient", consumer.client)
	}
	if enabled, ok := franz.client.OptValue(kgo.DisableAutoCommit).(bool); !ok || !enabled {
		t.Fatalf("DisableAutoCommit = %#v，期望 true", franz.client.OptValue(kgo.DisableAutoCommit))
	}
	if enabled, ok := franz.client.OptValue(kgo.BlockRebalanceOnPoll).(bool); !ok || !enabled {
		t.Fatalf("BlockRebalanceOnPoll = %#v，期望 true", franz.client.OptValue(kgo.BlockRebalanceOnPoll))
	}
}
