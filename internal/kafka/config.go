package kafka

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

const consumerClientID = "log-processor"

var topicNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Config 定义 log-processor 连接 Kafka 消费者组所需的最小启动配置。
// 所有字段都必须由部署层显式提供，避免使用会掩盖配置错误的隐式默认值。
type Config struct {
	Brokers []string
	GroupID string
	Topics  []string
}

func (config Config) validate() error {
	if err := validateBrokers(config.Brokers); err != nil {
		return err
	}

	if strings.TrimSpace(config.GroupID) == "" {
		return errors.New("Kafka consumer group 不能为空")
	}
	if config.GroupID != strings.TrimSpace(config.GroupID) {
		return errors.New("Kafka consumer group 不能包含首尾空白")
	}

	if len(config.Topics) == 0 {
		return errors.New("Kafka topics 不能为空")
	}
	seenTopics := make(map[string]struct{}, len(config.Topics))
	for index, topic := range config.Topics {
		if err := validateTopic(topic); err != nil {
			return fmt.Errorf("Kafka topic[%d]: %w", index, err)
		}
		if _, exists := seenTopics[topic]; exists {
			return fmt.Errorf("Kafka topic 不能重复: %q", topic)
		}
		seenTopics[topic] = struct{}{}
	}
	return nil
}

func validateBrokers(brokers []string) error {
	if len(brokers) == 0 {
		return errors.New("Kafka brokers 不能为空")
	}
	seenBrokers := make(map[string]struct{}, len(brokers))
	for index, broker := range brokers {
		if err := validateBroker(broker); err != nil {
			return fmt.Errorf("Kafka broker[%d]: %w", index, err)
		}
		if _, exists := seenBrokers[broker]; exists {
			return fmt.Errorf("Kafka broker 不能重复: %q", broker)
		}
		seenBrokers[broker] = struct{}{}
	}
	return nil
}

func validateBroker(broker string) error {
	if strings.TrimSpace(broker) == "" {
		return errors.New("地址不能为空")
	}
	if broker != strings.TrimSpace(broker) {
		return errors.New("地址不能包含首尾空白")
	}
	host, port, err := net.SplitHostPort(broker)
	if err != nil || strings.TrimSpace(host) == "" {
		return errors.New("地址必须使用 host:port 格式")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("端口必须是 1 到 65535 的整数")
	}
	return nil
}

func validateTopic(topic string) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("名称不能为空")
	}
	if topic != strings.TrimSpace(topic) {
		return errors.New("名称不能包含首尾空白")
	}
	if len(topic) > 249 {
		return errors.New("名称不能超过 249 字节")
	}
	if topic == "." || topic == ".." || !topicNamePattern.MatchString(topic) {
		return errors.New("名称只能包含字母、数字、点、下划线和连字符")
	}
	return nil
}

func (config Config) clientOptions() []kgo.Opt {
	brokers := append([]string(nil), config.Brokers...)
	topics := append([]string(nil), config.Topics...)
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(consumerClientID),
		kgo.ConsumerGroup(config.GroupID),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
}
