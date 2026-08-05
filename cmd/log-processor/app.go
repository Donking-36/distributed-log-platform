package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Donking-36/distributed-log-platform/internal/elasticsearch"
	"github.com/Donking-36/distributed-log-platform/internal/kafka"
	"github.com/Donking-36/distributed-log-platform/internal/pipeline"
)

type application struct {
	run   func(context.Context) error
	close func(context.Context) error
}

type applicationBuilder func(config) (*application, error)

// buildApplication 只负责组装真实外部客户端和处理边界，不承载业务判断。
func buildApplication(cfg config, logger *slog.Logger) (
	applicationResult *application,
	resultErr error,
) {
	if logger == nil {
		return nil, errors.New("日志记录器不能为空")
	}

	var (
		consumer         *kafka.Consumer
		deadLetterWriter *kafka.DeadLetterProducer
		searchClient     *elasticsearch.Client
	)
	// 后续构造失败时也要释放已经创建的客户端，避免启动失败泄漏 goroutine 和连接。
	defer func() {
		if resultErr == nil {
			return
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if cleanupErr := closeApplicationResources(
			cleanupContext,
			consumer,
			deadLetterWriter,
			searchClient,
		); cleanupErr != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("清理未完成的 log-processor 依赖: %w", cleanupErr),
			)
		}
	}()

	var err error
	consumer, err = kafka.NewConsumer(kafka.Config{
		Brokers: cfg.KafkaBrokers,
		GroupID: cfg.KafkaGroupID,
		Topics:  cfg.KafkaTopics,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 Kafka 消费者: %w", err)
	}

	searchClient, err = elasticsearch.NewClient(cfg.ElasticsearchEndpoint)
	if err != nil {
		return nil, fmt.Errorf("创建 Elasticsearch 客户端: %w", err)
	}

	deadLetterWriter, err = kafka.NewDeadLetterProducer(kafka.DeadLetterProducerConfig{
		Brokers: cfg.KafkaBrokers,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 Kafka 死信生产者: %w", err)
	}

	processor, err := pipeline.New(
		pipeline.Config{
			Index:         cfg.ElasticsearchIndex,
			WriteTimeout:  cfg.WriteTimeout,
			CommitTimeout: cfg.CommitTimeout,
		},
		searchClient,
		consumer,
	)
	if err != nil {
		return nil, fmt.Errorf("创建单记录处理器: %w", err)
	}
	deliveryCycle, err := pipeline.NewDeliveryCycle(processor)
	if err != nil {
		return nil, fmt.Errorf("创建有界投递周期: %w", err)
	}
	batchProcessor, err := pipeline.NewBatchProcessor(
		pipeline.Config{
			Index:         cfg.ElasticsearchIndex,
			WriteTimeout:  cfg.WriteTimeout,
			CommitTimeout: cfg.CommitTimeout,
		},
		searchClient,
		consumer,
	)
	if err != nil {
		return nil, fmt.Errorf("创建批量处理器: %w", err)
	}
	batchDeliveryCycle, err := pipeline.NewBatchDeliveryCycle(batchProcessor)
	if err != nil {
		return nil, fmt.Errorf("创建批量有界投递周期: %w", err)
	}
	deadLetterHandler, err := pipeline.NewDeadLetterHandler(
		pipeline.DeadLetterConfig{
			PublishTimeout: cfg.DeadLetterPublishTimeout,
			CommitTimeout:  cfg.CommitTimeout,
		},
		deadLetterWriter,
		consumer,
	)
	if err != nil {
		return nil, fmt.Errorf("创建死信处理器: %w", err)
	}
	runner, err := pipeline.NewBatchRunner(
		cfg.BatchSize,
		consumer,
		batchDeliveryCycle,
		deliveryCycle,
		deadLetterHandler,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("创建批量处理循环: %w", err)
	}
	healthState := newHealthState()
	healthServer, err := newHealthService(cfg.HealthAddress, healthState)
	if err != nil {
		return nil, fmt.Errorf("创建健康检查服务: %w", err)
	}

	return &application{
		run: func(ctx context.Context) error {
			return runApplication(ctx, runner.Run, healthServer, healthState)
		},
		close: func(ctx context.Context) error {
			return closeApplicationResources(
				ctx,
				consumer,
				deadLetterWriter,
				searchClient,
			)
		},
	}, nil
}

func closeApplicationResources(
	ctx context.Context,
	consumer *kafka.Consumer,
	deadLetterWriter *kafka.DeadLetterProducer,
	searchClient *elasticsearch.Client,
) error {
	if ctx == nil {
		return errors.New("关闭 context 不能为空")
	}
	// franz-go 的 Close 不接收 context，因此三个独立资源并行关闭。即使 Kafka
	// 离组暂时阻塞，调用方仍会在统一预算到期时返回，且其他资源已经开始清理。
	return runCloseOperations(
		ctx,
		func() error {
			consumer.Close()
			return nil
		},
		func() error {
			deadLetterWriter.Close()
			return nil
		},
		func() error {
			if err := searchClient.Close(ctx); err != nil {
				return fmt.Errorf("关闭 Elasticsearch 客户端: %w", err)
			}
			return nil
		},
	)
}

func runCloseOperations(ctx context.Context, operations ...func() error) error {
	if ctx == nil {
		return errors.New("关闭 context 不能为空")
	}
	for _, operation := range operations {
		if operation == nil {
			return errors.New("关闭操作不能为空")
		}
	}

	results := make(chan error, len(operations))
	for _, operation := range operations {
		operation := operation
		go func() {
			results <- operation()
		}()
	}

	remaining := len(operations)
	errorsToReturn := make([]error, 0, remaining+1)
	for remaining > 0 {
		select {
		case err := <-results:
			remaining--
			if err != nil {
				errorsToReturn = append(errorsToReturn, err)
			}
		case <-ctx.Done():
			// 先收集已经完成的结果，避免截止时间与最后一个关闭结果同时到达时误报。
			for remaining > 0 {
				select {
				case err := <-results:
					remaining--
					if err != nil {
						errorsToReturn = append(errorsToReturn, err)
					}
				default:
					errorsToReturn = append(
						errorsToReturn,
						fmt.Errorf("等待资源关闭: %w", ctx.Err()),
					)
					return errors.Join(errorsToReturn...)
				}
			}
		}
	}
	return errors.Join(errorsToReturn...)
}
