package pipeline

import "github.com/Donking-36/distributed-log-platform/internal/elasticsearch"

// ResultKind 表示一条 Kafka 记录经过解析、写入和确认后的端到端结果。
type ResultKind string

const (
	// ResultCreated 表示 Elasticsearch 已创建文档，且 Kafka 位点提交成功。
	ResultCreated ResultKind = "created"
	// ResultDuplicate 表示 Elasticsearch 已确认重复文档，且 Kafka 位点提交成功。
	ResultDuplicate ResultKind = "duplicate"
	// ResultInvalid 表示记录永久不符合事件契约；DLQ 写入路径尚未实现，因此不能提交位点。
	ResultInvalid ResultKind = "invalid"
	// ResultRetryableFailure 表示暂时性写入故障，调用方后续可执行有界重试。
	ResultRetryableFailure ResultKind = "retryable_failure"
	// ResultSystemFailure 表示配置、协议或内部不变量故障，必须停止推进位点。
	ResultSystemFailure ResultKind = "system_failure"
	// ResultCommitFailure 表示 Elasticsearch 已给出可确认结果，但 Kafka 提交未成功。
	ResultCommitFailure ResultKind = "commit_failure"
	// ResultCanceled 表示在取得可确认的 Elasticsearch 结果前处理被取消。
	ResultCanceled ResultKind = "canceled"
)

// Result 保存单条记录的端到端结果。
// ElasticsearchResult 只表示已取得的可信逐项结果；Committed 只有在 Kafka
// 明确确认位点提交后才为 true，避免把存储成功和消费进度成功混为一谈。
type Result struct {
	Kind                ResultKind
	ElasticsearchResult elasticsearch.ResultKind
	Committed           bool
}

// DeliveryResult 保存一次有界投递周期的最终状态。
// LastResult 是最后一次 Processor 尝试的原始结果；Attempts 是实际尝试次数。
// Exhausted 只在第六次仍为可重试或提交失败时为 true，取消和终止故障不会耗尽预算。
type DeliveryResult struct {
	LastResult Result
	Attempts   int
	Exhausted  bool
}
