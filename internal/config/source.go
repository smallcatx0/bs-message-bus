package config

import "context"

// ConfigOpKind 变更操作类型。
type ConfigOpKind string

const (
	// ConfigOpUpsert/ConfigOpDelete 为增量语义预留位（gRPC 契约范畴，蓝本 TODO 1，V1 不发送）。
	ConfigOpUpsert ConfigOpKind = "Upsert"
	ConfigOpDelete ConfigOpKind = "Delete"
	// ConfigOpReload 全量重载语义：V1 所有事件一律发 Reload（D02-2）。
	ConfigOpReload ConfigOpKind = "Reload"
)

// ConfigEvent 一次配置变更通知。
//
// V1 只使用全量语义：事件仅携带来源标识与操作类型，由 Reload 侧重读→校验→整体重建，
// 不做增量 diff（增量协议属 gRPC 契约范畴，蓝本 TODO 1 本轮不对齐）。
type ConfigEvent struct {
	Source string       // "file" | "static" | 未来 "grpc"
	Op     ConfigOpKind // V1 fileSource 一律发 Reload
}

// ConfigSource 配置来源抽象：占位实现与未来 grpcSource 可无缝替换（蓝本 6.8），
// 上层（Reload 管线与消费方）不感知具体来源。
//
// 生命周期约定：
//   - Load 同步返回当前完整配置（内部复用「解析→校验→构建」管线，首载与热载一致）；
//   - Watch 基于 ctx 启动监听 goroutine：ctx 取消 → goroutine 终止并关闭输出 channel；
//     goroutine panic → recover、记日志后同样关闭输出 channel（不静默复活，
//     交由装配层决定重启或退出，D02-3）；channel 关闭即「本来源不再产生事件」。
type ConfigSource interface {
	// Load 读取并校验当前完整配置；失败返回 error，不产生副作用。
	Load(ctx context.Context) (*Config, error)
	// Watch 启动变更监听，返回事件 channel（带背压，可能丢弃冗余事件，
	// 因 Reload 幂等且全量，丢失冗余通知不影响正确性）。
	Watch(ctx context.Context) (<-chan ConfigEvent, error)
}
