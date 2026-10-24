// Package dispatcher 分发引擎（蓝本 3.2 核心层）。
//
// 本 D03 切片只落地「路由消费契约」一角：把 config 的 RoutingTable 匹配结果
// 收敛为一条消息的就绪投递目标列表（Router.ResolveMQ/ResolveCanal）。
// 完整分发机制——熔断、幂等去重、per-sub 重试/park、DLQ、ack 编排——留后续 D0n / M2 全量（D03 范围边界）。
//
// 边界决策：
//   - 依赖方向 dispatcher → config（蓝本 6.10），config 不反向依赖本包；
//   - 不引入 envelope：解析入参用原始维度（topic/tag、db/table/event），
//     与 config.MatchMQ/MatchCanal 签名天然对齐，envelope 落地时加薄适配层映射（D03 边界 2）；
//   - 解析为纯计算：不 ack、不落库、不投递、不记录，空列表即「无路由或全被暂停」。
package dispatcher

import "message-bus/internal/config"

// Target 一个就绪投递目标：命中路由 + 解引用后的订阅方。
// 投递（callback）、重试、去重由后续能力承接，本契约只产出目标不做副作用。
type Target struct {
	Route *config.Route
	Sub   *config.Subscriber
}

// Router 路由消费器，只持 TableStore 引用（构造注入）。
// 每次解析按 Snapshot() 取当下不可变快照，与在途 Reload 无锁并发安全（D02-4）。
type Router struct {
	store config.TableStore
}

// NewRouter 创建路由消费器。store 通常由装配层经 config.NewTableStore + RunWatch 建好。
func NewRouter(store config.TableStore) *Router {
	return &Router{store: store}
}

// ResolveMQ 按 mq 维度解析投递目标（D03-3）：
//  1. 源级暂停短路——topic 命中 PausedTopic、或 topic+tag 命中 PausedTag（tag 空串不命中），
//     整条消息不扇出（V1 丢弃语义由调用方 ack，D02-6）；
//  2. Snapshot().MatchMQ(topic, tag) 取命中路由（通配桶在前 + 精确桶在后，顺序沿用 D02-4）；
//  3. 逐路由解引用 Subscribers：命中 PausedSubscriber 的目标跳过，其余入列。
//
// 返回空列表表示无路由或全部被暂停。
func (r *Router) ResolveMQ(topic, tag string) []Target {
	snap := r.store.Snapshot()
	// 步骤1：源级暂停短路，避免对已停发消息做无谓解引用
	if snap.PausedTopic(topic) || snap.PausedTag(topic, tag) {
		return nil
	}
	return fanOut(snap, snap.MatchMQ(topic, tag))
}

// ResolveCanal 按 canal 维度解析投递目标：入参 db/table/event，走 MatchCanal。
// topic/tag 暂停语义对 canal 不适用，仅 PausedSubscriber 生效。
func (r *Router) ResolveCanal(db, table, event string) []Target {
	snap := r.store.Snapshot()
	return fanOut(snap, snap.MatchCanal(db, table, event))
}

// fanOut 将命中路由展开为订阅方目标：订阅方级暂停过滤 + 解引用。
// 引用完整性已在 Reload 校验，Subscriber(id) 未命中即表被外部误用，跳过而非 panic。
func fanOut(snap *config.RoutingTable, routes []*config.Route) []Target {
	if len(routes) == 0 {
		return nil
	}
	var out []Target
	for _, rt := range routes {
		for _, id := range rt.Subscribers {
			if snap.PausedSubscriber(id) {
				continue
			}
			sub, err := snap.Subscriber(id)
			if err != nil {
				continue
			}
			out = append(out, Target{Route: rt, Sub: sub})
		}
	}
	return out
}
