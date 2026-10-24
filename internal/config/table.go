package config

import (
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"message-bus/pkg/logger"
	"message-bus/pkg/mask"
)

// TableStore 内存路由表存取（蓝本 6.8 草图，Snapshot/Reload 语义不变）。
type TableStore interface {
	// Snapshot 无锁读当前路由表快照；返回值为不可变对象，禁止就地修改。
	Snapshot() *RoutingTable
	// Reload 校验 → 重建 → 原子替换。失败策略（D02-4）：拒绝新配置、保留旧表，
	// 返回 error 并记 ERROR 日志（含脱敏上下文）——错误配置宁可用旧的正确路由，
	// 绝不清空表导致全量丢消息。
	Reload(cfg *Config) error
}

// RoutingTable 四类配置的不可变内存快照，构建时完成预索引（蓝本 7.2、D02-4）。
// 构建后只读，所有字段禁止导出写方法。
type RoutingTable struct {
	// mq 维度：topic → 通配路由（tag 空）；"topic:tag" → 精确路由。分桶设计：
	// 读路径 = 通配桶 + 精确桶拼接，无遍历无去重（热点路径，规范第 6 节）
	mqWildcard map[string][]*Route
	mqExact    map[string][]*Route
	// canal 维度："db.table" → 通配路由（event 空）；"db.table|EVENT" → 精确路由
	canalWildcard map[string][]*Route
	canalExact    map[string][]*Route
	// 订阅方：subscriber_id → *Subscriber（Route 只存 ID，扇出时解引用）
	subs map[string]*Subscriber
	// listeners / pauses 原样索引（消费方为 broker / dispatcher）
	listeners                            []*Listener
	pausedTopics, pausedTags, pausedSubs map[string]*Pause

	// raw 保存构建本表的配置（只读），供 Fingerprint 比对。
	raw *Config
}

// tableStore atomic.Pointer 承载的 TableStore 默认实现（蓝本 7.2）。
type tableStore struct {
	ptr atomic.Pointer[RoutingTable]
}

// NewTableStore 创建内存路由表存储；以空表起步，首次装载由装配层 Reload/Load 完成。
func NewTableStore() TableStore {
	t := &tableStore{}
	t.ptr.Store(emptyTable())
	return t
}

func (t *tableStore) Snapshot() *RoutingTable { return t.ptr.Load() }

// Reload 校验并原子替换；失败保留旧表（策略见 TableStore.Reload 注释）。
// 失败记 ERROR（含脱敏上下文）、成功记 INFO（按 subscriber 的 token 经 mask 脱敏）。
func (t *tableStore) Reload(cfg *Config) error {
	nt, err := buildTable(cfg)
	if err != nil {
		// 步骤1：失败拒绝新配置、保留旧表；日志只携结构错，不携 token/secret 明文
		logger.L().Error("config: Reload 校验失败，保留旧路由表", zap.Error(err))
		return err
	}
	t.ptr.Store(nt)
	// 步骤2：成功日志：总量指标 + 各订阅方 auth token 脱敏后回显（D02-7：config 为 mask 首个使用方）
	fields := []zap.Field{
		zap.Int("listeners", len(nt.listeners)),
		zap.Int("subscribers", len(nt.subs)),
	}
	for _, s := range cfg.Subscribers {
		if s.Auth.Mode == AuthModeStaticHeader {
			fields = append(fields, mask.TokenField("token_"+s.ID, s.Auth.Token))
		}
	}
	logger.L().Info("config: 路由表已热更新", fields...)
	return nil
}

// buildTable 执行「校验 → 预索引构建」管线。Load 与 Reload 复用同一入口，
// 保证首载与热载行为一致（D02-2）。
func buildTable(cfg *Config) (*RoutingTable, error) {
	if err := Validate(cfg); err != nil {
		return nil, err
	}
	nt := &RoutingTable{
		mqWildcard:    map[string][]*Route{},
		mqExact:       map[string][]*Route{},
		canalWildcard: map[string][]*Route{},
		canalExact:    map[string][]*Route{},
		subs:          map[string]*Subscriber{},
		pausedTopics:  map[string]*Pause{},
		pausedTags:    map[string]*Pause{},
		pausedSubs:    map[string]*Pause{},
		raw:           cfg,
	}
	// 步骤1：订阅方索引
	for _, s := range cfg.Subscribers {
		nt.subs[s.ID] = s
	}
	// 步骤2：路由预索引。通配（tag/event 空串）与精确分桶存放，
	// 读路径退化为两次 map 查找 + 拼接，避免每消息遍历路由表
	for _, r := range cfg.Routes {
		if r.Match.Topic != "" {
			if r.Match.Tag == "" {
				nt.mqWildcard[r.Match.Topic] = append(nt.mqWildcard[r.Match.Topic], r)
			} else {
				key := mqTagKey(r.Match.Topic, r.Match.Tag)
				nt.mqExact[key] = append(nt.mqExact[key], r)
			}
		}
		if r.Match.DB != "" && r.Match.Table != "" {
			tk := r.Match.DB + "." + r.Match.Table
			if r.Match.Event == "" {
				nt.canalWildcard[tk] = append(nt.canalWildcard[tk], r)
			} else {
				nt.canalExact[tk+"|"+r.Match.Event] = append(nt.canalExact[tk+"|"+r.Match.Event], r)
			}
		}
	}
	// 步骤3：暂停按 scope 预分组（生效判定放读取时，见 Paused* 方法）
	for _, p := range cfg.Pauses {
		switch p.Scope {
		case PauseScopeTopic:
			nt.pausedTopics[p.Target] = p
		case PauseScopeTag:
			nt.pausedTags[p.Target] = p
		case PauseScopeSubscriber:
			nt.pausedSubs[p.Target] = p
		}
	}
	nt.listeners = cfg.Listeners
	return nt, nil
}

func emptyTable() *RoutingTable {
	return &RoutingTable{
		mqWildcard:    map[string][]*Route{},
		mqExact:       map[string][]*Route{},
		canalWildcard: map[string][]*Route{},
		canalExact:    map[string][]*Route{},
		subs:          map[string]*Subscriber{},
		listeners:     nil,
	}
}

// ConfigFingerprint 当前表所承载配置的指纹，供 Watch 侧做变更去重。
// 空表（未装载）返回 0。
func (t *RoutingTable) ConfigFingerprint() (uint64, error) {
	if t == nil || t.raw == nil {
		return 0, nil
	}
	return Fingerprint(t.raw)
}

// MatchMQ 返回 mq 维度命中的路由：通配桶（tag 空）+ "topic:tag" 精确桶。
// 通配路由在前、精确路由在后，各自保持配置声明顺序；返回切片为表内部数据与
// 拼接结果，调用方只读。
func (t *RoutingTable) MatchMQ(topic, tag string) []*Route {
	if topic == "" {
		return nil
	}
	wild := t.mqWildcard[topic]
	if tag == "" {
		return wild
	}
	exact := t.mqExact[mqTagKey(topic, tag)]
	if len(exact) == 0 {
		return wild
	}
	if len(wild) == 0 {
		return exact
	}
	out := make([]*Route, 0, len(wild)+len(exact))
	out = append(out, wild...)
	return append(out, exact...)
}

// MatchCanal 返回 canal 维度命中的路由：通配桶（event 空）+ "db.table|EVENT" 精确桶。
func (t *RoutingTable) MatchCanal(db, table, event string) []*Route {
	if db == "" || table == "" {
		return nil
	}
	tk := db + "." + table
	wild := t.canalWildcard[tk]
	if event == "" {
		return wild
	}
	exact := t.canalExact[tk+"|"+event]
	if len(exact) == 0 {
		return wild
	}
	if len(wild) == 0 {
		return exact
	}
	out := make([]*Route, 0, len(wild)+len(exact))
	out = append(out, wild...)
	return append(out, exact...)
}

// Subscriber 解引用订阅方；引用完整性已在 Reload 校验，未命中即表被外部误用。
func (t *RoutingTable) Subscriber(id string) (*Subscriber, error) {
	s, ok := t.subs[id]
	if !ok {
		return nil, fmt.Errorf("config: 订阅方 %q 不在当前路由表", id)
	}
	return s, nil
}

// PausedTopic topic 作用域暂停是否生效（Expire 读取时判定，D02-4/6）。
func (t *RoutingTable) PausedTopic(topic string) bool { return pauseActive(t.pausedTopics[topic]) }

// PausedTag tag 作用域暂停是否生效，target 编码 "topic:tag"（D02-8）。
func (t *RoutingTable) PausedTag(topic, tag string) bool {
	return pauseActive(t.pausedTags[mqTagKey(topic, tag)])
}

// PausedSubscriber subscriber 作用域暂停是否生效。
func (t *RoutingTable) PausedSubscriber(id string) bool { return pauseActive(t.pausedSubs[id]) }

// Listeners 返回监听定义列表（只读）。
func (t *RoutingTable) Listeners() []*Listener { return t.listeners }

// pauseActive 暂停是否生效：不存在或已过期（now > Expire）即不生效；Expire=0 永久。
func pauseActive(p *Pause) bool {
	if p == nil {
		return false
	}
	if p.Expire == 0 {
		return true
	}
	return time.Now().Unix() <= p.Expire
}

// mqTagKey mq 维度 topic+tag 的索引键；tag 为空返回空串（表示走通配桶）。
func mqTagKey(topic, tag string) string {
	if tag == "" {
		return ""
	}
	return topic + ":" + tag
}
