package config

import "time"

// newValidConfig 构造一份通过全部校验规则的基准配置，供各测试在其上做单点变异。
// 覆盖：mq 通配路由、mq 精确 tag 路由、canal 精确 event 路由、三种 scope 暂停。
func newValidConfig() *Config {
	return &Config{
		Listeners: []*Listener{
			{ID: "l-order", Source: ListenerSourceMQ, Topic: "order-topic", Group: "g-order", Enabled: true},
			{ID: "l-canal", Source: ListenerSourceCanal, Topic: "canal-topic", Group: "g-canal", Enabled: true},
		},
		Routes: []*Route{
			{ID: "r-all", Match: MatchRule{Topic: "order-topic"}, Subscribers: []string{"s-erp"}},
			{ID: "r-created", Match: MatchRule{Topic: "order-topic", Tag: "created"}, Subscribers: []string{"s-erp", "s-wms"}},
			{ID: "r-canal", Match: MatchRule{DB: "shop", Table: "refund", Event: EventInsert}, Subscribers: []string{"s-wms"}},
		},
		Subscribers: []*Subscriber{
			newSubscriber("s-erp", "http://erp.internal/hook"),
			newSubscriber("s-wms", "https://wms.internal/hook"),
		},
		Pauses: []*Pause{
			{ID: "p-topic", Scope: PauseScopeTopic, Target: "order-topic", Reason: "演练", Expire: 0},
			{ID: "p-tag", Scope: PauseScopeTag, Target: "order-topic:refunded", Reason: "止损", Expire: time.Now().Add(time.Hour).Unix()},
			{ID: "p-sub", Scope: PauseScopeSubscriber, Target: "s-wms", Reason: "隔离", Expire: 0},
		},
	}
}

// newSubscriber 构造一个字段完整、可通过校验的订阅方。
func newSubscriber(id, endpoint string) *Subscriber {
	return &Subscriber{
		ID:       id,
		Endpoint: endpoint,
		Timeout:  Duration(5 * time.Second),
		Retry: RetryPolicy{
			MaxAttempts:    5,
			InitialBackoff: Duration(time.Second),
			MaxBackoff:     Duration(2 * time.Minute),
			Multiplier:     2.0,
		},
		Auth: CallbackAuth{Mode: AuthModeNone},
	}
}
